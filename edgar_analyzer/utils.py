import pandas as pd
import os
import requests as req
import sys
import re
import dask.dataframe as dd
from lxml import etree
import io
import logging

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    # filename='/temp/myapp.log',
                    filemode='w')
# console = logging.StreamHandler()
logger = logging.getLogger('EdgarAnalyzer')
#logger.setLevel('DEBUG')
# logger.addHandler(console)

dir_curr = os.path.abspath(os.path.dirname(__file__))


def symbol2cik(symbol):
    symbol = str(symbol).upper()
    cik = symbol2cik_sec(symbol) if symbol2cik_file(symbol) is None else symbol2cik_file(symbol)
    return cik


def conv_list(i):
    list_res = None
    if isinstance(i, str) | isinstance(i, int):
        list_res = [i]
    elif isinstance(i, list):
        list_res = i

    return list_res

def symbol2cik_file(symbol):
    symbol = str(symbol).upper()
    path_cik_mapping = os.path.join(dir_curr, 'config', 'cik_mapping.csv')
    df_mapping = pd.read_csv(path_cik_mapping).set_index('Ticker')['CIK']
    if symbol in df_mapping.index:
        if df_mapping[[symbol]].shape[0] == 1:
            cik = str(df_mapping[symbol]).zfill(10)
        else:
            logger.warning('non-unique CIK for Symbol={s} in cik_mapping.csv'.format(s=symbol))
            cik = symbol2cik_sec(symbol)
    else:
        logger.warning('Symbol not found in cik_mapping.csv.')
        cik = None
    return cik


def symbol2cik_sec(symbol, update_mapping=True):
    symbol = str(symbol).upper()
    try:
        uri = "http://www.sec.gov/cgi-bin/browse-edgar"
        resp = req.get(uri, {'CIK': symbol, 'action': 'getcompany'})
        results = re.compile(r'.*CIK=(\d{10}).*').findall(str(resp.content))
        cik = str(results[0])

    except Exception as ex:
        logger.error(ex)
        logger.error('Symbol not found in SEC')
        cik = None
    if update_mapping and (cik is not None):
        update_cik(symbol, cik)
    return cik


def update_cik(symbol, cik):
    logger.warning('update cik_mapping symbol={s}, cik={c}'.format(s=symbol, c=cik))
    symbol = str(symbol).upper()
    path_cik_mapping = os.path.join(dir_curr, 'config', 'cik_mapping.csv')
    df_mapping = pd.read_csv(path_cik_mapping).set_index('Ticker')
    if symbol in df_mapping.index:
        df_mapping = df_mapping.drop(symbol)
    df_mapping.loc[symbol] = int(cik)
    df_mapping.to_csv(path_cik_mapping)


def download_list(list_path, dir_report, uri='https://www.sec.gov/Archives/', force_download=False):
    from multiprocessing.pool import ThreadPool
    list_file = [os.path.join(dir_report, p) for p in list_path]
    if not force_download:
        list_path = [p for p in list_path if not os.path.exists(os.path.join(dir_report, p))]
    # list_url = [uri+p for p in list_path]
    # list_file = [os.path.join(dir, p) for p in list_path]
    def download_url(p):
        r = req.get(uri + p, stream=True)
        path_save = os.path.join(dir_report, p)
        logger.info('downloading {f}'.format(f=path_save))
        if r.status_code == 200:
            dir_name = os.path.dirname(path_save)
            if not os.path.isdir(dir_name):
                os.makedirs(dir_name)
            with open(path_save, 'wb') as f:
                for chunk in r:
                    f.write(chunk)
        else:
            logger.error('error downloading {f}'.format(f=uri + p))
        return path_save
    results = ThreadPool(4).imap_unordered(download_url, list_path)
    # for l in results:
    #     logger.info('downloaded '+l)
    return list_file


def re_string(keyword, data):
    s = re.search(r'<{kw}>([\s\S]*?)\n'.format(kw=keyword), data)
    res = s.group(1) if s else None
    return res


def re_tag(tag, data, find_all=False):
    s = re.search(r'<{tag}>\n([\s\S]*?)\n<\/{tag}>'.format(tag=tag), data)
    res = s.group(1) if s else None
    return res


def node2dict(node):
    d = {}
    for c in node.iterchildren():
        key = c.tag.split('}')[1] if '}' in c.tag else c.tag
        value = c.text if c.text else node2dict(c)
        d[key] = value
    return d


def parse_ins(txt_ins, has_dimension=False):
    xbrl_ins = re_tag('XBRL', txt_ins)
    if xbrl_ins is None:
        xbrl_ins = re_tag('XML', txt_ins)
    xbrl_ins = xbrl_ins.replace('>\n', '>')
    r_ins = etree.fromstring(xbrl_ins.encode('utf-8'))
    ns_ins = {k:v for k,v in r_ins.nsmap.items() if k is not None}
    if 'xbrli' not in ns_ins.keys():
        logger.info('fix missing namespace xbrli. {s}'.format(s=ns_ins))
        ns_ins['xbrli'] = "http://www.xbrl.org/2003/instance"
    list_context = r_ins.findall(r'xbrli:context', namespaces=ns_ins)
    list_period = [dict(i.attrib, **node2dict(i.find('xbrli:period', namespaces=ns_ins))) for i in list_context]
    df_period = pd.DataFrame(list_period)
    # if 'id' not in df_period.columns:
    #     print(r_ins[:10])
    #     print(r_ins.findall('context')[:10])
    #     print(len(list_context))
    #     print(len(list_period))
    #     print(df_period.head())
    df_period = df_period.set_index('id')
    # df_period.head()
    list_unit = r_ins.findall('xbrli:unit', namespaces=ns_ins)
    df_unit = pd.DataFrame([dict(i.attrib, **{'unit': i[0].text.split(':')[-1]})
                            for i in list_unit]).set_index('id')
    # df_unit
    list_dim = r_ins.xpath('.//*[@dimension]')
    df_dim = pd.DataFrame([dict(d.attrib, **{'member': d.text,
                                             'id': d.getparent().getparent().getparent().attrib['id']})
                           for d in list_dim]).set_index('id')
    # df_dim.head()
    list_measure = r_ins.xpath('.//*[@contextRef]')
    df_measure = pd.DataFrame([dict(i.attrib, **{'measure': i.tag, 'value': i.text}) for i in list_measure])
    # df_measure.head()
    df_merge = df_measure.join(df_period, on='contextRef').join(df_unit, on='unitRef').join(df_dim, on='contextRef')
    ns_reverse = {v: k for k, v in ns_ins.items()}
    df_merge['ns'] = df_merge.measure.apply(lambda ns: ns_reverse[re.search('{(.*)}', ns).group(1)])
    df_merge['item'] = df_merge['ns'] +":" +df_merge.measure.apply(lambda x: x.split('}')[-1])
    # df_merge['endDate'] = df_merge.endDate
    df_merge.endDate.update(df_merge.instant)
    df_merge.startDate.update(df_merge.instant)
    #parse dtype
    df_merge.endDate = pd.to_datetime(df_merge.endDate, infer_datetime_format=True)
    df_merge.startDate = pd.to_datetime(df_merge.startDate, infer_datetime_format=True)
    df_merge.value = pd.to_numeric(df_merge.value, errors='ignore', downcast='integer')
    df_merge.decimals = pd.to_numeric(df_merge.decimals, errors='ignore', downcast='integer')
    # re.search('{(.*)}', ns).group(1)
    # df_merge.head()
    df_ins = df_merge[['item', 'startDate', 'endDate', 'value', 'decimals', 'unit', 'ns',
                       'dimension', 'member']].drop_duplicates()
    if not has_dimension:
        df_ins = df_ins[df_ins.dimension.isna()]
    return df_ins


def parse_header(header, include_filer=False):
    dict_replace = {'\t': "", ":": "|", '<': "", '>': '|'}
    for k, v in dict_replace.items():
        header = header.replace(k, v)
    srs_header = pd.read_csv(io.StringIO(header), sep='|', header=None).set_index(0)[1]
    if not include_filer:
        srs_header = srs_header['ACCEPTANCE-DATETIME':'FILER'].dropna()
    for i in srs_header.index:
        if ('DATETIME' in i):
            srs_header[i] = pd.to_datetime(srs_header[i])
        elif ('DATE' in i) or ('PERIOD' in i):
            srs_header[i] = pd.to_datetime(srs_header[i]).date()
    return srs_header


def parse_doc(doc, include_text=False):
    # doc = re_doc[-1]
    res = {}
    re_string('TYPE', doc)
    for i in ['TYPE', 'SEQUENCE', 'FILENAME', 'DESCRIPTION']:
        res[i] = re_string(i, doc)
    if include_text:
        res['TEXT'] = re_tag('TEXT', doc)
    return pd.Series(res)
