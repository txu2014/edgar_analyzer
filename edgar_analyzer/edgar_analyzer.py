import pandas as pd
import os
import sys
#from bs4 import BeautifulSoup as bs

from .utils import *

class EdgarBase(object):
    def __init__(self):
        #self.dir_edgar =
        # self.__dir_download = None
        # self.__dir_data = None
        self.__dir_output = None
        self.ulr_sec = 'https://www.sec.gov/Archives/'
        self.__dir_config = None
        self.dir_curr = os.path.abspath(os.path.dirname(__file__))
        self.dir_config = os.path.join(self.dir_curr, 'config')
        self.today = pd.datetime.today()
        self.__fact_mapping = None
    @property
    def dir_edgar(self):
        if 'DIR_EDGAR' not in os.environ:
            logger.error('please set environment variable DIR_EDGAR')
            logger.error("os.environ['DIR_EDGAR']=/path/to/dir'")
            import tempfile
            os.environ['DIR_EDGAR'] = tempfile.gettempdir()
        return os.environ['DIR_EDGAR']


    @property
    def dir_download(self):
        dir_download = os.path.join(self.dir_edgar, 'download')
        if not os.path.isdir(dir_download):
            os.makedirs(dir_download)
        return dir_download

    def set_dir_config(self, dir_input):
        logger.info('setting dir_config={f}'.format(f=dir_input))
        self.dir_curr = dir_input

    @property
    def fact_mapping(self):
        if self.__fact_mapping is None:
            path_fact_mapping = os.path.join(self.dir_config, 'fact_mapping.csv')
            logger.info('reading fact_mapping from {f}'.format(f=path_fact_mapping))
            fm = pd.read_csv(path_fact_mapping).set_index('item')
            self.__fact_mapping = fm
        else:
            fm = self.__fact_mapping
        return fm

class EdgarDownloader(EdgarBase):
    def __init__(self):
        super(EdgarDownloader, self).__init__()
        self.__conn_master_db = None
        self.valid_form_type = ['10-Q', '10-K', '8-K']

    @property
    def dir_master(self):
        dir_master = os.path.join(self.dir_download, 'master')
        if not os.path.isdir(dir_master):
            os.makedirs(dir_master)
        return dir_master

    @property
    def conn_master_db(self):
        file_master_db = os.path.join(self.dir_edgar, 'master_idx.db')
        if self.__conn_master_db is None:
            import sqlite3
            if not os.path.exists(file_master_db):
                conn = sqlite3.connect(file_master_db)
                #df_init = pd.DataFrame(index=['cik', 'company','form_type','file_date','file_name'])
                pd.DataFrame().to_sql('master_idx', conn)
            else:
                conn = sqlite3.connect(file_master_db)
            self.__conn_master_db = conn
        return self.__conn_master_db

    def _close_master_db(self):
        conn = self.__conn_master_db
        conn.close()
        self.__conn_master_db = None

    def _update_master_db(self, list_files):
        conn = self.conn_master_db
        dfs = dd.read_csv(list_files, sep='|')
        df_load = dfs[dfs.form_type.isin(self.valid_form_type)].compute()
        sql_all = 'select * from master_idx'
        df_all = pd.read_sql_query(sql_all, conn)
        logger.info('read master_idx db, n={s}'.format(s=df_all.shape[0]))
        df_all = pd.concat([df_all, df_load], sort=False).drop_duplicates()
        df_all.to_sql('master_idx', conn, if_exists='replace', index=False)
        logger.info('write master_idx db, n={s}'.format(s=df_all.shape[0]))
        return 0

    def _refresh_master_idx(self, yyyy, q, force=False):
        # yyyy, q = self._year_quarter(date)
        file_master = os.path.join(self.dir_master, "{y}_QTR{q}_master.csv".format(y=yyyy, q=q))
        if not os.path.exists(file_master) or force:
            url_master = self._path_master_idx(yyyy, q)
            logger.info('downloading {f}'.format(f=url_master))
            resp = req.get(url_master)
            if resp.status_code != 200:
                logger.error('error downloading {f}'.format(f=url_master))
            else:
                write_data = '\n'.join(resp.content.decode('latin1').split('\n')[11:])
                logger.info('saving {f}'.format(f=file_master))
                with open(file_master, 'w+', encoding='utf-8') as f:
                    f.write("cik|company|form_type|file_date|file_name\n")
                    f.write(write_data)
                self._update_master_db([file_master])
        else:
            logger.info('use existing file. {f}'.format(f=file_master))
        return file_master

    def filings_between(self, symbol, start_date, end_date=None, form_type='10-K'):
        list_year_quarter = self._year_quarter_between(start_date, end_date)
        list_master_file = [self._refresh_master_idx(t[0], t[1]) for t in list_year_quarter]
        #dfs = dd.read_csv(list_master_file, sep='|')
        cik = int(symbol2cik(symbol))
        #df_res = dfs[(dfs.cik == cik) & (dfs.form_type == form_type)].compute()
        sql_filings = "select * from master_idx where cik=={cik} and form_type=='{f}' " \
                      "and file_date>='{t0}' ".format(cik=cik, f=form_type, t0=pd.to_datetime(start_date).date())
        if end_date:
            sql_filings += "and file_date<'{t1}'".format(t1=pd.to_datetime(end_date).date())
        df_res = pd.read_sql_query(sql_filings, self.conn_master_db)
        list_form = df_res.file_name.tolist()
        list_file = download_list(list_form, self.dir_download, force_download=False)
        return list_file

    @staticmethod
    def _path_master_idx(yyyy, q):
        url = "https://www.sec.gov/Archives/edgar/full-index/{yyyy}/QTR{q}/master.idx".format(yyyy=yyyy, q=q)
        return url

    @staticmethod
    def _year_quarter(date=pd.datetime.today()):
        t = pd.to_datetime(date).date()
        return t.year, (t.month - 1) // 3 + 1

    def _year_quarter_between(self, start_date, end_date=None):
        end_date = self.today if end_date is None else pd.to_datetime(end_date)
        #end_date += pd.DateOffset(months=3)
        start_date = pd.to_datetime(start_date)
        logger.info('using quarters between {t0} to {t1}'.format(t0=start_date, t1=end_date))
        # today = pd.datetime.today()
        list_year_quarter = [self._year_quarter(t) for t in
                             pd.date_range(start_date, end_date, freq='3M')]
        return list_year_quarter


class EdgarParser(EdgarBase):
    def __init__(self):
        super(EdgarParser, self).__init__()
        #from arelle import Cntlr
        self.ins_type = ['EX-101.INS']
        self.xbrl_type = ['EX-101.INS','EX-101.SCH','EX-101.CAL','EX-101.CAL','EX-101.LAB','EX-101.PRE','EX-101.DEF']
        #self.ctl = Cntlr.Cntlr()
        self.ed = EdgarDownloader()

    def _parse_txt(self, f_txt, header_only=False):
        filing_id = os.path.basename(f_txt).split('.')[0]
        if os.path.exists(f_txt):
            with open(f_txt, 'r+') as f:
                data = f.read()
                re_header = re.search('<SEC-HEADER>([\s\S]*?)\n<\/SEC-HEADER>', data).group(1)
                if not header_only:
                    re_doc = re.findall('<DOCUMENT>\n([\s\S]*?)\n<\/DOCUMENT>', data)
        else:
            logger.error('file not exists. file={f}'.format(f=f_txt))
        dict_header_mapping = {"ACCESSION NUMBER": 'filing_id',
                               'ACCEPTANCE-DATETIME': 'filing_datetime',
                               'CONFORMED PERIOD OF REPORT': 'report_period',
                               'FILED AS OF DATE': 'asof_date',
                               'DATE AS OF CHANGE': 'change_date',
                               'CONFORMED SUBMISSION TYPE': 'form_type',
                               'PUBLIC DOCUMENT COUNT': 'doc_count',}
        srs_header = parse_header(re_header)
        if header_only:
            df_doc = srs_header.to_frame().T.rename(columns=dict_header_mapping)
        else:
            list_df_doc = [parse_doc(d, include_text=True) for d in re_doc]
            df_doc = pd.concat(list_df_doc, axis=1).T
            for k, v in dict_header_mapping.items():
                df_doc[v] = srs_header[k]
        #df_doc['filing_id'] = filing_id

        # df_doc['file_path'] = f_txt
        df_doc['txt_dir'] = os.path.dirname(f_txt)
        df_doc['txt_name'] = os.path.basename(f_txt)
        # df_ins = df_doc[df_doc.TYPE=='EX-101.INS']
        return df_doc

    def _parse_ins_from_xml(self, f_xml, **kwargs):
        if not f_xml.startswith('<'):
            logger.info('trying to read f_xml. {f}'.format(f=f_xml[:100]))
            with open(f_xml, 'r+') as f:
                txt_ins = f.read()
        else:
            logger.info('use f_xml as string input. {f}'.format(f=f_xml[:50]))
            txt_ins = f_xml
        df_ins = parse_ins(txt_ins, **kwargs)
        return df_ins

    def _parse_ins_from_txt(self, f_txt, **kwargs):
        df_doc = self._parse_txt(f_txt)
        df_header = df_doc[['filing_id', 'filing_datetime', 'form_type']].iloc[[0]]
        txt_ins = None
        # for k in self.ins_type:
        #     if k in df_doc.TYPE.tolist():
        #         txt_ins = df_doc[df_doc.TYPE == k].TEXT.values[0]
        try:
            df_ins_type = df_doc[df_doc.TYPE.isin(self.ins_type)]
            if df_ins_type.empty:
                logger.warning('cannot find TYPE=EX-101.INS, match file_name {f}'.format(f=f_txt))
                sch_name = df_doc[df_doc.TYPE=='EX-101.SCH'].FILENAME.values[0].split('.')[0]
                df_ins_type = df_doc[(df_doc.TYPE == 'XML') & (df_doc.FILENAME.str.startswith(sch_name))]
                logger.warning('use {f}'.format(f=df_ins_type.FILENAME.values[0]))
                #df_ins_type = df_doc[df_doc.TYPE.isin(['10-K', '10-Q'])]
            txt_ins = df_ins_type.TEXT.values[0]
        except Exception as ex:
            logger.error('cannot find ins TYPE in doc.  list_types={l}'.format(l=df_doc.TYPE.tolist()))
            logger.error('file={f}'.format(f=f_txt))
        df_ins = self._parse_ins_from_xml(txt_ins, **kwargs)
        df_header = df_header.reindex(df_ins.index).fillna(method='ffill')
        df_ins = pd.concat([df_header, df_ins], axis=1)
        return df_ins

    def get_fact(self, ticker, start_date, end_date=None, item=None, form_type='10-K',
                 has_dimension=False):
        list_filings = self.ed.filings_between(ticker, start_date, end_date, form_type=form_type)
        list_df_ins = [self._parse_ins_from_txt(filing, has_dimension=has_dimension) for filing in list_filings]
        df_ins = pd.concat(list_df_ins).set_index('item')
        dict_fact = self.fact_mapping['name'].to_dict()
        df_ins = df_ins.rename(index=dict_fact)
        if item:
            df_ins = df_ins.loc[conv_list(item)]
        return df_ins

#### Arelle not integrated yet.

# class EdgarParseArelle(EdgarParser):
#     def __init__(self):
#         super(EdgarParseArelle, self).__init__()
#         from arelle import Cntlr
#         self.ins_type = ['EX-101.INS']
#         self.xbrl_type = ['EX-101.INS','EX-101.SCH','EX-101.CAL','EX-101.CAL','EX-101.LAB','EX-101.PRE','EX-101.DEF']
#         self.ctl = Cntlr.Cntlr()
#
#     def _get_ins_from_txt(self, f_txt, dir_xbrl=None, force_save=False):
#         df_doc = self._parse_txt(f_txt)
#         dir_xbrl = os.path.join(df_doc.txt_dir[0], df_doc.txt_name.split('.')[0])
#         p_ins = None
#         if not os.path.isdir(dir_xbrl):
#             os.makedirs(dir_xbrl)
#         for ix, r in df_doc.iterrows():
#             if r.TYPE in self.xbrl_type:
#                 f_name = r.FILENAME
#                 p_file = os.path.join(dir_xbrl, f_name)
#                 if not os.path.exists(p_file) or force_save:
#                     f_text = r.TEXT
#                     f_xml = re_tag('XBRL', f_text)
#                     logger.info('saving {f}'.format(f=p_file))
#                     with open(p_file, 'w+') as f:
#                         f.write(f_xml)
#                 if r.TYPE == 'EX-101.INS':
#                     p_ins = p_file
#         return p_ins
#
#     def _read_ins(self, p_ins):
#         mm = self.ctl.modelManager.load(p_ins)
#         return mm