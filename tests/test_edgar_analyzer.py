from unittest import TestCase
import edgar_analyzer as ea

class TestEdgarAnalyzer(TestCase):
    def test_init(self):
        ep = ea.EdgarParser()
        ed = ea.EdgarDownloader()
        self.assertEqual(0,0)

class TestEdgarParser(TestCase):
    def test_fact_mapping(self):
        ep = ea.EdgarParser()
        df = ep.fact_mapping
        self.assertGreater(df.shape[0],0)

class TestEdgarDownloader(TestCase):
    def test_dir_master_exists(self):
        ed = ea.EdgarDownloader()
        dd = ed.dir_download
        self.assertIsNotNone(dd)

    def test_filings_between_ibm_2019_10q(self):
        ed = ea.EdgarDownloader()
        list_f = ed.filings_between('IBM', '2019-01-01', '2019-06-01', '10-Q')
        self.assertGreater(len(list_f), 0)

    def test_filings_between_ibm_2019_10k(self):
        ed = ea.EdgarDownloader()
        list_f = ed.filings_between('IBM', '2019-01-01', '2019-06-01', '10-K')
        self.assertGreater(len(list_f), 0)