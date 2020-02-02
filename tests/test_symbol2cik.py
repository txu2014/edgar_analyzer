from unittest import TestCase
from edgar_analyzer import utils

class TestSymbol2cik(TestCase):
    def test_symbol2cik_file(self):
        self.assertEqual(utils.ticker2cik_file('AAPL'), '0000320193')

    def test_symbol2cik_sec(self):
        self.assertEqual(utils.ticker2cik_sec('AAPL'), '0000320193')

    def test_symbol2cik(self):
        list_s = ['BABA', 'IBM', 'UBER', 'AAPL']
        for s in list_s:
            self.assertEqual(utils.ticker2cik(s), utils.ticker2cik_sec(s, update_mapping=False), s)
