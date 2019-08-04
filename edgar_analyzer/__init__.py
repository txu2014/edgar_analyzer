# -*- coding: utf-8 -*-
"""Download, parse SEC EDGAR filings."""

from edgar_analyzer import metadata
from .edgar_analyzer import EdgarParser
from .edgar_analyzer import EdgarDownloader

__version__ = metadata.version
__author__ = metadata.authors[0]
__license__ = metadata.license
__copyright__ = metadata.copyright
