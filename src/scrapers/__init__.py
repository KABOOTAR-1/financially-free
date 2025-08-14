"""
Scrapers module for VAHAN web scraper.
Contains base scraper and VAHAN-specific implementation.
"""

from .base_scraper import BaseScraper
from .vahan_scraper import VahanScraper

__all__ = ['BaseScraper', 'VahanScraper']
