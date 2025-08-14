"""
Abstract base scraper for web scraping operations.
Provides common functionality and interface for all scrapers.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional
import pandas as pd
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait

from ..core.config import Config
from ..core.exceptions import ScrapingError, ConfigurationError
from ..utils.logging_utils import get_logger

class BaseScraper(ABC):
    """Abstract base class for web scrapers."""
    
    def __init__(self, base_url: str, wait_time: int = None):
        """Initialize the base scraper.
        
        Args:
            base_url: The base URL to scrape
            wait_time: Maximum wait time for elements (uses config default if None)
        """
        self.base_url = base_url
        self.wait_time = wait_time or Config.DEFAULT_WAIT_TIME
        self.driver: Optional[webdriver.Chrome] = None
        self.wait: Optional[WebDriverWait] = None
        self.logger = get_logger(self.__class__.__name__)
        
    def setup_driver(self, headless: bool = None) -> None:
        """Initialize the Chrome WebDriver with options.
        
        Args:
            headless: Whether to run in headless mode (uses config default if None)
        """
        if headless is None:
            headless = Config.HEADLESS_MODE
            
        options = webdriver.ChromeOptions()
        if headless:
            options.add_argument("--headless=new")
            options.add_argument("--disable-gpu")
        
        # Enhanced options for better compatibility
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument(f"--window-size={Config.WINDOW_SIZE}")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        try:
            self.driver = webdriver.Chrome(options=options)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.wait = WebDriverWait(self.driver, self.wait_time)
            self.logger.info("✅ WebDriver initialized successfully")
        except Exception as e:
            raise ConfigurationError(f"Failed to initialize WebDriver: {e}")
    
    def open_page(self) -> None:
        """Open the target page."""
        if not self.driver:
            raise ScrapingError("WebDriver not initialized. Call setup_driver() first.")
        
        try:
            self.driver.get(self.base_url)
            self.logger.info(f"📖 Opened page: {self.base_url}")
        except Exception as e:
            raise ScrapingError(f"Failed to open page: {e}")
    
    def close(self) -> None:
        """Close the browser and cleanup resources."""
        if self.driver:
            try:
                self.driver.quit()
                self.logger.info("🔄 Browser closed successfully")
            except Exception as e:
                self.logger.warning(f"⚠️ Error closing browser: {e}")
            finally:
                self.driver = None
                self.wait = None
    
    @abstractmethod
    def fetch_data(self) -> pd.DataFrame:
        """Fetch data from the website.
        
        Returns:
            pd.DataFrame: Scraped data
        """
        pass
    
    @abstractmethod
    def apply_filters(self, filters: Dict[str, str]) -> None:
        """Apply filters to the website.
        
        Args:
            filters: Dictionary of filter name to value mappings
        """
        pass
    
    @abstractmethod
    def scrape_dropdowns(self) -> Dict[str, List[str]]:
        """Scrape available dropdown options.
        
        Returns:
            Dict mapping dropdown names to their available options
        """
        pass
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
