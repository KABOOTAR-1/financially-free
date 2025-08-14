"""
Custom exceptions for VAHAN web scraper.
Provides specific error handling for different failure scenarios.
"""

class VahanScraperError(Exception):
    """Base exception for VAHAN scraper errors."""
    pass

class ScrapingError(VahanScraperError):
    """Raised when web scraping operations fail."""
    pass

class DataProcessingError(VahanScraperError):
    """Raised when data processing operations fail."""
    pass

class ConfigurationError(VahanScraperError):
    """Raised when configuration is invalid or missing."""
    pass

class ValidationError(VahanScraperError):
    """Raised when data validation fails."""
    pass

class ExportError(VahanScraperError):
    """Raised when data export operations fail."""
    pass

class DropdownError(ScrapingError):
    """Raised when dropdown interaction fails."""
    pass

class DynamicIDError(ScrapingError):
    """Raised when dynamic ID detection fails."""
    pass
