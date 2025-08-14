"""
Configuration settings for VAHAN web scraper.
Centralized configuration management for all modules.
"""

import os
from pathlib import Path
from typing import Dict, List

class Config:
    """Centralized configuration for VAHAN web scraper."""
    
    # Base directories
    BASE_DIR = Path(__file__).parent.parent.parent
    DATA_DIR = BASE_DIR / "data"
    OUTPUT_DIR = BASE_DIR / "output"
    LOGS_DIR = BASE_DIR / "logs"
    
    # VAHAN website settings
    VAHAN_BASE_URL = "https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml"
    DEFAULT_WAIT_TIME = 15
    MAX_RETRIES = 3
    
    # Scraping settings
    HEADLESS_MODE = True
    WINDOW_SIZE = "1920,1080"
    
    # Data processing settings
    NUMERIC_COLUMNS = ['2WIC', '2WN', '2WT', '3WN', '3WT', 'LMV', 'MMV', 'HMV', 'TOTAL']
    
    # Vehicle categorization
    VEHICLE_CATEGORIES = {
        '2W': ['MOTOR CYCLE', 'SCOOTER', 'M-CYCLE', 'MOPED'],
        '3W': ['AUTO RICKSHAW', '3W', 'THREE WHEELER'],
        '4W+': ['CAR', 'TRUCK', 'BUS', 'LMV', 'MMV', 'HMV']
    }
    
    # Static dropdown IDs (these don't change)
    STATIC_DROPDOWNS = {
        "Y-Axis": "yaxisVar",
        "X-Axis": "xaxisVar", 
        "Year": "selectedYear",
        "Year Type": "selectedYearType",
        "Vehicle Type": "vchgroupTable:selectCatgGrp"
    }
    
    # Logging configuration
    LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    LOG_LEVEL = 'INFO'
    
    # Export settings
    DEFAULT_EXPORT_FORMAT = 'csv'
    TIMESTAMP_FORMAT = '%Y%m%d_%H%M%S'
    
    @classmethod
    def ensure_directories(cls):
        """Ensure all required directories exist."""
        for directory in [cls.DATA_DIR, cls.OUTPUT_DIR, cls.LOGS_DIR]:
            directory.mkdir(parents=True, exist_ok=True)
    
    @classmethod
    def get_output_filename(cls, prefix: str, extension: str = 'csv') -> str:
        """Generate timestamped output filename."""
        from datetime import datetime
        timestamp = datetime.now().strftime(cls.TIMESTAMP_FORMAT)
        return f"{prefix}_{timestamp}.{extension}"
