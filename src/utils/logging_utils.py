"""
Logging utilities for VAHAN web scraper.
Provides centralized logging configuration and management.
"""

import logging
import sys
from pathlib import Path
from typing import Optional

from ..core.config import Config

def setup_logging(log_level: str = None, log_file: str = None) -> None:
    """Setup centralized logging configuration.
    
    Args:
        log_level: Logging level (uses config default if None)
        log_file: Log file path (uses default if None)
    """
    # Ensure logs directory exists
    Config.ensure_directories()
    
    # Set log level
    level = getattr(logging, (log_level or Config.LOG_LEVEL).upper(), logging.INFO)
    
    # Set log file
    if log_file is None:
        log_file = Config.LOGS_DIR / f"vahan_scraper_{Config.get_output_filename('', 'log').split('_')[-1]}"
    
    # Configure logging
    logging.basicConfig(
        level=level,
        format=Config.LOG_FORMAT,
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Set third-party loggers to WARNING to reduce noise
    logging.getLogger('selenium').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)

def get_logger(name: str) -> logging.Logger:
    """Get a logger instance with the specified name.
    
    Args:
        name: Logger name (usually __name__ or class name)
        
    Returns:
        logging.Logger: Configured logger instance
    """
    return logging.getLogger(name)
