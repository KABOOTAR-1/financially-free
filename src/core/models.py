"""
Data models and type definitions for VAHAN web scraper.
Provides structured data types for better type safety and validation.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Union
from datetime import datetime
import pandas as pd

@dataclass
class ScrapingConfig:
    """Configuration for scraping operations."""
    states: List[str]
    years: List[str]
    vehicle_types: Optional[List[str]] = None
    headless: bool = True
    wait_time: int = 15
    max_retries: int = 3

@dataclass
class FilterCombination:
    """Represents a filter combination for scraping."""
    state: str
    year: str
    vehicle_type: Optional[str] = None
    y_axis: Optional[str] = None
    x_axis: Optional[str] = None

@dataclass
class ScrapingResult:
    """Result of a scraping operation."""
    data: pd.DataFrame
    metadata: Dict
    timestamp: datetime
    success: bool
    error_message: Optional[str] = None

@dataclass
class GrowthMetrics:
    """Growth metrics for analysis."""
    yoy_growth: Dict[str, float]
    qoq_growth: Dict[str, float]
    category_growth: Dict[str, Dict[str, float]]
    state_growth: Dict[str, Dict[str, float]]
    manufacturer_growth: Dict[str, Dict[str, float]]

@dataclass
class MarketInsights:
    """Market insights and analysis results."""
    market_overview: Dict
    growth_leaders: List[str]
    risk_factors: List[str]
    investment_opportunities: List[str]
    top_manufacturers: List[Dict]
    market_share: Dict[str, float]

@dataclass
class ProcessingResult:
    """Result of data processing operations."""
    cleaned_data: pd.DataFrame
    growth_metrics: GrowthMetrics
    insights: MarketInsights
    processing_time: float
    records_processed: int
