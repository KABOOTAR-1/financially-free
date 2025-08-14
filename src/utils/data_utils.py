"""
Data utilities for VAHAN web scraper.
Provides data validation, sample data generation, and helper functions.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import random

from ..core.config import Config
from ..core.exceptions import ValidationError
from ..utils.logging_utils import get_logger

def create_sample_data() -> pd.DataFrame:
    """Create sample VAHAN data for testing purposes.
    
    Returns:
        pd.DataFrame: Sample VAHAN data
    """
    np.random.seed(42)
    random.seed(42)
    
    states = ['Karnataka', 'Maharashtra', 'Delhi', 'Tamil Nadu', 'Gujarat', 'Uttar Pradesh', 'West Bengal']
    years = [2021, 2022, 2023, 2024]
    vehicle_classes = ['MOTOR CYCLE', 'SCOOTER', 'CAR', 'AUTO RICKSHAW', 'TRUCK', 'BUS', 'TEMPO', 'TRACTOR']
    
    sample_data = []
    
    for state in states:
        for year in years:
            for vehicle_class in vehicle_classes:
                # Generate realistic registration numbers with growth trends
                base_registrations = np.random.randint(1000, 50000)
                
                # Add year-over-year growth
                growth_factor = 1.0
                if year == 2022:
                    growth_factor = np.random.uniform(1.05, 1.15)  # 5-15% growth
                elif year == 2023:
                    growth_factor = np.random.uniform(1.08, 1.20)  # 8-20% growth
                elif year == 2024:
                    growth_factor = np.random.uniform(1.10, 1.25)  # 10-25% growth
                
                registrations = int(base_registrations * growth_factor)
                
                # Generate category-specific numbers
                two_w_ic = np.random.randint(100, 1000) if 'MOTOR' in vehicle_class or 'SCOOTER' in vehicle_class else 0
                two_w_n = np.random.randint(500, 5000) if 'MOTOR' in vehicle_class or 'SCOOTER' in vehicle_class else 0
                two_w_t = np.random.randint(200, 2000) if 'MOTOR' in vehicle_class or 'SCOOTER' in vehicle_class else 0
                three_w_n = np.random.randint(100, 1500) if 'AUTO' in vehicle_class or 'TEMPO' in vehicle_class else 0
                three_w_t = np.random.randint(50, 800) if 'AUTO' in vehicle_class or 'TEMPO' in vehicle_class else 0
                lmv = np.random.randint(200, 3000) if vehicle_class in ['CAR', 'TEMPO'] else 0
                mmv = np.random.randint(50, 1000) if vehicle_class in ['TRUCK', 'BUS'] else 0
                hmv = np.random.randint(20, 500) if vehicle_class in ['TRUCK', 'BUS', 'TRACTOR'] else 0
                
                sample_data.append({
                    'S No': len(sample_data) + 1,
                    'Vehicle Class': vehicle_class,
                    '2WIC': two_w_ic,
                    '2WN': two_w_n,
                    '2WT': two_w_t,
                    '3WN': three_w_n,
                    '3WT': three_w_t,
                    'LMV': lmv,
                    'MMV': mmv,
                    'HMV': hmv,
                    'TOTAL': registrations,
                    'Filter_State': f"{state}({np.random.randint(10, 99)})",
                    'Filter_Year': str(year),
                    'Filter_Vehicle_Type': 'ALL',
                    'Scraped_Date': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
    
    return pd.DataFrame(sample_data)

def validate_data_format(data: pd.DataFrame) -> Tuple[bool, List[str]]:
    """Validate VAHAN data format and structure.
    
    Args:
        data: DataFrame to validate
        
    Returns:
        Tuple[bool, List[str]]: (is_valid, list_of_issues)
    """
    logger = get_logger(__name__)
    issues = []
    
    # Check if DataFrame is empty
    if data.empty:
        issues.append("DataFrame is empty")
        return False, issues
    
    # Check for required columns
    required_columns = ['TOTAL']
    missing_required = [col for col in required_columns if col not in data.columns]
    if missing_required:
        issues.append(f"Missing required columns: {missing_required}")
    
    # Check for numeric columns
    numeric_columns_present = [col for col in Config.NUMERIC_COLUMNS if col in data.columns]
    if not numeric_columns_present:
        issues.append("No numeric columns found from expected list")
    
    # Validate numeric data
    for col in numeric_columns_present:
        try:
            # Try to convert to numeric
            pd.to_numeric(data[col], errors='coerce')
        except Exception as e:
            issues.append(f"Column {col} contains non-numeric data: {e}")
    
    # Check for temporal columns
    temporal_columns = [col for col in data.columns if any(keyword in col.lower() 
                       for keyword in ['year', 'date', 'time'])]
    if not temporal_columns:
        issues.append("No temporal columns found (year, date, time)")
    
    # Check for state/location columns
    location_columns = [col for col in data.columns if any(keyword in col.lower() 
                       for keyword in ['state', 'location', 'region'])]
    if not location_columns:
        issues.append("No location columns found (state, region)")
    
    # Check data quality
    total_rows = len(data)
    
    # Check for excessive missing values
    for col in data.columns:
        missing_pct = (data[col].isnull().sum() / total_rows) * 100
        if missing_pct > 50:
            issues.append(f"Column {col} has {missing_pct:.1f}% missing values")
    
    # Check for duplicate rows
    duplicate_count = data.duplicated().sum()
    if duplicate_count > 0:
        issues.append(f"Found {duplicate_count} duplicate rows")
    
    # Log validation results
    if issues:
        logger.warning(f"⚠️ Data validation found {len(issues)} issues")
        for issue in issues:
            logger.warning(f"  - {issue}")
    else:
        logger.info("✅ Data validation passed")
    
    return len(issues) == 0, issues

def normalize_column_names(data: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names to standard format.
    
    Args:
        data: DataFrame with potentially inconsistent column names
        
    Returns:
        pd.DataFrame: DataFrame with normalized column names
    """
    data_copy = data.copy()
    
    # Column name mappings
    column_mappings = {
        # Common variations for total
        'total': 'TOTAL',
        'Total': 'TOTAL',
        'grand_total': 'TOTAL',
        'Grand Total': 'TOTAL',
        
        # State variations
        'state': 'State',
        'State Name': 'State',
        'state_name': 'State',
        'filter_state': 'Filter_State',
        
        # Year variations
        'year': 'Year',
        'Year': 'Year',
        'filter_year': 'Filter_Year',
        
        # Vehicle class variations
        'vehicle_class': 'Vehicle Class',
        'vehicleclass': 'Vehicle Class',
        'vehicle type': 'Vehicle Class',
        'vehicle_type': 'Vehicle Class',
    }
    
    # Apply mappings
    for old_name, new_name in column_mappings.items():
        if old_name in data_copy.columns:
            data_copy = data_copy.rename(columns={old_name: new_name})
    
    return data_copy

def detect_data_source(data: pd.DataFrame) -> str:
    """Detect the likely source/type of VAHAN data.
    
    Args:
        data: DataFrame to analyze
        
    Returns:
        str: Detected data source type
    """
    # Check for scraped data indicators
    if 'Scraped_Date' in data.columns:
        return "Live Scraped Data"
    
    # Check for filter columns (indicates dashboard export)
    filter_columns = [col for col in data.columns if col.startswith('Filter_')]
    if filter_columns:
        return "Dashboard Export"
    
    # Check for processed data indicators
    if 'Vehicle_Category' in data.columns:
        return "Processed Data"
    
    # Check column structure to infer source
    if all(col in data.columns for col in ['Vehicle Class', 'TOTAL']):
        return "Raw VAHAN Data"
    
    return "Unknown Source"

def calculate_data_quality_score(data: pd.DataFrame) -> Dict:
    """Calculate a data quality score and metrics.
    
    Args:
        data: DataFrame to analyze
        
    Returns:
        Dict: Data quality metrics and score
    """
    metrics = {
        'total_rows': len(data),
        'total_columns': len(data.columns),
        'completeness_score': 0,
        'consistency_score': 0,
        'validity_score': 0,
        'overall_score': 0,
        'issues': []
    }
    
    if data.empty:
        metrics['overall_score'] = 0
        metrics['issues'].append("Dataset is empty")
        return metrics
    
    # Completeness: percentage of non-null values
    total_cells = data.size
    non_null_cells = data.count().sum()
    completeness = (non_null_cells / total_cells) * 100
    metrics['completeness_score'] = round(completeness, 2)
    
    # Consistency: check for consistent data types and formats
    consistency_issues = 0
    
    # Check numeric columns
    for col in Config.NUMERIC_COLUMNS:
        if col in data.columns:
            try:
                pd.to_numeric(data[col], errors='raise')
            except:
                consistency_issues += 1
                metrics['issues'].append(f"Inconsistent numeric data in {col}")
    
    consistency_score = max(0, 100 - (consistency_issues * 10))
    metrics['consistency_score'] = consistency_score
    
    # Validity: check for reasonable value ranges
    validity_issues = 0
    
    if 'TOTAL' in data.columns:
        # Check for negative values
        negative_totals = (data['TOTAL'] < 0).sum()
        if negative_totals > 0:
            validity_issues += 1
            metrics['issues'].append(f"Found {negative_totals} negative TOTAL values")
        
        # Check for extremely large values (potential data entry errors)
        max_total = data['TOTAL'].max()
        if max_total > 1000000:  # 1 million threshold
            validity_issues += 1
            metrics['issues'].append(f"Extremely large TOTAL value found: {max_total}")
    
    validity_score = max(0, 100 - (validity_issues * 15))
    metrics['validity_score'] = validity_score
    
    # Overall score (weighted average)
    overall_score = (
        completeness * 0.4 +
        consistency_score * 0.3 +
        validity_score * 0.3
    )
    metrics['overall_score'] = round(overall_score, 2)
    
    return metrics

def generate_data_summary(data: pd.DataFrame) -> Dict:
    """Generate a comprehensive summary of the dataset.
    
    Args:
        data: DataFrame to summarize
        
    Returns:
        Dict: Dataset summary
    """
    summary = {
        'basic_info': {},
        'columns': {},
        'data_quality': {},
        'content_summary': {}
    }
    
    # Basic information
    summary['basic_info'] = {
        'rows': len(data),
        'columns': len(data.columns),
        'memory_usage_mb': round(data.memory_usage(deep=True).sum() / (1024 * 1024), 2),
        'data_source': detect_data_source(data)
    }
    
    # Column information
    summary['columns'] = {
        'numeric_columns': data.select_dtypes(include=[np.number]).columns.tolist(),
        'text_columns': data.select_dtypes(include=['object']).columns.tolist(),
        'datetime_columns': data.select_dtypes(include=['datetime']).columns.tolist()
    }
    
    # Data quality
    summary['data_quality'] = calculate_data_quality_score(data)
    
    # Content summary
    if 'TOTAL' in data.columns:
        summary['content_summary']['total_registrations'] = int(data['TOTAL'].sum())
        summary['content_summary']['avg_registrations'] = round(data['TOTAL'].mean(), 2)
    
    if 'State' in data.columns:
        summary['content_summary']['unique_states'] = data['State'].nunique()
        summary['content_summary']['top_states'] = data['State'].value_counts().head(5).to_dict()
    
    if 'Year' in data.columns:
        summary['content_summary']['year_range'] = f"{data['Year'].min()}-{data['Year'].max()}"
    
    if 'Vehicle Class' in data.columns:
        summary['content_summary']['unique_vehicle_classes'] = data['Vehicle Class'].nunique()
        summary['content_summary']['top_vehicle_classes'] = data['Vehicle Class'].value_counts().head(5).to_dict()
    
    return summary
