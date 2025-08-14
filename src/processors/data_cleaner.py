"""
Data cleaning utilities for VAHAN vehicle registration data.
Handles data validation, cleaning, and standardization.
"""

import pandas as pd
import numpy as np
import re
from typing import Dict, List, Optional, Union
from datetime import datetime

from ..core.config import Config
from ..core.exceptions import DataProcessingError, ValidationError
from ..utils.logging_utils import get_logger

class DataCleaner:
    """Utility class for cleaning and standardizing VAHAN data."""
    
    def __init__(self):
        """Initialize the data cleaner."""
        self.logger = get_logger(self.__class__.__name__)
    
    def validate_data(self, data: pd.DataFrame) -> bool:
        """Validate input data structure and content.
        
        Args:
            data: DataFrame to validate
            
        Returns:
            bool: True if data is valid
            
        Raises:
            ValidationError: If data validation fails
        """
        if data is None or data.empty:
            raise ValidationError("Data is None or empty")
        
        # Check for required columns (at least some numeric data should be present)
        numeric_cols_present = any(col in data.columns for col in Config.NUMERIC_COLUMNS)
        if not numeric_cols_present:
            self.logger.warning("⚠️ No standard numeric columns found in data")
        
        self.logger.info(f"✅ Data validation passed. Shape: {data.shape}")
        return True
    
    def clean_numeric_columns(self, data: pd.DataFrame) -> pd.DataFrame:
        """Clean numeric columns by removing commas and converting to numeric.
        
        Args:
            data: DataFrame to clean
            
        Returns:
            pd.DataFrame: DataFrame with cleaned numeric columns
        """
        data_copy = data.copy()
        
        for col in Config.NUMERIC_COLUMNS:
            if col in data_copy.columns:
                try:
                    # Remove commas and convert to string first
                    data_copy[col] = data_copy[col].astype(str).str.replace(',', '')
                    
                    # Replace empty strings and 'nan' with NaN
                    data_copy[col] = data_copy[col].replace(['', 'nan', 'None', '-'], np.nan)
                    
                    # Convert to numeric, coercing errors to NaN
                    data_copy[col] = pd.to_numeric(data_copy[col], errors='coerce')
                    
                    # Fill NaN with 0 for numeric columns
                    data_copy[col] = data_copy[col].fillna(0)
                    
                    self.logger.debug(f"✅ Cleaned numeric column: {col}")
                    
                except Exception as e:
                    self.logger.warning(f"⚠️ Error cleaning column {col}: {e}")
        
        return data_copy
    
    def clean_text_columns(self, data: pd.DataFrame) -> pd.DataFrame:
        """Clean text columns by standardizing format.
        
        Args:
            data: DataFrame to clean
            
        Returns:
            pd.DataFrame: DataFrame with cleaned text columns
        """
        data_copy = data.copy()
        
        text_columns = data_copy.select_dtypes(include=['object']).columns
        
        for col in text_columns:
            if col not in Config.NUMERIC_COLUMNS:  # Skip numeric columns
                try:
                    # Convert to string and strip whitespace
                    data_copy[col] = data_copy[col].astype(str).str.strip()
                    
                    # Replace 'nan' string with actual NaN
                    data_copy[col] = data_copy[col].replace(['nan', 'None', ''], np.nan)
                    
                    # Standardize case for certain columns
                    if 'state' in col.lower() or 'vehicle' in col.lower():
                        data_copy[col] = data_copy[col].str.title()
                    
                    self.logger.debug(f"✅ Cleaned text column: {col}")
                    
                except Exception as e:
                    self.logger.warning(f"⚠️ Error cleaning text column {col}: {e}")
        
        return data_copy
    
    def extract_temporal_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Extract and clean temporal information.
        
        Args:
            data: DataFrame to process
            
        Returns:
            pd.DataFrame: DataFrame with extracted temporal data
        """
        data_copy = data.copy()
        
        # Extract year information
        year_columns = [col for col in data_copy.columns if 'year' in col.lower()]
        for col in year_columns:
            try:
                # Extract year from various formats
                data_copy[col] = data_copy[col].astype(str)
                
                # Extract 4-digit year using regex
                year_pattern = r'(\d{4})'
                data_copy['Year'] = data_copy[col].str.extract(year_pattern)[0]
                data_copy['Year'] = pd.to_numeric(data_copy['Year'], errors='coerce')
                
                self.logger.debug(f"✅ Extracted year from column: {col}")
                break
                
            except Exception as e:
                self.logger.warning(f"⚠️ Error extracting year from {col}: {e}")
        
        # Extract state information
        state_columns = [col for col in data_copy.columns if 'state' in col.lower()]
        for col in state_columns:
            try:
                # Clean state names - remove parentheses and numbers
                data_copy['State'] = data_copy[col].astype(str)
                data_copy['State'] = data_copy['State'].str.replace(r'\([^)]*\)', '', regex=True)
                data_copy['State'] = data_copy['State'].str.strip()
                
                self.logger.debug(f"✅ Extracted state from column: {col}")
                break
                
            except Exception as e:
                self.logger.warning(f"⚠️ Error extracting state from {col}: {e}")
        
        return data_copy
    
    def categorize_vehicle(self, row: pd.Series) -> str:
        """Categorize vehicles into 2W, 3W, 4W+ based on available data.
        
        Args:
            row: DataFrame row containing vehicle information
            
        Returns:
            str: Vehicle category ('2W', '3W', '4W+', 'Other', 'Unknown')
        """
        try:
            # Check vehicle class column if available
            vehicle_class_cols = [col for col in row.index if 'vehicle' in col.lower() and 'class' in col.lower()]
            
            if vehicle_class_cols:
                vehicle_class = str(row[vehicle_class_cols[0]]).upper()
                
                # Check against predefined categories
                for category, keywords in Config.VEHICLE_CATEGORIES.items():
                    if any(keyword.upper() in vehicle_class for keyword in keywords):
                        return category
            
            # Fallback: check numeric columns for highest values
            numeric_values = {}
            for col in Config.NUMERIC_COLUMNS:
                if col in row.index:
                    try:
                        value = float(row[col]) if pd.notna(row[col]) else 0
                        numeric_values[col] = value
                    except (ValueError, TypeError):
                        numeric_values[col] = 0
            
            if numeric_values:
                # Determine category based on highest registration numbers
                two_wheeler_cols = ['2WIC', '2WN', '2WT']
                three_wheeler_cols = ['3WN', '3WT']
                four_wheeler_cols = ['LMV', 'MMV', 'HMV']
                
                two_w_total = sum(numeric_values.get(col, 0) for col in two_wheeler_cols)
                three_w_total = sum(numeric_values.get(col, 0) for col in three_wheeler_cols)
                four_w_total = sum(numeric_values.get(col, 0) for col in four_wheeler_cols)
                
                max_category = max([
                    (two_w_total, '2W'),
                    (three_w_total, '3W'),
                    (four_w_total, '4W+')
                ], key=lambda x: x[0])
                
                return max_category[1] if max_category[0] > 0 else 'Unknown'
            
            return 'Unknown'
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error categorizing vehicle: {e}")
            return 'Unknown'
    
    def remove_duplicates(self, data: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate rows from the dataset.
        
        Args:
            data: DataFrame to deduplicate
            
        Returns:
            pd.DataFrame: DataFrame with duplicates removed
        """
        initial_count = len(data)
        
        # Remove exact duplicates
        data_clean = data.drop_duplicates()
        
        # Remove duplicates based on key columns if they exist
        key_columns = []
        for col in ['State', 'Year', 'Vehicle Class']:
            if col in data_clean.columns:
                key_columns.append(col)
        
        if key_columns:
            data_clean = data_clean.drop_duplicates(subset=key_columns, keep='first')
        
        removed_count = initial_count - len(data_clean)
        if removed_count > 0:
            self.logger.info(f"🧹 Removed {removed_count} duplicate rows")
        
        return data_clean
    
    def handle_missing_values(self, data: pd.DataFrame) -> pd.DataFrame:
        """Handle missing values in the dataset.
        
        Args:
            data: DataFrame to process
            
        Returns:
            pd.DataFrame: DataFrame with missing values handled
        """
        data_copy = data.copy()
        
        # Fill numeric columns with 0
        numeric_cols = data_copy.select_dtypes(include=[np.number]).columns
        data_copy[numeric_cols] = data_copy[numeric_cols].fillna(0)
        
        # Fill text columns with 'Unknown' or appropriate defaults
        text_cols = data_copy.select_dtypes(include=['object']).columns
        for col in text_cols:
            if 'state' in col.lower():
                data_copy[col] = data_copy[col].fillna('Unknown State')
            elif 'vehicle' in col.lower():
                data_copy[col] = data_copy[col].fillna('Unknown Vehicle')
            else:
                data_copy[col] = data_copy[col].fillna('Unknown')
        
        self.logger.info("✅ Missing values handled")
        return data_copy
    
    def clean_all(self, data: pd.DataFrame) -> pd.DataFrame:
        """Perform comprehensive data cleaning.
        
        Args:
            data: Raw DataFrame to clean
            
        Returns:
            pd.DataFrame: Fully cleaned DataFrame
            
        Raises:
            DataProcessingError: If cleaning fails
        """
        try:
            self.logger.info("🧹 Starting comprehensive data cleaning...")
            
            # Validate input data
            self.validate_data(data)
            
            # Remove completely empty rows
            cleaned_data = data.dropna(how='all')
            
            # Clean numeric columns
            cleaned_data = self.clean_numeric_columns(cleaned_data)
            
            # Clean text columns
            cleaned_data = self.clean_text_columns(cleaned_data)
            
            # Extract temporal data
            cleaned_data = self.extract_temporal_data(cleaned_data)
            
            # Use actual vehicle class names as categories instead of generic 2W/3W/4W+
            if 'Vehicle Class' in cleaned_data.columns:
                # Use the actual vehicle class names from VAHAN as categories
                cleaned_data['Vehicle_Category'] = cleaned_data['Vehicle Class']
                self.logger.info("✅ Using actual vehicle class names as categories")
            else:
                # Fallback: create generic categories only if no Vehicle Class column
                cleaned_data['Vehicle_Category'] = cleaned_data.apply(self.categorize_vehicle, axis=1)
                self.logger.info("⚠️ Using generic vehicle categories as fallback")
            
            # Remove duplicates
            cleaned_data = self.remove_duplicates(cleaned_data)
            
            # Handle missing values
            cleaned_data = self.handle_missing_values(cleaned_data)
            
            self.logger.info(f"✅ Data cleaning completed. Final shape: {cleaned_data.shape}")
            return cleaned_data
            
        except Exception as e:
            raise DataProcessingError(f"Data cleaning failed: {e}")
