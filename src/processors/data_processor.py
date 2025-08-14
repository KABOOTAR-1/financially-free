"""
Main data processing module for VAHAN vehicle registration data.
Handles data processing, growth metrics calculation, and insights generation.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Union
from pathlib import Path
import time

from .data_cleaner import DataCleaner
from ..core.config import Config
from ..core.exceptions import DataProcessingError
from ..core.models import GrowthMetrics, MarketInsights, ProcessingResult
from ..utils.logging_utils import get_logger
from ..utils.data_utils import create_sample_data

class VahanDataProcessor:
    """Process and analyze VAHAN vehicle registration data with investor-focused metrics."""
    
    def __init__(self):
        """Initialize the data processor."""
        self.data: Optional[pd.DataFrame] = None
        self.processed_data: Optional[pd.DataFrame] = None
        self.growth_metrics: Dict = {}
        self.logger = get_logger(self.__class__.__name__)
        self.cleaner = DataCleaner()
        
    def load_data(self, file_path: Union[str, Path]) -> pd.DataFrame:
        """Load data from CSV file with validation.
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            pd.DataFrame: Loaded data
            
        Raises:
            FileNotFoundError: If file doesn't exist
            DataProcessingError: If file is empty or invalid
        """
        
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
            
        try:
            self.data = pd.read_csv(file_path)
            
            if self.data.empty:
                raise DataProcessingError("Loaded file is empty")
                
            self.logger.info(f"✅ Loaded {len(self.data)} rows from {file_path}")
            return self.data
            
        except Exception as e:
            self.logger.error(f"❌ Error loading data: {e}")
            raise DataProcessingError(f"Failed to load data: {e}")
    
    def clean_data(self, data: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """Clean and standardize the VAHAN data.
        
        Args:
            data: DataFrame to clean (uses self.data if None)
            
        Returns:
            pd.DataFrame: Cleaned data
            
        Raises:
            DataProcessingError: If no data available to clean
        """
        if data is None:
            if self.data is None:
                raise DataProcessingError("No data available to clean. Load data first.")
            data = self.data.copy()
        else:
            data = data.copy()
        
        if data.empty:
            self.logger.warning("⚠️ No data to clean")
            return data
        
        try:
            # Use the data cleaner for comprehensive cleaning
            cleaned_data = self.cleaner.clean_all(data)
            self.processed_data = cleaned_data
            return cleaned_data
            
        except Exception as e:
            raise DataProcessingError(f"Data cleaning failed: {e}")
    
    def calculate_growth_metrics(self, data: pd.DataFrame = None) -> Dict:
        """Calculate YoY and QoQ growth rates for vehicle categories and manufacturers.
        
        Args:
            data: DataFrame to analyze (uses processed_data if None)
            
        Returns:
            Dict: Growth metrics including YoY and QoQ rates
        """
        if data is None:
            data = self.processed_data
        
        if data is None or data.empty:
            self.logger.warning("⚠️ No data available for growth calculation")
            return {}
        
        try:
            self.logger.info("📈 Calculating growth metrics...")
            
            growth_metrics = {
                'yoy_growth': {},
                'qoq_growth': {},
                'category_growth': {},
                'state_growth': {},
                'manufacturer_growth': {}
            }
            
            # Calculate YoY growth for total registrations
            if 'TOTAL' in data.columns and 'Year' in data.columns:
                growth_metrics['yoy_growth'] = self._calculate_yoy_growth(data, 'TOTAL')
            
            # Calculate YoY growth by category
            growth_metrics['category_growth'] = self._calculate_yoy_by_category(data)
            
            # Calculate YoY growth by state
            growth_metrics['state_growth'] = self._calculate_yoy_by_state(data)
            
            # Calculate manufacturer trends
            growth_metrics['manufacturer_growth'] = self._analyze_manufacturer_trends(data)
            
            self.growth_metrics = growth_metrics
            self.logger.info("✅ Growth metrics calculation completed")
            
            return growth_metrics
            
        except Exception as e:
            self.logger.error(f"❌ Error calculating growth metrics: {e}")
            return {}
    
    def _calculate_yoy_growth(self, data: pd.DataFrame, column: str) -> Dict:
        """Calculate year-over-year growth for a specific column."""
        try:
            if 'Year' not in data.columns or column not in data.columns:
                return {}
            
            # Group by year and sum the column
            yearly_data = data.groupby('Year')[column].sum().reset_index()
            yearly_data = yearly_data.sort_values('Year')
            
            growth_rates = {}
            for i in range(1, len(yearly_data)):
                current_year = yearly_data.iloc[i]['Year']
                previous_year = yearly_data.iloc[i-1]['Year']
                current_value = yearly_data.iloc[i][column]
                previous_value = yearly_data.iloc[i-1][column]
                
                if previous_value > 0:
                    growth_rate = ((current_value - previous_value) / previous_value) * 100
                    growth_rates[f"{previous_year}-{current_year}"] = round(growth_rate, 2)
            
            return growth_rates
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error calculating YoY growth for {column}: {e}")
            return {}
    
    def _calculate_yoy_by_category(self, data: pd.DataFrame) -> Dict:
        """Calculate YoY growth by vehicle category."""
        try:
            if 'Vehicle_Category' not in data.columns or 'Year' not in data.columns:
                return {}
            
            category_growth = {}
            
            for category in data['Vehicle_Category'].unique():
                if pd.isna(category):
                    continue
                    
                category_data = data[data['Vehicle_Category'] == category]
                
                # Calculate growth for TOTAL column
                if 'TOTAL' in category_data.columns:
                    yearly_totals = category_data.groupby('Year')['TOTAL'].sum().reset_index()
                    yearly_totals = yearly_totals.sort_values('Year')
                    
                    growth_rates = {}
                    for i in range(1, len(yearly_totals)):
                        current_year = yearly_totals.iloc[i]['Year']
                        previous_year = yearly_totals.iloc[i-1]['Year']
                        current_value = yearly_totals.iloc[i]['TOTAL']
                        previous_value = yearly_totals.iloc[i-1]['TOTAL']
                        
                        if previous_value > 0:
                            growth_rate = ((current_value - previous_value) / previous_value) * 100
                            growth_rates[f"{previous_year}-{current_year}"] = round(growth_rate, 2)
                    
                    category_growth[category] = growth_rates
            
            return category_growth
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error calculating category growth: {e}")
            return {}
    
    def _calculate_yoy_by_state(self, data: pd.DataFrame) -> Dict:
        """Calculate YoY growth by state."""
        try:
            if 'State' not in data.columns or 'Year' not in data.columns:
                return {}
            
            state_growth = {}
            
            for state in data['State'].unique():
                if pd.isna(state):
                    continue
                    
                state_data = data[data['State'] == state]
                
                if 'TOTAL' in state_data.columns:
                    yearly_totals = state_data.groupby('Year')['TOTAL'].sum().reset_index()
                    yearly_totals = yearly_totals.sort_values('Year')
                    
                    growth_rates = {}
                    for i in range(1, len(yearly_totals)):
                        current_year = yearly_totals.iloc[i]['Year']
                        previous_year = yearly_totals.iloc[i-1]['Year']
                        current_value = yearly_totals.iloc[i]['TOTAL']
                        previous_value = yearly_totals.iloc[i-1]['TOTAL']
                        
                        if previous_value > 0:
                            growth_rate = ((current_value - previous_value) / previous_value) * 100
                            growth_rates[f"{previous_year}-{current_year}"] = round(growth_rate, 2)
                    
                    state_growth[state] = growth_rates
            
            return state_growth
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error calculating state growth: {e}")
            return {}
    
    def _analyze_manufacturer_trends(self, data: pd.DataFrame) -> Dict:
        """Analyze manufacturer/vehicle class trends."""
        try:
            manufacturer_data = {}
            
            if 'Vehicle Class' in data.columns:
                # Get top manufacturers by total registrations
                top_manufacturers = self._get_top_manufacturers(data)
                manufacturer_data['top_manufacturers'] = top_manufacturers
                
                # Calculate manufacturer growth rates
                manufacturer_growth = self._get_manufacturer_growth(data)
                manufacturer_data['growth_rates'] = manufacturer_growth
                
                # Calculate market share
                market_share = self._calculate_market_share(data)
                manufacturer_data['market_share'] = market_share
            
            return manufacturer_data
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error analyzing manufacturer trends: {e}")
            return {}
    
    def _get_top_manufacturers(self, data: pd.DataFrame, top_n: int = 10) -> List[Dict]:
        """Get top manufacturers by total registrations."""
        try:
            if 'Vehicle Class' not in data.columns or 'TOTAL' not in data.columns:
                return []
            
            manufacturer_totals = data.groupby('Vehicle Class')['TOTAL'].sum().reset_index()
            manufacturer_totals = manufacturer_totals.sort_values('TOTAL', ascending=False).head(top_n)
            
            return manufacturer_totals.to_dict('records')
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error getting top manufacturers: {e}")
            return []
    
    def _get_manufacturer_growth(self, data: pd.DataFrame) -> Dict:
        """Calculate manufacturer-wise growth rates."""
        try:
            if 'Vehicle Class' not in data.columns or 'Year' not in data.columns:
                return {}
            
            manufacturer_growth = {}
            
            for manufacturer in data['Vehicle Class'].unique():
                if pd.isna(manufacturer):
                    continue
                
                mfg_data = data[data['Vehicle Class'] == manufacturer]
                
                if 'TOTAL' in mfg_data.columns:
                    yearly_totals = mfg_data.groupby('Year')['TOTAL'].sum().reset_index()
                    yearly_totals = yearly_totals.sort_values('Year')
                    
                    if len(yearly_totals) >= 2:
                        latest_year = yearly_totals.iloc[-1]
                        previous_year = yearly_totals.iloc[-2]
                        
                        if previous_year['TOTAL'] > 0:
                            growth_rate = ((latest_year['TOTAL'] - previous_year['TOTAL']) / previous_year['TOTAL']) * 100
                            manufacturer_growth[manufacturer] = round(growth_rate, 2)
            
            return manufacturer_growth
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error calculating manufacturer growth: {e}")
            return {}
    
    def _calculate_market_share(self, data: pd.DataFrame) -> Dict[str, float]:
        """Calculate market share by manufacturer."""
        try:
            if 'Vehicle Class' not in data.columns or 'TOTAL' not in data.columns:
                return {}
            
            total_registrations = data['TOTAL'].sum()
            if total_registrations == 0:
                return {}
            
            manufacturer_totals = data.groupby('Vehicle Class')['TOTAL'].sum()
            market_share = (manufacturer_totals / total_registrations * 100).round(2)
            
            return market_share.to_dict()
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error calculating market share: {e}")
            return {}
    
    def get_investor_insights(self, data: pd.DataFrame = None) -> Dict:
        """Generate key investor insights from the processed data."""
        if data is None:
            data = self.processed_data
        
        if data is None or data.empty:
            return {}
        
        try:
            insights = {
                'market_overview': self._get_market_overview(data),
                'growth_leaders': self._get_growth_leaders(),
                'risk_factors': self._identify_risk_factors(),
                'investment_opportunities': self._identify_opportunities()
            }
            
            return insights
            
        except Exception as e:
            self.logger.error(f"❌ Error generating insights: {e}")
            return {}
    
    def _get_market_overview(self, data: pd.DataFrame) -> Dict:
        """Get overall market overview metrics."""
        try:
            overview = {}
            
            if 'TOTAL' in data.columns:
                overview['total_registrations'] = int(data['TOTAL'].sum())
            
            if 'Year' in data.columns:
                years = data['Year'].dropna().unique()
                if len(years) > 0:
                    overview['data_period'] = f"{int(min(years))}-{int(max(years))}"
            
            if 'State' in data.columns:
                overview['states_covered'] = int(data['State'].nunique())
            
            if 'Vehicle_Category' in data.columns:
                category_breakdown = data.groupby('Vehicle_Category')['TOTAL'].sum().to_dict()
                overview['category_breakdown'] = category_breakdown
            
            return overview
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error getting market overview: {e}")
            return {}
    
    def _get_growth_leaders(self) -> List[str]:
        """Identify growth leaders from calculated metrics."""
        try:
            leaders = []
            
            if 'category_growth' in self.growth_metrics:
                for category, growth_data in self.growth_metrics['category_growth'].items():
                    if growth_data:
                        latest_growth = list(growth_data.values())[-1] if growth_data else 0
                        if latest_growth > 10:  # 10% growth threshold
                            leaders.append(f"{category} category showing {latest_growth}% growth")
            
            if 'state_growth' in self.growth_metrics:
                for state, growth_data in self.growth_metrics['state_growth'].items():
                    if growth_data:
                        latest_growth = list(growth_data.values())[-1] if growth_data else 0
                        if latest_growth > 15:  # 15% growth threshold for states
                            leaders.append(f"{state} state showing {latest_growth}% growth")
            
            return leaders[:5]  # Top 5 leaders
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error identifying growth leaders: {e}")
            return []
    
    def _identify_risk_factors(self) -> List[str]:
        """Identify potential risk factors based on data analysis."""
        try:
            risks = []
            
            if 'category_growth' in self.growth_metrics:
                for category, growth_data in self.growth_metrics['category_growth'].items():
                    if growth_data:
                        latest_growth = list(growth_data.values())[-1] if growth_data else 0
                        if latest_growth < -5:  # Negative growth threshold
                            risks.append(f"{category} category declining by {abs(latest_growth)}%")
            
            return risks
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error identifying risk factors: {e}")
            return []
    
    def _identify_opportunities(self) -> List[str]:
        """Identify investment opportunities based on growth trends."""
        try:
            opportunities = []
            
            if 'manufacturer_growth' in self.growth_metrics and 'growth_rates' in self.growth_metrics['manufacturer_growth']:
                growth_rates = self.growth_metrics['manufacturer_growth']['growth_rates']
                
                for manufacturer, growth_rate in growth_rates.items():
                    if growth_rate > 20:  # High growth threshold
                        opportunities.append(f"{manufacturer} showing exceptional {growth_rate}% growth")
            
            return opportunities[:3]  # Top 3 opportunities
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error identifying opportunities: {e}")
            return []
    
    def export_processed_data(self, filename: str = None) -> str:
        """Export processed data with growth metrics to CSV."""
        if self.processed_data is None:
            raise DataProcessingError("No processed data available to export")
        
        if filename is None:
            filename = Config.get_output_filename("vahan_processed")
        
        filepath = Config.OUTPUT_DIR / filename
        Config.ensure_directories()
        
        try:
            # Export main data
            self.processed_data.to_csv(filepath, index=False)
            self.logger.info(f"💾 Processed data exported to {filepath}")
            
            # Export growth metrics if available
            if self.growth_metrics:
                metrics_filename = filepath.with_name(f"metrics_{filepath.name}")
                
                # Convert growth metrics to DataFrame format for export
                all_metrics = []
                for metric_name, metric_data in self.growth_metrics.items():
                    if isinstance(metric_data, dict) and metric_data:
                        metric_df = pd.DataFrame(list(metric_data.items()), 
                                               columns=['Category', 'Value'])
                        metric_df['Metric_Type'] = metric_name
                        all_metrics.append(metric_df)
                
                if all_metrics:
                    combined_metrics = pd.concat(all_metrics, ignore_index=True)
                    combined_metrics.to_csv(metrics_filename, index=False)
                    self.logger.info(f"📊 Growth metrics exported to {metrics_filename}")
            
            return str(filepath)
            
        except Exception as e:
            raise DataProcessingError(f"Failed to export data: {e}")
    
    def process_all(self, data: pd.DataFrame) -> ProcessingResult:
        """Perform complete data processing pipeline.
        
        Args:
            data: Raw input data
            
        Returns:
            ProcessingResult: Complete processing results
        """
        start_time = time.time()
        
        try:
            # Clean the data
            cleaned_data = self.clean_data(data)
            
            # Calculate growth metrics
            growth_metrics = self.calculate_growth_metrics(cleaned_data)
            
            # Generate insights
            insights = self.get_investor_insights(cleaned_data)
            
            processing_time = time.time() - start_time
            
            # Create structured result
            result = ProcessingResult(
                cleaned_data=cleaned_data,
                growth_metrics=GrowthMetrics(**growth_metrics) if growth_metrics else None,
                insights=MarketInsights(**insights) if insights else None,
                processing_time=processing_time,
                records_processed=len(cleaned_data)
            )
            
            self.logger.info(f"✅ Complete processing finished in {processing_time:.2f}s")
            return result
            
        except Exception as e:
            raise DataProcessingError(f"Complete processing failed: {e}")
