"""
Data processing module for VAHAN vehicle registration data.
Handles data cleaning, transformation, and calculation of YoY/QoQ growth rates.

This module provides a comprehensive data processing pipeline for VAHAN vehicle
registration data with the following capabilities:
- Data cleaning and standardization
- Growth metrics calculation (YoY/QoQ)
- Vehicle categorization
- Investor insights generation
- Data export functionality
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Union
import re
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class VahanDataProcessor:
    """Process and analyze VAHAN vehicle registration data with investor-focused metrics.
    
    This class provides a complete pipeline for processing VAHAN data including:
    - Data loading and validation
    - Comprehensive data cleaning
    - Growth metrics calculation
    - Investor insights generation
    - Data export capabilities
    """
    
    # Class constants
    NUMERIC_COLUMNS = ['2WIC', '2WN', '2WT', '3WN', '3WT', 'LMV', 'MMV', 'HMV', 'TOTAL']
    VEHICLE_CATEGORIES = {
        '2W': ['MOTOR CYCLE', 'SCOOTER', 'M-CYCLE', 'MOPED'],
        '3W': ['AUTO RICKSHAW', '3W', 'THREE WHEELER'],
        '4W+': ['CAR', 'TRUCK', 'BUS', 'LMV', 'MMV', 'HMV']
    }
    
    def __init__(self):
        """Initialize the data processor."""
        self.data: Optional[pd.DataFrame] = None
        self.processed_data: Optional[pd.DataFrame] = None
        self.growth_metrics: Dict = {}
        self.logger = logging.getLogger(__name__)
        
    def load_data(self, file_path: Union[str, Path]) -> pd.DataFrame:
        """Load data from CSV file with validation.
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            pd.DataFrame: Loaded data
            
        Raises:
            FileNotFoundError: If file doesn't exist
            ValueError: If file is empty or invalid
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
            
        try:
            self.data = pd.read_csv(file_path)
            
            if self.data.empty:
                raise ValueError("Loaded file is empty")
                
            self.logger.info(f"✅ Loaded {len(self.data)} rows from {file_path}")
            return self.data
            
        except Exception as e:
            self.logger.error(f"❌ Error loading data: {e}")
            raise
    
    def clean_data(self, data: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """Clean and standardize the VAHAN data.
        
        Args:
            data: DataFrame to clean (uses self.data if None)
            
        Returns:
            pd.DataFrame: Cleaned data
            
        Raises:
            ValueError: If no data available to clean
        """
        if data is None:
            if self.data is None:
                raise ValueError("No data available to clean. Load data first.")
            data = self.data.copy()
        else:
            data = data.copy()
        
        if data.empty:
            self.logger.warning("⚠️ No data to clean")
            return data
        
        self.logger.info("🧹 Cleaning data...")
        
        # Remove completely empty rows
        data = data.dropna(how='all')
        
        # Clean numeric columns using class constants
        data = self._clean_numeric_columns(data)
        
        # Clean text columns
        data = self._clean_text_columns(data)
        
        # Extract and clean temporal data
        data = self._extract_temporal_data(data)
        
        # Create vehicle category groupings
        data['Vehicle_Category'] = data.apply(self._categorize_vehicle, axis=1)
        
        self.logger.info(f"✅ Data cleaned. Shape: {data.shape}")
        self.processed_data = data
        return data
    
    def _clean_numeric_columns(self, data: pd.DataFrame) -> pd.DataFrame:
        """Clean numeric columns by removing commas and converting to numeric."""
        for col in self.NUMERIC_COLUMNS:
            if col in data.columns:
                try:
                    # Convert to string first, then clean
                    data[col] = data[col].astype(str)
                    # Remove commas, spaces, and replace dashes with 0
                    data[col] = data[col].str.replace(',', '', regex=False)
                    data[col] = data[col].str.replace(' ', '', regex=False)
                    data[col] = data[col].str.replace('-', '0', regex=False)
                    data[col] = data[col].str.replace('nan', '0', regex=False)
                    # Convert to numeric
                    data[col] = pd.to_numeric(data[col], errors='coerce').fillna(0)
                except Exception as e:
                    self.logger.warning(f"⚠️ Error cleaning column {col}: {str(e)}")
                    # Set to 0 if cleaning fails
                    data[col] = 0
        return data
    
    def _clean_text_columns(self, data: pd.DataFrame) -> pd.DataFrame:
        """Clean text columns by standardizing format."""
        # Clean vehicle class names
        if 'Vehicle Class' in data.columns:
            data['Vehicle Class'] = data['Vehicle Class'].str.strip().str.upper()
        return data
    
    def _extract_temporal_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Extract and clean temporal information."""
        # Extract and clean year information
        if 'Filter_Year' in data.columns:
            data['Year'] = data['Filter_Year'].astype(str).str.extract(r'(\d{4})')[0]
            data['Year'] = pd.to_numeric(data['Year'], errors='coerce')
        
        # Extract and clean state information
        if 'Filter_State' in data.columns:
            data['State'] = data['Filter_State'].str.replace(r'\(\d+\)', '', regex=True).str.strip()
        
        # Add quarter information if month data is available
        if 'Year' in data.columns:
            # For now, assume data is quarterly - this can be enhanced based on actual data structure
            data['Quarter'] = 'Q4'  # Default assumption - can be refined
            data['Year_Quarter'] = data['Year'].astype(str) + '_' + data['Quarter']
        
        return data
    
    def _categorize_vehicle(self, row) -> str:
        """Categorize vehicles into 2W, 3W, 4W+ based on available data.
        
        Args:
            row: DataFrame row containing vehicle information
            
        Returns:
            str: Vehicle category ('2W', '3W', '4W+', 'Other', 'Unknown')
        """
        vehicle_class = row.get('Vehicle Class', '')
        
        if pd.isna(vehicle_class) or vehicle_class == '':
            return 'Unknown'
        
        vehicle_class = str(vehicle_class).upper()
        
        # Use class constants for categorization
        for category, keywords in self.VEHICLE_CATEGORIES.items():
            if any(keyword in vehicle_class for keyword in keywords):
                return category
        
        return 'Other'
    
    def calculate_growth_metrics(self, data: pd.DataFrame = None) -> Dict:
        """Calculate YoY and QoQ growth rates for vehicle categories and manufacturers."""
        if data is None:
            data = self.processed_data if self.processed_data is not None else self.data
        
        if data is None or data.empty:
            print("⚠️ No data available for growth calculations")
            return {}
        
        print("📈 Calculating growth metrics...")
        
        growth_data = {}
        
        try:
            # Check if we have sufficient data for YoY calculations
            if 'Year' in data.columns:
                unique_years = data['Year'].nunique()
                if unique_years >= 2:
                    print(f"✅ Found {unique_years} years - calculating YoY growth")
                    growth_data['yoy_total'] = self._calculate_yoy_growth(data, 'TOTAL')
                    growth_data['yoy_by_category'] = self._calculate_yoy_by_category(data)
                    growth_data['yoy_by_state'] = self._calculate_yoy_by_state(data)
                else:
                    print(f"⚠️ Only {unique_years} year(s) available - YoY growth not possible")
                    growth_data['yoy_total'] = pd.DataFrame()
                    growth_data['yoy_by_category'] = pd.DataFrame()
                    growth_data['yoy_by_state'] = pd.DataFrame()
            
            # QoQ Growth Calculations (if quarter data available)
            if 'Year_Quarter' in data.columns:
                unique_quarters = data['Year_Quarter'].nunique()
                if unique_quarters >= 2:
                    growth_data['qoq_total'] = self._calculate_qoq_growth(data, 'TOTAL')
                    growth_data['qoq_by_category'] = self._calculate_qoq_by_category(data)
                else:
                    print(f"⚠️ Only {unique_quarters} quarter(s) available - QoQ growth not possible")
                    growth_data['qoq_total'] = pd.DataFrame()
                    growth_data['qoq_by_category'] = pd.DataFrame()
            
            # Manufacturer-wise analysis (if available)
            if 'Vehicle Class' in data.columns:
                growth_data['manufacturer_trends'] = self._analyze_manufacturer_trends(data)
            
        except Exception as e:
            print(f"⚠️ Error calculating growth metrics: {str(e)}")
            # Return empty DataFrames for all metrics
            growth_data = {
                'yoy_total': pd.DataFrame(),
                'yoy_by_category': pd.DataFrame(),
                'yoy_by_state': pd.DataFrame(),
                'qoq_total': pd.DataFrame(),
                'qoq_by_category': pd.DataFrame(),
                'manufacturer_trends': {}
            }
        
        self.growth_metrics = growth_data
        return growth_data
    
    def _calculate_yoy_growth(self, data: pd.DataFrame, column: str) -> pd.DataFrame:
        """Calculate year-over-year growth for a specific column."""
        if column not in data.columns or 'Year' not in data.columns:
            return pd.DataFrame()
        
        # Group by year and sum the values
        yearly_data = data.groupby('Year')[column].sum().reset_index()
        yearly_data = yearly_data.sort_values('Year')
        
        # Calculate YoY growth
        yearly_data['Previous_Year_Value'] = yearly_data[column].shift(1)
        yearly_data['YoY_Growth_Rate'] = ((yearly_data[column] - yearly_data['Previous_Year_Value']) / 
                                         yearly_data['Previous_Year_Value'] * 100).round(2)
        yearly_data['YoY_Growth_Absolute'] = yearly_data[column] - yearly_data['Previous_Year_Value']
        
        return yearly_data
    
    def _calculate_yoy_by_category(self, data: pd.DataFrame) -> pd.DataFrame:
        """Calculate YoY growth by vehicle category."""
        if 'Vehicle_Category' not in data.columns:
            return pd.DataFrame()
        
        category_growth = []
        
        for category in data['Vehicle_Category'].unique():
            if pd.isna(category):
                continue
                
            category_data = data[data['Vehicle_Category'] == category]
            yearly_totals = category_data.groupby('Year')['TOTAL'].sum().reset_index()
            yearly_totals = yearly_totals.sort_values('Year')
            
            for i in range(1, len(yearly_totals)):
                current_year = yearly_totals.iloc[i]
                previous_year = yearly_totals.iloc[i-1]
                
                growth_rate = ((current_year['TOTAL'] - previous_year['TOTAL']) / 
                              previous_year['TOTAL'] * 100) if previous_year['TOTAL'] > 0 else 0
                
                category_growth.append({
                    'Category': category,
                    'Year': current_year['Year'],
                    'Current_Value': current_year['TOTAL'],
                    'Previous_Value': previous_year['TOTAL'],
                    'YoY_Growth_Rate': round(growth_rate, 2),
                    'YoY_Growth_Absolute': current_year['TOTAL'] - previous_year['TOTAL']
                })
        
        return pd.DataFrame(category_growth)
    
    def _calculate_yoy_by_state(self, data: pd.DataFrame) -> pd.DataFrame:
        """Calculate YoY growth by state."""
        if 'State' not in data.columns:
            return pd.DataFrame()
        
        state_growth = []
        
        for state in data['State'].unique():
            if pd.isna(state):
                continue
                
            state_data = data[data['State'] == state]
            yearly_totals = state_data.groupby('Year')['TOTAL'].sum().reset_index()
            yearly_totals = yearly_totals.sort_values('Year')
            
            for i in range(1, len(yearly_totals)):
                current_year = yearly_totals.iloc[i]
                previous_year = yearly_totals.iloc[i-1]
                
                growth_rate = ((current_year['TOTAL'] - previous_year['TOTAL']) / 
                              previous_year['TOTAL'] * 100) if previous_year['TOTAL'] > 0 else 0
                
                state_growth.append({
                    'State': state,
                    'Year': current_year['Year'],
                    'Current_Value': current_year['TOTAL'],
                    'Previous_Value': previous_year['TOTAL'],
                    'YoY_Growth_Rate': round(growth_rate, 2),
                    'YoY_Growth_Absolute': current_year['TOTAL'] - previous_year['TOTAL']
                })
        
        return pd.DataFrame(state_growth)
    
    def _calculate_qoq_growth(self, data: pd.DataFrame, column: str) -> pd.DataFrame:
        """Calculate quarter-over-quarter growth."""
        if 'Year_Quarter' not in data.columns:
            return pd.DataFrame()
        
        quarterly_data = data.groupby('Year_Quarter')[column].sum().reset_index()
        quarterly_data = quarterly_data.sort_values('Year_Quarter')
        
        quarterly_data['Previous_Quarter_Value'] = quarterly_data[column].shift(1)
        quarterly_data['QoQ_Growth_Rate'] = ((quarterly_data[column] - quarterly_data['Previous_Quarter_Value']) / 
                                            quarterly_data['Previous_Quarter_Value'] * 100).round(2)
        quarterly_data['QoQ_Growth_Absolute'] = quarterly_data[column] - quarterly_data['Previous_Quarter_Value']
        
        return quarterly_data
    
    def _calculate_qoq_by_category(self, data: pd.DataFrame) -> pd.DataFrame:
        """Calculate QoQ growth by vehicle category."""
        if 'Vehicle_Category' not in data.columns or 'Year_Quarter' not in data.columns:
            return pd.DataFrame()
        
        category_qoq = []
        
        for category in data['Vehicle_Category'].unique():
            if pd.isna(category):
                continue
                
            category_data = data[data['Vehicle_Category'] == category]
            quarterly_totals = category_data.groupby('Year_Quarter')['TOTAL'].sum().reset_index()
            quarterly_totals = quarterly_totals.sort_values('Year_Quarter')
            
            for i in range(1, len(quarterly_totals)):
                current_quarter = quarterly_totals.iloc[i]
                previous_quarter = quarterly_totals.iloc[i-1]
                
                growth_rate = ((current_quarter['TOTAL'] - previous_quarter['TOTAL']) / 
                              previous_quarter['TOTAL'] * 100) if previous_quarter['TOTAL'] > 0 else 0
                
                category_qoq.append({
                    'Category': category,
                    'Quarter': current_quarter['Year_Quarter'],
                    'Current_Value': current_quarter['TOTAL'],
                    'Previous_Value': previous_quarter['TOTAL'],
                    'QoQ_Growth_Rate': round(growth_rate, 2),
                    'QoQ_Growth_Absolute': current_quarter['TOTAL'] - previous_quarter['TOTAL']
                })
        
        return pd.DataFrame(category_qoq)
    
    def _analyze_manufacturer_trends(self, data: pd.DataFrame) -> Dict:
        """Analyze manufacturer/vehicle class trends."""
        if 'Vehicle Class' not in data.columns:
            return {}
        
        trends = {
            'top_manufacturers': self._get_top_manufacturers(data),
            'manufacturer_growth': self._get_manufacturer_growth(data),
            'market_share': self._calculate_market_share(data)
        }
        
        return trends
    
    def _get_top_manufacturers(self, data: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
        """Get top manufacturers by total registrations."""
        manufacturer_totals = data.groupby('Vehicle Class')['TOTAL'].sum().reset_index()
        manufacturer_totals = manufacturer_totals.sort_values('TOTAL', ascending=False).head(top_n)
        manufacturer_totals['Rank'] = range(1, len(manufacturer_totals) + 1)
        
        return manufacturer_totals
    
    def _get_manufacturer_growth(self, data: pd.DataFrame) -> pd.DataFrame:
        """Calculate manufacturer-wise growth rates."""
        if 'Year' not in data.columns:
            return pd.DataFrame()
        
        manufacturer_growth = []
        
        for manufacturer in data['Vehicle Class'].unique():
            if pd.isna(manufacturer):
                continue
                
            manufacturer_data = data[data['Vehicle Class'] == manufacturer]
            yearly_totals = manufacturer_data.groupby('Year')['TOTAL'].sum().reset_index()
            yearly_totals = yearly_totals.sort_values('Year')
            
            if len(yearly_totals) >= 2:
                latest_year = yearly_totals.iloc[-1]
                previous_year = yearly_totals.iloc[-2]
                
                growth_rate = ((latest_year['TOTAL'] - previous_year['TOTAL']) / 
                              previous_year['TOTAL'] * 100) if previous_year['TOTAL'] > 0 else 0
                
                manufacturer_growth.append({
                    'Manufacturer': manufacturer,
                    'Latest_Year': latest_year['Year'],
                    'Current_Registrations': latest_year['TOTAL'],
                    'Previous_Registrations': previous_year['TOTAL'],
                    'YoY_Growth_Rate': round(growth_rate, 2)
                })
        
        return pd.DataFrame(manufacturer_growth).sort_values('YoY_Growth_Rate', ascending=False)
    
    def _calculate_market_share(self, data: pd.DataFrame) -> pd.DataFrame:
        """Calculate market share by manufacturer."""
        total_market = data['TOTAL'].sum()
        
        market_share = data.groupby('Vehicle Class')['TOTAL'].sum().reset_index()
        market_share['Market_Share_Percent'] = (market_share['TOTAL'] / total_market * 100).round(2)
        market_share = market_share.sort_values('Market_Share_Percent', ascending=False)
        
        return market_share
    
    def get_investor_insights(self, data: pd.DataFrame = None) -> Dict:
        """Generate key investor insights from the processed data."""
        if data is None:
            data = self.processed_data if self.processed_data is not None else self.data
        
        if data is None or data.empty:
            return {}
        
        insights = {
            'market_overview': self._get_market_overview(data),
            'growth_leaders': self._get_growth_leaders(),
            'risk_factors': self._identify_risk_factors(),
            'investment_opportunities': self._identify_opportunities()
        }
        
        return insights
    
    def _get_market_overview(self, data: pd.DataFrame) -> Dict:
        """Get overall market overview metrics."""
        try:
            # Ensure TOTAL column is numeric
            if 'TOTAL' in data.columns:
                data_copy = data.copy()
                # Clean and convert TOTAL column to numeric
                data_copy['TOTAL'] = pd.to_numeric(
                    data_copy['TOTAL'].astype(str).str.replace(',', '').str.replace('-', '0'), 
                    errors='coerce'
                ).fillna(0)
                
                total_registrations = data_copy['TOTAL'].sum()
                avg_annual_registrations = data_copy.groupby('Year')['TOTAL'].sum().mean() if 'Year' in data_copy.columns else total_registrations
                
                # Vehicle class breakdown with cleaned data - prioritize Vehicle Class over Vehicle_Category
                if 'Vehicle Class' in data_copy.columns:
                    category_breakdown = data_copy.groupby('Vehicle Class')['TOTAL'].sum().to_dict()
                elif 'Vehicle_Category' in data_copy.columns:
                    category_breakdown = data_copy.groupby('Vehicle_Category')['TOTAL'].sum().to_dict()
                else:
                    category_breakdown = {}
            else:
                total_registrations = 0
                avg_annual_registrations = 0
                category_breakdown = {}
            
            return {
                'total_registrations': int(total_registrations) if pd.notna(total_registrations) else 0,
                'average_annual_registrations': int(avg_annual_registrations) if pd.notna(avg_annual_registrations) else 0,
                'category_breakdown': {k: int(v) if pd.notna(v) else 0 for k, v in category_breakdown.items()},
                'data_period': f"{data['Year'].min()}-{data['Year'].max()}" if 'Year' in data.columns else "Unknown"
            }
        except Exception as e:
            print(f"⚠️ Error in market overview calculation: {str(e)}")
            return {
                'total_registrations': 0,
                'average_annual_registrations': 0,
                'category_breakdown': {},
                'data_period': "Unknown"
            }
    
    def _get_growth_leaders(self) -> Dict:
        """Identify growth leaders from calculated metrics."""
        leaders = {}
        
        try:
            if hasattr(self, 'growth_metrics') and self.growth_metrics:
                if 'yoy_by_category' in self.growth_metrics:
                    yoy_category = self.growth_metrics['yoy_by_category']
                    if not yoy_category.empty:
                        top_growth = yoy_category.nlargest(5, 'YoY_Growth_Rate')
                        leaders['top_category_growth'] = top_growth.to_dict('records')
                
                if 'manufacturer_trends' in self.growth_metrics and 'manufacturer_growth' in self.growth_metrics['manufacturer_trends']:
                    manufacturer_growth = self.growth_metrics['manufacturer_trends']['manufacturer_growth']
                    if not manufacturer_growth.empty:
                        top_manufacturers = manufacturer_growth.head(5)
                        leaders['top_manufacturer_growth'] = top_manufacturers.to_dict('records')
        except Exception as e:
            print(f"⚠️ Error getting growth leaders: {str(e)}")
            leaders = {}
        
        return leaders
    
    def _identify_risk_factors(self) -> List[str]:
        """Identify potential risk factors based on data analysis."""
        risks = []
        
        if 'yoy_by_category' in self.growth_metrics:
            yoy_data = self.growth_metrics['yoy_by_category']
            if not yoy_data.empty:
                negative_growth = yoy_data[yoy_data['YoY_Growth_Rate'] < 0]
                if len(negative_growth) > 0:
                    risks.append(f"{len(negative_growth)} vehicle categories showing negative growth")
        
        if 'yoy_by_state' in self.growth_metrics:
            state_data = self.growth_metrics['yoy_by_state']
            if not state_data.empty:
                declining_states = state_data[state_data['YoY_Growth_Rate'] < -5]  # More than 5% decline
                if len(declining_states) > 0:
                    risks.append(f"{len(declining_states)} states showing significant decline (>5%)")
        
        return risks
    
    def _identify_opportunities(self) -> List[str]:
        """Identify investment opportunities based on growth trends."""
        opportunities = []
        
        if 'yoy_by_category' in self.growth_metrics:
            yoy_data = self.growth_metrics['yoy_by_category']
            if not yoy_data.empty:
                high_growth = yoy_data[yoy_data['YoY_Growth_Rate'] > 15]  # More than 15% growth
                if len(high_growth) > 0:
                    top_category = high_growth.iloc[0]
                    opportunities.append(f"{top_category['Category']} category showing strong growth ({top_category['YoY_Growth_Rate']}%)")
        
        if 'manufacturer_trends' in self.growth_metrics and 'top_manufacturers' in self.growth_metrics['manufacturer_trends']:
            top_manufacturers = self.growth_metrics['manufacturer_trends']['top_manufacturers']
            if not top_manufacturers.empty:
                opportunities.append(f"Market leader: {top_manufacturers.iloc[0]['Vehicle Class']} with {top_manufacturers.iloc[0]['TOTAL']} registrations")
        
        return opportunities
    
    def export_processed_data(self, filename: str = None) -> str:
        """Export processed data with growth metrics to CSV."""
        if self.processed_data is None:
            print("⚠️ No processed data to export")
            return ""
        
        if filename is None:
            filename = f"vahan_processed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        self.processed_data.to_csv(filename, index=False)
        print(f"💾 Processed data exported to {filename}")
        
        # Also export growth metrics if available
        if self.growth_metrics:
            metrics_filename = filename.replace('.csv', '_growth_metrics.csv')
            
            # Combine all growth metrics into a single file
            all_metrics = []
            for metric_name, metric_data in self.growth_metrics.items():
                if isinstance(metric_data, pd.DataFrame) and not metric_data.empty:
                    metric_data_copy = metric_data.copy()
                    metric_data_copy['Metric_Type'] = metric_name
                    all_metrics.append(metric_data_copy)
            
            if all_metrics:
                combined_metrics = pd.concat(all_metrics, ignore_index=True)
                combined_metrics.to_csv(metrics_filename, index=False)
                print(f"📊 Growth metrics exported to {metrics_filename}")
        
        return filename


# Utility functions for data analysis
def create_sample_data() -> pd.DataFrame:
    """Create sample VAHAN data for testing purposes."""
    np.random.seed(42)
    
    states = ['Karnataka', 'Maharashtra', 'Delhi', 'Tamil Nadu', 'Gujarat']
    years = [2021, 2022, 2023, 2024]
    vehicle_classes = ['MOTOR CYCLE', 'SCOOTER', 'CAR', 'AUTO RICKSHAW', 'TRUCK', 'BUS']
    
    sample_data = []
    
    for state in states:
        for year in years:
            for vehicle_class in vehicle_classes:
                # Generate realistic registration numbers
                base_registrations = np.random.randint(1000, 50000)
                growth_factor = 1.1 if year > 2021 else 1.0
                registrations = int(base_registrations * growth_factor)
                
                sample_data.append({
                    'S No': len(sample_data) + 1,
                    'Vehicle Class': vehicle_class,
                    '2WIC': np.random.randint(100, 1000) if 'MOTOR' in vehicle_class or 'SCOOTER' in vehicle_class else 0,
                    '2WN': np.random.randint(500, 5000) if 'MOTOR' in vehicle_class or 'SCOOTER' in vehicle_class else 0,
                    '2WT': np.random.randint(200, 2000) if 'MOTOR' in vehicle_class or 'SCOOTER' in vehicle_class else 0,
                    'TOTAL': registrations,
                    'Filter_State': f"{state}({np.random.randint(10, 99)})",
                    'Filter_Year': str(year),
                    'Filter_Vehicle_Type': 'ALL',
                    'Scraped_Date': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
    
    return pd.DataFrame(sample_data)


# Example usage
if __name__ == "__main__":
    # Create processor instance
    processor = VahanDataProcessor()
    
    # For testing, create sample data
    print("🧪 Creating sample data for testing...")
    sample_df = create_sample_data()
    
    # Process the data
    print("🔄 Processing data...")
    cleaned_data = processor.clean_data(sample_df)
    
    # Calculate growth metrics
    print("📈 Calculating growth metrics...")
    growth_metrics = processor.calculate_growth_metrics(cleaned_data)
    
    # Generate investor insights
    print("💡 Generating investor insights...")
    insights = processor.get_investor_insights(cleaned_data)
    
    # Display results
    print("\n" + "="*50)
    print("📊 PROCESSING RESULTS")
    print("="*50)
    
    print(f"\nData Shape: {cleaned_data.shape}")
    print(f"Years Covered: {cleaned_data['Year'].min()} - {cleaned_data['Year'].max()}")
    print(f"States Covered: {cleaned_data['State'].nunique()}")
    
    if insights:
        print("\n💡 Key Insights:")
        if 'market_overview' in insights:
            overview = insights['market_overview']
            print(f"- Total Registrations: {overview.get('total_registrations', 'N/A'):,}")
            print(f"- Data Period: {overview.get('data_period', 'Unknown')}")
        
        if 'risk_factors' in insights and insights['risk_factors']:
            print(f"\n⚠️ Risk Factors:")
            for risk in insights['risk_factors']:
                print(f"  - {risk}")
        
        if 'investment_opportunities' in insights and insights['investment_opportunities']:
            print(f"\n🚀 Investment Opportunities:")
            for opportunity in insights['investment_opportunities']:
                print(f"  - {opportunity}")
    
    # Export processed data
    output_file = processor.export_processed_data("sample_vahan_processed.csv")
    print(f"\n💾 Results saved to: {output_file}")