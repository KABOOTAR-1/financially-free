"""
Growth analysis module for VAHAN data.
Provides advanced growth metrics and trend analysis.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta

from ..core.config import Config
from ..core.exceptions import DataProcessingError
from ..utils.logging_utils import get_logger

class GrowthAnalyzer:
    """Advanced growth analysis for VAHAN vehicle registration data."""
    
    def __init__(self):
        """Initialize the growth analyzer."""
        self.logger = get_logger(self.__class__.__name__)
    
    def calculate_compound_growth_rate(self, data: pd.DataFrame, 
                                     column: str = 'TOTAL',
                                     period_column: str = 'Year') -> float:
        """Calculate Compound Annual Growth Rate (CAGR).
        
        Args:
            data: DataFrame containing the data
            column: Column to calculate CAGR for
            period_column: Column containing time periods
            
        Returns:
            float: CAGR percentage
        """
        try:
            if column not in data.columns or period_column not in data.columns:
                return 0.0
            
            # Group by period and sum
            period_data = data.groupby(period_column)[column].sum().reset_index()
            period_data = period_data.sort_values(period_column)
            
            if len(period_data) < 2:
                return 0.0
            
            start_value = period_data.iloc[0][column]
            end_value = period_data.iloc[-1][column]
            num_periods = len(period_data) - 1
            
            if start_value <= 0:
                return 0.0
            
            cagr = (pow(end_value / start_value, 1/num_periods) - 1) * 100
            return round(cagr, 2)
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error calculating CAGR: {e}")
            return 0.0
    
    def analyze_seasonal_trends(self, data: pd.DataFrame) -> Dict:
        """Analyze seasonal trends in vehicle registrations.
        
        Args:
            data: DataFrame with date/time information
            
        Returns:
            Dict: Seasonal trend analysis
        """
        try:
            seasonal_analysis = {}
            
            # If we have monthly data, analyze by month
            if 'Month' in data.columns:
                monthly_trends = data.groupby('Month')['TOTAL'].mean()
                seasonal_analysis['monthly_averages'] = monthly_trends.to_dict()
                
                # Identify peak and low seasons
                peak_month = monthly_trends.idxmax()
                low_month = monthly_trends.idxmin()
                
                seasonal_analysis['peak_season'] = {
                    'month': peak_month,
                    'average_registrations': monthly_trends[peak_month]
                }
                seasonal_analysis['low_season'] = {
                    'month': low_month,
                    'average_registrations': monthly_trends[low_month]
                }
            
            return seasonal_analysis
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error analyzing seasonal trends: {e}")
            return {}
    
    def calculate_market_penetration(self, data: pd.DataFrame) -> Dict:
        """Calculate market penetration metrics.
        
        Args:
            data: DataFrame with state and registration data
            
        Returns:
            Dict: Market penetration analysis
        """
        try:
            penetration_metrics = {}
            
            if 'State' in data.columns and 'TOTAL' in data.columns:
                # Calculate registrations per state
                state_registrations = data.groupby('State')['TOTAL'].sum()
                total_registrations = state_registrations.sum()
                
                # Calculate market share by state
                market_share = (state_registrations / total_registrations * 100).round(2)
                penetration_metrics['state_market_share'] = market_share.to_dict()
                
                # Identify dominant and emerging markets
                dominant_states = market_share[market_share > 10].index.tolist()
                emerging_states = market_share[(market_share > 2) & (market_share <= 10)].index.tolist()
                
                penetration_metrics['dominant_markets'] = dominant_states
                penetration_metrics['emerging_markets'] = emerging_states
            
            return penetration_metrics
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error calculating market penetration: {e}")
            return {}
    
    def analyze_growth_volatility(self, data: pd.DataFrame) -> Dict:
        """Analyze growth volatility and stability.
        
        Args:
            data: DataFrame with time series data
            
        Returns:
            Dict: Volatility analysis
        """
        try:
            volatility_analysis = {}
            
            if 'Year' in data.columns and 'TOTAL' in data.columns:
                # Calculate year-over-year growth rates
                yearly_data = data.groupby('Year')['TOTAL'].sum().reset_index()
                yearly_data = yearly_data.sort_values('Year')
                
                growth_rates = []
                for i in range(1, len(yearly_data)):
                    current = yearly_data.iloc[i]['TOTAL']
                    previous = yearly_data.iloc[i-1]['TOTAL']
                    
                    if previous > 0:
                        growth_rate = ((current - previous) / previous) * 100
                        growth_rates.append(growth_rate)
                
                if growth_rates:
                    volatility_analysis['growth_rates'] = growth_rates
                    volatility_analysis['average_growth'] = round(np.mean(growth_rates), 2)
                    volatility_analysis['growth_volatility'] = round(np.std(growth_rates), 2)
                    volatility_analysis['stability_score'] = self._calculate_stability_score(growth_rates)
            
            return volatility_analysis
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error analyzing growth volatility: {e}")
            return {}
    
    def _calculate_stability_score(self, growth_rates: List[float]) -> str:
        """Calculate stability score based on growth rate variance."""
        if not growth_rates:
            return "Unknown"
        
        volatility = np.std(growth_rates)
        
        if volatility < 5:
            return "Very Stable"
        elif volatility < 10:
            return "Stable"
        elif volatility < 20:
            return "Moderate"
        elif volatility < 30:
            return "Volatile"
        else:
            return "Highly Volatile"
    
    def identify_growth_patterns(self, data: pd.DataFrame) -> Dict:
        """Identify growth patterns and trends.
        
        Args:
            data: DataFrame with time series data
            
        Returns:
            Dict: Growth pattern analysis
        """
        try:
            patterns = {}
            
            if 'Year' in data.columns and 'TOTAL' in data.columns:
                yearly_data = data.groupby('Year')['TOTAL'].sum().reset_index()
                yearly_data = yearly_data.sort_values('Year')
                
                if len(yearly_data) >= 3:
                    # Analyze trend direction
                    recent_years = yearly_data.tail(3)
                    trend_direction = self._analyze_trend_direction(recent_years)
                    patterns['trend_direction'] = trend_direction
                    
                    # Identify acceleration/deceleration
                    acceleration = self._analyze_growth_acceleration(yearly_data)
                    patterns['growth_acceleration'] = acceleration
                    
                    # Identify cyclical patterns
                    cyclical = self._identify_cyclical_patterns(yearly_data)
                    patterns['cyclical_patterns'] = cyclical
            
            return patterns
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error identifying growth patterns: {e}")
            return {}
    
    def _analyze_trend_direction(self, data: pd.DataFrame) -> str:
        """Analyze the overall trend direction."""
        if len(data) < 2:
            return "Insufficient Data"
        
        values = data['TOTAL'].values
        
        # Calculate trend using linear regression slope
        x = np.arange(len(values))
        slope = np.polyfit(x, values, 1)[0]
        
        if slope > 0:
            return "Upward Trend"
        elif slope < 0:
            return "Downward Trend"
        else:
            return "Flat Trend"
    
    def _analyze_growth_acceleration(self, data: pd.DataFrame) -> str:
        """Analyze if growth is accelerating or decelerating."""
        if len(data) < 3:
            return "Insufficient Data"
        
        # Calculate growth rates
        growth_rates = []
        for i in range(1, len(data)):
            current = data.iloc[i]['TOTAL']
            previous = data.iloc[i-1]['TOTAL']
            
            if previous > 0:
                growth_rate = ((current - previous) / previous) * 100
                growth_rates.append(growth_rate)
        
        if len(growth_rates) < 2:
            return "Insufficient Data"
        
        # Compare recent growth rates
        recent_avg = np.mean(growth_rates[-2:])
        earlier_avg = np.mean(growth_rates[:-2]) if len(growth_rates) > 2 else growth_rates[0]
        
        if recent_avg > earlier_avg + 2:
            return "Accelerating"
        elif recent_avg < earlier_avg - 2:
            return "Decelerating"
        else:
            return "Stable"
    
    def _identify_cyclical_patterns(self, data: pd.DataFrame) -> Dict:
        """Identify cyclical patterns in the data."""
        try:
            if len(data) < 4:
                return {"pattern": "Insufficient Data"}
            
            values = data['TOTAL'].values
            
            # Simple peak/trough analysis
            peaks = []
            troughs = []
            
            for i in range(1, len(values) - 1):
                if values[i] > values[i-1] and values[i] > values[i+1]:
                    peaks.append(i)
                elif values[i] < values[i-1] and values[i] < values[i+1]:
                    troughs.append(i)
            
            cycle_info = {
                "peaks_count": len(peaks),
                "troughs_count": len(troughs),
                "pattern": "Linear" if len(peaks) + len(troughs) <= 1 else "Cyclical"
            }
            
            return cycle_info
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error identifying cyclical patterns: {e}")
            return {"pattern": "Unknown"}
    
    def generate_growth_forecast(self, data: pd.DataFrame, 
                               forecast_periods: int = 2) -> Dict:
        """Generate simple growth forecast based on historical trends.
        
        Args:
            data: Historical data
            forecast_periods: Number of periods to forecast
            
        Returns:
            Dict: Forecast results
        """
        try:
            if 'Year' in data.columns and 'TOTAL' in data.columns:
                yearly_data = data.groupby('Year')['TOTAL'].sum().reset_index()
                yearly_data = yearly_data.sort_values('Year')
                
                if len(yearly_data) < 2:
                    return {"error": "Insufficient historical data"}
                
                # Simple linear trend forecast
                years = yearly_data['Year'].values
                values = yearly_data['TOTAL'].values
                
                # Fit linear trend
                coeffs = np.polyfit(years, values, 1)
                slope, intercept = coeffs
                
                # Generate forecasts
                last_year = years[-1]
                forecasts = {}
                
                for i in range(1, forecast_periods + 1):
                    forecast_year = last_year + i
                    forecast_value = slope * forecast_year + intercept
                    forecasts[int(forecast_year)] = max(0, int(forecast_value))
                
                # Calculate confidence based on historical fit
                predicted_values = slope * years + intercept
                r_squared = 1 - (np.sum((values - predicted_values) ** 2) / 
                                np.sum((values - np.mean(values)) ** 2))
                
                confidence = "High" if r_squared > 0.8 else "Medium" if r_squared > 0.5 else "Low"
                
                return {
                    "forecasts": forecasts,
                    "confidence": confidence,
                    "r_squared": round(r_squared, 3),
                    "trend_slope": round(slope, 2)
                }
            
            return {"error": "Required columns not found"}
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error generating forecast: {e}")
            return {"error": str(e)}
