"""
Insight generation module for VAHAN data analysis.
Generates business insights and investment recommendations.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from datetime import datetime

from ..core.config import Config
from ..core.exceptions import DataProcessingError
from ..utils.logging_utils import get_logger

class InsightGenerator:
    """Generate business insights and investment recommendations from VAHAN data."""
    
    def __init__(self):
        """Initialize the insight generator."""
        self.logger = get_logger(self.__class__.__name__)
    
    def generate_market_insights(self, data: pd.DataFrame, 
                               growth_metrics: Dict) -> Dict:
        """Generate comprehensive market insights.
        
        Args:
            data: Processed VAHAN data
            growth_metrics: Calculated growth metrics
            
        Returns:
            Dict: Market insights and recommendations
        """
        try:
            insights = {
                'executive_summary': self._generate_executive_summary(data, growth_metrics),
                'market_opportunities': self._identify_market_opportunities(data, growth_metrics),
                'risk_assessment': self._assess_market_risks(data, growth_metrics),
                'investment_recommendations': self._generate_investment_recommendations(data, growth_metrics),
                'competitive_landscape': self._analyze_competitive_landscape(data),
                'regulatory_insights': self._generate_regulatory_insights(data)
            }
            
            return insights
            
        except Exception as e:
            self.logger.error(f"❌ Error generating market insights: {e}")
            return {}
    
    def _generate_executive_summary(self, data: pd.DataFrame, 
                                  growth_metrics: Dict) -> Dict:
        """Generate executive summary of market conditions."""
        try:
            summary = {}
            
            # Market size and growth
            if 'TOTAL' in data.columns:
                total_market = data['TOTAL'].sum()
                summary['market_size'] = f"{total_market:,} total registrations"
            
            # Growth trend
            if 'yoy_growth' in growth_metrics and growth_metrics['yoy_growth']:
                latest_growth = list(growth_metrics['yoy_growth'].values())[-1]
                if latest_growth > 0:
                    summary['growth_trend'] = f"Market growing at {latest_growth}% YoY"
                else:
                    summary['growth_trend'] = f"Market declining by {abs(latest_growth)}% YoY"
            
            # Key segments
            if 'Vehicle_Category' in data.columns:
                category_breakdown = data.groupby('Vehicle_Category')['TOTAL'].sum()
                dominant_category = category_breakdown.idxmax()
                summary['dominant_segment'] = f"{dominant_category} leads with {category_breakdown[dominant_category]:,} registrations"
            
            # Geographic spread
            if 'State' in data.columns:
                state_count = data['State'].nunique()
                summary['geographic_coverage'] = f"Active in {state_count} states"
            
            return summary
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error generating executive summary: {e}")
            return {}
    
    def _identify_market_opportunities(self, data: pd.DataFrame, 
                                     growth_metrics: Dict) -> List[Dict]:
        """Identify market opportunities based on data analysis."""
        try:
            opportunities = []
            
            # High-growth categories
            if 'category_growth' in growth_metrics:
                for category, growth_data in growth_metrics['category_growth'].items():
                    if growth_data:
                        latest_growth = list(growth_data.values())[-1]
                        if latest_growth > 15:  # High growth threshold
                            opportunities.append({
                                'type': 'High Growth Segment',
                                'description': f"{category} category showing {latest_growth}% growth",
                                'priority': 'High',
                                'potential_impact': 'Market expansion opportunity'
                            })
            
            # Emerging markets (states with moderate growth)
            if 'state_growth' in growth_metrics:
                for state, growth_data in growth_metrics['state_growth'].items():
                    if growth_data:
                        latest_growth = list(growth_data.values())[-1]
                        if 5 < latest_growth < 20:  # Moderate growth range
                            opportunities.append({
                                'type': 'Emerging Market',
                                'description': f"{state} showing steady {latest_growth}% growth",
                                'priority': 'Medium',
                                'potential_impact': 'Geographic expansion opportunity'
                            })
            
            # Underperforming segments with potential
            if 'Vehicle_Category' in data.columns:
                category_totals = data.groupby('Vehicle_Category')['TOTAL'].sum()
                smallest_category = category_totals.idxmin()
                if category_totals[smallest_category] > 0:
                    opportunities.append({
                        'type': 'Underserved Segment',
                        'description': f"{smallest_category} category has low penetration",
                        'priority': 'Medium',
                        'potential_impact': 'Market development opportunity'
                    })
            
            return opportunities[:5]  # Top 5 opportunities
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error identifying opportunities: {e}")
            return []
    
    def _assess_market_risks(self, data: pd.DataFrame, 
                           growth_metrics: Dict) -> List[Dict]:
        """Assess market risks and challenges."""
        try:
            risks = []
            
            # Declining segments
            if 'category_growth' in growth_metrics:
                for category, growth_data in growth_metrics['category_growth'].items():
                    if growth_data:
                        latest_growth = list(growth_data.values())[-1]
                        if latest_growth < -5:  # Decline threshold
                            risks.append({
                                'type': 'Market Decline',
                                'description': f"{category} category declining by {abs(latest_growth)}%",
                                'severity': 'High' if latest_growth < -15 else 'Medium',
                                'mitigation': 'Diversification or market exit strategy needed'
                            })
            
            # Market concentration risk
            if 'State' in data.columns and 'TOTAL' in data.columns:
                state_totals = data.groupby('State')['TOTAL'].sum()
                total_market = state_totals.sum()
                top_state_share = (state_totals.max() / total_market) * 100
                
                if top_state_share > 40:  # High concentration
                    risks.append({
                        'type': 'Geographic Concentration',
                        'description': f"Top state represents {top_state_share:.1f}% of market",
                        'severity': 'Medium',
                        'mitigation': 'Geographic diversification recommended'
                    })
            
            # Manufacturer concentration
            if 'Vehicle Class' in data.columns and 'TOTAL' in data.columns:
                mfg_totals = data.groupby('Vehicle Class')['TOTAL'].sum()
                total_registrations = mfg_totals.sum()
                top_mfg_share = (mfg_totals.max() / total_registrations) * 100
                
                if top_mfg_share > 30:  # High concentration
                    risks.append({
                        'type': 'Manufacturer Concentration',
                        'description': f"Top manufacturer has {top_mfg_share:.1f}% market share",
                        'severity': 'Medium',
                        'mitigation': 'Monitor competitive dynamics'
                    })
            
            return risks
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error assessing risks: {e}")
            return []
    
    def _generate_investment_recommendations(self, data: pd.DataFrame, 
                                          growth_metrics: Dict) -> List[Dict]:
        """Generate investment recommendations based on analysis."""
        try:
            recommendations = []
            
            # Growth-based recommendations
            if 'category_growth' in growth_metrics:
                high_growth_categories = []
                for category, growth_data in growth_metrics['category_growth'].items():
                    if growth_data:
                        latest_growth = list(growth_data.values())[-1]
                        if latest_growth > 10:
                            high_growth_categories.append((category, latest_growth))
                
                if high_growth_categories:
                    top_category = max(high_growth_categories, key=lambda x: x[1])
                    recommendations.append({
                        'type': 'Growth Investment',
                        'recommendation': f"Increase investment in {top_category[0]} segment",
                        'rationale': f"Showing strong {top_category[1]}% growth",
                        'timeframe': 'Short-term (6-12 months)',
                        'confidence': 'High'
                    })
            
            # Market expansion recommendations
            if 'state_growth' in growth_metrics:
                emerging_states = []
                for state, growth_data in growth_metrics['state_growth'].items():
                    if growth_data:
                        latest_growth = list(growth_data.values())[-1]
                        if 5 < latest_growth < 25:  # Sustainable growth range
                            emerging_states.append((state, latest_growth))
                
                if emerging_states:
                    top_state = max(emerging_states, key=lambda x: x[1])
                    recommendations.append({
                        'type': 'Market Expansion',
                        'recommendation': f"Expand operations in {top_state[0]}",
                        'rationale': f"Emerging market with {top_state[1]}% growth",
                        'timeframe': 'Medium-term (12-18 months)',
                        'confidence': 'Medium'
                    })
            
            # Diversification recommendations
            if len(recommendations) == 0:  # If no clear growth opportunities
                recommendations.append({
                    'type': 'Diversification',
                    'recommendation': 'Focus on market diversification and efficiency',
                    'rationale': 'Limited high-growth opportunities identified',
                    'timeframe': 'Long-term (18+ months)',
                    'confidence': 'Medium'
                })
            
            return recommendations
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error generating recommendations: {e}")
            return []
    
    def _analyze_competitive_landscape(self, data: pd.DataFrame) -> Dict:
        """Analyze competitive landscape and market dynamics."""
        try:
            landscape = {}
            
            if 'Vehicle Class' in data.columns and 'TOTAL' in data.columns:
                # Market share analysis
                mfg_totals = data.groupby('Vehicle Class')['TOTAL'].sum().sort_values(ascending=False)
                total_market = mfg_totals.sum()
                
                # Top players
                top_5 = mfg_totals.head(5)
                landscape['market_leaders'] = []
                
                for i, (manufacturer, registrations) in enumerate(top_5.items(), 1):
                    market_share = (registrations / total_market) * 100
                    landscape['market_leaders'].append({
                        'rank': i,
                        'manufacturer': manufacturer,
                        'registrations': int(registrations),
                        'market_share': round(market_share, 2)
                    })
                
                # Market concentration
                hhi = sum([(reg / total_market) ** 2 for reg in mfg_totals]) * 10000
                if hhi > 2500:
                    concentration = "Highly Concentrated"
                elif hhi > 1500:
                    concentration = "Moderately Concentrated"
                else:
                    concentration = "Competitive"
                
                landscape['market_concentration'] = {
                    'hhi_index': round(hhi, 0),
                    'classification': concentration
                }
            
            return landscape
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error analyzing competitive landscape: {e}")
            return {}
    
    def _generate_regulatory_insights(self, data: pd.DataFrame) -> Dict:
        """Generate insights about regulatory environment and compliance."""
        try:
            regulatory_insights = {}
            
            # State-wise analysis for regulatory patterns
            if 'State' in data.columns and 'TOTAL' in data.columns:
                state_totals = data.groupby('State')['TOTAL'].sum().sort_values(ascending=False)
                
                # Identify states with unusual patterns
                mean_registrations = state_totals.mean()
                std_registrations = state_totals.std()
                
                outlier_states = []
                for state, registrations in state_totals.items():
                    if registrations > mean_registrations + 2 * std_registrations:
                        outlier_states.append({
                            'state': state,
                            'registrations': int(registrations),
                            'type': 'High Activity'
                        })
                    elif registrations < mean_registrations - 2 * std_registrations:
                        outlier_states.append({
                            'state': state,
                            'registrations': int(registrations),
                            'type': 'Low Activity'
                        })
                
                regulatory_insights['regulatory_patterns'] = outlier_states
            
            # Vehicle category compliance patterns
            if 'Vehicle_Category' in data.columns:
                category_distribution = data['Vehicle_Category'].value_counts()
                regulatory_insights['category_compliance'] = {
                    'dominant_categories': category_distribution.head(3).to_dict(),
                    'total_categories': len(category_distribution)
                }
            
            return regulatory_insights
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error generating regulatory insights: {e}")
            return {}
    
    def generate_dashboard_summary(self, data: pd.DataFrame, 
                                 growth_metrics: Dict) -> Dict:
        """Generate summary for dashboard display.
        
        Args:
            data: Processed data
            growth_metrics: Growth metrics
            
        Returns:
            Dict: Dashboard summary
        """
        try:
            summary = {
                'key_metrics': {},
                'highlights': [],
                'alerts': []
            }
            
            # Key metrics
            if 'TOTAL' in data.columns:
                summary['key_metrics']['total_registrations'] = int(data['TOTAL'].sum())
            
            if 'State' in data.columns:
                summary['key_metrics']['states_covered'] = int(data['State'].nunique())
            
            if 'Vehicle_Category' in data.columns:
                summary['key_metrics']['vehicle_categories'] = int(data['Vehicle_Category'].nunique())
            
            # Highlights (positive insights)
            if 'category_growth' in growth_metrics:
                for category, growth_data in growth_metrics['category_growth'].items():
                    if growth_data:
                        latest_growth = list(growth_data.values())[-1]
                        if latest_growth > 15:
                            summary['highlights'].append(
                                f"{category} segment showing exceptional {latest_growth}% growth"
                            )
            
            # Alerts (areas needing attention)
            if 'category_growth' in growth_metrics:
                for category, growth_data in growth_metrics['category_growth'].items():
                    if growth_data:
                        latest_growth = list(growth_data.values())[-1]
                        if latest_growth < -10:
                            summary['alerts'].append(
                                f"{category} segment declining by {abs(latest_growth)}%"
                            )
            
            return summary
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error generating dashboard summary: {e}")
            return {}
