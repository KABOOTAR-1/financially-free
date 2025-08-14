"""
Streamlit Dashboard for VAHAN Vehicle Registration Data Analysis
Updated to use the new modular architecture.
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
import sys
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.core.config import Config
from src.scrapers import VahanScraper
from src.processors import VahanDataProcessor
from src.analytics import GrowthAnalyzer, InsightGenerator
from src.utils import setup_logging, get_logger, create_sample_data
from src.core.models import FilterCombination

# Page configuration
st.set_page_config(
    page_title="VAHAN Vehicle Registration Analytics",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Setup logging
setup_logging()

# Custom CSS for investor-focused styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 10px;
        border-left: 5px solid #1f77b4;
    }
    .growth-positive {
        color: #28a745;
        font-weight: bold;
    }
    .growth-negative {
        color: #dc3545;
        font-weight: bold;
    }
    .insight-box {
        background-color: #e8f4f8;
        padding: 1rem;
        border-radius: 5px;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

class ModularVahanDashboard:
    """Modular VAHAN Dashboard using the new architecture."""
    
    def __init__(self):
        """Initialize the dashboard with modular components."""
        self.processor = VahanDataProcessor()
        self.growth_analyzer = GrowthAnalyzer()
        self.insight_generator = InsightGenerator()
        self.logger = get_logger(self.__class__.__name__)
        
        # Initialize session state for data persistence
        if 'scraped_data' not in st.session_state:
            st.session_state.scraped_data = None
        if 'scraped_growth_metrics' not in st.session_state:
            st.session_state.scraped_growth_metrics = {}
        if 'data_source_type' not in st.session_state:
            st.session_state.data_source_type = None
        # Cache for dropdown data to avoid re-scraping
        if 'cached_dropdown_data' not in st.session_state:
            st.session_state.cached_dropdown_data = None
        if 'dropdown_cache_timestamp' not in st.session_state:
            st.session_state.dropdown_cache_timestamp = None

    def load_data(self, data_source="sample"):
        """Load data using the modular processor."""
        # Check if we have persisted scraped data
        if (data_source == "scrape" and 
            st.session_state.scraped_data is not None and 
            st.session_state.data_source_type == "scrape"):
            self.data = st.session_state.scraped_data
            self.growth_metrics = st.session_state.scraped_growth_metrics
            st.success("✅ Using previously scraped data!")
            return True
        
        if data_source == "sample":
            # Use modular sample data generation
            self.data = create_sample_data()
            st.session_state.data_source_type = "sample"
            st.success("✅ Sample data loaded successfully!")
        elif data_source == "upload":
            uploaded_file = st.sidebar.file_uploader("Choose CSV file", type="csv")
            if uploaded_file is not None:
                self.data = pd.read_csv(uploaded_file)
                st.session_state.data_source_type = "upload"
                st.success(f"✅ Uploaded file loaded! {len(self.data)} rows")
            else:
                st.info("👆 Please upload a CSV file to continue.")
                return False
        elif data_source == "scrape":
            return self.scrape_live_data()
        
        if hasattr(self, 'data') and self.data is not None:
            # Process the data using modular processor
            try:
                result = self.processor.process_all(self.data)
                self.data = result.cleaned_data
                self.growth_metrics = result.growth_metrics.__dict__ if result.growth_metrics else {}
                
                # Store in session state if scraped
                if data_source == "scrape":
                    st.session_state.scraped_data = self.data
                    st.session_state.scraped_growth_metrics = self.growth_metrics
                
                return True
            except Exception as e:
                st.error(f"❌ Error processing data: {e}")
                return False
        
        return False
    
    def get_cached_dropdown_data(self):
        """Get cached dropdown data or scrape if not available."""
        from datetime import datetime, timedelta
        
        # Check if we have valid cached data (less than 1 hour old)
        if (st.session_state.cached_dropdown_data is not None and 
            st.session_state.dropdown_cache_timestamp is not None):
            
            cache_age = datetime.now() - st.session_state.dropdown_cache_timestamp
            if cache_age < timedelta(hours=1):
                st.info("📋 Using cached dropdown data (refreshed within last hour)")
                return st.session_state.cached_dropdown_data
        
        # Need to scrape fresh dropdown data
        st.info("🔄 Fetching fresh dropdown data from VAHAN website...")
        
        try:
            scraper = VahanScraper()
            
            with st.spinner("🔄 Initializing browser and detecting dynamic elements..."):
                scraper.setup_driver(headless=True)
                scraper.open_page()
            
            with st.spinner("📋 Fetching available dropdown options..."):
                dropdown_data = scraper.scrape_dropdowns()
            
            scraper.close()
            
            if dropdown_data:
                # Cache the data
                st.session_state.cached_dropdown_data = dropdown_data
                st.session_state.dropdown_cache_timestamp = datetime.now()
                st.success("✅ Dropdown data cached successfully!")
                return dropdown_data
            else:
                st.error("❌ Failed to fetch dropdown options")
                return None
                
        except Exception as e:
            st.error(f"❌ Error fetching dropdown data: {e}")
            return None
    
    def scrape_live_data(self):
        """Scrape live data using the modular scraper."""
        st.info("🌐 Scraping live data from VAHAN website...")
        
        # Add cache refresh button
        col1, col2 = st.columns([3, 1])
        with col1:
            st.info("📋 Using cached dropdown data for better performance")
        with col2:
            if st.button("🔄 Refresh Cache"):
                st.session_state.cached_dropdown_data = None
                st.session_state.dropdown_cache_timestamp = None
                st.rerun()
        
        # Get cached dropdown data first
        dropdown_data = self.get_cached_dropdown_data()
        
        if not dropdown_data:
            return False
        
        try:
            # Display available options and get user selection
            st.success("✅ Successfully loaded dropdown options!")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("📍 Select States")
                available_states = dropdown_data.get('State', [])
                if available_states:
                    selected_states = st.multiselect(
                        "Choose states to scrape:",
                        available_states[:20],  # Limit for performance
                        default=available_states[:3] if len(available_states) >= 3 else available_states
                    )
                else:
                    st.error("No states available")
                    scraper.close()
                    return False
            
            with col2:
                st.subheader("📅 Select Years")
                available_years = dropdown_data.get('Year', [])
                if available_years:
                    selected_years = st.multiselect(
                        "Choose years to scrape:",
                        available_years,
                        default=available_years[-2:] if len(available_years) >= 2 else available_years
                    )
                else:
                    st.error("No years available")
                    scraper.close()
                    return False
            
            # Vehicle types (optional)
            st.subheader("🚗 Vehicle Types (Optional)")
            available_vehicle_types = dropdown_data.get('Vehicle Type', [])
            selected_vehicle_types = []
            if available_vehicle_types:
                selected_vehicle_types = st.multiselect(
                    "Choose vehicle types (leave empty for all):",
                    available_vehicle_types[:10],  # Limit for performance
                    default=[]
                )
            
            # Scraping configuration
            if st.button("🚀 Start Scraping", type="primary"):
                if not selected_states or not selected_years:
                    st.error("❌ Please select at least one state and one year")
                    return False
                
                # Create filter combinations as dictionaries (old scraper format)
                combinations = []
                for state in selected_states:
                    for year in selected_years:
                        if selected_vehicle_types:
                            for vehicle_type in selected_vehicle_types:
                                combinations.append({
                                    "State": state,
                                    "Year": year,
                                    "Vehicle Type": vehicle_type
                                })
                        else:
                            combinations.append({
                                "State": state,
                                "Year": year
                            })
                
                # Progress tracking
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                total_combinations = len(combinations)
                status_text.text(f"📊 Scraping {total_combinations} combinations...")
                
                try:
                    # Initialize new scraper for actual data scraping
                    with st.spinner("🔄 Initializing scraper for data collection..."):
                        scraper = VahanScraper()
                        scraper.setup_driver(headless=True)
                        scraper.open_page()
                    
                    # Scrape data
                    scraped_data = scraper.scrape_multiple_combinations(combinations)
                    
                    if not scraped_data.empty:
                        progress_bar.progress(90)
                        status_text.text("🔄 Processing scraped data...")
                        
                        # Process the scraped data like the old dashboard does
                        try:
                            # Clean the data using the data processor (this handles TOTAL column properly)
                            processed_data = self.processor.clean_data(scraped_data)
                            self.growth_metrics = self.processor.calculate_growth_metrics(processed_data)
                            
                            # Store processed data
                            self.data = processed_data
                            st.session_state.scraped_data = processed_data.copy()
                            st.session_state.scraped_growth_metrics = self.growth_metrics.copy()
                            st.session_state.data_source_type = "scrape"
                            
                            progress_bar.progress(100)
                            status_text.text("✅ Scraping and processing completed successfully!")
                            
                            st.success(f"🎉 Successfully scraped and processed {len(processed_data)} records!")
                            
                            # Show preview of processed data
                            with st.expander("📋 Preview Processed Data"):
                                st.dataframe(processed_data.head(10))
                                
                                # Show TOTAL column info for debugging
                                if 'TOTAL' in processed_data.columns:
                                    total_sum = processed_data['TOTAL'].sum()
                                    st.info(f"📊 Total registrations: {total_sum:,}")
                            
                            scraper.close()
                            return True
                            
                        except Exception as processing_error:
                            st.warning(f"⚠️ Data processing failed: {processing_error}")
                            # Fall back to raw data
                            self.data = scraped_data
                            st.session_state.scraped_data = scraped_data
                            st.session_state.data_source_type = "scrape"
                            
                            progress_bar.progress(100)
                            status_text.text("✅ Scraping completed (using raw data)")
                            st.success(f"🎉 Successfully scraped {len(scraped_data)} records (raw data)!")
                            
                            scraper.close()
                            return True
                    else:
                        st.error("❌ No data was scraped")
                        scraper.close()
                        return False
                        
                except Exception as e:
                    st.error(f"❌ Scraping failed: {e}")
                    try:
                        scraper.close()
                    except:
                        pass
                    return False
                finally:
                    # Always clean up scraper
                    try:
                        scraper.close()
                    except:
                        pass
            
            else:
                # Show configuration summary
                st.info(f"""
                **Scraping Configuration:**
                - States: {len(selected_states)} selected
                - Years: {len(selected_years)} selected  
                - Vehicle Types: {len(selected_vehicle_types) if selected_vehicle_types else 'All'}
                - Total Combinations: {len(selected_states) * len(selected_years) * (len(selected_vehicle_types) if selected_vehicle_types else 1)}
                
                Click "Start Scraping" to begin data collection.
                """)
                return False
                
        except Exception as e:
            st.error(f"❌ Failed to initialize scraper: {e}")
            return False
    
    def create_sidebar_filters(self):
        """Create sidebar filters using processed data."""
        st.sidebar.header("🔍 Data Filters")
        
        filters = {}
        
        # Data source indicator
        if st.session_state.data_source_type:
            source_type = st.session_state.data_source_type
            if source_type == "scrape":
                st.sidebar.success("🌐 Live Scraped Data")
                if st.sidebar.button("🗑️ Clear Scraped Data"):
                    st.session_state.scraped_data = None
                    st.session_state.scraped_growth_metrics = {}
                    st.session_state.data_source_type = None
                    st.experimental_rerun()
            elif source_type == "sample":
                st.sidebar.info("🧪 Sample Data")
            elif source_type == "upload":
                st.sidebar.info("📁 Uploaded Data")
        
        # Year filter
        if 'Year' in self.data.columns:
            years = sorted(self.data['Year'].dropna().unique())
            if years:
                filters['years'] = st.sidebar.multiselect(
                    "📅 Select Years",
                    years,
                    default=years
                )
        
        # State filter
        if 'State' in self.data.columns:
            states = sorted(self.data['State'].dropna().unique())
            if states:
                filters['states'] = st.sidebar.multiselect(
                    "📍 Select States",
                    states,
                    default=states[:10] if len(states) > 10 else states
                )
        
        # Vehicle category filter
        if 'Vehicle_Category' in self.data.columns:
            categories = sorted(self.data['Vehicle_Category'].dropna().unique())
            if categories:
                filters['categories'] = st.sidebar.multiselect(
                    "🚗 Select Vehicle Categories",
                    categories,
                    default=categories
                )
        
        return filters
    
    def apply_filters(self, filters):
        """Apply filters to the data."""
        filtered_data = self.data.copy()
        
        if 'years' in filters and filters['years']:
            filtered_data = filtered_data[filtered_data['Year'].isin(filters['years'])]
        
        if 'states' in filters and filters['states']:
            filtered_data = filtered_data[filtered_data['State'].isin(filters['states'])]
        
        if 'categories' in filters and filters['categories']:
            filtered_data = filtered_data[filtered_data['Vehicle_Category'].isin(filters['categories'])]
        
        return filtered_data
    
    def create_kpi_cards(self, data):
        """Create KPI cards using modular analytics."""
        st.subheader("📊 Key Performance Indicators")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            # Handle TOTAL column - data should already be processed by data processor
            if 'TOTAL' in data.columns:
                try:
                    # Data should already be cleaned by the data processor
                    total_registrations = int(data['TOTAL'].sum())
                    formatted_total = f"{total_registrations:,}"
                except Exception as e:
                    formatted_total = "Processing..."
            else:
                formatted_total = "0"
            
            st.metric(
                "Total Registrations",
                formatted_total,
                help="Total vehicle registrations in the dataset"
            )
        
        with col2:
            # Check for different state column names
            states_count = 0
            if 'State' in data.columns:
                states_count = data['State'].nunique()
            elif 'Filter_State' in data.columns:
                states_count = data['Filter_State'].nunique()
            
            st.metric(
                "States Covered",
                states_count,
                help="Number of unique states in the dataset"
            )
        
        with col3:
            # Calculate YoY growth if we have multiple years
            if 'Year' in data.columns and data['Year'].nunique() > 1:
                try:
                    # Calculate simple YoY growth
                    yearly_totals = data.groupby('Year')['TOTAL'].sum().sort_index()
                    if len(yearly_totals) >= 2:
                        latest_year = yearly_totals.index[-1]
                        previous_year = yearly_totals.index[-2]
                        growth_rate = ((yearly_totals[latest_year] - yearly_totals[previous_year]) / yearly_totals[previous_year]) * 100
                        st.metric(
                            "Avg YoY Growth",
                            f"{growth_rate:.1f}%",
                            help="Year-over-year growth rate"
                        )
                    else:
                        st.metric("Avg YoY Growth", "N/A", help="Insufficient data for growth calculation")
                except Exception:
                    st.metric("Avg YoY Growth", "N/A", help="Single year data")
        
        with col4:
            # Calculate data period
            if 'Year' in data.columns:
                years_span = data['Year'].nunique()
                min_year = data['Year'].min()
                max_year = data['Year'].max()
                if years_span > 1:
                    period_text = f"{years_span} years ({min_year}-{max_year})"
                else:
                    period_text = f"{min_year}"
                help_text = f"Data spans from {min_year} to {max_year}"
            elif 'Filter_Year' in data.columns:
                years_span = data['Filter_Year'].nunique()
                min_year = data['Filter_Year'].min()
                max_year = data['Filter_Year'].max()
                if years_span > 1:
                    period_text = f"{years_span} years ({min_year}-{max_year})"
                else:
                    period_text = f"{min_year}"
                help_text = f"Data spans from {min_year} to {max_year}"
            else:
                period_text = "Unknown"
                help_text = "Data period information not available"
            
            st.metric(
                "Data Period",
                period_text,
                help=help_text
            )
    
    def create_growth_charts(self, data):
        """Create growth charts using modular analytics."""
        st.header("📈 Growth Analysis")
        
        if 'Year' in data.columns and 'TOTAL' in data.columns:
            # Calculate CAGR using modular analyzer
            cagr = self.growth_analyzer.calculate_compound_growth_rate(data)
            
            col1, col2 = st.columns([2, 1])
            
            with col1:
                # Yearly trend chart
                yearly_data = data.groupby('Year')['TOTAL'].sum().reset_index()
                
                fig = px.line(yearly_data, x='Year', y='TOTAL',
                             title='Total Registrations Trend',
                             markers=True)
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.metric("CAGR", f"{cagr:.2f}%", help="Compound Annual Growth Rate")
                
                # Growth volatility analysis
                volatility = self.growth_analyzer.analyze_growth_volatility(data)
                if volatility and 'stability_score' in volatility:
                    st.metric("Stability", volatility['stability_score'])
        
        # Category-wise growth - use actual vehicle class names
        st.subheader("📊 Growth by Vehicle Category")
        
        # Check what columns are available for vehicle categorization
        vehicle_col = None
        year_col = None
        
        # Find the best column for vehicle categories
        if 'Vehicle_Category' in data.columns:
            vehicle_col = 'Vehicle_Category'
        elif 'Vehicle Class' in data.columns:
            vehicle_col = 'Vehicle Class'
        elif 'Vehicle_Type' in data.columns:
            vehicle_col = 'Vehicle_Type'
        
        # Find the best column for years
        if 'Year' in data.columns:
            year_col = 'Year'
        elif 'Filter_Year' in data.columns:
            year_col = 'Filter_Year'
        
        if vehicle_col and year_col and data[year_col].nunique() > 1:
            try:
                # Group by year and vehicle category
                category_yearly = data.groupby([year_col, vehicle_col])['TOTAL'].sum().reset_index()
                
                if not category_yearly.empty:
                    fig = px.line(category_yearly, x=year_col, y='TOTAL', 
                                 color=vehicle_col,
                                 title='Growth by Vehicle Category',
                                 markers=True)
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("📊 No data available for category-wise growth analysis")
            except Exception as e:
                st.warning(f"⚠️ Error creating category growth chart: {e}")
                st.info("📊 Available columns: " + ", ".join(data.columns.tolist()))
        elif vehicle_col and year_col:
            # Single year data - show current distribution
            st.info("📊 Single year data - showing current vehicle category distribution")
            category_totals = data.groupby(vehicle_col)['TOTAL'].sum().reset_index()
            category_totals = category_totals.sort_values('TOTAL', ascending=False)
            
            fig = px.bar(category_totals, x=vehicle_col, y='TOTAL',
                        title='Vehicle Category Distribution (Current Year)',
                        text='TOTAL')
            fig.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
            fig.update_layout(height=400, xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("⚠️ Cannot create growth chart: Missing vehicle category or year data")
            st.info(f"📊 Available columns: {', '.join(data.columns.tolist())}")
            if vehicle_col:
                st.info(f"✅ Vehicle column found: {vehicle_col}")
            if year_col:
                st.info(f"✅ Year column found: {year_col}")
            st.info(f"📅 Unique years: {data[year_col].nunique() if year_col else 'N/A'}")
    
    def create_market_share_analysis(self, data):
        """Create market share and composition analysis."""
        st.subheader("🥧 Market Composition & Share Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Market share by vehicle class
            if 'Vehicle Class' in data.columns:
                vehicle_totals = data.groupby('Vehicle Class')['TOTAL'].sum().reset_index()
                vehicle_totals = vehicle_totals.sort_values('TOTAL', ascending=False)
                
                fig_pie = px.pie(
                    vehicle_totals,
                    values='TOTAL',
                    names='Vehicle Class',
                    title='Market Share by Vehicle Class'
                )
                fig_pie.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig_pie, use_container_width=True)
            elif 'Vehicle_Category' in data.columns:
                category_totals = data.groupby('Vehicle_Category')['TOTAL'].sum().reset_index()
                category_totals = category_totals.sort_values('TOTAL', ascending=False)
                
                fig_pie = px.pie(
                    category_totals,
                    values='TOTAL',
                    names='Vehicle_Category',
                    title='Market Share by Vehicle Category'
                )
                fig_pie.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig_pie, use_container_width=True)
        
        with col2:
            # Top states by registrations
            if 'State' in data.columns:
                state_totals = data.groupby('State')['TOTAL'].sum().reset_index()
                state_totals = state_totals.sort_values('TOTAL', ascending=False).head(10)
                
                fig_states = px.bar(
                    state_totals,
                    x='TOTAL',
                    y='State',
                    orientation='h',
                    title='Top 10 States by Registrations',
                    text='TOTAL'
                )
                fig_states.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
                fig_states.update_layout(height=500)
                st.plotly_chart(fig_states, use_container_width=True)
            elif 'Filter_State' in data.columns:
                state_totals = data.groupby('Filter_State')['TOTAL'].sum().reset_index()
                state_totals = state_totals.sort_values('TOTAL', ascending=False).head(10)
                
                fig_states = px.bar(
                    state_totals,
                    x='TOTAL',
                    y='Filter_State',
                    orientation='h',
                    title='Top 10 States by Registrations',
                    text='TOTAL'
                )
                fig_states.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
                fig_states.update_layout(height=500)
                st.plotly_chart(fig_states, use_container_width=True)
    
    def create_time_series_analysis(self, data):
        """Create detailed time series analysis."""
        st.subheader("📅 Time Series Analysis")
        
        # Prioritize Vehicle Class over Vehicle_Category
        if 'Year' in data.columns and 'Vehicle Class' in data.columns:
            # Check if we have multiple years
            unique_years = data['Year'].nunique()
            if unique_years > 1:
                # Yearly trends by vehicle class
                yearly_vehicle = data.groupby(['Year', 'Vehicle Class'])['TOTAL'].sum().reset_index()
                
                fig_timeline = px.line(
                    yearly_vehicle,
                    x='Year',
                    y='TOTAL',
                    color='Vehicle Class',
                    title='Registration Trends by Vehicle Class Over Time',
                    markers=True
                )
                fig_timeline.update_layout(height=500)
                st.plotly_chart(fig_timeline, use_container_width=True)
                
                # Heatmap of registrations by year and vehicle class
                heatmap_data = yearly_vehicle.pivot(index='Vehicle Class', columns='Year', values='TOTAL')
                
                fig_heatmap = px.imshow(
                    heatmap_data,
                    title='Registration Intensity Heatmap (Vehicle Class vs Year)',
                    color_continuous_scale='Viridis',
                    aspect='auto'
                )
                fig_heatmap.update_layout(height=400)
                st.plotly_chart(fig_heatmap, use_container_width=True)
            else:
                # Single year - show vehicle class breakdown
                st.info("📊 Single year data - showing vehicle class distribution")
                vehicle_data = data.groupby('Vehicle Class')['TOTAL'].sum().reset_index()
                vehicle_data = vehicle_data.sort_values('TOTAL', ascending=False)
                
                fig_single = px.bar(
                    vehicle_data,
                    x='Vehicle Class',
                    y='TOTAL',
                    title='Vehicle Class Distribution (Current Year)',
                    text='TOTAL'
                )
                fig_single.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
                fig_single.update_layout(
                    height=400,
                    xaxis_tickangle=-45,
                    margin=dict(b=100)
                )
                st.plotly_chart(fig_single, use_container_width=True)
        
        elif 'Year' in data.columns and 'Vehicle_Category' in data.columns:
            # Fallback to Vehicle_Category if Vehicle Class not available
            yearly_category = data.groupby(['Year', 'Vehicle_Category'])['TOTAL'].sum().reset_index()
            
            fig_timeline = px.line(
                yearly_category,
                x='Year',
                y='TOTAL',
                color='Vehicle_Category',
                title='Registration Trends by Category Over Time',
                markers=True
            )
            fig_timeline.update_layout(height=500)
            st.plotly_chart(fig_timeline, use_container_width=True)
        
        elif 'Filter_Year' in data.columns:
            # Handle scraped data with Filter_Year column
            if 'Vehicle Class' in data.columns:
                yearly_vehicle = data.groupby(['Filter_Year', 'Vehicle Class'])['TOTAL'].sum().reset_index()
                
                fig_timeline = px.line(
                    yearly_vehicle,
                    x='Filter_Year',
                    y='TOTAL',
                    color='Vehicle Class',
                    title='Registration Trends by Vehicle Class Over Time',
                    markers=True
                )
                fig_timeline.update_layout(height=500)
                st.plotly_chart(fig_timeline, use_container_width=True)
    
    def create_manufacturer_analysis(self, data):
        """Create manufacturer/vehicle class analysis."""
        st.subheader("🏭 Manufacturer & Vehicle Class Analysis")
        
        if 'Vehicle Class' in data.columns:
            # Top manufacturers
            manufacturer_totals = data.groupby('Vehicle Class')['TOTAL'].sum().reset_index()
            manufacturer_totals = manufacturer_totals.sort_values('TOTAL', ascending=False).head(15)
            
            col1, col2 = st.columns(2)
            
            with col1:
                fig_manufacturers = px.bar(
                    manufacturer_totals,
                    x='TOTAL',
                    y='Vehicle Class',
                    orientation='h',
                    title='Top 15 Vehicle Classes by Registrations',
                    text='TOTAL'
                )
                fig_manufacturers.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
                fig_manufacturers.update_layout(height=600)
                st.plotly_chart(fig_manufacturers, use_container_width=True)
            
            with col2:
                # Show data table
                st.subheader("📊 Data Table")
                display_data = manufacturer_totals.head(10)
                st.dataframe(display_data, use_container_width=True)
    
    def create_comparison_tool(self, data):
        """Create interactive comparison tool."""
        st.subheader("🔄 Interactive Comparison Tool")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Compare States**")
            if 'State' in data.columns:
                states_to_compare = st.multiselect(
                    "Select states to compare",
                    options=sorted(data['State'].unique()),
                    default=sorted(data['State'].unique())[:3]
                )
                
                if states_to_compare:
                    comparison_data = data[data['State'].isin(states_to_compare)]
                    if 'Year' in data.columns:
                        yearly_comparison = comparison_data.groupby(['Year', 'State'])['TOTAL'].sum().reset_index()
                        
                        fig_compare = px.line(
                            yearly_comparison,
                            x='Year',
                            y='TOTAL',
                            color='State',
                            title='State-wise Registration Trends',
                            markers=True
                        )
                        fig_compare.update_layout(height=400)
                        st.plotly_chart(fig_compare, use_container_width=True)
            elif 'Filter_State' in data.columns:
                states_to_compare = st.multiselect(
                    "Select states to compare",
                    options=sorted(data['Filter_State'].unique()),
                    default=sorted(data['Filter_State'].unique())[:3]
                )
                
                if states_to_compare:
                    comparison_data = data[data['Filter_State'].isin(states_to_compare)]
                    state_totals = comparison_data.groupby('Filter_State')['TOTAL'].sum().reset_index()
                    
                    fig_compare = px.bar(
                        state_totals,
                        x='Filter_State',
                        y='TOTAL',
                        title='State-wise Registration Comparison',
                        text='TOTAL'
                    )
                    fig_compare.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
                    fig_compare.update_layout(height=400)
                    st.plotly_chart(fig_compare, use_container_width=True)
        
        with col2:
            st.markdown("**Compare Vehicle Classes**")
            if 'Vehicle Class' in data.columns:
                vehicle_classes_to_compare = st.multiselect(
                    "Select vehicle classes to compare",
                    options=sorted(data['Vehicle Class'].unique()),
                    default=sorted(data['Vehicle Class'].unique())[:3] if len(data['Vehicle Class'].unique()) >= 3 else sorted(data['Vehicle Class'].unique())
                )
                
                if vehicle_classes_to_compare:
                    vehicle_comparison = data[data['Vehicle Class'].isin(vehicle_classes_to_compare)]
                    vehicle_totals = vehicle_comparison.groupby('Vehicle Class')['TOTAL'].sum().reset_index()
                    vehicle_totals = vehicle_totals.sort_values('TOTAL', ascending=False)
                    
                    fig_vehicle_bar = px.bar(
                        vehicle_totals,
                        x='Vehicle Class',
                        y='TOTAL',
                        title='Vehicle Class Comparison',
                        text='TOTAL'
                    )
                    fig_vehicle_bar.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
                    fig_vehicle_bar.update_layout(
                        height=400,
                        xaxis_tickangle=-45,
                        margin=dict(b=100)
                    )
                    st.plotly_chart(fig_vehicle_bar, use_container_width=True)
    
    def create_investor_insights(self, data):
        """Generate and display investor-focused insights."""
        st.subheader("💡 Investor Insights & Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### 📊 Market Overview")
            total_registrations = data['TOTAL'].sum() if 'TOTAL' in data.columns else 0
            
            # Market insights
            if 'Vehicle Class' in data.columns:
                top_vehicle = data.groupby('Vehicle Class')['TOTAL'].sum().idxmax()
                st.info(f"🚗 **Top Vehicle Class**: {top_vehicle}")
            
            if 'State' in data.columns or 'Filter_State' in data.columns:
                state_col = 'State' if 'State' in data.columns else 'Filter_State'
                top_state = data.groupby(state_col)['TOTAL'].sum().idxmax()
                st.info(f"🏆 **Top State**: {top_state}")
            
            # Ensure it's numeric before formatting
            try:
                total_registrations = int(float(total_registrations))
                formatted_total = f"{total_registrations:,}"
            except (ValueError, TypeError):
                formatted_total = str(total_registrations)
            
            st.metric("Total Market Size", formatted_total)
        
        with col2:
            st.markdown("### 🎯 Key Insights")
            
            # Growth insights
            if 'Year' in data.columns and data['Year'].nunique() > 1:
                yearly_totals = data.groupby('Year')['TOTAL'].sum().sort_index()
                if len(yearly_totals) >= 2:
                    latest_growth = ((yearly_totals.iloc[-1] - yearly_totals.iloc[-2]) / yearly_totals.iloc[-2]) * 100
                    if latest_growth > 0:
                        st.success(f"📈 **Growth**: +{latest_growth:.1f}% YoY")
                    else:
                        st.warning(f"📉 **Decline**: {latest_growth:.1f}% YoY")
            
            # Market concentration
            if 'Vehicle Class' in data.columns:
                vehicle_totals = data.groupby('Vehicle Class')['TOTAL'].sum().sort_values(ascending=False)
                top_3_share = vehicle_totals.head(3).sum() / vehicle_totals.sum() * 100
                st.info(f"🎯 **Top 3 Classes**: {top_3_share:.1f}% market share")
    
    def create_export_section(self, data):
        """Create data export functionality."""
        st.subheader("📥 Export & Download")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("📊 Export Processed Data"):
                csv_data = data.to_csv(index=False)
                st.download_button(
                    label="Download Processed Data CSV",
                    data=csv_data,
                    file_name=f"vahan_processed_data_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv"
                )
        
        with col2:
            if st.button("📈 Export Summary Report"):
                # Create summary report
                summary_text = f"""
VAHAN Vehicle Registration Analysis Report
Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

SUMMARY STATISTICS:
- Total Records: {len(data):,}
- Total Registrations: {data['TOTAL'].sum():,}
- Data Period: {data['Year'].min() if 'Year' in data.columns else 'N/A'} - {data['Year'].max() if 'Year' in data.columns else 'N/A'}

TOP PERFORMERS:
"""
                if 'Vehicle Class' in data.columns:
                    top_vehicles = data.groupby('Vehicle Class')['TOTAL'].sum().sort_values(ascending=False).head(5)
                    summary_text += "\nTop Vehicle Classes:\n"
                    for vehicle, total in top_vehicles.items():
                        summary_text += f"- {vehicle}: {total:,}\n"
                
                st.download_button(
                    label="Download Summary Report",
                    data=summary_text,
                    file_name=f"vahan_summary_report_{datetime.now().strftime('%Y%m%d')}.txt",
                    mime="text/plain"
                )
        
        with col3:
            st.info("💡 **Export Options**\n\nDownload processed data and insights for further analysis in Excel or other tools.")

def main():
    """Main dashboard application."""
    st.markdown('<h1 class="main-header">🚗 VAHAN Vehicle Registration Analytics</h1>', 
                unsafe_allow_html=True)
    
    # Initialize dashboard
    dashboard = ModularVahanDashboard()
    
    # Sidebar for data source selection
    st.sidebar.title("📊 Data Management")
    
    data_source = st.sidebar.radio(
        "Select Data Source",
        ["Use Sample Data", "Upload CSV File", "🌐 Live Scraping"]
    )
    
    # Map data source to parameter
    if data_source == "Use Sample Data":
        source_param = "sample"
    elif data_source == "Upload CSV File":
        source_param = "upload"
    else:  # Live Scraping
        source_param = "scrape"
    
    # Load data
    if dashboard.load_data(data_source=source_param):
        # Create filters
        filters = dashboard.create_sidebar_filters()
        
        # Apply filters
        filtered_data = dashboard.apply_filters(filters)
        
        if len(filtered_data) > 0:
            # Main dashboard tabs - exactly like the old working dashboard
            tab1, tab2, tab3, tab4, tab5 = st.tabs([
                "📊 Overview", "📈 Growth Analysis", "🥧 Market Share", 
                "🔄 Comparisons", "💡 Insights"
            ])
            
            with tab1:
                st.header("📊 Dashboard Overview")
                dashboard.create_kpi_cards(filtered_data)
                st.markdown("---")
                dashboard.create_time_series_analysis(filtered_data)
            
            with tab2:
                dashboard.create_growth_charts(filtered_data)
            
            with tab3:
                dashboard.create_market_share_analysis(filtered_data)
                st.markdown("---")
                dashboard.create_manufacturer_analysis(filtered_data)
            
            with tab4:
                dashboard.create_comparison_tool(filtered_data)
            
            with tab5:
                dashboard.create_investor_insights(filtered_data)
                st.markdown("---")
                dashboard.create_export_section(filtered_data)
            
            # Footer with data info - exactly like old dashboard
            st.markdown("---")
            st.markdown(f"""
            <div style="text-align: center; color: #666; font-size: 0.9em;">
            Dashboard showing {len(filtered_data):,} records | 
            Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | 
            Data period: {filtered_data['Year'].min() if 'Year' in filtered_data.columns else 'N/A'} - 
            {filtered_data['Year'].max() if 'Year' in filtered_data.columns else 'N/A'}
            </div>
            """, unsafe_allow_html=True)
            
            # Footer with data info
            st.markdown("---")
            st.markdown(f"""
            <div style="text-align: center; color: #666; font-size: 0.9em;">
            Dashboard showing {len(filtered_data):,} records | 
            Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | 
            Data period: {filtered_data['Year'].min() if 'Year' in filtered_data.columns else 'N/A'} - 
            {filtered_data['Year'].max() if 'Year' in filtered_data.columns else 'N/A'}
            </div>
            """, unsafe_allow_html=True)
        
        else:
            st.error("⚠️ No data available with current filters. Please adjust your filter selections.")
    
    else:
        st.info("👆 Please load data using the sidebar controls to begin analysis.")
        
        # Show instructions
        st.markdown("""
        ### 📋 Getting Started with Modular VAHAN Analytics
        
        1. **Data Loading**: Choose from three options:
           - **Sample Data**: Use pre-loaded demo data for testing
           - **Upload CSV**: Upload your own VAHAN data file
           - **🌐 Live Scraping**: Scrape real-time data directly from VAHAN website
        2. **Configure Scraping** (if using Live Scraping): Select states, years, and data types
        3. **Apply Filters**: Use the sidebar to filter by year, state, and vehicle category
        4. **Explore Tabs**: Navigate through different analysis views
        
        ### 🏗️ New Modular Architecture Features
        - **Improved Performance**: Optimized data processing pipeline
        - **Better Error Handling**: Comprehensive exception management
        - **Enhanced Analytics**: Advanced growth analysis and insights
        - **Modular Design**: Clean separation of concerns for maintainability
        - **Comprehensive Logging**: Detailed operation tracking
        """)

if __name__ == "__main__":
    main()
