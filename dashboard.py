"""
Interactive Streamlit Dashboard for VAHAN Vehicle Registration Data
Investor-focused analytics with YoY/QoQ growth metrics and trends.
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Import our custom modules
from data_processor import VahanDataProcessor, create_sample_data
from vahan_data_extractor import VahanScraper

# Page configuration
st.set_page_config(
    page_title="VAHAN Vehicle Registration Analytics",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="expanded"
)

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

class VahanDashboard:
    def __init__(self):
        self.processor = VahanDataProcessor()
        self.data = None
        self.growth_metrics = {}
        
        # Initialize session state for data persistence
        if 'scraped_data' not in st.session_state:
            st.session_state.scraped_data = None
        if 'scraped_growth_metrics' not in st.session_state:
            st.session_state.scraped_growth_metrics = {}
        if 'data_source_type' not in st.session_state:
            st.session_state.data_source_type = None

    def load_data(self, data_source="sample"):
        """Load data for the dashboard."""
        # Check if we have persisted scraped data and the source matches
        if (data_source == "scrape" and 
            st.session_state.scraped_data is not None and 
            st.session_state.data_source_type == "scrape"):
            # Use persisted scraped data
            self.data = st.session_state.scraped_data
            self.growth_metrics = st.session_state.scraped_growth_metrics
            st.success("✅ Using previously scraped data!")
            return True
        
        if data_source == "sample":
            # Use sample data
            from data_processor import create_sample_data
            self.data = create_sample_data()
            st.session_state.data_source_type = "sample"
            st.success("✅ Sample data loaded successfully!")
        elif data_source == "upload":
            # File upload option
            uploaded_file = st.sidebar.file_uploader("Choose CSV file", type="csv")
            if uploaded_file is not None:
                self.data = pd.read_csv(uploaded_file)
                st.session_state.data_source_type = "upload"
                st.success(f"✅ Uploaded file loaded! {len(self.data)} rows")
            else:
                st.info("👆 Please upload a CSV file to continue.")
                return False
        elif data_source == "scrape":
            # Live scraping
            return self.scrape_live_data()
        
        if self.data is not None:
            # Process the data
            self.data = self.processor.clean_data(self.data)
            self.growth_metrics = self.processor.calculate_growth_metrics(self.data)
            return True
        return False
    
    def scrape_live_data(self):
        """Scrape live data from VAHAN website."""
        st.info("🌐 Scraping live data from VAHAN website...")
        
        # Initialize session state for dropdown options
        if 'dropdown_options' not in st.session_state:
            st.session_state.dropdown_options = None
        
        # Step 1: Fetch available dropdown options
        if st.session_state.dropdown_options is None:
            if st.button("🔍 Fetch Available Options", type="primary"):
                with st.spinner("🔄 Connecting to VAHAN and fetching dropdown options..."):
                    try:
                        # Initialize scraper to get dropdown options
                        URL = "https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml"
                        scraper = VahanScraper(URL, wait_time=20)
                        scraper.setup_driver(headless=True)
                        scraper.open_page()
                        
                        # Get dropdown options
                        dropdown_data = scraper.scrape_dropdowns()
                        scraper.close()
                        
                        # Store in session state
                        st.session_state.dropdown_options = dropdown_data
                        
                        st.success("✅ Successfully fetched dropdown options from VAHAN!")
                        st.rerun()
                        
                    except Exception as e:
                        st.error(f"❌ Failed to fetch dropdown options: {str(e)}")
                        return False
            
            else:
                st.info("👆 Click 'Fetch Available Options' to get the latest dropdown values from VAHAN website")
                return False
        
        # Step 2: Show available options and configuration interface
        dropdown_data = st.session_state.dropdown_options
        
        # Display available options summary
        with st.expander("📋 Available Options from VAHAN", expanded=False):
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if 'State' in dropdown_data:
                    st.write(f"**States Available:** {len(dropdown_data['State'])}")
                    with st.container():
                        st.write("All available states:")
                        for state in dropdown_data['State']:
                            st.write(f"• {state}")
            
            with col2:
                if 'Year' in dropdown_data:
                    st.write(f"**Years Available:** {len(dropdown_data['Year'])}")
                    st.write("All available years:")
                    for year in dropdown_data['Year']:
                        st.write(f"• {year}")
            
            with col3:
                if 'Vehicle Type' in dropdown_data:
                    st.write(f"**Vehicle Types:** {len(dropdown_data['Vehicle Type'])}")
                    with st.container():
                        st.write("All available vehicle types:")
                        for vtype in dropdown_data['Vehicle Type']:
                            st.write(f"• {vtype}")
        
        # Step 3: Create scraping configuration interface with real options
        with st.expander("🔧 Scraping Configuration", expanded=True):
            col1, col2 = st.columns(2)
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.subheader("📍 Geographic Selection")
                # Use actual states from VAHAN
                available_states = dropdown_data.get('State', [])
                if available_states:
                    selected_states = st.multiselect(
                        "Select States to Scrape",
                        options=available_states,
                        default=available_states[:3] if len(available_states) >= 3 else available_states,
                        help="Choose states for data collection"
                    )
                else:
                    st.warning("⚠️ No states found in dropdown data")
                    selected_states = []
            
            with col2:
                st.subheader("📅 Time Period")
                # Use actual years from VAHAN
                available_years = dropdown_data.get('Year', [])
                if available_years:
                    selected_years = st.multiselect(
                        "Select Years",
                        options=available_years,
                        default=available_years[-2:] if len(available_years) >= 2 else available_years,
                        help="Choose years for data collection"
                    )
                else:
                    st.warning("⚠️ No years found in dropdown data")
                    selected_years = []
            
            with col3:
                st.subheader("🚗 Vehicle Types")
                # Use actual vehicle types from VAHAN
                available_vehicle_types = dropdown_data.get('Vehicle Type', [])
                if available_vehicle_types:
                    selected_vehicle_types = st.multiselect(
                        "Select Vehicle Types",
                        options=available_vehicle_types,
                        default=available_vehicle_types[:3] if len(available_vehicle_types) >= 3 else available_vehicle_types,
                        help="Choose specific vehicle types to scrape"
                    )
                else:
                    st.warning("⚠️ No vehicle types found in dropdown data")
                    selected_vehicle_types = []
            
            # Additional options row
            st.subheader("⚙️ Scraping Options")
            col_opt1, col_opt2 = st.columns(2)
            
            with col_opt1:
                headless_mode = st.checkbox(
                    "Run in Background (Headless)", 
                    value=True,
                    help="Run browser in background without GUI"
                )
                
                # Y-Axis selection for data type
                y_axis_options = dropdown_data.get('Y-Axis', [])
                if y_axis_options:
                    selected_y_axis = st.selectbox(
                        "Data Analysis Type (Y-Axis)",
                        options=y_axis_options,
                        help="Choose what type of data to analyze"
                    )
                else:
                    selected_y_axis = "Manufacturer"
            
            with col_opt2:
                # X-Axis selection
                x_axis_options = dropdown_data.get('X-Axis', [])
                if x_axis_options:
                    selected_x_axis = st.selectbox(
                        "Data Grouping (X-Axis)",
                        options=x_axis_options,
                        help="Choose how to group the data"
                    )
                else:
                    selected_x_axis = "State"
                
                # Reset options button
                if st.button("🔄 Refresh Options", help="Fetch latest options from VAHAN"):
                    st.session_state.dropdown_options = None
                    st.rerun()
        
        # Start scraping button
        if st.button("🚀 Start Live Scraping", type="primary"):
            if not selected_states or not selected_years:
                st.error("⚠️ Please select at least one state and one year.")
                return False
            
            # Initialize progress tracking
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            try:
                # Initialize scraper
                status_text.text("🔧 Initializing scraper...")
                URL = "https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml"
                scraper = VahanScraper(URL, wait_time=20)
                scraper.setup_driver(headless=headless_mode)
                progress_bar.progress(10)
                
                # Open VAHAN page
                status_text.text("🌐 Opening VAHAN dashboard...")
                scraper.open_page()
                progress_bar.progress(20)
                
                all_scraped_data = []
                total_combinations = len(selected_states) * len(selected_years) * len(selected_vehicle_types) if selected_vehicle_types else len(selected_states) * len(selected_years)
                current_combination = 0
                
                # Direct scraping approach - iterate through each combination
                for state in selected_states:
                    for year in selected_years:
                        # If vehicle types are selected, scrape for each type
                        if selected_vehicle_types:
                            for vehicle_type in selected_vehicle_types:
                                current_combination += 1
                                progress = 20 + (current_combination / total_combinations) * 70
                                progress_bar.progress(int(progress))
                                
                                status_text.text(f"🔄 Scraping {state} - {year} - {vehicle_type} ({current_combination}/{total_combinations})")
                                
                                try:
                                    # Apply filters for this specific combination
                                    filters = {
                                        "State": state,
                                        "Year": year,
                                        "Vehicle Type": vehicle_type,
                                        "Y-Axis": selected_y_axis,
                                        "X-Axis": selected_x_axis
                                    }
                                    
                                    scraper.apply_filters(filters)
                                    scraper._click_refresh_button()
                                    
                                    # Wait for data to load
                                    import time
                                    time.sleep(3)
                                    
                                    # Fetch the data
                                    data_dict = scraper.fetch_data()
                                    
                                    # Check if data_dict is a dictionary and has the expected structure
                                    if isinstance(data_dict, dict) and data_dict.get('status') == 'success' and data_dict.get('rows'):
                                        # Convert dictionary to DataFrame
                                        headers = data_dict.get('headers', [])
                                        rows = data_dict.get('rows', [])
                                        
                                        if headers and rows:
                                            try:
                                                data = pd.DataFrame(rows, columns=headers)
                                                
                                                # Add metadata
                                                data['Filter_State'] = state
                                                data['Filter_Year'] = year
                                                data['Filter_Vehicle_Type'] = vehicle_type
                                                data['Y_Axis_Type'] = selected_y_axis
                                                data['X_Axis_Type'] = selected_x_axis
                                                all_scraped_data.append(data)
                                                
                                                st.success(f"✅ Scraped {len(data)} records for {state}-{year}-{vehicle_type}")
                                            except Exception as df_error:
                                                st.warning(f"⚠️ Error creating DataFrame for {state}-{year}-{vehicle_type}: {str(df_error)}")
                                        else:
                                            st.warning(f"⚠️ No valid data structure for {state}-{year}-{vehicle_type}")
                                    else:
                                        st.warning(f"⚠️ No data found for {state}-{year}-{vehicle_type}")
                                        
                                except Exception as e:
                                    st.warning(f"⚠️ Failed to scrape {state}-{year}-{vehicle_type}: {str(e)}")
                                    continue
                        else:
                            # Scrape without vehicle type filter
                            current_combination += 1
                            progress = 20 + (current_combination / total_combinations) * 70
                            progress_bar.progress(int(progress))
                            
                            status_text.text(f"🔄 Scraping {state} - {year} ({current_combination}/{total_combinations})")
                            
                            try:
                                # Apply filters for this specific combination
                                filters = {
                                    "State": state,
                                    "Year": year,
                                    "Y-Axis": selected_y_axis,
                                    "X-Axis": selected_x_axis
                                }
                                
                                scraper.apply_filters(filters)
                                scraper._click_refresh_button()
                                
                                # Wait for data to load
                                import time
                                time.sleep(3)
                                
                                # Fetch the data
                                data_dict = scraper.fetch_data()
                                
                                # Check if data_dict is a dictionary and has the expected structure
                                if isinstance(data_dict, dict) and data_dict.get('status') == 'success' and data_dict.get('rows'):
                                    # Convert dictionary to DataFrame
                                    headers = data_dict.get('headers', [])
                                    rows = data_dict.get('rows', [])
                                    
                                    if headers and rows:
                                        try:
                                            data = pd.DataFrame(rows, columns=headers)
                                            
                                            # Add metadata
                                            data['Filter_State'] = state
                                            data['Filter_Year'] = year
                                            data['Filter_Vehicle_Type'] = 'All'
                                            data['Y_Axis_Type'] = selected_y_axis
                                            data['X_Axis_Type'] = selected_x_axis
                                            all_scraped_data.append(data)
                                            
                                            st.success(f"✅ Scraped {len(data)} records for {state}-{year}")
                                        except Exception as df_error:
                                            st.warning(f"⚠️ Error creating DataFrame for {state}-{year}: {str(df_error)}")
                                    else:
                                        st.warning(f"⚠️ No valid data structure for {state}-{year}")
                                else:
                                    st.warning(f"⚠️ No data found for {state}-{year}")
                                    
                            except Exception as e:
                                st.warning(f"⚠️ Failed to scrape {state}-{year}: {str(e)}")
                                continue
                
                # Close scraper first
                scraper.close()
                
                # Combine all scraped data
                if all_scraped_data:
                    status_text.text("🔄 Processing scraped data...")
                    try:
                        self.data = pd.concat(all_scraped_data, ignore_index=True)
                        
                        # Add scraping metadata
                        self.data['Scraped_Timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        self.data['Data_Source'] = 'Live_Scrape'
                        
                        progress_bar.progress(100)
                        status_text.text("✅ Scraping completed successfully!")
                        
                        # Display scraping summary
                        st.success(f"🎉 Successfully scraped {len(self.data)} records from VAHAN!")
                        
                        with st.expander("📊 Scraping Summary", expanded=True):
                            col1, col2, col3 = st.columns(3)
                            
                            with col1:
                                st.metric("Total Records", f"{len(self.data):,}")
                                if 'Filter_State' in self.data.columns:
                                    st.metric("States Covered", self.data['Filter_State'].nunique())
                            
                            with col2:
                                if 'Filter_Year' in self.data.columns:
                                    st.metric("Years Covered", self.data['Filter_Year'].nunique())
                                if 'Filter_Vehicle_Type' in self.data.columns:
                                    st.metric("Vehicle Types", self.data['Filter_Vehicle_Type'].nunique())
                            
                            with col3:
                                if 'TOTAL' in self.data.columns:
                                    try:
                                        # Ensure TOTAL column is numeric before summing
                                        total_col = pd.to_numeric(self.data['TOTAL'], errors='coerce').fillna(0)
                                        total_registrations = int(total_col.sum())
                                        st.metric("Total Registrations", f"{total_registrations:,}")
                                    except Exception as metric_error:
                                        st.metric("Total Registrations", "Processing...")
                        
                        # Process the scraped data for dashboard use
                        self.data = self.processor.clean_data(self.data)
                        self.growth_metrics = self.processor.calculate_growth_metrics(self.data)
                        
                        # Persist scraped data in session state
                        st.session_state.scraped_data = self.data.copy()
                        st.session_state.scraped_growth_metrics = self.growth_metrics.copy()
                        st.session_state.data_source_type = "scrape"
                        
                        st.info("🎯 Data is ready! You can now use the dashboard analytics with your scraped data.")
                        st.success("💾 Data has been saved and will persist until you scrape new data or refresh the page.")
                        return True
                        
                    except Exception as processing_error:
                        st.error(f"❌ Error processing scraped data: {str(processing_error)}")
                        # Still return True if we have some data, even if processing failed
                        if len(all_scraped_data) > 0:
                            st.warning("⚠️ Using raw scraped data without full processing.")
                            try:
                                # Combine all scraped data as fallback
                                self.data = pd.concat(all_scraped_data, ignore_index=True)
                                
                                # Basic cleaning for raw data
                                if 'TOTAL' in self.data.columns:
                                    self.data['TOTAL'] = pd.to_numeric(self.data['TOTAL'], errors='coerce').fillna(0)
                                
                                # Add basic metadata
                                self.data['Scraped_Timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                self.data['Data_Source'] = 'Live_Scrape_Raw'
                                
                                # Persist raw data in session state
                                st.session_state.scraped_data = self.data.copy()
                                st.session_state.scraped_growth_metrics = {}
                                st.session_state.data_source_type = "scrape"
                                
                                st.info("📊 Raw data is available for basic analysis.")
                                return True
                            except Exception as fallback_error:
                                st.error(f"❌ Error even with raw data: {str(fallback_error)}")
                                return False
                        return False
                    
                else:
                    st.error("❌ No data was scraped. Please check your selections and try again.")
                    st.info("💡 Try selecting different states, years, or vehicle types that might have data available.")
                    return False
                    
            except Exception as e:
                st.error(f"❌ Scraping failed: {str(e)}")
                try:
                    scraper.close()
                except:
                    pass
                return False
        
        return False
    
    def create_sidebar_filters(self):
        """Create sidebar filters for the dashboard."""
        st.sidebar.header("🔍 Filters & Controls")
        
        filters = {}
        
        if self.data is not None:
            # Year filter
            if 'Year' in self.data.columns:
                years = sorted(self.data['Year'].dropna().unique())
                filters['years'] = st.sidebar.multiselect(
                    "Select Years",
                    options=years,
                    default=years[-2:] if len(years) >= 2 else years
                )
            
            # State filter
            if 'State' in self.data.columns:
                states = sorted(self.data['State'].dropna().unique())
                filters['states'] = st.sidebar.multiselect(
                    "Select States",
                    options=states,
                    default=states[:5] if len(states) > 5 else states
                )
            
            # Vehicle class filter - prioritize Vehicle Class over Vehicle_Category
            if 'Vehicle Class' in self.data.columns:
                vehicle_classes = sorted(self.data['Vehicle Class'].dropna().unique())
                filters['vehicle_classes'] = st.sidebar.multiselect(
                    "Select Vehicle Classes",
                    options=vehicle_classes,
                    default=vehicle_classes
                )
            elif 'Vehicle_Category' in self.data.columns:
                categories = sorted(self.data['Vehicle_Category'].dropna().unique())
                filters['categories'] = st.sidebar.multiselect(
                    "Select Vehicle Categories",
                    options=categories,
                    default=categories
                )
            
            # Date range selector
            st.sidebar.subheader("📅 Analysis Period")
            analysis_type = st.sidebar.radio(
                "Growth Analysis Type",
                ["Year-over-Year (YoY)", "Quarter-over-Quarter (QoQ)", "Both"]
            )
            filters['analysis_type'] = analysis_type
        
        return filters
    
    def apply_filters(self, filters):
        """Apply filters to the data."""
        filtered_data = self.data.copy()
        
        if filters.get('years'):
            filtered_data = filtered_data[filtered_data['Year'].isin(filters['years'])]
        
        if filters.get('states'):
            filtered_data = filtered_data[filtered_data['State'].isin(filters['states'])]
        
        if filters.get('vehicle_classes'):
            filtered_data = filtered_data[filtered_data['Vehicle Class'].isin(filters['vehicle_classes'])]
        elif filters.get('categories'):
            filtered_data = filtered_data[filtered_data['Vehicle_Category'].isin(filters['categories'])]
        
        return filtered_data
    
    def create_kpi_cards(self, data):
        """Create KPI cards for key metrics."""
        col1, col2, col3, col4 = st.columns(4)
        
        # Ensure TOTAL column is numeric
        try:
            if 'TOTAL' in data.columns:
                # Convert TOTAL column to numeric, handling strings and commas
                data_copy = data.copy()
                data_copy['TOTAL'] = pd.to_numeric(
                    data_copy['TOTAL'].astype(str).str.replace(',', '').str.replace('-', '0'), 
                    errors='coerce'
                ).fillna(0)
                total_registrations = data_copy['TOTAL'].sum()
            else:
                total_registrations = 0
        except Exception as e:
            st.warning(f"⚠️ Error calculating total registrations: {str(e)}")
            total_registrations = 0
        
        avg_annual_growth = 0
        top_state = "N/A"
        top_category = "N/A"
        
        # Calculate average growth if we have growth metrics
        if self.growth_metrics and 'yoy_total' in self.growth_metrics:
            yoy_data = self.growth_metrics['yoy_total']
            if not yoy_data.empty and 'YoY_Growth_Rate' in yoy_data.columns:
                avg_annual_growth = yoy_data['YoY_Growth_Rate'].mean()
        
        # Get top performing state and category
        if 'State' in data.columns:
            try:
                state_totals = data_copy.groupby('State')['TOTAL'].sum() if 'data_copy' in locals() else data.groupby('State')['TOTAL'].sum()
                if not state_totals.empty:
                    top_state = state_totals.idxmax()
            except:
                top_state = "N/A"
        
        # Prioritize Vehicle Class over Vehicle_Category
        if 'Vehicle Class' in data.columns:
            try:
                vehicle_totals = data_copy.groupby('Vehicle Class')['TOTAL'].sum() if 'data_copy' in locals() else data.groupby('Vehicle Class')['TOTAL'].sum()
                if not vehicle_totals.empty:
                    top_category = vehicle_totals.idxmax()
            except:
                top_category = "N/A"
        elif 'Vehicle_Category' in data.columns:
            try:
                category_totals = data_copy.groupby('Vehicle_Category')['TOTAL'].sum() if 'data_copy' in locals() else data.groupby('Vehicle_Category')['TOTAL'].sum()
                if not category_totals.empty:
                    top_category = category_totals.idxmax()
            except:
                top_category = "N/A"
        
        with col1:
            st.metric(
                label="📊 Total Registrations",
                value=f"{int(total_registrations):,}" if total_registrations > 0 else "0",
                delta=None
            )
        
        with col2:
            growth_color = "normal" if pd.isna(avg_annual_growth) else ("inverse" if avg_annual_growth < 0 else "normal")
            st.metric(
                label="📈 Avg YoY Growth",
                value=f"{avg_annual_growth:.1f}%" if not pd.isna(avg_annual_growth) else "N/A",
                delta=None
            )
        
        with col3:
            st.metric(
                label="🏆 Top State",
                value=top_state,
                delta=None
            )
        
        with col4:
            st.metric(
                label="🚗 Leading Category",
                value=top_category,
                delta=None
            )
    
    def create_growth_charts(self, data):
        """Create growth trend charts."""
        st.subheader("📈 Growth Trends Analysis")
        
        # Check if we have growth data
        has_growth_data = (self.growth_metrics and 
                          'yoy_total' in self.growth_metrics and 
                          not self.growth_metrics['yoy_total'].empty)
        
        if has_growth_data:
            yoy_data = self.growth_metrics['yoy_total']
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**Year-over-Year Total Growth**")
                fig_yoy = px.line(
                    yoy_data, 
                    x='Year', 
                    y='YoY_Growth_Rate',
                    title='YoY Growth Rate (%)',
                    markers=True
                )
                fig_yoy.add_hline(y=0, line_dash="dash", line_color="red")
                fig_yoy.update_layout(height=400)
                st.plotly_chart(fig_yoy, use_container_width=True)
            
            with col2:
                st.write("**Absolute Growth Values**")
                fig_abs = px.bar(
                    yoy_data,
                    x='Year',
                    y='YoY_Growth_Absolute',
                    title='YoY Absolute Growth',
                    color='YoY_Growth_Absolute',
                    color_continuous_scale='RdYlGn'
                )
                fig_abs.update_layout(height=400)
                st.plotly_chart(fig_abs, use_container_width=True)
        else:
            # Show alternative analysis for single-year data
            st.info("📊 **Growth Analysis Not Available** - Need multiple years of data for YoY growth calculations")
            
            # Show current year breakdown instead using Vehicle Class
            if 'Vehicle Class' in data.columns:
                st.write("**Vehicle Class Breakdown**")
                vehicle_data = data.groupby('Vehicle Class')['TOTAL'].sum().reset_index()
                vehicle_data = vehicle_data.sort_values('TOTAL', ascending=False)
                
                fig_vehicle = px.bar(
                    vehicle_data,
                    x='Vehicle Class',
                    y='TOTAL',
                    title='Vehicle Registrations by Class',
                    text='TOTAL'
                )
                fig_vehicle.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
                fig_vehicle.update_layout(
                    height=400,
                    xaxis_tickangle=-45,  # Rotate labels for better readability
                    margin=dict(b=100)    # Add bottom margin for rotated labels
                )
                st.plotly_chart(fig_vehicle, use_container_width=True)
            elif 'Vehicle_Category' in data.columns:
                st.write("**Current Year Vehicle Category Breakdown**")
                category_data = data.groupby('Vehicle_Category')['TOTAL'].sum().reset_index()
                category_data = category_data.sort_values('TOTAL', ascending=False)
                
                fig_category = px.bar(
                    category_data,
                    x='Vehicle_Category',
                    y='TOTAL',
                    title='Vehicle Registrations by Category',
                    text='TOTAL'
                )
                fig_category.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
                fig_category.update_layout(height=400)
                st.plotly_chart(fig_category, use_container_width=True)
        
        # Category-wise Growth
        if self.growth_metrics and 'yoy_by_category' in self.growth_metrics:
            category_growth = self.growth_metrics['yoy_by_category']
            if not category_growth.empty:
                st.write("**Category-wise YoY Growth Analysis**")
                
                # Latest year growth by category
                latest_year = category_growth['Year'].max()
                latest_growth = category_growth[category_growth['Year'] == latest_year]
                
                fig_cat = px.bar(
                    latest_growth,
                    x='Category',
                    y='YoY_Growth_Rate',
                    title=f'YoY Growth by Category ({latest_year})',
                    color='YoY_Growth_Rate',
                    color_continuous_scale='RdYlGn',
                    text='YoY_Growth_Rate'
                )
                fig_cat.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
                fig_cat.add_hline(y=0, line_dash="dash", line_color="red")
                fig_cat.update_layout(height=400)
                st.plotly_chart(fig_cat, use_container_width=True)
    
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
            
            # Heatmap of registrations by year and category
            heatmap_data = yearly_category.pivot(index='Vehicle_Category', columns='Year', values='TOTAL')
            
            fig_heatmap = px.imshow(
                heatmap_data,
                title='Registration Intensity Heatmap (Category vs Year)',
                color_continuous_scale='Viridis',
                aspect='auto'
            )
            fig_heatmap.update_layout(height=400)
            st.plotly_chart(fig_heatmap, use_container_width=True)
    
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
                # Manufacturer growth analysis
                if self.growth_metrics and 'manufacturer_trends' in self.growth_metrics:
                    manufacturer_growth = self.growth_metrics['manufacturer_trends'].get('manufacturer_growth', pd.DataFrame())
                    if not manufacturer_growth.empty:
                        top_growth = manufacturer_growth.head(10)
                        
                        fig_growth = px.scatter(
                            top_growth,
                            x='Current_Registrations',
                            y='YoY_Growth_Rate',
                            size='Current_Registrations',
                            hover_data=['Manufacturer'],
                            title='Manufacturer Performance Matrix',
                            labels={
                                'Current_Registrations': 'Current Registrations',
                                'YoY_Growth_Rate': 'YoY Growth Rate (%)'
                            }
                        )
                        fig_growth.add_hline(y=0, line_dash="dash", line_color="red")
                        fig_growth.add_vline(x=top_growth['Current_Registrations'].median(), line_dash="dash", line_color="blue")
                        st.plotly_chart(fig_growth, use_container_width=True)
    
    def create_investor_insights(self, data):
        """Generate and display investor-focused insights."""
        st.subheader("💡 Investor Insights & Analysis")
        
        insights = self.processor.get_investor_insights(data)
        
        if insights:
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("### 🚀 Investment Opportunities")
                if 'investment_opportunities' in insights and insights['investment_opportunities']:
                    for i, opportunity in enumerate(insights['investment_opportunities'], 1):
                        st.markdown(f"**{i}.** {opportunity}")
                else:
                    st.info("No specific opportunities identified in current data.")
                
                # Growth leaders
                if 'growth_leaders' in insights:
                    leaders = insights['growth_leaders']
                    if 'top_category_growth' in leaders:
                        st.markdown("### 📈 Category Growth Leaders")
                        growth_df = pd.DataFrame(leaders['top_category_growth'])
                        if not growth_df.empty:
                            st.dataframe(
                                growth_df[['Category', 'Year', 'YoY_Growth_Rate', 'Current_Value']].head(),
                                use_container_width=True
                            )
            
            with col2:
                st.markdown("### ⚠️ Risk Factors")
                if 'risk_factors' in insights and insights['risk_factors']:
                    for i, risk in enumerate(insights['risk_factors'], 1):
                        st.markdown(f"**{i}.** {risk}")
                else:
                    st.success("No significant risk factors identified.")
                
                # Market overview
                if 'market_overview' in insights:
                    overview = insights['market_overview']
                    st.markdown("### 📊 Market Overview")
                    st.json(overview)
    
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
        
        with col2:
            st.markdown("**Compare Vehicle Classes**")
            # Prioritize Vehicle Class over Vehicle_Category
            if 'Vehicle Class' in data.columns:
                vehicle_classes_to_compare = st.multiselect(
                    "Select vehicle classes to compare",
                    options=sorted(data['Vehicle Class'].unique()),
                    default=sorted(data['Vehicle Class'].unique())[:3] if len(data['Vehicle Class'].unique()) >= 3 else sorted(data['Vehicle Class'].unique())
                )
                
                if vehicle_classes_to_compare:
                    vehicle_comparison = data[data['Vehicle Class'].isin(vehicle_classes_to_compare)]
                    
                    # Check if we have multiple years for trend analysis
                    if 'Year' in data.columns and data['Year'].nunique() > 1:
                        yearly_vehicle_comparison = vehicle_comparison.groupby(['Year', 'Vehicle Class'])['TOTAL'].sum().reset_index()
                        
                        fig_vehicle_compare = px.area(
                            yearly_vehicle_comparison,
                            x='Year',
                            y='TOTAL',
                            color='Vehicle Class',
                            title='Vehicle Class Registration Trends',
                        )
                        fig_vehicle_compare.update_layout(height=400)
                        st.plotly_chart(fig_vehicle_compare, use_container_width=True)
                    else:
                        # Single year - show bar comparison
                        vehicle_totals = vehicle_comparison.groupby('Vehicle Class')['TOTAL'].sum().reset_index()
                        vehicle_totals = vehicle_totals.sort_values('TOTAL', ascending=False)
                        
                        fig_vehicle_bar = px.bar(
                            vehicle_totals,
                            x='Vehicle Class',
                            y='TOTAL',
                            title='Vehicle Class Comparison (Current Year)',
                            text='TOTAL'
                        )
                        fig_vehicle_bar.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
                        fig_vehicle_bar.update_layout(
                            height=400,
                            xaxis_tickangle=-45,
                            margin=dict(b=100)
                        )
                        st.plotly_chart(fig_vehicle_bar, use_container_width=True)
            
            elif 'Vehicle_Category' in data.columns:
                # Fallback to Vehicle_Category if Vehicle Class not available
                categories_to_compare = st.multiselect(
                    "Select categories to compare",
                    options=sorted(data['Vehicle_Category'].unique()),
                    default=sorted(data['Vehicle_Category'].unique())
                )
                
                if categories_to_compare:
                    category_comparison = data[data['Vehicle_Category'].isin(categories_to_compare)]
                    yearly_cat_comparison = category_comparison.groupby(['Year', 'Vehicle_Category'])['TOTAL'].sum().reset_index()
                    
                    fig_cat_compare = px.area(
                        yearly_cat_comparison,
                        x='Year',
                        y='TOTAL',
                        color='Vehicle_Category',
                        title='Category-wise Registration Trends',
                    )
                    fig_cat_compare.update_layout(height=400)
                    st.plotly_chart(fig_cat_compare, use_container_width=True)
    
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
            if st.button("📈 Export Growth Metrics") and self.growth_metrics:
                # Combine growth metrics for export
                export_metrics = []
                for metric_name, metric_data in self.growth_metrics.items():
                    if isinstance(metric_data, pd.DataFrame) and not metric_data.empty:
                        metric_copy = metric_data.copy()
                        metric_copy['Metric_Type'] = metric_name
                        export_metrics.append(metric_copy)
                
                if export_metrics:
                    combined_metrics = pd.concat(export_metrics, ignore_index=True)
                    csv_metrics = combined_metrics.to_csv(index=False)
                    st.download_button(
                        label="Download Growth Metrics CSV",
                        data=csv_metrics,
                        file_name=f"vahan_growth_metrics_{datetime.now().strftime('%Y%m%d')}.csv",
                        mime="text/csv"
                    )
        
        with col3:
            if st.button("💡 Export Insights"):
                insights = self.processor.get_investor_insights(data)
                insights_text = f"""
VAHAN Vehicle Registration Analysis - Investment Insights
Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

MARKET OVERVIEW:
{insights.get('market_overview', 'No overview available')}

INVESTMENT OPPORTUNITIES:
{chr(10).join(insights.get('investment_opportunities', ['None identified']))}

RISK FACTORS:
{chr(10).join(insights.get('risk_factors', ['None identified']))}
                """
                
                st.download_button(
                    label="Download Insights Report",
                    data=insights_text,
                    file_name=f"vahan_insights_report_{datetime.now().strftime('%Y%m%d')}.txt",
                    mime="text/plain"
                )


def main():
    """Main dashboard application."""
    st.markdown('<h1 class="main-header">🚗 VAHAN Vehicle Registration Analytics Dashboard</h1>', 
                unsafe_allow_html=True)
    
    st.markdown("""
    <div class="insight-box">
    <h4>🎯 Investor-Focused Vehicle Registration Analytics</h4>
    <p>This dashboard provides comprehensive analysis of vehicle registration data from the VAHAN system, 
    designed specifically for investment decision-making with YoY/QoQ growth metrics, market share analysis, 
    and trend identification.</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Initialize dashboard
    dashboard = VahanDashboard()
    
    # Sidebar for data loading
    st.sidebar.header("📁 Data Management")
    
    # Show current data status
    if st.session_state.get('data_source_type'):
        if st.session_state.data_source_type == "scrape":
            st.sidebar.success("🌐 Live scraped data loaded")
            if st.sidebar.button("🗑️ Clear Scraped Data"):
                st.session_state.scraped_data = None
                st.session_state.scraped_growth_metrics = {}
                st.session_state.data_source_type = None
                st.rerun()
        elif st.session_state.data_source_type == "sample":
            st.sidebar.info("🧪 Sample data loaded")
        elif st.session_state.data_source_type == "upload":
            st.sidebar.info("📁 Uploaded data loaded")
    
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
            # Main dashboard tabs
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
        ### 📋 Getting Started
        
        1. **Data Loading**: Choose from three options:
           - **Sample Data**: Use pre-loaded demo data for testing
           - **Upload CSV**: Upload your own VAHAN data file
           - **🌐 Live Scraping**: Scrape real-time data directly from VAHAN website
        2. **Configure Scraping** (if using Live Scraping): Select states, years, and data types
        3. **Apply Filters**: Use the sidebar to filter by year, state, and vehicle category
        4. **Explore Tabs**: Navigate through different analysis views
        5. **Export Results**: Download processed data and insights
        
        ### 🌐 Live Scraping Features
        - **Real-time Data**: Get the latest vehicle registration data directly from VAHAN
        - **Customizable Selection**: Choose specific states, years, and vehicle categories
        - **Automated Processing**: Data is automatically cleaned and processed for analysis
        - **Progress Tracking**: Monitor scraping progress with real-time updates
        
        ### 📁 Expected Data Format (for CSV uploads)
        Your CSV should include columns like:
        - `Vehicle Class`: Type/manufacturer of vehicle
        - `TOTAL`: Total registrations
        - `Filter_State`: State information
        - `Filter_Year`: Year information
        - Category columns: `2WIC`, `2WN`, `2WT`, etc.
        """)


if __name__ == "__main__":
    main()