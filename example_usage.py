"""
Complete example demonstrating the full VAHAN analytics pipeline:
1. Data scraping from VAHAN dashboard
2. Data processing and growth calculations
3. Dashboard visualization
4. Export and analysis

This script shows how all components work together.
"""

import os
import sys
import pandas as pd
from datetime import datetime
import logging

# Import our custom modules
from vahan_data_extractor import VahanScraper
from data_processor import VahanDataProcessor

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class VahanAnalyticsPipeline:
    """Complete analytics pipeline for VAHAN vehicle registration data."""
    
    def __init__(self, output_dir="data"):
        self.output_dir = output_dir
        self.scraper = None
        self.processor = VahanDataProcessor()
        self.raw_data_file = None
        self.processed_data_file = None
        
        # Create output directories
        os.makedirs(f"{output_dir}/raw", exist_ok=True)
        os.makedirs(f"{output_dir}/processed", exist_ok=True)
        os.makedirs(f"{output_dir}/exports", exist_ok=True)
    
    def setup_scraper(self, headless=True, wait_time=20):
        """Initialize the VAHAN scraper."""
        logger.info("🔧 Setting up VAHAN scraper...")
        
        url = "https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml"
        self.scraper = VahanScraper(url, wait_time=wait_time)
        self.scraper.setup_driver(headless=headless)
        
        logger.info("✅ Scraper setup complete")
    
    def scrape_comprehensive_data(self, 
                                 states_limit=5, 
                                 years_limit=3, 
                                 vehicle_types_limit=3):
        """Scrape comprehensive data across multiple dimensions."""
        if not self.scraper:
            self.setup_scraper()
        
        logger.info("🚀 Starting comprehensive data scraping...")
        
        try:
            # Open the VAHAN page
            self.scraper.open_page()
            
            # Get all available dropdown options
            logger.info("📋 Discovering available options...")
            dropdown_options = self.scraper.scrape_dropdowns()
            
            # Log available options
            for dropdown_name, options in dropdown_options.items():
                logger.info(f"{dropdown_name}: {len(options)} options available")
                if options and not options[0].startswith("⚠️"):
                    logger.info(f"  Examples: {options[:3]}")
            
            # Define scraping combinations
            combinations = []
            
            # Get limited sets for comprehensive scraping
            states = dropdown_options.get("State", [])[:states_limit]
            years = dropdown_options.get("Year", [])[-years_limit:]  # Latest years
            vehicle_types = dropdown_options.get("Vehicle Type", [])[:vehicle_types_limit]
            
            # Generate all combinations
            for state in states:
                if state.startswith("⚠️"):
                    continue
                for year in years:
                    if year.startswith("⚠️"):
                        continue
                    for vehicle_type in vehicle_types:
                        if vehicle_type.startswith("⚠️"):
                            continue
                        combinations.append({
                            "State": state,
                            "Year": year,
                            "Vehicle Type": vehicle_type
                        })
            
            logger.info(f"📊 Planning to scrape {len(combinations)} combinations")
            
            # Perform scraping
            if combinations:
                scraped_df = self.scraper.scrape_multiple_combinations(combinations)
                
                if not scraped_df.empty:
                    # Save raw data
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    self.raw_data_file = f"{self.output_dir}/raw/vahan_raw_data_{timestamp}.csv"
                    scraped_df.to_csv(self.raw_data_file, index=False)
                    
                    logger.info(f"✅ Scraped {len(scraped_df)} rows of data")
                    logger.info(f"💾 Raw data saved to: {self.raw_data_file}")
                    
                    return scraped_df
                else:
                    logger.warning("⚠️ No data was scraped")
                    return pd.DataFrame()
            else:
                logger.warning("⚠️ No valid combinations found for scraping")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"❌ Error during scraping: {e}")
            return pd.DataFrame()
        
        finally:
            if self.scraper:
                self.scraper.close()
    
    def process_scraped_data(self, data_file=None):
        """Process the scraped data and calculate growth metrics."""
        logger.info("🔄 Processing scraped data...")
        
        if data_file is None:
            data_file = self.raw_data_file
        
        if data_file is None:
            logger.error("❌ No data file specified for processing")
            return None
        
        try:
            # Load and process data
            raw_data = self.processor.load_data(data_file)
            
            if raw_data.empty:
                logger.error("❌ No data loaded for processing")
                return None
            
            # Clean and process
            processed_data = self.processor.clean_data(raw_data)
            
            # Calculate growth metrics
            growth_metrics = self.processor.calculate_growth_metrics(processed_data)
            
            # Generate investor insights
            insights = self.processor.get_investor_insights(processed_data)
            
            # Save processed data
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.processed_data_file = f"{self.output_dir}/processed/vahan_processed_{timestamp}.csv"
            
            processed_filename = self.processor.export_processed_data(self.processed_data_file)
            
            logger.info(f"✅ Data processing complete")
            logger.info(f"📊 Growth metrics calculated: {list(growth_metrics.keys())}")
            logger.info(f"💾 Processed data saved to: {processed_filename}")
            
            return {
                'processed_data': processed_data,
                'growth_metrics': growth_metrics,
                'insights': insights,
                'processed_file': processed_filename
            }
            
        except Exception as e:
            logger.error(f"❌ Error during processing: {e}")
            return None
    
    def generate_analysis_report(self, processing_results):
        """Generate a comprehensive analysis report."""
        logger.info("📝 Generating analysis report...")
        
        if not processing_results:
            logger.error("❌ No processing results available for report")
            return None
        
        try:
            processed_data = processing_results['processed_data']
            growth_metrics = processing_results['growth_metrics']
            insights = processing_results['insights']
            
            # Create report
            report_lines = []
            report_lines.append("=" * 80)
            report_lines.append("VAHAN VEHICLE REGISTRATION ANALYSIS REPORT")
            report_lines.append("=" * 80)
            report_lines.append(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            report_lines.append(f"Data period: {processed_data['Year'].min()} - {processed_data['Year'].max()}")
            report_lines.append(f"Total records analyzed: {len(processed_data):,}")
            report_lines.append("")
            
            # Executive Summary
            report_lines.append("EXECUTIVE SUMMARY")
            report_lines.append("-" * 50)
            total_registrations = processed_data['TOTAL'].sum()
            report_lines.append(f"Total vehicle registrations: {total_registrations:,}")
            
            if 'State' in processed_data.columns:
                unique_states = processed_data['State'].nunique()
                report_lines.append(f"States covered: {unique_states}")
                
                # Top performing state
                top_state_data = processed_data.groupby('State')['TOTAL'].sum()
                top_state = top_state_data.idxmax()
                top_state_registrations = top_state_data.max()
                report_lines.append(f"Top performing state: {top_state} ({top_state_registrations:,} registrations)")
            
            if 'Vehicle_Category' in processed_data.columns:
                # Category breakdown
                category_breakdown = processed_data.groupby('Vehicle_Category')['TOTAL'].sum()
                report_lines.append(f"Vehicle categories analyzed: {len(category_breakdown)}")
                for category, count in category_breakdown.items():
                    percentage = (count / total_registrations) * 100
                    report_lines.append(f"  - {category}: {count:,} ({percentage:.1f}%)")
            
            report_lines.append("")
            
            # Growth Analysis
            report_lines.append("GROWTH ANALYSIS")
            report_lines.append("-" * 50)
            
            if 'yoy_total' in growth_metrics:
                yoy_total = growth_metrics['yoy_total']
                if not yoy_total.empty and 'YoY_Growth_Rate' in yoy_total.columns:
                    avg_growth = yoy_total['YoY_Growth_Rate'].mean()
                    latest_growth = yoy_total['YoY_Growth_Rate'].iloc[-1] if len(yoy_total) > 0 else 0
                    report_lines.append(f"Average YoY growth rate: {avg_growth:.2f}%")
                    report_lines.append(f"Latest year growth rate: {latest_growth:.2f}%")
            
            if 'yoy_by_category' in growth_metrics:
                category_growth = growth_metrics['yoy_by_category']
                if not category_growth.empty:
                    # Top growing category
                    top_growth = category_growth.loc[category_growth['YoY_Growth_Rate'].idxmax()]
                    report_lines.append(f"Fastest growing category: {top_growth['Category']} ({top_growth['YoY_Growth_Rate']:.2f}%)")
                    
                    # Declining categories
                    declining = category_growth[category_growth['YoY_Growth_Rate'] < 0]
                    if not declining.empty:
                        report_lines.append(f"Categories showing decline: {len(declining)}")
                        for _, row in declining.iterrows():
                            report_lines.append(f"  - {row['Category']}: {row['YoY_Growth_Rate']:.2f}%")
            
            report_lines.append("")
            
            # Investment Insights
            report_lines.append("INVESTMENT INSIGHTS")
            report_lines.append("-" * 50)
            
            if 'investment_opportunities' in insights:
                report_lines.append("Investment Opportunities:")
                for i, opportunity in enumerate(insights['investment_opportunities'], 1):
                    report_lines.append(f"  {i}. {opportunity}")
            
            if 'risk_factors' in insights:
                report_lines.append("Risk Factors:")
                for i, risk in enumerate(insights['risk_factors'], 1):
                    report_lines.append(f"  {i}. {risk}")
            
            report_lines.append("")
            report_lines.append("METHODOLOGY")
            report_lines.append("-" * 50)
            report_lines.append("• Data sourced from VAHAN dashboard (vahan.parivahan.gov.in)")
            report_lines.append("• YoY growth calculated as: ((Current - Previous) / Previous) * 100")
            report_lines.append("• Vehicle categories: 2W (Two Wheeler), 3W (Three Wheeler), 4W+ (Four Wheeler+)")
            report_lines.append("• Data cleaned for missing values and formatting inconsistencies")
            
            # Save report
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_file = f"{self.output_dir}/exports/vahan_analysis_report_{timestamp}.txt"
            
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write('\n'.join(report_lines))
            
            logger.info(f"📄 Analysis report saved to: {report_file}")
            
            # Also print key findings
            print("\n" + "="*60)
            print("🎯 KEY FINDINGS")
            print("="*60)
            print(f"📊 Total Registrations: {total_registrations:,}")
            
            if 'yoy_total' in growth_metrics:
                yoy_data = growth_metrics['yoy_total']
                if not yoy_data.empty and 'YoY_Growth_Rate' in yoy_data.columns:
                    avg_growth = yoy_data['YoY_Growth_Rate'].mean()
                    print(f"📈 Average Growth: {avg_growth:.2f}%")
            
            if 'investment_opportunities' in insights and insights['investment_opportunities']:
                print("🚀 Top Opportunity:", insights['investment_opportunities'][0])
            
            if 'risk_factors' in insights and insights['risk_factors']:
                print("⚠️ Key Risk:", insights['risk_factors'][0])
            
            return report_file
            
        except Exception as e:
            logger.error(f"❌ Error generating report: {e}")
            return None
    
    def run_complete_pipeline(self, 
                             scrape_new_data=True,
                             states_limit=3, 
                             years_limit=2,
                             generate_report=True):
        """Run the complete analytics pipeline."""
        logger.info("🚀 Starting complete VAHAN analytics pipeline...")
        
        try:
            # Step 1: Data Scraping (optional)
            if scrape_new_data:
                logger.info("Step 1: Data Scraping")
                scraped_data = self.scrape_comprehensive_data(
                    states_limit=states_limit,
                    years_limit=years_limit,
                    vehicle_types_limit=3
                )
                
                if scraped_data.empty:
                    logger.error("❌ No data scraped, pipeline terminated")
                    return None
            
            # Step 2: Data Processing
            logger.info("Step 2: Data Processing")
            processing_results = self.process_scraped_data()
            
            if not processing_results:
                logger.error("❌ Data processing failed, pipeline terminated")
                return None
            
            # Step 3: Report Generation
            if generate_report:
                logger.info("Step 3: Report Generation")
                report_file = self.generate_analysis_report(processing_results)
            
            # Step 4: Dashboard Instructions
            logger.info("Step 4: Dashboard Launch Instructions")
            print("\n" + "="*60)
            print("🎯 PIPELINE COMPLETE - NEXT STEPS")
            print("="*60)
            print("To launch the interactive dashboard, run:")
            print("  streamlit run dashboard.py")
            print("\nProcessed data files:")
            if self.processed_data_file:
                print(f"  - {self.processed_data_file}")
            if generate_report and 'report_file' in locals():
                print(f"  - {report_file}")
            print("\n✅ Pipeline completed successfully!")
            
            return {
                'raw_data_file': self.raw_data_file,
                'processed_data_file': self.processed_data_file,
                'processing_results': processing_results,
                'report_file': report_file if generate_report else None
            }
            
        except Exception as e:
            logger.error(f"❌ Pipeline failed: {e}")
            return None


def main():
    """Main execution function with different usage scenarios."""
    print("🚗 VAHAN Vehicle Registration Analytics Pipeline")
    print("=" * 60)
    
    # Scenario selection
    print("\nSelect execution scenario:")
    print("1. Full Pipeline (Scrape + Process + Analyze)")
    print("2. Process Existing Data Only")
    print("3. Quick Demo with Sample Data")
    print("4. Dashboard Only")
    
    try:
        choice = input("\nEnter your choice (1-4): ").strip()
        
        if choice == "1":
            # Full pipeline
            print("\n🚀 Running Full Pipeline...")
            pipeline = VahanAnalyticsPipeline()
            
            states_limit = int(input("Number of states to scrape (default 3): ") or "3")
            years_limit = int(input("Number of recent years (default 2): ") or "2")
            
            results = pipeline.run_complete_pipeline(
                scrape_new_data=True,
                states_limit=states_limit,
                years_limit=years_limit,
                generate_report=True
            )
            
            if results:
                print(f"\n✅ Pipeline completed successfully!")
                print(f"Files generated:")
                for key, value in results.items():
                    if value:
                        print(f"  - {key}: {value}")
        
        elif choice == "2":
            # Process existing data
            print("\n🔄 Processing Existing Data...")
            data_file = input("Enter path to your CSV data file: ").strip()
            
            if not os.path.exists(data_file):
                print(f"❌ File not found: {data_file}")
                return
            
            pipeline = VahanAnalyticsPipeline()
            results = pipeline.process_scraped_data(data_file)
            
            if results:
                report_file = pipeline.generate_analysis_report(results)
                print(f"✅ Processing complete!")
                print(f"Processed data: {results['processed_file']}")
                if report_file:
                    print(f"Analysis report: {report_file}")
        
        elif choice == "3":
            # Quick demo
            print("\n🧪 Quick Demo with Sample Data...")
            from data_processor import create_sample_data
            
            # Create sample data
            sample_data = create_sample_data()
            
            # Save sample data
            os.makedirs("data/demo", exist_ok=True)
            sample_file = "data/demo/sample_vahan_data.csv"
            sample_data.to_csv(sample_file, index=False)
            
            # Process sample data
            pipeline = VahanAnalyticsPipeline(output_dir="data/demo")
            results = pipeline.process_scraped_data(sample_file)
            
            if results:
                report_file = pipeline.generate_analysis_report(results)
                print(f"\n✅ Demo completed!")
                print(f"Sample data processed: {results['processed_file']}")
                if report_file:
                    print(f"Demo report: {report_file}")
                print("\nTo view in dashboard:")
                print("  streamlit run dashboard.py")
        
        elif choice == "4":
            # Dashboard only
            print("\n🎛️ Launching Dashboard...")
            print("Make sure you have data files ready!")
            os.system("streamlit run dashboard.py")
        
        else:
            print("❌ Invalid choice. Please run the script again.")
    
    except KeyboardInterrupt:
        print("\n\n⚠️ Operation cancelled by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")


if __name__ == "__main__":
    main()