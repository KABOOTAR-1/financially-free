"""
Main CLI entry point for VAHAN web scraper.
Provides command-line interface for all scraping and processing operations.
"""

import argparse
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.core.config import Config
from src.core.exceptions import VahanScraperError
from src.scrapers import VahanScraper
from src.processors import VahanDataProcessor
from src.analytics import GrowthAnalyzer, InsightGenerator
from src.utils import setup_logging, get_logger, FileManager, create_sample_data

def setup_cli_parser():
    """Setup command line argument parser."""
    parser = argparse.ArgumentParser(
        description="VAHAN Vehicle Registration Data Scraper and Analyzer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py scrape --states Karnataka Maharashtra --years 2023 2024
  python main.py process --input data/raw_data.csv --output processed_data.csv
  python main.py analyze --input processed_data.csv
  python main.py sample --output sample_data.csv
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Scraping command
    scrape_parser = subparsers.add_parser('scrape', help='Scrape data from VAHAN website')
    scrape_parser.add_argument('--states', nargs='+', required=True, 
                              help='States to scrape (e.g., Karnataka Maharashtra)')
    scrape_parser.add_argument('--years', nargs='+', required=True,
                              help='Years to scrape (e.g., 2023 2024)')
    scrape_parser.add_argument('--vehicle-types', nargs='*',
                              help='Vehicle types to scrape (optional)')
    scrape_parser.add_argument('--output', '-o', 
                              help='Output filename (auto-generated if not specified)')
    scrape_parser.add_argument('--headless', action='store_true', default=True,
                              help='Run browser in headless mode')
    
    # Processing command
    process_parser = subparsers.add_parser('process', help='Process and clean scraped data')
    process_parser.add_argument('--input', '-i', required=True,
                               help='Input CSV file path')
    process_parser.add_argument('--output', '-o',
                               help='Output filename (auto-generated if not specified)')
    process_parser.add_argument('--export-metrics', action='store_true',
                               help='Export growth metrics separately')
    
    # Analysis command
    analyze_parser = subparsers.add_parser('analyze', help='Analyze processed data')
    analyze_parser.add_argument('--input', '-i', required=True,
                               help='Input processed CSV file path')
    analyze_parser.add_argument('--output-dir', '-o',
                               help='Output directory for analysis results')
    analyze_parser.add_argument('--generate-insights', action='store_true',
                               help='Generate business insights report')
    
    # Sample data command
    sample_parser = subparsers.add_parser('sample', help='Generate sample data for testing')
    sample_parser.add_argument('--output', '-o', default='sample_vahan_data.csv',
                              help='Output filename for sample data')
    sample_parser.add_argument('--rows', type=int, default=None,
                              help='Number of sample rows (uses default if not specified)')
    
    # Global options
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       default='INFO', help='Logging level')
    parser.add_argument('--log-file', help='Log file path (auto-generated if not specified)')
    
    return parser

def scrape_command(args):
    """Execute scraping command."""
    logger = get_logger(__name__)
    
    try:
        logger.info("🚀 Starting VAHAN data scraping...")
        
        # Initialize scraper
        scraper = VahanScraper()
        scraper.setup_driver(headless=args.headless)
        scraper.open_page()
        
        # Get available options
        logger.info("📋 Fetching available dropdown options...")
        dropdown_data = scraper.scrape_dropdowns()
        
        # Validate requested states and years
        available_states = dropdown_data.get('State', [])
        available_years = dropdown_data.get('Year', [])
        
        valid_states = [state for state in args.states if state in available_states]
        valid_years = [year for year in args.years if year in available_years]
        
        if not valid_states:
            logger.error(f"❌ No valid states found. Available: {available_states[:10]}")
            return False
        
        if not valid_years:
            logger.error(f"❌ No valid years found. Available: {available_years}")
            return False
        
        logger.info(f"✅ Scraping {len(valid_states)} states for {len(valid_years)} years")
        
        # Create filter combinations
        from src.core.models import FilterCombination
        combinations = []
        
        for state in valid_states:
            for year in valid_years:
                combo = FilterCombination(state=state, year=year)
                if args.vehicle_types:
                    for vehicle_type in args.vehicle_types:
                        combo_with_vehicle = FilterCombination(
                            state=state, year=year, vehicle_type=vehicle_type
                        )
                        combinations.append(combo_with_vehicle)
                else:
                    combinations.append(combo)
        
        # Scrape data
        scraped_data = scraper.scrape_multiple_combinations(combinations)
        
        if scraped_data.empty:
            logger.error("❌ No data was scraped")
            return False
        
        # Save data
        output_file = args.output or Config.get_output_filename("scraped_vahan_data")
        saved_path = scraper.save_data(scraped_data, output_file)
        
        logger.info(f"✅ Scraping completed! Data saved to: {saved_path}")
        logger.info(f"📊 Total records scraped: {len(scraped_data)}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Scraping failed: {e}")
        return False
    
    finally:
        if 'scraper' in locals():
            scraper.close()

def process_command(args):
    """Execute processing command."""
    logger = get_logger(__name__)
    
    try:
        logger.info("🔄 Starting data processing...")
        
        # Initialize processor
        processor = VahanDataProcessor()
        
        # Load data
        data = processor.load_data(args.input)
        
        # Process data
        result = processor.process_all(data)
        
        # Export processed data
        output_file = args.output or Config.get_output_filename("processed_vahan_data")
        saved_path = processor.export_processed_data(output_file)
        
        logger.info(f"✅ Processing completed! Data saved to: {saved_path}")
        logger.info(f"📊 Records processed: {result.records_processed}")
        logger.info(f"⏱️ Processing time: {result.processing_time:.2f} seconds")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Processing failed: {e}")
        return False

def analyze_command(args):
    """Execute analysis command."""
    logger = get_logger(__name__)
    
    try:
        logger.info("📈 Starting data analysis...")
        
        # Load processed data
        file_manager = FileManager()
        data = file_manager.load_dataframe(args.input)
        
        # Initialize analyzers
        growth_analyzer = GrowthAnalyzer()
        insight_generator = InsightGenerator()
        
        # Perform analysis
        logger.info("📊 Calculating growth metrics...")
        cagr = growth_analyzer.calculate_compound_growth_rate(data)
        volatility = growth_analyzer.analyze_growth_volatility(data)
        patterns = growth_analyzer.identify_growth_patterns(data)
        
        # Generate insights if requested
        if args.generate_insights:
            logger.info("💡 Generating business insights...")
            # Mock growth metrics for insight generation
            growth_metrics = {
                'yoy_growth': {'2022-2023': 12.5, '2023-2024': 15.2},
                'category_growth': {'2W': {'2023-2024': 18.3}, '4W+': {'2023-2024': 8.7}},
                'state_growth': {'Karnataka': {'2023-2024': 22.1}}
            }
            insights = insight_generator.generate_market_insights(data, growth_metrics)
        
        # Save analysis results
        output_dir = Path(args.output_dir) if args.output_dir else Config.OUTPUT_DIR
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create analysis report
        analysis_results = {
            'cagr': cagr,
            'volatility_analysis': volatility,
            'growth_patterns': patterns
        }
        
        if args.generate_insights:
            analysis_results['business_insights'] = insights
        
        # Save as JSON report
        import json
        report_file = output_dir / Config.get_output_filename("analysis_report", "json")
        with open(report_file, 'w') as f:
            json.dump(analysis_results, f, indent=2, default=str)
        
        logger.info(f"✅ Analysis completed! Report saved to: {report_file}")
        logger.info(f"📈 CAGR: {cagr}%")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Analysis failed: {e}")
        return False

def sample_command(args):
    """Execute sample data generation command."""
    logger = get_logger(__name__)
    
    try:
        logger.info("🧪 Generating sample data...")
        
        # Generate sample data
        sample_data = create_sample_data()
        
        # Save sample data
        file_manager = FileManager()
        saved_path = file_manager.save_dataframe(sample_data, args.output)
        
        logger.info(f"✅ Sample data generated! Saved to: {saved_path}")
        logger.info(f"📊 Sample records: {len(sample_data)}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Sample generation failed: {e}")
        return False

def main():
    """Main CLI entry point."""
    parser = setup_cli_parser()
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level, args.log_file)
    logger = get_logger(__name__)
    
    # Ensure directories exist
    Config.ensure_directories()
    
    logger.info("🚗 VAHAN Web Scraper CLI Started")
    
    try:
        # Execute command
        if args.command == 'scrape':
            success = scrape_command(args)
        elif args.command == 'process':
            success = process_command(args)
        elif args.command == 'analyze':
            success = analyze_command(args)
        elif args.command == 'sample':
            success = sample_command(args)
        else:
            parser.print_help()
            success = False
        
        if success:
            logger.info("✅ Operation completed successfully!")
            sys.exit(0)
        else:
            logger.error("❌ Operation failed!")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("⏹️ Operation cancelled by user")
        sys.exit(1)
    except VahanScraperError as e:
        logger.error(f"❌ VAHAN Scraper Error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
