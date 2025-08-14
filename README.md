## 🚗 VAHAN Web Scraper - Modular Analytics Platform

A comprehensive, modular solution for scraping and analyzing VAHAN vehicle registration data with investor-focused analytics and growth metrics.

## 🏗️ Architecture Overview

The project follows a clean, modular architecture with separation of concerns:

```
vahan_web_scrapper/
├── src/                          # Source code modules
│   ├── core/                     # Core functionality
│   │   ├── config.py            # Centralized configuration
│   │   ├── exceptions.py        # Custom exceptions
│   │   └── models.py            # Data models & type definitions
│   ├── scrapers/                # Web scraping modules
│   │   ├── base_scraper.py      # Abstract base scraper
│   │   └── vahan_scraper.py     # VAHAN-specific implementation
│   ├── processors/              # Data processing modules
│   │   ├── data_cleaner.py      # Data cleaning utilities
│   │   └── data_processor.py    # Main processing logic
│   ├── analytics/               # Analytics and insights
│   │   ├── growth_analyzer.py   # Growth metrics calculation
│   │   └── insight_generator.py # Business insights
│   └── utils/                   # Utility functions
│       ├── file_utils.py        # File management
│       ├── data_utils.py        # Data utilities
│       └── logging_utils.py     # Logging configuration
├── data/                        # Data storage
├── output/                      # Processed outputs  
├── logs/                        # Application logs
├── main.py                      # CLI entry point
├── dashboard_new.py             # Modular Streamlit dashboard
└── README.md                    # This file
```

## ✨ Key Features

### 🌐 Live Data Scraping
- **Real-time Data Collection**: Scrape directly from VAHAN website
- **Dynamic ID Detection**: Automatically handles changing webpage elements
- **Robust Error Handling**: Comprehensive retry logic and error recovery
- **Progress Tracking**: Real-time scraping progress monitoring

### 📊 Advanced Analytics
- **Growth Metrics**: YoY/QoQ growth rates and CAGR calculation
- **Market Analysis**: Market share, penetration, and competitive landscape
- **Trend Analysis**: Seasonal patterns and growth volatility assessment
- **Forecasting**: Simple trend-based forecasting capabilities

### 💡 Business Insights
- **Investment Recommendations**: Data-driven investment suggestions
- **Risk Assessment**: Market risk identification and mitigation strategies
- **Opportunity Analysis**: Growth opportunities and market gaps
- **Executive Summaries**: High-level business intelligence reports

### 🎯 Investor-Focused Dashboard
- **Interactive Visualizations**: Plotly-powered charts and graphs
- **KPI Monitoring**: Key performance indicators and metrics
- **Comparative Analysis**: Multi-dimensional data comparison tools
- **Export Capabilities**: Data and insights export functionality

## 🚀 Quick Start

### Prerequisites
```bash
Python 3.8+
Chrome browser (for web scraping)
```

### Installation
```bash
# Clone the repository
git clone <repository-url>
cd vahan_web_scrapper

# Install dependencies
pip install -r requirements.txt

# Ensure Chrome WebDriver is available (auto-managed by selenium)
```

### Usage Options

#### 1. 🖥️ Interactive Dashboard
```bash
# Launch the modular Streamlit dashboard
streamlit run dashboard_new.py
```

#### 2. 🔧 Command Line Interface
```bash
# Generate sample data
python main.py sample --output sample_data.csv

# Scrape live data
python main.py scrape --states Karnataka Maharashtra --years 2023 2024 --output scraped_data.csv

# Process scraped data
python main.py process --input scraped_data.csv --output processed_data.csv

# Analyze processed data
python main.py analyze --input processed_data.csv --generate-insights
```

#### 3. 📚 Programmatic Usage
```python
from src.scrapers import VahanScraper
from src.processors import VahanDataProcessor
from src.analytics import GrowthAnalyzer, InsightGenerator

# Initialize components
scraper = VahanScraper()
processor = VahanDataProcessor()
analyzer = GrowthAnalyzer()

# Scrape data
scraper.setup_driver()
scraper.open_page()
data = scraper.fetch_data()

# Process and analyze
result = processor.process_all(data)
cagr = analyzer.calculate_compound_growth_rate(result.cleaned_data)
```

## 📋 Detailed Usage Guide

### Web Scraping
The scraper handles the complex VAHAN website with dynamic elements:

```python
from src.scrapers import VahanScraper
from src.core.models import FilterCombination

scraper = VahanScraper()
scraper.setup_driver(headless=True)
scraper.open_page()

# Create filter combinations
combinations = [
    FilterCombination(state="Karnataka", year="2024"),
    FilterCombination(state="Maharashtra", year="2024")
]

# Scrape data
data = scraper.scrape_multiple_combinations(combinations)
scraper.close()
```

### Data Processing
Comprehensive data cleaning and processing pipeline:

```python
from src.processors import VahanDataProcessor

processor = VahanDataProcessor()

# Load and process data
data = processor.load_data("raw_data.csv")
result = processor.process_all(data)

# Access results
cleaned_data = result.cleaned_data
growth_metrics = result.growth_metrics
insights = result.insights
```

### Analytics and Insights
Advanced analytics for business intelligence:

```python
from src.analytics import GrowthAnalyzer, InsightGenerator

analyzer = GrowthAnalyzer()
insight_gen = InsightGenerator()

# Calculate advanced metrics
cagr = analyzer.calculate_compound_growth_rate(data)
volatility = analyzer.analyze_growth_volatility(data)
forecast = analyzer.generate_growth_forecast(data, periods=2)

# Generate business insights
insights = insight_gen.generate_market_insights(data, growth_metrics)
```

## 🔧 Configuration

### Environment Configuration
Modify `src/core/config.py` for custom settings:

```python
class Config:
    # VAHAN website settings
    VAHAN_BASE_URL = "https://vahan.parivahan.gov.in/..."
    DEFAULT_WAIT_TIME = 15
    
    # Scraping settings
    HEADLESS_MODE = True
    WINDOW_SIZE = "1920,1080"
    
    # Data processing settings
    NUMERIC_COLUMNS = ['2WIC', '2WN', '2WT', '3WN', '3WT', 'LMV', 'MMV', 'HMV', 'TOTAL']
```

### Logging Configuration
Centralized logging with configurable levels:

```python
from src.utils import setup_logging

# Setup logging
setup_logging(log_level='INFO', log_file='custom.log')
```

## 📊 Data Format

### Input Data Structure
Expected CSV format for uploaded data:
```
Vehicle Class,2WIC,2WN,2WT,3WN,3WT,LMV,MMV,HMV,TOTAL,Filter_State,Filter_Year
MOTOR CYCLE,1000,5000,2000,0,0,0,0,0,8000,Karnataka(29),2024
CAR,0,0,0,0,0,3000,0,0,3000,Karnataka(29),2024
```

### Output Data Structure
Processed data includes additional analytical columns:
- `Vehicle_Category`: Categorized vehicle types (2W, 3W, 4W+)
- `State`: Cleaned state names
- `Year`: Extracted year information
- `Scraped_Date`: Data collection timestamp

## 🎯 Key Metrics and KPIs

### Growth Metrics
- **YoY Growth**: Year-over-year growth rates
- **QoQ Growth**: Quarter-over-quarter growth rates
- **CAGR**: Compound Annual Growth Rate
- **Market Share**: Manufacturer and category market shares

### Business Intelligence
- **Market Penetration**: Geographic market analysis
- **Competitive Landscape**: Market concentration and competition
- **Risk Assessment**: Market risks and mitigation strategies
- **Investment Opportunities**: Growth opportunities identification

## 🛠️ Development

### Project Structure Benefits
1. **Modularity**: Clean separation of concerns
2. **Testability**: Easy unit testing of individual components
3. **Maintainability**: Clear code organization and documentation
4. **Extensibility**: Easy to add new features and analytics
5. **Reusability**: Components can be used independently

### Adding New Features
1. **New Scrapers**: Extend `BaseScraper` class
2. **New Analytics**: Add methods to `GrowthAnalyzer` or `InsightGenerator`
3. **New Processors**: Extend `VahanDataProcessor` or create new processors
4. **New Utilities**: Add to appropriate utils module

### Error Handling
Comprehensive exception hierarchy:
- `VahanScraperError`: Base exception
- `ScrapingError`: Web scraping failures
- `DataProcessingError`: Data processing failures
- `ValidationError`: Data validation failures

## 📈 Performance Optimization

### Scraping Performance
- **Parallel Processing**: Multiple filter combinations
- **Smart Retry Logic**: Automatic retry with exponential backoff
- **Resource Management**: Proper browser cleanup and memory management

### Data Processing Performance
- **Vectorized Operations**: Pandas-optimized data processing
- **Memory Efficiency**: Chunked processing for large datasets
- **Caching**: Session state management for dashboard

## 🔒 Security and Best Practices

### Web Scraping Ethics
- **Rate Limiting**: Respectful scraping with delays
- **User Agent Rotation**: Avoid detection as automated bot
- **Error Handling**: Graceful failure handling

### Data Security
- **No Hardcoded Credentials**: Configuration-based settings
- **Secure File Handling**: Safe file operations and validation
- **Logging Security**: No sensitive data in logs

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/new-feature`)
3. Follow the modular architecture patterns
4. Add comprehensive tests
5. Update documentation
6. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support and Troubleshooting

### Common Issues
1. **Chrome Driver Issues**: Ensure Chrome browser is installed
2. **Website Changes**: VAHAN website structure may change
3. **Network Issues**: Check internet connectivity for scraping
4. **Memory Issues**: Use smaller batch sizes for large datasets

### Getting Help
- Check the logs in the `logs/` directory
- Review error messages for specific guidance
- Ensure all dependencies are properly installed
- Verify Chrome browser compatibility

## 🔄 Version History

### v1.0.0 - Modular Architecture Release
- ✅ Complete modular restructure
- ✅ Enhanced error handling and logging
- ✅ Advanced analytics and insights
- ✅ Improved dashboard with new architecture
- ✅ Comprehensive CLI interface
- ✅ Professional documentation

---

**Built with ❤️ for the Indian automotive analytics community**