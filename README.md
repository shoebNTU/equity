# Disclaimer
This project is for educational and informational purposes only. I am not a certified financial or Halal compliance expert. The information and filters provided by this app should not be considered financial advice or a guarantee of Halal status. Please conduct your own research and consult qualified professionals before making any investment decisions. I accept no liability for any financial actions taken based on the use of this app. This is simply a personal hobby project.


# Halal & Financial Stock Screener

A Streamlit web app and data pipeline for screening and filtering NASDAQ stocks, with a focus on Halal compliance and financial metrics.

## Features

- **Streamlit Web App**: Interactive UI for searching, filtering, and analyzing NASDAQ stocks.
- **Halal Compliance Checks**: Filters stocks based on non-compliant income, interest-bearing securities, and debt ratios.
- **Custom Filters**: Search by ticker, company name, keywords, beta, quick ratio, and analyst price targets.
- **Live Data**: Fetches and processes data from Yahoo Finance and NASDAQ APIs.
- **News Integration**: Displays recent news articles for queried tickers.
- **Automated Data Pipeline**: `scraper.py` downloads and cleans the latest NASDAQ data.

## Setup

1. **Clone the repository** and install dependencies:
	```bash
	pip install -r requirements.txt
	```

2. **Run the Streamlit app**:
	```bash
	streamlit run app_run.py
	```

3. **(Optional) Update NASDAQ data**:
	```bash
	python scraper.py
	```

## Usage

- Use the sidebar to filter stocks by various criteria.
- Check Halal status for any ticker.
- View financial metrics and recent news for selected companies.

## Explanation of Filters (For Laymen)

The app provides several filters to help you find stocks that match your preferences. Here’s what each filter means:

- **Name**: Enter part or all of a company’s name to search for it (e.g., "Apple").
- **Ticker**: Enter the stock symbol (e.g., "AAPL" for Apple) to search for a specific company.
- **Halal Filter**: Choose "Yes" to only show stocks that meet Halal (Islamic finance) criteria. This checks:
  - Non-compliant income (like interest) is less than 5% of total revenue.
  - Interest-bearing securities and debt are each less than 30% of the company’s market value.
- **Number of Keywords**: Lets you search for companies whose descriptions contain certain words (e.g., "technology").
- **Beta Value**: Beta measures how much a stock’s price moves compared to the market. Lower beta means less risk. Set a maximum value to filter for less volatile stocks.
- **Quick Ratio**: This shows a company’s ability to pay its short-term debts. Higher is safer. Set a minimum value to filter for financially healthy companies.
- **Current Price < Analyst’s Low Price?**: Choose "Yes" to find stocks whose current price is below the lowest price target set by analysts (may indicate undervalued stocks).
- **Number of Analyst Opinions**: When the above is "Yes", you can require a minimum number of analyst opinions for more reliable estimates.

## Files

- `app_run.py`: Main Streamlit app.
- `scraper.py`: Script to fetch and preprocess NASDAQ data.
- `data_generation.ipynb`: Notebook for data experiments.
- `requirements.txt`: Python dependencies.
- `22Mar26_nasdaq_2.csv`: Example data file.

## License

See [LICENSE](LICENSE).
