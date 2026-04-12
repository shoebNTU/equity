import yfinance as yf
import pandas as pd
import numpy as np
import datetime
import time
import requests
import sys
import io
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# %% LOGGING CONFIGURATION
# Set up professional logging format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(sys.stdout) # Ensures GitHub Actions captures the output properly
    ]
)
logger = logging.getLogger(__name__)

# %% GLOBAL CONFIGURATION
OUTPUT_FILE = 'latest_sgx.csv'
CURR_STR = 'SGD'
EXCHANGE_CACHE = {}

# Configure a robust requests session ONLY for the SGX screener download
sgx_session = requests.Session()
sgx_session.verify = False  # Disable SSL verification for SGX API
retry = Retry(
    total=5,
    backoff_factor=2, 
    status_forcelist=[403, 429, 500, 502, 503, 504]
)
adapter = HTTPAdapter(max_retries=retry)
sgx_session.mount('http://', adapter)
sgx_session.mount('https://', adapter)

# %% 1. DYNAMICALLY DOWNLOAD SGX SCREENER DATA
logger.info("Connecting to SGX API to download latest stock screener data...")
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://investors.sgx.com/",
    "Origin": "https://investors.sgx.com",
    "Accept": "application/json, text/plain, */*"
}
sgx_url = "https://api.sgx.com/stockscreener/v2.0/all"

try:
    response = sgx_session.get(sgx_url, headers=headers, timeout=15)
    response.raise_for_status()
    data = response.json().get('data',[])
    if not data:
        raise ValueError("Empty data list returned from SGX API.")
    df = pd.DataFrame(data)
    logger.info(f"Successfully downloaded {len(df)} initial rows from SGX.")
except Exception as e:
    logger.warning(f"Failed to download SGX list from API: {e}. Attempting reliable Wikipedia fallback...")
    try:
        # Robust Fallback to STI Constituents (Fixes Wikipedia 403 block on urllib)
        wiki_url = "https://en.wikipedia.org/wiki/Straits_Times_Index"
        
        # We explicitly use requests with a standard browser User-Agent to fetch the HTML
        wiki_response = sgx_session.get(wiki_url, headers=headers, timeout=15)
        wiki_response.raise_for_status()
        
        # Parse the raw HTML text using io.StringIO to satisfy Pandas
        tables = pd.read_html(io.StringIO(wiki_response.text))
        
        df = None
        for table in tables:
            cols_lower = [str(c).lower() for c in table.columns]
            if 'stock symbol' in cols_lower or 'ticker' in cols_lower:
                df = table.copy()
                break
        
        if df is None:
            raise ValueError("Could not find table with tickers in Wikipedia fallback.")
        logger.info(f"Successfully loaded {len(df)} STI constituents from Wikipedia fallback.")
    except Exception as e_fallback:
        logger.error(f"Fallback also failed: {e_fallback}. Exiting.")
        sys.exit(1)

# %% 2. CLEAN AND PREPARE DATA
logger.info("Cleaning and formatting SGX data...")

# Map various possible column names from SGX JSON or Fallback to standard names
column_mapping = {
    'stockcode': 'Symbol',
    'ticker': 'Symbol',
    'symbol': 'Symbol',
    'stock symbol': 'Symbol',
    'companyname': 'Name',
    'name': 'Name',
    'company': 'Name',
    'marketcap': 'Market Cap',
    'industry': 'Industry',
    'sector': 'Industry'
}
df.rename(columns=lambda c: column_mapping.get(str(c).lower().strip(), c), inplace=True)

if 'Symbol' not in df.columns:
    logger.error(f"Could not locate 'Symbol' column in the response. Columns found: {df.columns.tolist()}")
    sys.exit(1)

# Provide a default Market Cap dummy if the endpoint obscured/skipped it to pass the filter
if 'Market Cap' not in df.columns:
    df['Market Cap'] = 1.0

df['Market Cap'] = pd.to_numeric(df['Market Cap'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
df = df[df['Market Cap'] > 0]

if 'Name' in df.columns:
    df = df[~df['Name'].str.contains(' Warrant', case=False, na=False)]

# Singapore stocks in Yahoo Finance require the '.SI' suffix (e.g. D05 -> D05.SI)
df['Symbol'] = df['Symbol'].astype(str).str.replace('/', '-').str.replace('SGX:', '', case=False).str.strip()
df['Symbol'] = df['Symbol'].apply(lambda x: x if x.endswith('.SI') else f"{x}.SI")

df.reset_index(drop=True, inplace=True)
logger.info(f"Data cleaned. {len(df)} valid tickers remain to be processed.")

# %% 3. HELPER FUNCTIONS
def get_exchange_rate(base_currency, target_currency):
    if base_currency == target_currency:
        return 1.0
        
    pair = f"{base_currency}{target_currency}=X"
    if pair not in EXCHANGE_CACHE:
        yesterday = datetime.date.today() - datetime.timedelta(days=5)
        # Let yf handle the download naturally
        data = yf.download(pair, start=yesterday, end=datetime.date.today(), progress=False)
        if not data.empty:
            EXCHANGE_CACHE[pair] = data.iloc[-1].iloc[0]
            logger.info(f"Cached new exchange rate for {pair}: {EXCHANGE_CACHE[pair]:.4f}")
        else:
            logger.warning(f"Failed to get exchange rate for {pair}. Defaulting to 1.0")
            EXCHANGE_CACHE[pair] = 1.0
            
    return EXCHANGE_CACHE[pair]

def get_data(ticker_in, to_get_info):
    ticker = yf.Ticker(ticker_in)
    info_ = ticker.info
    
    if not info_ or 'symbol' not in info_:
        return ['Invalid'] * 8 + ['Invalid'] * len(to_get_info)
            
    mkt_cap_curr = info_.get('currency', CURR_STR)
    debt_curr = info_.get('financialCurrency', CURR_STR)

    qtr_st = ticker.quarterly_income_stmt
    st = ticker.income_stmt

    # Total Revenue
    ret_total_income = 'Not Found'
    total_income = 0.0
    if not qtr_st.empty and 'Total Revenue' in qtr_st.index:
        val = qtr_st.loc['Total Revenue'].iloc[:4].sum()
        total_income = val if not np.isnan(val) else 0.0
        ret_total_income = np.round(total_income, 2)
    elif not st.empty and 'Total Revenue' in st.index:
        val = st.loc['Total Revenue'].iloc[0]
        total_income = val if not np.isnan(val) else 0.0
        ret_total_income = np.round(total_income, 2)
    
    # Interest Income
    ret_int_income = 'Not Found'
    non_compliant_income = 0.0
    if not qtr_st.empty and 'Interest Income' in qtr_st.index: 
        val = qtr_st.loc['Interest Income'].iloc[:4].sum()
        non_compliant_income = val if not np.isnan(val) else 0.0
        ret_int_income = np.round(non_compliant_income, 2)
    elif not st.empty and 'Interest Income' in st.index:
        val = st.loc['Interest Income'].iloc[0]
        non_compliant_income = val if not np.isnan(val) else 0.0
        ret_int_income = np.round(non_compliant_income, 2)

    # Total Cash
    qtr_bs = ticker.quarterly_balance_sheet
    ret_total_cash = 'Not Found'
    total_cash = 0.0
    if not qtr_bs.empty and 'Cash And Cash Equivalents' in qtr_bs.index:
        val = qtr_bs.loc['Cash And Cash Equivalents'].iloc[0]
        total_cash = val if not np.isnan(val) else 0.0
        ret_total_cash = np.round(total_cash, 2)
        
    # Debt & Market Cap
    total_debt = info_.get('totalDebt', 0.0) 
    total_debt = total_debt if total_debt is not None and not np.isnan(total_debt) else 0.0
    
    market_cap_raw = info_.get('marketCap', 0.0)
    market_cap_raw = market_cap_raw if market_cap_raw is not None and not np.isnan(market_cap_raw) else 0.0

    if mkt_cap_curr != debt_curr and market_cap_raw > 0:
        market_cap = market_cap_raw * get_exchange_rate(mkt_cap_curr, debt_curr)
    else:
        market_cap = market_cap_raw

    ret_total_debt = np.round(total_debt, 2) if total_debt > 0 else 'Not Found'
    ret_market_cap = np.round(market_cap, 2) if market_cap > 0 else 'Not Found'

    if total_income > 0:
        non_compliant_ratio = non_compliant_income / total_income
    elif non_compliant_income > 0:
        non_compliant_ratio = 1.0 
    else:
        non_compliant_ratio = 0.0

    info_list =[info_.get(i) for i in to_get_info]
                
    if market_cap > 0:
        return[np.round(100*non_compliant_ratio, 2), np.round(100*total_cash/market_cap, 2), np.round(100*total_debt/market_cap, 2),
                ret_int_income, ret_total_income, ret_market_cap, ret_total_cash, ret_total_debt] + info_list
    else:
        return[np.round(100*non_compliant_ratio, 2), 0.0, 0.0, 
                ret_int_income, ret_total_income, ret_market_cap, ret_total_cash, ret_total_debt] + info_list

def fetch_ticker_robust(ticker_in, to_get_info, max_retries=3):
    for attempt in range(max_retries):
        try:
            return get_data(ticker_in, to_get_info)
        except Exception as e:
            if "404" in str(e) or "No data found" in str(e):
                return ['Invalid'] * 8 + ['Invalid'] * len(to_get_info)
            
            if attempt < max_retries - 1:
                sleep_time = 2 ** (attempt + 1)
                logger.debug(f"Transient error for {ticker_in}: {e}. Retrying in {sleep_time}s...")
                time.sleep(sleep_time) 
            else:
                logger.warning(f"Failed completely for {ticker_in} after {max_retries} attempts: {e}")
                return ['Not Found'] * 8 +['Not Found'] * len(to_get_info)

def is_all_not_found(row):
    return all(str(row[field]) in ('Not Found', 'Invalid') for field in to_get_info)

# %% 4. MAIN PROCESSING LOOP
to_get_info =['shortName', 'longBusinessSummary', 'lastDividendValue', 'currentPrice', 'targetHighPrice', 'targetLowPrice',
               'targetMedianPrice', 'currency', 'numberOfAnalystOpinions', 'returnOnEquity', 'beta', 'quickRatio',
               'trailingPE', 'forwardPE', 'earningsQuarterlyGrowth', 'earningsGrowth']

cols =['nc_income', 'interest_bearing_securities', 'interest_bearing_debt', 'int_income', 'total_income', 'market_cap', 'total_cash', 'total_debt']

data_list =[]
total_tickers = len(df)
logger.info(f"Starting to fetch API data for {total_tickers} valid tickers...")

for i, row in df.iterrows():
    data_list.append(fetch_ticker_robust(row['Symbol'], to_get_info))
    
    # Mild pause to play nice with Yahoo's limits
    if i % 200 == 0 and i > 0:
        logger.info(f"Processed {i}/{total_tickers} tickers... Taking a 5-second breather.")
        time.sleep(5)

df[cols + to_get_info] = data_list
logger.info("Finished primary API fetching loop.")

# %% 5. RETRY LOGIC FOR MISSING DATA
df_not_found = df[df.apply(is_all_not_found, axis=1)]

max_retry_rounds = 3
rounds = 0

while len(df_not_found) >= 5 and rounds < max_retry_rounds:
    logger.warning(f"Missing {len(df_not_found)} tickers. Waiting 60 seconds before retry round {rounds + 1}/{max_retry_rounds}...")
    time.sleep(60) 
    
    for i, row in df_not_found.iterrows():
        df.loc[i, cols + to_get_info] = fetch_ticker_robust(row['Symbol'], to_get_info, max_retries=2)
        
    df_not_found = df[df.apply(is_all_not_found, axis=1)]
    rounds += 1

# %% 6. FINAL CLEANUP AND CONDITIONAL SAVE
df.rename(columns={'longBusinessSummary': 'Description'}, inplace=True)

missing_count = len(df_not_found)

if missing_count < 5:
    logger.info(f"SUCCESS: Only {missing_count} tickers missing due to API errors. Saving output to {OUTPUT_FILE}.")
    df.to_csv(OUTPUT_FILE, index=False)
    sys.exit(0)
else:
    logger.error(f"FAILURE: {missing_count} tickers still missing after all retries. Threshold is < 5.")
    sys.exit(1)
