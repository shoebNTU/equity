import yfinance as yf
import pandas as pd
import time
import requests
import sys
import logging
import io
import numpy as np
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3

# ==========================================
# 0. CONFIGURATION & LOGGING SETUP
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Suppress the insecure request warnings so it doesn't spam your console
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

OUTPUT_FILE = 'latest_india.csv'

# Robust requests session for downloading the ticker lists
session = requests.Session()
session.verify = False
retry = Retry(
    total=5,
    backoff_factor=2,
    status_forcelist=[429, 500, 502, 503, 504]
)
adapter = HTTPAdapter(max_retries=retry)
session.mount('http://', adapter)
session.mount('https://', adapter)

# Universal Headers to bypass basic bot-blocking
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br"
}
session.headers.update(headers)


# ==========================================
# 1. DOWNLOAD ALL NSE STOCKS
# ==========================================
logger.info("Downloading all NSE stocks...")
# Using the static CSV archive is much faster and bypasses the 500-stock API limit
nse_csv_url = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"

try:
    # Hit base URL first to grab required session cookies (MANDATORY for NSE)
    session.get("https://www.nseindia.com", timeout=15)
    
    # Now fetch the actual CSV
    nse_resp = session.get(nse_csv_url, timeout=15)
    nse_resp.raise_for_status()
    
    # Read the content into Pandas
    nse_df = pd.read_csv(io.BytesIO(nse_resp.content))
    
    # Format symbol for yfinance (.NS for NSE)
    nse_df['Symbol'] = nse_df['SYMBOL'].astype(str) + '.NS'
    nse_df.rename(columns={'NAME OF COMPANY': 'companyName'}, inplace=True)
    
    nse_df = nse_df[['Symbol', 'companyName']]
    logger.info(f"NSE: {len(nse_df)} stocks loaded.")
    print(f"NSE tickers retrieved: {len(nse_df)}")
except Exception as e:
    logger.error(f"Failed to download NSE list: {e}")
    nse_df = pd.DataFrame(columns=['Symbol', 'companyName'])


# ==========================================
# 2. DOWNLOAD ALL BSE STOCKS
# ==========================================
logger.info("Downloading all BSE stocks...")
bse_url = "https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w"

# BSE API often requires ALL of these parameters present, even if empty
bse_params = {
    "Group": "",
    "Scripcode": "",
    "industry": "",
    "segment": "Equity",
    "status": "Active"
}

bse_headers = session.headers.copy()
bse_headers["Referer"] = "https://www.bseindia.com/"

try:
    bse_resp = session.get(bse_url, params=bse_params, headers=bse_headers, timeout=15)
    bse_resp.raise_for_status()
    
    bse_data = bse_resp.json()
    bse_df = pd.DataFrame(bse_data)
    
    if bse_df.empty:
        logger.error("BSE API returned empty data. They might be temporarily rate-limiting.")
    else:
        # Convert all columns to lowercase to prevent KeyErrors due to casing changes
        cols = [str(c).lower() for c in bse_df.columns]
        bse_df.columns = cols
        
        # Dynamically find the correct column names
        sym_col = next((col for col in ['scrip_cd', 'scripcode', 'security code'] if col in cols), None)
        name_col = next((col for col in['scrip_name', 'scripname', 'company name', 'name'] if col in cols), None)
        
        if sym_col and name_col:
            # Format symbol for yfinance (.BO for BSE)
            bse_df['Symbol'] = bse_df[sym_col].astype(str) + '.BO'
            bse_df.rename(columns={name_col: 'companyName'}, inplace=True)
            
            bse_df = bse_df[['Symbol', 'companyName']]
            logger.info(f"BSE: {len(bse_df)} stocks loaded.")
        else:
            logger.error(f"Unexpected BSE columns returned: {bse_df.columns.tolist()}")
            bse_df = pd.DataFrame(columns=['Symbol', 'companyName'])

except Exception as e:
    logger.error(f"Failed to download BSE list: {e}")
    bse_df = pd.DataFrame(columns=['Symbol', 'companyName'])


# ==========================================
# 3. COMBINE LISTS
# ==========================================
df = pd.concat([nse_df, bse_df], ignore_index=True)
df.drop_duplicates('Symbol', inplace=True)
df.reset_index(drop=True, inplace=True)

if df.empty:
    logger.error("No stock data available from NSE or BSE. Exiting script.")
    sys.exit(1)

logger.info(f"Total unique stocks to process: {len(df)}")


# ==========================================
# 4. YFINANCE DATA FETCHING LOGIC
# ==========================================

# --- Advanced get_data logic from scraper.py ---

CURR_STR = 'INR'

def get_data(ticker_in, to_get_info):
    ticker = yf.Ticker(ticker_in)
    try:
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

        market_cap = market_cap_raw  # No FX conversion for India

        ret_total_debt = np.round(total_debt, 2) if total_debt > 0 else 'Not Found'
        ret_market_cap = np.round(market_cap, 2) if market_cap > 0 else 'Not Found'

        if total_income > 0:
            non_compliant_ratio = non_compliant_income / total_income
        elif non_compliant_income > 0:
            non_compliant_ratio = 1.0
        else:
            non_compliant_ratio = 0.0

        info_list = [info_.get(i) for i in to_get_info]

        if market_cap > 0:
            return [np.round(100*non_compliant_ratio, 2), np.round(100*total_cash/market_cap, 2), np.round(100*total_debt/market_cap, 2),
                    ret_int_income, ret_total_income, ret_market_cap, ret_total_cash, ret_total_debt] + info_list
        else:
            return [np.round(100*non_compliant_ratio, 2), 0.0, 0.0,
                    ret_int_income, ret_total_income, ret_market_cap, ret_total_cash, ret_total_debt] + info_list
    except Exception as e:
        # Pass exception up so the robust retry logic catches it
        raise e

def fetch_ticker_robust(ticker_in, to_get_info, max_retries=3):
    for attempt in range(max_retries):
        try:
            return get_data(ticker_in, to_get_info)
        except Exception as e:
            if attempt < max_retries - 1:
                sleep_time = 2 ** (attempt + 1)
                logger.debug(f"Transient error for {ticker_in}: {e}. Retrying in {sleep_time}s...")
                time.sleep(sleep_time)
            else:
                logger.warning(f"Failed completely for {ticker_in} after {max_retries} attempts: {e}")
                return ['Not Found'] * len(to_get_info)


# --- Use the same columns as scraper.py ---
to_get_info = [
    'shortName', 'longBusinessSummary', 'lastDividendValue', 'currentPrice', 'targetHighPrice', 'targetLowPrice',
    'targetMedianPrice', 'currency', 'numberOfAnalystOpinions', 'returnOnEquity', 'beta', 'quickRatio',
    'trailingPE', 'forwardPE', 'earningsQuarterlyGrowth', 'earningsGrowth'
]
cols = ['nc_income', 'interest_bearing_securities', 'interest_bearing_debt', 'int_income', 'total_income', 'market_cap', 'total_cash', 'total_debt']

# Add 'industry' to to_get_info for yfinance enrichment
to_get_info.append('industry')

logger.info(f"Fetching Yahoo Finance data for {len(df)} stocks...")
logger.info(f"NOTE: Fetching {len(df)} stocks sequentially will take a few hours. Grab a coffee!")

data_list = []
for i, row in df.iterrows():
    data_list.append(fetch_ticker_robust(row['Symbol'], to_get_info))
    # Print progress every 50 stocks
    if (i + 1) % 50 == 0:
        logger.info(f"Processed {i + 1} / {len(df)} tickers...")
    # Avoid IP bans / Ratelimits
    if (i + 1) % 200 == 0:
        logger.info("Taking a 5-second breather to respect API limits...")
        time.sleep(5)


# ==========================================
# 5. SAVE DATA
# ==========================================


# Assign initial results (all columns)
df[cols + to_get_info] = data_list


# Retry logic for missing data: only if ALL fields are 'Not Found' or 'Invalid'
def is_all_not_found(row):
    return all(str(row[field]) in ('Not Found', 'Invalid') for field in to_get_info)

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

df.rename(columns={'longBusinessSummary': 'Description'}, inplace=True)
# Ensure 'Industry' column is present and named correctly
df.rename(columns={'industry': 'Industry'}, inplace=True)
# Ensure compatibility with app_run.py: rename companyName to Name
if 'companyName' in df.columns:
    df.rename(columns={'companyName': 'Name'}, inplace=True)
missing_count = len(df_not_found)

if missing_count == 0:
    logger.info(f"SUCCESS: No tickers missing due to API errors. Saving output to {OUTPUT_FILE}.")
    df.to_csv(OUTPUT_FILE, index=False)
else:
    logger.warning(f"{missing_count} tickers missing after all retries. Saving output to {OUTPUT_FILE} anyway.")
    logger.warning(f"Tickers missing after all retries: {list(df_not_found['Symbol'])}")
    df.to_csv(OUTPUT_FILE, index=False)
# No sys.exit() calls, script always completes successfully
