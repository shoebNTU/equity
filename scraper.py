# %%
import yfinance as yf
import pandas as pd
import numpy as np
import datetime
import time
import requests
import sys
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# %% CONFIGURATION
OUTPUT_FILE = 'latest_nasdaq.csv'
CURR_STR = 'USD'
EXCHANGE_CACHE = {}

# Configure a robust requests session ONLY for the Nasdaq CSV download
nasdaq_session = requests.Session()
retry = Retry(
    total=5,
    backoff_factor=2, 
    status_forcelist=[429, 500, 502, 503, 504]
)
adapter = HTTPAdapter(max_retries=retry)
nasdaq_session.mount('http://', adapter)
nasdaq_session.mount('https://', adapter)

# %% 1. DYNAMICALLY DOWNLOAD NASDAQ SCREENER DATA
print("Downloading latest stock screener data directly from NASDAQ...")
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*"
}
nasdaq_url = "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=25&offset=0&download=true"

try:
    response = nasdaq_session.get(nasdaq_url, headers=headers, timeout=15)
    response.raise_for_status()
    data = response.json()['data']['rows']
    df = pd.DataFrame(data)
except Exception as e:
    print(f"Failed to download NASDAQ list: {e}")
    sys.exit(1)

# %% 2. CLEAN AND PREPARE DATA
df.rename(columns={'symbol': 'Symbol', 'marketCap': 'Market Cap', 'name': 'Name', 'industry': 'Industry'}, inplace=True)

df['Market Cap'] = pd.to_numeric(df['Market Cap'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
df = df[df['Market Cap'] > 0]
df = df[~df['Name'].str.contains(' Warrant', case=False, na=False)]

df['Symbol'] = df['Symbol'].astype(str).str.replace('/', '-').str.strip()
df.reset_index(drop=True, inplace=True)

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
        else:
            EXCHANGE_CACHE[pair] = 1.0
            
    return EXCHANGE_CACHE[pair]

def get_data(ticker_in, to_get_info):
    # FIXED: Removed session=session. We let yfinance use its native curl_cffi handling.
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
                return['Invalid'] * 8 + ['Invalid'] * len(to_get_info)
            
            if attempt < max_retries - 1:
                time.sleep(2 ** (attempt + 1)) 
            else:
                print(f"Failed completely for {ticker_in}: {e}")
                return['Not Found'] * 8 + ['Not Found'] * len(to_get_info)

# %% 4. MAIN PROCESSING LOOP
to_get_info =['shortName', 'longBusinessSummary', 'lastDividendValue', 'currentPrice', 'targetHighPrice', 'targetLowPrice',
               'targetMedianPrice', 'currency', 'numberOfAnalystOpinions', 'returnOnEquity', 'beta', 'quickRatio',
               'trailingPE', 'forwardPE', 'earningsQuarterlyGrowth', 'earningsGrowth']

cols =['nc_income', 'interest_bearing_securities', 'interest_bearing_debt', 'int_income', 'total_income', 'market_cap', 'total_cash', 'total_debt']

data_list =[]
total_tickers = len(df)
print(f"Starting to fetch API data for {total_tickers} valid tickers...")

for i, row in df.iterrows():
    data_list.append(fetch_ticker_robust(row['Symbol'], to_get_info))
    
    # Mild pause to play nice with Yahoo's limits
    if i % 200 == 0 and i > 0:
        print(f"Processed {i}/{total_tickers} tickers...")
        time.sleep(5)

df[cols + to_get_info] = data_list

# %% 5. RETRY LOGIC FOR MISSING DATA
df_not_found = df[df['longBusinessSummary'] == 'Not Found']

max_retry_rounds = 3
rounds = 0

while len(df_not_found) >= 5 and rounds < max_retry_rounds:
    print(f"Missing {len(df_not_found)} tickers. Waiting 60 seconds before retry round {rounds + 1}/{max_retry_rounds}...")
    time.sleep(60) 
    
    for i, row in df_not_found.iterrows():
        df.loc[i, cols + to_get_info] = fetch_ticker_robust(row['Symbol'], to_get_info, max_retries=2)
        
    df_not_found = df[df['longBusinessSummary'] == 'Not Found']
    rounds += 1

# %% 6. FINAL CLEANUP AND CONDITIONAL SAVE
df.rename(columns={'longBusinessSummary': 'Description'}, inplace=True)

missing_count = len(df_not_found)

if missing_count < 5:
    print(f"SUCCESS: Only {missing_count} tickers missing due to API errors. Saving output to {OUTPUT_FILE}.")
    df.to_csv(OUTPUT_FILE, index=False)
    sys.exit(0)
else:
    print(f"FAILURE: {missing_count} tickers still missing after retries. Threshold is < 5.")
    sys.exit(1)
