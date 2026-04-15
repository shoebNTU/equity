import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
from PIL import Image
import datetime
import requests

# --- UI LOGIC START ---
st.set_page_config(layout="wide")

st.title('Halal & Financial Stock Screener')

# === DISCLAIMER ===
with st.expander("⚠️ Disclaimer", expanded=False):
    st.markdown(
        """
        This project is for educational and informational purposes only. I am not a certified financial or Halal compliance expert. The information and filters provided by this app should not be considered financial advice or a guarantee of Halal status. Please conduct your own research and consult qualified professionals before making any investment decisions. I accept no liability for any financial actions taken based on the use of this app. This is simply a personal hobby project.
        """
    )


# --- CONFIGURATION ---
# Map countries to their GitHub CSV URLs
COUNTRY_URLS = {
    "India": "https://github.com/shoebNTU/equity/releases/download/daily-data/latest_india.csv",
    "USA": "https://github.com/shoebNTU/equity/releases/download/daily-data/latest_nasdaq.csv",
    "SG": "https://github.com/shoebNTU/equity/releases/download/daily-data/latest_sgx.csv",
    # Add more countries and URLs as needed
}

# --- SIDEBAR COUNTRY FILTER ---
st.sidebar.markdown('### Country')
selected_country = st.sidebar.selectbox(
    'Select Country',
    options=list(COUNTRY_URLS.keys()),
    index=1 if "USA" in COUNTRY_URLS else 0,
    help='Choose the country/market to screen stocks from.'
)
GITHUB_RELEASE_URL = COUNTRY_URLS[selected_country]

# --- HELPER FUNCTIONS ---
@st.cache_data(ttl=86400) # Cache exchange rates for a full 24 hours to speed up Halal checks
def get_exchange_rate(base_currency, target_currency):
    if base_currency == target_currency:
        return 1.0
    pair = f"{base_currency}{target_currency}=X"
    yesterday = datetime.date.today() - datetime.timedelta(days=5)
    data = yf.download(pair, start=yesterday, end=datetime.date.today(), progress=False)
    if not data.empty:
        return data.iloc[-1].iloc[0]
    return 1.0

def is_valid_ticker(symbol):
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        return 'symbol' in info and info['symbol'].upper() == symbol.upper()
    except Exception as e:
        print(f"Error validating {symbol}: {e}")
        return False

@st.cache_data(ttl=3600) # Caches the downloaded CSV for 1 hour
def load_data(file_path_or_url):
    try:
        data = pd.read_csv(file_path_or_url)
        # sort by Symbol for easier lookup
        if 'Symbol' in data.columns:
            data.sort_values(by='Symbol', inplace=True)
            data.reset_index(drop=True, inplace=True)
        if 'nc_income' in data.columns:
            data = data[data.nc_income != 'Not Found'].reset_index(drop=True)
        return data
    except Exception as e:
        st.error(f"Error loading dataset: {e}")
        return pd.DataFrame()
    
@st.cache_data(ttl=3600)
def get_data_update_time(selected_country):
    try:
        # Map selected_country to the correct CSV filename
        csv_map = {
            "India": "latest_india.csv",
            "USA": "latest_nasdaq.csv",
            "SG": "latest_sgx.csv"
        }
        csv_name = csv_map.get(selected_country, "latest_nasdaq.csv")
        response = requests.get(
            "https://api.github.com/repos/shoebNTU/equity/releases/tags/daily-data",
            headers={"Accept": "application/vnd.github.v3+json"}
        )
        if response.status_code == 200:
            assets = response.json().get('assets', [])
            for asset in assets:
                if asset.get('name') == csv_name:
                    updated_at = asset.get('updated_at')
                    if updated_at:
                        return datetime.datetime.fromisoformat(updated_at.replace('Z', '+00:00'))
            # fallback to published_at if asset not found
            published_at = response.json().get('published_at')
            if published_at:
                return datetime.datetime.fromisoformat(published_at.replace('Z', '+00:00'))
    except Exception as e:
        print(f"Could not fetch release time: {e}")
    return None

# Synced optimizations from the daily scraper
def get_data(ticker_in, to_get_info):
    try:
        ticker = yf.Ticker(ticker_in)
        info_ = ticker.info

        mkt_cap_curr = info_.get('currency', 'USD')
        debt_curr = info_.get('financialCurrency', 'USD')

        qtr_st = ticker.quarterly_income_stmt
        st_ = ticker.income_stmt

        # Total Revenue
        total_income = 0.0
        ret_total_income = 'Not Found'
        if not qtr_st.empty and 'Total Revenue' in qtr_st.index:
            val = qtr_st.loc['Total Revenue'].iloc[:4].sum()
            if not np.isnan(val):
                total_income = val
                ret_total_income = np.round(total_income, 2)
        elif not st_.empty and 'Total Revenue' in st_.index:
            val = st_.loc['Total Revenue'].iloc[0]
            if not np.isnan(val):
                total_income = val
                ret_total_income = np.round(total_income, 2)

        # Interest Income
        non_compliant_income = 0.0
        ret_int_income = 'Not Found'
        if not qtr_st.empty and 'Interest Income' in qtr_st.index: 
            val = qtr_st.loc['Interest Income'].iloc[:4].sum()
            if not np.isnan(val):
                non_compliant_income = val
                ret_int_income = np.round(non_compliant_income, 2)
        elif not st_.empty and 'Interest Income' in st_.index:
            val = st_.loc['Interest Income'].iloc[0]
            if not np.isnan(val):
                non_compliant_income = val
                ret_int_income = np.round(non_compliant_income, 2)

        # Cash
        qtr_bs = ticker.quarterly_balance_sheet
        total_cash = 0.0
        ret_total_cash = 'Not Found'
        if not qtr_bs.empty and 'Cash And Cash Equivalents' in qtr_bs.index:
            val = qtr_bs.loc['Cash And Cash Equivalents'].iloc[0]
            if not np.isnan(val):
                total_cash = val
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

        info = [info_.get(i) for i in to_get_info]

        if market_cap > 0:
            return[np.round(100 * non_compliant_ratio, 2), np.round(100 * total_cash / market_cap, 2), np.round(100 * total_debt / market_cap, 2),
                    ret_int_income, ret_total_income, ret_market_cap, ret_total_cash, ret_total_debt, info]
        else:
            return[100 * non_compliant_ratio, 0.0, 0.0, 
                    ret_int_income, ret_total_income, ret_market_cap, ret_total_cash, ret_total_debt, info]
                    
    except Exception as e:
        print(f'Not found {ticker_in}: {e}')
        return ['Not Found'] * 9 



# Load data early so sidebar can use live Industry options
df_raw = load_data(GITHUB_RELEASE_URL)
if df_raw.empty:
    st.error(f"No data available for {selected_country}. Please wait for the daily GitHub Action to generate the file.")
    st.stop()

st.sidebar.title('Screening Filters')

st.sidebar.info('Tip: Hover over the ⓘ icons for explanations of each filter.')

data_update_time = get_data_update_time(selected_country)
if data_update_time:
    st.sidebar.info(f"📅 Data last updated:  \n {data_update_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
else:
    st.sidebar.info("📅 Data update time unavailable")

st.sidebar.markdown('### Halal Compliance')
halal_check = st.sidebar.checkbox(
    'Show only Halal-compliant stocks',
    value=False,
    help='Filter to only show stocks that pass all three Halal criteria (income ratio, cash, debt).'
)

 # --- Industry Filter: Always show if Industry column exists ---
if 'Industry' in df_raw.columns:
    st.sidebar.markdown('### Industry')
    enable_industry = st.sidebar.checkbox(
        'Apply Industry filter',
        value=False,
        help='Enable to filter by industry.'
    )
    all_industries = sorted(df_raw['Industry'].dropna().astype(str).unique().tolist())
    if enable_industry:
        selected_industries = st.sidebar.multiselect(
            'Filter by Industry',
            options=all_industries,
            default=[],
            help='Select one or more industries to narrow results. Leave empty to include all.'
        )
    else:
        selected_industries = []
else:
    enable_industry = False
    selected_industries = []

st.sidebar.markdown('### Description Keywords')
enable_keywords = st.sidebar.checkbox(
    'Apply Keyword search',
    value=False,
    help='Enable to filter by keywords in company description.'
)
if enable_keywords:
    st.sidebar.info('Popular keywords: cloud, AI, quantum, semiconductor, fintech, blockchain, SaaS, green, EV, pharma')
    keywords_input = st.sidebar.text_input(
        'Keywords (comma-separated)',
        value='',
        help='Search company descriptions for keywords, e.g. "cloud, semiconductor, AI".'
    ).lower().strip()
else:
    keywords_input = ''

st.sidebar.markdown('### Financial Filters')
enable_beta = st.sidebar.checkbox(
    'Apply Max Beta filter',
    value=False,
    help='Enable to filter out high-volatility stocks.'
)
if enable_beta:
    beta_value_filter = st.sidebar.number_input(
        'Max Beta',
        value=1.0,
        help='Beta measures volatility relative to the market. Lower = more stable.'
    )
else:
    beta_value_filter = None


enable_quick_ratio = st.sidebar.checkbox(
    'Apply Min Quick Ratio filter',
    value=False,
    help="Enable to filter out companies with low quick ratio."
)
if enable_quick_ratio:
    quick_ratio_filter = st.sidebar.number_input(
        label='Min Quick Ratio',
        value=1.0,
        help="Quick Ratio shows a company's ability to pay short-term debts. Higher is safer."
    )
else:
    quick_ratio_filter = None


st.sidebar.markdown('### Analyst Price Target')
analyst_price_filter = st.sidebar.checkbox(
    "Current price below analyst target",
    value=False,
    help='Find stocks trading below the selected analyst price target — a potential value signal.'
)
if analyst_price_filter:
    analyst_target_type = st.sidebar.selectbox(
        "Select analyst target type",
        options=[
            ("Low", "targetLowPrice"),
            ("Median", "targetMedianPrice"),
            ("High", "targetHighPrice")
        ],
        format_func=lambda x: x[0],
        index=0,
        help="Choose which analyst target to compare against."
    )[1]
    no_analyst = st.sidebar.number_input(
        label="Min number of analyst opinions",
        value=1,
        min_value=1,
        help='Require at least this many analyst opinions for more reliable estimates.'
    )

st.sidebar.markdown('---')
submit = st.sidebar.button('Submit')

# Explicit copy to prevent mutating Streamlit's cached state
df = df_raw.copy()

with st.expander('Halal calculation'):
    st.info("""
    - non_compliant_income = (Interest-Income/Total-Revenue) --> `<5%`
    - Interest-bearing securities = (Cash + Cash Equivalents + Deposits) / Market Cap --> `<30%`
    - Interest-bearing debt = Total debt / Market Cap --> `<30%`
    """)

if submit:
    
    df['Industry'] = df.get('Industry', pd.Series(dtype=str)).fillna('None')

    if enable_industry and selected_industries:
        df = df[df['Industry'].isin(selected_industries)]

    # Filter based on beta and quick ratio (safely coercing string values to numeric)
    df['beta_num'] = pd.to_numeric(df.get('beta'), errors='coerce')
    df['quickRatio_num'] = pd.to_numeric(df.get('quickRatio'), errors='coerce')

    if enable_beta and beta_value_filter is not None:
        df = df[(df.beta_num <= beta_value_filter) | (df.beta_num.isna())]
    if enable_quick_ratio and quick_ratio_filter is not None:
        df = df[(df.quickRatio_num >= quick_ratio_filter) | (df.quickRatio_num.isna())]

    if enable_keywords and keywords_input:
        for kw in [k.strip() for k in keywords_input.split(',') if k.strip()]:
            df = df[df['Description'].astype(str).str.contains(kw, case=False, na=False)]

    if halal_check:
        # Safely casts strings like 'Not Found' to NaN before comparing to prevent float crash
        df = df[(pd.to_numeric(df['nc_income'], errors='coerce') < 5) & \
                (pd.to_numeric(df['interest_bearing_securities'], errors='coerce') < 30) & \
                (pd.to_numeric(df['interest_bearing_debt'], errors='coerce') < 30)]
        
    if analyst_price_filter:
        # Ensure the selected target column exists
        if analyst_target_type in df.columns:
            df = df[df['currentPrice'].notna() & df[analyst_target_type].notna() & df['numberOfAnalystOpinions'].notna()].reset_index(drop=True)
            df = df[pd.to_numeric(df['numberOfAnalystOpinions'], errors='coerce') >= no_analyst]
            df = df[(pd.to_numeric(df['currentPrice'], errors='coerce') < pd.to_numeric(df[analyst_target_type], errors='coerce'))]
            df['percent_diff'] = (pd.to_numeric(df[analyst_target_type], errors='coerce') - pd.to_numeric(df['currentPrice'], errors='coerce')) / pd.to_numeric(df['currentPrice'], errors='coerce')
            df.sort_values(by=['percent_diff', 'numberOfAnalystOpinions'], ascending=False, inplace=True)

    columns_to_show =['Symbol','Name','Industry', 'market_cap', 'currentPrice', 'targetLowPrice', 'targetHighPrice', 'targetMedianPrice','numberOfAnalystOpinions','returnOnEquity',
                       'nc_income', 'interest_bearing_securities', 'interest_bearing_debt', 'beta', 'quickRatio', 'Description', 'forwardPE', 'trailingPE', 'earningsQuarterlyGrowth', 'earningsGrowth']
    
    # Ensure columns exist before filtering to avoid UI crash
    columns_to_show =[col for col in columns_to_show if col in df.columns]
    df = df[columns_to_show]
    
    df.reset_index(drop=True, inplace=True)
    st.success(f'Total number of rows found - {len(df)}')
    st.dataframe(df, use_container_width=True)

with st.expander('Exchange Rate (overriden)'):
    c_exchange_rate,_ = st.columns([1,3])
    with c_exchange_rate:
        default_exchange_rate = st.number_input('Please enter exchange rate for currency conversion', value=1.0)

with st.expander('Ticker Query for Halal Check'):
    st.info('Please enter ticker symbol to check for `HALAL` status')
    ticker_input = st.text_input(label='Please enter symbol. Refer https://finance.yahoo.com for correct ticker symbol.', value='').upper().strip()
    
    if ticker_input:
        get_status = st.button('Check')
        if get_status:
            if is_valid_ticker(ticker_input):
                to_get_info =['shortName', 'longBusinessSummary', 'beta','currentPrice','targetHighPrice','targetLowPrice', 
                               'currency','numberOfAnalystOpinions','returnOnEquity',
                               'fiftyTwoWeekLow','fiftyTwoWeekHigh']
                
                nc_income, interest_bearing_securities, interest_bearing_debt, \
                int_income, total_income, market_cap, total_cash, total_debt, info = get_data(ticker_input, to_get_info)
                
                df_ticker = pd.DataFrame({'nc_income':[nc_income], 'interest_bearing_securities':[interest_bearing_securities], 
                                    'interest_bearing_debt':[interest_bearing_debt],
                                    'int_income':int_income, 'total_income':total_income, 'market_cap':market_cap, 'Cash':total_cash, 'Debt':total_debt})
                
                c_title,_ = st.columns([1,2])
                with c_title:
                    st.info(f'###### {info[0]}')
                    st.dataframe(df_ticker, use_container_width=True)
                    
                c1,_ = st.columns([1,4])
                to_show =[]
                for i, j in enumerate(info[1:]):
                    to_show.append(f'**{to_get_info[i+1]}**: {j}')

                ticker = yf.Ticker(ticker_input)
                try:
                    earnings_dates = ticker.earnings_dates
                except Exception:
                    earnings_dates = None

                if earnings_dates is not None:
                    current_date = datetime.datetime.now(earnings_dates.index[0].tz)
                    next_earnings = earnings_dates[earnings_dates.index > current_date].sort_index()
                    if not next_earnings.empty:
                        to_show.append(f'**nextEarningDate**: {next_earnings.index[0]}')
                
                with c1:
                    maybe = 0
                    if df_ticker.iloc[:,3:].apply(lambda x: x.astype(str).str.contains('Not Found').any(), axis=1).values[0]:
                        st.warning('Maybe HALAL. Please check.')
                        maybe = 1
                    elif (isinstance(nc_income, (int, float)) and nc_income >= 5) or \
                         (isinstance(interest_bearing_securities, (int, float)) and interest_bearing_securities >= 30) or \
                         (isinstance(interest_bearing_debt, (int, float)) and interest_bearing_debt >= 30):
                        st.error('Non-HALAL')
                    else:
                        st.success('HALAL')
                        
                    if maybe:
                        st.write('No calculation violations to rule out `Halal` status. However, one or more calculation related values were `Not Found`.')
                
                st.info(' \n'.join(to_show))
                st.markdown('---')
                st.info('#### News Articles')
                
                # --- THIS IS THE RESTORED/FIXED PORTION ---
                try:
                    news_articles = ticker.news
                    if news_articles:
                        for article in news_articles:
                            title = article.get('title', 'No Title')
                            link = article.get('link', '#')
                            pub_time = article.get('providerPublishTime')
                            
                            if pub_time:
                                date_str = datetime.datetime.fromtimestamp(pub_time).strftime('%Y-%m-%d %H:%M:%S')
                            else:
                                date_str = "Unknown Date"
                                
                            st.markdown(f"**{date_str}** - [{title}]({link})")
                    else:
                        st.write("No news articles found for this ticker.")
                except Exception as e:
                    st.write(f"Could not fetch news articles. ({e})")
            else:
                st.error("Invalid Ticker Symbol. Please check and try again.")
    st.image(Image.open('yfinance.png'), width=750)
