import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
from PIL import Image
import datetime

# === DISCLAIMER ===
st.warning(
    """
    **Disclaimer**\n
    This project is for educational and informational purposes only. I am not a certified financial or Halal compliance expert. The information and filters provided by this app should not be considered financial advice or a guarantee of Halal status. Please conduct your own research and consult qualified professionals before making any investment decisions. I accept no liability for any financial actions taken based on the use of this app. This is simply a personal hobby project.
    """,
    icon="⚠️"
)

# --- CONFIGURATION ---
# The URL to the static Release Asset we created in the GitHub Action
GITHUB_RELEASE_URL = "https://github.com/shoebNTU/equity/releases/download/daily-data/latest_nasdaq.csv"

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
        if 'nc_income' in data.columns:
            data = data[data.nc_income != 'Not Found'].reset_index(drop=True)
        return data
    except Exception as e:
        st.error(f"Error loading dataset: {e}")
        return pd.DataFrame()

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

# --- UI LOGIC START ---
st.set_page_config(layout="wide")
st.title('Halal & Financial Stock Screener')


st.sidebar.title('Search Parameters')

st.sidebar.markdown('### Basic Search')
name = st.sidebar.text_input(
    'Company Name',
    value='',
    help='Enter part or all of a company’s name (e.g., "Apple"). Leave blank to skip.'
).lower().strip()
symbol = st.sidebar.text_input(
    'Ticker',
    value='',
    help='Enter the stock symbol (e.g., "AAPL" for Apple). Leave blank to skip.'
).lower().strip()

st.sidebar.markdown('### Halal Compliance')
halal_check = st.sidebar.selectbox(
    label='Filter out non-Halal stocks?',
    options=['Yes','No'],
    index=1,
    help='Choose "Yes" to only show stocks that meet Halal (Islamic finance) criteria.'
)

st.sidebar.markdown('### Keyword Search')
no_of_search = st.sidebar.number_input(
    label='Number of keywords to search (optional)',
    value=0,
    min_value=0,
    help='How many keywords do you want to search for in company descriptions?'
)
search_text =[]
if no_of_search:
    for i in range(no_of_search):
        search_text.append(st.sidebar.text_input(
            label=f"Keyword {i+1}",
            value='',
            key=i,
            help='Enter a word to search for in the company’s description (e.g., "technology").'
        ).lower().strip())

st.sidebar.markdown('### Financial Filters')
beta_value_filter = st.sidebar.number_input(
    label='Max Beta',
    value=0.0,
    help='Beta measures how much a stock’s price moves compared to the market. Lower beta means less risk.'
)
quick_ratio_filter = st.sidebar.number_input(
    label='Min Quick Ratio',
    value=1.0,
    help='Quick Ratio shows a company’s ability to pay short-term debts. Higher is safer.'
)

st.sidebar.markdown('### Analyst Price Filter')
low_price = st.sidebar.selectbox(
    label="Current Price < Analyst's Low Price?",
    options=['Yes','No'],
    index=1,
    help='Choose "Yes" to find stocks whose current price is below the lowest price target set by analysts.'
)
if low_price == 'Yes':
    no_analyst = st.sidebar.number_input(
        label="Min number of analyst opinions",
        value=1,
        min_value=1,
        help='Require at least this many analyst opinions for more reliable estimates.'
    )

st.sidebar.markdown('---')
st.sidebar.info('Tip: Hover over the ⓘ icons for explanations of each filter.')
submit = st.sidebar.button('Submit')

df_raw = load_data(GITHUB_RELEASE_URL)
if df_raw.empty:
    st.error("No data available. Please wait for the daily GitHub Action to generate the file.")
    st.stop()
# Explicit copy to prevent mutating Streamlit's cached state
df = df_raw.copy()

with st.expander('Halal calculation'):
    st.info("""
    - non_compliant_income = (Interest-Income/Total-Revenue) --> `<5%`
    - Interest-bearing securities = (Cash + Cash Equivalents + Deposits) / Market Cap --> `<30%`
    - Interest-bearing debt = Total debt / Market Cap --> `<30%`
    """)

if submit:
    # Filter Dataframe securely
    if symbol:
        df = df[df['Symbol'].astype(str).str.contains(symbol, case=False, na=False)]
    
    df['Industry'] = df.get('Industry', pd.Series(dtype=str)).fillna('None')

    if len(df) and name:
        df = df[df['Name'].astype(str).str.contains(name, case=False, na=False)]

    # Filter based on beta and quick ratio (safely coercing string values to numeric)
    df['beta_num'] = pd.to_numeric(df.get('beta'), errors='coerce')
    df['quickRatio_num'] = pd.to_numeric(df.get('quickRatio'), errors='coerce')
    
    df = df[(df.beta_num <= beta_value_filter) | (df.beta_num.isna())]
    df = df[(df.quickRatio_num >= quick_ratio_filter) | (df.quickRatio_num.isna())]

    # Filter industry
    df = df[~df['Industry'].str.contains('bio', case=False, na=False)]

    if no_of_search:
        # Replaced unsafe eval() with secure string filtering loop
        for text in search_text:
            if text:
                df = df[df['Description'].astype(str).str.contains(text, case=False, na=False)]
                
    if halal_check == 'Yes':
        # Safely casts strings like 'Not Found' to NaN before comparing to prevent float crash
        df = df[(pd.to_numeric(df['nc_income'], errors='coerce') < 5) & \
                (pd.to_numeric(df['interest_bearing_securities'], errors='coerce') < 30) & \
                (pd.to_numeric(df['interest_bearing_debt'], errors='coerce') < 30)]
        
    if low_price == 'Yes':
        df = df[df['currentPrice'].notna() & df['targetLowPrice'].notna() & df['numberOfAnalystOpinions'].notna()].reset_index(drop=True)
        df = df[pd.to_numeric(df['numberOfAnalystOpinions'], errors='coerce') >= no_analyst]
        df = df[(pd.to_numeric(df['currentPrice'], errors='coerce') < pd.to_numeric(df['targetLowPrice'], errors='coerce'))]
        
        df['percent_diff'] = (pd.to_numeric(df['targetLowPrice'], errors='coerce') - pd.to_numeric(df['currentPrice'], errors='coerce')) / pd.to_numeric(df['currentPrice'], errors='coerce')
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
