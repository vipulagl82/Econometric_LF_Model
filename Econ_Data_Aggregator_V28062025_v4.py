import streamlit as st
import pandas as pd
import pandas_datareader.data as web
import datetime
from fredapi import Fred
import io
import collections
import altair as alt
from statsmodels.tsa.stattools import adfuller
from docx import Document
from docx.shared import Inches
import numpy as np # Added for CSV processing
import re # For variable name cleanup

# --- Streamlit App Configuration ---\
st.set_page_config(layout="wide", page_title="FRED Macro Data Downloader & Analyzer")

st.title("Enhanced FRED Macro Data Downloader & Analyzer")
st.write("""
This application allows you to download, process, and analyze macroeconomic data.
**Note:** The analysis section requires `statsmodels`, `python-docx`, and `vl-convert-python`. If running locally, please install them (`pip install statsmodels python-docx vl-convert-python`).
""")

# --- FRED API Key Setup ---
FRED_API_KEY = "1bef8a463132e1f36f04a5fd56a92cc0"
if not FRED_API_KEY:
    st.error("FRED API Key is missing. Please provide a valid key to proceed.")
    st.stop()

# --- FRED Client Initialization ---
@st.cache_resource
def get_fred_client(api_key):
    try:
        return Fred(api_key=api_key)
    except Exception as e:
        st.error(f"Failed to initialize FRED client: {e}")
        st.stop()

fred = get_fred_client(FRED_API_KEY)

# --- Predefined Macro Series ---
PREDEFINED_SERIES = {
    # GDP
    "GDP: Nominal GDP (Billions of Dollars)": {
        "id": "GDP", "units": "Billions of Dollars", "frequency": "Quarterly", "description": "Gross Domestic Product", "notes": "Nominal", "category": "GDP"
    },
    "GDP: Real GDP (Billions of Chained 2017 Dollars)": {
        "id": "GDPC1", "units": "Billions of Chained 2017 Dollars", "frequency": "Quarterly", "description": "Real Gross Domestic Product", "notes": "Chained 2017 Dollars", "category": "GDP"
    },
    # Labor Market
    "Unemployment Rate: Civilian Unemployment Rate (%)": {
        "id": "UNRATE", "units": "%", "frequency": "Monthly", "description": "Civilian Unemployment Rate, Seasonally Adjusted", "category": "Labor Market"
    },
    # Inflation
    "Inflation: CPI (% Year-over-Year)": {
        "id": "CPIAUCSL", "units": "Index (1982-84=100)", "frequency": "Monthly", "description": "Consumer Price Index for All Urban Consumers: All Items", "notes": "This is the index. YoY change must be calculated.", "category": "Inflation"
    },
    # Interest Rates
    "Interest Rates: Effective Federal Funds Rate (%)": {
        "id": "DFF", "units": "%", "frequency": "Daily", "description": "Effective Federal Funds Rate", "category": "Interest Rates"
    },
    # Consumer
    "Retail Sales: Total Retail Sales (Millions of Dollars)": {
        "id": "RSXFS", "units": "Millions of Dollars", "frequency": "Monthly", "description": "Retail and Food Services Sales, Seasonally Adjusted", "category": "Consumer"
    },
    "HPI: S&P/Case-Shiller U.S. National Home Price Index": {
        "id": "CSUSHPISA", "units": "Index (Jan 2000=100)", "frequency": "Quarterly", "description": "S&P/Case-Shiller U.S. National Home Price Index, Seasonally Adjusted", "category": "Consumer"
    },
    "Real Disposable Personal Income (Billions of Chained 2017 Dollars)": {
        "id": "DSPIC96", "units": "Billions of Chained 2017 Dollars", "frequency": "Monthly", "description": "Real Disposable Personal Income, Seasonally Adjusted", "category": "Consumer"
    },
    "Consumer Sentiment: University of Michigan": {
        "id": "UMCSENT", "units": "Index (1966:Q1=100)", "frequency": "Monthly", "description": "University of Michigan: Consumer Sentiment, Seasonally Adjusted", "category": "Consumer"
    },
    # Credit and Mortgage Metrics
    "Credit Card Delinquency Rate (%)": {
        "id": "DRCCLACBS", "units": "%", "frequency": "Quarterly", "description": "Delinquency Rate on Credit Card Loans, All Commercial Banks", "category": "Credit and Mortgage Metrics"
    },
    "Credit Card Charge-Off Rate (%)": {
        "id": "CORCCACBS", "units": "%", "frequency": "Quarterly", "description": "Charge-Off Rate on Credit Card Loans, All Commercial Banks", "category": "Credit and Mortgage Metrics"
    },
    "Mortgage Delinquency Rate (%)": {
        "id": "DRSFRMACBS", "units": "%", "frequency": "Quarterly", "description": "Delinquency Rate on Single-Family Residential Mortgages", "category": "Credit and Mortgage Metrics"
    },
     "Mortgage Charge-Off Rate (%)": {
        "id": "CORSFRMACBS", "units": "%", "frequency": "Quarterly", "description": "Charge-Off Rate on Single-Family Residential Mortgages", "category": "Credit and Mortgage Metrics"
    },
}

# --- Initialize session state ---
if 'screen' not in st.session_state:
    st.session_state.screen = 'data_selection'
if 'analysis_data' not in st.session_state:
    st.session_state.analysis_data = None
if 'target_frequency' not in st.session_state:
    st.session_state.target_frequency = 'Quarterly'
if 'analysis_results' not in st.session_state:
    st.session_state.analysis_results = None
if 'data_source' not in st.session_state:
    st.session_state.data_source = 'fred'


# --- Helper Functions ---\
def go_to_data_selection():
    """Resets state to return to the data selection screen."""
    st.session_state.screen = 'data_selection'
    st.session_state.analysis_data = None
    st.session_state.analysis_results = None

def convert_frequency(data, source_freq, target_freq, agg_method, interp_method):
    if target_freq == 'Default (No Change)' or source_freq is None:
        return data
    freq_rank = {'Daily': 1, 'Weekly': 2, 'Monthly': 3, 'Quarterly': 4, 'Annual': 5}
    freq_map = {'Annual': 'A', 'Quarterly': 'Q', 'Monthly': 'M', 'Weekly': 'W', 'Daily': 'D'}
    source_rank = freq_rank.get(source_freq)
    target_rank = freq_rank.get(target_freq)
    resample_freq = freq_map.get(target_freq)

    if not source_rank or not target_rank or not resample_freq:
        return data
    data.index = pd.to_datetime(data.index)
    if source_rank < target_rank:
        resampled_data = data.resample(resample_freq).agg(agg_method)
    elif source_rank > target_rank:
        resampled_data = data.resample(resample_freq).asfreq().interpolate(method=interp_method)
    else:
        return data
    return resampled_data.dropna(how='all')

def check_stationarity_and_transform(df, target_frequency):
    transformed_data = {}
    summary_data = []

    for col in df.columns:
        series = df[col].dropna()
        if len(series) < 10:
            is_stationary = "N/A"
            p_value = "N/A"
            transformation_applied = "None (Not enough data)"
            transformed_series = series
        else:
            result = adfuller(series)
            p_value = result[1]
            is_stationary = p_value <= 0.05
            transformation_applied = "None"

            if not is_stationary:
                periods = {'Quarterly': 4, 'Monthly': 12, 'Annual': 1}.get(target_frequency, 4)
                transformed_series = series.pct_change(periods=periods) * 100
                transformation_applied = f"YoY % Change (p={periods})"
            else:
                transformed_series = series

        transformed_data[col] = transformed_series
        summary_data.append({
            "Variable": col,
            "ADF P-Value": f"{p_value:.4f}" if isinstance(p_value, float) else p_value,
            "Is Stationary": is_stationary,
            "Transformation Applied": transformation_applied
        })

    return pd.DataFrame(transformed_data).dropna(how='all'), pd.DataFrame(summary_data)

def get_basic_analysis(dv_name, iv_name, correlation, rolling_corr_summary, stationarity_info):
    """Generates a basic, data-driven analysis without external API calls."""
    corr_strength = "weak"
    if abs(correlation) >= 0.7:
        corr_strength = "strong"
    elif abs(correlation) >= 0.4:
        corr_strength = "moderate"
    direction = "positive" if correlation > 0 else "negative"

    analysis = f"""
**Relationship Analysis:**
The overall relationship between the transformed series of '{dv_name}' and '{iv_name}' is a **{corr_strength} {direction} correlation** ({correlation:.2f}).

**Macroeconomic Context:**
"""
    if "Unemployment" in iv_name and ("Delinquency" in dv_name or "Charge-Off" in dv_name):
        analysis += "This is a common economic relationship, as rising unemployment puts financial pressure on households, often leading to an increase in loan defaults and charge-offs."
    elif "GDP" in iv_name and ("Delinquency" in dv_name or "Charge-Off" in dv_name):
        analysis += "Typically, as the economy grows (positive GDP growth), household financial health improves, leading to a decrease in loan defaults. The observed correlation aligns with this principle."
    else:
        analysis += "The interaction reflects how broader economic conditions influence consumer credit performance."

    analysis += f"""

**Temporal Dynamics:**
{rolling_corr_summary}
This indicates how the strength and direction of the relationship have evolved over time, potentially influenced by different economic cycles or events.
"""
    return analysis

def save_chart(chart):
    try:
        import vl_convert as vlc
        # Updated function name from chart_to_png to vegalite_to_png
        png_data = vlc.vegalite_to_png(chart.to_dict())
        return io.BytesIO(png_data)
    except ImportError:
        st.warning("Chart saving requires the `vl-convert-python` package. Charts will not be included in the Word document. Please run `pip install vl-convert-python`.")
        return None
    except Exception as e:
        st.error(f"An error occurred while saving the chart: {e}")
        return None

def process_uploaded_csv(uploaded_file):
    """Process uploaded CSV file and return a cleaned dataframe"""
    try:
        df = pd.read_csv(uploaded_file)
        date_columns = [col for col in df.columns if any(word in col.lower() for word in ['date', 'time', 'period'])]

        if date_columns:
            date_col = date_columns[0]
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            df = df.set_index(date_col)
        elif df.index.dtype == 'object':
            try:
                df.index = pd.to_datetime(df.index, errors='coerce')
            except Exception:
                pass
        
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        df = df[numeric_columns]
        df = df.sort_index()
        df = df.dropna(how='all')
        return df
    except Exception as e:
        st.error(f"Error processing CSV file: {e}")
        return None

# --- Data Transformation Helper Functions ---
def clean_variable_names(df):
    """Clean variable names by removing special characters except underscore"""
    cleaned_columns = {}
    for col in df.columns:
        # Replace special characters with underscore, keep only alphanumeric and underscore
        cleaned_name = re.sub(r'[^a-zA-Z0-9_]', '_', col)
        # Remove multiple consecutive underscores
        cleaned_name = re.sub(r'_+', '_', cleaned_name)
        # Remove leading/trailing underscores
        cleaned_name = cleaned_name.strip('_')
        cleaned_columns[col] = cleaned_name
    return cleaned_columns

def create_variable_aliases():
    """Create alias names for variables"""
    aliases = {
        'GDP_Nominal_GDP_Billions_of_Dollars': 'GDP',
        'GDP_Real_GDP_Billions_of_Chained_2017_Dollars': 'RGDP',
        'Unemployment_Rate_Civilian_Unemployment_Rate': 'UNRATE',
        'Inflation_CPI_Year_over_Year': 'CPI',
        'Interest_Rates_Effective_Federal_Funds_Rate': 'FFR',
        'Retail_Sales_Total_Retail_Sales_Millions_of_Dollars': 'RS',
        'HPI_SP_Case_Shiller_US_National_Home_Price_Index': 'HPI',
        'Real_Disposable_Personal_Income_Billions_of_Chained_2017_Dollars': 'DPI',
        'Consumer_Sentiment_University_of_Michigan': 'CSENT',
        'Credit_Card_Delinquency_Rate': 'CCDR',
        'Credit_Card_Charge_Off_Rate': 'CCCO',
        'Mortgage_Delinquency_Rate': 'MDR',
        'Mortgage_Charge_Off_Rate': 'MCO'
    }
    return aliases

def create_macro_transformations(df, predictor_vars):
    """Create all macro transformations for predictor variables only"""
    transformed_df = df.copy()
    
    for var in predictor_vars:
        if var not in df.columns:
            continue
            
        # Step 1: Create basic transformations
        # YoY percentage change (t1 suffix)
        yoy_pct = df[var].pct_change(periods=4) * 100
        transformed_df[f"{var}_YoY_t1"] = yoy_pct
        
        # QoQ percentage change (t1 suffix)
        qoq_pct = df[var].pct_change(periods=1) * 100
        transformed_df[f"{var}_QoQ_t1"] = qoq_pct
        
        # YoY level difference (t2 suffix)
        yoy_diff = df[var].diff(periods=4)
        transformed_df[f"{var}_YoY_t2"] = yoy_diff
        
        # QoQ level difference (t2 suffix)
        qoq_diff = df[var].diff(periods=1)
        transformed_df[f"{var}_QoQ_t2"] = qoq_diff
        
        # Step 2: Create moving averages for all base transformations
        # Moving averages for raw variable
        for window in [1, 2, 3, 4]:
            transformed_df[f"{var}_w{window}"] = df[var].rolling(window=window).mean()
        
        # Moving averages for YoY percentage change
        for window in [1, 2, 3, 4]:
            transformed_df[f"{var}_YoY_t1_w{window}"] = yoy_pct.rolling(window=window).mean()
        
        # Moving averages for QoQ percentage change
        for window in [1, 2, 3, 4]:
            transformed_df[f"{var}_QoQ_t1_w{window}"] = qoq_pct.rolling(window=window).mean()
        
        # Moving averages for YoY level difference
        for window in [1, 2, 3, 4]:
            transformed_df[f"{var}_YoY_t2_w{window}"] = yoy_diff.rolling(window=window).mean()
        
        # Moving averages for QoQ level difference
        for window in [1, 2, 3, 4]:
            transformed_df[f"{var}_QoQ_t2_w{window}"] = qoq_diff.rolling(window=window).mean()
        
        # Step 3: Create lags for all transformations (raw + all moving averages)
        # Lags for raw variable
        for lag in [1, 2, 3, 4]:
            transformed_df[f"{var}_l{lag}"] = df[var].shift(lag)
        
        # Lags for YoY percentage change
        for lag in [1, 2, 3, 4]:
            transformed_df[f"{var}_YoY_t1_l{lag}"] = yoy_pct.shift(lag)
        
        # Lags for QoQ percentage change
        for lag in [1, 2, 3, 4]:
            transformed_df[f"{var}_QoQ_t1_l{lag}"] = qoq_pct.shift(lag)
        
        # Lags for YoY level difference
        for lag in [1, 2, 3, 4]:
            transformed_df[f"{var}_YoY_t2_l{lag}"] = yoy_diff.shift(lag)
        
        # Lags for QoQ level difference
        for lag in [1, 2, 3, 4]:
            transformed_df[f"{var}_QoQ_t2_l{lag}"] = qoq_diff.shift(lag)
        
        # Lags for all moving averages of raw variable
        for window in [1, 2, 3, 4]:
            for lag in [1, 2, 3, 4]:
                transformed_df[f"{var}_w{window}_l{lag}"] = transformed_df[f"{var}_w{window}"].shift(lag)
        
        # Lags for all moving averages of YoY percentage change
        for window in [1, 2, 3, 4]:
            for lag in [1, 2, 3, 4]:
                transformed_df[f"{var}_YoY_t1_w{window}_l{lag}"] = transformed_df[f"{var}_YoY_t1_w{window}"].shift(lag)
        
        # Lags for all moving averages of QoQ percentage change
        for window in [1, 2, 3, 4]:
            for lag in [1, 2, 3, 4]:
                transformed_df[f"{var}_QoQ_t1_w{window}_l{lag}"] = transformed_df[f"{var}_QoQ_t1_w{window}"].shift(lag)
        
        # Lags for all moving averages of YoY level difference
        for window in [1, 2, 3, 4]:
            for lag in [1, 2, 3, 4]:
                transformed_df[f"{var}_YoY_t2_w{window}_l{lag}"] = transformed_df[f"{var}_YoY_t2_w{window}"].shift(lag)
        
        # Lags for all moving averages of QoQ level difference
        for window in [1, 2, 3, 4]:
            for lag in [1, 2, 3, 4]:
                transformed_df[f"{var}_QoQ_t2_w{window}_l{lag}"] = transformed_df[f"{var}_QoQ_t2_w{window}"].shift(lag)
    
    return transformed_df

def test_stationarity(df, variables):
    """Test stationarity for all variables using ADF test"""
    stationarity_results = []
    
    for var in variables:
        if var not in df.columns:
            continue
            
        series = df[var].dropna()
        if len(series) < 10:
            stationarity_results.append({
                'Variable': var,
                'ADF_Statistic': 'N/A',
                'P_Value': 'N/A',
                'Critical_Values': 'N/A',
                'Is_Stationary': False,
                'Reason': 'Insufficient data'
            })
            continue
        
        try:
            result = adfuller(series)
            adf_stat = result[0]
            p_value = result[1]
            critical_values = result[4]
            
            is_stationary = p_value < 0.05
            
            stationarity_results.append({
                'Variable': var,
                'ADF_Statistic': round(adf_stat, 4),
                'P_Value': round(p_value, 4),
                'Critical_Values': f"1%: {critical_values['1%']:.4f}, 5%: {critical_values['5%']:.4f}, 10%: {critical_values['10%']:.4f}",
                'Is_Stationary': is_stationary,
                'Reason': 'Stationary' if is_stationary else 'Non-stationary'
            })
        except Exception as e:
            stationarity_results.append({
                'Variable': var,
                'ADF_Statistic': 'N/A',
                'P_Value': 'N/A',
                'Critical_Values': 'N/A',
                'Is_Stationary': False,
                'Reason': f'Error: {str(e)}'
            })
    
    return pd.DataFrame(stationarity_results)

def create_panel_data(df, anchor_var, dependent_var, predictor_vars, performance_quarters=13):
    """Create panel data with anchoring and performance quarters"""
    panel_data = []
    
    # Ensure index is datetime-like for proper date operations
    if not hasattr(df.index, 'year'):
        try:
            df.index = pd.to_datetime(df.index)
        except:
            raise ValueError("Index must be datetime-like for panel data creation")
    
    for snapshot_date in df.index:
        try:
            # Get snapshot date in YYYYQQ format
            snapshot_quarter = f"{snapshot_date.year}Q{(snapshot_date.month - 1) // 3 + 1}"
            
            # Get anchor variable value at snapshot date (time T)
            anchor_value = df.loc[snapshot_date, anchor_var]
            
            for perf_q in range(1, performance_quarters + 1):
                # Calculate performance date (T + k)
                perf_date = snapshot_date + pd.DateOffset(months=3 * perf_q)
                perf_quarter = f"{perf_date.year}Q{(perf_date.month - 1) // 3 + 1}"
                
                # Create row for this snapshot-performance combination
                row = {
                    'Snapshot_Date': snapshot_quarter,
                    'Performance_Quarter': perf_q,
                    'Calendar_Qtr': perf_quarter,
                    f'Snapshot_{anchor_var}': anchor_value
                }
                
                # Add predictor variables aligned to performance date
                for var in predictor_vars:
                    if var in df.columns and perf_date in df.index:
                        row[var] = df.loc[perf_date, var]
                    else:
                        row[var] = np.nan
                
                panel_data.append(row)
        except Exception as e:
            # Skip problematic dates and continue
            continue
    
    return pd.DataFrame(panel_data)

def calculate_empirical_loss_rate(panel_df, dependent_var, performance_quarters=13):
    """Calculate empirical loss rate for performance quarters 1 to 13"""
    # This is a placeholder - in practice, you would calculate actual loss rates
    # based on your specific methodology
    loss_rates = {}
    
    for perf_q in range(1, performance_quarters + 1):
        # Example calculation - replace with your actual loss rate calculation
        # This could be based on charge-off rates, delinquency rates, etc.
        loss_rates[perf_q] = np.random.uniform(0.01, 0.05)  # Placeholder values
    
    return loss_rates


# --- Main Application Logic ---

# SCREEN 1: DATA SELECTION
if st.session_state.screen == 'data_selection':
    st.sidebar.title("Data Source")
    data_source_option = st.sidebar.radio(
        "Choose your data source:",
        ["Download from FRED", "Upload CSV File"],
        key="data_source_radio"
    )
    st.session_state.data_source = 'fred' if data_source_option == "Download from FRED" else 'csv'

    # --- FRED Data Download Section ---
    if st.session_state.data_source == 'fred':
        st.header("1. Data Selection and Preparation (FRED)")
        st.subheader("Select Variables")

        for name in PREDEFINED_SERIES:
            if name not in st.session_state:
                st.session_state[name] = False

        col_a, col_b, _ = st.columns([0.15, 0.15, 0.7])
        if col_a.button("Select All"):
            for name in PREDEFINED_SERIES:
                st.session_state[name] = True
        if col_b.button("Deselect All"):
            for name in PREDEFINED_SERIES:
                st.session_state[name] = False
        st.markdown("---")
        
        categorized_series = collections.defaultdict(list)
        for name, info in PREDEFINED_SERIES.items():
            categorized_series[info['category']].append(name)

        columns = st.columns(3)
        sorted_categories = sorted(categorized_series.items())
        for i, (category, names) in enumerate(sorted_categories):
            with columns[i % 3]:
                st.subheader(category)
                for name in names:
                    st.checkbox(name, key=name)

        selected_display_names = [name for name, selected in st.session_state.items() if name in PREDEFINED_SERIES and selected]

        if selected_display_names:
            st.subheader("Select Time Frame and Frequency")
            col1, col2 = st.columns(2)
            start_date = col1.date_input("Start Date", datetime.date(2000, 1, 1))
            end_date = col2.date_input("End Date", datetime.date.today())

            if start_date > end_date:
                st.error("Error: End date must be after or equal to start date.")
            else:
                target_frequency = st.selectbox("Target Frequency:", ["Quarterly", "Monthly"], index=0, help="Quarterly is recommended for credit metrics.")
                col3, col4 = st.columns(2)
                agg_method = col3.selectbox("Aggregation Method:", ['mean', 'sum', 'last', 'first'])
                interp_method = col4.selectbox("Interpolation Method:", ['linear', 'time', 'ffill', 'bfill'])

                if st.button("Fetch and Process Data", type="primary"):
                    with st.spinner("Fetching and preparing data..."):
                        try:
                            selected_fred_ids = [PREDEFINED_SERIES[name]["id"] for name in selected_display_names]
                            raw_data = web.DataReader(name=selected_fred_ids, data_source='fred', start=start_date, end=end_date, api_key=FRED_API_KEY)

                            if isinstance(raw_data, pd.Series):
                                raw_data = raw_data.to_frame(name=selected_fred_ids[0])
                            
                            # Remove any duplicate indices in raw data
                            if raw_data.index.duplicated().any():
                                raw_data = raw_data.groupby(level=0).first()
                            
                            st.toast("Raw data fetched!")

                            id_to_info = {info["id"]: info for info in PREDEFINED_SERIES.values()}
                            freq_map = {'Annual': 'A', 'Quarterly': 'Q', 'Monthly': 'M', 'Weekly': 'W-SUN', 'Daily': 'D'}
                            processed_series_list = []
                            for series_id in raw_data.columns:
                                series_data = raw_data[[series_id]].dropna()
                                if series_data.empty: continue
                                source_freq_str = id_to_info.get(series_id, {}).get("frequency")
                                converted_series = convert_frequency(series_data, source_freq_str, target_frequency, agg_method, interp_method)
                                if converted_series is None or converted_series.empty: continue
                                target_freq_code = freq_map.get(target_frequency)
                                if target_freq_code:
                                    converted_series.index = pd.to_datetime(converted_series.index).to_period(freq=target_freq_code).to_timestamp(how='end')
                                # Remove any duplicate indices that may have been created
                                if converted_series.index.duplicated().any():
                                    converted_series = converted_series.groupby(level=0).first()
                                processed_series_list.append(converted_series)
                            
                            st.toast("Data processed.")
                            
                            if processed_series_list:
                                final_data = pd.concat(processed_series_list, axis=1, join='outer').groupby(level=0).first().sort_index().dropna(how='all')
                                id_to_display_name = {v['id']: k for k, v in PREDEFINED_SERIES.items()}
                                final_data.rename(columns={col: id_to_display_name.get(col, col) for col in final_data.columns}, inplace=True)
                                
                                st.session_state.analysis_data = final_data
                                st.session_state.target_frequency = target_frequency
                                st.session_state.screen = 'analysis'
                                st.session_state.analysis_results = None # Clear old results
                                st.rerun()
                            else:
                                st.warning("No data returned for the selected criteria. Please adjust your selections.")

                        except Exception as e:
                            st.error(f"An error occurred during data preparation: {e}")
        else:
            st.info("Please select at least one macro variable to proceed.")

    # --- CSV Upload Section ---
    else:
        st.header("1. Upload CSV Data")
        uploaded_file = st.file_uploader("Choose a CSV file", type="csv")
        if uploaded_file is not None:
            with st.spinner("Processing uploaded file..."):
                processed_data = process_uploaded_csv(uploaded_file)
                if processed_data is not None and not processed_data.empty:
                    st.session_state.analysis_data = processed_data
                    st.session_state.target_frequency = 'Quarterly'  # Default
                    st.session_state.screen = 'analysis'
                    st.session_state.analysis_results = None # Clear old results
                    st.rerun()
                else:
                    st.error("Failed to process the uploaded CSV file or the file is empty.")

# SCREEN 2: ANALYSIS
elif st.session_state.screen == 'analysis':
    st.button("⬅️ Select Different Data", on_click=go_to_data_selection)
    
    final_data = st.session_state.analysis_data
    target_frequency = st.session_state.target_frequency

    # --- Data Preview and Download Section ---
    st.header("2. Data Preview and Download")
    col1, col2, col3 = st.columns(3)
    col1.metric("Observations", len(final_data))
    col2.metric("Variables", len(final_data.columns))
    col3.metric("Date Range", f"{final_data.index.min().strftime('%Y-%m-%d')} to {final_data.index.max().strftime('%Y-%m-%d')}")
    st.subheader("Data Preview")
    st.dataframe(final_data)
    st.subheader("Download Data")
    
    # Create a copy for download with formatted date
    download_df = final_data.copy()
    download_df.index = download_df.index.strftime('%Y-%m-%d')
    download_df.index.name = "Date"

    col5, col6 = st.columns(2)
    csv_buffer = io.StringIO()
    download_df.to_csv(csv_buffer)
    col5.download_button(label="Download as CSV", data=csv_buffer.getvalue(), file_name=f"macro_data_{datetime.date.today().strftime('%Y%m%d')}.csv", mime="text/csv", use_container_width=True)
    
    excel_buffer = io.BytesIO()
    with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
        download_df.to_excel(writer, sheet_name='Data', index=True)
    col6.download_button(label="Download as Excel", data=excel_buffer.getvalue(), file_name=f"macro_data_{datetime.date.today().strftime('%Y%m%d')}.xlsx", mime="application/vnd.openxmlformats-officedocument.sheetml.sheet", use_container_width=True)

    # --- Data Summary Table Section ---
    st.header("3. Data Summary Table")
    st.write("""
    This table shows all available variables categorized into:
    - **Predictor Variables**: Macro-economic indicators for loss forecasting models
    - **Dependent Variables**: Credit card and mortgage delinquency and charge-off metrics
    """)
    
    # Get all columns from the fetched data
    all_columns = list(final_data.columns)
    
    # Categorize variables
    credit_columns = [col for col in all_columns if 'delinquency' in col.lower() or 'charge-off' in col.lower()]
    macro_columns = [col for col in all_columns if col not in credit_columns]

    # Build summary data
    summary_rows = []
    
    # Add predictor variables
    for col in macro_columns:
        col_data = final_data[col].dropna()
        if not col_data.empty:
            start_date = col_data.index.min().strftime('%Y-%m-%d')
            end_date = col_data.index.max().strftime('%Y-%m-%d')
            data_range = f"{start_date} to {end_date}"
            observations = len(col_data)
    else:
            data_range = "No data"
            observations = 0
        
            summary_rows.append({
            "Variable Type": "Predictor Variable",
            "Variable Name": col,
            "Data Range": data_range,
            "Observations": observations
        })
    
    # Add dependent variables
    for col in credit_columns:
        col_data = final_data[col].dropna()
        if not col_data.empty:
            start_date = col_data.index.min().strftime('%Y-%m-%d')
            end_date = col_data.index.max().strftime('%Y-%m-%d')
            data_range = f"{start_date} to {end_date}"
            observations = len(col_data)
        else:
            data_range = "No data"
            observations = 0
        
        summary_rows.append({
            "Variable Type": "Dependent Variable",
            "Variable Name": col,
            "Data Range": data_range,
            "Observations": observations
        })
    
    # Create and display summary dataframe
    summary_df = pd.DataFrame(summary_rows)
    
    # Style the dataframe for better visualization
    st.subheader("Variables Summary")
    st.dataframe(
        summary_df,
        use_container_width=True,
        hide_index=True
    )
    
    # Add summary statistics
    st.subheader("Quick Statistics")
    col_stat1, col_stat2, col_stat3 = st.columns(3)
    col_stat1.metric("Total Predictor Variables", len(macro_columns))
    col_stat2.metric("Total Dependent Variables", len(credit_columns))
    col_stat3.metric("Total Variables", len(macro_columns) + len(credit_columns))
    
    # --- Descriptive Statistics Table ---
    st.subheader("Descriptive Statistics")
    st.write("""
    Comprehensive statistical summary of all variables including missing values, 
    central tendency measures, and distribution percentiles.
    """)
    
    # Calculate descriptive statistics
    desc_stats = []
    
    for col in all_columns:
        col_data = final_data[col].dropna()
        
        if len(col_data) > 0:
            # Basic statistics
            missing_count = len(final_data[col]) - len(col_data)
            missing_pct = (missing_count / len(final_data[col])) * 100
            
            # Central tendency
            mean_val = col_data.mean()
            median_val = col_data.median()
            
            # Percentiles
            p25 = col_data.quantile(0.25)
            p50 = col_data.quantile(0.50)  # Same as median
            p75 = col_data.quantile(0.75)
            p90 = col_data.quantile(0.90)
            p99 = col_data.quantile(0.99)
            
            # Range
            min_val = col_data.min()
            max_val = col_data.max()
            
            # Standard deviation
            std_val = col_data.std()
            
            desc_stats.append({
                "Variable": col,
                "Missing": f"{missing_count} ({missing_pct:.1f}%)",
                "Count": len(col_data),
                "Mean": f"{mean_val:.2f}",
                "Median": f"{median_val:.2f}",
                "Std Dev": f"{std_val:.2f}",
                "Min": f"{min_val:.2f}",
                "P25": f"{p25:.2f}",
                "P50": f"{p50:.2f}",
                "P75": f"{p75:.2f}",
                "P90": f"{p90:.2f}",
                "P99": f"{p99:.2f}",
                "Max": f"{max_val:.2f}"
            })
        else:
            # Handle case where column has no data
            desc_stats.append({
                "Variable": col,
                "Missing": f"{len(final_data[col])} (100.0%)",
                "Count": "0",
                "Mean": "N/A",
                "Median": "N/A",
                "Std Dev": "N/A",
                "Min": "N/A",
                "P25": "N/A",
                "P50": "N/A",
                "P75": "N/A",
                "P90": "N/A",
                "P99": "N/A",
                "Max": "N/A"
            })
    
    # Create and display descriptive statistics dataframe
    desc_stats_df = pd.DataFrame(desc_stats)
    
    # Display the table with better formatting
    st.dataframe(
        desc_stats_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Variable": st.column_config.TextColumn(
                "Variable Name",
                width="large",
                help="Name of the economic indicator"
            ),
            "Missing": st.column_config.TextColumn(
                "Missing Values",
                width="medium",
                help="Number and percentage of missing values"
            ),
            "Count": st.column_config.TextColumn(
                "Valid Count",
                width="small",
                help="Number of non-missing observations"
            ),
            "Mean": st.column_config.TextColumn(
                "Mean",
                width="small",
                help="Arithmetic mean"
            ),
            "Median": st.column_config.TextColumn(
                "Median",
                width="small",
                help="50th percentile (middle value)"
            ),
            "Std Dev": st.column_config.TextColumn(
                "Std Dev",
                width="small",
                help="Standard deviation"
            ),
            "Min": st.column_config.TextColumn(
                "Min",
                width="small",
                help="Minimum value"
            ),
            "P25": st.column_config.TextColumn(
                "25th %ile",
                width="small",
                help="25th percentile"
            ),
            "P50": st.column_config.TextColumn(
                "50th %ile",
                width="small",
                help="50th percentile (median)"
            ),
            "P75": st.column_config.TextColumn(
                "75th %ile",
                width="small",
                help="75th percentile"
            ),
            "P90": st.column_config.TextColumn(
                "90th %ile",
                width="small",
                help="90th percentile"
            ),
            "P99": st.column_config.TextColumn(
                "99th %ile",
                width="small",
                help="99th percentile"
            ),
            "Max": st.column_config.TextColumn(
                "Max",
                width="small",
                help="Maximum value"
            )
        }
    )
    
    # Add download option for descriptive statistics
    st.subheader("Download Descriptive Statistics")
    col_desc1, col_desc2 = st.columns(2)
    
    # CSV download for descriptive stats
    desc_csv_buffer = io.StringIO()
    desc_stats_df.to_csv(desc_csv_buffer, index=False)
    col_desc1.download_button(
        label="Download Descriptive Stats as CSV", 
        data=desc_csv_buffer.getvalue(), 
        file_name=f"descriptive_statistics_{datetime.date.today().strftime('%Y%m%d')}.csv", 
        mime="text/csv", 
        use_container_width=True
    )
    
    # Excel download for descriptive stats
    desc_excel_buffer = io.BytesIO()
    with pd.ExcelWriter(desc_excel_buffer, engine='xlsxwriter') as writer:
        desc_stats_df.to_excel(writer, sheet_name='Descriptive Stats', index=False)
    col_desc2.download_button(
        label="Download Descriptive Stats as Excel", 
        data=desc_excel_buffer.getvalue(), 
        file_name=f"descriptive_statistics_{datetime.date.today().strftime('%Y%m%d')}.xlsx", 
        mime="application/vnd.openxmlformats-officedocument.sheetml.sheet", 
        use_container_width=True
    )

    # --- Data Transformation Module ---
    st.header("4. Data Transformation Module")
    st.write("""
    This module provides comprehensive data transformations for loss forecasting model development.
    You can use data from the previous step or upload your own dataset.
    """)
    
    # Data source selection
    st.subheader("Data Source Selection")
    transform_data_source = st.radio(
        "Choose data source for transformations:",
        ["Use FRED data from previous step", "Upload new CSV file"],
        key="transform_data_source"
    )
    
    transform_data = None
    
    if transform_data_source == "Use FRED data from previous step":
        if final_data is not None and not final_data.empty:
            transform_data = final_data.copy()
            st.success(f"Using FRED data with {len(transform_data)} observations and {len(transform_data.columns)} variables.")
        else:
            st.warning("No FRED data available. Please fetch data first or upload a CSV file.")
    else:
        uploaded_transform_file = st.file_uploader("Upload CSV file for transformations", type="csv", key="transform_csv")
        if uploaded_transform_file is not None:
            with st.spinner("Processing uploaded file..."):
                transform_data = process_uploaded_csv(uploaded_transform_file)
                if transform_data is not None and not transform_data.empty:
                    st.success(f"Uploaded data processed: {len(transform_data)} observations and {len(transform_data.columns)} variables.")
                else:
                    st.error("Failed to process the uploaded CSV file.")
    
    if transform_data is not None and not transform_data.empty:
        # Variable selection for modeling
        st.subheader("Variable Selection for Modeling")
        
        all_transform_columns = list(transform_data.columns)
        credit_columns = [col for col in all_transform_columns if 'delinquency' in col.lower() or 'charge-off' in col.lower()]
        macro_columns = [col for col in all_transform_columns if col not in credit_columns]
        
        col_dep, col_anchor, col_time = st.columns(3)
        
        with col_dep:
            st.write("**Dependent Variable Selection**")
            if credit_columns:
                dependent_variable = st.selectbox(
                    "Select dependent variable (Y):",
                options=credit_columns,
                    key="dependent_var_select",
                    help="Target variable for the loss forecasting model"
                )
            else:
                st.warning("No delinquency or charge-off variables found. Please select from all variables:")
                dependent_variable = st.selectbox(
                    "Select dependent variable (Y):",
                    options=all_transform_columns,
                    key="dependent_var_select_all",
                    help="Target variable for the loss forecasting model"
                )
        
        with col_anchor:
            st.write("**Anchor Variable Selection**")
            if credit_columns:
                anchor_variable = st.selectbox(
                    "Select anchor variable:",
                    options=credit_columns,
                    key="anchor_var_select",
                    help="Variable that remains constant across performance quarters for each snapshot"
                )
            else:
                st.warning("No delinquency or charge-off variables found for anchor selection:")
                anchor_variable = st.selectbox(
                    "Select anchor variable:",
                    options=all_transform_columns,
                    key="anchor_var_select_all",
                    help="Variable that remains constant across performance quarters for each snapshot"
                )
        
        with col_time:
            st.write("**Time Varying Variables Selection**")
            available_time_varying = [col for col in macro_columns if col not in [dependent_variable, anchor_variable]]
            time_varying_variables = st.multiselect(
                "Select time varying variables:",
                options=available_time_varying,
                default=available_time_varying[:3] if available_time_varying else [],
                key="time_varying_vars_select",
                help="Macro-economic variables that will undergo transformations"
            )
        
        if dependent_variable and anchor_variable and time_varying_variables:
            st.success(f"Selected {len(time_varying_variables)} time varying variables for transformations.")
            
            # Transformation buttons
            st.subheader("Data Transformations")
            
            col_trans1, col_trans2 = st.columns(2)
            
            with col_trans1:
                if st.button("🔄 Macro Transformations", type="primary", key="macro_transform_btn"):
                    with st.spinner("Performing macro transformations..."):
                        try:
                            # Step 1: Clean variable names
                            cleaned_names = clean_variable_names(transform_data)
                            transform_data_cleaned = transform_data.rename(columns=cleaned_names)
                            
                            # Step 2: Create aliases
                            aliases = create_variable_aliases()
                            
                            # Step 3: Map time varying variables to cleaned names
                            cleaned_time_varying_vars = []
                            for var in time_varying_variables:
                                if var in cleaned_names:
                                    cleaned_time_varying_vars.append(cleaned_names[var])
                            else:
                                    cleaned_time_varying_vars.append(var)
                            
                            # Step 4: Create macro transformations for time varying variables only
                            transformed_data = create_macro_transformations(transform_data_cleaned, cleaned_time_varying_vars)
                            
                            # Step 4: Test stationarity
                            all_vars = list(transformed_data.columns)
                            stationarity_results = test_stationarity(transformed_data, all_vars)
                            
                            # Step 5: Create final data with all variables
                            # Include all original variables (dependent, anchor, time varying) plus stationary transformations
                            stationary_vars = stationarity_results[stationarity_results['Is_Stationary'] == True]['Variable'].tolist()
                            
                            # Start with original data and add stationary transformations
                            final_transformed_data = transform_data_cleaned.copy()
                            
                            # Add stationary transformations with proper index alignment
                            for var in stationary_vars:
                                if var in transformed_data.columns:
                                    # Use intersection of indices to avoid index mismatch
                                    common_index = final_transformed_data.index.intersection(transformed_data.index)
                                    if len(common_index) > 0:
                                        final_transformed_data.loc[common_index, var] = transformed_data.loc[common_index, var]
                            
                            # Remove rows where any variable is missing
                            final_transformed_data = final_transformed_data.dropna()
                            
                            # Ensure index is properly formatted as datetime for panel data creation
                            if not hasattr(final_transformed_data.index, 'year'):
                                # If index is not datetime-like, convert it
                                try:
                                    final_transformed_data.index = pd.to_datetime(final_transformed_data.index)
                                except:
                                    # If conversion fails, keep original index
                                    pass
                            
                            # Store results in session state
                            st.session_state.transformed_data = final_transformed_data
                            st.session_state.stationarity_results = stationarity_results
                            st.session_state.aliases = aliases
                            st.session_state.cleaned_names = cleaned_names
                            st.session_state.transform_data = transform_data  # Store original data for panel creation
                            
                            st.success(f"Macro transformations completed! {len(stationary_vars)} stationary variables retained.")
                            
                            # Display results
                            st.subheader("Transformation Results")
                            
                            col_results1, col_results2 = st.columns(2)
                            
                            with col_results1:
                                st.metric("Original Variables", len(transform_data.columns))
                                st.metric("Transformed Variables", len(transformed_data.columns))
                                st.metric("Stationary Variables", len(stationary_vars))
                                st.metric("Final Observations", len(final_transformed_data))
                            
                            with col_results2:
                                st.metric("Variables Dropped", len(transformed_data.columns) - len(stationary_vars))
                                st.metric("Missing Records Dropped", len(transformed_data) - len(final_transformed_data))
                            
                            # Display stationarity results
                            st.subheader("Stationarity Test Results")
                            st.dataframe(stationarity_results, use_container_width=True, hide_index=True)
                            
                            # Download options
                            st.subheader("Download Transformation Results")
                            
                            col_download1, col_download2, col_download3 = st.columns(3)
                            
                            # Download transformed data
                            with col_download1:
                                transform_csv_buffer = io.StringIO()
                                final_transformed_data.to_csv(transform_csv_buffer)
                                st.download_button(
                                    label="Download Transformed Data (CSV)",
                                    data=transform_csv_buffer.getvalue(),
                                    file_name=f"transformed_data_{datetime.date.today().strftime('%Y%m%d')}.csv",
                                    mime="text/csv",
                                    use_container_width=True
                                )
                            
                            # Download stationarity results
                            with col_download2:
                                stationarity_csv_buffer = io.StringIO()
                                stationarity_results.to_csv(stationarity_csv_buffer, index=False)
                                st.download_button(
                                    label="Download Stationarity Results (CSV)",
                                    data=stationarity_csv_buffer.getvalue(),
                                    file_name=f"stationarity_results_{datetime.date.today().strftime('%Y%m%d')}.csv",
                                    mime="text/csv",
                                    use_container_width=True
                                )
                            
                            # Download Excel with multiple sheets
                            with col_download3:
                                excel_buffer = io.BytesIO()
                                with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                                    final_transformed_data.to_excel(writer, sheet_name='Transformed_Data', index=True)
                                    stationarity_results.to_excel(writer, sheet_name='Stationarity_Results', index=False)
                                st.download_button(
                                    label="Download All Results (Excel)",
                                    data=excel_buffer.getvalue(),
                                    file_name=f"transformation_results_{datetime.date.today().strftime('%Y%m%d')}.xlsx",
                                    mime="application/vnd.openxmlformats-officedocument.sheetml.sheet",
                                    use_container_width=True
                                )
                            
                    except Exception as e:
                            st.error(f"Error during macro transformations: {e}")
            
            with col_trans2:
                if st.button("📊 Panel Data Creation", type="primary", key="panel_data_btn"):
                    if 'transformed_data' in st.session_state:
                        with st.spinner("Creating panel data..."):
                            try:
                                # Get cleaned names from session state
                                cleaned_names = st.session_state.get('cleaned_names', {})
                                
                                # Use the selected variables from the variable selection
                                st.subheader("Panel Data Configuration")
                                
                                # Map anchor variable to cleaned name
                                cleaned_anchor_var = None
                                if anchor_variable in cleaned_names:
                                    cleaned_anchor_var = cleaned_names[anchor_variable]
                                else:
                                    cleaned_anchor_var = anchor_variable
                                
                                # Map dependent variable to cleaned name
                                cleaned_dependent_var = None
                                if dependent_variable in cleaned_names:
                                    cleaned_dependent_var = cleaned_names[dependent_variable]
                                else:
                                    cleaned_dependent_var = dependent_variable
                                
                                # Get all stationary variables (excluding anchor and dependent variables)
                                stationary_vars = st.session_state.stationarity_results[st.session_state.stationarity_results['Is_Stationary'] == True]['Variable'].tolist()
                                macro_transformations = [var for var in stationary_vars if var not in [cleaned_anchor_var, cleaned_dependent_var]]
                                
                                # Use the final transformed data directly (includes all variables)
                                panel_data_source = st.session_state.transformed_data.copy()
                                
                                st.info(f"Using {len(macro_transformations)} stationary macro transformations for panel data.")
                                
                                # Create panel data
                                panel_data = create_panel_data(
                                    panel_data_source,
                                    cleaned_anchor_var,
                                    cleaned_dependent_var,
                                    macro_transformations,
                                    performance_quarters=13
                                )
                                
                                # Merge dependent variable by calendar quarter
                                dependent_data = panel_data_source[[cleaned_dependent_var]].reset_index()
                                dependent_data['Calendar_Qtr'] = dependent_data['index'].apply(lambda x: f"{x.year}Q{(x.month - 1) // 3 + 1}")
                                dependent_data = dependent_data.rename(columns={cleaned_dependent_var: 'Dependent_Variable'})
                                
                                # Merge dependent variable to panel data
                                panel_data = panel_data.merge(
                                    dependent_data[['Calendar_Qtr', 'Dependent_Variable']], 
                                    on='Calendar_Qtr', 
                                    how='left'
                                )
                                
                                # Calculate average dependent variable by performance quarter
                                avg_dependent_by_perf = panel_data.groupby('Performance_Quarter')['Dependent_Variable'].mean().reset_index()
                                avg_dependent_by_perf = avg_dependent_by_perf.rename(columns={'Dependent_Variable': 'Avg_Dependent_By_Perf'})
                                
                                # Merge average dependent variable back to panel data
                                panel_data = panel_data.merge(avg_dependent_by_perf, on='Performance_Quarter', how='left')
                                
                                # Store in session state
                                st.session_state.panel_data = panel_data
                                
                                st.success(f"Panel data created successfully! {len(panel_data)} observations.")
                                
                                # Display panel data summary
                                st.subheader("Panel Data Summary")
                                
                                col_panel1, col_panel2, col_panel3 = st.columns(3)
                                
                                with col_panel1:
                                    st.metric("Total Observations", len(panel_data))
                                    st.metric("Unique Snapshots", panel_data['Snapshot_Date'].nunique())
                                    st.metric("Anchor Variable", cleaned_anchor_var)
                                
                                with col_panel2:
                                    st.metric("Performance Quarters", panel_data['Performance_Quarter'].max())
                                    st.metric("Macro Variables", len(macro_transformations))
                                    st.metric("Dependent Variable", cleaned_dependent_var)
                                
                                with col_panel3:
                                    st.metric("Date Range", f"{panel_data['Snapshot_Date'].min()} to {panel_data['Snapshot_Date'].max()}")
                                    st.metric("Total Variables", len(panel_data.columns))
                                
                                # Display panel data structure
                                st.subheader("Panel Data Structure")
                                st.write("""
                                **Panel Data Components:**
                                - **Snapshot Variables**: Anchor variable at time T (constant across performance quarters)
                                - **Performance Variables**: Macro transformations aligned to calendar quarter (T+k)
                                - **Dependent Variable**: Merged by calendar quarter
                                - **Average Dependent**: Average dependent variable by performance quarter
                                """)
                                
                                # Display sample of panel data
                                st.subheader("Panel Data Preview")
                                display_columns = ['Snapshot_Date', 'Performance_Quarter', 'Calendar_Qtr', f'Snapshot_{cleaned_anchor_var}', 'Dependent_Variable', 'Avg_Dependent_By_Perf']
                                available_display_columns = [col for col in display_columns if col in panel_data.columns]
                                st.dataframe(panel_data[available_display_columns].head(20), use_container_width=True)
                                
                                # Download panel data
                                st.subheader("Download Panel Data")
                                
                                col_panel_download1, col_panel_download2 = st.columns(2)
                                
                                with col_panel_download1:
                                    panel_csv_buffer = io.StringIO()
                                    panel_data.to_csv(panel_csv_buffer, index=False)
                                    st.download_button(
                                        label="Download Panel Data (CSV)",
                                        data=panel_csv_buffer.getvalue(),
                                        file_name=f"panel_data_{datetime.date.today().strftime('%Y%m%d')}.csv",
                                        mime="text/csv",
                                        use_container_width=True
                                    )
                                
                                with col_panel_download2:
                                    panel_excel_buffer = io.BytesIO()
                                    with pd.ExcelWriter(panel_excel_buffer, engine='xlsxwriter') as writer:
                                        panel_data.to_excel(writer, sheet_name='Panel_Data', index=False)
                                        if 'transformed_data' in st.session_state:
                                            st.session_state.transformed_data.to_excel(writer, sheet_name='Transformed_Data', index=True)
                                        if 'stationarity_results' in st.session_state:
                                            st.session_state.stationarity_results.to_excel(writer, sheet_name='Stationarity_Results', index=False)
                                    st.download_button(
                                        label="Download Complete Dataset (Excel)",
                                        data=panel_excel_buffer.getvalue(),
                                        file_name=f"complete_dataset_{datetime.date.today().strftime('%Y%m%d')}.xlsx",
                                        mime="application/vnd.openxmlformats-officedocument.sheetml.sheet",
                                        use_container_width=True
                                    )
                                
                            except Exception as e:
                                st.error(f"Error during panel data creation: {e}")
                    else:
                        st.warning("Please run Macro Transformations first to create the transformed dataset.")
        else:
            st.warning("Please select both dependent and predictor variables to proceed with transformations.")

    # --- Notes Section ---
    st.header("5. Notes")
    st.write("""
    **About This Application:**
    - This tool is designed for loss forecasting model development
    - **Predictor Variables**: Use macro-economic indicators to forecast credit performance
    - **Dependent Variables**: Credit card and mortgage delinquency/charge-off rates
    - All data is sourced from FRED (Federal Reserve Economic Data)
    - Data is processed to the selected frequency (Monthly or Quarterly)
    
    **Data Transformation Features:**
    - **Macro Transformations**: YoY/QoQ changes, moving averages, lags, stationarity testing
    - **Panel Data Creation**: Anchoring with performance quarters for loss forecasting
    - **Export Options**: CSV and Excel downloads for all transformation results
    
    **Next Steps:**
    - Use the transformation module to prepare data for modeling
    - Download transformed datasets for statistical software
    - Build econometric models to forecast credit losses
    - Consider the panel data structure for time-series analysis
    """)

# --- Footer ---
st.markdown("---")
st.markdown("**Note:** This application uses the FRED API for economic data. For more information, visit [FRED Economic Data](https://fred.stlouisfed.org/).")
