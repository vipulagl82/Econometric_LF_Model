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

    # --- Notes Section ---
    st.header("4. Notes")
    st.write("""
    **About This Application:**
    - This tool is designed for loss forecasting model development
    - **Predictor Variables**: Use macro-economic indicators to forecast credit performance
    - **Dependent Variables**: Credit card and mortgage delinquency/charge-off rates
    - All data is sourced from FRED (Federal Reserve Economic Data)
    - Data is processed to the selected frequency (Monthly or Quarterly)
    
    **Next Steps:**
    - Download the data using the buttons above
    - Import into your statistical software (Python, R, SAS, etc.)
    - Build econometric models to forecast credit losses
    - Consider lag structures and transformations as needed
    """)

# --- Footer ---
st.markdown("---")
st.markdown("**Note:** This application uses the FRED API for economic data. For more information, visit [FRED Economic Data](https://fred.stlouisfed.org/).")
