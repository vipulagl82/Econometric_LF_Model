import streamlit as st
import pandas as pd
import pandas_datareader.data as web
import datetime
from fredapi import Fred
import io
import os
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
# Retrieve the secret and assign it
try:
    FRED_API_KEY = st.secrets["FRED_API_KEY"]
    os.environ['FRED_API_KEY'] = FRED_API_KEY
except KeyError:
    FRED_API_KEY = None

# Check if the key is valid
if not FRED_API_KEY:
    st.error("FRED API Key is missing. Please add 'FRED_API_KEY' to your secrets.")
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

    # --- Bivariate Analysis Section ---
    st.header("3. Bivariate Analysis")
    st.subheader("Select Variables for Analysis")

    all_columns = list(final_data.columns)
    credit_columns = [col for col in all_columns if 'delinquency' in col.lower() or 'charge-off' in col.lower()]
    macro_columns = [col for col in all_columns if col not in credit_columns]

    if not credit_columns:
        st.warning("No delinquency or charge-off variables found for dependent variable selection.")
    elif not macro_columns:
        st.warning("No macro-economic variables found for independent variable selection.")
    else:
        col_left, col_right = st.columns(2)
        with col_left:
            dependent_var_name = st.selectbox(
                "Select Dependent Variable (Y-axis):",
                options=credit_columns,
                key="dv_select_main"
            )
        with col_right:
            independent_vars_names = st.multiselect(
                "Select Independent Variables (X-axis):",
                options=macro_columns,
                default=macro_columns[:1] if macro_columns else [],
                key="iv_multi_select_main"
            )

        if not independent_vars_names:
            st.warning("Please select at least one independent variable for analysis.")
        else:
            if st.button("Run Bivariate Analysis", type="primary"):
                all_analysis_content = []
                with st.spinner("Running all analyses..."):
                    for iv_name in independent_vars_names:
                        try:
                            pair_df = final_data[[dependent_var_name, iv_name]].dropna(how='any')
                            if pair_df.empty or len(pair_df) < 8:
                                st.warning(f"Not enough overlapping data for {iv_name}.")
                                continue
                            
                            transformed_pair_df, stationarity_summary = check_stationarity_and_transform(pair_df, target_frequency)
                            analysis_df = transformed_pair_df.dropna()

                            if analysis_df.empty or len(analysis_df) < 8:
                                st.warning(f"Not enough data after transformation for {iv_name}.")
                                continue
                            
                            analysis_df_for_chart = analysis_df.reset_index()
                            date_col_name = analysis_df_for_chart.columns[0]
                            df_melted = analysis_df_for_chart.melt(id_vars=[date_col_name], var_name='Variable', value_name='Value')
                            base = alt.Chart(df_melted).encode(x=alt.X(f'{date_col_name}:T', title='Date'))
                            line1 = base.transform_filter(alt.datum.Variable == dependent_var_name).mark_line().encode(y=alt.Y('Value:Q', title=dependent_var_name, axis=alt.Axis(titleColor='#5276A7')), color=alt.value("#5276A7"))
                            line2 = base.transform_filter(alt.datum.Variable == iv_name).mark_line(strokeDash=[5,5]).encode(y=alt.Y('Value:Q', title=iv_name, axis=alt.Axis(titleColor='#F1872B')), color=alt.value("#F1872B"))
                            time_series_chart = alt.layer(line1, line2).resolve_scale(y='independent').properties(title=f"Time Series: {dependent_var_name} vs. {iv_name} (Transformed)", width=800, height=400).interactive()
                            
                            window_size = min(8, len(analysis_df) // 3) if len(analysis_df) // 3 > 0 else 1
                            rolling_corr = analysis_df[dependent_var_name].rolling(window=window_size).corr(analysis_df[iv_name]).dropna()
                            rolling_corr_chart_obj = None
                            if not rolling_corr.empty and len(rolling_corr) > 1:
                                rolling_corr_df = rolling_corr.reset_index(); rolling_corr_df.columns = ['Date', 'Correlation']
                                rolling_corr_chart = alt.Chart(rolling_corr_df).mark_line(color='goldenrod').encode(x=alt.X('Date:T', title='Date'), y=alt.Y('Correlation:Q', title='Correlation', scale=alt.Scale(domain=[-1, 1]))).properties(title=f"{window_size}-Period Rolling Correlation", width=800, height=300)
                                rolling_corr_text = f"The {window_size}-period rolling correlation ranges from {rolling_corr.min():.2f} to {rolling_corr.max():.2f}, with recent correlation of {rolling_corr.iloc[-1]:.2f}."
                                rolling_corr_chart_obj = rolling_corr_chart
                            else:
                                rolling_corr_text = "Rolling correlation could not be computed due to insufficient data."

                            correlation_val = analysis_df[dependent_var_name].corr(analysis_df[iv_name])
                            stationarity_info_str = stationarity_summary[stationarity_summary['Variable'].isin([dependent_var_name, iv_name])].to_json(orient='records')
                            analysis_text = get_basic_analysis(dependent_var_name, iv_name, correlation_val, rolling_corr_text, stationarity_info_str)
                            
                            all_analysis_content.append({"dv": dependent_var_name, "iv": iv_name, "ts_chart_obj": time_series_chart, "corr_chart_obj": rolling_corr_chart_obj, "analysis_text": analysis_text, "correlation": correlation_val, "stationarity_summary": stationarity_summary})
                        except Exception as e:
                            st.error(f"An error during analysis for {iv_name}: {e}")
                            continue
                    st.session_state['analysis_results'] = all_analysis_content
                if st.session_state.get('analysis_results'):
                    st.success("All analyses completed successfully!")

    # --- Display Bivariate Analysis Results ---
    if st.session_state.get('analysis_results'):
        st.header("Bivariate Analysis Results")
        for result in st.session_state.analysis_results:
            st.markdown(f"### Analysis: `{result['dv']}` vs. `{result['iv']}`")
            with st.expander("Stationarity Test and Transformation Details"):
                st.dataframe(result['stationarity_summary'].set_index("Variable"))
            st.altair_chart(result['ts_chart_obj'], use_container_width=True)
            if result['corr_chart_obj']:
                st.altair_chart(result['corr_chart_obj'], use_container_width=True)
            st.markdown(result['analysis_text'])
            st.markdown("---")


    # --- Report Generation Section ---
    if st.session_state.get('analysis_results'):
        st.header("4. Generate Report")
        if st.button("Generate Word Document", type="primary"):
            with st.spinner("Creating Word document..."):
                try:
                    doc = Document()
                    doc.add_heading('Bivariate Analysis Report', 0)
                    doc.add_paragraph(f'Generated on: {datetime.date.today().strftime("%B %d, %Y")}')
                    doc.add_paragraph('')
                    for i, analysis in enumerate(st.session_state['analysis_results'], 1):
                        doc.add_heading(f"{i}. Analysis: {analysis['dv']} vs. {analysis['iv']}", level=1)
                        doc.add_paragraph(f"Overall Correlation: {analysis['correlation']:.4f}\n")
                        ts_chart_img = save_chart(analysis['ts_chart_obj'])
                        if ts_chart_img:
                            doc.add_heading("Time Series Analysis", level=2)
                            try: doc.add_picture(ts_chart_img, width=Inches(6.0))
                            except: doc.add_paragraph("Chart could not be embedded.")
                            doc.add_paragraph('')
                        if analysis['corr_chart_obj']:
                            corr_chart_img = save_chart(analysis['corr_chart_obj'])
                            if corr_chart_img:
                                doc.add_heading("Rolling Correlation Analysis", level=2)
                                try: doc.add_picture(corr_chart_img, width=Inches(6.0))
                                except: doc.add_paragraph("Chart could not be embedded.")
                                doc.add_paragraph('')
                        doc.add_heading("Economic Analysis", level=2)
                        doc.add_paragraph(analysis['analysis_text'])
                        if i < len(st.session_state['analysis_results']): doc.add_page_break()
                    
                    doc_io = io.BytesIO()
                    doc.save(doc_io)
                    doc_io.seek(0)
                    st.download_button(label="Download Analysis Report", data=doc_io, file_name=f"Bivariate_Analysis_Report_{datetime.date.today().strftime('%Y%m%d')}.docx", mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document", use_container_width=True)
                    st.success("Report generated successfully!")
                except Exception as e:
                    st.error(f"Error generating report: {e}")

# --- Footer ---
st.markdown("---")
st.markdown("**Note:** This application uses the FRED API for economic data. For more information, visit [FRED Economic Data](https://fred.stlouisfed.org/).")
