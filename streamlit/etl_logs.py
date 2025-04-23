import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from datetime import datetime, timedelta
import psycopg2
import os
from psycopg2.extras import RealDictCursor

def connect_db():
    return psycopg2.connect(
        dbname=os.environ.get("DB_NAME"),
        user=os.environ.get("DB_USERNAME"),
        password=os.environ.get("DB_PASSWORD"),
        host="localhost",
        port=5433,
    )

def execute_query(query, params=None, fetch=False):
    conn = connect_db()
    cur = conn.cursor()
    cur.execute(query, params or ())
    result = cur.fetchall() if fetch else None
    conn.commit()
    cur.close()
    conn.close()
    return result

def query_to_dataframe(query, columns=None):
    try:
        results = execute_query(query, fetch=True)
        if results:
            if columns:
                return pd.DataFrame(results, columns=columns)
            else:
                return pd.DataFrame(results, columns=[f"col_{i}" for i in range(len(results[0]))])
        else:
            return pd.DataFrame()
    except Exception as e:
        st.error(f"Database query error: {e}")
        return pd.DataFrame()

def convert_timestamp(batch_timestamp):
    if batch_timestamp:
        return datetime.fromtimestamp(batch_timestamp / 1000)
    return None

st.set_page_config(page_title="ETL Pipeline Monitoring", layout="wide")

st.sidebar.title("ETL Pipeline Monitor")

st.sidebar.subheader("Date Range")
default_start_date = datetime(2021, 4, 1)
default_end_date = datetime(2021, 5, 1)

start_date = st.sidebar.date_input("Start Date", default_start_date)
end_date = st.sidebar.date_input("End Date", default_end_date)

countries_query = "SELECT id, name FROM extract.country ORDER BY name"
countries_df = query_to_dataframe(countries_query, columns=['id', 'name'])
all_countries = st.sidebar.checkbox("All Countries", value=True)
selected_countries = []

if not all_countries:
    if not countries_df.empty:
        selected_countries = st.sidebar.multiselect(
            "Select Countries",
            options=countries_df['name'].tolist(),
            default=countries_df['name'].tolist()[:3] if len(countries_df) >= 3 else countries_df['name'].tolist()
        )
        selected_country_ids = countries_df[countries_df['name'].isin(selected_countries)]['id'].tolist()
    else:
        st.sidebar.warning("No countries found in database")
        selected_country_ids = []
else:
    selected_country_ids = countries_df['id'].tolist() if not countries_df.empty else []

status_options = ["All", "Success", "Failed", "In Progress"]
selected_status = st.sidebar.selectbox("Filter by Status", status_options)

api_query = "SELECT id, type FROM extract.api ORDER BY type"
api_df = query_to_dataframe(api_query, columns=['id', 'type'])
all_apis = st.sidebar.checkbox("All API Types", value=True)
selected_apis = []

if not all_apis:
    if not api_df.empty:
        selected_apis = st.sidebar.multiselect(
            "Select API Types",
            options=api_df['type'].tolist(),
            default=api_df['type'].tolist()[:2] if len(api_df) >= 2 else api_df['type'].tolist()
        )
        selected_api_ids = api_df[api_df['type'].isin(selected_apis)]['id'].tolist()
    else:
        st.sidebar.warning("No API types found in database")
        selected_api_ids = []
else:
    selected_api_ids = api_df['id'].tolist() if not api_df.empty else []

if st.sidebar.button("Refresh Data"):
    st.rerun()

country_filter = ""
if not all_countries and selected_country_ids:
    country_ids_str = ", ".join(map(str, selected_country_ids))
    country_filter = f"AND il.country_id IN ({country_ids_str})"

api_filter = ""
if not all_apis and selected_api_ids:
    api_ids_str = ", ".join(map(str, selected_api_ids))
    api_filter = f"AND ail.api_id IN ({api_ids_str})"

status_filter = ""
if selected_status != "All":
    status_map = {"Success": "'Processed'", "Failed": "'Error'", "In Progress": "'in_progress'"}
    status_filter = f"AND (tl.status = {status_map[selected_status]} OR ll.status = {status_map[selected_status]})"

st.title("ETL Pipeline Monitoring Dashboard")

st.write(f"Showing data from {start_date} to {end_date}")

col1, col2, col3, col4 = st.columns(4)

total_files_query = f"""
SELECT COUNT(*) as total_files 
FROM extract.import_logs il
WHERE il.backfill_date BETWEEN '{start_date}' AND '{end_date}'
{country_filter}
"""
total_files_result = execute_query(total_files_query, fetch=True)
total_files = total_files_result[0][0] if total_files_result else 0

col1.metric("Total Files Processed", total_files)

api_calls_query = f"""
SELECT COUNT(*) as total_api_calls 
FROM extract.api_import_logs ail
JOIN extract.import_logs il ON ail.import_logs_id = il.id
WHERE il.backfill_date BETWEEN '{start_date}' AND '{end_date}'
{country_filter}
{api_filter}
"""
api_calls_result = execute_query(api_calls_query, fetch=True)
api_calls = api_calls_result[0][0] if api_calls_result else 0

col2.metric("Total API Calls", api_calls)

transform_success_query = f"""
SELECT 
    COUNT(*) as total,
    SUM(CASE WHEN tl.status = 'Processed' THEN 1 ELSE 0 END) as successful
FROM transform.logs tl
JOIN extract.import_logs il ON tl.import_logs_id = il.id
WHERE il.backfill_date BETWEEN '{start_date}' AND '{end_date}'
{country_filter}
"""
transform_success_result = execute_query(transform_success_query, fetch=True)
if transform_success_result and transform_success_result[0][0] > 0:
    transform_success_rate = round((transform_success_result[0][1] / transform_success_result[0][0]) * 100, 1)
else:
    transform_success_rate = 0

col3.metric("Transform Success Rate", f"{transform_success_rate}%")

load_success_query = f"""
SELECT 
    COUNT(*) as total,
    SUM(CASE WHEN tl.status = 'Processed' THEN 1 ELSE 0 END) as successful
FROM load.logs ll
JOIN transform.logs tl ON ll.transform_logs_id = tl.id
JOIN extract.import_logs il ON tl.import_logs_id = il.id
WHERE il.backfill_date BETWEEN '{start_date}' AND '{end_date}'
{country_filter}
{status_filter}
"""
load_success_result = execute_query(load_success_query, fetch=True)
if load_success_result and load_success_result[0][0] > 0:
    load_success_rate = round((load_success_result[0][1] / load_success_result[0][0]) * 100, 1)
else:
    load_success_rate = 0

col4.metric("Load Success Rate", f"{load_success_rate}%")

st.subheader("ETL Pipeline Performance")
chart_col1, chart_col2 = st.columns(2)

pipeline_status_query = f"""
SELECT 
    c.name as country,
    COUNT(DISTINCT il.id) as total_files,
    COUNT(DISTINCT tl.id) as transformed,
    COUNT(DISTINCT ll.id) as loaded,
    SUM(CASE WHEN tl.status = 'Processed' THEN 1 ELSE 0 END) as transform_success
FROM extract.import_logs il
LEFT JOIN extract.country c ON il.country_id = c.id
LEFT JOIN transform.logs tl ON il.id = tl.import_logs_id
LEFT JOIN load.logs ll ON tl.id = ll.transform_logs_id
WHERE il.backfill_date BETWEEN '{start_date}' AND '{end_date}'
{country_filter}
{status_filter}
GROUP BY c.name
ORDER BY total_files DESC
"""
pipeline_status_result = execute_query(pipeline_status_query, fetch=True)
pipeline_status_columns = ['country', 'total_files', 'transformed', 'loaded', 'transform_success']
pipeline_status_df = pd.DataFrame(pipeline_status_result, columns=pipeline_status_columns) if pipeline_status_result else pd.DataFrame()

if not pipeline_status_df.empty:
    fig1 = go.Figure()
    fig1.add_trace(go.Bar(
        x=pipeline_status_df['country'],
        y=pipeline_status_df['total_files'],
        name='Extracted',
        marker_color='lightblue'
    ))
    fig1.add_trace(go.Bar(
        x=pipeline_status_df['country'],
        y=pipeline_status_df['transform_success'],
        name='Transformed',
        marker_color='orange'
    ))
    fig1.add_trace(go.Bar(
        x=pipeline_status_df['country'],
        y=pipeline_status_df['loaded'],
        name='Loaded',
        marker_color='green'
    ))
    fig1.update_layout(
        title='ETL Pipeline Status by Country',
        xaxis_title='Country',
        yaxis_title='Count',
        barmode='group',
        height=400
    )
    chart_col1.plotly_chart(fig1, use_container_width=True)
else:
    chart_col1.info("No pipeline status data available for the selected filters")

time_trend_query = f"""
SELECT 
    il.backfill_date as process_date,
    COUNT(DISTINCT il.id) as extractions,
    COUNT(DISTINCT tl.id) as transformations,
    COUNT(DISTINCT ll.id) as loads
FROM extract.import_logs il
LEFT JOIN transform.logs tl ON il.id = tl.import_logs_id
LEFT JOIN load.logs ll ON tl.id = ll.transform_logs_id
WHERE il.backfill_date BETWEEN '{start_date}' AND '{end_date}'
{country_filter}
{status_filter}
GROUP BY process_date
ORDER BY process_date
"""
time_trend_result = execute_query(time_trend_query, fetch=True)
time_trend_columns = ['process_date', 'extractions', 'transformations', 'loads']
time_trend_df = pd.DataFrame(time_trend_result, columns=time_trend_columns) if time_trend_result else pd.DataFrame()

if not time_trend_df.empty:
    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(
        x=time_trend_df['process_date'],
        y=time_trend_df['extractions'],
        mode='lines+markers',
        name='Extractions',
        line=dict(color='blue')
    ))
    fig2.add_trace(go.Scatter(
        x=time_trend_df['process_date'],
        y=time_trend_df['transformations'],
        mode='lines+markers',
        name='Transformations',
        line=dict(color='orange')
    ))
    fig2.add_trace(go.Scatter(
        x=time_trend_df['process_date'],
        y=time_trend_df['loads'],
        mode='lines+markers',
        name='Loads',
        line=dict(color='green')
    ))
    fig2.update_layout(
        title='ETL Processing Trend',
        xaxis_title='Date',
        yaxis_title='Count',
        height=400
    )
    chart_col2.plotly_chart(fig2, use_container_width=True)
else:
    chart_col2.info("No time trend data available for the selected filters")

st.subheader("API Performance Metrics")
api_col1, api_col2 = st.columns(2)

api_response_query = f"""
SELECT 
    a.type as api_type,
    AVG(EXTRACT(EPOCH FROM (ail.end_time - ail.start_time))) as avg_response_time,
    COUNT(*) as call_count
FROM extract.api_import_logs ail
JOIN extract.api a ON ail.api_id = a.id
JOIN extract.import_logs il ON ail.import_logs_id = il.id
WHERE il.backfill_date BETWEEN '{start_date}' AND '{end_date}'
{country_filter}
{api_filter}
GROUP BY a.type
ORDER BY avg_response_time DESC
"""
api_response_result = execute_query(api_response_query, fetch=True)
api_response_columns = ['api_type', 'avg_response_time', 'call_count']
api_response_df = pd.DataFrame(api_response_result, columns=api_response_columns) if api_response_result else pd.DataFrame()

if not api_response_df.empty:
    fig3 = px.bar(
        api_response_df,
        x='api_type',
        y='avg_response_time',
        color='call_count',
        labels={'api_type': 'API Type', 'avg_response_time': 'Avg Response Time (s)', 'call_count': 'Call Count'},
        title='API Average Response Time',
        height=400,
        color_continuous_scale=px.colors.sequential.Viridis
    )
    api_col1.plotly_chart(fig3, use_container_width=True)
else:
    api_col1.info("No API response time data available for the selected filters")

api_error_query = f"""
SELECT 
    a.type as api_type,
    COUNT(*) as total_calls,
    SUM(CASE WHEN ail.code_response >= 400 THEN 1 ELSE 0 END) as error_calls,
    ROUND(SUM(CASE WHEN ail.code_response >= 400 THEN 1 ELSE 0 END)::numeric / COUNT(*)::numeric * 100, 2) as error_rate
FROM extract.api_import_logs ail
JOIN extract.api a ON ail.api_id = a.id
JOIN extract.import_logs il ON ail.import_logs_id = il.id
WHERE il.backfill_date BETWEEN '{start_date}' AND '{end_date}'
{country_filter}
{api_filter}
GROUP BY a.type
ORDER BY error_rate DESC
"""
api_error_result = execute_query(api_error_query, fetch=True)
api_error_columns = ['api_type', 'total_calls', 'error_calls', 'error_rate']
api_error_df = pd.DataFrame(api_error_result, columns=api_error_columns) if api_error_result else pd.DataFrame()

if not api_error_df.empty:
    fig4 = px.bar(
        api_error_df,
        x='api_type',
        y='error_rate',
        color='total_calls',
        labels={'api_type': 'API Type', 'error_rate': 'Error Rate (%)', 'total_calls': 'Total Calls'},
        title='API Error Rate',
        height=400,
        color_continuous_scale=px.colors.sequential.Viridis
    )
    api_col2.plotly_chart(fig4, use_container_width=True)
else:
    api_col2.info("No API error rate data available for the selected filters")

st.subheader("ETL Process Logs")
tab1, tab2, tab3 = st.tabs(["Extract Logs", "Transform Logs", "Load Logs"])

with tab1:
    extract_logs_query = f"""
    SELECT 
        il.id,
        c.name as country,
        il.import_directory_name,
        il.import_file_name,
        to_timestamp(il.batch_timestamp) as batch_time,
        il.file_created_date,
        il.file_last_modified_date,
        il.backfill_date,
        CASE 
            WHEN tl.id IS NOT NULL THEN 'Transformed'
            ELSE 'Extracted Only'
        END as status
    FROM extract.import_logs il
    JOIN extract.country c ON il.country_id = c.id
    LEFT JOIN transform.logs tl ON il.id = tl.import_logs_id
    WHERE il.backfill_date BETWEEN '{start_date}' AND '{end_date}'
    {country_filter}
    ORDER BY batch_time DESC
    LIMIT 1000
    """
    extract_logs_result = execute_query(extract_logs_query, fetch=True)
    extract_logs_columns = ['id', 'country', 'import_directory_name', 'import_file_name', 'batch_time', 
                         'file_created_date', 'file_last_modified_date', 'backfill_date', 'status']
    extract_logs_df = pd.DataFrame(extract_logs_result, columns=extract_logs_columns) if extract_logs_result else pd.DataFrame()
    
    if not extract_logs_df.empty:
        st.dataframe(extract_logs_df, use_container_width=True)
    else:
        st.info("No extraction logs found for the selected filters")

with tab2:
    transform_logs_query = f"""
    SELECT 
        tl.id,
        c.name as country,
        il.import_file_name,
        tl.processed_directory_name,
        tl.processed_file_name,
        to_timestamp(il.batch_timestamp) as batch_time,
        tl.status,
        CASE 
            WHEN ll.id IS NOT NULL THEN 'Loaded'
            ELSE 'Not Loaded'
        END as load_status
    FROM transform.logs tl
    JOIN extract.import_logs il ON tl.import_logs_id = il.id
    JOIN extract.country c ON il.country_id = c.id
    LEFT JOIN load.logs ll ON tl.id = ll.transform_logs_id
    WHERE il.backfill_date BETWEEN '{start_date}' AND '{end_date}'
    {country_filter}
    {status_filter}
    ORDER BY batch_time DESC
    LIMIT 1000
    """
    transform_logs_result = execute_query(transform_logs_query, fetch=True)
    transform_logs_columns = ['id', 'country', 'import_file_name', 'processed_directory_name', 
                           'processed_file_name', 'batch_time', 'status', 'load_status']
    transform_logs_df = pd.DataFrame(transform_logs_result, columns=transform_logs_columns) if transform_logs_result else pd.DataFrame()
    
    if not transform_logs_df.empty:
        st.dataframe(transform_logs_df, use_container_width=True)
    else:
        st.info("No transformation logs found for the selected filters")

with tab3:
    load_logs_query = f"""
    SELECT 
        ll.id,
        c.name as country,
        il.import_file_name,
        tl.processed_file_name,
        to_timestamp(il.batch_timestamp) as batch_time,
        ll.status
    FROM load.logs ll
    JOIN transform.logs tl ON ll.transform_logs_id = tl.id
    JOIN extract.import_logs il ON tl.import_logs_id = il.id
    JOIN extract.country c ON il.country_id = c.id
    WHERE il.backfill_date BETWEEN '{start_date}' AND '{end_date}'
    {country_filter}
    {status_filter}
    ORDER BY batch_time DESC
    LIMIT 1000
    """
    load_logs_result = execute_query(load_logs_query, fetch=True)
    load_logs_columns = ['id', 'country', 'import_file_name', 'processed_file_name', 'batch_time', 'status']
    load_logs_df = pd.DataFrame(load_logs_result, columns=load_logs_columns) if load_logs_result else pd.DataFrame()
    
    if not load_logs_df.empty:
        st.dataframe(load_logs_df, use_container_width=True)
    else:
        st.info("No load logs found for the selected filters")

st.subheader("API Error Analysis")
api_errors_query = f"""
SELECT 
    a.type as api_type,
    c.name as country,
    ail.code_response,
    ail.error_message,
    ail.start_time,
    ail.end_time,
    EXTRACT(EPOCH FROM (ail.end_time - ail.start_time)) as duration_seconds
FROM extract.api_import_logs ail
JOIN extract.api a ON ail.api_id = a.id
JOIN extract.import_logs il ON ail.import_logs_id = il.id
JOIN extract.country c ON il.country_id = c.id
WHERE il.backfill_date BETWEEN '{start_date}' AND '{end_date}'
AND ail.code_response >= 400
{country_filter}
{api_filter}
ORDER BY ail.start_time DESC
LIMIT 1000
"""
api_errors_result = execute_query(api_errors_query, fetch=True)
api_errors_columns = ['api_type', 'country', 'code_response', 'error_message', 
                    'start_time', 'end_time', 'duration_seconds']
api_errors_df = pd.DataFrame(api_errors_result, columns=api_errors_columns) if api_errors_result else pd.DataFrame()

if not api_errors_df.empty:
    st.dataframe(api_errors_df, use_container_width=True)
    
    error_counts = api_errors_df.groupby(['api_type', 'code_response']).size().reset_index(name='count')
    
    fig5 = px.bar(
        error_counts,
        x='api_type',
        y='count',
        color='code_response',
        labels={'api_type': 'API Type', 'count': 'Error Count', 'code_response': 'HTTP Code'},
        title='API Errors by Type and Response Code',
        height=400
    )
    st.plotly_chart(fig5, use_container_width=True)
else:
    st.info("No API errors found for the selected filters")

st.subheader("Backfill Analysis")
backfill_query = f"""
SELECT 
    c.name as country,
    DATE(il.backfill_date) as backfill_date,
    COUNT(*) as file_count
FROM extract.import_logs il
JOIN extract.country c ON il.country_id = c.id
WHERE il.backfill_date IS NOT NULL
AND il.backfill_date BETWEEN '{start_date}' AND '{end_date}'
{country_filter}
GROUP BY c.name, DATE(il.backfill_date)
ORDER BY backfill_date DESC
"""
backfill_result = execute_query(backfill_query, fetch=True)
backfill_columns = ['country', 'backfill_date', 'file_count']
backfill_df = pd.DataFrame(backfill_result, columns=backfill_columns) if backfill_result else pd.DataFrame()

if not backfill_df.empty:
    fig6 = px.bar(
        backfill_df,
        x='backfill_date',
        y='file_count',
        color='country',
        labels={'backfill_date': 'Backfill Date', 'file_count': 'File Count', 'country': 'Country'},
        title='Backfill Operations',
        height=400
    )
    st.plotly_chart(fig6, use_container_width=True)
else:
    st.info("No backfill operations found for the selected filters")