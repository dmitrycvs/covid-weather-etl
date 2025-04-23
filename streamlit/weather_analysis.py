import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from statsmodels.tsa.seasonal import seasonal_decompose
import os
from sqlalchemy import create_engine
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

st.set_page_config(layout="wide", page_title="Weather Data Analysis")

st.title("Weather Data Analysis Dashboard")
st.write("Analyze weather patterns across different countries")

@st.cache_resource
def connect_db():
    db_url = f"postgresql+psycopg2://{os.environ.get('DB_USERNAME')}:{os.environ.get('DB_PASSWORD')}@localhost:5433/{os.environ.get('DB_NAME')}"
    engine = create_engine(db_url)
    return engine

@st.cache_data
def load_data():
    connection = connect_db()
    query = "SELECT * FROM load.weather"
    df = pd.read_sql_query(query, connection)
    connection.dispose()
    
    if 'date' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['date']):
        df['date'] = pd.to_datetime(df['date'])
        
    for col in df.select_dtypes(include=['object']).columns:
        try:
            sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
            if sample and hasattr(sample, 'timestamp'):
                df[col] = pd.to_datetime(df[col])
        except (AttributeError, TypeError):
            pass
    
    return df

try:
    df = load_data()
    
    countries = df['country'].unique()
    
    country_selection = st.sidebar.selectbox("Select a country:", countries)
    
    filtered_df = df[df['country'] == country_selection].copy()
    
    if filtered_df.empty:
        st.error(f"No data available for {country_selection}")
    else:
        tab1, tab2, tab3, tab4, tab5 = st.tabs(["Temperature Analysis", "Seasonal Decomposition", 
                                              "Correlation Analysis", "Statistical Summary", 
                                              "Additional EDA"])
        
        with tab1:
            st.header(f"Temperature Trends for {country_selection}")
            
            min_date = filtered_df['date'].min().date()
            max_date = filtered_df['date'].max().date()
            date_range = st.date_input("Select date range:", 
                                     [min_date, max_date],
                                     min_value=min_date,
                                     max_value=max_date)
            
            if len(date_range) == 2:
                start_date, end_date = date_range
                date_filtered_df = filtered_df[(filtered_df['date'].dt.date >= start_date) & 
                                              (filtered_df['date'].dt.date <= end_date)].copy()
                
                fig = px.line(date_filtered_df, x='date', y=['tmin', 'tavg', 'tmax'], 
                             labels={'value': 'Temperature (°C)', 'date': 'Date'},
                             title=f"Temperature Range ({start_date} to {end_date})")
                
                fig.update_layout(legend_title_text='Temperature Type',
                                 xaxis_title="Date",
                                 yaxis_title="Temperature (°C)")
                
                st.plotly_chart(fig, use_container_width=True)
                
                if 'prcp' in date_filtered_df.columns:
                    st.subheader("Precipitation Analysis")
                    fig_prcp = px.bar(date_filtered_df, x='date', y='prcp',
                                    labels={'prcp': 'Precipitation (mm)', 'date': 'Date'},
                                    title='Precipitation Over Time')
                    st.plotly_chart(fig_prcp, use_container_width=True)
        
        with tab2:
            st.header(f"Seasonal Decomposition for {country_selection}")
            
            period = st.slider("Select period for decomposition (days):", 7, 365, 30)
            
            temp_type = st.selectbox("Select temperature type for decomposition:", ['tavg', 'tmin', 'tmax'])
            
            if len(filtered_df) > period * 2:
                try:
                    temp_series = filtered_df[temp_type].interpolate()
                    
                    decomposition = seasonal_decompose(temp_series, model='additive', period=period)
                    
                    fig = make_subplots(rows=4, cols=1, subplot_titles=("Observed", "Trend", "Seasonal", "Residual"))
                    
                    fig.add_trace(go.Scatter(x=filtered_df['date'], y=decomposition.observed.astype(float), name="Observed"), row=1, col=1)
                    fig.add_trace(go.Scatter(x=filtered_df['date'], y=decomposition.trend.astype(float), name="Trend"), row=2, col=1)
                    fig.add_trace(go.Scatter(x=filtered_df['date'], y=decomposition.seasonal.astype(float), name="Seasonal"), row=3, col=1)
                    fig.add_trace(go.Scatter(x=filtered_df['date'], y=decomposition.resid.astype(float), name="Residual"), row=4, col=1)
                    
                    fig.update_layout(height=800, title_text=f"Seasonal Decomposition of {temp_type} (Period: {period} days)")
                    st.plotly_chart(fig, use_container_width=True)
                    
                except Exception as e:
                    st.error(f"Error in seasonal decomposition: {str(e)}")
                    st.info("Try a different period or ensure you have sufficient data.")
            else:
                st.warning(f"Not enough data for seasonal decomposition with period={period}. Try a smaller period.")
        
        with tab3:
            st.header(f"Correlation Analysis for {country_selection}")
            
            numeric_cols = filtered_df.select_dtypes(include=['number']).columns.tolist()
            
            if len(numeric_cols) > 1:
                corr_matrix = filtered_df[numeric_cols].corr()
                
                fig = px.imshow(corr_matrix, 
                               text_auto=True, 
                               color_continuous_scale='RdBu_r',
                               title="Correlation Matrix")
                st.plotly_chart(fig, use_container_width=True)
                
                col1, col2 = st.columns(2)
                with col1:
                    x_var = st.selectbox("Select X variable:", numeric_cols)
                with col2:
                    y_var = st.selectbox("Select Y variable:", [col for col in numeric_cols if col != x_var])
                
                scatter_fig = px.scatter(filtered_df, x=x_var, y=y_var, trendline="ols",
                                       title=f"Scatter plot: {x_var} vs {y_var}")
                st.plotly_chart(scatter_fig, use_container_width=True)
            else:
                st.warning("Not enough numeric columns for correlation analysis.")
        
        with tab4:
            st.header(f"Statistical Summary for {country_selection}")
            
            st.subheader("Descriptive Statistics")
            stats_df = filtered_df.describe().transpose().reset_index()
            stats_df.columns = stats_df.columns.astype(str)
            
            for col in stats_df.select_dtypes(include=['float']).columns:
                stats_df[col] = stats_df[col].astype(float)
                
            st.dataframe(stats_df)
            
            st.subheader("Data Distribution")
            
            col1, col2 = st.columns(2)
            
            with col1:
                num_var = st.selectbox("Select variable for histogram:", numeric_cols)
                hist_fig = px.histogram(filtered_df, x=num_var, 
                                     title=f"Distribution of {num_var}",
                                     marginal="box")
                st.plotly_chart(hist_fig, use_container_width=True)
                
            with col2:
                if 'date' in filtered_df.columns:
                    filtered_df.loc[:, 'month'] = filtered_df['date'].dt.month
                    monthly_avg = filtered_df.groupby('month')['tavg'].mean().reset_index()
                    
                    month_names = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
                                 7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}
                    monthly_avg['month_name'] = monthly_avg['month'].map(month_names)
                    
                    bar_fig = px.bar(monthly_avg, x='month_name', y='tavg',
                                   title=f"Average Monthly Temperature for {country_selection}",
                                   labels={'tavg': 'Avg Temperature (°C)', 'month_name': 'Month'})
                    st.plotly_chart(bar_fig, use_container_width=True)
        
        with tab5:
            st.header("Additional Exploratory Data Analysis")
            
            st.subheader("Monthly Temperature Variation")
            
            if 'date' in filtered_df.columns:
                filtered_df.loc[:, 'month'] = filtered_df['date'].dt.month
                filtered_df.loc[:, 'year'] = filtered_df['date'].dt.year
                
                box_fig = px.box(filtered_df, x='month', y='tavg',
                               title=f"Monthly Temperature Distribution for {country_selection}",
                               labels={'tavg': 'Avg Temperature (°C)', 'month': 'Month'})
                st.plotly_chart(box_fig, use_container_width=True)
                
                years = sorted(filtered_df['year'].unique())
                if len(years) > 1:
                    st.subheader("Year-over-Year Comparison")
                    
                    selected_years = st.multiselect("Select years to compare:", years, default=years[-2:])
                    
                    if selected_years:
                        yearly_data = filtered_df[filtered_df['year'].isin(selected_years)].copy()
                        
                        yearly_fig = px.line(yearly_data, x='month', y='tavg', color='year',
                                          title=f"Monthly Temperature Comparison by Year",
                                          labels={'tavg': 'Avg Temperature (°C)', 'month': 'Month', 'year': 'Year'})
                        st.plotly_chart(yearly_fig, use_container_width=True)
                
                st.subheader("Temperature Extremes Analysis")
                
                temp_col = st.radio("Select temperature metric:", ['tmax', 'tmin'])
                percentile = st.slider("Select percentile for extremes:", 1, 99, 95)
                
                if temp_col == 'tmax':
                    threshold = np.percentile(filtered_df[temp_col], percentile)
                    extreme_days = filtered_df[filtered_df[temp_col] >= threshold].copy()
                    title = f"Hottest Days ({percentile}th percentile, {threshold:.1f}°C or higher)"
                else:
                    threshold = np.percentile(filtered_df[temp_col], 100 - percentile)
                    extreme_days = filtered_df[filtered_df[temp_col] <= threshold].copy()
                    title = f"Coldest Days ({100-percentile}th percentile, {threshold:.1f}°C or lower)"
                
                if not extreme_days.empty:
                    extreme_fig = px.scatter(extreme_days, x='date', y=temp_col,
                                          hover_data=['date', 'tavg', 'tmin', 'tmax'],
                                          title=title)
                    st.plotly_chart(extreme_fig, use_container_width=True)
                    
                    st.write(f"Top 10 extreme days:")
                    if temp_col == 'tmax':
                        show_df = extreme_days.sort_values(by=temp_col, ascending=False).head(10)
                    else:
                        show_df = extreme_days.sort_values(by=temp_col, ascending=True).head(10)
                    
                    display_df = show_df[['date', 'tavg', 'tmin', 'tmax']].reset_index(drop=True)
                    display_df['date'] = display_df['date'].dt.strftime('%Y-%m-%d')
                    st.dataframe(display_df)
            
            st.subheader("Data Availability Analysis")
            
            missing_data = filtered_df.isnull().sum()
            if missing_data.sum() > 0:
                st.write("Missing values in the dataset:")
                missing_df = pd.DataFrame({
                    'Column': missing_data.index,
                    'Missing Values': missing_data.values,
                    'Percentage': (missing_data.values / len(filtered_df) * 100).round(2)
                })
                st.dataframe(missing_df[missing_df['Missing Values'] > 0])
            else:
                st.write("No missing values in the dataset.")
            
            st.write(f"Data ranges from {filtered_df['date'].min().date()} to {filtered_df['date'].max().date()}")
            st.write(f"Total of {len(filtered_df)} daily records available for {country_selection}")
            
except Exception as e:
    st.error(f"An error occurred: {str(e)}")
    st.info("Please check your database connection and ensure environment variables are properly set.")