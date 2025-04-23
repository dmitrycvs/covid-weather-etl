import streamlit as st
import pandas as pd
import numpy as np
import joblib
import os
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns

st.set_page_config(
    page_title="Weather Prediction App",
    layout="wide"
)

st.title("🌦️ Weather Prediction Application")
st.write("Select a country and input weather parameters to get forecasts and rain predictions.")

DEFAULT_BASE_DIR = os.path.abspath(os.path.join(os.getcwd(), os.pardir))

with st.sidebar:
    st.header("Model Path Configuration")
    use_custom_path = st.checkbox("Use custom model path", True)
    
    if use_custom_path:
        base_dir = st.text_input("Base directory for models:", DEFAULT_BASE_DIR)
    else:
        base_dir = DEFAULT_BASE_DIR
    
country = st.selectbox(
    "Select a country:",
    ["Moldova", "Germany", "Italy"]
)

def load_model(model_type, country_name):
    if model_type == "forecast":
        path = os.path.join(base_dir, "models", "forecasts", country_name, "forecast_model.joblib")
    else:
        path = os.path.join(base_dir, "models", "classification", country_name, "rain_prediction_model.joblib")
    
    try:
        model = joblib.load(path)
        st.success(f"Successfully loaded model from {path}")
        return model
    except Exception as e:
        st.error(f"Error loading model: {str(e)}")
        return None

tab1, tab2 = st.tabs(["Weather Forecast", "Rain Prediction"])

with tab1:
    st.header("Weather Forecast Model")
    st.write("This model predicts weather conditions based on the provided parameters.")
    
    forecast_date = st.date_input("Select date for forecast:", datetime.now())
    
    col1, col2 = st.columns(2)
    with col1:
        prcp = st.number_input("Precipitation (mm):", min_value=0.0, step=0.1)
        wspd = st.number_input("Wind Speed (km/h):", min_value=0.0, step=0.1)
    with col2:
        snow = st.number_input("Snow (cm):", min_value=0.0, step=0.1)
        pres = st.number_input("Pressure (hPa):", min_value=900.0, max_value=1100.0, value=1013.0, step=0.1)
    
    if st.button("Run Forecast Prediction"):
        forecast_model = load_model("forecast", country)
        
        if forecast_model:
            try:
                date_timestamp = pd.Timestamp(forecast_date)
                
                input_data = pd.DataFrame({
                    'prcp': [prcp],
                    'wspd': [wspd],
                    'snow': [snow],
                    'pres': [pres]
                }, index=[date_timestamp])

                input_data.index = pd.to_datetime(input_data.index)

                st.write("Input data for model:")
                st.write(input_data)
                
                forecast_result = forecast_model.get_forecast(steps=1, exog=input_data)
                forecast_value = forecast_result.predicted_mean.iloc[0]
                
                st.success(f"Forecast prediction for {country} on {forecast_date}:")
                
                st.write("Prediction details:", forecast_value)
                
            except Exception as e:
                st.error(f"An error occurred during prediction: {e}")
            

with tab2:
    st.header("Rain Prediction Model")
    st.write("This classification model predicts rain likelihood based on various weather parameters.")
    
    col1, col2 = st.columns(2)
    with col1:
        tmin = st.number_input("Minimum Temperature (°C):", min_value=-50.0, max_value=50.0, value=10.0, step=0.1)
        wspd_class = st.number_input("Wind Speed for Classification (km/h):", min_value=0.0, step=0.1)
        tavg = st.number_input("Average Temperature (°C):", min_value=-50.0, max_value=50.0, value=15.0, step=0.1)
        wpgt = st.number_input("Wind Peak Gust (km/h):", min_value=0.0, step=0.1)
    
    with col2:
        snow_class = st.number_input("Snow for Classification (cm):", min_value=0.0, step=0.1)
        tmax = st.number_input("Maximum Temperature (°C):", min_value=-50.0, max_value=50.0, value=20.0, step=0.1)
        wdir = st.number_input("Wind Direction (degrees):", min_value=0.0, max_value=360.0, value=180.0, step=1.0)
        pres_class = st.number_input("Pressure for Classification (hPa):", min_value=900.0, max_value=1100.0, value=1013.0, step=0.1)
    
    if st.button("Run Rain Prediction"):
        classification_model = load_model("classification", country)
        
        if classification_model:
            try:
                input_data = pd.DataFrame({
                    'tmin': [tmin],
                    'wspd': [wspd_class],
                    'tavg': [tavg],
                    'wpgt': [wpgt],
                    'snow': [snow_class],
                    'tmax': [tmax],
                    'wdir': [wdir],
                    'pres': [pres_class]
                })
                
                st.write("Input data for model:")
                st.write(input_data)
                
                prediction = classification_model.predict(input_data)
                
                try:
                    prediction_proba = classification_model.predict_proba(input_data)
                    has_proba = True
                except:
                    has_proba = False
                
                st.success(f"Rain prediction for {country}:")
                
                if prediction[0] == 1:
                    rain_result = "Rain Predicted"
                    color = "blue"
                else:
                    rain_result = "No Rain Predicted"
                    color = "orange"
                
                st.markdown(f"### Prediction: **{rain_result}**")
                
                if has_proba:
                    fig, ax = plt.subplots(figsize=(10, 6))
                    probabilities = prediction_proba[0]
                    labels = ["No Rain", "Rain"]
                    sns.barplot(x=labels, y=probabilities, palette=["orange", "blue"], ax=ax)
                    ax.set_ylabel("Probability")
                    ax.set_title(f"Rain Prediction Probabilities for {country}")
                    
                    for i, p in enumerate(ax.patches):
                        height = p.get_height()
                        ax.text(p.get_x() + p.get_width()/2., height + 0.01,
                                f'{height:.2f}', ha="center")
                    
                    st.pyplot(fig)
                
            except Exception as e:
                st.error(f"An error occurred during prediction: {e}")