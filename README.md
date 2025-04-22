# COVID-Weather ETL

This project is an ETL (Extract, Transform, Load) pipeline designed to process and analyze data related to COVID-19 and weather patterns. The goal is to combine these datasets to uncover potential correlations and insights.

## Features

- **Data Extraction**: Fetch COVID-19 data from public APIs and weather data from meteorological sources.
- **Data Transformation**: Clean, normalize, and handle missing values.
- **Data Loading**: Store the processed data into a database or file system for further use.
- **Analysis**: Perform exploratory data analysis (EDA) to identify trends and correlations.
- **Machine Learning**: Train various models that can solve forecasting and classification problems.

## Installation

1. Clone the repository:
    ```bash
    git clone https://github.com/dmitrycvs/covid-weather-etl
    cd covid-weather-etl
    ```

2. Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

3. Set up API keys and environment variables

## Project Structure

```
covid-weather-etl/
├── S3/                 # Raw and processed data
├── dags/               # Airflow Dags stored here
├── tests/              # Unit tests
├── requirements.txt    # Python dependencies
├── README.md           # Project documentation
├── notebooks           # .ipynb notebooks, for the Data Science
├── streamlit           # Dashboards made using Streamlit
└── .env                # Environment variables
```

## Acknowledgments

- COVID-19 data provided by: covid-19-statistics.
- Weather data provided by: meteostats.
- Inspired by the need to analyze the impact of weather on pandemic trends.