from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os
import sys
from sqlalchemy import create_engine
from dotenv import load_dotenv
import joblib
from sklearn.metrics import mean_absolute_error, root_mean_squared_error
from pmdarima import auto_arima
from statsmodels.tsa.statespace.sarimax import SARIMAX
import base64
import io

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils import setup_logging

logger = setup_logging("ml_models", "forecasting_process")

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "weather_forecasting_by_country",
    default_args=default_args,
    schedule_interval=timedelta(days=4),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["weather", "forecasting", "time_series", "machine_learning"],
)


def get_engine():
    load_dotenv()
    db_url = (
        f"postgresql+psycopg2://{os.environ.get('DB_USERNAME')}:{os.environ.get('DB_PASSWORD')}"
        f"@{os.environ.get('DB_HOST')}:{os.environ.get('DB_PORT')}/{os.environ.get('DB_NAME')}"
    )
    return create_engine(db_url)


@task
def fetch_weather_data():
    engine = get_engine()
    query = "SELECT * FROM load.weather"
    df = pd.read_sql_query(query, engine)
    engine.dispose()
    logger.info(f"Fetched weather data with {df.shape[0]} rows and {df.shape[1]} columns")
    return df.to_json()


@task
def extract_country_data(df_json, country_name):
    df = pd.read_json(df_json)
    country_df = df[df["country"] == country_name].copy()
    logger.info(f"Extracted {country_df.shape[0]} rows for {country_name}")
    return country_df.to_json()


@task
def preprocess_time_series_data(df_json, country_name):
    df = pd.read_json(df_json)

    df = df.sort_values("date")
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)

    cols_to_drop = ["id", "country", "tsun"]
    df.drop(columns=cols_to_drop, inplace=True)

    logger.info(
        f"Preprocessed time series data for {country_name}: {df.shape[0]} rows, {df.shape[1]} columns"
    )
    logger.info(f"Available columns: {df.columns.tolist()}")

    return df.to_json(date_format="iso")


@task
def prepare_train_test_data(df_json, country_name, target="tavg", exog_vars=None):
    if exog_vars is None:
        exog_vars = ["prcp", "wspd", "snow", "pres"]

    try:
        df = pd.read_json(df_json)

        if "date" not in df.columns and not isinstance(df.index, pd.DatetimeIndex):
            df = df.reset_index()
            if "index" in df.columns:
                df.rename(columns={"index": "date"}, inplace=True)

        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
            df.set_index("date", inplace=True)

        logger.info(f"DataFrame shape for {country_name} before split: {df.shape}")
        logger.info(f"DataFrame columns: {df.columns.tolist()}")

        train_size = int(len(df) * 0.8)
        train = df.iloc[:train_size]
        test = df.iloc[train_size:]

        logger.info(
            f"Split data for {country_name}: {train.shape[0]} train, {test.shape[0]} test samples"
        )

        return {
            "train": train.to_json(),
            "test": test.to_json(),
            "target": target,
            "exog_vars": exog_vars,
        }
    except Exception as e:
        logger.error(f"Error in prepare_train_test_data for {country_name}: {str(e)}")
        logger.error(
            f"DataFrame JSON sample: {df_json[:200]}..."
            if isinstance(df_json, str)
            else f"DataFrame JSON type: {type(df_json)}"
        )
        raise


@task
def find_optimal_parameters(data, country_name):
    train_df = pd.read_json(data["train"])
    target = data["target"]
    exog_vars = data["exog_vars"]

    available_exog = [col for col in exog_vars if col in train_df.columns]
    if len(available_exog) < len(exog_vars):
        missing = set(exog_vars) - set(available_exog)
        logger.warning(f"Missing exogenous variables for {country_name}: {missing}")

    train_exog = train_df[available_exog] if available_exog else None

    logger.info(f"Finding optimal ARIMA parameters for {country_name}")

    auto_model = auto_arima(
        train_df[target],
        exogenous=train_exog,
        seasonal=True,
        m=30,
        d=None,
        D=None,
        trace=True,
        error_action="ignore",
        suppress_warnings=True,
        stepwise=True,
    )

    p, d, q = auto_model.order
    P, D, Q, s = auto_model.seasonal_order

    logger.info(
        f"Optimal parameters for {country_name}: ARIMA({p},{d},{q})({P},{D},{Q}){s}"
    )

    return {"order": (p, d, q), "seasonal_order": (P, D, Q, s)}


@task
def train_forecasting_model(data, country_name, model_params=None):
    train_df = pd.read_json(data["train"])
    target = data["target"]
    exog_vars = data["exog_vars"]

    available_exog = [col for col in exog_vars if col in train_df.columns]
    train_exog = train_df[available_exog] if available_exog else None

    model = SARIMAX(
        train_df[target],
        exog=train_exog,
        order=model_params["order"],
        seasonal_order=model_params["seasonal_order"],
        enforce_stationarity=False,
        enforce_invertibility=False,
    )

    logger.info(f"Training SARIMAX model for {country_name}")
    result = model.fit()

    model_bytes = io.BytesIO()
    joblib.dump(result, model_bytes)
    model_bytes.seek(0)

    return base64.b64encode(model_bytes.getvalue()).decode("utf-8")


@task
def forecast_and_evaluate(model_base64, data, country_name):
    test_df = pd.read_json(data["test"])
    target = data["target"]
    exog_vars = data["exog_vars"]

    available_exog = [col for col in exog_vars if col in test_df.columns]
    test_exog = test_df[available_exog] if available_exog else None

    model_bytes = base64.b64decode(model_base64)
    model_buffer = io.BytesIO(model_bytes)
    model_result = joblib.load(model_buffer)

    logger.info(f"Generating forecast for {country_name}")
    forecast = model_result.get_forecast(steps=len(test_df), exog=test_exog)
    forecast_mean = forecast.predicted_mean
    forecast_ci = forecast.conf_int()

    mae = mean_absolute_error(test_df[target], forecast_mean)
    rmse = root_mean_squared_error(test_df[target], forecast_mean)

    logger.info(f"Forecast evaluation for {country_name}: MAE={mae:.4f}, RMSE={rmse:.4f}")

    date_strings = (
        test_df.index.strftime("%Y-%m-%d %H:%M:%S").tolist()
        if hasattr(test_df.index, "strftime")
        else [str(d) for d in test_df.index]
    )

    forecast_results = {
        "forecast_mean": forecast_mean.to_list(),
        "forecast_lower": forecast_ci.iloc[:, 0].to_list(),
        "forecast_upper": forecast_ci.iloc[:, 1].to_list(),
        "actual": test_df[target].to_list(),
        "dates": date_strings,
        "metrics": {"mae": mae, "rmse": rmse},
    }

    return forecast_results


@task
def export_forecast_results(model_base64, data, forecast_results, country_name):
    os.makedirs(f"models/forecasts/{country_name}", exist_ok=True)
    model_path = f"models/forecasts/{country_name}/forecast_model.joblib"
    model_bytes = base64.b64decode(model_base64)

    with open(model_path, "wb") as f:
        f.write(model_bytes)

    target = data["target"]

    forecast_df = pd.DataFrame(
        {
            "date_string": forecast_results["dates"],
            "actual": forecast_results["actual"],
            "forecast": forecast_results["forecast_mean"],
            "lower_ci": forecast_results["forecast_lower"],
            "upper_ci": forecast_results["forecast_upper"],
        }
    )

    try:
        forecast_df["date"] = pd.to_datetime(forecast_df["date_string"])
        forecast_df.set_index("date", inplace=True)
        forecast_df.drop(columns=["date_string"], inplace=True)
    except (ValueError, TypeError):
        forecast_df = forecast_df.set_index("date_string")
        logger.warning(
            f"Could not parse dates for {country_name}, using string index instead"
        )

    csv_path = f"models/forecasts/{country_name}/forecast_results.csv"
    forecast_df.to_csv(csv_path)

    metrics = {
        "country": country_name,
        "model": "SARIMAX",
        "target_variable": target,
        "mae": forecast_results["metrics"]["mae"],
        "rmse": forecast_results["metrics"]["rmse"],
        "confidence_interval_width": np.mean(
            np.array(forecast_results["forecast_upper"])
            - np.array(forecast_results["forecast_lower"])
        ),
    }
    logger.info(f"Forecast metrics are: {metrics}")
    logger.info(f"Exported forecast results for {country_name} to {csv_path}")

    return {"model_path": model_path, "csv_path": csv_path}


countries = ["Moldova", "Germany", "Italy"]
target_variable = "tavg"
exogenous_variables = ["prcp", "wspd", "snow", "pres"]

with dag:
    raw_data = fetch_weather_data()

    for country in countries:
        country_df = extract_country_data(raw_data, country)
        preprocessed = preprocess_time_series_data(country_df, country)
        train_test_data = prepare_train_test_data(
            preprocessed, country, target=target_variable, exog_vars=exogenous_variables
        )
        model_params = find_optimal_parameters(train_test_data, country)
        trained_model = train_forecasting_model(train_test_data, country, model_params)
        forecast_results = forecast_and_evaluate(trained_model, train_test_data, country)
        export_forecast_results(trained_model, train_test_data, forecast_results, country)
