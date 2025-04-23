from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import pandas as pd
import os
import sys
from sqlalchemy import create_engine
from dotenv import load_dotenv
import joblib
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import classification_report
from xgboost import XGBClassifier
import base64
import io

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils import setup_logging

logger = setup_logging("ml_models", "classification_process")

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "weather_prediction_by_country",
    default_args=default_args,
    schedule_interval=timedelta(days=4),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["weather", "machine_learning", "classification"],
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
def preprocess_data(df_json, country_name):
    df = pd.read_json(df_json)
    df.drop(columns=["id", "tsun", "country"], inplace=True)
    df["date"] = pd.to_datetime(df["date"])
    df["rain"] = (df["prcp"] > 0).astype(int)
    logger.info(
        f"Preprocessed data for {country_name}: {df.shape[0]} rows, {df.shape[1]} columns"
    )
    return df.to_json()


@task
def prepare_train_test_data(df_json, country_name):
    df = pd.read_json(df_json)
    features = ["tmin", "wspd", "tavg", "wpgt", "snow", "tmax", "wdir", "pres"]
    X = df[features]
    y = df["rain"]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.3, random_state=42, stratify=y
    )

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    logger.info(
        f"Split and scaled data for {country_name}: {X_train.shape[0]} train, {X_test.shape[0]} test"
    )
    return {
        "X_train": X_train_scaled.tolist(),
        "X_test": X_test_scaled.tolist(),
        "y_train": y_train.tolist(),
        "y_test": y_test.tolist(),
    }


@task
def train_model(data, country_name):
    model = XGBClassifier(
        learning_rate=0.1, max_depth=7, n_estimators=100, subsample=0.8, random_state=42
    )
    model.fit(data["X_train"], data["y_train"])
    logger.info(f"Trained XGBoost model for {country_name}")

    model_bytes = io.BytesIO()
    joblib.dump(model, model_bytes)
    model_bytes.seek(0)

    return base64.b64encode(model_bytes.getvalue()).decode("utf-8")


@task
def evaluate_model(model_base64, data, country_name):
    model_bytes = base64.b64decode(model_base64)

    model_buffer = io.BytesIO(model_bytes)
    model = joblib.load(model_buffer)

    y_pred = model.predict(data["X_test"])
    report = classification_report(data["y_test"], y_pred, output_dict=True)
    return {"report": report, "predictions": y_pred.tolist(), "actual": data["y_test"]}


@task
def export_model(model_base64, country_name, evaluation):
    os.makedirs(f"models/classification/{country_name}", exist_ok=True)
    model_path = f"models/classification/{country_name}/rain_prediction_model.joblib"

    model_bytes = base64.b64decode(model_base64)

    with open(model_path, "wb") as f:
        f.write(model_bytes)

    logger.info(f"Results: {evaluation['report']}")

    return model_path


@task
def export_predictions(country_name, evaluation):
    os.makedirs(f"models/classification/{country_name}", exist_ok=True)
    predictions_df = pd.DataFrame(
        {"actual": evaluation["actual"], "predicted": evaluation["predictions"]}
    )
    predictions_path = f"models/classification/{country_name}/predictions.csv"
    predictions_df.to_csv(predictions_path, index=False)
    logger.info(f"Exported predictions for {country_name} to {predictions_path}")
    return predictions_path


countries = ["Moldova", "Germany", "Italy"]

with dag:
    raw_data = fetch_weather_data()

    for country in countries:
        country_df = extract_country_data(raw_data, country)
        preprocessed = preprocess_data(country_df, country)
        split_data = prepare_train_test_data(preprocessed, country)
        model = train_model(split_data, country)
        evaluation = evaluate_model(model, split_data, country)
        export_model(model, country, evaluation)
        export_predictions(country, evaluation)
