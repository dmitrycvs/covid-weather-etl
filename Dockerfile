FROM apache/airflow:2.10.5

WORKDIR /opt/airflow

RUN pip install --no-cache-dir \
    sqlglot \
    psycopg2-binary \
    scikit-learn \
    xgboost \
    joblib \
    pmdarima \
    statsmodels
