import logging
import os
from logging.handlers import RotatingFileHandler


def setup_logging(logger_name="covid_weather_etl", file_name="etl_process"):
    os.makedirs("logs", exist_ok=True)

    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)

    file_handler = RotatingFileHandler(
        f"logs/{file_name}.log", maxBytes=10 * 1024 * 1024, backupCount=5
    )

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)

    return logger
