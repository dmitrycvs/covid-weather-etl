from datetime import datetime
from etl.extractor import Extractor
from etl.transformer import Transformer
from etl.loader import Loader
import utils as u
import os
from time import time


def main():
    logger = u.setup_logging()
    start_date = datetime(2021, 4, 1)
    end_date = datetime(2021, 4, 3)

    try:
        # extractor = Extractor("COVID", os.environ.get("COVID_API_URL"), start_date, end_date, round(time()), logger)
        # extractor.run()

        transformer = Transformer(start_date, end_date, logger)
        transformer.run()

        # loader = Loader(start_date, end_date, logger)
        # loader.run()

        logger.info("ETL Pipeline finished successfully")
    except Exception as e:
        logger.error(f"ETL Pipeline failed: {e}")


if __name__ == "__main__":
    main()
