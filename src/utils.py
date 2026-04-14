import logging
import os
import datetime
import pandas as pd

SCHEMA = ['date', 'product_identifier', 'total_units', 'total_revenue', 'data_source']


def setup_logger():
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    log_filename = os.path.join(
        log_dir,
        f"pipeline_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()
        ]
    )

    return logging.getLogger(__name__)


def _clean_numeric(series):
    return pd.to_numeric(series, errors='coerce').fillna(0)


def _aggregate(df, logger):
    logger.info("Aggregating data")

    result = (
        df.groupby(['date', 'product_identifier', 'data_source'], as_index=False)
          .agg(total_units=('total_units', 'sum'),
               total_revenue=('total_revenue', 'sum'))
    )[SCHEMA]

    logger.info(f"Aggregation complete | rows={len(result)}")
    return result