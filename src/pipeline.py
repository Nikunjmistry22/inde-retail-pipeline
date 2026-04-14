import os
import pandas as pd

from utils import setup_logger, SCHEMA
from ingest import *
from validation import *

def main():
    logger = setup_logger()
    logger.info("Pipeline started")


    main_folder = "data"

    targets = ["blinkit", "nykaa", "zepto", "myntra"]
    matched_paths = {t: [] for t in targets}

    # file discovery
    for f in os.listdir(main_folder):
        for t in targets:
            if t in f.lower():
                matched_paths[t].append(os.path.join(main_folder, f))

    dataframes = {}

    # ingestion
    for brand, paths in matched_paths.items():
        dfs = []

        for p in paths:
            try:
                if brand == "blinkit":
                    df = ingest_blinkit(p, logger)
                elif brand == "zepto":
                    df = ingest_zepto(p, logger)
                elif brand == "nykaa":
                    df = ingest_nykaa(p, logger)
                elif brand == "myntra":
                    df = ingest_myntra(p, logger)
                else:
                    continue

                dfs.append(df)

            except Exception as e:
                logger.error(f"Error processing {p}: {e}")

        dataframes[brand] = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    # combine
    sales = pd.concat(dataframes.values(), ignore_index=True)

    # validation
    check_schema(sales, SCHEMA, logger)
    check_nulls(sales, logger)
    check_integrity(sales, logger)
    summarize(sales, logger)

    # output
    os.makedirs("output", exist_ok=True)
    sales.to_csv("output/final_sales.csv", index=False)

    logger.info("Pipeline completed successfully")


if __name__ == "__main__":
    main()