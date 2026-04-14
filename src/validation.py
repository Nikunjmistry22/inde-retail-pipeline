def check_schema(df, schema, logger):
    if list(df.columns) != schema:
        logger.error("Schema mismatch")
        raise ValueError("Schema mismatch")
    logger.info("Schema validation passed")


def check_nulls(df, logger):
    nulls = df.isna().sum()
    if nulls.sum() > 0:
        logger.error(f"Nulls found:\n{nulls}")
        raise ValueError("Null values found")
    logger.info("No null values found")


def check_integrity(df, logger):
    neg_rev = df[df['total_revenue'] < 0]
    zero_qty = df[df['total_units'] == 0]

    logger.info(f"Negative revenue rows: {len(neg_rev)}")
    logger.info(f"Zero-unit rows: {len(zero_qty)}")


def summarize(df, logger):
    logger.info(f"Date range: {df['date'].min()} → {df['date'].max()}")
    logger.info(f"Unique SKUs: {df['product_identifier'].nunique()}")