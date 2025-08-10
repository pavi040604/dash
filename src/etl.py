import pandas as pd
from sqlalchemy import create_engine, inspect
from loguru import logger
import os
import config

logger.add("logs/etl.log", rotation="1 MB")

def extract(path):
    try:
        df = pd.read_csv(path)
        logger.info(f"Extracted {len(df)} rows from {path}")
        return df
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        raise

def transform(df):
    try:
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        df.dropna(inplace=True)
        logger.info(f"Transformed dataset, now {len(df)} rows")
        return df
    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        raise

def load(df, db_url):
    try:
        engine = create_engine(db_url)
        df.to_sql("sales", engine, if_exists="replace", index=False)
        logger.info("Data loaded into DB successfully")
    except Exception as e:
        logger.error(f"Load failed: {e}")
        raise

def init_db():
    """Create and load sales table if not exists."""
    engine = create_engine(config.DB_URL)
    inspector = inspect(engine)
    if "sales" not in inspector.get_table_names():
        logger.info("Sales table not found. Creating and loading data...")
        df = extract(config.RAW_DATA_PATH)
        df = transform(df)
        load(df, config.DB_URL)
    else:
        logger.info("Sales table already exists. Skipping ETL.")

if __name__ == "__main__":
    init_db()
