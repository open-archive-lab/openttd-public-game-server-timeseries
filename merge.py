import logging
import pandas as pd

from config import settings
from database import engine

logger = logging.getLogger(__name__)


def merge():
    staging_db_path = settings.STAGING_DB
    final_db_path = settings.FINAL_DB

    if not staging_db_path.exists():
        logger.info(
            f"Staging database {staging_db_path} does not exist. Nothing to merge."
        )
        return

    try:
        # Read the staging database
        logger.info("Reading staging database.")
        df_staging = pd.read_sql_table("timepoints", con=engine)
    except ValueError:
        logger.info(
            "Table 'timepoints' not found in staging database. Nothing to merge."
        )
        return

    if df_staging.empty:
        logger.info("Staging database is empty. Nothing to merge.")
        return

    # Ensure timestamp is parsed as datetime
    df_staging["timestamp"] = pd.to_datetime(df_staging["timestamp"])

    # Read existing final database (CSV) if it exists
    if final_db_path.exists():
        logger.info("Reading existing final database.")
        df_final = pd.read_csv(final_db_path)
        df_final["timestamp"] = pd.to_datetime(df_final["timestamp"])

        # Combine both DataFrames
        # We append staging data to final data so that when we drop duplicates,
        # keeping the 'last' occurrence preserves the updated staging values.
        df_merged = pd.concat([df_final, df_staging], ignore_index=True)
        df_merged = df_merged.drop_duplicates(
            subset=["series_id", "timestamp"], keep="last"
        )
    else:
        logger.info("Final database does not exist yet. Creating a new one.")
        df_merged = df_staging

    # Sort data for consistent ordering
    df_merged = df_merged.sort_values(by=["timestamp", "series_id"])

    # Write back to the final database (CSV)
    logger.info(f"Writing {len(df_merged)} total records to {final_db_path}.")
    final_db_path.parent.mkdir(parents=True, exist_ok=True)
    df_merged.to_csv(final_db_path, index=False)

    logger.info("Merge complete.")


if __name__ == "__main__":
    merge()
