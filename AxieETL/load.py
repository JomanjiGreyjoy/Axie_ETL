import os
import logging
import pandas as pd
from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine, text

def load_data(csv_filenames: list):
    """
    Reads each CSV file from the 'axie-transformed-data' container, 
    truncates the permanent staging table, bulk-inserts data into staging,
    calls the MERGE stored procedure, and truncates again to clean up.

    :param csv_filenames: A list of CSV blob names, e.g. ["axies_20231021123456.csv", ...].
    """

    logging.info("Starting Load step.")

    # ---------------------------------------------------
    # 1) Connect to Blob Storage
    # ---------------------------------------------------
    blob_conn_str = os.getenv("BLOB_CONNECTION_STRING", "")
    container_name = "axie-transformed-data"
    blob_service_client = BlobServiceClient.from_connection_string(blob_conn_str)
    container_client = blob_service_client.get_container_client(container_name)

    # ---------------------------------------------------
    # 2) Connect to Azure SQL via SQLAlchemy
    # ---------------------------------------------------
    db_server = os.getenv("DB_SERVER", "")
    db_name = os.getenv("DB_NAME", "")
    db_user = os.getenv("DB_USER", "")
    db_pass = os.getenv("DB_PASS", "")
    db_driver = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")

    connection_string = (
        f"mssql+pyodbc://{db_user}:{db_pass}@{db_server}/{db_name}"
        f"?driver={db_driver.replace(' ', '+')}"
    )
    engine = create_engine(connection_string)

    # ---------------------------------------------------
    # 3) Map dim/fact to stored procedure
    # ---------------------------------------------------
    table_sp_map = {
        "axies": "usp_Merge_Axies",
        "eyes": "usp_Merge_Eyes",
        "mouth": "usp_Merge_Mouth",
        "ears": "usp_Merge_Ears",
        "horn": "usp_Merge_Horn",
        "back": "usp_Merge_Back",
        "tail": "usp_Merge_Tail",
        "mouth_abilities": "usp_Merge_MouthAbilities",
        "horn_abilities": "usp_Merge_HornAbilities",
        "back_abilities": "usp_Merge_BackAbilities",
        "tail_abilities": "usp_Merge_TailAbilities",
        "order_offers": "usp_Merge_OrderOffers",
        "transfers": "usp_Merge_Transfers"
    }

    staging_prefix = "staging_"

    # ---------------------------------------------------
    # 4) Loop over each CSV file
    # ---------------------------------------------------
    for csv_name in csv_filenames:
        logging.info(f"Loading CSV file: {csv_name}")

        # Example naming pattern: "axies_20231021123456.csv" -> "axies"
        table_part = csv_name.split("_")[0]  # e.g. "axies"

        staging_table_name = staging_prefix + table_part.lower()  # e.g. "staging_axies"
        sp_merge_name = table_sp_map.get(table_part.lower())      # e.g. "usp_Merge_Axies"

        if not sp_merge_name:
            logging.warning(f"No stored procedure mapping found for '{table_part}'. Skipping.")
            continue

        try:
            # ---------------------------------------------------
            # 4A) TRUNCATE staging table BEFORE loading
            # ---------------------------------------------------
            with engine.begin() as conn:
                truncate_stmt = text(f"TRUNCATE TABLE dbo.{staging_table_name};")
                conn.execute(truncate_stmt)
            logging.info(f"Truncated staging table: {staging_table_name} before load.")

            # ---------------------------------------------------
            # 4B) Download the CSV -> DataFrame -> to_sql
            # ---------------------------------------------------
            blob_client = container_client.get_blob_client(csv_name)
            csv_bytes = blob_client.download_blob().readall()
            csv_str = csv_bytes.decode("utf-8")
            df = pd.read_csv(pd.io.common.StringIO(csv_str))

            logging.info(f"DataFrame shape for {table_part} is {df.shape}")

            # Insert into staging table
            df.to_sql(
                name=staging_table_name,
                con=engine,
                schema="dbo",
                if_exists="append",
                index=False,
                method="multi",
                chunksize=500
            )
            logging.info(f"Inserted data into staging table: {staging_table_name}")

            # ---------------------------------------------------
            # 4C) Call MERGE stored procedure
            # ---------------------------------------------------
            with engine.begin() as conn:
                merge_call = text(f"EXEC dbo.{sp_merge_name}")
                conn.execute(merge_call)
            logging.info(f"Called stored procedure: {sp_merge_name} for final MERGE.")

            # ---------------------------------------------------
            # 4D) TRUNCATE staging table AFTER merging
            # ---------------------------------------------------
            with engine.begin() as conn:
                truncate_stmt = text(f"TRUNCATE TABLE dbo.{staging_table_name};")
                conn.execute(truncate_stmt)
            logging.info(f"Truncated staging table: {staging_table_name} after merge.")

            logging.info(f"Successfully processed CSV '{csv_name}'.")

        except Exception as e:
            logging.error(f"Error loading {csv_name} into {staging_table_name}: {e}")

    logging.info("All CSV files processed in Load step.")