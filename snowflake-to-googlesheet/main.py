import os
import json
import logging
import base64
from pathlib import Path
from datetime import date

import pandas as pd
from sqlalchemy import create_engine, text
from snowflake.sqlalchemy import URL
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Setup logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def load_config(path: str = "config.json") -> dict:
    try:
        with open(path, "r") as file:
            config = json.load(file)
            logger.info("Loaded config.json successfully.")
            return config
    except Exception as e:
        logger.error(f"Error loading config.json: {e}")
        raise


def load_query(filepath: str = "query.sql") -> str:
    return Path(filepath).read_text(encoding="utf-8")


def load_private_key_base64(path: str) -> str:
    try:
        with open(path, "rb") as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None,
                backend=default_backend()
            )
            logger.info("Private key loaded successfully")
            private_key_der = private_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
            return base64.b64encode(private_key_der).decode("utf-8")
    except Exception as e:
        logger.error(f"Failed to load or convert private key: {e}")
        raise


class SnowflakeClient:
    def __init__(self, config: dict, private_key_base64: str):
        try:
            self.url = URL(
                user=config["SNOWFLAKE_USER"],
                private_key=private_key_base64,
                account=config["SNOWFLAKE_ACCOUNT"],
                warehouse=config["SNOWFLAKE_WAREHOUSE"],
                database=config["SNOWFLAKE_DATABASE"],
                schema=config["SNOWFLAKE_SCHEMA"]
            )
            self.engine = create_engine(self.url)
            self.connection = self.engine.connect()
            self.connection.execute(text(f"USE WAREHOUSE {config['SNOWFLAKE_WAREHOUSE']};"))
            logger.info("Snowflake connection established.")
        except Exception as e:
            logger.error(f"Error initializing Snowflake: {e}")
            raise

    def fetch_data(self, query: str) -> pd.DataFrame:
        try:
            logger.info("Fetching data from Snowflake...")
            return pd.read_sql(query, self.connection)
        except Exception as e:
            logger.error(f"Error fetching data: {e}")
            raise

    def close(self):
        try:
            logger.info("Closing Snowflake connection.")
            self.connection.close()
        except Exception as e:
            logger.error(f"Error closing Snowflake connection: {e}")


class GoogleSheetsClient:
    def __init__(self, json_keyfile: str):
        scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        try:
            credentials = ServiceAccountCredentials.from_json_keyfile_name(json_keyfile, scope)
            self.gc = gspread.authorize(credentials)
            logger.info("Google Sheets authorization successful")
        except Exception as e:
            logger.error(f"Authorization error: {e}")
            raise

    def get_spreadsheet_by_url(self, url: str):
        try:
            spreadsheet = self.gc.open_by_url(url)
            logger.info(f"Opened Google Spreadsheet: {url}")
            return spreadsheet
        except Exception as e:
            logger.error(f"Could not open spreadsheet by URL: {e}")
            raise


class SheetManager:
    def __init__(self, spreadsheet):
        self.spreadsheet = spreadsheet

    def overwrite_with_dataframe(self, sheet_name: str, df: pd.DataFrame) -> bool:
        try:
            try:
                worksheet = self.spreadsheet.worksheet(sheet_name)
                worksheet.clear()
                logger.info(f"Sheet '{sheet_name}' cleared.")
            except gspread.exceptions.WorksheetNotFound:
                worksheet = self.spreadsheet.add_worksheet(
                    title=sheet_name,
                    rows=str(max(100, len(df) + 1)),
                    cols=str(max(10, len(df.columns) + 1))
                )
                logger.info(f"Created new sheet '{sheet_name}'.")

            data = [df.columns.values.tolist()] + df.values.tolist()
            worksheet.update("A1", data)
            logger.info(f"Data successfully written to sheet '{sheet_name}'.")
            return True
        except Exception as e:
            logger.error(f"Failed to write data to sheet '{sheet_name}': {e}")
            return False


def main():
    config = load_config()
    query = load_query("query.sql")
    private_key_base64 = load_private_key_base64(config["RSA_KEY_PATH"])

    db = SnowflakeClient(config, private_key_base64)
    try:
        df = db.fetch_data(query)
        df["update_date"] = date.today().isoformat()
    finally:
        db.close()

    sheets_client = GoogleSheetsClient(config["GOOGLE_KEYFILE_PATH"])
    spreadsheet = sheets_client.get_spreadsheet_by_url(config["SPREADSHEET_URL"])

    manager = SheetManager(spreadsheet)
    success = manager.overwrite_with_dataframe(config["SHEET_NAME"], df)

    if success:
        logger.info("Data successfully pushed to Google Sheets.")
    else:
        logger.warning("Data push to Google Sheets failed.")


if __name__ == "__main__":
    main()
