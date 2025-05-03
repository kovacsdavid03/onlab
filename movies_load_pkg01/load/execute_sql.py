import pyodbc
import movies_load_pkg01.resources.db_conn as db_conn
from sqlalchemy import text
from dagster import get_dagster_logger 

def execute_sql(sql_file_path):
    logger = get_dagster_logger()
    logger.info("Starting SQL script execution process...")
    try:
        with db_conn().connect() as connection:
            with open(sql_file_path, 'r') as sql_file:
                logger.info(f"Reading SQL script from '{sql_file_path}'...")
                sql_script = sql_file.read().strip()  # Read and remove leading/trailing whitespace

            logger.info(f"Executing SQL script from '{sql_file_path}'...")
            connection.execute(text(sql_script))

            connection.commit()
            logger.info(f"SQL script '{sql_file_path}' executed successfully.")

    except Exception as e:
        if 'connection' in locals():
            connection.rollback()
        logger.critical(f"An error occurred while executing '{sql_file_path}': {e}")