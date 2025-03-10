import pyodbc
import movies_load_pkg01.resources.db_conn as db_conn
from sqlalchemy import text 

def execute_load(sql_file_path):
    try:
        # Connect to the database using the SQLAlchemy engine
        with db_conn().connect() as connection:
            # Read the SQL file
            with open(sql_file_path, 'r') as sql_file:
                sql_script = sql_file.read().strip()  # Read and remove leading/trailing whitespace

            # Execute the SQL script
            connection.execute(text(sql_script))

            # Commit the transaction (if applicable)
            connection.commit()
            print(f"SQL script '{sql_file_path}' executed successfully.")

    except Exception as e:
        # Rollback in case of error
        if 'connection' in locals():
            connection.rollback()
        print(f"An error occurred while executing '{sql_file_path}': {e}")