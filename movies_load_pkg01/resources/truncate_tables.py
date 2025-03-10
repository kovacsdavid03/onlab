from sqlalchemy import text
from movies_load_pkg01.resources.db_conn import db_conn

tables: list = [
    "temp_spoken_languages",
    "temp_production_countries",
    "temp_production_companies",
    "temp_genres",
    "temp_movies",
    "temp_crew",
    "temp_cast",
    "temp_keywords"
]

def truncate_tables():
    try:
        with db_conn().connect() as connection:
            for table_name in tables:
                truncate_sql = f"TRUNCATE TABLE {table_name};"

                connection.execute(text(truncate_sql))

                connection.commit()
                print(f"Table '{table_name}' truncated successfully.")

    except Exception as e:
        if 'connection' in locals():
            connection.rollback()
        print(f"An error occurred while truncating tables: {e}")