from sqlalchemy import text
from movies_load_pkg01.resources.db_conn import db_conn
from dagster import get_dagster_logger

temp_tables: list = [
    "temp_spoken_languages",
    "temp_production_countries",
    "temp_production_companies",
    "temp_genres",
    "temp_movies",
    "temp_crew",
    "temp_cast",
    "temp_keywords"
]

raw_tables: list = [
    "spoken_languages",
    "production_countries",
    "production_companies",
    "genres",
    "movies",
    "crew",
    "cast",
    "keywords"
]

temp_ss_tables: list = [
    "temp_bridge_genre",
    "temp_bridge_cast",
    "temp_bridge_crew",
    "temp_bridge_company",
    "temp_bridge_country",
    "temp_bridge_language",
    "temp_bridge_keyword",
    "temp_dim_genre",
    "temp_dim_cast",
    "temp_dim_crew",
    "temp_dim_company",
    "temp_dim_country",
    "temp_dim_language",
    "temp_dim_keyword",
    "temp_fact_movies",
    "temp_dim_time",
    "temp_dim_favourite"
]

def truncate_tables(tables: list) -> None:
    logger = get_dagster_logger()
    logger.info("Starting truncate tables process...")
    try:
        with db_conn().connect() as connection:
            for table_name in tables:
                truncate_sql = f"TRUNCATE TABLE {table_name};"

                connection.execute(text(truncate_sql))

                connection.commit()
                logger.info(f"Table '{table_name}' truncated successfully.")

    except Exception as e:
        if 'connection' in locals():
            connection.rollback()
        logger.critical(f"An error occurred while truncating tables: {e}")
