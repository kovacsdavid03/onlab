from asyncio import sleep
from dagster import asset, get_dagster_logger
from kaggle import KaggleApi
from movies_load_pkg01.transform.T_keywords import T_keywords
from movies_load_pkg01.transform.T_cast import T_cast
from movies_load_pkg01.transform.T_crew import T_crew
from movies_load_pkg01.transform.T_movies import T_movies
from movies_load_pkg01.transform.T_genres import T_genres
from movies_load_pkg01.transform.T_production_companies import T_production_companies
from movies_load_pkg01.transform.T_production_countries import T_production_countries
from movies_load_pkg01.transform.T_spoken_languages import T_spoken_languages
from movies_load_pkg01.load import execute_sql
from movies_load_pkg01.resources.truncate_tables import truncate_tables
from movies_load_pkg01.resources.truncate_tables import temp_tables
from movies_load_pkg01.resources.truncate_tables import raw_tables
from movies_load_pkg01.resources.truncate_tables import temp_ss_tables
from movies_load_pkg01.resources.archive_lz import archive_lz
from movies_load_pkg01.resources.delete_tables import delete_ss_tables

logger = get_dagster_logger()

@asset(group_name="truncate_s_tables", description="Truncate tables", compute_kind="SQL")
def truncate_s_tables():
    truncate_tables(raw_tables)

@asset(group_name="Extract_movies", description="Extract movies dataset from Kaggle", deps=[truncate_s_tables], compute_kind="CSV")
def extract_movies():
    logger.info("Starting extraction phase")
    api = KaggleApi()
    api.authenticate()

    api.dataset_download_files('rounakbanik/the-movies-dataset', path='./movies_load_pkg01/landing_zone', unzip=True, quiet=False)
    logger.info("Extraction phase completed")

@asset(group_name="T_keywords", description="Transformed keywords dataset", deps=[extract_movies], compute_kind="pandas")
def transform_keywords():
    T_keywords()

@asset(group_name="T_cast", description="Transformed cast dataset", deps=[extract_movies], compute_kind="pandas")
def transform_cast():
    T_cast()

@asset(group_name="T_crew", description="Transformed crew dataset", deps=[extract_movies], compute_kind="pandas")
def transfrom_crew():
    T_crew()

@asset(group_name="T_movies", description="Transformed movies dataset", deps=[extract_movies], compute_kind="pandas")
def transform_movies():
    T_movies()

@asset(group_name="T_genres", description="Transformed genres dataset", deps=[extract_movies], compute_kind="pandas")
def transform_genres():
    T_genres()

@asset(group_name="T_production_companies", description="Transformed production companies dataset", deps=[extract_movies], compute_kind="pandas")
def transform_production_companies():
    T_production_companies()

@asset(group_name="T_production_countries", description="Transformed production countries dataset", deps=[extract_movies], compute_kind="pandas")
def transform_production_countries():
    T_production_countries()

@asset(group_name="T_spoken_languages", description="Transformed spoken languages dataset", deps=[extract_movies], compute_kind="pandas")
def transform_spoken_languages():
    T_spoken_languages()

@asset(group_name="L_keywords", description="Load keywords dataset", deps=[transform_keywords], compute_kind="SQL")
def load_keywords():
    execute_sql("./movies_load_pkg01/resources/load_scripts/raw_tables/L_keywords.sql")

@asset(group_name="L_cast", description="Load cast dataset", deps=[transform_cast], compute_kind="SQL")
def load_cast():
    execute_sql("./movies_load_pkg01/resources/load_scripts/raw_tables/L_cast.sql")

@asset(group_name="L_crew", description="Load crew dataset", deps=[transfrom_crew], compute_kind="SQL")
def load_crew():
    execute_sql("./movies_load_pkg01/resources/load_scripts/raw_tables/L_crew.sql")

@asset(group_name="L_movies", description="Load movies dataset", deps=[transform_movies], compute_kind="SQL")
def load_movies():
    execute_sql("./movies_load_pkg01/resources/load_scripts/raw_tables/L_movies.sql")

@asset(group_name="L_genres", description="Load genres dataset", deps=[transform_genres], compute_kind="SQL")
def load_genres():
    execute_sql("./movies_load_pkg01/resources/load_scripts/raw_tables/L_genres.sql")

@asset(group_name="L_production_companies", description="Load production companies dataset", deps=[transform_production_companies], compute_kind="SQL")
def load_production_companies():
    execute_sql("./movies_load_pkg01/resources/load_scripts/raw_tables/L_production_companies.sql")

@asset(group_name="L_production_countries", description="Load production countries dataset", deps=[transform_production_countries], compute_kind="SQL")
def load_production_countries():
    execute_sql("./movies_load_pkg01/resources/load_scripts/raw_tables/L_production_countries.sql")

@asset(group_name="L_spoken_languages", description="Load spoken languages dataset", deps=[transform_spoken_languages], compute_kind="SQL")
def load_spoken_languages():
    execute_sql("./movies_load_pkg01/resources/load_scripts/raw_tables/L_spoken_languages.sql")

@asset(group_name="archive_pkg", description="Archive package", deps=[load_keywords, load_cast, load_crew, load_movies, load_genres, load_production_companies, load_production_countries, load_spoken_languages], compute_kind="OS")
def archive_package():
    archive_lz("./movies_load_pkg01/landing_zone", "./movies_load_pkg01/archive")

@asset(group_name="truncate_temp_tables", description="Truncate tables", deps=[archive_package], compute_kind="SQL")
def truncate_temp_table():
    truncate_tables(temp_tables)

@asset(group_name="waiting_for_steps", description="Waiting for steps to finish", deps=[truncate_temp_table])
def waiting_for_steps():
    sleep(10)

@asset(group_name="delete_ss_tables", description="Delete star schema tables", deps=[waiting_for_steps], compute_kind="SQL")
def delete_star_schema_tables():
    delete_ss_tables()

@asset(group_name="T_dim_cast", description="Transformed dim cast dataset", deps=[delete_star_schema_tables], compute_kind="SQL")
def transform_dim_cast():
    execute_sql("./movies_load_pkg01/resources/transform_scripts/T_dim_cast.sql")

@asset(group_name="T_dim_crew", description="Transformed dim crew dataset", deps=[delete_star_schema_tables], compute_kind="SQL")
def transform_dim_crew():
    execute_sql("./movies_load_pkg01/resources/transform_scripts/T_dim_crew.sql")

@asset(group_name="T_dim_company", description="Transformed dim company dataset", deps=[delete_star_schema_tables], compute_kind="SQL")
def transform_dim_company():
    execute_sql("./movies_load_pkg01/resources/transform_scripts/T_dim_company.sql")

@asset(group_name="T_dim_country", description="Transformed dim country dataset", deps=[delete_star_schema_tables], compute_kind="SQL")
def transform_dim_country():
    execute_sql("./movies_load_pkg01/resources/transform_scripts/T_dim_country.sql")

@asset(group_name="T_dim_genre", description="Transformed dim genre dataset", deps=[delete_star_schema_tables], compute_kind="SQL")
def transform_dim_genre():
    execute_sql("./movies_load_pkg01/resources/transform_scripts/T_dim_genre.sql")

@asset(group_name="T_dim_language", description="Transformed dim language dataset", deps=[delete_star_schema_tables], compute_kind="SQL")
def transform_dim_language():
    execute_sql("./movies_load_pkg01/resources/transform_scripts/T_dim_language.sql")

@asset(group_name="T_dim_keyword", description="Transformed dim keyword dataset", deps=[delete_star_schema_tables], compute_kind="SQL")
def transform_dim_keyword():
    execute_sql("./movies_load_pkg01/resources/transform_scripts/T_dim_keyword.sql")

@asset(group_name="T_dim_time", description="Transformed dim time dataset", deps=[delete_star_schema_tables], compute_kind="SQL")
def transform_dim_time():
    execute_sql("./movies_load_pkg01/resources/transform_scripts/T_dim_time.sql")

@asset(group_name="L_dim_cast", description="Load dim cast dataset", deps=[transform_dim_cast], compute_kind="SQL")
def load_dim_cast():
    execute_sql("./movies_load_pkg01/resources/load_scripts/ss_tables/L_dim_cast.sql")

@asset(group_name="L_dim_crew", description="Load dim crew dataset", deps=[transform_dim_crew], compute_kind="SQL")
def load_dim_crew():
    execute_sql("./movies_load_pkg01/resources/load_scripts/ss_tables/L_dim_crew.sql")

@asset(group_name="L_dim_company", description="Load dim company dataset", deps=[transform_dim_company], compute_kind="SQL")
def load_dim_company():
    execute_sql("./movies_load_pkg01/resources/load_scripts/ss_tables/L_dim_company.sql")

@asset(group_name="L_dim_country", description="Load dim country dataset", deps=[transform_dim_country], compute_kind="SQL")
def load_dim_country():
    execute_sql("./movies_load_pkg01/resources/load_scripts/ss_tables/L_dim_country.sql")

@asset(group_name="L_dim_genre", description="Load dim genre dataset", deps=[transform_dim_genre], compute_kind="SQL")
def load_dim_genre():
    execute_sql("./movies_load_pkg01/resources/load_scripts/ss_tables/L_dim_genre.sql")

@asset(group_name="L_dim_language", description="Load dim language dataset", deps=[transform_dim_language], compute_kind="SQL")
def load_dim_language():
    execute_sql("./movies_load_pkg01/resources/load_scripts/ss_tables/L_dim_language.sql")

@asset(group_name="L_dim_keyword", description="Load dim keyword dataset", deps=[transform_dim_keyword], compute_kind="SQL")
def load_dim_keyword():
    execute_sql("./movies_load_pkg01/resources/load_scripts/ss_tables/L_dim_keyword.sql")

@asset(group_name="L_dim_time", description="Load dim time dataset", deps=[transform_dim_time], compute_kind="SQL")
def load_dim_time():
    execute_sql("./movies_load_pkg01/resources/load_scripts/ss_tables/L_dim_time.sql")

@asset(group_name="T_fact_movies", description="Transformed fact movies dataset", deps=[load_dim_cast, load_dim_crew, load_dim_company, load_dim_country, load_dim_genre, load_dim_language, load_dim_keyword, load_dim_time], compute_kind="SQL")
def transform_fact_movies():
    execute_sql("./movies_load_pkg01/resources/transform_scripts/T_fact_movies.sql")

@asset(group_name="L_fact_movies", description="Load fact movies dataset", deps=[transform_fact_movies], compute_kind="SQL")
def load_fact_movies():
    execute_sql("./movies_load_pkg01/resources/load_scripts/ss_tables/L_fact_movies.sql")

@asset(group_name="T_bridge_cast", description="Transformed bridge cast dataset", deps=[load_fact_movies], compute_kind="SQL")
def transform_bridge_cast():
    execute_sql("./movies_load_pkg01/resources/transform_scripts/T_bridge_cast.sql")

@asset(group_name="T_bridge_crew", description="Transformed bridge crew dataset", deps=[load_fact_movies], compute_kind="SQL")
def transform_bridge_crew():
    execute_sql("./movies_load_pkg01/resources/transform_scripts/T_bridge_crew.sql")

@asset(group_name="T_bridge_company", description="Transformed bridge company dataset", deps=[load_fact_movies], compute_kind="SQL")
def transform_bridge_company():
    execute_sql("./movies_load_pkg01/resources/transform_scripts/T_bridge_company.sql")

@asset(group_name="T_bridge_country", description="Transformed bridge country dataset", deps=[load_fact_movies], compute_kind="SQL")
def transform_bridge_country():
    execute_sql("./movies_load_pkg01/resources/transform_scripts/T_bridge_country.sql")

@asset(group_name="T_bridge_genre", description="Transformed bridge genre dataset", deps=[load_fact_movies], compute_kind="SQL")
def transform_bridge_genre():
    execute_sql("./movies_load_pkg01/resources/transform_scripts/T_bridge_genre.sql")

@asset(group_name="T_bridge_language", description="Transformed bridge language dataset", deps=[load_fact_movies], compute_kind="SQL")
def transform_bridge_language():
    execute_sql("./movies_load_pkg01/resources/transform_scripts/T_bridge_language.sql")

@asset(group_name="T_bridge_keyword", description="Transformed bridge keyword dataset", deps=[load_fact_movies], compute_kind="SQL")
def transform_bridge_keyword():
    execute_sql("./movies_load_pkg01/resources/transform_scripts/T_bridge_keyword.sql")

@asset(group_name="L_bridge_cast", description="Load bridge cast dataset", deps=[transform_bridge_cast], compute_kind="SQL")
def load_bridge_cast():
    execute_sql("./movies_load_pkg01/resources/load_scripts/ss_tables/L_bridge_cast.sql")

@asset(group_name="L_bridge_crew", description="Load bridge crew dataset", deps=[transform_bridge_crew], compute_kind="SQL")
def load_bridge_crew():
    execute_sql("./movies_load_pkg01/resources/load_scripts/ss_tables/L_bridge_crew.sql")

@asset(group_name="L_bridge_company", description="Load bridge company dataset", deps=[transform_bridge_company], compute_kind="SQL")
def load_bridge_company():
    execute_sql("./movies_load_pkg01/resources/load_scripts/ss_tables/L_bridge_company.sql")

@asset(group_name="L_bridge_country", description="Load bridge country dataset", deps=[transform_bridge_country], compute_kind="SQL")
def load_bridge_country():
    execute_sql("./movies_load_pkg01/resources/load_scripts/ss_tables/L_bridge_country.sql")

@asset(group_name="L_bridge_genre", description="Load bridge genre dataset", deps=[transform_bridge_genre], compute_kind="SQL")
def load_bridge_genre():
    execute_sql("./movies_load_pkg01/resources/load_scripts/ss_tables/L_bridge_genre.sql")

@asset(group_name="L_bridge_language", description="Load bridge language dataset", deps=[transform_bridge_language], compute_kind="SQL")
def load_bridge_language():
    execute_sql("./movies_load_pkg01/resources/load_scripts/ss_tables/L_bridge_language.sql")

@asset(group_name="L_bridge_keyword", description="Load bridge keyword dataset", deps=[transform_bridge_keyword], compute_kind="SQL")
def load_bridge_keyword():
    execute_sql("./movies_load_pkg01/resources/load_scripts/ss_tables/L_bridge_keyword.sql")

@asset(group_name="truncate_temp_ss_tables", description="Truncate temp star schema tables", deps=[load_bridge_keyword, load_bridge_cast, load_bridge_company, load_bridge_country, load_bridge_crew, load_bridge_genre, load_bridge_language], compute_kind="SQL")
def truncate_temp_star_schema_tables():
    truncate_tables(temp_ss_tables)