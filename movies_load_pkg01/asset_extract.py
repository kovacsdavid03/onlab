from dagster import asset
from kaggle import KaggleApi
from movies_load_pkg01.transform.T_keywords import T_keywords
from movies_load_pkg01.transform.T_cast import T_cast
from movies_load_pkg01.transform.T_crew import T_crew
from movies_load_pkg01.transform.T_movies import T_movies
from movies_load_pkg01.transform.T_genres import T_genres
from movies_load_pkg01.transform.T_production_companies import T_production_companies
from movies_load_pkg01.transform.T_production_countries import T_production_countries
from movies_load_pkg01.transform.T_spoken_languages import T_spoken_languages
from movies_load_pkg01.load import execute_load
from movies_load_pkg01.resources.truncate_tables import truncate_tables
from movies_load_pkg01.resources.archive_lz import archive_lz

@asset(group_name="Extract_movies", description="Extract movies dataset from Kaggle", compute_kind="CSV")
def extract_movies():
    api = KaggleApi()
    api.authenticate()

    api.dataset_download_files('rounakbanik/the-movies-dataset', path='./movies_load_pkg01/landing_zone', unzip=True, quiet=False)

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
    execute_load("./movies_load_pkg01/resources/load_scripts/L_keywords.sql")

@asset(group_name="L_cast", description="Load cast dataset", deps=[transform_cast], compute_kind="SQL")
def load_cast():
    execute_load("./movies_load_pkg01/resources/load_scripts/L_cast.sql")

@asset(group_name="L_crew", description="Load crew dataset", deps=[transfrom_crew], compute_kind="SQL")
def load_crew():
    execute_load("./movies_load_pkg01/resources/load_scripts/L_crew.sql")

@asset(group_name="L_movies", description="Load movies dataset", deps=[transform_movies], compute_kind="SQL")
def load_movies():
    execute_load("./movies_load_pkg01/resources/load_scripts/L_movies.sql")

@asset(group_name="L_genres", description="Load genres dataset", deps=[transform_genres], compute_kind="SQL")
def load_genres():
    execute_load("./movies_load_pkg01/resources/load_scripts/L_genres.sql")

@asset(group_name="L_production_companies", description="Load production companies dataset", deps=[transform_production_companies], compute_kind="SQL")
def load_production_companies():
    execute_load("./movies_load_pkg01/resources/load_scripts/L_production_companies.sql")

@asset(group_name="L_production_countries", description="Load production countries dataset", deps=[transform_production_countries], compute_kind="SQL")
def load_production_countries():
    execute_load("./movies_load_pkg01/resources/load_scripts/L_production_countries.sql")

@asset(group_name="L_spoken_languages", description="Load spoken languages dataset", deps=[transform_spoken_languages], compute_kind="SQL")
def load_spoken_languages():
    execute_load("./movies_load_pkg01/resources/load_scripts/L_spoken_languages.sql")

@asset(group_name="archive_pkg", description="Archive package", deps=[load_keywords, load_cast, load_crew, load_movies, load_genres, load_production_companies, load_production_countries, load_spoken_languages], compute_kind="OS")
def archive_package():
    archive_lz("./movies_load_pkg01/landing_zone", "./movies_load_pkg01/archive")

@asset(group_name="truncate_temp_tables", description="Truncate tables", deps=[archive_package], compute_kind="SQL")
def truncate_temp_tables():
    truncate_tables()
