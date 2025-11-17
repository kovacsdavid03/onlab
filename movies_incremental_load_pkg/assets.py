"""
Incremental Load Assets

This module contains Dagster assets for incremental loading and data processing on the movies data warehouse.
Performs incremental updates without truncating existing star schema data operations.
"""

from anyio import sleep
from dagster import asset, get_dagster_logger, AssetIn
from sqlalchemy import text
from movies_load_pkg01.resources.db_conn import db_conn
from movies_load_pkg01.load.execute_sql import execute_sql
from movies_load_pkg01.resources.truncate_tables import truncate_tables
from movies_load_pkg01.resources.truncate_tables import temp_ss_tables

logger = get_dagster_logger()

@asset(group_name="incremental_transform_load_movies", 
       description="transform and load new movies from existing database",
       compute_kind="sql")
def incremental_transform_movies():
    execute_sql('./movies_incremental_load_pkg/sql_scripts/T_inc_fact_movies.sql')

@asset(group_name="incremental_load_fact", 
       description="Load new movies to fact table incrementally",
       deps=[ incremental_transform_movies],
             compute_kind="sql")
def incremental_load_fact_movies():
    execute_sql('./movies_load_pkg01/resources/load_scripts/ss_tables/L_fact_movies.sql')

@asset(group_name="T_Wait", description="Wait for fact movies load to complete", deps=[incremental_load_fact_movies])
def incremental_init():
    sleep(1)  

@asset(group_name="incremental_transform", 
       description="Extract crew data for new movies from existing database",
       deps=[incremental_init],
       compute_kind="sql")
def incremental_transform_crew():
    execute_sql('./movies_incremental_load_pkg/sql_scripts/T_inc_dim_crew.sql')

@asset(group_name="incremental_transform", 
       description="Extract genres data for new movies from existing database",
       deps=[incremental_init],
       compute_kind="sql")
def incremental_transform_genres():
    execute_sql('./movies_incremental_load_pkg/sql_scripts/T_inc_dim_genre.sql')

@asset(group_name="incremental_transform", 
       description="Extract keywords data for new movies from existing database",
       deps=[incremental_init],
       compute_kind="sql")
def incremental_transform_keywords():
    execute_sql('./movies_incremental_load_pkg/sql_scripts/T_inc_dim_keyword.sql')

@asset(group_name="incremental_transform", 
       description="Extract production companies data for new movies from existing database",
       deps=[incremental_init],
       compute_kind="sql")
def incremental_transform_production_companies():
    execute_sql('./movies_incremental_load_pkg/sql_scripts/T_inc_dim_company.sql')
    

@asset(group_name="incremental_transform", 
       description="Extract production countries data for new movies from existing database",
       deps=[incremental_init],
       compute_kind="sql")
def incremental_transform_production_countries():
    execute_sql('./movies_incremental_load_pkg/sql_scripts/T_inc_dim_country.sql')

@asset(group_name="incremental_transform", 
       description="Extract spoken languages data for new movies from existing database",
       deps=[incremental_init],
       compute_kind="sql")
def incremental_transform_spoken_languages():
    execute_sql('./movies_incremental_load_pkg/sql_scripts/T_inc_dim_language.sql')


@asset(group_name="incremental_transform", description="Extract new cast data from existing database",
       deps=[incremental_init],
       compute_kind="sql")
def incremental_transform_cast():
    execute_sql('./movies_incremental_load_pkg/sql_scripts/T_inc_dim_cast.sql')

@asset(group_name="incremental_transform", description="Extract new favourite data from existing database",
       deps=[incremental_init],
       compute_kind="sql")
def incremental_transform_favourites():
    execute_sql('./movies_incremental_load_pkg/sql_scripts/T_inc_dim_favourite.sql')

@asset(group_name="incremental_transform", description="Extract new time data from existing database",
       deps=[incremental_init],
       compute_kind="sql")
def incremental_transform_time():
    execute_sql('./movies_incremental_load_pkg/sql_scripts/T_inc_dim_time.sql')

@asset(group_name="L_wait", description="Wait step to ensure all transforms are complete before loading dimensions",
       deps=[incremental_transform_crew, incremental_transform_genres, incremental_transform_keywords,
             incremental_transform_production_companies, incremental_transform_production_countries,
             incremental_transform_spoken_languages, incremental_transform_time])
def incremental_wait():
    sleep(1) 

@asset(group_name="incremental_load_dimensions", 
       description="Load new dimension data incrementally",
       deps=[incremental_wait],
       compute_kind="sql")
def incremental_load_dim_cast():
   execute_sql('./movies_load_pkg01/resources/load_scripts/ss_tables/L_dim_cast.sql')

@asset(group_name="incremental_load_dimensions", 
       description="Load new crew dimension data incrementally", 
       deps=[incremental_wait],
       compute_kind="sql")
def incremental_load_dim_crew():
    execute_sql('./movies_load_pkg01/resources/load_scripts/ss_tables/L_dim_crew.sql')

@asset(group_name="incremental_load_dimensions", 
        description="Load new genre dimension data incrementally",
       deps=[incremental_wait],
       compute_kind="sql")
def incremental_load_dim_genre():
    execute_sql('./movies_load_pkg01/resources/load_scripts/ss_tables/L_dim_genre.sql')

@asset(group_name="incremental_load_dimensions", 
       description="Load new keyword dimension data incrementally",
       deps=[incremental_wait],
       compute_kind="sql")
def incremental_load_dim_keyword():
    execute_sql('./movies_load_pkg01/resources/load_scripts/ss_tables/L_dim_keyword.sql')

@asset(group_name="incremental_load_dimensions", 
       description="Load new company dimension data incrementally",
       deps=[incremental_wait],
       compute_kind="sql")
def incremental_load_dim_company():
    execute_sql('./movies_load_pkg01/resources/load_scripts/ss_tables/L_dim_company.sql')

@asset(group_name="incremental_load_dimensions", 
       description="Load new country dimension data incrementally",
       deps=[incremental_wait],
       compute_kind="sql")
def incremental_load_dim_country():
    execute_sql('./movies_load_pkg01/resources/load_scripts/ss_tables/L_dim_country.sql')

@asset(group_name="incremental_load_dimensions", 
       description="Load new language dimension data incrementally",
       deps=[incremental_wait],
       compute_kind="sql")
def incremental_load_dim_language():
    execute_sql('./movies_load_pkg01/resources/load_scripts/ss_tables/L_dim_language.sql')

@asset(group_name="incremental_load_dimensions", 
       description="Load new favourites dimension data incrementally",
       deps=[incremental_wait],
       compute_kind="sql")
def incremental_load_dim_favourites():
    execute_sql('./movies_load_pkg01/resources/load_scripts/ss_tables/L_dim_favourites.sql')

@asset(group_name="incremental_load_dimensions", 
       description="Load new time dimension data incrementally",
       deps=[incremental_wait],
       compute_kind="sql")
def incremental_load_dim_time():
    execute_sql('./movies_load_pkg01/resources/load_scripts/ss_tables/L_dim_time.sql')

@asset(group_name="L_wait2", description="Wait step to ensure all transforms are complete before bridges",
       deps=[incremental_load_dim_crew, incremental_load_dim_genre, incremental_load_dim_keyword,
             incremental_load_dim_company, incremental_load_dim_country,
             incremental_load_dim_language, incremental_load_dim_time])
def incremental_wait2():
    sleep(1)

@asset(group_name="incremental_load_bridges", 
       description="Transform new cast bridge relationships",
       deps=[incremental_wait2],
       compute_kind="sql")
def incremental_transform_bridge_cast():
    execute_sql('./movies_load_pkg01/resources/transform_scripts/T_bridge_cast.sql')

@asset(group_name="incremental_load_bridges", 
       description="Load new cast bridge relationships",
       deps=[incremental_transform_bridge_cast],
       compute_kind="sql")
def incremental_load_bridge_cast():
    execute_sql('./movies_load_pkg01/resources/load_scripts/ss_tables/L_bridge_cast.sql')

@asset(group_name="incremental_load_bridges", 
       description="Transform new crew bridge relationships",
       deps=[incremental_wait2],
       compute_kind="sql")
def incremental_transform_bridge_crew():
    execute_sql('./movies_load_pkg01/resources/transform_scripts/T_bridge_crew.sql')

@asset(group_name="incremental_load_bridges", 
       description="Load new crew bridge relationships",
       deps=[incremental_transform_bridge_crew],
       compute_kind="sql")
def incremental_load_bridge_crew():
    execute_sql('./movies_load_pkg01/resources/load_scripts/ss_tables/L_bridge_crew.sql')

@asset(group_name="incremental_load_bridges", 
       description="Transform new genre bridge relationships",
       deps=[incremental_wait2],
       compute_kind="sql")
def incremental_transform_bridge_genre():
    execute_sql('./movies_load_pkg01/resources/transform_scripts/T_bridge_genre.sql')

@asset(group_name="incremental_load_bridges", 
       description="Load new genre bridge relationships",
       deps=[incremental_transform_bridge_genre],
       compute_kind="sql")
def incremental_load_bridge_genre():
    execute_sql('./movies_load_pkg01/resources/load_scripts/ss_tables/L_bridge_genre.sql')

@asset(group_name="incremental_load_bridges", 
       description="Transform new keyword bridge relationships",
       deps=[incremental_wait2],
       compute_kind="sql")
def incremental_transform_bridge_keyword():
    execute_sql('./movies_load_pkg01/resources/transform_scripts/T_bridge_keyword.sql')

@asset(group_name="incremental_load_bridges", 
       description="Load new keyword bridge relationships",
       deps=[incremental_transform_bridge_keyword],
       compute_kind="sql")
def incremental_load_bridge_keyword():
    execute_sql('./movies_load_pkg01/resources/load_scripts/ss_tables/L_bridge_keyword.sql')

@asset(group_name="incremental_load_bridges", 
       description="Transform new company bridge relationships",
       deps=[incremental_wait2],
       compute_kind="sql")
def incremental_transform_bridge_company():
    execute_sql('./movies_load_pkg01/resources/transform_scripts/T_bridge_company.sql')

@asset(group_name="incremental_load_bridges", 
       description="Load new company bridge relationships",
       deps=[incremental_transform_bridge_company],
       compute_kind="sql")
def incremental_load_bridge_company():
    execute_sql('./movies_load_pkg01/resources/load_scripts/ss_tables/L_bridge_company.sql')

@asset(group_name="incremental_load_bridges", 
       description="Transform new country bridge relationships incrementally",
       deps=[incremental_wait2],
       compute_kind="sql")
def incremental_transform_bridge_country():
    execute_sql('./movies_load_pkg01/resources/transform_scripts/T_bridge_country.sql')

@asset(group_name="incremental_load_bridges", 
       description="Load new country bridge relationships incrementally",
       deps=[incremental_transform_bridge_country],
       compute_kind="sql")
def incremental_load_bridge_country():
    execute_sql('./movies_load_pkg01/resources/load_scripts/ss_tables/L_bridge_country.sql')

@asset(group_name="incremental_load_bridges", 
       description="Transform new language bridge relationships incrementally",
       deps=[incremental_wait2],
       compute_kind="sql")
def incremental_transform_bridge_language():
    execute_sql('./movies_load_pkg01/resources/transform_scripts/T_bridge_language.sql')

@asset(group_name="incremental_load_bridges", 
       description="Load new language bridge relationships incrementally",
       deps=[incremental_transform_bridge_language],
       compute_kind="sql")
def incremental_load_bridge_language():
    execute_sql('./movies_load_pkg01/resources/load_scripts/ss_tables/L_bridge_language.sql')

@asset(group_name="incremental_cleanup", 
       description="Clean up incremental staging",
       deps=[incremental_load_bridge_cast, incremental_load_bridge_crew, incremental_load_bridge_genre, 
             incremental_load_bridge_keyword, incremental_load_bridge_company, incremental_load_bridge_country, 
             incremental_load_bridge_language],
             compute_kind="sql")
def incremental_cleanup():
    truncate_tables(temp_ss_tables)


