import pandas as pd
from movies_load_pkg01.resources.db_conn import db_conn
from dagster import get_dagster_logger


def T_movies():
    #TODO: Chinese and arabic characters are not displayed correctly in the database, need to fix this

    csv_file = './movies_load_pkg01/landing_zone/movies_metadata.csv'
    logger = get_dagster_logger()
    df = pd.read_csv(csv_file, encoding='utf-8', on_bad_lines='error', low_memory=False)
    logger.info(f"Loaded {csv_file} with {df.shape[0]} rows and {df.shape[1]} columns.")


    movies_df = df[[
        'id', 'imdb_id', 'adult', 'budget', 'original_title',
        'popularity', 'release_date', 'revenue', 'runtime', 'tagline', 'vote_average', 'vote_count'
    ]]

    movies_df = movies_df.rename(columns={
        'id': 'movieId',
        'imdb_id': 'imdbId'
    })

    movies_df['tagline'] = movies_df['tagline'].fillna('')
    movies_df['movieId'] = pd.to_numeric(movies_df['movieId'], errors='coerce') #there is a malformed entry in the dataset that pandas can't detect, so we force it to numeric
    movies_df['popularity'] = pd.to_numeric(movies_df['popularity'], errors='coerce', downcast='float') #convert scientific notation to standard decimal
    movies_df.dropna(subset=['movieId', 'imdbId', 'original_title'], inplace=True)
    logger.info(f"DataFrame 'temp_movies' has {movies_df.shape[0]} rows and {movies_df.shape[1]} columns.")

    logger.info(movies_df.to_sql('temp_movies', con=db_conn(), if_exists='append', index=False))
    logger.info(f"DataFrame written to SQL table 'temp_movies' with {movies_df.shape[0]} rows.")