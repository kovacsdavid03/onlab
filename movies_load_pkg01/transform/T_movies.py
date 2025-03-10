import pandas as pd
import ast
import sqlalchemy
import pyodbc
from movies_load_pkg01.resources.db_conn import db_conn


def T_movies():
    #TODO: Chinese and arabic characters are not displayed correctly in the database, need to fix this

    csv_file = './movies_load_pkg01/landing_zone/movies_metadata.csv'
    df = pd.read_csv(csv_file, encoding='utf-8', on_bad_lines='error')


    movies_df = df[[
        'id', 'imdb_id', 'adult', 'budget', 'original_title',
        'popularity', 'release_date', 'revenue', 'runtime', 'tagline', 'vote_average', 'vote_count'
    ]]

    movies_df = movies_df.rename(columns={
        'id': 'MovieId',
        'imdb_id': 'imdbId'
    })

    movies_df['tagline'] = movies_df['tagline'].fillna('')
    movies_df['imdbId'] = movies_df['imdbId'].str.lstrip('tt') #imdbId are stored as strings, remove the 'tt' prefix
    movies_df['MovieId'] = pd.to_numeric(movies_df['MovieId'], errors='coerce') #there is a malformed entry in the dataset that pandas can't detect, so we force it to numeric
    movies_df.dropna(subset=['MovieId', 'imdbId', 'original_title'], inplace=True)
    #print(movies_df['tagline'].max() ) 
    #print(len(movies_df['tagline'].max()))
    #output_csv_file = 'movies.csv'  
    #movies_df.to_csv(output_csv_file, index=False, encoding='utf-8')
    movies_df.to_sql('temp_movies', con=db_conn(), if_exists='append', index=False)