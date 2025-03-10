import pandas as pd
import ast
import sqlalchemy
import pyodbc
from movies_load_pkg01.resources.db_conn import db_conn
from movies_load_pkg01.resources.safe_literal_eval import safe_literal_eval

def T_genres():
    csv_file = './movies_load_pkg01/landing_zone/movies_metadata.csv'  # Replace with your CSV file path
    df = pd.read_csv(csv_file, encoding='utf-8')  # Use 'latin1' if 'utf-8' fails

    # Step 2: Handle NaN values in the 'genres' column
    # Replace NaN with an empty list '[]' so it can be parsed as JSON
    df['genres'] = df['genres'].fillna('[]')
    df['genres'] = df['genres'].apply(safe_literal_eval)

    # Step 4: Filter out rows where the 'genres' column is an empty list
    df = df[df['genres'].apply(lambda x: isinstance(x, list) and len(x) > 0)]

    # Step 5: Explode the list of genres into separate rows
    df_exploded = df.explode('genres')

    # Step 6: Extract relevant fields from the genres dictionaries and create a new DataFrame
    df_final = pd.DataFrame({
        'movieId': df_exploded['id'],
        'genre': df_exploded['genres'].apply(lambda x: x['name'] if pd.notna(x) and isinstance(x, dict) else None)
    })

    # Drop rows with NaN values (if any)
    df_final['movieId'] = pd.to_numeric(df_final['movieId'], errors='coerce') #there is a malformed entry in the dataset that pandas can't detect, so we force it to numeric
    df_final.dropna(subset=['movieId', 'genre'], inplace=True)
    df_final.dropna(subset=['genre'], inplace=True)

    # Step 7: Export the DataFrame to a CSV file (optional)
    #output_csv_file = 'genre_table.csv'  # Replace with your desired output CSV file path
    #df_final.to_csv(output_csv_file, index=False, encoding='utf-8')
    df_final.to_sql('temp_genres', con=db_conn(), if_exists='append', index=False)