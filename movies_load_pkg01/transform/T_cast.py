import pandas as pd
import ast
import sqlalchemy
import pyodbc
from movies_load_pkg01.resources.db_conn import db_conn
from dagster import get_dagster_logger

def T_cast():
    logger = get_dagster_logger()
    csv_file = './movies_load_pkg01/landing_zone/credits.csv'  
    df = pd.read_csv(csv_file)

    logger.info(f"Loaded {csv_file} with {df.shape[0]} rows and {df.shape[1]} columns.")

    df['cast'] = df['cast'].apply(ast.literal_eval)
    df = df[df['cast'].apply(lambda x: len(x) > 0)]
    df_exploded = df.explode('cast')

    df_final = pd.DataFrame({
        'MovieId': df_exploded['id'],
        'character': df_exploded['cast'].apply(lambda x: x['character'] if pd.notna(x) else None),
        'gender': df_exploded['cast'].apply(lambda x: x['gender'] if pd.notna(x) else None),
        'name': df_exploded['cast'].apply(lambda x: x['name'] if pd.notna(x) else None)
    })

    df_final['character'] = df_final['character'].str.replace(r'\s*\(voice\)', '', regex=True)

    def dedup_characters(char_list):
        if not isinstance(char_list, list): 
            return char_list
        seen = set()
        deduped = []
        for char in char_list:
            if isinstance(char, str):
                lower_char = char.strip().lower()
                if lower_char not in seen:
                    seen.add(lower_char)
                    deduped.append(char)
            else:
                deduped.append(char)  # Handle non-string types if any
        return deduped

    df_final['character'] = df_final['character'].str.split(' / ').apply(dedup_characters)

    df_final = df_final.explode('character')

    df_final.dropna(subset=['character', 'gender', 'name'], inplace=True)

    logger.info(f"DataFrame 'temp_cast' has {df_final.shape[0]} rows and {df_final.shape[1]} columns.")

    df_final.to_sql('temp_cast', con=db_conn(), if_exists='append', index=False)
    logger.info(f"DataFrame written to SQL table 'temp_cast' with {df_final.shape[0]} rows.")
    