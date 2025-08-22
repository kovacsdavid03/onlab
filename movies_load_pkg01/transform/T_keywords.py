import pandas as pd
import ast
from movies_load_pkg01.resources.db_conn import db_conn
from dagster import get_dagster_logger


def T_keywords():
    logger = get_dagster_logger()
    csv_file = './movies_load_pkg01/landing_zone/keywords.csv'
    df = pd.read_csv(csv_file)
    logger.info(f"Loaded {csv_file} with {df.shape[0]} rows and {df.shape[1]} columns.")
    df['keywords'] = df['keywords'].apply(ast.literal_eval)

    df_exploded = df.explode('keywords')

    df_final = pd.DataFrame({
        'movieID': df_exploded['id'],
        'keyword': df_exploded['keywords'].apply(lambda x: x['name'] if pd.notna(x) else None)
    })

    df_final = df_final[df_final['keyword'] != '']

    df_final.dropna(subset=['keyword'], inplace=True)

    logger.info(f"DataFrame 'temp_keywords' has {df_final.shape[0]} rows and {df_final.shape[1]} columns.")

    df_final.to_sql('temp_keywords', con=db_conn(), if_exists='append', index=False)
    logger.info(f"DataFrame written to SQL table 'temp_keywords' with {df_final.shape[0]} rows.")