import pandas as pd
import ast
from movies_load_pkg01.resources.db_conn import db_conn
from dagster import get_dagster_logger

def T_crew():
    logger = get_dagster_logger()
    csv_file = './movies_load_pkg01/landing_zone/credits.csv'  
    df = pd.read_csv(csv_file)
    logger.info(f"Loaded {csv_file} with {df.shape[0]} rows and {df.shape[1]} columns.")

    df['crew'] = df['crew'].apply(ast.literal_eval)

    df = df[df['crew'].apply(lambda x: len(x) > 0)]
    logger.info(f"Exploding column 'crew': rows before explode = {df.shape[0]}.")
    df_exploded = df.explode('crew')
    logger.info(
        f"Exploded column 'crew' into fields [department, gender, job, name]: rows after explode = {df_exploded.shape[0]} (delta = {df_exploded.shape[0] - df.shape[0]})."
    )

    df_final = pd.DataFrame({
        'movieid': df_exploded['id'],
        'department': df_exploded['crew'].apply(lambda x: x['department'] if pd.notna(x) else None),
        'gender': df_exploded['crew'].apply(lambda x: x['gender'] if pd.notna(x) else None),
        'job': df_exploded['crew'].apply(lambda x: x['job'] if pd.notna(x) else None),
        'name': df_exploded['crew'].apply(lambda x: x['name'] if pd.notna(x) else None)
    })

    df_final.dropna(subset=['department', 'gender', 'job', 'name'], inplace=True)

    logger.info(f"DataFrame 'temp_crew' has {df_final.shape[0]} rows and {df_final.shape[1]} columns.")

    df_final.to_sql('temp_crew', con=db_conn(), if_exists='append', index=False)
    logger.info(f"DataFrame written to SQL table 'temp_crew' with {df_final.shape[0]} rows.")
