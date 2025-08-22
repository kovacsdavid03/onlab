import pandas as pd
from movies_load_pkg01.resources.db_conn import db_conn
from movies_load_pkg01.resources.safe_literal_eval import safe_literal_eval
from dagster import get_dagster_logger

def T_production_companies():
    csv_file = './movies_load_pkg01/landing_zone/movies_metadata.csv'
    logger = get_dagster_logger()
    df = pd.read_csv(csv_file, encoding='utf-8')
    logger.info(f"Loaded {csv_file} with {df.shape[0]} rows and {df.shape[1]} columns.")

    df['production_companies'] = df['production_companies'].fillna('[]')

    df['production_companies'] = df['production_companies'].apply(safe_literal_eval)

    df = df[df['production_companies'].apply(lambda x: isinstance(x, list) and len(x) > 0)]
    logger.info(f"Exploding column 'production_companies': rows before explode = {df.shape[0]}.")
    df_exploded = df.explode('production_companies')
    logger.info(
        f"Exploded column 'production_companies' into fields [production_company]: rows after explode = {df_exploded.shape[0]} (delta = {df_exploded.shape[0] - df.shape[0]})."
    )

    df_final = pd.DataFrame({
        'movieid': df_exploded['id'],
        'production_company': df_exploded['production_companies'].apply(lambda x: x['name'] if pd.notna(x) and isinstance(x, dict) else None)
    })

    df_final.dropna(subset=['production_company'], inplace=True)
    logger.info(f"DataFrame 'temp_production_companies' has {df_final.shape[0]} rows and {df_final.shape[1]} columns.")
    df_final.to_sql('temp_production_companies', con=db_conn(), if_exists='append', index=False)
    logger.info(f"DataFrame written to SQL table 'temp_production_companies' with {df_final.shape[0]} rows.")