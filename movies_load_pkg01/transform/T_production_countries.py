import pandas as pd
import ast
import sqlalchemy
import pyodbc
from movies_load_pkg01.resources.db_conn import db_conn
from movies_load_pkg01.resources.safe_literal_eval import safe_literal_eval

def T_production_countries():
    csv_file = './movies_load_pkg01/landing_zone/movies_metadata.csv'
    df = pd.read_csv(csv_file, encoding='utf-8')

    df['production_countries'] = df['production_countries'].fillna('[]')

    df['production_countries'] = df['production_countries'].apply(safe_literal_eval)

    df = df[df['production_countries'].apply(lambda x: isinstance(x, list) and len(x) > 0)]

    df_exploded = df.explode('production_countries')

    df_final = pd.DataFrame({
        'movieId': df_exploded['id'],
        'production_country': df_exploded['production_countries'].apply(lambda x: x['name'] if pd.notna(x) and isinstance(x, dict) else None)
    })

    df_final.dropna(subset=['production_country'], inplace=True)

    #print(df_final)
    #output_csv_file = 'production_countries.csv'  
    #df_final.to_csv(output_csv_file, index=False, encoding='utf-8')

    df_final.to_sql('temp_production_countries', con=db_conn(), if_exists='append', index=False)