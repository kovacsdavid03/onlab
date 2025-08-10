import pandas as pd
from movies_load_pkg01.resources.db_conn import db_conn
from movies_load_pkg01.resources.safe_literal_eval import safe_literal_eval

def T_production_companies():
    csv_file = './movies_load_pkg01/landing_zone/movies_metadata.csv'
    df = pd.read_csv(csv_file, encoding='utf-8')

    df['production_companies'] = df['production_companies'].fillna('[]')

    df['production_companies'] = df['production_companies'].apply(safe_literal_eval)

    df = df[df['production_companies'].apply(lambda x: isinstance(x, list) and len(x) > 0)]

    df_exploded = df.explode('production_companies')

    df_final = pd.DataFrame({
        'movieid': df_exploded['id'],
        'production_company': df_exploded['production_companies'].apply(lambda x: x['name'] if pd.notna(x) and isinstance(x, dict) else None)
    })

    df_final.dropna(subset=['production_company'], inplace=True)

    df_final.to_sql('temp_production_companies', con=db_conn(), if_exists='append', index=False)