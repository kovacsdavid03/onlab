import pandas as pd
import ast
from movies_load_pkg01.resources.db_conn import db_conn


def T_keywords():
    df = pd.read_csv('./movies_load_pkg01/landing_zone/keywords.csv')
    df['keywords'] = df['keywords'].apply(ast.literal_eval)

    df_exploded = df.explode('keywords')

    df_final = pd.DataFrame({
        'movieID': df_exploded['id'],
        'keyword': df_exploded['keywords'].apply(lambda x: x['name'] if pd.notna(x) else None)
    })

    df_final = df_final[df_final['keyword'] != '']

    df_final.dropna(subset=['keyword'], inplace=True)

    df_final.to_sql('temp_keywords', con=db_conn(), if_exists='append', index=False)