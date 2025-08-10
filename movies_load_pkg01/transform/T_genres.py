import pandas as pd
from movies_load_pkg01.resources.db_conn import db_conn
from movies_load_pkg01.resources.safe_literal_eval import safe_literal_eval

def T_genres():
    csv_file = './movies_load_pkg01/landing_zone/movies_metadata.csv'  
    df = pd.read_csv(csv_file, encoding='utf-8') 

    df['genres'] = df['genres'].fillna('[]')
    df['genres'] = df['genres'].apply(safe_literal_eval)

    df = df[df['genres'].apply(lambda x: isinstance(x, list) and len(x) > 0)]

    df_exploded = df.explode('genres')

    df_final = pd.DataFrame({
        'movieId': df_exploded['id'],
        'genre': df_exploded['genres'].apply(lambda x: x['name'] if pd.notna(x) and isinstance(x, dict) else None)
    })

    df_final['movieId'] = pd.to_numeric(df_final['movieId'], errors='coerce') #there is a malformed entry in the dataset that pandas can't detect, so we force it to numeric
    df_final.dropna(subset=['movieId', 'genre'], inplace=True)
    df_final.dropna(subset=['genre'], inplace=True)

    df_final.to_sql('temp_genres', con=db_conn(), if_exists='append', index=False)