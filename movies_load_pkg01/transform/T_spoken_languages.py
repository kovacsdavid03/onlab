import pandas as pd
import ast
import sqlalchemy
import pyodbc
from movies_load_pkg01.resources.db_conn import db_conn
from movies_load_pkg01.resources.safe_literal_eval import safe_literal_eval

def T_spoken_languages():
    csv_file = './movies_load_pkg01/landing_zone/movies_metadata.csv' 
    df = pd.read_csv(csv_file, encoding='utf-8')  

    df['spoken_languages'] = df['spoken_languages'].fillna('[]')

    df['spoken_languages'] = df['spoken_languages'].apply(safe_literal_eval)

    df = df[df['spoken_languages'].apply(lambda x: isinstance(x, list) and len(x) > 0)]

    df_exploded = df.explode('spoken_languages')

    df_final = pd.DataFrame({
        'movieid': df_exploded['id'],
        'language': df_exploded['spoken_languages'].apply(lambda x: x['name'] if pd.notna(x) and isinstance(x, dict) else None)
    })

    df_final = df_final[df_final['language'] != '']

    df_final.dropna(subset=['language'], inplace=True)

    print(df_final)

    #output_csv_file = 'spoken_languages.csv'  
    #df_final.to_csv(output_csv_file, index=False, encoding='utf-8')
    df_final.to_sql('temp_spoken_languages', con=db_conn(), if_exists='append', index=False)