import pandas as pd
import ast
import sqlalchemy
import pyodbc
from movies_load_pkg01.resources.db_conn import db_conn

def T_cast():
    csv_file = './movies_load_pkg01/landing_zone/credits.csv' 
    df = pd.read_csv(csv_file)

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

    df_final['character'] = df_final['character'].str.split(' / ')
    df_final = df_final.explode('character')

    df_final.dropna(subset=['character', 'gender', 'name'], inplace=True)
    #print(df_final)
    #output_csv_file = 'cast.csv'  
    #df_final.to_csv(output_csv_file, index=False, encoding='utf-8')
    df_final.to_sql('temp_cast', con=db_conn(), if_exists='append', index=False)