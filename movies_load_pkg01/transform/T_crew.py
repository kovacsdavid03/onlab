import pandas as pd
import ast
from movies_load_pkg01.resources.db_conn import db_conn

def T_crew():
    csv_file = './movies_load_pkg01/landing_zone/credits.csv'  
    df = pd.read_csv(csv_file)

    df['crew'] = df['crew'].apply(ast.literal_eval)

    df = df[df['crew'].apply(lambda x: len(x) > 0)]

    df_exploded = df.explode('crew')

    df_final = pd.DataFrame({
        'movieid': df_exploded['id'],
        'department': df_exploded['crew'].apply(lambda x: x['department'] if pd.notna(x) else None),
        'gender': df_exploded['crew'].apply(lambda x: x['gender'] if pd.notna(x) else None),
        'job': df_exploded['crew'].apply(lambda x: x['job'] if pd.notna(x) else None),
        'name': df_exploded['crew'].apply(lambda x: x['name'] if pd.notna(x) else None)
    })

    df_final.dropna(subset=['department', 'gender', 'job', 'name'], inplace=True)

    df_final.to_sql('temp_crew', con=db_conn(), if_exists='append', index=False)