"""
Incremental Load Configuration

Configuration settings for the incremental loading pipeline
"""

# Tables that support incremental loading
INCREMENTAL_TABLES = {
    'fact_movies': {
        'key_column': 'movieId',
        'temp_table': 'temp_movies',
        'staging_table': 'incremental_movies_staging'
    },
    'dim_cast': {
        'key_columns': ['name', 'gender'],
        'temp_table': 'temp_cast', 
        'staging_table': 'incremental_cast_staging'
    },
    'dim_crew': {
        'key_columns': ['name', 'gender'],
        'temp_table': 'temp_crew',
        'staging_table': 'incremental_crew_staging'
    },
    'dim_genre': {
        'key_columns': ['genre'],
        'temp_table': 'temp_genres',
        'staging_table': 'incremental_genres_staging'
    }
}

# Data quality rules
DATA_QUALITY_RULES = {
    'fact_movies': {
        'required_columns': ['movieId', 'original_title'],
        'non_null_columns': ['movieId'],
        'unique_columns': ['movieId']
    },
    'dim_cast': {
        'required_columns': ['name', 'gender'],
        'non_null_columns': ['name'],
        'unique_columns': []  # Combinations can be unique but individual values can repeat
    }
}

# Batch processing settings
BATCH_SIZE = 1000  # Process records in batches
MAX_RETRY_ATTEMPTS = 3
STAGING_TABLE_PREFIX = 'incremental_'