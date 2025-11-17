from sqlalchemy import text
from typing import List
from movies_load_pkg01.resources.db_conn import db_conn
from dagster import get_dagster_logger

bridge_tables = [
    'bridge_genre', 'bridge_cast', 'bridge_crew', 'bridge_company',
    'bridge_country', 'bridge_language', 'bridge_keyword'
]

operations = [
    ('dim_genre', 'bridge_genre', True),
    ('dim_cast', 'bridge_cast', True),
    ('dim_crew', 'bridge_crew', True),
    ('dim_company', 'bridge_company', True),
    ('dim_country', 'bridge_country', True),
    ('dim_language', 'bridge_language', True),
    ('dim_keyword', 'bridge_keyword', True),
    ('dim_time', None, False),  # No identity column
    ('fact_movies', None, True) # Has identity column
]

def delete_ss_tables() -> None:
    logger = get_dagster_logger()
    logger.info("Starting delete star schema tables process...")
    engine = db_conn()
    try:
        with engine.connect() as conn:
            logger.info("Starting transaction...")
            trans = conn.begin()

            try:
                for table in bridge_tables:
                    stmt = text(f"ALTER TABLE [ONLAB].[dbo].[{table}] NOCHECK CONSTRAINT ALL")
                    conn.execute(stmt)
                logger.info("Disabled all constraints on bridge tables")

                for dim_table, bridge_table, has_identity in operations:
                    conn.execute(text(f"DELETE FROM [ONLAB].[dbo].[{dim_table}]"))
                    logger.info(f"Cleared {dim_table}")

                    if has_identity:
                        conn.execute(text(f"DBCC CHECKIDENT ('ONLAB.dbo.{dim_table}', RESEED, 0)"))
                        logger.info(f"Reseeded {dim_table}")

                    if bridge_table:
                        conn.execute(text(f"DELETE FROM [ONLAB].[dbo].[{bridge_table}]"))
                        logger.info(f"Cleared {bridge_table}")

                for table in bridge_tables:
                    stmt = text(f"ALTER TABLE [ONLAB].[dbo].[{table}] CHECK CONSTRAINT ALL")
                    conn.execute(stmt)
                logger.info("Re-enabled all constraints on bridge tables")

                trans.commit()
                logger.info("\nAll operations completed successfully!")

            except Exception as e:
                logger.critical(f"\nError occurred: {str(e)}")
                trans.rollback()
                logger.critical("Transaction rolled back")
                raise

    except Exception as e:
        logger.info(f"Database connection error: {str(e)}")

    finally:
        engine.dispose()