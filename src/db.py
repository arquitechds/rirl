import pymysql
import os 
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

# Canonic DB functions

def get_retail_db_connection():
    ''' 
    Connect to any mysql db
    '''

    retrieve_db = pymysql.connect(
        host=os.environ["DB_HOST"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        db=os.environ["DB_NAME"]
    )
    logger.info('Succesful connection')
    return retrieve_db


def insert_data(data,table_name):
    ''' 
    Insert data to any mysql table
        inputs 
            data:(list of dicts) with data to insert
            table_name(str) table name
    '''
    writer = get_retail_db_connection()
    writer.begin()
    try:
        with writer.cursor() as insert_cursor:
            sqlcmd = (
            f"REPLACE INTO {table_name} ({', '.join(data[0].keys())}) "
            f"VALUES (%({')s, %('.join(data[0].keys())})s)")
            insert_cursor.executemany(sqlcmd, data)
            inserted = insert_cursor.rowcount
    except:
        writer.rollback()
        logger.info('\aScores table insert failed. Transaction rolled back')
        raise
    writer.commit()
    logger.info('Inserted', inserted, 'row(s) into Scores table')
    writer.close()
