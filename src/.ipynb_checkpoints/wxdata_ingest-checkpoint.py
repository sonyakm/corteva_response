#!/usr/bin/env python
#database libraries
import psycopg2
from psycopg2 import OperationalError, errorcodes
from db_util import connect_to_db, create_table, init_table, execute_insert_db #local library

#general use libraries
import pandas as pd
import glob
import numpy as np

#logging
import logging

#maindir
maindir = '../'

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format = '%(asctime)s - %(message)s', filename=maindir+'wxingest.log')

# Database connection parameters. 
dbname = "wxdata"
dbuser = "postgres"
dbpassword = "test"
dbhost = "localhost"  
dbport = "5432"  


def init_station_table(logger):
    """
    Connect to the wxdata database and create the station_data table.
    """

    create_table_sql = """
        CREATE TABLE IF NOT EXISTS station_data (
            station_id VARCHAR(20) NOT NULL,
            date DATE NOT NULL,
            max_temperature DECIMAL(7, 2),
            min_temperature DECIMAL(7, 2),
            precipitation DECIMAL(7, 2),
            PRIMARY KEY (station_id, date)
        );
    """

    return init_table(create_table_sql, logger)


def upsert_station_data(conn, data, logger):
    """
    Inserts data into the station_data table or updates the record
    if station_id and date already exist.

    Input:
        conn: The connection object to the db
        data: a tuple containing:
            station_id: station id (key)
            date: date to be added (key)
            maxt: maximum temperature (C)
            mint: minimum temperature (C)
            precip: accumulated precipitation (mm)

    """
    
    sql = """
    INSERT INTO station_data (station_id, date, max_temperature, min_temperature, precipitation)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (station_id, date) DO UPDATE
    SET max_temperature = EXCLUDED.max_temperature,
        min_temperature = EXCLUDED.min_temperature,
        precipitation = EXCLUDED.precipitation;
    """
    execute_insert_db(conn, logger, sql, data=data)
    
    return

def wxconv(x):
    #convert raw GHCN from tenths of a unit to actual values
    return int(x)/10.

if (__name__ == "__main__"):

    #create table if not already created
    mytable = init_station_table(logger)
    
    
    #process weather data
    logger.info('Started ')
    
    ningest = 0
    wxfiles = glob.glob(maindir+'wx_data/*txt') #get list of files
    for file in wxfiles:
        #read GHCN station data from file, set column names, set date column to date, convert data to actual values
        df = pd.read_csv(file,sep='\t', header=None, parse_dates=[0], 
                         converters={1:wxconv, 2:wxconv, 3:wxconv},names=['Date','MaxTemp','MinTemp','Precip'])

        # deal with missing data
        df = df.astype({'MaxTemp': 'float','MinTemp':'float', 'Precip':'float'})
        df.replace(-999.9, None, inplace=True) #set missing to None
        df = df.dropna(subset=['MaxTemp','MinTemp','Precip']) #drop rows where all data are missing 

        #get station ID from the file name
        station = file.lstrip(maindir+'/wx_data/').rstrip('.txt') #get station ID

        #TBD: check data for valid ranges, unphysical values (min > max, etc)
        
        #add data to database
        conn = connect_to_db(logger)
        if conn is not None:
            for idx,row in df.iterrows():
                #only put rows where data exists in the database
                if ((row['MaxTemp'] is not None) and (row['MinTemp'] is not None) and (row['Precip'] is not None)):
                    data = (station, row['Date'],row['MaxTemp'],row['MinTemp'],row['Precip'])
                    upsert_station_data(conn, data, logger)
                    ningest += 1
    
    
    message = f'Successfully ingested {ningest} rows'
    logger.info(message)
    logger.info('Ended')




