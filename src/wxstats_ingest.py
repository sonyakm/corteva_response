#!/usr/bin/env python
#PostgreSQL 
import psycopg2
from psycopg2 import OperationalError, errorcodes
from db_util import connect_to_db, create_table, init_table, execute_insert_db, execute_select_db #local library

#general use libraries
import numpy as np

#logging
import logging

maindir = '../'

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format = '%(asctime)s - %(message)s', filename=maindir + 'wxstats.log')


# Database connection parameters. 
dbname = "wxdata"
dbuser = "postgres"
dbpassword = "test"
dbhost = "localhost"  
dbport = "5432"  

def init_stats_table(logger):
    """
    Connect to the wxdata database and create the station_data table.
    """

    create_table_sql = """
        CREATE TABLE IF NOT EXISTS weather_stats (
            station_id VARCHAR(20) NOT NULL,
            year INT NOT NULL,
            max_temperature_avg DECIMAL(7, 2),
            min_temperature_avg DECIMAL(7, 2),
            precipitation_accum DECIMAL(10, 2),
            number_obs_maxtemp INT NOT NULL,
            number_obs_precip INT NOT NULL,
            PRIMARY KEY (station_id, year)
        );
    """

    init_table(create_table_sql, logger)
    
    return 


def get_stations(logger):
    """
    Retrieve list of stations
    Input:
        logger: logging object
    Output:
        stations: list of stations
    """
    conn = connect_to_db(logger)
    if conn is not None:
        sql = """
            SELECT DISTINCT station_id FROM station_data;
            """
        stations = [row[0] for row in execute_select_db(conn, logger, sql)]
        conn.close()
        return stations

    return

def get_min_max_year(station, logger):
    """
    Get min and max year in db
    Input:
        station: station id
        logger: logging object
    Output:
        minyear, maxyear: min and max year for station
    """
    conn = connect_to_db(logger)
    if conn is not None:
        sql = """
            SELECT MIN(date_part('year',date)), MAX(date_part('year',date)) FROM station_data 
            WHERE station_id = '{}';
            """
        res = execute_select_db(conn, logger, sql.format(station))
        conn.close()
        return res[0][0],res[0][1]

    return
    

def get_stats(station, year, logger):
    """
    Get average max T, average min T, accumulated precip, and number of obs for station and year
    Input:
        station: station id for db
        year: year to calculate statistics
        logger: logging object
    Output:
        maxt_avg, mint_avg, precip_sum, nobs_temp, nobs_precip
    """
    conn = connect_to_db(logger)
    if conn is not None:
        sql = """
            SELECT AVG(max_temperature), AVG(min_temperature), SUM(precipitation), 
                   count(max_temperature),count(precipitation)
            FROM station_data 
            WHERE station_id = '{}' and date_part('year',date) = {};
            """
        res = execute_select_db(conn, logger, sql.format(station, year))
        conn.close()
        return res[0]

    return
    

def upsert_stats_data(conn, data, logger):
    """
    Inserts data into the weather_stats table or updates the record
    if station_id and date already exist.

    Input:
        conn: The connection object to the db
        data: a tuple containing:
            station_id: station id (key)
            year: year to be added (key)
            avgmaxt: maximum temperature (C)
            avgmint: minimum temperature (C)
            accprecip: accumulated precipitation (mm)
            nobs_temperature: number of temperature obs included
            nobs_precip: number of precip obs included

    """
    
    sql = """
    INSERT INTO weather_stats 
            (station_id, 
            year, 
            max_temperature_avg,
            min_temperature_avg,
            precipitation_accum,
            number_obs_maxtemp,
            number_obs_precip)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (station_id, year) DO UPDATE
    SET max_temperature_avg = EXCLUDED.max_temperature_avg,
        min_temperature_avg = EXCLUDED.min_temperature_avg,
        precipitation_accum = EXCLUDED.precipitation_accum,
        number_obs_maxtemp = EXCLUDED.number_obs_maxtemp,
        number_obs_precip = EXCLUDED.number_obs_precip;
    """
    execute_insert_db(conn, logger, sql, data=data)

    return


if (__name__ == "__main__"):

    #create stats table if it does not exist
    init_stats_table(logger)
    logger.info('Started stats')
    
    """
    for each station:
    - retrieve min/max year
    - for each year:
        - calculate avg maxt, mint, sum precip, nobs_temp, nobs_precip
        - upsert to weather stats db
    """
    for stn in get_stations(logger):
        miny, maxy = get_min_max_year(stn, logger)
        if miny is not None:
            for year in range(int(miny), int(maxy+1)):
                avgmaxt, avgmint, psum, nobst, nobsp = get_stats(stn, year, logger)
                if psum is not None:
                    psum = float(psum)/10. #convert to cm
                conn = connect_to_db(logger)
                if conn is not None:
                    upsert_stats_data(conn,(stn, year, avgmaxt, avgmint, psum, nobst, nobsp), logger)
                conn.close()
                
    logger.info('Ended stats')




