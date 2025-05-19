# Code Challenge 
Response code

in src/:

- db_util.py: A collection of utility functions for accessing the local PostgreSQL database 'wxdata' 
- wxdata_ingest.py: code for ingesting GHCN data in wx_data subdirectory and uploading it to the 'station_data' data table in the 'wxdata' database
- wxstats_ingest.py: code for calculating statistics from the data in the station_data data table and uploading to the 'weather_stats' data table

in ./:
- api.py: code for a simple REST API using Flask to serve weather data from 'station_data' and 'weather_stats' (running locally)

Written discussion in answers/: 
- discussion.pdf

Log Files:

- wxingest.log: simple log file for wxingest
- wxstats.log: simple log file for wxstats

Input Data:

- wx_data: given weather data (GHCN data at US locations from 1985-2014)
- yld_data: US Corn yield data (yearly).  Not used in this challenge
