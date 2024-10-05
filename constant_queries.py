'''Contains the main functions which are used multiple times throughout the program.'''

import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def warehouse_connect() -> psycopg2.connect:
    return psycopg2.connect(
        host = os.getenv('HOST'),
        user = os.getenv('USER'),
        password = os.getenv('PASS'),
        database = os.getenv('DB_NAME')
    )


warehouse_params = {
    'pos' : ' SERIAL PRIMARY KEY, ',
    'latitude ' : 'REAL, ',
    'longitude ' : 'REAL, ',
    'time_period ' : 'TEXT, ',
    'time ' : 'INTEGER, ',
    'velocity ' : 'REAL, ',
    'footprint ' : 'INTEGER, ',
    'units ' : 'TEXT, ',
    'solar_latitude ' : 'REAL, ',
    'solar_longitude ' : 'REAL'
}

normalized_params = {
    'pos' : ' SERIAL, ',
    'latitude ' : 'REAL, ',
    'longitude ' : 'REAL, ',
    'time_period ' : 'TEXT, ',
    'time ' : 'INTEGER, ',
    'velocity ' : 'REAL, ',
    'footprint ' : 'INTEGER, ',
    'units ' : 'TEXT, ',
    'solar_latitude ' : 'REAL, ',
    'solar_longitude ' : 'REAL, ',
    'distance_travelled ' : 'REAL, ',
    'location ' : 'TEXT'
}

warehouse_columns = ''.join([f'{i.rstrip()}, ' for i in warehouse_params])[5:-2]

normalized_columns = ''.join([f'{i.rstrip()}, ' for i in normalized_params])[5:-2]

geocoding_params = {'User-Agent' : os.getenv('UserAgent'), 'api_key' : os.getenv('API_KEY')}

select_query = 'SELECT * FROM iss_25544_warehouse'

columns = ['pos', 'latitude', 'longitude', 'time_period', 'time', 'velocity', 'footprint', 'units', 'solar_latitude', 'solar_longitude']