'''Module which extracts the data from the data lake and inserts it into the database.'''

import psycopg2
import json
import time
import os
import constant_queries as utils
from dotenv import load_dotenv

load_dotenv()

class Warehouse_Manager:
    def __init__(self):
        '''The current index will be used to make sure that only the recently extracted data will be inserted.'''
        self.curr_index = 0

    def extract_lake(self) -> list:
        '''Extracts the data from the lake, cleans it and writes it all in a list. This method returns a list of
        the ISS data.'''
        with open(f'{os.getenv('LAKE_NAME')}.json', 'r') as file:
            lake = json.load(file)
        finalized_data = []
        length = len(lake['iss']) - 1
        for ind in range(self.curr_index, length + 1):
            finalized_data.append({'lat' : lake['iss'][ind]['latitude'], 'long' : lake['iss'][ind]['longitude'], 'time_period' : lake['iss'][ind]['visibility'],
                                   'time' : lake['iss'][ind]['timestamp'], 'velocity' : lake['iss'][ind]['velocity'], 'footprint' : lake['iss'][ind]['footprint'],
                                   'used_units' : lake['iss'][ind]['units'], 'solar_lat' : lake['iss'][ind]['solar_lat'], 'solar_lon' : lake['iss'][ind]['solar_lon']})
        self.curr_index = length - 1

        return finalized_data

    def insert_data(self) -> None:
        '''Connects to the database and inserts the recently extractes data into it.
        Partitioning and other main queries are written in the: "constant_queries.py" module. '''
        conn = utils.warehouse_connect()
        cur = conn.cursor()
        cur.execute(f'CREATE TABLE IF NOT EXISTS iss_25544_warehouse({''.join([i + utils.warehouse_params[i] for i in utils.warehouse_params])})')
        data = self.extract_lake()
        
        for inst in data:
            with conn:
                cur.execute(f'INSERT INTO iss_25544_warehouse({utils.warehouse_columns})' + 'VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)',(inst['lat'], inst['long'], inst['time_period'].strip(''' ' '''), inst['time'], 
                                                                                                         inst['velocity'], inst['footprint'], inst['used_units'].strip(''' ' '''), inst['solar_lat'],
                                                                                                         inst['solar_lon']))
    
warehouse = Warehouse_Manager()


while True:
    time.sleep(120)
    warehouse.insert_data() 