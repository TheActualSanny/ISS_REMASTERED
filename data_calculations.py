'''Module which the distance travelled is calculated and the iss_normalized table is created and managed.'''

import psycopg2
import pandas
import time
import constant_queries as utils
from reverse_geocoding import reverse_geocode
from logging_config import logger
from configparser import ConfigParser

reader = ConfigParser()
reader.read('config.ini')

class Data_Calculations:
    def __init__(self):
        self.conn = utils.warehouse_connect()
        self.cur = self.conn.cursor()

    def update_dataframe(self):
        '''Module which just updates the dataframe (It is used in calculate_data).'''
        self.cur.execute(utils.select_query)
        self.df = pandas.DataFrame(data = self.cur.fetchall(), columns = utils.columns)

    def create_table(self) -> None:
        '''Method which creates the partitioned iss_normalized table.'''
        with self.conn:
            self.cur.execute(f'CREATE TABLE IF NOT EXISTS iss_normalized({''.join([i + utils.normalized_params[i] for i in utils.normalized_params])}) PARTITION BY LIST (time_period)')
            self.cur.execute('''CREATE TABLE IF NOT EXISTS normalized_day PARTITION OF iss_normalized FOR VALUES IN ('daylight')''')
            self.cur.execute('''CREATE TABLE IF NOT EXISTS normalized_eclipsed PARTITION OF iss_normalized FOR VALUES IN ('eclipsed')''')

    def avrg_distance(self) -> float:
        '''Method that calculates the average distance traveled by the ISS.'''
        approx_time = self.df.iloc[-1]['time'] - 300
        closest_row = self.df.iloc[(self.df['time'] - approx_time).abs().argsort()[0]] # This is the closest row the timestamp of which was captured approx 5 minutes ago.
        avrg_velo = round((self.df.iloc[-1]['velocity'] + closest_row['velocity']) / 2, 4)

        return round(float((self.df.iloc[-1]['time'] - closest_row['time']) * avrg_velo / 3600), 2)
        
    
    def get_location(self) -> str:
        '''Simply calls the reverse_geocode function and passes the current coordinates.'''
        return reverse_geocode(self.df.iloc[-1]['latitude'], self.df.iloc[-1]['longitude'])
    
    def calculate_data(self) -> None:
        '''Method which combines all other methods. Creates the partitioned table, gets the average distance and the location and
        inserts it all into the table.'''

        self.create_table()
        self.update_dataframe()
        self.cur.execute('SELECT * FROM iss_25544_warehouse ORDER BY pos DESC')
        distance = self.avrg_distance()
        loc = self.get_location()
        last_row = list(self.cur.fetchone())
        last_row.append(distance)
        last_row.append(loc)
        with self.conn:
            self.cur.execute(f'INSERT INTO iss_normalized({utils.normalized_columns})' + 'VALUES(%s, %s, %s, %s , %s, %s, %s, %s, %s, %s, %s)', last_row[1::])
        return (distance, loc)
        

calc = Data_Calculations()


while True:
    time.sleep(300)
    res = calc.calculate_data()
    logger.info(f'In the last ~5 minutes the ISS has travelled {res[0]} kilometers and is currently at: {res[1]}')