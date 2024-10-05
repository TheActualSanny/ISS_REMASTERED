'''Module which writes the ISS data into the data lake (In this case a JSON file).'''

import requests
import time
import json
import os
from dotenv import load_dotenv
from configparser import ConfigParser

load_dotenv()

class JSON_Manager:
    def __init__(self):
        '''Sets the user parameters.'''
        self._user_params = {'User-Agent' : os.getenv('UserAgentg')} 
    
    def fetch_json(self) -> dict:
        '''Method which send a request to the API and gets the ISS data.'''
        raw_json = requests.get(os.getenv('URL'), params = self._user_params)
        return raw_json.content
    

    def update_json(self) -> None:
        '''Module which writes data into the JSON file (It first opens the data inside the file,
        adds the new dictionary onto it and inserts it again).'''

        new_data = json.loads(self.fetch_json())
        with open(f'{os.getenv('LAKE_NAME')}.json', 'r') as lake:
            existing_data = json.load(lake)
        
        existing_data['iss'].append(new_data)
        with open(f'{os.getenv('LAKE_NAME')}.json', 'w') as lake:
            json.dump(existing_data, lake, indent = 4)

json_manager = JSON_Manager()


while True:
    json_manager.update_json()
    time.sleep(2)