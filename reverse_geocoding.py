'''Module which calls the reverse geocoding API and gets the location of the ISS'''
import requests
import json
import os
import constant_queries as utils
from logging_config import logger
from dotenv import load_dotenv

load_dotenv()

def check_data(data):
    '''Checks whether or not the response is an error.'''
    return data['response']['features'][0]['properties']['label']

def reverse_geocode(lat, lon):
    '''Does the geocoding by passing the lat and lon as parameters.'''
    utils.geocoding_params['lat'] = lat
    utils.geocoding_params['lng'] = lon
    return check_data(json.loads(requests.get(os.getenv('URL_GEO'), params = utils.geocoding_params).content))
