'''Module which contains the config for the logger'''
import logging

# Inittializing the logger object itself
logger = logging.Logger('transforming')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Adding the handlers
file = logging.FileHandler('transforming_logs.logs')
stream = logging.StreamHandler()
file.setFormatter(formatter)
stream.setFormatter(formatter)
logger.addHandler(file)
logger.addHandler(stream)