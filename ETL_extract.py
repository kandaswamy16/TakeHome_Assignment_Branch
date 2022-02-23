import requests
import json
import sys


class Extract:
    def __init__(self):
        
        '''
        load the configuration file to read in configuration information
        to start the raw data extraction process
        '''
        
        self.data_sources = json.load(open('datasource_config.json'))
        self.api = self.data_sources['data_sources']['api']
    
    
    def get_rest_api_data(self, api_name):
        
        '''
        Fetch Raw json data for Random Users and if the request was successful
        In case of failure to reach out the API, the script will catch and
        print the exception and the program execution will be terminated
        '''
        
        api_url = self.api[api_name]
        try:
            response = requests.get(api_url)
            return response.json()
        except Exception as e:
            print(e)
            sys.exit(1)
            




























