from ETL_transform import Transformation
import json


class Engine:
    
    '''
    This is the entry point of our ETL module. This class reads in the "datasource_config.json" file to get the data_source, dataset inputs
    Then it steps through the Transformation class in the ETL_transform.py file which in turn calls the extract and then the extracted
    data is transformed and loaded/written to a CSV file 
    '''
    
    def __init__(self, data_source, dataset):
        trans_obj = Transformation(data_source, dataset)
        
        
if __name__ == '__main__':
    
    etl_data = json.load(open('datasource_config.json'))
    for data_source, dataset in etl_data['data_sources'].items():
        print(data_source)
        for data in dataset:
            print(data)
            main_obj = Engine(data_source, data)
