import requests
import csv
import copy
from ETL_extract import Extract


class Transformation:

    def __init__(self, data_source, dataset):
        
        '''
        creating a extractobj to utilize class methods available in
        ETL_extract.py
        Additionally defining the filename to be used to store the transformed data in CSV format
        name_dict is used to rename the transformed data columns to the desired column names before
        writing the transformed data to CSV file
        '''
        
        extractObj = Extract()
        
        if data_source == 'api':
            self.data = extractObj.get_rest_api_data(dataset)
            
            self.filename = "UserData.csv"
            
            self.name_dict = {
               "gender": "gender", "name_title": "title", "name_first": "first_name", "name_last": "last_name", "location_street_number": "street_number", "location_street_name" : "street_name",
               "location_city" : "city", "location_state" : "state", "location_country" : "country",  "location_postcode" : "postcode", "location_coordinates_latitude" : "latitude",
               "location_coordinates_longitude" : "longitude", "location_timezone_offset": "timezone_offset", "location_timezone_description": "timezone_description", "email": "email",
               "login_uuid" : "login_uuid", "login_username":"username","login_password" : "password", "login_salt": "login_salt", "login_md5" : "login_md5", "login_sha1" :"login_sha1",
               "login_sha256" : "login_sha256", "dob_date": "dob", "dob_age" : "age", "registered_date" : "registered_date", "registered_age" : "registered_age", "phone" : "phone",
               "cell" : "cell", "id_name" : "id_name", "id_value" : "id_value", "picture_large" : "picture_large" , "picture_medium" : "picture_medium",
               "picture_thumbnail" : "picture_thumbnail", "nat" : "nationality"}
            
            funcName = data_source+'_'+dataset
            
            # getattr function accepts method name of a class and calls it
            getattr(self, funcName)(self.data, self.filename, self.name_dict)
            
        else:
            
            print('Unkown Data Source!!! Please try again...')
            
    def flatten_json(self, pattern):
        
        '''
        This class method flattens the raw json data recursively until all of the nested fields are flattened 
        and returns the result to the api_random_user caller method.
        '''
        
        newPattern = {}
        
        if type(pattern) is list:
            pattern = pattern[0]
            
        if type(pattern) is not str:
            for key, value in pattern.items():
                if type(value) in (list, dict):
                    returnedData = self.flatten_json(value)
                    for i,j in returnedData.items():
                        if key == "results" or key == "info":
                            newPattern[i] = j
                        else:
                            newPattern[key + "_" + i] = j
                else:
                   newPattern[key] = value
                   
        return newPattern
    
    def convert_dict_val_type(self, json_data):

        '''
        method to convert dict values to Int/Float
        '''

        correctedDict = {}
        for key, value in json_data.items():
            
            try:
                value = int(value)
            except Exception as ex:
                pass
            try:
                value = float(value)
            except Exception as ex:
                pass
            
            correctedDict[key] = value

        return correctedDict

    def api_random_user(self, data, filename, name_dict):
        
        '''
        Transform and Load function
        This class method calls the flatten_json passing in the raw json data extracted
        from the randomuser.api call.
        Additionally the keys in the transformed(json field names) transformed
        dict are renamed before being written to a CSV file
        '''
        
        with open(filename, 'w', encoding='UTF-8', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            user_data = data['results']
            
            for data in user_data:
                result = self.flatten_json(data)
                result1 = copy.copy(result)
                for item in result1:
                    for k, v in name_dict.items():
                        if k == item:
                            result[v] = result.pop(item)
                result = self.convert_dict_val_type(result)
                if csvfile.tell() == 0:
                    header = result.keys()
                    csv_writer.writerow(header)
                csv_writer.writerow(result.values())































