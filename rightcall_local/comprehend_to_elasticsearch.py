#! /user/bin/python
""" Ensures all objects in the comprehend.rightcall s3 bucket
    are added, along with their metadata to the local elasticsearch index.

    Metadata is stored in local dynamodb database.

    Flow:
        If ref from s3 object exists in elasticsearch index with all its meta data:
            Do Nothing
        If exists without metadata add ref to list csv file of refs for which metadata
            is needed.
        If doesn't exist in index, download it and try to get metadata
"""

import dynamodb_tools
import elasticsearch_tools
import s3 as s3py
import pandas as pd
import boto3
import json
import logging
# import datetime



class Comp2Elas:
    def __init__(self, region, db_endpoint, bucket, directory, es_endpoint, loglevel='INFO'):
        self.region = region
        self.db_endpoint = db_endpoint
        self.bucket = bucket
        self.directory = directory
        self.es_endpoint = es_endpoint
        self.LOGLEVEL = loglevel

        self.setup()
        

    def setup(self):
        # Create the following directories if they don't already exist
        self.csv_dir = self.directory + 'data/csvs/'
        self.mp3_dir = self.directory + 'data/mp3s'


        self.dynamodb = boto3.resource('dynamodb',
                                  region_name=self.region,
                                  endpoint_url=self.db_endpoint)

        # Find the name of the table(s) that exist at this endpoint
        self.TABLE_NAME = 'Rightcall'
        self.table = self.dynamodb.Table(self.TABLE_NAME)
        
        self.INDEX_NAME = 'rightcall'
        self.TYPE_NAME = '_doc'

        
        self.s3 = boto3.client('s3')
        # Get host and port from endpoint string
        self.es_host = self.es_endpoint.split(':')[1].replace('/', '')
        self.es_port = int(self.es_endpoint.split(':')[2])
        self.es = elasticsearch_tools.Elasticsearch([{'host': self.es_host,
                             'port': self.es_port}])

        # Logging
        levels=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if self.LOGLEVEL not in levels:
            raise ValueError(f"Invalid log level choice {LOGLEVEL}")
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(self.LOGLEVEL)
        # create console handler and set level to LOGLEVEL
        ch = logging.StreamHandler()
        ch.setLevel(self.LOGLEVEL)
        # create file handler and set level to DEBUG
        fh = logging.FileHandler('rightcall_local.log')
        fh.setLevel(logging.DEBUG)
        # create formatter
        formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
        # add formatter to ch
        ch.setFormatter(formatter)
        fh.setFormatter(formatter)
        # add ch to logger
        self.logger.addHandler(ch)
        self.logger.addHandler(fh)


    def update_existing_items(self, source=None):
        if source is None:
            source = self.bucket
        else:
            source = str(source)
        refs = self.get_all_refs_from_s3_objects(source)
        get_meta_data = []
        # Forcing the function to update all documents in index with values in objects in bucket
        for i, call_record in enumerate(refs):
            s3_item = None
            ref = call_record['Name']
            s3_item = s3py.get_first_matching_item(ref, source)
            s3_item = elasticsearch_tools.rename(s3_item)
            try:
                result = elasticsearch_tools.update_document(self.es, self.INDEX_NAME, s3_item['referenceNumber'], s3_item)
                self.logger.debug(f"Result: {result}")
            except elasticsearch.exceptions.NotFoundError as err:
                self.logger.error(str(err))
        return


    def get_all_refs_from_s3_objects(self, bucket_name):
        """Given an s3 bucket name, returns a list of the reference numbers
        contained in the names of all objects in that bucket
        
        Input: <string> 'comprehend.rightcall'
        Output: <list> ['b310f08130r3', 'c210935j22239', ...]
        """
        self.logger.info(f"Getting objects from {bucket_name}")
        keys = self.s3.list_objects_v2(Bucket=bucket_name)
        self.logger.debug(f"Received {len(keys['Contents'])} objects from {bucket_name}")
        list_of_reference_numbers = []
        for key in keys['Contents']:
            ref = get_reference_number_from_object_name(key['Key'])
            list_of_reference_numbers.append({'Name': ref})
        return list_of_reference_numbers


    def get_reference_number_from_object_name(self, object_name_string):
        """ Given s3 object name: 'e23413582523--QUIDP.json' or 'e23413582523P.json':
                return just 'e23413582523'
        """
        self.logger.debug(f"Received: {object_name_string}")
        if '--' in object_name_string:
            reference_number = object_name_string.split('--')[0]
        elif '.json' in object_name_string:
            reference_number =  object_name_string.split('.')[0]
        else:
            reference_number = object_name_string
        self.logger.debug(f"Ref Num: {reference_number}")
        if '--' in reference_number or '.json' in reference_number:
            raise ValueError(f"Invalid characters detected in reference number: {object_name_string}")
        return reference_number


    def add_new_or_incomplete_items(self, source=None):
        """Ensures elasticsearch index has all the records that exist in comprehend.rightcall bucket
            and that they are fully populated with as much information as possible. 
        Pulls objects down from comprehend.rightcall bucket.
            For each object:
                Checks if it exists in elasticsearch already.
                Checks if it has all the required fields populated with data.
                If so - moves on to next item
                If not - Checks if that missing data can be found in dynamodb
                    if so - grabs it from dynamodb, combines it with s3 obeject data
                        and uploads to elasticsearch index
                    if not - adds the filename (refNumber) to csv file to be returned."""
        print(source)
        if source is None:
            source = self.bucket
        else:
            source = str(source)
        refs = self.get_all_refs_from_s3_objects(source)
        get_meta_data = []
                
        # For each reference number:
        for i, call_record in enumerate(refs):
            s3_item = None
            db_item = None
            self.logger.debug('---------------------------------------')
            self.logger.debug(f"Working on {i} : {call_record['Name']}")
            
            ref = call_record['Name']
            
            if elasticsearch_tools.exists(self.es, self.INDEX_NAME, ref):
                self.logger.debug(f"{ref} already in {self.INDEX_NAME} index")               

            else:
                self.logger.debug(f"{ref} not in {self.INDEX_NAME} index")
                
                self.logger.debug(f"Checking {source} bucket for {call_record['Name']}")
                s3_item = s3py.get_first_matching_item(ref, source)
                
                self.logger.debug(f"Preparing data")
                s3_item = elasticsearch_tools.rename(s3_item)


            if elasticsearch_tools.fully_populated_in_elasticsearch(ref, self.es, self.INDEX_NAME):
                self.logger.debug(f"{ref} fully populated in {self.INDEX_NAME}")
                continue
            else:
                logger.debug(f"{ref} missing metadata")
                       
            logger.debug(f"Checking {self.table} database for missing metadata")
            db_item = dynamodb_tools.get_db_item(ref, self.table)
            if not db_item:
                logger.debug(f"Adding {ref} to 'get_meta_data'")
                get_meta_data.append(ref)
                continue
            else:
                logger.debug(f"Data present in {self.table} database: {db_item}")
                  
            # Upload to elasticsearch
            if s3_item is None:
                logger.debug(f"Ensuring object is downloaded from {source}")
                s3_item = s3py.get_first_matching_item(ref, source)
                # Prepare data for ES
                logger.debug(f"cleaning data")
                s3_item = elasticsearch_tools.rename(s3_item)
                
            self.logger.debug(f"Combining data for {ref} from {self.table} and {source} and adding to {self.INDEX_NAME} index")

            result = elasticsearch_tools.load_call_record(
                    db_item,
                    s3_item,
                    self.es,
                    self.INDEX_NAME)
            if result:
                self.logger.debug(f"{ref} successfully added to {self.INDEX_NAME} index")
            else:
                self.logger.error(f"Couldn't upload to elasticsearch: {result}")

        self.logger.debug(f"Refs without metadata {get_meta_data}")
        return get_meta_data      


def parse_csv(path_to_file):
    file = pd.read_csv(path_to_file, sep=';')
    json_file = file.to_json(orient='records')
    data = json.loads(json_file)
    return data

def write_to_csv(ref_list, path):
    logger.debug(ref_list)
    df = pd.DataFrame.from_dict({'col': ref_list})
    logger.debug(f"Writing {df} records back to csv")
    try:
        df.to_csv(path, sep=';', header=False,index=False)
    except PermissionError:
        logger.info(df)


if __name__ == '__main__':
    c2es = Comp2Elas('eu-west-1', 'http://localhost:8000', 'comprehend.rightcall', 'C:/Users/RSTAUNTO/Desktop/Python/projects/rightcall_robin/', 'http://localhost:9200')
##    json_data = parse_csv(CSV)
##    get_meta_data = add_new_or_incomplete_items('comprehend.rightcall')
    
##    if get_meta_data is not None:
##        write_to_csv(get_meta_data, RC_DIR + '/data/csvs/'+ 'to_download.csv')
