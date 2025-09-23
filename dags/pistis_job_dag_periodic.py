# Copyright 2024 Eviden Spain S.A
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import pendulum
import requests
import pathlib 
import logging
from airflow import Dataset
from airflow.decorators import dag, task
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.models.dagrun import DagRun
from airflow.operators.empty import EmptyOperator
from jsoninja import Jsoninja
from urllib.parse import urlparse
from datetime import datetime
from airflow.models import Variable
from base64 import decodebytes
from io import BytesIO
from base64 import b64encode
from minio import Minio
from fastparquet import ParquetFile
import os.path
import pandas as pd
import re
import csv
import chardet

now = pendulum.now()

@dag(start_date=datetime(2023,1,1), schedule="@once", catchup=False, params={
        # job data        
        "job_data": Param(
            {
                "prev_run": "000",
                "root_dag_run": "000",
                "job_name": "test_job",
                "source": "http://dataset.pistis",
                "content-type": "application/json",
                "input_data": [],
                "endpoint": "http://",
                "method": "get",
                "destination_type": "memory",
                "lineage_tracking": True,
                "uuid": "0",
                "data_uuid": "0",
                "access_token": ""
            },
            schema =  {
                "type": "object",
                "properties": {
                    "prev_run": {
                        "type": "string"
                    },
                    "root_dag_run": {
                        "type": "string"
                    }, 
                    "wf_results_id": {
                        "type": "string"
                    },            
                    "job_name": {
                        "type": "string"
                    },
                    "source": {
                        "type": "string"
                    },
                    "metadata": {
                        "type": "object"
                    },
                    "content-type": {
                        "type": "string"
                    },
                    "endpoint": {
                        "type": "string",
                        "format": "uri",
                        "pattern": "^(https?|wss?|ftp)://"
                    },
                    "input_data": {
                        "type": "array",
                        "minItems": 0,
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"},
                                "value": {"type": "string"}
                            },
                            "required": [
                                "name",
                                "value"
                            ]               
                        }
                    },
                    "method": {
                        "type": "string",
                        "enum": ["get", "post", "put", "delete"]
                    },
                    "destination_type": {
                        "type": "string",
                        "enum": ["memory", "factory_storage", "nifi", "lineage_tracker"]
                    },
                    "response_dataset_field_path": {
                        "type": "string"
                    },
                    "response_metadata_field_path": {
                        "type": "string"
                    },
                    "lineage_tracking": {
                        "type": "boolean"
                    },
                    "uuid": {
                        "type": "string"
                    },
                    "data_uuid": {
                        "type": "string"
                    },
                    "access_token": {
                        "type": "string"
                    }
                },
                "required": [
                    "prev_run",
                    "job_name",
                    "source",
                    "endpoint",
                    "input_data",
                    "method",
                    "destination_type"
                ]
            }   
        )
    }
)

def pistis_job_periodic():

    MINIO_BUCKET_NAME = Variable.get("minio_pistis_bucket_api_key")
    MINIO_ROOT_USER = Variable.get("minio_api_key")
    MINIO_ROOT_PASSWORD = Variable.get("minio_passwd")
    MINIO_URL =  Variable.get("minio_url")
    DATA_CATALOGUE_URL = Variable.get("factory_data_catalogue_url")
    DATA_REPO_URL = Variable.get("factory_data_repo_url")
    DATA_STORAGE_URL = Variable.get("factory_data_storage_url")
    DATA_CATALOGUE_API_KEY = Variable.get("factory_data_catalogue_api_key")
    DATA_STORAGE_API_KEY = Variable.get("factory_data_storage_api_key")
    DATASET_JSON_LD_TEMPLATE = Variable.get("factory_dataset_json_ld_template")
    DATASET_JSON_LD_DATA_DISTRIBUTION_TEMPLATE = Variable.get("factory_dataset_json_ld_data_distribution_template")
    IAM_URL = Variable.get("iam_url")
    AUTH_URL = Variable.get("auth_url")
    ORCHESTRABLE_SERVICES = ["data-check-in", "data-transformation", "insights-generator"]
    CATALOG_NAME = Variable.get("catalog_name")
    PUBLISHER =  Variable.get("publisher_name")
    XML = ".xml"
    JSON = ".json"
    XLSX = ".xlsx"
    PARQUET = ".parquet"
    CSV = ".csv"
    TSV = ".tsv"

    client = Minio(MINIO_URL, access_key=MINIO_ROOT_USER, secret_key=MINIO_ROOT_PASSWORD, secure=False)
    jsoninja = Jsoninja()
      
    def is_valid_url(url):
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except ValueError:
            return False
        
    def format_orginal_ds_name(ds_name):
        # Regular expression pattern to match and remove the substring between _jc_ tags
        pattern = r'_jc.*?_jc'

        # Remove the matched substring
        final_name = re.sub(pattern, '', ds_name) 

        # Update file extension        
        #final_name = re.sub(r'\.[^.]+$', extension, temp_name)
        #logging.info(" ### format_orginal_ds_name: final name is " + final_name)
        return final_name  
        
    def add_dataset_to_factory_data_storage(source, uuid, access_token, append_required):
        logging.info(" ### Adding dataset to Factory Data Storage using source: " + source)
        file=[]
        s3_path = source[len("s3://" + MINIO_URL + "/"):]
        s3_list = s3_path.split('/')
        index = len(s3_list) - 2
        asset_uuid = uuid
        
        if (len(s3_list) > 0):
            bucket_name = s3_list[index]
            object_name = s3_list[index + 1]
            logging.info(" ### Getting S3 Object with bucket = " + bucket_name + " and object_name = " + object_name)
            file = client.get_object(bucket_name,object_name)   

            files=[
                   #('file',(re.sub(r'\.[^.]+$', '', object_name), res_file,'rb'))
                   ('file',(object_name, file,'rb'))
                ]

        payload = {}
          
        headers = {
                    "Authorization": "Bearer " + access_token # DATA_STORAGE_API_KEY
                  }
        if (uuid == "none"):
            endpoint = DATA_STORAGE_URL + "/api/files/create_file"
            logging.info(" pistis_job_periodic#add_dataset_to_factory_data_storage: Calling Service with: headers = " + str(headers) + "; files = " + str(files) + "; endpoint = " + str(endpoint) + "; data = " + str(payload))
            res = requests.post(url=endpoint, headers=headers, data=payload, files=files)
        else:

           if (append_required):
              endpoint = DATA_STORAGE_URL + "/api/files/append_files?asset_uuid=" + uuid 
              logging.info(" pistis_job_periodic#append_dataset_to_factory_data_storage: Calling Service with: headers = " + str(headers) + "; files = " + str(files) + "; endpoint = " + str(endpoint) + "; data = " + str(payload))
              res = requests.post(url=endpoint, headers=headers, data=payload, files=files) 
           else:
              endpoint = DATA_STORAGE_URL + "/api/files/update_file?asset_uuid=" + uuid 
              logging.info(" pistis_job_periodic#update_dataset_to_factory_data_storage: Calling Service with: headers = " + str(headers) + "; files = " + str(files) + "; endpoint = " + str(endpoint) + "; data = " + str(payload))
              res = requests.put(url=endpoint, headers=headers, data=payload, files=files)
              json_res = res.json()
              asset_uuid = json_res['asset_uuid']  
            
        
        logging.info(" ### add_dataset_to_factory_data_storage request: " + str(res.json()))
        
        return asset_uuid
    
    def get_dataset_to_factory_data_storage(ds_path, access_token):
        logging.info(" ### Getting dataset to Factory Data Storage using URL: " + ds_path)
        
        payload = {}
          
        #access_token = get_access_token()  
        headers = {
                    "Authorization": "Bearer " + access_token # DATA_STORAGE_API_KEY
                  }
        
        endpoint = ds_path
        logging.info(" pistis_job_periodic#get_dataset_to_factory_data_storage: Calling Service with: headers = " + str(headers) + "; endpoint = " + str(endpoint) + "; data = " + str(payload))
        res = requests.get(url=endpoint, headers=headers, data=payload)
             
        logging.info(" ### get_dataset_to_factory_data_storage request: " + str(res))
        
        return res

    def add_dataset_to_factory_data_catalogue(ds_json_ld, access_token):
        # TO-DO define catalogue name as input
        logging.info(" ### Adding dataset to Factory Data Catalogue ... ")

        files = []
        payload = json.dumps(ds_json_ld)
            
        headers = {
                   "X-API-Key": DATA_CATALOGUE_API_KEY,
                   #"Authorization": "Bearer " + access_token,
                   "Content-Type": "application/ld+json"
                  }
        endpoint = DATA_REPO_URL + "/catalogues/" + CATALOG_NAME + "/datasets"
            
        logging.info(" pistis_job_periodic#add_dataset_to_factory_data_storage: Calling Service with: headers = " + str(headers) + "; files = " + str(files) + "; endpoint = " + str(endpoint) + "; data = " + str(payload))
        
        res = requests.post(url=endpoint, headers=headers, data=payload, files=files)
        logging.info(" ### add_dataset_to_factory_data_catalogue request: " + str(res))
        return res.json()        
    
    
    def persist_in_minio(field_value, source):
        logging.info(" ### Persisting object in MINIO ... ")
        object_url = field_value

        persist_required = False
        s3_path = source[len("s3://" + MINIO_URL + "/"):]
        s3_list = s3_path.split('/')
        index = len(s3_list) - 2
        if (len(s3_list) > 0):
            #bucket_name = s3_list[index]
            object_name = s3_list[index + 1]

        if (type(field_value) is dict):
            logging.info(" ### Persisting a JSON object ... ")
            persist_required = True
            encoded_data = json.dumps(field_value).encode('utf-8')
        elif (not is_valid_url(field_value)): 
            logging.info(" ### Persisting a File ...  ")
            persist_required = True
            encoded_data = bytes(field_value, 'utf-8')   
            
        if (persist_required):

            # Check and convert encoding if needed to UTF-8

            # Put  data in the bucket
            result = client.put_object(MINIO_BUCKET_NAME, object_name, data=BytesIO(encoded_data), length=len(encoded_data)) 
            object_url = "s3://" + MINIO_URL + "/" + MINIO_BUCKET_NAME + "/" + result.object_name
            logging.info(" ### Object persisted in MINIO. URL: " + object_url)    
        
        return object_url 

    def update_workflow_status(status, message, runId):
       json_wf = { "runId": runId, "status": status, "catalogue_dataset_endpoint": "none", "message": message }
       wf_s3_endpoint =  "s3://" + MINIO_URL + "/" + MINIO_BUCKET_NAME + "/" + runId + ".json"
       persist_in_minio(json_wf, wf_s3_endpoint)


    def getFileName(source):
       s3_path = source[len("s3://" + MINIO_URL + "/"):]
       s3_list = s3_path.split('/')
       object_name = s3_list[len(s3_list)-1]
       return os.path.splitext(object_name)[0]
    
    def getFileExtension(source):
       s3_path = source[len("s3://" + MINIO_URL + "/"):]
       s3_list = s3_path.split('/')
       object_name = s3_list[len(s3_list)-1]
       extension = os.path.splitext(object_name)[1]
       return extension.upper()[1:]

    def generate_dataset_json_ld(source, metadata, uuid_url, extension):
       
       logging.info(" pistis_job_periodic#generate_dataset_json_ld: Starting json ld generation ... ") 
       evaluable_attrs = ['dataset_name','dataset_description', 'insights'] ## Add  meta fields to be evaluated
       ds_title = "Pistis Dataset"
       ds_description = "Pistis Dataset"
       s3_path = source[len("s3://" + MINIO_URL + "/"):]
       s3_list = s3_path.split('/')
       index = len(s3_list) - 2
       insightsURL = "none"
       ds_name = ""
       
       if (len(s3_list) > 0):
           bucket_name = s3_list[index]
           object_name = s3_list[index + 1]
           stat = client.stat_object(bucket_name, object_name)

       logging.info(" pistis_job_periodic#generate_dataset_json_ld: Evaluating metadata ... ") 
       for meta_field in metadata.keys():
           logging.info(" ### metadata = " + meta_field + " and value = " + metadata[meta_field]) 
           if (meta_field in evaluable_attrs):
               if (meta_field == "dataset_name"):
                    ds_title = metadata[meta_field]
               elif (meta_field == "dataset_description"):
                    ds_description = metadata[meta_field] 
               elif (meta_field == "insights"):
                    insightsURL = metadata[meta_field]     
                              
       date = datetime.utcnow().isoformat()

       raw_name = format_orginal_ds_name(object_name)
       replacements = {
                       "foaf_mbox_id" : "mailto:admin@pistis.eu",
                       "foaf_name": PUBLISHER,
                       "skos_exactMatch_id": "", #"https://subscriptionlicense.com",
                       "ds_language": "en",
                       "ds_description": ds_description,
                       "date_issued": date,
                       "date_modified": date,
                       "ds_title": ds_title,
                       "file_name": raw_name,
                       "accessURL": uuid_url,
                       "ds_byte_size": str(stat.size),
                       "insights": insightsURL,
                       "file_type": extension
                      }
       template = json.loads(DATASET_JSON_LD_TEMPLATE)
       logging.info(" pistis_job_periodic#generate_dataset_json_ld: TEMPLATE GENERATED = " + str(template)) 
       return jsoninja.replace(template, replacements)

 
    def generate_json_ld_data_distribution(access_url, extension):      
       logging.info(" pistis_job_periodic#generate_dataset_json_ld: Starting json ld generation ... ") 
       
       ds_title = "Additional Distribution - "                  
       date = datetime.utcnow().isoformat()

       replacements = {
                       "data_dist_name": ds_title + date,
                       "data_dist_accessURL": access_url,
                       "date_issued": date,
                       "file_type": extension
                      }
       template = json.loads(DATASET_JSON_LD_DATA_DISTRIBUTION_TEMPLATE)
       logging.info(" pistis_job_periodic#generate_json_ld_data_distribution: TEMPLATE GENERATED = " + str(template)) 
       return jsoninja.replace(template, replacements)
        
    def notify_access_policy(uuid, name, description, access_token):
       logging.info(" pistis_job_periodic#notify_access_policy: Calling to IAM to notify access policy for DS with UUID = " + str(uuid))
       payload = {
            "id": uuid,
            "name": name,
            "description": description
       }
          
       headers = {
                  "Content-Type": "application/json",
                  "Authorization": "Bearer " + access_token 
                  }
       
       endpoint = IAM_URL 
            
       logging.info(" pistis_job_periodic#notify_access_policy: Calling Service with: headers = " + str(headers) + "; endpoint = " + str(endpoint) + "; data = " + str(payload))
        
       res = requests.post(url=endpoint, headers=headers, json=payload)
       logging.info(" ### pistis_job_periodic#notify_access_policy  Response: " + str(res))
       return res 
    
    def add_distribution_to_data_catalogue(uuid, ds_json_ld, access_token):
       logging.info(" pistis_job_periodic#add_distribution_to_data_catalogue: Adding distribution to DS with UUID = " + str(uuid))

       payload = json.dumps(ds_json_ld)
            
       headers = {
                   "X-API-Key": DATA_CATALOGUE_API_KEY,
                   #"Authorization": "Bearer " + access_token,
                   "Content-Type": "application/ld+json"
                  }
       endpoint = DATA_REPO_URL + "/datasets/" + uuid + "/distributions" 
            
       logging.info(" pistis_job_periodic#add_distribution_to_data_catalogue: Calling Service with: headers = " + str(headers) + "; endpoint = " + str(endpoint) + "; data = " + str(payload))
        
       res = requests.post(url=endpoint, headers=headers, data=payload)
       logging.info(" ### pistis_job_periodic#add_distribution_to_data_catalogue  Response: " + str(res))
       return res

    def update_value(data, key_to_match, new_value):
        #logging.info(" ### pistis_job_periodic#update_value: KEY_TO_MATCH = " + key_to_match)
        if isinstance(data, dict):
            for key, value in data.items():
                if key_to_match in key:
                    #logging.info(" ### pistis_job_periodic#update_value: KEY = " + key + "; VALUE = " + new_value)
                    if (isinstance(value, (dict))):
                      data[key]["@value"] = new_value
                    else:
                      data[key] = new_value        
                elif isinstance(value, (dict, list)):
                    update_value(value, key_to_match, new_value)     
        elif isinstance(data, list):
            for item in data:
                update_value(item, key_to_match, new_value)      

    def update_metadata_in_data_catalogue(uuid, metadata, access_token):
        # TO-DO define catalogue name as input
        logging.info(" pistis_job_periodic#update_metadata_in_data_catalogue: Updating dataset to Factory Data Catalogue with ID = " + uuid)

        payload = [] # json.dumps(ds_json_ld)
            
        headers = {
                   "X-API-Key": DATA_CATALOGUE_API_KEY,
                   #"Authorization": "Bearer " + access_token,
                   "Content-Type": "application/json"
                  }
        endpoint = DATA_REPO_URL + "/datasets/" + uuid
            
        logging.info(" pistis_job_periodic#update_metadata_in_data_catalogue: Calling Service with: headers = " + str(headers) + "; endpoint = " + str(endpoint) + "; data = " + str(payload))
        
        res = requests.get(url=endpoint, headers=headers, data=payload)

        logging.info(" ### pistis_job_periodic#update_metadata_in_data_catalogue  Response: " + str(res))

        ds_json = res.json()
        for meta_field in metadata.keys():
           if (metadata[meta_field].startswith("s3:/")):
               ds_uuid = add_dataset_to_factory_data_storage(metadata[meta_field], "none", access_token, False)
               metadata[meta_field] = DATA_STORAGE_URL + "/api/files/get_file?asset_uuid=" + str(ds_uuid)
           update_value(ds_json, meta_field, metadata[meta_field])  

        payload = json.dumps(ds_json)
            
        headers = {
                   "X-API-Key": DATA_CATALOGUE_API_KEY,
                   #"Authorization": "Bearer " + access_token,
                   "Content-Type": "application/ld+json"
                  }
        endpoint = DATA_REPO_URL + "/datasets/" + uuid
            
        logging.info(" pistis_job_periodic#update_metadata_in_data_catalogue: Calling Service with: headers = " + str(headers) + "; endpoint = " + str(endpoint) + "; data = " + str(payload))
        
        res = requests.put(url=endpoint, headers=headers, data=payload)

        logging.info(" ### pistis_job_periodic#add_distribution_to_data_catalogue  Response: " + str(res))
        return res


    def convert_df_to_file (df, file, format):

        logging.info(" ### convert_df_to_file: Converting dataset to  " + format)
        print(df.to_string())

        if (format.lower() == XML):
                logging.info(" ### convert_df_to_file: Formatting to XML ... ")
                df.to_xml(file)
        elif (format.lower() == XLSX):
                logging.info(" ### convert_df_to_file: Formatting to XLSX ... ")
                df.to_excel(file, index=False)
        elif (format.lower() == JSON):
                logging.info(" ### convert_df_to_file: Formatting to JSON ... ")
                df.to_json(file, orient='records', lines=True)
        elif (format.lower() == CSV):
                logging.info(" ### convert_df_to_file: Formatting to CSV ... ")
                df.to_csv(file, index=False)
        elif (format.lower() == PARQUET):
                logging.info(" ### convert_df_to_file: Formatting to Parquet ... ")
                df.to_parquet(file)
        elif (format.lower() == TSV):
                logging.info(" ### convert_df_to_file: Formatting to TSV ... ")
                df.to_csv(file, sep='\t', index=False)                  

    def detect_encoding(file_path):
        with open(file_path, 'rb') as f:
            raw_data = f.read()
            result = chardet.detect(raw_data)
            return result['encoding']

    def convert_to_utf8(file_path):
        encoding = detect_encoding(file_path)
        with open(file_path, 'r', encoding=encoding) as f:
            content = f.read()
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)

        logging.info(" ### File " + file_path + " has been converted to UTF-8 encoding.")

    def transform_ds_format(file, converted_file, extension, output_format):
        
        #logging.info(" ### pistis_job_periodic#transform_to_csv: Evaluating file format for conversion to CSV ")
        logging.info(" ### pistis_job_periodic#transform_to_csv: Converting file from " + extension + " to " + output_format)
        if extension.lower() == JSON:           
           with open(file, encoding='utf-8') as inputfile:
              df = pd.read_json(inputfile)
              
           #df.to_csv(converted_file, encoding='utf-8', index=False)            
        
        elif extension.lower() == TSV:
           # Read the TSV file into a DataFrame
           df = pd.read_csv(file, sep='\t')
            
           # Write the DataFrame to a CSV file
           #df.to_csv(converted_file, index=False)

        elif extension.lower() == XML:
           # Read the XML file into a DataFrame
           df = pd.read_xml(file)
            
           # Write the DataFrame to a CSV file
           #df.to_csv(converted_file, index=False)  

        elif extension.lower() == XLSX:
           # Read the XLSX file into a DataFrame
           df = pd.read_excel(file)
            
           # Write the DataFrame to a CSV file
           #df.to_csv(converted_file, index=False)      
        
        elif extension.lower() == PARQUET:
           # Reading the data from Parquet File
           pf = ParquetFile(file)

           # Converting data in to pandas dataFrame
           df = pf.to_pandas()

           # Converting to CSV
           #df(converted_file, index = False)

        elif extension.lower() == CSV:
           # Read the TSV file into a DataFrame
           df = pd.read_csv(file)   

        convert_df_to_file(df, converted_file, output_format.strip())   


    @task()
    def retrieve_and_dump_data_and_metadata_to_bucket():

        context = get_current_context()
        job_info = context["params"]["job_data"]
        source = job_info["source"]
        root_run_id = job_info["root_dag_run"]
        wf_results_id = job_info['wf_results_id'] 
        job_name = job_info["job_name"]
        content = ""
        prefixes = [JSON, TSV, PARQUET, XML, XLSX]
        
        try:
            dr_list = DagRun.find(dag_id="pistis_periodic_workflow", run_id=root_run_id)
            
            # Retrieve wf raw data using wf param dataset and update them over job source
            if (len(dr_list) > 0):
                #job_info["source"] = dr_list[0].conf['dataset']
                
                ds_name = dr_list[0].conf['dataset_name']
                ds_description = dr_list[0].conf['dataset_description']

                if (source == "workflow"):
                    logging.info(" pistis_job_periodic#retrieve: Retrievind data and metadata from workflow ... ") 

                    # Initialze JSON workflow resukts 
                    # wf_results = { "runId": root_run_id, "status": "executing", "catalogue_dataset_endpoint": "none" }
                    # wf_s3_endpoint =  "s3://" + MINIO_BUCKET_NAME + "/" + root_run_id + ".json"
                    # persist_in_minio(wf_results, wf_s3_endpoint)

                    # Retrieving dataset
                    json_dataset = dr_list[0].conf['dataset']
                    file_full_name = json_dataset['name']
                    file_name = os.path.splitext(file_full_name)[0]
                    extension = os.path.splitext(file_full_name)[1]
                    file_base64_string = json_dataset['content']
                    conv_file = "raw_"  + file_full_name
                    decoded_data = decodebytes(file_base64_string.encode("utf-8"))
                    bytesio_object = BytesIO(decoded_data)

                    # Write the stuff
                    with open(file_full_name, "wb") as file:
                        file.write(bytesio_object.getbuffer())

                    #files[field['name']] = ("airflow_dataset.csv", file, 'text/csv') 

                    # Retrieving metadata
                    #json_meta = dr_list[0].conf['metadata']
                    #json_meta_encode_data = json.dumps(json_meta).encode('utf-8')

                    # Check file format is: json, csv, tsv or parquet
                    if file_full_name.lower().endswith(tuple(prefixes)):
                        #transform_ds_format(file_full_name, conv_file, extension, CSV)
                        with open(file_full_name, "rb") as file:
                            file_content = file.read()
                            file_base64_string = b64encode(file_content).decode('utf-8')
                            decoded_data = decodebytes(file_base64_string.encode("utf-8"))
                    #elif extension.lower() != CSV:
                    #    raise Exception("File format not supported. Formats supported are: CSV, Json, Xml, TSV, Xlsx and Parquet")
                        
                    # Put  data in the bucket
                    result = client.put_object(MINIO_BUCKET_NAME, file_name + "_jc" + root_run_id + "_jc" + extension, data=BytesIO(decoded_data), length=len(decoded_data)) 
                    
                    # update source using minio uri
                    job_info["source"] = "s3://" + MINIO_URL + "/" + MINIO_BUCKET_NAME + "/" + result.object_name

                    # Put metedata in a bucket
                    #result = client.put_object(MINIO_BUCKET_NAME, file_name + "_meta." + root_run_id + ".json", data=BytesIO(json_meta_encode_data), length=len(json_meta_encode_data)) 

                    #remove temp file
                    os.remove(file_full_name)

                elif (source == "job"):
                    # To Do -> use source and auth info to retrieve data source
                    logging.info(" pistis_job_periodic#retrieve: Retrievind data internally from job ... ")       
                elif (is_valid_url(source)):
                    # To Do -> use source and auth info to retrieve data source
                    logging.info(" pistis_job_periodic#retrieve: Retrievind data from url (data path)" + str(source)) 


                # Update metadata using wf inputs
                job_info["metadata"] = {"dataset_name": ds_name, "dataset_description": ds_description}
                
            return job_info
        
        except Exception as e:
             update_workflow_status("error", "JOB =" + job_name + " ; TASK => retrieve_and_dump_data_and_metadata_to_bucket : " +  repr(e), wf_results_id)
             raise Exception(" TASK => retrieve_and_dump_data_and_metadata_to_bucket : " +  repr(e))

    @task()
    def resolve_mappings(job_info):
        context = get_current_context()        
        source = job_info["source"]
        root_run_id = job_info["root_dag_run"]
        wf_results_id = job_info['wf_results_id'] 
        prev_run = job_info["prev_run"]
        current_job_name = job_info["job_name"]
        jobs = [current_job_name]
        evaluable_attrs = ['input_data','source']

        try: 
            prev_run_list = prev_run.split('#')
            if (len(prev_run_list) > 0):
                prev_job_name = prev_run_list[0]
                prev_run_id = prev_run_list[1]
            
                if prev_job_name not in jobs:
                    jobs.append(prev_job_name)

                if (prev_job_name != 'none'):

                    logging.info("pistis_periodic_workflow#resolve_mappings: Dag Run_Id = " + str(prev_run_id)) 
                    dr_list = DagRun.find(dag_id="pistis_job_periodic", run_id=prev_run_id)
                    logging.info("pistis_periodic_workflow#resolve_mappings: DR List = " + str(len(dr_list))) 
                    
                    if (len(dr_list) > 0):

                        ti = dr_list[0].get_task_instance(task_id='storage')
                        logging.info("pistis_periodic_workflow#resolve_mappings: Dag Task Instance = " + str(ti))
                        
                        task_meta_result = ti.xcom_pull(task_ids='storage', key='return_value') 
                        logging.info("pistis_periodic_workflow#resolve_mappings: Task Result = " + str(task_meta_result))    

                        # update metadata using previous job metadata
                        job_info["metadata"] = task_meta_result['metadata']

                    # Update input_data with results comming from previous job execution
                    #input_data = job_info["input_data"]
                        
                    logging.info("pistis_periodic_workflow#resolve_mappings: Job List = " + str(jobs))     
                    for job_name in jobs:
                        logging.info("pistis_periodic_workflow#resolve_mappings: Jobs loop -> Job = " + str(job_name))
                        for attr in evaluable_attrs:
                            value_list = []
                            if (type(job_info[attr]) is list):
                                value_list = value_list + job_info[attr]
                            else:  
                                value_list.append(attr) 

                            for idata in value_list:
                                logging.info("pistis_periodic_workflow#resolve_mappings: Input Data -> Field = " + str(idata))

                                mapping_regex = "none"    

                                if ("value" in idata):
                                   mapping_regex = idata['value'] 
                                else:      
                                   mapping_regex =  job_info[idata] 

                                if (mapping_regex.startswith(job_name)):
                                
                                    # remove job name from mapping definition
                                    mapping =  mapping_regex.replace(job_name + ".", "")
                                    map_list = mapping.split('.') 
                                    map_value = "none"
                                    job_data = "none"

                                    if (job_name == current_job_name):
                                        job_data = job_info
                                    else:
                                        job_data = task_meta_result    
                                    
                                    for attr in map_list:
                                        logging.info("pistis_periodic_workflow#resolve_mappings: Mapping List -> attr = " + str(attr))
                                        # if current job use context to resolve
                                        #if (job_name == current_job_name):
                                        
                                        if type(job_data) is list:
                                                # if (attr in job_data.keys()):
                                                #     map_value = job_data[attr]
                                                #     logging.info("pistis_periodic_workflow#resolve_mappings: Map Value using " + str(attr) + " => " + str(map_value))
                                                    
                                                # else:
                                                    
                                                # Search data based on key and value using filter and list method
                                                rlist = list(filter(lambda x: (x['name']==attr), job_data))
                                                if (len(rlist) > 0):
                                                    map_value= rlist[0]['value']
                                                    logging.info("pistis_periodic_workflow#resolve_mappings: Map Value using " + str(rlist[0]['value']) + " => " + str(map_value))

                                        elif (attr in job_data.keys()):
                                            if (type(job_data[attr]) is not list):
                                                map_value = job_data[attr]
                                                logging.info("pistis_periodic_workflow#resolve_mappings: Map Value using " + str(job_data[attr]) + " => " + str(map_value))
                                            job_data = job_data[attr]
                                                
                                        else:        
                                            map_value = job_data
                                            logging.info("pistis_periodic_workflow#resolve_mappings: Map Value using " + str(job_data) + " => " + str(map_value))    

                                        # else:     

                                        #     # check if attr is key on json object
                                        #     if attr in task_result.keys():
                                        #         map_value = task_result[attr]
                                        #         logging.info("pistis_periodic_workflow#resolve_mappings: Map Value using " + str(attr) + " => " + str(map_value))
                                        #     else:
                                        #     # Search data based on key and value using filter and list method
                                        #         rlist = list(filter(lambda x: (x['name']==attr), input_data))
                                        #         if (len(rlist) > 0):
                                        #             map_value= rlist[0]['value'] 
                                        #             logging.info("pistis_periodic_workflow#resolve_mappings: Map Value using " + str(rlist[0]['value']) + " => " + str(map_value))    
                                                        
                                    # update resolved mapping into job input data
                                    if ('value' in idata):
                                        idata['value'] = map_value
                                    else:
                                        job_info[idata] = map_value   
                                    logging.info("pistis_periodic_workflow#resolve_mappings: Mapping resolved for " + str(idata) + " with " + str(map_value))                            
                    
                    
            return job_info
        
        except Exception as e:
             update_workflow_status("error", "JOB =" + current_job_name + " ; TASK => resolve_mappings: " +  repr(e), wf_results_id)
             raise Exception(" TASK => resolve_mappings: " +  repr(e))

    
    @task()
    def callService (job_info):
        context = get_current_context()
        endpoint = job_info["endpoint"]
        method = job_info["method"]
        ctype=  job_info["content-type"]
        job_name = job_info["job_name"]
        wf_results_id = job_info['wf_results_id'] 
        access_token = job_info['access_token'] 
               
        headers = {
                    "Accept": ctype,
                    "Authorization": "Bearer " + access_token 
                  }
        
        #evaluable_attrs = ['file', 'metadata']
        evaluable_attrs = ['file']
        input_data= job_info["input_data"]
        data = json.loads('{}')
        files = json.loads('{}')
        run_id = context['dag_run'].run_id
        root_run_id = job_info["root_dag_run"]

        try:

            # Build json data from job input data
            for field in input_data:
                logging.info(" pistis_job_periodic#callService: Calling Service with: Field = " + str(field))
                field_value = field['value']
                if (type(field_value) is str):
                        field_value = field_value.replace("'", "\"")

                if (field['name'] in evaluable_attrs):
                    file_name = "airflow_dataset:_" + run_id
                    if (field_value.startswith("s3://")):
                        field_value = field_value[len("s3://" + MINIO_URL + "/"):]
                        s3_list = field_value.split('/')
                        if (len(s3_list) > 0):
                            bucket_name = s3_list[0]
                            object_name = s3_list[1]
                            file = client.get_object(bucket_name,object_name)
                            file_name = object_name
                    else:
                        base64_string = field_value             
                        decoded_data = decodebytes(base64_string.encode("utf-8"))
                        file = BytesIO(decoded_data)

                    if (field['name'] == "file"):
                        files[field['name']] = (file_name, file) 
                    #elif (field['name'] == "metadata"):   
                    #   data[field['name']] = file

                else:     
                  data[field['name']] = field_value
                
            
            logging.info(" pistis_job_periodic#callService: Calling Service with: headers = " + str(headers) + "; files = " + str(files) + "; endpoint = " + str(endpoint) + "; method = " + str(method) + "; data = " + str(data))
            if (method.lower() == "post" ):
              res = requests.post(url=endpoint,headers=headers,data=data, files=files)
            elif (method.lower() == "get" ):
              res = requests.get(url=endpoint,headers=headers,data=data, files=files)
            elif (method.lower() == "put" ):
              res = requests.put(url=endpoint,headers=headers,data=data, files=files)
            elif (method.lower() == "delete" ):
              res = requests.delete(url=endpoint,headers=headers,data=data)      
            logging.info(" pistis_job_periodic#callService: Service Reponse: " + str(res) ) 
            
            #update source with last version of ds
            logging.info(" ### Updating source with last version of ds .... ")
            #job_info["source"] = "s3://" + MINIO_BUCKET_NAME + "/" + result.object_name

            dataset_field_path = job_info["response_dataset_field_path"]
            if (not dataset_field_path == "") and (not dataset_field_path.isspace()) and (dataset_field_path.strip().lower() != "none"): 
                if (dataset_field_path.strip().lower() == "file"):
                    logging.info(" ### Getting request content and decoding .... ")
                    res_val = res.content.decode('utf-8')
                    logging.info(" ### Result:  " + str(res_val))
                else:
                    logging.info(" ### Getting request as JSON .... ")
                    res_val = res.json()
                    logging.info(" ### Result:  " + str(res_val))    

                    if (dataset_field_path.strip().lower() != "json"):
                        field_list = dataset_field_path.split('.')
       
                        for field in field_list:
                            res_val = res_val[field]
                
                logging.info(" ### Persisting Resuls in Minio ... ")
                object_url = persist_in_minio(res_val, job_info["source"])
                job_info["source"] = object_url

            # TO-DO update metadata with last version of metadara
            metadata_field_path = job_info["response_metadata_field_path"]
            if (not metadata_field_path == "") and (not metadata_field_path.isspace()) and (metadata_field_path.strip().lower() != "none"): 
                #json_res_val = res.json()
                
                # TO-DO support metadata fiels path as list ([key:path;key2:path; ...])
                meta_field_list = metadata_field_path.split(':')
                if (len(meta_field_list) > 1):
                    template_field_key = meta_field_list[0]
                    meta_field_path = meta_field_list[1]
                    
                    if (meta_field_path.strip().lower() == "html"):  
                        res_val = res.content.decode('utf-8')
                        json_s3_name = endpoint[len("http://"):]
                        s3_full_name = "s3://" + MINIO_URL + "/" + MINIO_BUCKET_NAME + "/" + json_s3_name.split('.')[0] + "_request_" + run_id + ".html"
                        json_res_val = persist_in_minio(res_val, s3_full_name)   
                    
                    elif (meta_field_path.strip().lower() == "json"):
                        json_res_val = res.json()
                        json_s3_name = endpoint[len("http://"):]
                        s3_full_name = "s3://" + MINIO_URL + "/" + MINIO_BUCKET_NAME + "/" + json_s3_name.split('.')[0] + "_request_" + run_id + ".json"
                        json_res_val = persist_in_minio(json_res_val, s3_full_name)

                    else: 
                        json_res_val = res.json()
                        field_list = metadata_field_path.split('.')
                                
                        for field in field_list:
                            json_res_val = json_res_val[field]    

                #object_url = persist_in_minio(json_res_val, job_info["metadata"])
                dict = { template_field_key: json_res_val }
                job_info["metadata"].update(dict)

            return job_info 
        
        except Exception as e:
             update_workflow_status("error", "JOB =" + job_name + " ; TASK => callService task: " +  repr(e), wf_results_id)  
             raise Exception(" TASK => callService: " +  repr(e))
    
       
    def register_default_access_policy(uuid, ds_name, ds_desc, access_token):
        logging.info("### pistis_job_periodic#register_default_access_policy: Registering default access policy ... ")

        ## Get Access Token
        #access_token = get_access_token()

        ## Call to IAM to notify access policy 
        notify_access_policy(uuid, ds_name, ds_desc, access_token)   

    def requires_add_data_distribution(service_endpoint):
        return ORCHESTRABLE_SERVICES[1] in service_endpoint
    
    def requires_append_data(service_endpoint):
        return ORCHESTRABLE_SERVICES[0] in service_endpoint
    
    def requires_only_metadata_update(service_endpoint):
        return ORCHESTRABLE_SERVICES[2] in service_endpoint
        
    @task()
    def storage(job_info):

        wf_results_id = job_info['wf_results_id'] 
        root_run_id = job_info["root_dag_run"]
        job_name = job_info["job_name"]
        source = job_info["source"]
        metadata = job_info["metadata"]
        service_endpoint = job_info["endpoint"]
        prev_run = job_info["prev_run"]
        last_job = job_info["is_last_job"]
        access_token = job_info["access_token"]
        ds_path_url = source
        uuid = 0
        extension = getFileExtension(source)

        try:
            
            # Check if lineage tracking is needed
            #if (lineage_tracking or (destination_type == "factory_storage")):
            if requires_append_data(service_endpoint):
                # Store data asset in factory storage
                logging.info(" pistis_job_periodic#requires_append_data: Appending data in factory storage ... ")
                data_uuid = add_dataset_to_factory_data_storage(source, job_info["data_uuid"], access_token, True)
                logging.info(" pistis_job_periodic#requires_append_data: Persited with DATA_UUID " + str(data_uuid))

                ds_path_url = DATA_STORAGE_URL + "/api/files/get_file?asset_uuid=" + str(data_uuid)
                res = get_dataset_to_factory_data_storage(ds_path_url, access_token)

                
                logging.info(" pistis_job_periodic#requires_append_data: Appended DS got it from Factory storage ...")

                # Rename file to JC naming 
                job_info["source"] = "s3://" + MINIO_URL + "/" + MINIO_BUCKET_NAME  + "/" + getFileName(job_info["source"]) + "_jc" + root_run_id + "_jc" + getFileExtension(job_info["source"])    
                
                # update source using minio uri
                logging.info(" pistis_job_periodic#Persisting Resuls in Minio ... ")
                object_url = persist_in_minio(res.content.decode('utf-8'), job_info["source"])
                
                logging.info(" pistis_job_periodic#requires_append_data: Persited with DATA_UUID " + str(data_uuid))

                # Store metadata in factory data catalogue using uuid got it from data storage                            
                logging.info(" pistis_job_periodic#requires_append_data: Updating metadata in data catalogue: modified and size ... ") 
                
                metadata_to_update = {"modified": datetime.utcnow().isoformat(), "byteSize": str(len(res.content))}
                update_metadata_in_data_catalogue(job_info["uuid"], metadata_to_update, access_token)
                logging.info(" pistis_job_periodic#requires_append_data: Metadata updated in the Catalogue for DS with uuui: "+ job_info["uuid"])

            if requires_add_data_distribution(service_endpoint):

                #TO-DO: Delete previous data distributions 

                # Add data distribution to DS in the catalogue 
                logging.info(" pistis_job_periodic#requires_access_policy_notification: Adding data distribution in data catalogue ... ")

                prev_run_list = prev_run.split('#')
                if (len(prev_run_list) > 0):
                    prev_job_name = prev_run_list[0]
                    prev_run_id = prev_run_list[1]

                    if (prev_job_name != 'none'):
                        dr_list = DagRun.find(dag_id="pistis_job_periodic", run_id=prev_run_id)
                        logging.info("pistis_periodic_workflow#requires_add_data_distribution: DR List = " + str(len(dr_list))) 
                        
                        if (len(dr_list) > 0):
                            ti = dr_list[0].get_task_instance(task_id='resolve_mappings')
                            logging.info("pistis_periodic_workflow#requires_add_data_distribution: Dag Task Instance = " + str(ti))
                                
                            task_result = ti.xcom_pull(task_ids='storage', key='return_value') 
                            logging.info("pistis_periodic_workflow#requires_add_data_distribution: Task Result = " + str(task_result))

                            # update metadata using previous job metadata
                            job_info["uuid"] = task_result['uuid']
                            job_info["data_uuid"] = task_result['data_uuid']

                if ("uuid" in job_info.keys()): 
                    logging.info(" pistis_job_periodic#requires_add_data_distribution: Storing data in factory storage ... ")
                    new_uuid = add_dataset_to_factory_data_storage(source, job_info["data_uuid"], access_token, False)
                    logging.info(" pistis_job_periodic#requires_add_data_distribution: Added DS with UUID = " + str(new_uuid))
                    accessURL = DATA_STORAGE_URL + "/api/files/get_file?asset_uuid=" + str(new_uuid)
                    json_ld = generate_json_ld_data_distribution(accessURL, extension)
                    logging.info(" pistis_job_periodic#requires_add_data_distribution: Adding data distribution ... ")           
                    add_distribution_to_data_catalogue(job_info["uuid"], json_ld, access_token)
                    logging.info(" pistis_job_periodic#requires_add_data_distribution: Data distribution added ... ")

            if (requires_only_metadata_update(service_endpoint)):
                # Add data distribution to DS in the catalogue 
                logging.info(" pistis_job_periodic#requires_only_metadata_update: Update only metadata in data catalogue ... ")
                # Store metadata in factory data catalogue using uuid got it from data storage
                prev_run_list = prev_run.split('#')
                if (len(prev_run_list) > 0):
                    prev_job_name = prev_run_list[0]
                    prev_run_id = prev_run_list[1]

                    if (prev_job_name != 'none'):
                        dr_list = DagRun.find(dag_id="pistis_job_periodic", run_id=prev_run_id)
                        logging.info("pistis_periodic_workflow#requires_only_metadata_update: DR List = " + str(len(dr_list))) 
                        
                        if (len(dr_list) > 0):
                            ti = dr_list[0].get_task_instance(task_id='resolve_mappings')
                            logging.info("pistis_periodic_workflow#requires_only_metadata_update: Dag Task Instance = " + str(ti))
                                
                            task_result = ti.xcom_pull(task_ids='storage', key='return_value') 
                            logging.info("pistis_periodic_workflow#requires_only_metadata_update: Task Result = " + str(task_result))

                            # update metadata using previous job metadata
                            job_info["uuid"] = task_result['uuid']
                            job_info["data_uuid"] = task_result['data_uuid']

                if ("uuid" in job_info.keys()): 
                    logging.info(" pistis_periodic_workflow#requires_only_metadata_update: Updating metadata in Factory Data Catalogue using UUID = " + job_info["uuid"])
                    #uuid = add_dataset_to_factory_data_storage(source)
                    uuid = job_info["uuid"]
                    #logging.info(" pistis_job_periodic#requires_only_metadata_update: Added DS with UUID = " + uuid)
                    #ds_path_url = DATA_STORAGE_URL + "/api/files/get_file?asset_uuid=" + uuid
                    #ds_json_ld = generate_dataset_json_ld(source, metadata, ds_path_url)
                    logging.info(" pistis_job_periodic#requires_only_metadata_update: Updating metadata in data catalogue ... ") 
                    update_metadata_in_data_catalogue(job_info["uuid"], metadata, access_token)
                    logging.info(" pistis_job_periodic#requires_only_metadata_update: Metadata updated ... ") 

            # Update JSON workflow resukts 
            if (last_job):
                ds_catalogue_url = {'id': job_info["uuid"]} # DATA_CATALOGUE_URL + "/datasets/" + job_info["uuid"]
                wf_results = { "runId": wf_results_id, "status": "finished", "catalogue_dataset_endpoint": ds_catalogue_url }
                wf_s3_endpoint =  "s3://" + MINIO_URL + "/" + MINIO_BUCKET_NAME + "/" + root_run_id + ".json"
                persist_in_minio(wf_results, wf_s3_endpoint)
                
            return job_info 

        except Exception as e:
             logging.info(" ### STORE TASK EXCEPTION ... ")
             update_workflow_status("error", "JOB =" + job_name + " ; TASK => storage task: " +  repr(e), wf_results_id) 
             raise Exception(" TASK => storage task: " +  repr(e))

    job_retrieve = retrieve_and_dump_data_and_metadata_to_bucket()
    job_mappings = resolve_mappings(job_retrieve)
    job_data = callService(job_mappings)
    ds_path_url = storage(job_data)

    job_retrieve >> job_mappings >> job_data >> ds_path_url

pistis_job_periodic()