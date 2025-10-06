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

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from airflow.models import DAG
from airflow.models.param import Param
from airflow.decorators import dag, task, task_group
from airflow.operators.python import get_current_context, BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.dagrun import DagRun
import logging
import os
from airflow.settings import json
from airflow.models import Variable

@dag(start_date=datetime(2023,1,1), schedule="@once", catchup=False, render_template_as_native_obj=True, params={

        "workflow": Param(
            [{
                "prev_run": "000",
                "root_dag_run": "000",
                "job_name": "test_job",
                "source": "http://dataset.pistis",
                "input_data": [],
                "content-type": "application/json",
                "endpoint": "http://",
                "method": "get",
                "destination_type": "memory",
                "lineage_tracking": "true",
                "uuid": "0",
                "data_uuid": "0"
            }],
            schema = {
                "workflow": {
                    "type": "array",
                    "minItems": 0,
                    "items": {                        
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
                            "content-type": {
                                "type": "string"
                            },    
                            "source": {
                                "type": "string",
                                "pattern": "^(?:https?://|ftp://|none|workflow)"
                            },
                            "metadata": {
                                "type": "object"
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
                                "enum": ["memory", "factory_storage", "nifi"]
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
                            }    
                        },
                        "required": [
                            "source",
                            "endpoint",
                            "input_data",
                            "method",
                            "destination_type",
                            "content-type"
                        ]
                    }
                }    
            }    
        ),
        "dataset": Param({"key": "value"}, type=["object", "null"]),
        "dataset_name": Param("Pistis DataSet", type="string"),
        "dataset_description": Param("Pistis DataSet", type="string"),
        "access_token": Param("Access Token", type="string"),
        "encryption": Param("Encryption Flag", type="string"),
        "periodicity": Param("", type="string"),
        "dataset_category": Param("Category", type="string"),
        "dataset_keywords": Param(default='[]', type="string"),
        "raw_wf": Param(
            [{
                "prev_run": "000",
                "root_dag_run": "000",
                "job_name": "test_job",
                "source": "http://dataset.pistis",
                "input_data": [],
                "content-type": "application/json",
                "endpoint": "http://",
                "method": "get",
                "destination_type": "memory",
                "lineage_tracking": "true",
                "uuid": "0",
                "data_uuid": "0"
            }],
            schema = {
                "workflow": {
                    "type": "array",
                    "minItems": 0,
                    "items": {                        
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
                            "content-type": {
                                "type": "string"
                            },    
                            "source": {
                                "type": "string",
                                "pattern": "^(?:https?://|ftp://|none|workflow)"
                            },
                            "metadata": {
                                "type": "object"
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
                                "enum": ["memory", "factory_storage", "nifi"]
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
                            }    
                        },
                        "required": [
                            "source",
                            "endpoint",
                            "input_data",
                            "method",
                            "destination_type",
                            "content-type"
                        ]
                    }
                }    
            }    
        )
    }
)

def pistis_periodic_workflow():
        
    #Variable.set(key="current_job", value="none")

    @task()
    def get_job_from_workflow():
        job = {}
        context = get_current_context()
        #wf = context["params"]["workflow"]
        wf = context["params"]["workflow"]
        wf_size = len(wf)
        logging.info("### pistis_periodic_workflow.workflow: wf = "+ str(wf) + " type = " + str(type(wf)))
        if (wf_size > 0):
           job = wf[0]
           #Variable.update(key="current_job", value=job['job_name'])
           logging.info("### pistis_periodic_workflow.workflow: currrent_job = "+ job['job_name'])

           # update root dag id
           if (not 'root_dag_run' in job.keys()):
                job['root_dag_run'] = context['dag_run'].run_id
           
           # update wf_results_id
           if (not 'wf_results_id' in job.keys()):
                job['wf_results_id'] = job['root_dag_run']

           # remove current job from wf
           context["params"]["workflow"] = wf.pop(0)
           job['is_last_job'] = (len(wf) == 0)
           
           # check if there is info about prev job
           if (not 'prev_run' in job.keys()):
               job['prev_run'] = job['job_name'] + "#" +  context['dag_run'].run_id

           # Update in next job the current job + run_id
           if (wf_size >= 2):
                  next_job = wf[0]
                  next_job['wf_results_id'] = job['wf_results_id']
                  next_job['prev_run'] =  job['job_name'] + "#" + context['dag_run'].run_id
                  next_job['root_dag_run'] = job['root_dag_run'] 


        return job   
       
    # @task()
    # def resolve_mappings():
    #     context = get_current_context()
    #     job = context["ti"].xcom_pull(task_ids='get_job_from_workflow', key='return_value')
    #     logging.info("pistis_periodic_workflow#resolve_mappings: Retrieving mappings ... " + str(job))
    #     # if mappings have been defined
    #     if ("mappings" in job): 
    #         #job_list = job['job_name'].split("->")
    #         mappings = job['mappings']
    #         #if (len(job_list) > 1):
    #         logging.info("pistis_periodic_workflow#resolve_mappings: Mappings = " + str(mappings))
    #         # To Do -> manage mappings (update source, ...)

    #     else:        
    #         logging.info("pistis_periodic_workflow#resolve_mappings: No mappings were defined ")

    # @task()
    # def retrieve_component_input_schema(job):
    #     context = get_current_context()
    #      #print("### pistis_periodic_workflow.workflow: wf = "+ str(wf) + " type = " + str(type(wf)))
    #     logging.info("pistis_periodic_workflow#retrieve_component_input_schema: Retrieving Component schema ... \n")

    # @task()
    # def retrieve_component_endpoint():
    #     logging.info("pistis_periodic_workflow#retrieve_component_endpoint: Retrieving Component endpoint ... \n")        

    # @task()
    # def validate_component_input_schema():
    #     logging.info("pistis_periodic_workflow#validate_component_input_schema: Validating Component schema ... \n")    

    # execute dag supporting pistis job

    @task()        
    def generate_conf_for_job_dag():
        context = get_current_context()
        job_data = context["ti"].xcom_pull(task_ids='get_job_from_workflow', key='return_value')
        prev_run = job_data["prev_run"]
        prev_run_updated = prev_run
        prev_run_list = prev_run.split('#')
        prev_job_name = "none"
        wf_res_id = job_data['wf_results_id']
        encryption = context["params"]["encryption"]
        periodicity = context["params"]["periodicity"]
        dataset_category = CAT_PREFIX + context["params"]["dataset_category"]
        dataset_keywords = context["params"]["dataset_keywords"] 

        if (len(prev_run_list) > 0):
            prev_job_name = prev_run_list[0]
            prev_run_id = prev_run_list[1]

        conf_params = {}
        if (job_data['job_name'] != prev_job_name):
            
            logging.info("pistis_periodic_workflow#generate_conf_for_job_dag: New Job = " + str(job_data['job_name']))
            dr_list = DagRun.find(dag_id="pistis_periodic_workflow", run_id=prev_run_id)
            logging.info("pistis_periodic_workflow#generate_conf_for_job_dag: Dag Run with id = " + str(prev_run_id) + " => " + str(dr_list))
            if (len(dr_list) > 0):
                ti = dr_list[0].get_task_instance(task_id='triggerDagRunOperator')
                logging.info("pistis_periodic_workflow#generate_conf_for_job_dag: Dag Task Instance = " + str(ti))
                    
                trigger_run_id = ti.xcom_pull(task_ids='triggerDagRunOperator', key='trigger_run_id')
                prev_run_updated = prev_job_name + "#" + trigger_run_id

                #Update in current job uuid and data_uuid from previoues job
                dr_list = DagRun.find(dag_id="pistis_job_periodic", run_id=trigger_run_id)
                if (len(dr_list) > 0):
                    ti = dr_list[0].get_task_instance(task_id='storage')
                    prev_job_data = ti.xcom_pull(task_ids='storage', key='return_value')
                    logging.info("pistis_periodic_workflow#generate_conf_for_job_dag: Updating UUID and DATA_UUID from job = " + str(prev_job_name))
                    uuid = prev_job_data['uuid']
                    data_uuid = prev_job_data['data_uuid']
        else:
           logging.info("pistis_periodic_workflow#generate_conf_for_job_dag: Updating UUID and DATA_UUID from current job = " + str(context["ti"].xcom_pull(task_ids='get_job_from_workflow', key='return_value')))
           uuid= context["ti"].xcom_pull(task_ids='get_job_from_workflow', key='return_value')['uuid']
           data_uuid= context["ti"].xcom_pull(task_ids='get_job_from_workflow', key='return_value')['data_uuid']            


        conf_params={
             "job_data": {
                 "prev_run": prev_run_updated,
                 "wf_results_id": wf_res_id,
                 "job_name": context["ti"].xcom_pull(task_ids='get_job_from_workflow', key='return_value')['job_name'],
                 "root_dag_run": context["ti"].xcom_pull(task_ids='get_job_from_workflow', key='return_value')['root_dag_run'],
                 "input_data": context["ti"].xcom_pull(task_ids='get_job_from_workflow', key='return_value')['input_data'],
                 "endpoint": context["ti"].xcom_pull(task_ids='get_job_from_workflow', key='return_value')['endpoint'],
                 "content-type": context["ti"].xcom_pull(task_ids='get_job_from_workflow', key='return_value')['content-type'],
                 "source": context["ti"].xcom_pull(task_ids='get_job_from_workflow', key='return_value')['source'],
                 #"metadata": context["ti"].xcom_pull(task_ids='get_job_from_workflow', key='return_value')['metadata'],
                 "method": context["ti"].xcom_pull(task_ids='get_job_from_workflow', key='return_value')['method'],
                 "destination_type": context["ti"].xcom_pull(task_ids='get_job_from_workflow', key='return_value')['destination_type'],
                 "response_dataset_field_path": context["ti"].xcom_pull(task_ids='get_job_from_workflow', key='return_value')['response_dataset_field_path'],
                 "response_metadata_field_path": context["ti"].xcom_pull(task_ids='get_job_from_workflow', key='return_value')['response_metadata_field_path'],
                 #"lineage_tracking": context["ti"].xcom_pull(task_ids='get_job_from_workflow', key='return_value')['lineage_tracking']
                 "is_last_job": context["ti"].xcom_pull(task_ids='get_job_from_workflow', key='return_value')['is_last_job'],
                 "access_token": context["params"]["access_token"],
                 "uuid": uuid,
                 "data_uuid": data_uuid,
                 "encryption": encryption,
                 "periodicity": periodicity,
                 "dataset_category": dataset_category,
                 "dataset_keywords": dataset_keywords
                 }
             }

        return conf_params

    trigger_pistis_job = TriggerDagRunOperator(
              task_id = "triggerDagRunOperator",
              trigger_dag_id="pistis_job_periodic",
              conf={
                     "job_data": "{{ ti.xcom_pull(task_ids='generate_conf_for_job_dag', key='return_value')['job_data'] }}"
              }       
    )


    # trigger_pistis_job = TriggerDagRunOperator(
    #      task_id="pistis_job_triggering",
    #      trigger_dag_id="pistis_job_template",
    #      conf={
    #          "job_data": {
    #              "prev_run": "{{ ti.xcom_pull(task_ids='get_job_from_workflow', key='return_value')['prev_run'] }}",
    #              "root_dag_run": "{{ ti.xcom_pull(task_ids='get_job_from_workflow', key='return_value')['root_dag_run'] }}",
    #              "job_name": "{{ ti.xcom_pull(task_ids='get_job_from_workflow', key='return_value')['job_name'] }}",
    #              "input_data": "{{ ti.xcom_pull(task_ids='get_job_from_workflow', key='return_value')['input_data'] }}",
    #              "content-type": "{{ ti.xcom_pull(task_ids='get_job_from_workflow', key='return_value')['content-type'] }}",
    #              "endpoint": "{{ ti.xcom_pull(task_ids='get_job_from_workflow', key='return_value')['endpoint'] }}",
    #              "source": "{{ ti.xcom_pull(task_ids='get_job_from_workflow', key='return_value')['source'] }}",
    #              "method": "{{ ti.xcom_pull(task_ids='get_job_from_workflow', key='return_value')['method'] }}",
    #              "destination_type": "{{ ti.xcom_pull(task_ids='get_job_from_workflow', key='return_value')['destination_type'] }}"
    #          }
    #      }
    #     )

    # @task()
    #def notify_lineage_tracker():
    #    logging.info("pistis_periodic_workflow#notify_lineage_tracker: Notifiying to lineage tracker ... \n")     

    # @task()
    # def store_dataset_in_fatory_storage(): 
    #     logging.info("pistis_periodic_workflow#store_dataset_in_fatory_storage: Storing dataset in factory storage ... \n")     

        
    @task()
    def get_current_workflow():
        context = get_current_context()
        logging.info("### pistis_periodic_workflow.workflow: wf = "+ str(context["params"]["workflow"]) + " type = " + str(type(context["params"]["workflow"])))
        return context["params"]["workflow"] 
        
    @task.branch
    def check_pending_jobs():
        context = get_current_context()
        wf = context["params"]["workflow"]
        periodicity = context["params"]["periodicity"]

        logging.info("### pistis_periodic_workflow.def check_pending_jobs(): WF = "+ str(wf))
        if (len(wf) > 0):
            return "self_triggering_pistis_workflow"
        else:
            if (periodicity):
                return "periodic_group.build_conf"
            else: 
                return "fingerprint_group.build_fingerprint_conf"
            
    @task_group(group_id='periodic_group')
    def periodic_group():    
        
        @task
        def build_conf():
            context = get_current_context()
            conf = {}
            root_run_id = context["ti"].xcom_pull(task_ids='get_job_from_workflow', key='return_value')['root_dag_run']
            access_token = context["params"]["access_token"]
            periodicity = context["params"]["periodicity"]
            dataset_category = context["params"]["dataset_category"]
            dataset_keywords = context["params"]["dataset_keywords"]
            ds_name = context["params"]["dataset_name"]
            ds_description = context["params"]["dataset_description"]
            trigger_run_id = context["ti"].xcom_pull(task_ids='triggerDagRunOperator', key='trigger_run_id')
            wf = []
            
            ## Add DS name and desscription
            conf["dataset_name"] = ds_name
            conf["dataset_description"] = ds_description

            ## Add access token
            conf["access_token"] = access_token 

            ## Add periodicity
            conf["periodicity"] = periodicity

            ## Add category
            conf["dataset_category"] = dataset_category

            ## Add keywords
            conf["dataset_keywords"] = dataset_keywords

            dr_list = DagRun.find(dag_id="pistis_periodic_workflow", run_id=root_run_id)
            # Add wf and raw_wf: Retrieve initial root wf raw data
            if (len(dr_list) > 0):
                #job_info["source"] = dr_list[0].conf['dataset']
                
                wf = dr_list[0].conf['raw_wf']
                conf["raw_wf"] = wf
                conf["workflow"] = wf
            
            # Add logical_date
            conf["logical_date"] = setup_periodicity_time(periodicity)        

            return json.loads(json.dumps(conf))

        def setup_periodicity_time(periodicity):
            
            start_time = datetime.now()
            if (periodicity == "hourly"):
                start_time = start_time + timedelta(hours=1)
            elif (periodicity == "daily"):
                start_time = start_time + timedelta(days=1)
            elif (periodicity == "montly"):
                start_time = start_time + relativedelta(months=1)
            
            return start_time.isoformat()

        # Self-trigger & Looping in DAG to execute pending jobs
        triggering_pistis_periodic_workflow = TriggerDagRunOperator(
            task_id='triggering_pistis_periodic_workflow',
            trigger_dag_id='pistis_periodic_workflow',
            conf= {"periodicity": "{{ ti.xcom_pull(task_ids='periodic_group.build_conf', key='return_value').periodicity }}", 
                   "workflow": "{{ ti.xcom_pull(task_ids='periodic_group.build_conf', key='return_value').raw_wf }}", 
                   "raw_wf": "{{ ti.xcom_pull(task_ids='periodic_group.build_conf', key='return_value').raw_wf }}", 
                   "access_token": "{{ ti.xcom_pull(task_ids='periodic_group.build_conf', key='return_value').access_token }}",
                   "dataset_name": "{{ ti.xcom_pull(task_ids='periodic_group.build_conf', key='return_value').dataset_name }}",
                   "dataset_description": "{{ ti.xcom_pull(task_ids='periodic_group.build_conf', key='return_value').dataset_description }}",
                   "dataset_category": "{{ ti.xcom_pull(task_ids='periodic_group.build_conf', key='return_value').dataset_category }}",
                   "dataset_keywords": "{{ ti.xcom_pull(task_ids='periodic_group.build_conf', key='return_value').dataset_keywords }}"},
            wait_for_completion=False,
            poke_interval=10,
            execution_date = "{{ ti.xcom_pull(task_ids='periodic_group.build_conf', key='return_value').logical_date }}" 
            #  "{{ ti.xcom_pull(task_ids='get_job_from_workflow', key='return_value').job_id }}"
            #conf={"workflow": "{{ ti.xcom_pull(task_ids='get_current_workflow', key='return_value') }} ", "executed_jobs": "{{ ti.xcom_pull(task_ids='get_job_from_workflow', key='return_value').job_id }}" }
        )         

        build_conf() >> triggering_pistis_periodic_workflow       

    # Self-trigger & Looping in DAG to execute pending jobs
    self_triggering_pistis_workflow = TriggerDagRunOperator(
        task_id='self_triggering_pistis_workflow',
        trigger_dag_id='pistis_periodic_workflow',
        conf={"dataset_category": "{{ ti.xcom_pull(task_ids='generate_conf_for_job_dag', key='return_value').job_data.dataset_category }}", "dataset_keywords": "{{ ti.xcom_pull(task_ids='generate_conf_for_job_dag', key='return_value').job_data.dataset_keywords }}", "encryption": "{{ ti.xcom_pull(task_ids='generate_conf_for_job_dag', key='return_value').job_data.encryption }}", "periodicity": "{{ ti.xcom_pull(task_ids='generate_conf_for_job_dag', key='return_value').job_data.periodicity }}", "workflow": "{{ ti.xcom_pull(task_ids='get_current_workflow', key='return_value') }}", "access_token": "{{ ti.xcom_pull(task_ids='generate_conf_for_job_dag', key='return_value').job_data.access_token }}" },
        wait_for_completion=True,
        poke_interval=10 
        #  "{{ ti.xcom_pull(task_ids='get_job_from_workflow', key='return_value').job_id }}"
        #conf={"workflow": "{{ ti.xcom_pull(task_ids='get_current_workflow', key='return_value') }} ", "executed_jobs": "{{ ti.xcom_pull(task_ids='get_job_from_workflow', key='return_value').job_id }}" }
    )

    @task_group(group_id='fingerprint_group')
    def fingerprint_group(): 

        @task
        def build_fingerprint_conf():
            context = get_current_context()
            conf = {}
            root_run_id = context["ti"].xcom_pull(task_ids='get_job_from_workflow', key='return_value')['root_dag_run']
            trigger_run_id = context["ti"].xcom_pull(task_ids='triggerDagRunOperator', key='trigger_run_id')
            dr_list = DagRun.find(dag_id="pistis_job_periodic", run_id=root_run_id)
                
            if (len(dr_list) > 0):
        
                ti = dr_list[0].get_task_instance(task_id='storage')
                task_result = ti.xcom_pull(task_ids='storage', key='return_value') 
                input_data = task_result["input_data"][0]
                source = input_data["source"]

                ## setup conf
                conf["dataset_id"] = os.path.splitext(os.path.basename(source))[0]
                conf["source"] = source 
        
            return conf
            
        fingerprint_triggering = TriggerDagRunOperator(
                task_id='fingerprint_triggering',
                trigger_dag_id='pistis_fingerprint_dag',
                conf={"dataset_id": "{{ ti.xcom_pull(task_ids='build_fingerprint_conf', key='return_value').conf.dataset_id }}",
                    "source": "{{ ti.xcom_pull(task_ids='build_fingerprint_conf', key='return_value').conf.source }}", 
                    "fingerprint_method": "adhoc_minhash" },
                wait_for_completion=False,
                poke_interval=10,
                #  "{{ ti.xcom_pull(task_ids='get_job_from_workflow', key='return_value').job_id }}"
                #conf={"workflow": "{{ ti.xcom_pull(task_ids='get_current_workflow', key='return_value') }} ", "executed_jobs": "{{ ti.xcom_pull(task_ids='get_job_from_workflow', key='return_value').job_id }}" }
        )

        build_fingerprint_conf() >> fingerprint_triggering

    get_job_from_workflow() >> generate_conf_for_job_dag() >> trigger_pistis_job >> get_current_workflow() >> check_pending_jobs() >> [self_triggering_pistis_workflow, periodic_group(), fingerprint_group()]
        
pistis_periodic_workflow()