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

from datetime import datetime
from airflow.models import DAG
from airflow.models.param import Param
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context, BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.dagrun import DagRun
import logging
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
                "uuid": "0"
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
        "bearer_token": Param("Access Token", type="string")
    }
)

def pistis_workflow_template():
        
    #Variable.set(key="current_job", value="none")

    @task()
    def get_job_from_workflow():
        job = {}
        context = get_current_context()
        #wf = context["params"]["workflow"]
        wf = context["params"]["workflow"]
        wf_size = len(wf)
        logging.info("### pistis_workflow_template.workflow: wf = "+ str(wf) + " type = " + str(type(wf)))
        if (wf_size > 0):
           job = wf[0]
           #Variable.update(key="current_job", value=job['job_name'])
           logging.info("### pistis_workflow_template.workflow: currrent_job = "+ job['job_name'])

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
    #     logging.info("pistis_workflow_template#resolve_mappings: Retrieving mappings ... " + str(job))
    #     # if mappings have been defined
    #     if ("mappings" in job): 
    #         #job_list = job['job_name'].split("->")
    #         mappings = job['mappings']
    #         #if (len(job_list) > 1):
    #         logging.info("pistis_workflow_template#resolve_mappings: Mappings = " + str(mappings))
    #         # To Do -> manage mappings (update source, ...)

    #     else:        
    #         logging.info("pistis_workflow_template#resolve_mappings: No mappings were defined ")

    # @task()
    # def retrieve_component_input_schema(job):
    #     context = get_current_context()
    #      #print("### pistis_workflow_template.workflow: wf = "+ str(wf) + " type = " + str(type(wf)))
    #     logging.info("pistis_workflow_template#retrieve_component_input_schema: Retrieving Component schema ... \n")

    # @task()
    # def retrieve_component_endpoint():
    #     logging.info("pistis_workflow_template#retrieve_component_endpoint: Retrieving Component endpoint ... \n")        

    # @task()
    # def validate_component_input_schema():
    #     logging.info("pistis_workflow_template#validate_component_input_schema: Validating Component schema ... \n")    

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

        if (len(prev_run_list) > 0):
            prev_job_name = prev_run_list[0]
            prev_run_id = prev_run_list[1]

        conf_params = {}
        if (job_data['job_name'] != prev_job_name):
            
            logging.info("pistis_workflow_template#generate_conf_for_job_dag: New Job = " + str(job_data['job_name']))
            dr_list = DagRun.find(dag_id="pistis_workflow_template", run_id=prev_run_id)
            logging.info("pistis_workflow_template#generate_conf_for_job_dag: Dag Run with id = " + str(prev_run_id) + " => " + str(dr_list))
            if (len(dr_list) > 0):
                ti = dr_list[0].get_task_instance(task_id='triggerDagRunOperator')
                logging.info("pistis_workflow_template#generate_conf_for_job_dag: Dag Task Instance = " + str(ti))
                    
                trigger_run_id = ti.xcom_pull(task_ids='triggerDagRunOperator', key='trigger_run_id')
                prev_run_updated = prev_job_name + "#" + trigger_run_id

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
                 "bearer_token": context["params"]["access_token"]
                 }
             }

        return conf_params

    trigger_pistis_job = TriggerDagRunOperator(
              task_id = "triggerDagRunOperator",
              trigger_dag_id="pistis_job_template",
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
    #    logging.info("pistis_workflow_template#notify_lineage_tracker: Notifiying to lineage tracker ... \n")     

    # @task()
    # def store_dataset_in_fatory_storage(): 
    #     logging.info("pistis_workflow_template#store_dataset_in_fatory_storage: Storing dataset in factory storage ... \n")     

        
    @task()
    def get_current_workflow():
        context = get_current_context()
        logging.info("### pistis_workflow_template.workflow: wf = "+ str(context["params"]["workflow"]) + " type = " + str(type(context["params"]["workflow"])))
        return context["params"]["workflow"] 
        
    @task.branch
    def check_pending_jobs():
        context = get_current_context()
        wf = context["params"]["workflow"]

        logging.info("### pistis_workflow_template.def check_pending_jobs(): WF = "+ str(wf))
        if (len(wf) > 0):
            return "self_triggering_pistis_workflow"
        else: 
            return "skip_self_triggering"

    # Self-trigger & Looping in DAG to execute pending jobs
    self_triggering_pistis_workflow = TriggerDagRunOperator(
        task_id='self_triggering_pistis_workflow',
        trigger_dag_id='pistis_workflow_template',
        conf={"workflow": "{{ ti.xcom_pull(task_ids='get_current_workflow', key='return_value') }}", "access_token": "{{ ti.xcom_pull(task_ids='generate_conf_for_job_dag', key='return_value').job_data.bearer_token }}" },
        wait_for_completion=True,
        poke_interval=10 
        #  "{{ ti.xcom_pull(task_ids='get_job_from_workflow', key='return_value').job_id }}"
        #conf={"workflow": "{{ ti.xcom_pull(task_ids='get_current_workflow', key='return_value') }} ", "executed_jobs": "{{ ti.xcom_pull(task_ids='get_job_from_workflow', key='return_value').job_id }}" }
    )

    skip_self_triggering = EmptyOperator(
        task_id = 'skip_self_triggering'
    )

    get_job_from_workflow() >> generate_conf_for_job_dag() >> trigger_pistis_job >> get_current_workflow() >> check_pending_jobs() >> [self_triggering_pistis_workflow, skip_self_triggering]
        
pistis_workflow_template()