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

import hashlib
import logging
from datetime import datetime
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.models import Variable
from minio import Minio

@dag(
    start_date=datetime(2023, 1, 1),
    schedule="@once",
    catchup=False,
    params={
        "source": Param(
            "s3://dataset/path/to/file.csv",
            type="string",
            description="S3 path to the dataset (format: s3://minio_url/bucket/object)"
        ),
        "checksum_algorithm": Param(
            "sha256",
            type="string",
            enum=["md5", "sha1", "sha256", "sha512"],
            description="Checksum algorithm to use"
        )
    }
)
def pistis_fingerprint_dag():
    """
    DAG that retrieves a dataset from MinIO storage and calculates its checksum.
    """

    MINIO_BUCKET_NAME = Variable.get("minio_pistis_bucket_api_key")
    MINIO_ROOT_USER = Variable.get("minio_api_key")
    MINIO_ROOT_PASSWORD = Variable.get("minio_passwd")
    MINIO_URL = Variable.get("minio_url")

    client = Minio(MINIO_URL, access_key=MINIO_ROOT_USER, secret_key=MINIO_ROOT_PASSWORD, secure=False)

    @task()
    def get_dataset():
        """
        Retrieve dataset from MinIO storage.
        """
        context = get_current_context()
        source = context["params"]["source"]

        logging.info(f"### Retrieving dataset from: {source}")

        try:
            # Parse S3 path
            s3_path = source[len("s3://" + MINIO_URL + "/"):]
            s3_list = s3_path.split('/')

            if len(s3_list) < 2:
                raise ValueError(f"Invalid S3 path format: {source}")

            bucket_name = s3_list[0]
            object_name = '/'.join(s3_list[1:])

            logging.info(f"### Bucket: {bucket_name}, Object: {object_name}")

            # Get object from MinIO
            response = client.get_object(bucket_name, object_name)
            data = response.read()
            response.close()
            response.release_conn()

            logging.info(f"### Successfully retrieved {len(data)} bytes")

            return {
                "data": data,
                "bucket": bucket_name,
                "object": object_name,
                "size": len(data)
            }

        except Exception as e:
            logging.error(f"### Error retrieving dataset: {repr(e)}")
            raise Exception(f"Failed to retrieve dataset: {repr(e)}")

    @task()
    def calculate_checksum(dataset_info):
        """
        Calculate checksum of the dataset using the specified algorithm.
        """
        context = get_current_context()
        algorithm = context["params"]["checksum_algorithm"]

        logging.info(f"### Calculating {algorithm} checksum")

        try:
            data = dataset_info["data"]

            # Calculate checksum based on algorithm
            if algorithm == "md5":
                checksum = hashlib.md5(data).hexdigest()
            elif algorithm == "sha1":
                checksum = hashlib.sha1(data).hexdigest()
            elif algorithm == "sha256":
                checksum = hashlib.sha256(data).hexdigest()
            elif algorithm == "sha512":
                checksum = hashlib.sha512(data).hexdigest()
            else:
                raise ValueError(f"Unsupported algorithm: {algorithm}")

            result = {
                "bucket": dataset_info["bucket"],
                "object": dataset_info["object"],
                "size": dataset_info["size"],
                "algorithm": algorithm,
                "checksum": checksum
            }

            logging.info(f"### Checksum calculated successfully:")
            logging.info(f"###   File: {dataset_info['object']}")
            logging.info(f"###   Size: {dataset_info['size']} bytes")
            logging.info(f"###   Algorithm: {algorithm}")
            logging.info(f"###   Checksum: {checksum}")

            return result

        except Exception as e:
            logging.error(f"### Error calculating checksum: {repr(e)}")
            raise Exception(f"Failed to calculate checksum: {repr(e)}")

    @task()
    def store_checksum_result(checksum_result):
        """
        Store the checksum result back to MinIO as a JSON file.
        """
        import json
        from io import BytesIO

        context = get_current_context()
        run_id = context['dag_run'].run_id

        logging.info("### Storing checksum result to MinIO")

        try:
            # Create result filename
            original_object = checksum_result["object"]
            result_object = f"checksums/{original_object}.{checksum_result['algorithm']}.json"

            # Prepare JSON result
            result_json = {
                "file": checksum_result["object"],
                "size": checksum_result["size"],
                "algorithm": checksum_result["algorithm"],
                "checksum": checksum_result["checksum"],
                "calculated_at": datetime.utcnow().isoformat(),
                "dag_run_id": run_id
            }

            # Convert to bytes
            json_data = json.dumps(result_json, indent=2).encode('utf-8')

            # Store in MinIO
            result = client.put_object(
                MINIO_BUCKET_NAME,
                result_object,
                data=BytesIO(json_data),
                length=len(json_data),
                content_type='application/json'
            )

            result_url = f"s3://{MINIO_URL}/{MINIO_BUCKET_NAME}/{result.object_name}"

            logging.info(f"### Checksum result stored at: {result_url}")

            return {
                "result_url": result_url,
                "checksum": checksum_result["checksum"],
                "algorithm": checksum_result["algorithm"]
            }

        except Exception as e:
            logging.error(f"### Error storing checksum result: {repr(e)}")
            raise Exception(f"Failed to store checksum result: {repr(e)}")

    # Define task dependencies
    dataset = get_dataset()
    checksum = calculate_checksum(dataset)
    result = store_checksum_result(checksum)

    dataset >> checksum >> result

pistis_fingerprint_dag()
