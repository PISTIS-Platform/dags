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
from datetime import datetime, timedelta
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
        "fingerprint_algorithm": Param(
            "sha256",
            type="string",
            enum=["md5", "sha1", "sha256", "sha512"],
            description="Fingerprint algorithm to use"
        )
    }
)
def pistis_fingerprint_dag():
    """
    DAG that retrieves a dataset from MinIO storage and calculates its fingerprint.
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
    def calculate_fingerprint(dataset_info):
        """
        Calculate fingerprint of the dataset. Uses MinHash signatures for CSV inputs to aid similarity checks, and
        falls back to traditional hashing for other file types.
        """
        context = get_current_context()
        data = dataset_info["data"]
        object_name = dataset_info["object"]

        def _hash_bytes(algo: str, payload: bytes) -> str:
            if algo == "md5":
                return hashlib.md5(payload).hexdigest()
            if algo == "sha1":
                return hashlib.sha1(payload).hexdigest()
            if algo == "sha256":
                return hashlib.sha256(payload).hexdigest()
            if algo == "sha512":
                return hashlib.sha512(payload).hexdigest()
            raise ValueError(f"Unsupported algorithm: {algo}")

        def _minhash_signature(payload: bytes, num_perm: int = 128):
            import csv
            import io
            import random

            PRIME = 4_294_967_311
            rng = random.Random(0)
            hash_functions = [(rng.randint(1, PRIME - 1), rng.randint(0, PRIME - 1)) for _ in range(num_perm)]
            signature = [PRIME] * num_perm

            text_stream = io.StringIO(payload.decode("utf-8", errors="ignore"))
            reader = csv.reader(text_stream)
            row_count = 0

            for row in reader:
                if not row:
                    continue
                normalized = ",".join(cell.strip() for cell in row)
                if not normalized:
                    continue
                row_hash = int(hashlib.sha1(normalized.encode("utf-8")).hexdigest(), 16) % PRIME
                for idx, (a, b) in enumerate(hash_functions):
                    candidate = (a * row_hash + b) % PRIME
                    if candidate < signature[idx]:
                        signature[idx] = candidate
                row_count += 1

            if row_count == 0:
                logging.warning("### CSV dataset is empty; falling back to sha256 fingerprint")
                return _hash_bytes("sha256", payload), "sha256"

            return signature, "minhash"

        try:
            if object_name.lower().endswith(".csv"):
                logging.info("### CSV detected; generating MinHash fingerprint for similarity analysis")
                fingerprint_value, algorithm = _minhash_signature(data)
            else:
                algorithm = context["params"]["fingerprint_algorithm"]
                logging.info(f"### Calculating {algorithm} fingerprint")
                fingerprint_value = _hash_bytes(algorithm, data)

            result = {
                "bucket": dataset_info["bucket"],
                "object": dataset_info["object"],
                "size": dataset_info["size"],
                "algorithm": algorithm,
                "fingerprint": fingerprint_value
            }

            logging.info("### Fingerprint calculated successfully:")
            logging.info(f"###   File: {dataset_info['object']}")
            logging.info(f"###   Size: {dataset_info['size']} bytes")
            logging.info(f"###   Algorithm: {algorithm}")
            if isinstance(fingerprint_value, list):
                logging.info(f"###   Fingerprint signature length: {len(fingerprint_value)}")
                logging.info(f"###   Fingerprint sample: {fingerprint_value[:5]}")
            else:
                logging.info(f"###   Fingerprint: {fingerprint_value}")

            return result

        except Exception as e:
            logging.error(f"### Error calculating fingerprint: {repr(e)}")
            raise Exception(f"Failed to calculate fingerprint: {repr(e)}")

    @task()
    def store_fingerprint_result(fingerprint_result):
        """
        Store the fingerprint result back to MinIO as a JSON file.
        """
        import json
        from io import BytesIO

        context = get_current_context()
        run_id = context['dag_run'].run_id

        logging.info("### Storing fingerprint result to MinIO")

        try:
            # Create result filename
            original_object = fingerprint_result["object"]
            result_object = f"fingerprints/{original_object}.{fingerprint_result['algorithm']}.json"

            # Prepare JSON result
            result_json = {
                "file": fingerprint_result["object"],
                "size": fingerprint_result["size"],
                "algorithm": fingerprint_result["algorithm"],
                "fingerprint": fingerprint_result["fingerprint"],
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

            result_url = f"s3://{MINIO_URL}/{MINIO_BUCKET_NAME}/{result_object}"

            presigned_url = client.presigned_get_object(
                MINIO_BUCKET_NAME,
                result_object,
                expires=timedelta(hours=1)
            )

            logging.info(f"### Fingerprint result stored at: {result_url}")
            logging.info("### Generated presigned URL for fingerprint result")

            return {
                "result_url": result_url,
                "presigned_url": presigned_url,
                "fingerprint": fingerprint_result["fingerprint"],
                "algorithm": fingerprint_result["algorithm"]
            }

        except Exception as e:
            logging.error(f"### Error storing fingerprint result: {repr(e)}")
            raise Exception(f"Failed to store fingerprint result: {repr(e)}")

    # Define task dependencies
    dataset = get_dataset()
    fingerprint = calculate_fingerprint(dataset)
    result = store_fingerprint_result(fingerprint)

    dataset >> fingerprint >> result

pistis_fingerprint_dag()
