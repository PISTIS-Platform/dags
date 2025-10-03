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
        "fingerprint_method": Param(
            "adhoc_minhash",
            type="string",
            enum=["adhoc_minhash", "datasketch_minhash"],
            description="Fingerprint method to use for CSV datasets"
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
        falls back to doing nothing for other file types.
        """
        context = get_current_context()
        data = dataset_info["data"]
        object_name = dataset_info["object"]

        def _adhoc_minhash(payload: bytes, num_perm: int = 128):
            import csv
            import io
            import random

            PRIME = 4_294_967_311  # Large prime ensures hashing works well with modular arithmetic
            rng = random.Random(0)
            hash_functions = [(rng.randint(1, PRIME - 1), rng.randint(0, PRIME - 1)) for _ in range(num_perm)]
            signature = [PRIME] * num_perm

            text_stream = io.StringIO(payload.decode("utf-8", errors="ignore"))
            reader = csv.reader(text_stream)
            rows_processed = 0

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
                rows_processed += 1

            if rows_processed == 0:
                logging.warning("### CSV dataset had no rows; returning default MinHash signature")

            return signature, "adhoc_minhash"

        def _datasketch_minhash(payload: bytes, num_perm: int = 128):
            import csv
            import io
            try:
                from datasketch import MinHash
            except ImportError as exc:
                raise ImportError(
                    "datasketch is required for the 'datasketch_minhash' method. Install datasketch to enable it."
                ) from exc

            text_stream = io.StringIO(payload.decode("utf-8", errors="ignore"))
            reader = csv.reader(text_stream)
            minhash = MinHash(num_perm=num_perm)
            rows_processed = 0

            for row in reader:
                if not row:
                    continue
                normalized = ",".join(cell.strip() for cell in row)
                if not normalized:
                    continue
                minhash.update(normalized.encode("utf-8"))
                rows_processed += 1

            if rows_processed == 0:
                logging.warning("### CSV dataset had no rows; returning default datasketch MinHash signature")

            return minhash.hashvalues.tolist(), "datasketch_minhash"

        try:
            if not object_name.lower().endswith(".csv"):
                logging.info("### Non-CSV dataset detected; skipping fingerprint calculation")
                return None

            method = context["params"].get("fingerprint_method", "adhoc_minhash")

            if method == "datasketch_minhash":
                fingerprint_value, algorithm = _datasketch_minhash(data)
            elif method == "adhoc_minhash":
                fingerprint_value, algorithm = _adhoc_minhash(data)
            else:
                raise ValueError(f"Unsupported fingerprint method: {method}")

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
            logging.info(f"###   Method: {algorithm}")
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

        if not fingerprint_result:
            logging.info("### No fingerprint result supplied; skipping storage")
            return None

        fingerprint_value = fingerprint_result.get("fingerprint")
        algorithm = fingerprint_result.get("algorithm")

        if algorithm is None or fingerprint_value is None:
            logging.info("### Fingerprint data missing; skipping storage")
            return None

        try:
            # Create result filename
            original_object = fingerprint_result["object"]
            result_object = f"fingerprints/{original_object}.{algorithm}.json"

            # Prepare JSON result
            result_json = {
                "file": fingerprint_result["object"],
                "size": fingerprint_result["size"],
                "algorithm": algorithm,
                "fingerprint": fingerprint_value,
                "calculated_at": datetime.utcnow().isoformat(),
                "dag_run_id": run_id
            }

            # Convert to bytes
            json_data = json.dumps(result_json, indent=2).encode('utf-8')

            # Store in MinIO
            _ = client.put_object(
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
                "fingerprint": fingerprint_value,
                "algorithm": algorithm
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
