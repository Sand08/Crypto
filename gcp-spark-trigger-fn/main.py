import logging
import os

import functions_framework
from google.cloud import dataproc_v1

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ID = os.getenv("GCP_PROJECT", "crypto-480212")
REGION = "us-central1"
CLUSTER_NAME = "crypto-spark-cluster"
MAIN_PY_URI = "gs://crypto-480212-crypto-scripts/gcs_to_bq_crypto.py"  # Spark job


@functions_framework.http
def trigger_crypto_spark_job(request):
    logger.info("🚀 Triggering Dataproc Spark job for crypto GCS → BigQuery")

    job_client = dataproc_v1.JobControllerClient(
        client_options={"api_endpoint": f"{REGION}-dataproc.googleapis.com:443"}
    )

    job = {
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": MAIN_PY_URI,
        },
    }

    result = job_client.submit_job(
        project_id=PROJECT_ID,
        region=REGION,
        job=job,
    )

    job_id = result.reference.job_id
    logger.info("✅ Submitted Spark job with ID: %s", job_id)

    return f"Submitted crypto Spark job: {job_id}", 200