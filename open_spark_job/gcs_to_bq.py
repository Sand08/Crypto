import logging
import sys

from pyspark.sql import SparkSession, functions as F, types as T
from google.cloud import storage

# ── Config ──────────────────────────────────────────────────────────────
PROJECT_ID = "crypto-480212"
BUCKET_NAME = "crypto-480212-crypto-rt-data"     # your new bucket
PREFIX = "crypto_realtime"                       # base folder in bucket

BQ_DATASET = "crypto_raw"
BQ_TABLE = "raw_realtime_crypto_trades"
BQ_TABLE_FQN = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)


# ── GCS helpers ─────────────────────────────────────────────────────────
def list_incoming_json_blobs():
    """
    List all JSON blobs under:
      gs://<BUCKET_NAME>/crypto_realtime/incoming/...
    Returns:
      (blobs, uris)
      blobs: list of Blob objects
      uris: list of 'gs://...' strings for Spark to read
    """
    storage_client = storage.Client(project=PROJECT_ID)
    prefix_in = f"{PREFIX}/incoming/"  # crypto_realtime/incoming/
    blobs = list(storage_client.list_blobs(BUCKET_NAME, prefix=prefix_in))

    json_blobs = [b for b in blobs if b.name.endswith(".json")]
    uris = [f"gs://{BUCKET_NAME}/{b.name}" for b in json_blobs]

    logger.info("📂 Found %d JSON files under %s", len(json_blobs), prefix_in)
    return json_blobs, uris


def move_processed_files(blobs):
    """
    Move processed blobs from:
      crypto_realtime/incoming/...  →  crypto_realtime/processed/...
    Keeps the rest of the path (including ingestion_date=...).
    """
    if not blobs:
        logger.info("📁 No blobs to move from incoming → processed.")
        return

    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(BUCKET_NAME)

    prefix_in = f"{PREFIX}/incoming/"
    prefix_out = f"{PREFIX}/processed/"

    moved = 0
    for blob in blobs:
        dest_name = blob.name.replace(prefix_in, prefix_out, 1)
        logger.info("📦 Moving %s → %s", blob.name, dest_name)
        bucket.copy_blob(blob, bucket, dest_name)
        blob.delete()
        moved += 1

    logger.info("✅ Moved %d blobs from incoming → processed.", moved)


# ── Main Spark job ──────────────────────────────────────────────────────
def main():
    logger.info("🚀 Starting GCS → BigQuery load for crypto realtime data")

    # 1) Check if there is anything to read
    blobs, input_paths = list_incoming_json_blobs()
    if not input_paths:
        logger.warning("⚠️ No JSON files found in incoming/, nothing to load.")
        return

    logger.info("📂 Reading JSON from:")
    for p in input_paths:
        logger.info("   • %s", p)

    spark = (
        SparkSession.builder
        .appName("Crypto GCS → BigQuery Full Load")
        .getOrCreate()
    )

    try:
        df = spark.read.json(input_paths)

        logger.info("🧪 Sample schema:")
        df.printSchema()

        # Cast numeric fields that might be strings
        num_cols = ["price", "size", "best_bid", "best_ask", "volume_24h"]
        for c in num_cols:
            if c in df.columns:
                df = df.withColumn(c, F.col(c).cast(T.DoubleType()))

        # Parse timestamps
        if "event_time" in df.columns:
            df = df.withColumn("event_time", F.to_timestamp("event_time"))
        if "ingestion_time" in df.columns:
            df = df.withColumn("ingestion_time", F.to_timestamp("ingestion_time"))

        # Only send columns that exist in BQ table
        wanted_cols = [
            "product_id",
            "price",
            "best_bid",
            "best_ask",
            "size",
            "volume_24h",
            "side",
            "event_time",
            "ingestion_time",
            "trade_id",
        ]
        available = [c for c in wanted_cols if c in df.columns]
        df = df.select(*available)

        logger.info("📤 Writing data to BigQuery table: %s", BQ_TABLE_FQN)

        (
            df.write.format("bigquery")
            .option("table", BQ_TABLE_FQN)
            .option("writeMethod", "direct")
            .mode("append")
            .save()
        )

        logger.info("✅ Finished writing to BigQuery, now moving files...")
        move_processed_files(blobs)

    except Exception as e:
        logger.error("❌ Error occurred while writing to BigQuery: %s", e, exc_info=True)
        raise
    finally:
        logger.info("🛑 Stopping Spark session")
        spark.stop()


if __name__ == "__main__":
    main()