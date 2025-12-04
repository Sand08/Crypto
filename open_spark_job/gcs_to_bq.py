import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ----------------- Logging Setup -----------------
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)

# ----------------- Config -----------------
PROJECT_ID = "crypto-480212"
BQ_DATASET = "crypto_raw"
BQ_TABLE = "raw_realtime_crypto_trades"

# GCS path where the Cloud Function writes realtime JSON
GCS_PATH = "gs://crypto-480212-crypto-data/crypto_realtime/*/*.json"


def main():
    logger.info("🚀 Starting GCS → BigQuery load for crypto realtime data")
    logger.info("📂 Reading JSON from: %s", GCS_PATH)

    # SparkSession on Dataproc (BigQuery connector is pre-installed)
    spark = (
        SparkSession.builder.appName("Crypto GCS → BigQuery Full Load")
        .getOrCreate()
    )

    try:
        # ---------- Read JSON from GCS ----------
        df = spark.read.json(GCS_PATH)
    except Exception as e:
        logger.error("❌ Error reading JSON from GCS: %s", e, exc_info=True)
        spark.stop()
        raise

    # ---------- Handle empty data ----------
    if df.rdd.isEmpty():
        logger.warning("⚠️ No data found at %s, exiting without writing to BigQuery.", GCS_PATH)
        spark.stop()
        return

    logger.info("🧪 Sample schema:\n%s", df._jdf.schema().treeString())

    # ---------- Cast timestamps ----------
    # event_time, ingestion_time are strings (ISO); cast them to TIMESTAMP
    if "event_time" in df.columns:
        df = df.withColumn("event_time", col("event_time").cast("timestamp"))
    if "ingestion_time" in df.columns:
        df = df.withColumn("ingestion_time", col("ingestion_time").cast("timestamp"))

    # ---------- Cast numeric fields to DOUBLE ----------
    # BigQuery has these as FLOAT/DOUBLE; we must ensure Parquet is DOUBLE, not STRING.
    numeric_cols = ["best_ask", "best_bid", "price", "size", "volume_24h"]
    for c in numeric_cols:
        if c in df.columns:
            logger.info("🔢 Casting column '%s' to double", c)
            df = df.withColumn(c, col(c).cast("double"))

    # ---------- Align with BigQuery schema ----------
    # Your BQ table crypto_raw.raw_realtime_crypto_trades does NOT have 'source' column.
    # Connector used to fail with: "Cannot add fields (field: source)" → drop it.
    if "source" in df.columns:
        logger.info("🔧 Dropping 'source' column to match BigQuery schema")
        df = df.drop("source")

    full_table_name = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    logger.info("📤 Writing data to BigQuery table: %s", full_table_name)

    try:
        (
            df.write.format("bigquery")
            .option("table", full_table_name)
            .mode("append")   # append new realtime records
            .save()
        )
        logger.info("✅ Data written to BigQuery successfully")
    except Exception as e:
        logger.error("❌ Error occurred while writing to BigQuery: %s", e, exc_info=True)
        raise
    finally:
        logger.info("🛑 Stopping Spark session")
        spark.stop()


if __name__ == "__main__":
    main()