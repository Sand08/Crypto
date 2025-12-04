import json
import logging
import sys
from datetime import datetime, timezone

import functions_framework
from google.cloud import storage

from coinbase_api import CoinbaseAPI

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ID = "crypto-480212"

# ✅ NEW bucket
BUCKET_NAME = "crypto-480212-crypto-rt-data"

# ✅ Put files under crypto_realtime/incoming/
GCS_PREFIX = "crypto_realtime/incoming"

PRODUCTS = ["BTC-USD", "ETH-USD", "BNB-USD", "SOL-USD", "ADA-USD"]


def _fetch_and_write_once():
    logger.info("🚀 Starting crypto realtime fetch → GCS")

    api = CoinbaseAPI()
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(BUCKET_NAME)

    now = datetime.now(timezone.utc)
    iso_now = now.isoformat()
    date_str = now.strftime("%Y-%m-%d")
    ts_str = now.strftime("%Y%m%dT%H%M%S")

    records = []

    for product in PRODUCTS:
        try:
            t = api.get_ticker(product)
            record = {
                "product_id": product,
                "trade_id": str(t.get("trade_id")) if t.get("trade_id") else None,
                "price": float(t["price"]) if t.get("price") else None,
                "size": float(t["size"]) if t.get("size") else None,
                # ✅ use 'bid' and 'ask' from Coinbase API
                "best_bid": float(t["bid"]) if t.get("bid") else None,
                "best_ask": float(t["ask"]) if t.get("ask") else None,
                "volume_24h": float(t["volume"]) if t.get("volume") else None,
                # `side` is not in ticker response, so expect None – optional field
                "side": t.get("side"),
                "event_time": t.get("time", iso_now),
                "ingestion_time": iso_now,
                "source": "coinbase_rest",
            }
            records.append(record)
        except Exception as e:
            logger.error("❌ Error fetching ticker for %s: %s", product, e)

    if not records:
        logger.warning("⚠️ No records fetched, nothing to write.")
        return "no-data"

    blob_name = (
        f"{GCS_PREFIX}/ingestion_date={date_str}/"
        f"crypto_ticker_{ts_str}.json"
    )

    blob = bucket.blob(blob_name)
    blob.upload_from_string(
        data=json.dumps(records),
        content_type="application/json",
    )

    logger.info("📦 Wrote %d records to gs://%s/%s", len(records), BUCKET_NAME, blob_name)
    return "ok"


@functions_framework.http
def fetch_crypto_to_gcs(request):
    try:
        status = _fetch_and_write_once()
        return f"Status: {status}", 200
    except Exception as e:
        logger.error("🔥 Error in fetch_crypto_to_gcs: %s", e, exc_info=True)
        return f"Error: {e}", 500


if __name__ == "__main__":
    _fetch_and_write_once()