from google.cloud import bigquery
from datetime import datetime, timezone
import time

from coinbase_api import CoinbaseAPI   # your class

PROJECT_ID = "crypto-480212"
DATASET = "crypto_raw"
TABLE = "raw_realtime_crypto_trades"

PRODUCTS = ["BTC-USD", "ETH-USD", "BNB-USD", "SOL-USD", "ADA-USD"]

# Poll frequency (choose 10 sec or 15 sec)
REFRESH_SECONDS = 10


def insert_to_bigquery(rows):
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"
    errors = client.insert_rows_json(table_id, rows)

    if errors:
        print("BigQuery Insert Errors:", errors)
    else:
        print(f"Inserted {len(rows)} rows into {table_id}")


def main():
    api = CoinbaseAPI()
    print("Starting REAL-TIME ingestion using REST API...")

    while True:
        now = datetime.now(timezone.utc)
        rows = []

        for p in PRODUCTS:
            try:
                t = api.get_ticker(p)

                row = {
                    "product_id": p,
                    "trade_id": str(t.get("trade_id", None)),
                    "price": float(t.get("price")) if t.get("price") else None,
                    "size": float(t.get("size")) if t.get("size") else None,
                    "best_bid": float(t.get("best_bid")) if t.get("best_bid") else None,
                    "best_ask": float(t.get("best_ask")) if t.get("best_ask") else None,
                    "volume_24h": float(t.get("volume")) if t.get("volume") else None,
                    "side": t.get("side"),
                    "event_time": t.get("time", now.isoformat()),
                    "ingestion_time": now.isoformat(),
                }

                rows.append(row)

            except Exception as e:
                print(f"Error fetching ticker for {p}: {str(e)}")

        if rows:
            insert_to_bigquery(rows)

        time.sleep(REFRESH_SECONDS)


if __name__ == "__main__":
    main()