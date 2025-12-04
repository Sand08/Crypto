import datetime as dt
import time
import requests
from google.cloud import bigquery

PROJECT_ID = "crypto-480212"
DATASET = "crypto_raw"
TABLE = "raw_historic_crypto_ohlc"

# 5 best cryptos – USD pairs
PRODUCTS = ["BTC-USD", "ETH-USD", "BNB-USD", "SOL-USD", "ADA-USD"]

GRANULARITY = 3600  # 1 hour in seconds
COINBASE_URL = "https://api.exchange.coinbase.com/products/{product_id}/candles"


def get_candles(product_id, start, end):
    params = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "granularity": GRANULARITY,
    }
    print(f"Requesting {product_id} from {start} to {end}")
    resp = requests.get(COINBASE_URL.format(product_id=product_id), params=params)
    print(f"Status {resp.status_code} for {product_id}")
    resp.raise_for_status()
    data = resp.json()
    print(f"Received {len(data)} candles for {product_id}")
    return data


def main():
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"
    print(f"Target table: {table_id}")

    # last 7 days of hourly candles
    days_back = 7
    end = dt.datetime.now(dt.timezone.utc)
    start = end - dt.timedelta(days=days_back)

    rows_to_insert = []

    for product in PRODUCTS:
        # one big window is fine for 7 days of hourly
        candles = get_candles(product, start, end)

        for c in candles:
            # Coinbase format: [ time, low, high, open, close, volume ]
            ts, low, high, open_, close, volume = c
            ts = int(ts)
            timestamp = dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc)

            rows_to_insert.append(
                {
                    "product_id": product,
                    "ts": ts,
                    "timestamp": timestamp.isoformat(),
                    "open": float(open_),
                    "high": float(high),
                    "low": float(low),
                    "close": float(close),
                    "volume": float(volume),
                    "source": "coinbase_rest",
                }
            )

        time.sleep(0.2)

    print(f"Total rows to insert: {len(rows_to_insert)}")

    if not rows_to_insert:
        print("No rows collected – something is wrong with the API or time window.")
        return

    # insert rows
    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        print("BigQuery insertion errors:")
        for err in errors:
            print(err)
    else:
        print(f"Inserted {len(rows_to_insert)} rows into {table_id}")


if __name__ == "__main__":
    main()
