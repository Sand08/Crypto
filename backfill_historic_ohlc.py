import logging
import time
from datetime import datetime, timedelta, timezone
from typing import List, Dict

import requests
from google.cloud import bigquery

# ───────────────────────── Config ─────────────────────────
PROJECT_ID = "crypto-480212"
DATASET_ID = "crypto_raw"
TABLE_ID = "raw_historic_crypto_ohlc"
TABLE_FQN = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"
INTERVAL = "1h"                 # 1-hour candles
DAYS_BACK = 365                 # 1 year of history
MAX_LIMIT = 1000                # Binance max per request

# Coinbase-style product_id → Binance symbol
SYMBOLS = {
    "BTC-USD": "BTCUSDT",
    "ETH-USD": "ETHUSDT",
    "ADA-USD": "ADAUSDT",
    "SOL-USD": "SOLUSDT",
    "BNB-USD": "BNBUSDT",
}

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger(__name__)


# ───────────────────── Binance fetch helper ─────────────────────
def fetch_ohlc_binance(
    product_id: str,
    symbol: str,
    start: datetime,
    end: datetime,
) -> List[Dict]:
    """
    Fetch 1h OHLC candles from Binance between start and end (UTC) for one symbol.
    Returns list of rows ready for BigQuery.
    """
    rows: List[Dict] = []

    # Binance expects ms timestamps
    current = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)

    while current < end_ms:
        params = {
            "symbol": symbol,
            "interval": INTERVAL,
            "limit": MAX_LIMIT,
            "startTime": current,
        }

        resp = requests.get(BINANCE_KLINES_URL, params=params, timeout=10)
        resp.raise_for_status()
        klines = resp.json()

        if not klines:
            logger.info("  (%s) no more candles returned", product_id)
            break

        for k in klines:
            open_time_ms = k[0]
            open_ = float(k[1])
            high = float(k[2])
            low = float(k[3])
            close = float(k[4])
            volume = float(k[5])

            if open_time_ms >= end_ms:
                break

            ts = datetime.fromtimestamp(open_time_ms / 1000, timezone.utc)

            rows.append(
                {
                    "product_id": product_id,
                    # BigQuery TIMESTAMP accepts RFC3339 string
                    "timestamp": ts.isoformat().replace("+00:00", "Z"),
                    "open": open_,
                    "high": high,
                    "low": low,
                    "close": close,
                    "volume": volume,
                    "source": "binance_1h",
                }
            )

        logger.info("  → fetched %d, total %d", len(klines), len(rows))

        # Move to next hour after last candle we got
        last_open_time_ms = klines[-1][0]
        next_open_time = last_open_time_ms + 60 * 60 * 1000
        if next_open_time <= current:
            # Safety guard to avoid infinite loop
            break
        current = next_open_time

        # Small pause to be nice to Binance & avoid rate limits
        time.sleep(0.2)

    return rows


# ───────────────────── BigQuery load helper ─────────────────────
def load_to_bigquery(rows: List[Dict], batch_size: int = 5000) -> None:
    """
    Stream rows into BigQuery in batches to avoid 413 Request Too Large.
    """
    client = bigquery.Client(project=PROJECT_ID)
    total = len(rows)
    logger.info("Uploading %d rows to %s in batches of %d", total, TABLE_FQN, batch_size)

    for i in range(0, total, batch_size):
        chunk = rows[i : i + batch_size]
        errors = client.insert_rows_json(TABLE_FQN, chunk)

        if errors:
            logger.error("Errors in batch %d–%d: %s", i, i + len(chunk) - 1, errors)
            raise RuntimeError(errors)

        logger.info("  ✅ Uploaded rows %d–%d", i, i + len(chunk) - 1)


# ─────────────────────────── Main ───────────────────────────
def main():
    # Time window: last 365 days, aligned to whole hour
    end = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(days=DAYS_BACK)

    logger.info("📅 Backfilling OHLC from %s to %s (UTC)", start, end)

    all_rows: List[Dict] = []

    for product_id, symbol in SYMBOLS.items():
        logger.info("📡 Fetching OHLC for %s (%s)", product_id, symbol)
        rows = fetch_ohlc_binance(product_id, symbol, start, end)
        logger.info("🎯 %s → fetched %d rows", product_id, len(rows))
        all_rows.extend(rows)

    logger.info("📊 Total rows to upload: %d", len(all_rows))

    if not all_rows:
        logger.warning("No rows to upload, exiting.")
        return

    load_to_bigquery(all_rows)
    logger.info("✅ OHLC backfill completed for all products.")


if __name__ == "__main__":
    main()