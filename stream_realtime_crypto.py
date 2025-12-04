import json
import time
import datetime as dt

from websocket import WebSocketApp
from google.cloud import bigquery

PROJECT_ID = "crypto-480212"
DATASET = "crypto_raw"
TABLE = "raw_realtime_crypto_trades"

# 5 cryptos – USD pairs
PRODUCTS = ["BTC-USD", "ETH-USD", "BNB-USD", "SOL-USD", "ADA-USD"]

WS_URL = "wss://ws-feed.exchange.coinbase.com"


class CoinbaseStreamer:
    def __init__(self):
        self.client = bigquery.Client(project=PROJECT_ID)
        self.table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"
        self.buffer = []
        self.buffer_size = 50  # how many messages before flush to BQ

    # ------------- BigQuery insert -------------

    def flush_buffer(self):
        if not self.buffer:
            return
        errors = self.client.insert_rows_json(self.table_id, self.buffer)
        if errors:
            print("BQ insert errors:", errors)
        else:
            print(f"Inserted {len(self.buffer)} rows to {self.table_id}")
        self.buffer = []

    # ------------- WebSocket callbacks -------------

    def on_open(self, ws):
        print("WebSocket opened, subscribing to ticker…")
        sub_msg = {
            "type": "subscribe",
            "channels": [{"name": "ticker", "product_ids": PRODUCTS}],
        }
        ws.send(json.dumps(sub_msg))

    def on_message(self, ws, message):
        msg = json.loads(message)

        # ignore non-ticker messages
        if msg.get("type") != "ticker":
            return

        product_id = msg.get("product_id")
        price = msg.get("price")
        size = msg.get("last_size")
        best_bid = msg.get("best_bid")
        best_ask = msg.get("best_ask")
        volume_24h = msg.get("volume_24h")
        trade_id = msg.get("trade_id")
        side = msg.get("side")
        time_str = msg.get("time")  # ISO8601

        # parse ISO timestamp -> Python datetime
        event_time = None
        if time_str:
            event_time = dt.datetime.fromisoformat(time_str.replace("Z", "+00:00"))

        ingestion_time = dt.datetime.now(dt.timezone.utc)

        row = {
            "product_id": product_id,
            "trade_id": str(trade_id) if trade_id is not None else None,
            "price": float(price) if price is not None else None,
            "size": float(size) if size is not None else None,
            "best_bid": float(best_bid) if best_bid is not None else None,
            "best_ask": float(best_ask) if best_ask is not None else None,
            "volume_24h": float(volume_24h) if volume_24h is not None else None,
            "side": side,
            "event_time": event_time.isoformat() if event_time else None,
            "ingestion_time": ingestion_time.isoformat(),
        }

        self.buffer.append(row)

        if len(self.buffer) >= self.buffer_size:
            self.flush_buffer()

    def on_error(self, ws, error):
        print("WebSocket error:", error)

    def on_close(self, ws, close_status_code, close_msg):
        print("WebSocket closed:", close_status_code, close_msg)
        # flush anything left
        self.flush_buffer()

    # ------------- Runner -------------

    def run_forever(self):
        ws = WebSocketApp(
            WS_URL,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )

        while True:
            try:
                print("Connecting to Coinbase WebSocket…")
                ws.run_forever()
            except KeyboardInterrupt:
                print("Stopping streamer…")
                break
            except Exception as e:
                print("Unexpected error, retrying in 5s:", e)
                time.sleep(5)


if __name__ == "__main__":
    streamer = CoinbaseStreamer()
    streamer.run_forever()