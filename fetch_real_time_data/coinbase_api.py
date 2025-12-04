import requests
import pandas as pd
import time
from datetime import datetime, timedelta


class CoinbaseAPI:

    def __init__(self, base_url="https://api.exchange.coinbase.com", rate_limit=10):
        """
        base_url: Coinbase Exchange REST API base
        rate_limit: number of requests per second allowed (approx)
        """
        self.base_url = base_url
        self.rate_limit = rate_limit
        self.last_request_time = None

    # -----------------------------
    #  Rate Limiter
    # -----------------------------
    def rate_limiter(self):
        """
        Simple rate limiter:
        Coinbase APIs allow good throughput but we stay safe.
        Ensures at least 1/rate_limit seconds between calls.
        """
        if self.last_request_time:
            elapsed = time.time() - self.last_request_time
            wait_time = max(0, 1 / self.rate_limit - elapsed)
            if wait_time > 0:
                time.sleep(wait_time)

        self.last_request_time = time.time()

    # -----------------------------
    #  Get all Coinbase products
    # -----------------------------
    def get_products(self):
        """
        Returns list of trading pairs (BTC-USD, ETH-USD, etc.)
        """
        self.rate_limiter()
        url = f"{self.base_url}/products"
        resp = requests.get(url)

        if resp.status_code == 200:
            return pd.DataFrame(resp.json())
        else:
            raise Exception(f"Error fetching products: {resp.status_code}, {resp.text}")

    # -----------------------------
    #  Get Live Ticker (REAL-TIME PRICE)
    # -----------------------------
    def get_ticker(self, product_id):
        """
        Returns live ticker data for a product (BTC-USD, ETH-USD)
        """
        self.rate_limiter()

        url = f"{self.base_url}/products/{product_id}/ticker"
        resp = requests.get(url)

        if resp.status_code == 200:
            return resp.json()
        else:
            raise Exception(
                f"Error fetching ticker for {product_id}: {resp.status_code}, {resp.text}"
            )

    # -----------------------------
    #  Get Historic Candles (OHLC)
    # -----------------------------
    def get_candles(self, product_id, start, end, granularity):
        """
        Returns OHLC candles:
        [ time, low, high, open, close, volume ]
        """
        self.rate_limiter()

        url = f"{self.base_url}/products/{product_id}/candles"
        params = {"start": start, "end": end, "granularity": granularity}

        resp = requests.get(url, params=params)
        if resp.status_code == 200:
            columns = ["time", "low", "high", "open", "close", "volume"]
            data = resp.json()
            df = pd.DataFrame(data, columns=columns)
            df["time"] = pd.to_datetime(df["time"], unit="s")
            return df.sort_values("time")
        else:
            raise Exception(
                f"Error fetching candles for {product_id}: {resp.status_code}, {resp.text}"
            )

    # -----------------------------
    #  Get Stats (24h metrics)
    # -----------------------------
    def get_stats(self, product_id):
        """
        Returns 24-hour statistics: open, high, low, volume, etc.
        """
        self.rate_limiter()

        url = f"{self.base_url}/products/{product_id}/stats"
        resp = requests.get(url)

        if resp.status_code == 200:
            return resp.json()
        else:
            raise Exception(
                f"Error fetching stats for {product_id}: {resp.status_code}, {resp.text}"
            )