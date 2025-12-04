select
  product_id,
  price,
  size,
  best_bid,
  best_ask,
  volume_24h,
  side,
  event_time,
  ingestion_time
from {{ source('crypto_raw', 'raw_realtime_crypto_trades') }}
