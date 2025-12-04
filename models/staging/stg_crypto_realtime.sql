-- models/staging/stg_crypto_realtime.sql
select
  product_id,
  safe_cast(price as float64)      as price,
  safe_cast(size as float64)       as size,
  safe_cast(best_bid as float64)   as best_bid,
  safe_cast(best_ask as float64)   as best_ask,
  safe_cast(volume_24h as float64) as volume_24h,
  side,
  TIMESTAMP_SECONDS(event_time) as event_time,  -- if event_time is unix seconds
  ingestion_time
from {{ source('crypto_raw', 'raw_realtime_crypto_trades') }}