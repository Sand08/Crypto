select
  product_id,
  timestamp,      -- already a clean TIMESTAMP
  open,
  high,
  low,
  close,
  volume,
  source
from {{ source('crypto_raw', 'raw_historic_crypto_ohlc') }}
