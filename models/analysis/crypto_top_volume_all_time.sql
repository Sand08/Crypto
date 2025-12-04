select
  product_id,
  sum(volume) as total_volume
from {{ ref('stg_crypto_ohlc') }}
group by product_id
order by total_volume desc