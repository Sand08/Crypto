with base as (
  select
    product_id,
    timestamp,
    first_value(close) over (
      partition by product_id
      order by timestamp
    ) as start_price,
    last_value(close) over (
      partition by product_id
      order by timestamp
      rows between unbounded preceding and unbounded following
    ) as latest_price
  from {{ ref('stg_crypto_ohlc') }}
)

select distinct
  product_id,
  start_price,
  latest_price,
  round(((latest_price - start_price) / start_price) * 100, 2) as all_time_return_pct
from base
order by all_time_return_pct desc