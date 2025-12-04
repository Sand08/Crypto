with returns as (
  select
    product_id,
    timestamp,
    close,
    lag(close) over (partition by product_id order by timestamp) as prev_close
  from {{ ref('stg_crypto_ohlc') }}
),
calc as (
  select
    product_id,
    (close - prev_close) / prev_close as hourly_return
  from returns
  where prev_close is not null
)

select
  product_id,
  round(stddev_samp(hourly_return), 6) as volatility
from calc
group by product_id
order by volatility desc