with ohlc as (
  select * from {{ ref('stg_crypto_ohlc') }}
),
rt_agg as (
  select
    product_id,
    timestamp_trunc(event_time, minute) as ts_minute,
    avg(price)     as avg_price,
    avg(best_bid)  as avg_bid,
    avg(best_ask)  as avg_ask,
    sum(size)      as traded_volume
  from {{ ref('stg_crypto_realtime') }}
  group by product_id, ts_minute
)

select
  o.product_id,
  o.timestamp,
  o.open,
  o.high,
  o.low,
  o.close,
  o.volume,
  r.avg_price,
  r.avg_bid,
  r.avg_ask,
  r.traded_volume
from ohlc o
left join rt_agg r
  on o.product_id = r.product_id
 and o.timestamp  = r.ts_minute
