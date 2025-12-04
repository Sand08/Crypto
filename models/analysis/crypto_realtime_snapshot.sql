{{ config(materialized='view') }}

with latest as (
    select *
    from {{ ref('stg_crypto_realtime') }}
    qualify row_number() over (
        partition by product_id
        order by event_time desc
    ) = 1
)

select
    product_id,
    event_time,
    ingestion_time,
    price,
    best_bid,
    best_ask,
    (best_ask - best_bid)                          as spread_abs,
    (best_ask - best_bid) / nullif(price, 0)       as spread_pct,
    volume_24h,
    side,
    size                                           as last_trade_size
from latest