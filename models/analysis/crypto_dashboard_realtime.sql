{{ config(materialized = 'view') }}

with latest_rt as (
    -- latest tick per product
    select *
    from {{ ref('crypto_realtime_snapshot') }}
),

latest_daily as (
    select *
    from {{ ref('crypto_daily_metrics') }}
    qualify row_number() over (
        partition by product_id
        order by date desc
    ) = 1
),

latest_trend as (
    select *
    from {{ ref('crypto_trend_model') }}
    qualify row_number() over (
        partition by product_id
        order by date desc
    ) = 1
)

select
    rt.product_id,

    -- realtime
    rt.event_time,
    rt.ingestion_time,
    rt.price          as last_price,
    rt.best_bid,
    rt.best_ask,
    rt.spread_abs,
    rt.spread_pct,
    rt.volume_24h,
    rt.side,
    rt.last_trade_size,

    -- daily metrics
    d.date            as last_ohlc_ts,
    d.daily_close     as last_daily_close,
    d.return_24h      as change_pct_24h,
    d.vol_24h         as vol_24h,
    d.daily_volume    as daily_volume_24h,

    -- trend info
    t.ma_7,
    t.ma_30,
    t.ma_100,
    t.trend_regime,
    t.distance_from_ma_30
from latest_rt rt
left join latest_daily d
  on rt.product_id = d.product_id
left join latest_trend t
  on rt.product_id = t.product_id