{{ config(materialized = 'view') }}

with hourly as (
    select
      product_id,
      timestamp,
      open,
      high,
      low,
      close,
      volume,
      date(timestamp) as d,

      -- hourly log return
      ln(
        close /
        lag(close) over (
          partition by product_id
          order by timestamp
        )
      ) as log_ret
    from {{ ref('stg_crypto_ohlc') }}
),

daily_enriched as (
    select
      product_id,
      d as date,

      -- ohlc per day
      first_value(open) over (
        partition by product_id, d
        order by timestamp
      ) as daily_open,

      max(high) over (
        partition by product_id, d
      ) as daily_high,

      min(low) over (
        partition by product_id, d
      ) as daily_low,

      last_value(close) over (
        partition by product_id, d
        order by timestamp
        rows between unbounded preceding and unbounded following
      ) as daily_close,

      sum(volume) over (
        partition by product_id, d
      ) as daily_volume,

      -- intra-day volatility from hourly log returns
      stddev_pop(log_ret) over (
        partition by product_id, d
      ) as vol_24h
    from hourly
),

daily_dedup as (
    select distinct
      product_id,
      date,
      daily_open,
      daily_high,
      daily_low,
      daily_close,
      daily_volume,
      vol_24h
    from daily_enriched
),

with_returns as (
    select
      product_id,
      date,
      daily_open,
      daily_high,
      daily_low,
      daily_close,
      daily_volume,
      vol_24h,

      -- 24h return vs previous close
      daily_close
        / lag(daily_close) over (
            partition by product_id
            order by date
          ) - 1
        as return_24h
    from daily_dedup
)

select *
from with_returns
order by product_id, date