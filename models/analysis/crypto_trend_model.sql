{{ config(materialized='view') }}

with base as (
    select *
    from {{ ref('crypto_daily_metrics') }}
),

ma_calc as (
    select
      product_id,
      date,
      daily_close,
      daily_volume,
      return_24h,
      vol_24h,

      -- Moving averages
      avg(daily_close) over (
        partition by product_id
        order by date
        rows between 6 preceding and current row
      )  as ma_7,

      avg(daily_close) over (
        partition by product_id
        order by date
        rows between 29 preceding and current row
      )  as ma_30,

      avg(daily_close) over (
        partition by product_id
        order by date
        rows between 99 preceding and current row
      )  as ma_100
    from base
),

trend as (
    select
      product_id,
      date,
      daily_close,
      daily_volume,
      return_24h,
      vol_24h,
      ma_7,
      ma_30,
      ma_100,

      -- Price relative to moving averages
      daily_close > ma_7   as above_ma_7,
      daily_close > ma_30  as above_ma_30,
      daily_close > ma_100 as above_ma_100,

      -- Trend classification
      case
        when daily_close > ma_7 and ma_7 > ma_30 then 'strong_uptrend'
        when daily_close < ma_7 and ma_7 < ma_30 then 'strong_downtrend'
        else 'sideways'
      end as trend_regime,

      -- Distance from MA-30
      (daily_close - ma_30) / nullif(ma_30, 0) as distance_from_ma_30
    from ma_calc
)

select *
from trend
order by product_id, date