with ret as (
  select * from {{ ref('crypto_all_time_returns') }}
),
vol as (
  select * from {{ ref('crypto_volatility_summary') }}
),
volu as (
  select * from {{ ref('crypto_top_volume_all_time') }}
)

select
  ret.product_id,
  ret.all_time_return_pct,
  vol.volatility,
  volu.total_volume,
  (
    (ret.all_time_return_pct * 0.5) -
    (vol.volatility * 0.2) +
    (volu.total_volume * 0.3)
  ) as overall_score
from ret
left join vol using (product_id)
left join volu using (product_id)
order by overall_score desc