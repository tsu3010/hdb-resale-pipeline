{{
    config(materialized='table')
}}

with prices as (
    select * from {{ ref('stg_hdb_prices') }}
),

-- Latest 24 months of data
recent as (
    select *
    from prices
    where month >= date_sub(date_trunc(current_date(), month), interval 24 month)
),

by_town as (
    select
        town,
        flat_type,
        count(*)                                                        as transaction_count,
        round(avg(resale_price), 0)                                     as avg_resale_price,
        round(approx_quantiles(resale_price, 2)[offset(1)], 0)         as median_resale_price,
        round(avg(floor_area_sqm), 1)                                   as avg_floor_area_sqm,
        round(avg(resale_price / nullif(floor_area_sqm, 0)), 0)         as avg_price_per_sqm
    from recent
    group by town, flat_type
)

select
    town,
    flat_type,
    transaction_count,
    avg_resale_price,
    median_resale_price,
    avg_floor_area_sqm,
    avg_price_per_sqm,
    round(median_resale_price / nullif(avg_floor_area_sqm, 0), 0)       as affordability_index
from by_town
