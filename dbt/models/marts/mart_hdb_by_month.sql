{{
    config(materialized='table')
}}

with prices as (
    select * from {{ ref('stg_hdb_prices') }}
),

locations as (
    select * from {{ ref('stg_locations') }}
),

joined as (
    select
        p.month,
        p.town,
        p.flat_type,
        p.floor_area_sqm,
        p.resale_price,
        p.storey_range,
        l.latitude,
        l.longitude
    from prices p
    left join locations l on p.street_name = l.street_name
),

aggregated as (
    select
        month,
        town,
        flat_type,
        count(*)                                                        as transaction_count,
        round(avg(resale_price), 0)                                     as avg_resale_price,
        round(percentile_cont(resale_price, 0.5) over (
            partition by month, town, flat_type), 0)                    as median_resale_price,
        round(avg(resale_price / nullif(floor_area_sqm, 0)), 0)         as avg_price_per_sqm,
        round(avg(floor_area_sqm), 1)                                   as avg_floor_area_sqm,
        avg(latitude)                                                   as latitude,
        avg(longitude)                                                  as longitude
    from joined
    group by month, town, flat_type, resale_price
)

select distinct
    month,
    town,
    flat_type,
    transaction_count,
    avg_resale_price,
    median_resale_price,
    avg_price_per_sqm,
    avg_floor_area_sqm,
    latitude,
    longitude
from aggregated
