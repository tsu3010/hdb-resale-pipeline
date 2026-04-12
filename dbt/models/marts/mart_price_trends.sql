{{
    config(materialized='table')
}}

with prices as (
    select * from {{ ref('stg_hdb_prices') }}
),

quarterly as (
    select
        date_trunc(month, quarter)                                      as quarter,
        town,
        flat_type,
        count(*)                                                        as transaction_count,
        round(avg(resale_price), 0)                                     as avg_resale_price,
        round(approx_quantiles(resale_price, 2)[offset(1)], 0)         as median_resale_price
    from prices
    group by quarter, town, flat_type
),

with_lag as (
    select
        quarter,
        town,
        flat_type,
        transaction_count,
        avg_resale_price,
        median_resale_price,
        lag(median_resale_price) over (
            partition by town, flat_type
            order by quarter
        )                                                               as prev_quarter_median
    from quarterly
)

select
    quarter,
    town,
    flat_type,
    transaction_count,
    avg_resale_price,
    median_resale_price,
    prev_quarter_median,
    round(
        safe_divide(median_resale_price - prev_quarter_median, prev_quarter_median) * 100,
        2
    )                                                                   as qoq_change_pct
from with_lag
