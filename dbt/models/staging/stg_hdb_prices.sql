with source as (
    select * from {{ source('raw_hdb', 'hdb_resale') }}
),

staged as (
    select
        parse_date('%Y-%m', month)                          as month,
        upper(trim(town))                                   as town,
        upper(trim(flat_type))                              as flat_type,
        trim(block)                                         as block,
        upper(trim(street_name))                            as street_name,
        trim(storey_range)                                  as storey_range,
        safe_cast(floor_area_sqm as float64)                as floor_area_sqm,
        upper(trim(flat_model))                             as flat_model,
        safe_cast(lease_commence_date as int64)             as lease_commence_date,
        trim(remaining_lease)                               as remaining_lease,
        safe_cast(resale_price as float64)                  as resale_price,
        ingested_at
    from source
    where month is not null
      and resale_price is not null
)

select * from staged
