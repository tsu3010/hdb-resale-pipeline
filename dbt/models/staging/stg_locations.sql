with source as (
    select * from {{ source('raw_hdb', 'hdb_locations') }}
),

staged as (
    select
        upper(trim(street_name))    as street_name,
        latitude,
        longitude,
        geocode_status,
        geocoded_at
    from source
    where geocode_status = 'OK'
)

select * from staged
