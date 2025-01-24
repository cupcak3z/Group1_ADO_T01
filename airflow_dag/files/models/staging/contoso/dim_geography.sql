with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMGEOGRAPHY_RAW') }}
),

geography as (
    select
        -- ids
        cast(geographykey as numeric(38, 0)) as geographykey_updated,

        -- strings
        geographytype,
        continentname,
        cityname,
        stateprovincename,
        regioncountryname,

        -- creation timing
        to_timestamp(loaddate) as created_at

    from source
)

select * from geography
