with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMGEOGRAPHY_RAW')}}
),

geography as (
    select
        -- ids
        cast(GEOGRAPHYKEY as numeric) as GEOGRAPHYKEY_updated,

        -- strings
        GEOGRAPHYTYPE,
        CONTINENTNAME,
        CITYNAME,
        STATEPROVINCENAME,
        REGIONCOUNTRYNAME,

        -- creation timing
        to_timestamp(LOADDATE, 'DD/MM/YYYY HH24:MI') as created_at

    from source
)

select * from geography