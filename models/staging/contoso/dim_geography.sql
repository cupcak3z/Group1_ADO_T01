with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMGEOGRAPHY_RAW')}}
),

geography as (
    select
        -- ids
        cast(GEOGRAPHYKEY as numeric(38,0)) as GEOGRAPHYKEY_updated,

        -- strings
        GEOGRAPHYTYPE,
        CONTINENTNAME,
        CITYNAME,
        STATEPROVINCENAME,
        REGIONCOUNTRYNAME,

        -- creation timing
        to_timestamp(LOADDATE) as created_at

    from source
)

select * from geography