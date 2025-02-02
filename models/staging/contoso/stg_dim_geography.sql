-- getting raw data from database
with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMGEOGRAPHY_RAW') }}
),

geography as (
    select
        -- ids
        -- converting data type to ensure correct parsing
        cast(geographykey as numeric(38, 0)) as geographykey_updated,

        -- strings
        geographytype,
        continentname,
        cityname,
        stateprovincename,
        regioncountryname,

        -- creation timing
        -- converting data type to ensure correct parsing
        to_timestamp(loaddate) as created_at

    from source
)

-- putting the transformed data into a table
select * from geography
