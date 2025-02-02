-- getting raw data from database
with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMGEOGRAPHY_RAW') }}
),

geography as (
    select
        -- ids
        cast(geographykey as numeric(38, 0)) as geographykey_updated, -- converting data type to ensure correct parsing

        -- strings
        geographytype,
        continentname,
        cityname,
        stateprovincename,
        regioncountryname,

        -- creation timing
        to_timestamp(loaddate) as created_at -- converting data type to ensure correct parsing

    from source
)

-- putting the transformed data into a table
select * from geography
