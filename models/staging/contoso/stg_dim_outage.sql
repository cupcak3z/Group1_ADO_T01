-- getting raw data from database
with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMOUTAGE_RAW') }}
),

outage as (
    select
        -- ids
        -- converting data type to ensure correct parsing
        cast(outagekey as numeric(38, 0)) as outagekey_updated,

        -- strings
        outagename,
        outagetype,
        outagesubtype,
        outagesubtypedescription,

        -- creation timing
        -- converting data type to ensure correct parsing
        cast(loaddate as timestamp_ntz) as created_at

    from source
)

-- putting the transformed data into a table
select * from outage
