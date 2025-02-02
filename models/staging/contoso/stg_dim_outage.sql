-- getting raw data from database
with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMOUTAGE_RAW') }}
),

outage as (
    select
        -- ids
        cast(outagekey as numeric(38, 0)) as outagekey_updated, -- converting data type to ensure correct parsing

        -- strings
        outagename,
        outagetype,
        outagesubtype,
        outagesubtypedescription,

        -- creation timing
        cast(loaddate as timestamp_ntz) as created_at -- converting data type to ensure correct parsing

    from source
)

-- putting the transformed data into a table
select * from outage
