-- getting raw data from database
with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMCHANNEL_RAW') }}
),

channel as (
    select
        -- ids
        -- converting data type to ensure correct parsing
        cast(channelkey as numeric(38, 0)) as channelkey_updated,
        -- strings
        channelname,
        channeldescription,
        -- creation timing
        -- converting data type to ensure correct parsing
        cast(loaddate as timestamp_ntz) as created_at
    from source
)

-- putting the transformed data into a table
select * from channel
