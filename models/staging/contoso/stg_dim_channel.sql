-- getting raw data from database
with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMCHANNEL_RAW') }}
),

channel as (
    select
        -- ids
        cast(channelkey as numeric(38, 0)) as channelkey_updated, -- converting data type to ensure correct parsing
        -- strings
        channelname,
        channeldescription,
        -- creation timing
        cast(loaddate as timestamp_ntz) as created_at -- converting data type to ensure correct parsing
    from source
)

-- putting the transformed data into a table
select * from channel
