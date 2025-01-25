with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMCHANNEL_RAW') }}
),

channel as (
    select
        -- ids
        cast(channelkey as numeric(38, 0)) as channelkey_updated,
        -- strings
        channelname,
        channeldescription,
        -- creation timing
        cast(loaddate as timestamp_ntz) as created_at
    from source
)

select * from channel
