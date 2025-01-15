with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMCHANNEL_RAW')}}
),

channel as (
    select
        -- ids
        cast(CHANNELKEY as numeric(38,0)) as CHANNELKEY_updated,
        -- strings
        CHANNELNAME,
        CHANNELDESCRIPTION,
        -- creation timing
        LOADDATE::timestamp_ntz as created_at
    from source
)

select * from channel