with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMOUTAGE_RAW')}}
),

outage as (
    select
        -- ids
        cast(OUTAGEKEY as numeric) as OUTAGEKEY_updated,

        -- strings
        OUTAGENAME,
        OUTAGETYPE,
        OUTAGESUBTYPE,
        OUTAGESUBTYPEDESCRIPTION,

        -- creation timing
        LOADDATE::timestamp_ntz as created_at

    from source
)

select * from outage