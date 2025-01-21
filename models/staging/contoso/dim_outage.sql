with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMOUTAGE_RAW') }}
),

outage as (
    select
        -- ids
        cast(outagekey as numeric(38, 0)) as outagekey_updated,

        -- strings
        outagename,
        outagetype,
        outagesubtype,
        outagesubtypedescription,

        -- creation timing
        cast(loaddate as timestamp_ntz) as created_at

    from source
)

select * from outage
