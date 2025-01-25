with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'FACTITMACHINE_RAW') }}
),

itmachine as (
    select
        -- IDs
        cast(itmachinekey as numeric(38, 0)) as itmachinekey_updated,
        cast(machinekey as numeric(38, 0)) as machinekey_updated,
        cast(datekey as date) as datekey_updated,

        -- String
        costtype,

        -- Amounts
        cast(costamount as numeric(38, 0)) as costamount_updated,

        -- Creation Timings
        cast(loaddate as timestamp_ntz) as created_at

    from source
)

select * from itmachine
