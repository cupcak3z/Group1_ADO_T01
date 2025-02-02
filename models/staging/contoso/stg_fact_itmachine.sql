-- getting raw data from database
with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'FACTITMACHINE_RAW') }}
),

itmachine as (
    select
        -- IDs
        -- converting data type
        cast(itmachinekey as numeric(38, 0)) as itmachinekey_updated,
        -- converting data type
        cast(machinekey as numeric(38, 0)) as machinekey_updated,
        cast(datekey as date) as datekey_updated, -- converting data type

        -- String
        costtype,

        -- Amounts
        -- converting data type
        cast(costamount as numeric(38, 0)) as costamount_updated,

        -- Creation Timings
        cast(loaddate as timestamp_ntz) as created_at -- converting data type

    from source
)

-- putting the transformed data into a table
select * from itmachine
