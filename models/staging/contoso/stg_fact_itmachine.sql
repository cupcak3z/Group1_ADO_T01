-- getting raw data from database
with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'FACTITMACHINE_RAW') }}
),

itmachine as (
    select
        -- IDs
        cast(itmachinekey as numeric(38, 0)) as itmachinekey_updated, -- converting data type
        cast(machinekey as numeric(38, 0)) as machinekey_updated, -- converting data type
        cast(datekey as date) as datekey_updated, -- converting data type

        -- String
        costtype,

        -- Amounts
        cast(costamount as numeric(38, 0)) as costamount_updated, -- converting data type

        -- Creation Timings
        cast(loaddate as timestamp_ntz) as created_at -- converting data type

    from source
)

-- putting the transformed data into a table
select * from itmachine
