with 
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'FACTITMACHINE_RAW') }}
),

itmachine as (
    select
        -- IDs
        cast(ITMACHINEKEY as numeric) as ITMACHINEKEY_updated,
        cast(MACHINEKEY as numeric) as MACHINEKEY_updated,
        to_char(DATEKEY) as DATEKEY_updated,
        
        -- String
        COSTTYPE,

        -- Amounts
        cast(COSTAMOUNT as numeric) as COSTAMOUNT_updated,

        -- Creation Timings
        to_timestamp_ntz(DATEKEY) as created_at  

    from source
)

select * from itmachine