with 
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'FACTITMACHINE_RAW') }}
),

itmachine as (
    select
        -- IDs
        cast(ITMACHINEKEY as numeric(38,0)) as ITMACHINEKEY_updated,
        cast(MACHINEKEY as numeric(38,0)) as MACHINEKEY_updated,
        to_char(DATEKEY) as DATEKEY_updated,
        
        -- String
        COSTTYPE,

        -- Amounts
        cast(COSTAMOUNT as numeric(38,0)) as COSTAMOUNT_updated,

        -- Creation Timings
        to_timestamp_ntz(DATEKEY) as created_at  

    from source
)

select * from itmachine