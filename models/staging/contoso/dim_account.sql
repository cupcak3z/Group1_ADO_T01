with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'DIMACCOUNT_RAW')}}
),

account as (
    select
        -- ids
        cast (ACCOUNTKEY as numeric(38,0)) as ACCOUNTKEY_updated,
        case
            when PARENTACCOUNTKEY = 'NULL' then '1'
            else cast(PARENTACCOUNTKEY as numeric(38,0))
        end as PARENTACCOUNTKEY_updated,

        -- strings
        ACCOUNTNAME,
        ACCOUNTDESCRIPTION,
        ACCOUNTTYPE,
        VALUETYPE,
        case
            when ACCOUNTTYPE = 'NULL' then 'Income'
            else ACCOUNTTYPE
        end as ACCOUNTTYPE_updated,
        
        case
            when VALUETYPE = 'NULL' then 'Income'
            else VALUETYPE
        end as VALUETYPE_updated,

        -- creation timing
        LOADDATE::timestamp_ntz as created_at
    from source
)

select * from account