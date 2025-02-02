-- getting raw data from database
with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'FACTEXCHANGERATE_RAW') }}
),

exchangerate as (
    select
    --ids
        cast(exchangeratekey as numeric(38, 0)) as exchangeratekey_updated, -- converting data type
        cast(currencykey as numeric(38, 0)) as currencykey_updated, -- converting data type

        --numbers
        cast(averagerate as numeric(38, 5)) as averagerate_updated, -- converting data type
        cast(endofdayrate as numeric(38, 6)) as endofdayrate_updated, -- converting data type

        --date
        cast(datekey as date) as datekey_updated, -- converting data type

        --creation timing
        cast(loaddate as timestamp_ntz) as created_at -- converting data type

    from source
)

-- putting the transformed data into a table
select * from exchangerate
