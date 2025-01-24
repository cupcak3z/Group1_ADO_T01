with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'FACTEXCHANGERATE_RAW') }}
),

exchangerate as (
    select
    --ids
        cast(exchangeratekey as numeric(38, 0)) as exchangeratekey_updated,
        cast(currencykey as numeric(38, 0)) as currencykey_updated,

        --numbers
        cast(averagerate as numeric(38, 5)) as averagerate_updated,
        cast(endofdayrate as numeric(38, 6)) as endofdayrate_updated,

        --date
        cast(datekey as date) as datekey_updated,

        --creation timing
        cast(loaddate as timestamp_ntz) as created_at

    from source
)

select * from exchangerate
