with
source as(
    select * from {{ source('ADO_GROUP1_DB_RAW', 'FACTEXCHANGERATE_RAW') }}
),

exchangerate as (
    select
    --ids
    cast(EXCHANGERATEKEY as numeric) as EXCHANGERATEKEY_updated,
    cast(CURRENCYKEY as numeric) as CURRENCYKEY_updated,

    --numbers
    cast(AVERAGERATE as numeric) as AVERAGERATE_updated,
    cast(ENDOFDAYRATE as numeric) as ENDOFDAYRATE_updated,

    --date
    cast(DATEKEY as date) as DATEKEY_updated,

    --creation timing
    to_timestamp(DATEKEY) as created_at

    from source
)

SELECT * FROM exchangerate