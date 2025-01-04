with
source as(
    select * from {{ source('ADO_GROUP1_DB_RAW', 'FACTEXCHANGERATE_RAW') }}
),

exchangerate as (
    select
    --ids
    cast(EXCHANGERATEKEY as numeric) as EXCHANGERATEKEY_Updated,
    cast(CURRENCYKEY as numeric) as CURRENCYKEY_Updated,

    --numbers
    cast(AVERAGERATE as numeric) as AVERAGERATE_Updated,
    cast(ENDOFDAYRATE as numeric) as ENDOFDAYRATE_Updated,

    --date
    to_char(DATEKEY,'DD/MM/YYYY') as DATEKEY_updated,

    --creation timing
    to_timestamp(DATEKEY) as created_at

    from source
)

SELECT * FROM exchangerate