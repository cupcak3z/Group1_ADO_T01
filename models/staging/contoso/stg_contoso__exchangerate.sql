with
source as(
    select * from {{ source('ADO_GROUP1_DB_RAW', 'FACTEXCHANGERATE_RAW') }}
),

exchangerate as (
    select
    --ids
    cast(EXCHANGERATEKEY as numeric(38,0)) as EXCHANGERATEKEY_updated,
    cast(CURRENCYKEY as numeric(38,0)) as CURRENCYKEY_updated,

    --numbers
    cast(AVERAGERATE as numeric(38,5)) as AVERAGERATE_updated,
    cast(ENDOFDAYRATE as numeric(38,6)) as ENDOFDAYRATE_updated,

    --date
    cast(DATEKEY as date) as DATEKEY_updated,

    --creation timing
    LOADDATE::timestamp_ntz as created_at  

    from source
)

SELECT * FROM exchangerate