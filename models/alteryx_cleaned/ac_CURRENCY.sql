{{ config(materialized='incremental', unique_key='CURRENCYKEY') }}

SELECT *
FROM ADO_GROUP1_DB_ANALYSIS.CONTOSO.CURRENCY
{% if is_incremental() %}
WHERE CREATED_AT > (SELECT MAX(CREATED_AT) FROM {{ this }})
{% endif %}