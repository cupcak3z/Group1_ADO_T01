{{ config(materialized='incremental', unique_key='SALESTERRITORYKEY') }}

SELECT *
FROM ADO_GROUP1_DB_ANALYSIS.CONTOSO.SALESTERRITORY
{% if is_incremental() %}
WHERE CREATED_AT > (SELECT MAX(CREATED_AT) FROM {{ this }})
   OR UPDATED_AT > (SELECT MAX(UPDATED_AT) FROM {{ this }})
{% endif %}