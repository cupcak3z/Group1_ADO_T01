{{ config(materialized='incremental', unique_key='DATEKEY') }}

SELECT *
FROM ADO_GROUP1_DB_ANALYSIS.CONTOSO.DATE
{% if is_incremental() %}
WHERE CREATED_AT > (SELECT MAX(CREATED_AT) FROM {{ this }})
{% endif %}