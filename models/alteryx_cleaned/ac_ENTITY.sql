{{ config(materialized='incremental', unique_key='ENTITYKEY') }}

SELECT *
FROM ADO_GROUP1_DB_ANALYSIS.CONTOSO.ENTITY
{% if is_incremental() %}
WHERE CREATED_AT > (SELECT MAX(CREATED_AT) FROM {{ this }})
   OR UPDATED_AT > (SELECT MAX(UPDATED_AT) FROM {{ this }})
{% endif %}