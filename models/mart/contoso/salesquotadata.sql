{{ config(materialized='table') }}

select * from {{ ref('completedfullsalesquota_table') }}