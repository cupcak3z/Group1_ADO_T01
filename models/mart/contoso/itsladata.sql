{{ config(materialized='table') }}

select * from {{ ref('completedfullitsla_table') }}