{{ config(materialized='table') }}

select * from {{ ref('completedfullsales_table') }}