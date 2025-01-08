{{ config(materialized='table') }}

select * from {{ ref('completedfullitmachine_table') }}