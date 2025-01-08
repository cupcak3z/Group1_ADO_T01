{{ config(materialized='table') }}

select * from {{ ref('completedfullexchangerate_table') }}