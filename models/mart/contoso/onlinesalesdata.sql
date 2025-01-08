{{ config(materialized='table') }}

select * from {{ ref('completedfullonlinesales_table') }}