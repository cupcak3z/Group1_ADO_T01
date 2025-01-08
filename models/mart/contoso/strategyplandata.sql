{{ config(materialized='table') }}

select * from {{ ref('completedfullstrategyplan_table') }}