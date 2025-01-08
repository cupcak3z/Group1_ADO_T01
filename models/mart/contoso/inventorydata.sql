{{ config(materialized='table') }}

select * from {{ ref('completedfullinventory_table') }}