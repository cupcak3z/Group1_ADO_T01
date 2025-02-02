-- get data from sales staging models
select * from {{ ref('stg_fact_sales') }}
