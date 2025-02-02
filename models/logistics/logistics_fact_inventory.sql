-- get data from inventory staging model
select * from {{ ref('stg_fact_inventory') }}
