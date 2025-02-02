-- get data from itmachine staging model
select * from {{ ref('stg_fact_itmachine') }}
