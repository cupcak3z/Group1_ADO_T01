-- get data from itsla staging model
select * from {{ ref('stg_fact_itsla') }}
