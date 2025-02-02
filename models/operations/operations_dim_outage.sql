-- get data from outage staging model
select * from {{ ref('stg_dim_outage') }}
