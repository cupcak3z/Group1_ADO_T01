-- get data from machine staging model
select * from {{ ref('stg_dim_machine') }}
