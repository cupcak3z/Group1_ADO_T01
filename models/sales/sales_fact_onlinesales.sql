-- get data from online sales staging models
select * from {{ ref('stg_fact_onlinesales') }}
