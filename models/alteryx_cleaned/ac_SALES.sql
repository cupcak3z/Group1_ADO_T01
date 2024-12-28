{{ config(materialized="incremental") }}

select *
from sales as s
{% if is_incremental() %}
    where
        cast(s.datekey as date)
        >= (select max(cast(this.datekey as date)) from {{ this }} as this)
{% endif %}