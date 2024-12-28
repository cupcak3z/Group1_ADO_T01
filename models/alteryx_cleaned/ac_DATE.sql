{{ config(materialized="incremental") }}

select *
from date as d
{% if is_incremental() %}
    where
        cast(d.datekey as date)
        >= (select max(cast(this.datekey as date)) from {{ this }} as this)
{% endif %}
