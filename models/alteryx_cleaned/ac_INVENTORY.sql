{{ config(materialized="incremental") }}

select *
from inventory as i
{% if is_incremental() %}
    where
        cast(i.datekey as date)
        >= (select max(cast(this.datekey as date)) from {{ this }} as this)
{% endif %}