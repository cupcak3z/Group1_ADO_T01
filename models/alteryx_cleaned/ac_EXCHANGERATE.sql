{{ config(materialized="incremental") }}

select *
from exchangerate as er
{% if is_incremental() %}
    where
        cast(er.datekey as date)
        >= (select max(cast(this.datekey as date)) from {{ this }} as this)
{% endif %}