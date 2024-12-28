{{ config(materialized="incremental") }}

select *
from itsla as its
{% if is_incremental() %}
    where
        cast(its.datekey as date)
        >= (select max(cast(this.datekey as date)) from {{ this }} as this)
{% endif %}