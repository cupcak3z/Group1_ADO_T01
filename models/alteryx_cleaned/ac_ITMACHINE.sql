{{ config(materialized="incremental") }}

select *
from itmachine as itm
{% if is_incremental() %}
    where
        cast(itm.datekey as date)
        >= (select max(cast(this.datekey as date)) from {{ this }} as this)
{% endif %}