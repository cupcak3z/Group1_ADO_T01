{{ config(materialized="incremental") }}

select *
from hongkailovesadotesting as h
{% if is_incremental() %}
    where
        cast(h.hongkaikey as int)
        >= (select max(cast(this.hongkaikey as int)) from {{ this }} as this)
{% endif %}
