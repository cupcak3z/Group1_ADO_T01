{{ config(materialized="incremental") }}

select *
from salesquota as sq
{% if is_incremental() %}
    where
        cast(sq.datekey as date)
        >= (select max(cast(this.datekey as date)) from {{ this }} as this)
{% endif %}