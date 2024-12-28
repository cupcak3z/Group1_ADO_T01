{{ config(materialized="incremental") }}

select *
from strategyplan as sp
{% if is_incremental() %}
    where
        cast(sp.datekey as date)
        >= (select max(cast(this.datekey as date)) from {{ this }} as this)
{% endif %}