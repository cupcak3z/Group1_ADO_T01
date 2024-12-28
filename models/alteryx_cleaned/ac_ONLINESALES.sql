{{ config(materialized="incremental") }}

select *
from onlinesales as os
{% if is_incremental() %}
    where
        cast(os.datekey as date)
        >= (select max(cast(this.datekey as date)) from {{ this }} as this)
{% endif %}