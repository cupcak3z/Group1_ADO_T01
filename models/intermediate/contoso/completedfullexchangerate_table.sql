with exchange_rate as (
    select *

    from {{ ref('stg_contoso__onlinesales') }}
),

