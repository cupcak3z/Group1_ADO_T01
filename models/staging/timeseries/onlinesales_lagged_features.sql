with
onlinesales as (
    select *
    from {{ ref('fact_onlinesales') }}
),

aggregated_onlinesales as (
    select
        created_at::date as date, -- noqa: RF04
        extract(year from created_at) as year_number,
        extract(month from created_at) as month_number,
        extract(day from created_at) as day_number,
        sum(salesamount_updated) as total_onlinesales,
        avg(salesamount_updated) as avg_onlinesales
    from onlinesales
    group by
        extract(year from created_at),
        extract(month from created_at),
        extract(day from created_at),
        created_at::date
),

lagged_features as (
    select
        *,
        coalesce(
            lag(total_onlinesales) over (
                order by date
            ), total_onlinesales
        ) as lag_total_onlinesales,
        coalesce(
            lag(avg_onlinesales) over (
                order by date
            ), avg_onlinesales
        ) as lag_avg_onlinesales
    from aggregated_onlinesales
)

select
    year_number,
    month_number,
    day_number,
    date,
    total_onlinesales,
    avg_onlinesales,
    lag_total_onlinesales,
    lag_avg_onlinesales
from lagged_features
order by date
