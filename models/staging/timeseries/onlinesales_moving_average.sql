with
sales as (
    select *
    from {{ ref('fact_onlinesales') }}
),

aggregated_sales as (
    select
        created_at::date as date, -- noqa: RF04
        extract(year from created_at) as year_number,
        extract(month from created_at) as month_number,
        extract(day from created_at) as day_number,
        sum(salesamount_updated) as total_onlinesales
    from sales
    group by
        extract(year from created_at),
        extract(month from created_at),
        extract(day from created_at),
        created_at::date
),

moving_averages as (
    select
        *,
        -- Moving average of sales over 7 days (current day + 6 preceding days)
        avg(total_onlinesales) over (
            order by date
            rows between 6 preceding and current row
        ) as moving_avg_onlinesales
    from aggregated_sales
)

select
    year_number,
    month_number,
    day_number,
    date,
    total_onlinesales,
    moving_avg_onlinesales
from moving_averages
order by date
