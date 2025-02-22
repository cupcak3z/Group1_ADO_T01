-- putting data into a sales table
with
sales as (
    select *
    from {{ ref('stg_fact_sales') }}
),

aggregated_sales as (
    select -- extract date data
        created_at::date as date, -- noqa: RF04
        extract(year from created_at) as year_number,
        extract(month from created_at) as month_number,
        -- Use date for daily grouping
        extract(day from created_at) as day_number,
        sum(salesamount_updated) as total_sales
    from sales
    group by
        extract(year from created_at),
        extract(month from created_at),
        extract(day from created_at),
        created_at::date
),

moving_averages as (
    select -- create moving averages
        *,
        -- Moving average of sales over 7 days (current day + 6 preceding days)
        avg(total_sales) over (
            order by date
            rows between 6 preceding and current row
        ) as moving_avg_sales
    from aggregated_sales
)

-- putting data into a table
select
    year_number,
    month_number,
    day_number,
    date,
    total_sales,
    moving_avg_sales
from moving_averages
order by date
