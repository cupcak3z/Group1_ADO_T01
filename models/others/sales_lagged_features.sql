with
sales as (
    select *
    from {{ ref('stg_fact_sales') }}
),

aggregated_sales as (
    select
        created_at::date as date, -- noqa: RF04
        extract(year from created_at) as year_number,
        extract(month from created_at) as month_number,
        -- Use date for daily grouping
        extract(day from created_at) as day_number,
        sum(salesamount_updated) as total_sales,
        avg(salesamount_updated) as avg_sales
    from sales
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
            lag(total_sales) over (
                order by date
            ), total_sales
        ) as lag_total_sales,
        coalesce(
            lag(avg_sales) over (
                order by date
            ), avg_sales
        ) as lag_avg_sales
    from aggregated_sales
)

select
    year_number,
    month_number,
    day_number,
    date,
    total_sales,
    avg_sales,
    lag_total_sales,
    lag_avg_sales
from lagged_features
order by date
