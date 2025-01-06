with 
-- Reference the enriched online sales data
onlinesales_with_product_date as (
    select * 
    from {{ ref('int_product_joinsalesanddatedata') }}
),

aggregated_sales as (
    select
        CALENDARYEAR as year_number,
        case 
            when CALENDARQUARTERLABEL = 'Q1' then 1
            when CALENDARQUARTERLABEL = 'Q2' then 2
            when CALENDARQUARTERLABEL = 'Q3' then 3
            when CALENDARQUARTERLABEL = 'Q4' then 4
        end as quarter_number,
        case 
            when CALENDARMONTHLABEL = 'January' then 1
            when CALENDARMONTHLABEL = 'February' then 2
            when CALENDARMONTHLABEL = 'March' then 3
            when CALENDARMONTHLABEL = 'April' then 4
            when CALENDARMONTHLABEL = 'May' then 5
            when CALENDARMONTHLABEL = 'June' then 6
            when CALENDARMONTHLABEL = 'July' then 7
            when CALENDARMONTHLABEL = 'August' then 8
            when CALENDARMONTHLABEL = 'September' then 9
            when CALENDARMONTHLABEL = 'October' then 10
            when CALENDARMONTHLABEL = 'November' then 11
            when CALENDARMONTHLABEL = 'December' then 12
        end as month_number,
        CALENDARMONTHLABEL as month_name,
        sum(NET_SALES_AMOUNT) as total_sales,
        avg(NET_SALES_AMOUNT) as avg_sales,
        sum(TOTAL_PROFIT) as total_profit
    from onlinesales_with_product_date
    group by 
        CALENDARYEAR,
        case 
            when CALENDARQUARTERLABEL = 'Q1' then 1
            when CALENDARQUARTERLABEL = 'Q2' then 2
            when CALENDARQUARTERLABEL = 'Q3' then 3
            when CALENDARQUARTERLABEL = 'Q4' then 4
        end,
        case 
            when CALENDARMONTHLABEL = 'January' then 1
            when CALENDARMONTHLABEL = 'February' then 2
            when CALENDARMONTHLABEL = 'March' then 3
            when CALENDARMONTHLABEL = 'April' then 4
            when CALENDARMONTHLABEL = 'May' then 5
            when CALENDARMONTHLABEL = 'June' then 6
            when CALENDARMONTHLABEL = 'July' then 7
            when CALENDARMONTHLABEL = 'August' then 8
            when CALENDARMONTHLABEL = 'September' then 9
            when CALENDARMONTHLABEL = 'October' then 10
            when CALENDARMONTHLABEL = 'November' then 11
            when CALENDARMONTHLABEL = 'December' then 12
        end,
        CALENDARMONTHLABEL
),

lagged_features as (
    select
        *,
        -- Lagged features for year
        lag(total_sales) over (
            order by year_number
        ) as lag_total_sales_year,
        lag(avg_sales) over (
            order by year_number
        ) as lag_avg_sales_year,
        lag(total_profit) over (
            order by year_number
        ) as lag_total_profit_year,

        -- Lagged features for quarter
        lag(total_sales) over (
            order by year_number, quarter_number
        ) as lag_total_sales_quarter,
        lag(avg_sales) over (
            order by year_number, quarter_number
        ) as lag_avg_sales_quarter,
        lag(total_profit) over (
            order by year_number, quarter_number
        ) as lag_total_profit_quarter,

        -- Lagged features for month
        lag(total_sales) over (
            order by year_number, quarter_number, month_number
        ) as lag_total_sales_month,
        lag(avg_sales) over (
            order by year_number, quarter_number, month_number
        ) as lag_avg_sales_month,
        lag(total_profit) over (
            order by year_number, quarter_number, month_number
        ) as lag_total_profit_month
    from aggregated_sales
)

select 
    year_number,
    quarter_number,
    month_number,
    month_name,
    total_sales,
    avg_sales,
    total_profit,
    lag_total_sales_year,
    lag_avg_sales_year,
    lag_total_profit_year,
    lag_total_sales_quarter,
    lag_avg_sales_quarter,
    lag_total_profit_quarter,
    lag_total_sales_month,
    lag_avg_sales_month,
    lag_total_profit_month
from lagged_features
order by year_number, quarter_number, month_number
