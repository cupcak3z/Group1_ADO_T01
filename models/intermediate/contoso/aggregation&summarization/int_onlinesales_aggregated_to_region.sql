with
onlinesalesregiondata as (
    select * from {{ ref('int_sales_joinregiondata') }}  -- Referencing the join model
),

regiononlinesales as (
-- Aggregate sales by Country and Store Type
    select
        REGIONCOUNTRYNAME, 
        STORETYPE,
        round(sum(SALESAMOUNT_UPDATED), 2) as total_sales_amount,
        round(sum(NET_SALES_AMOUNT), 2) as total_net_sales,
        round(sum(TOTAL_PROFIT), 2) as total_profit
    from onlinesalesregiondata
    group by REGIONCOUNTRYNAME, STORETYPE
)

select * from regiononlinesales