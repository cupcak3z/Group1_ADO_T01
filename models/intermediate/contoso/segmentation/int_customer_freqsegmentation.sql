with

customer_orders as (

    select
        FULLNAME,
        count(distinct ONLINESALESKEY_updated) as order_count
    from {{ ref('int_onlinesales_joinwithcustomer') }}
    group by FULLNAME

),

customer_segments as (

    select
        FULLNAME,
        case
            when order_count = 1 then 'First Time Buyer'
            when order_count between 2 and 4 then 'Occasional Buyer'
            when order_count between 5 and 9 then 'Regular Buyer'
            when order_count >= 10 then 'Loyal Customer'
            else 'Unknown' 
        end as customer_segment,
        order_count

    from customer_orders
    order by order_count desc

)

select * from customer_segments