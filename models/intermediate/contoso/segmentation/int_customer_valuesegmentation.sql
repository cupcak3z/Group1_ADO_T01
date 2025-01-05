with

customer_values as (

    select
        FULLNAME,
        sum(SALESAMOUNT_UPDATED) as customer_value 
    from {{ ref('int_onlinesales_joinwithcustomer') }}
    where FULLNAME != 'NULL NULL'
    group by FULLNAME

),

customer_valuesegment as (

    select
        FULLNAME,
        customer_value,
        case
            when customer_value < 100 then 'Low Value'
            when customer_value >= 100 and customer_value < 500 then 'Mid Value'
            when customer_value >= 500 then 'High Value'
            else 'Unknown' 
        end as customer_value_segment
    from customer_values
    order by customer_value desc

)

select * from customer_valuesegment