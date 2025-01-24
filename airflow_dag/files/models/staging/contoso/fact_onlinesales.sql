with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'FACTONLINESALES_RAW') }}
),

onlinesales as (
    select
    --ids
        cast(onlinesaleskey as numeric(38, 0)) as onlinesaleskey_updated,
        cast(storekey as numeric(38, 0)) as storekey_updated,
        cast(productkey as numeric(38, 0)) as productkey_updated,
        cast(promotionkey as numeric(38, 0)) as promotionkey_updated,
        cast(currencykey as numeric(38, 0)) as currencykey_updated,
        cast(customerkey as numeric(38, 0)) as customerkey_updated,

        --string
        salesordernumber,

        --numbers
        cast(salesorderlinenumber as numeric(38, 0))
            as salesordernumber_updated,
        cast(salesquantity as numeric(38, 0)) as salesquantity_updated,
        cast(salesamount as numeric(38, 4)) as salesamount_updated,
        cast(returnquantity as numeric(38, 0)) as returnquantity_updated,
        cast(returnamount as numeric(38, 4)) as returnamount_updated,
        cast(discountquantity as numeric(38, 0)) as discountquantity_updated,
        cast(discountamount as numeric(38, 4)) as discountamount_updated,
        cast(totalcost as numeric(38, 2)) as totalcost_updated,
        cast(unitcost as numeric(38, 2)) as unitcost_updated,
        cast(unitprice as numeric(38, 2)) as unitprice_updated,

        --date
        cast(datekey as date) as datekey_updated,

        -- Derived Columns
        cast(salesamount - discountamount as numeric(38, 4))
            as net_sales_amount,
        cast((unitprice - unitcost) * salesquantity as numeric(38, 2))
            as total_profit,

        --creation date
        to_timestamp(datekey) as created_at

    from source
)

select * from onlinesales
