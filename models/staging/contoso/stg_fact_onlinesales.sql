-- getting raw data from database
with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'FACTONLINESALES_RAW') }}
),

onlinesales as (
    select
    --ids
        -- converting data type
        cast(onlinesaleskey as numeric(38, 0)) as onlinesaleskey_updated,
        -- converting data type
        cast(storekey as numeric(38, 0)) as storekey_updated,
        -- converting data type
        cast(productkey as numeric(38, 0)) as productkey_updated,
        -- converting data type
        cast(promotionkey as numeric(38, 0)) as promotionkey_updated,
        -- converting data type
        cast(currencykey as numeric(38, 0)) as currencykey_updated,
        -- converting data type
        cast(customerkey as numeric(38, 0)) as customerkey_updated,

        --string
        salesordernumber,

        --numbers
        cast(salesorderlinenumber as numeric(38, 0)) -- converting data type
            as salesordernumber_updated,
        -- converting data type
        cast(salesquantity as numeric(38, 0)) as salesquantity_updated,
        -- converting data type
        cast(salesamount as numeric(38, 4)) as salesamount_updated,
        -- converting data type
        cast(returnquantity as numeric(38, 0)) as returnquantity_updated,
        -- converting data type
        cast(returnamount as numeric(38, 4)) as returnamount_updated,
        -- converting data type
        cast(discountquantity as numeric(38, 0)) as discountquantity_updated,
        -- converting data type
        cast(discountamount as numeric(38, 4)) as discountamount_updated,
        -- converting data type
        cast(totalcost as numeric(38, 2)) as totalcost_updated,
        -- converting data type
        cast(unitcost as numeric(38, 2)) as unitcost_updated,
        -- converting data type
        cast(unitprice as numeric(38, 2)) as unitprice_updated,

        --date
        cast(datekey as date) as datekey_updated,

        -- Derived Columns
        -- creating derived metrics
        cast(salesamount - discountamount as numeric(38, 4))
            as net_sales_amount,
        -- creating derived metrics
        cast((unitprice - unitcost) * salesquantity as numeric(38, 2))
            as total_profit,

        --creation date
        to_timestamp(datekey) as created_at -- converting data type

    from source
)

-- putting the transformed data into a table
select * from onlinesales
