with
source as (
    select * from {{ source('ADO_GROUP1_DB_RAW', 'FACTSALES_RAW') }}
),

sales as (
    select
        -- IDs
        CAST(saleskey as NUMERIC(38, 0)) as saleskey_updated,
        CAST(storekey as NUMERIC(38, 0)) as storekey_updated,
        CAST(datekey as DATE) as datekey_updated,
        CAST(productkey as NUMERIC(38, 0)) as productkey_updated,
        CAST(promotionkey as NUMERIC(38, 0)) as promotionkey_updated,
        CAST(channelkey as NUMERIC(38, 0)) as channelkey_updated,
        CAST(currencykey as NUMERIC(38, 0)) as currencykey_updated,

        -- Amounts
        CAST(salesamount as NUMERIC(38, 4)) as salesamount_updated,
        CAST(discountamount as NUMERIC(38, 4)) as discountamount_updated,
        CAST(returnamount as NUMERIC(38, 2)) as returnamount_updated,
        CAST(salesquantity as NUMERIC(38, 0)) as salesquantity_updated,
        CAST(discountquantity as NUMERIC(38, 0)) as discountquantity_updated,
        CAST(returnquantity as NUMERIC(38, 0)) as returnquantity_updated,
        CAST(unitcost as NUMERIC(38, 2)) as unitcost_updated,
        CAST(unitprice as NUMERIC(38, 3)) as unitprice_updated,
        CAST(totalcost as NUMERIC(38, 2)) as totalcost_updated,

        -- Derived Columns
        CAST(salesamount - discountamount as NUMERIC(38, 4))
            as net_sales_amount,
        CAST((unitprice - unitcost) * salesquantity as NUMERIC(38, 2))
            as total_profit,

        -- Creation Timings
        TO_TIMESTAMP_NTZ(datekey) as created_at

    from source
)

select * from sales
