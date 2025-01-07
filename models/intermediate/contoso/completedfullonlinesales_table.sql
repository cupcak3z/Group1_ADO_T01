with online_sales as (
    select 
        ONLINESALESKEY_updated,
        STOREKEY_updated as online_sales_storekey,
        DATEKEY_updated as online_sales_datekey,
        PRODUCTKEY_updated,
        CUSTOMERKEY_updated,
        SALESAMOUNT_updated,
        DISCOUNTAMOUNT_updated,
        RETURNAMOUNT_updated,
        SALESQUANTITY_updated,
        DISCOUNTQUANTITY_updated,
        RETURNQUANTITY_updated,
        UNITCOST_UPDATED as online_sales_unitcost, -- Alias to avoid conflict
        UNITPRICE_updated,
        TOTALCOST_updated,
        NET_SALES_AMOUNT,
        TOTAL_PROFIT
    from {{ ref('stg_contoso__onlinesales') }}
),

date as (
    select 
        DATEKEY_updated as date_table_datekey,
        CALENDARYEAR_updated,
        CALENDARMONTHLABEL,
        CALENDARHALFYEARLABEL,
        CALENDARQUARTERLABEL,
        CALENDARWEEKLABEL,
        CALENDARDAYOFWEEKLABEL,
        ISHOLIDAY_updated,
        ISWORKDAY,
        EUROPESEASON_updated,
        NORTHAMERICASEASON_updated,
        ASIASEASON_updated
    from {{ ref('stg_contoso__date') }}
),

geography as (
    select 
        GEOGRAPHYKEY_updated as geography_table_geographykey,
        GEOGRAPHYTYPE,
        CONTINENTNAME,
        CITYNAME,
        STATEPROVINCENAME,
        REGIONCOUNTRYNAME
    from {{ ref('stg_contoso__geography') }}
),

store as (
    select 
        STOREKEY_updated as store_table_storekey,
        GEOGRAPHYKEY_updated as store_geographykey,
        STORETYPE,
        STORENAME,
        STATUS,
        CLOSEREASON_UPDATED,
        STOREMANAGER_UPDATED,
        EMPLOYEECOUNT_UPDATED,
        SELLINGAREASIZE_UPDATED,
        OPENDATE_UPDATED,
        CLOSEDATE_UPDATED,
        LASTREMODELDATE
    from {{ ref('stg_contoso__store') }}
),

product_information as (
    select 
        PRODUCTKEY_UPDATED as product_table_productkey,
        PRODUCTSUBCATEGORYKEY_UPDATED as product_subcategorykey,
        PRODUCTNAME,
        PRODUCTDESCRIPTION,
        BRANDNAME,
        CLASSNAME,
        COLORNAME,
        PRODUCTCATEGORYNAME,
        PRODUCTSUBCATEGORYNAME
    from {{ ref('int_product_jointogether') }}
),

customer as (
    select 
        CUSTOMERKEY_UPDATED as customer_table_customerkey,
        FIRSTNAME,
        LASTNAME,
        MARITALSTATUS_UPDATED,
        GENDER_UPDATED,
        EMAILADDRESS,
        EDUCATION,
        OCCUPATION,
        CUSTOMERTYPE,
        COMPANYNAME_UPDATED,
        YEARLYINCOME_UPDATED,
        TOTALCHILDREN_UPDATED,
        NUMBERCHILDRENATHOME_UPDATED,
        HOUSEOWNERFLAG_UPDATED,
        NUMBERCARSOWNED_UPDATED,
        BIRTHDATE_UPDATED
    from {{ ref('stg_contoso__customer') }}
),

joined_data as (
    select 
        os.*,
        d.*,
        s.*,
        g.*,
        p.*, 
        c.*
    from online_sales os
    left join date d on os.online_sales_datekey = d.date_table_datekey
    left join store s on os.online_sales_storekey = s.store_table_storekey
    left join geography g on s.store_geographykey = g.geography_table_geographykey
    left join product_information p on os.PRODUCTKEY_updated = p.product_table_productkey
    left join customer c on os.CUSTOMERKEY_updated = c.customer_table_customerkey
)

select *
from joined_data
