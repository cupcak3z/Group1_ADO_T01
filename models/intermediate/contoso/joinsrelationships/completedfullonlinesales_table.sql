with
online_sales as (
    select
        ONLINESALESKEY_updated,
        DATEKEY_updated,
        STOREKEY_updated,
        PRODUCTKEY_updated,
        PROMOTIONKEY_updated,
        CUSTOMERKEY_updated,
        SALESAMOUNT_updated,
        DISCOUNTAMOUNT_updated,
        RETURNAMOUNT_updated,
        SALESQUANTITY_updated,
        DISCOUNTQUANTITY_updated,
        RETURNQUANTITY_updated,
        UNITCOST_updated,
        UNITPRICE_updated,
        TOTALCOST_updated,
        NET_SALES_AMOUNT,
        TOTAL_PROFIT
    from {{ ref('stg_contoso__onlinesales') }}
),

date as (
    select
        DATEKEY_updated,
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
        GEOGRAPHYKEY_updated,
        GEOGRAPHYTYPE,
        CONTINENTNAME,
        CITYNAME,
        STATEPROVINCENAME,
        REGIONCOUNTRYNAME
    from {{ ref('stg_contoso__geography') }}
),

store as (
    select
        STOREKEY_updated,
        GEOGRAPHYKEY_updated,
        STORETYPE,
        STORENAME,
        STATUS,
        CLOSEREASON_UPDATED,
        STOREMANAGER_UPDATED,
        EMPLOYEECOUNT_UPDATED,
        SELLINGAREASIZE_UPDATED,
        YEARSSINCEOPEN,
        YEARSLEFT,
        DAYSSINCELASTREMODEL
    from {{ ref('stg_contoso__store') }}
),

product_information as (
    select
        PRODUCTKEY_updated,
        PRODUCTNAME,
        PRODUCTCATEGORYNAME,
        PRODUCTSUBCATEGORYNAME,
        MANUFACTURER,
        BRANDNAME,
        CLASSNAME,
        COLORNAME,
        STOCKTYPENAME,
        UNITCOST_updated,
        UNITPRICE_updated,
        MARKUP,
        DAYSSINCEAVAILABLEFORSALE
    from {{ ref('int_product_jointogether') }}
),

promotion as (
    select
        PROMOTIONKEY_updated,
        PROMOTIONNAME,
        PROMOTIONTYPE,
        PROMOTIONCATEGORY,
        DISCOUNTPERCENT_updated,
        DAYSPROMOTIONDURATION
    from {{ ref('stg_contoso__promotion')}}
),

customer as (
    select 
        CUSTOMERKEY_UPDATED,
        FULLNAME,
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
        d.CALENDARYEAR_updated,
        d.CALENDARMONTHLABEL,
        d.CALENDARHALFYEARLABEL,
        d.CALENDARQUARTERLABEL,
        d.CALENDARWEEKLABEL,
        d.CALENDARDAYOFWEEKLABEL,
        d.ISHOLIDAY_updated,
        d.ISWORKDAY,
        d.EUROPESEASON_updated,
        d.NORTHAMERICASEASON_updated,
        d.ASIASEASON_updated,
        s.STORETYPE,
        s.STORENAME,
        s.STATUS,
        s.CLOSEREASON_UPDATED,
        s.STOREMANAGER_UPDATED,
        s.EMPLOYEECOUNT_UPDATED,
        s.SELLINGAREASIZE_UPDATED,
        s.YEARSSINCEOPEN,
        s.YEARSLEFT,
        s.DAYSSINCELASTREMODEL,
        g.GEOGRAPHYTYPE,
        g.CONTINENTNAME,
        g.CITYNAME,
        g.STATEPROVINCENAME,
        g.REGIONCOUNTRYNAME,
        p.PRODUCTNAME,
        p.PRODUCTCATEGORYNAME,
        p.PRODUCTSUBCATEGORYNAME,
        p.MANUFACTURER,
        p.BRANDNAME,
        p.CLASSNAME,
        p.COLORNAME,
        p.STOCKTYPENAME,
        p.MARKUP,
        DAYSSINCEAVAILABLEFORSALE,
        pro.PROMOTIONNAME,
        pro.PROMOTIONTYPE,
        pro.PROMOTIONCATEGORY,
        pro.DISCOUNTPERCENT_updated,
        pro.DAYSPROMOTIONDURATION, 
        c.FULLNAME,
        c.MARITALSTATUS_UPDATED,
        c.GENDER_UPDATED,
        c.EDUCATION,
        c.OCCUPATION,
        c.CUSTOMERTYPE,
        c.COMPANYNAME_UPDATED,
        c.YEARLYINCOME_UPDATED,
        c.TOTALCHILDREN_UPDATED,
        c.NUMBERCHILDRENATHOME_UPDATED,
        c.HOUSEOWNERFLAG_UPDATED,
        c.NUMBERCARSOWNED_UPDATED,
        c.BIRTHDATE_UPDATED
    from online_sales os
    left join date d on os.DATEKEY_updated = d.DATEKEY_updated
    left join store s on os.STOREKEY_updated = s.STOREKEY_updated
    left join geography g on s.GEOGRAPHYKEY_updated = g.GEOGRAPHYKEY_updated
    left join product_information p on os.PRODUCTKEY_updated = p.PRODUCTKEY_updated
    left join promotion pro on os.PROMOTIONKEY_updated = pro.PROMOTIONKEY_updated
    left join customer c on os.CUSTOMERKEY_updated = c.CUSTOMERKEY_updated
)

select *
from joined_data
