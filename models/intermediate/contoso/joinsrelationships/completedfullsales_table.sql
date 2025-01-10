with
salesdata as (
    select
        SALESKEY_updated,
        DATEKEY_updated,
        STOREKEY_updated,
        PRODUCTKEY_updated,
        PROMOTIONKEY_updated,
        CHANNELKEY_updated,
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
    from {{ ref('stg_contoso__sales') }}
),

datedata as (
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

geographydata as (
    select
        GEOGRAPHYKEY_updated,
        GEOGRAPHYTYPE,
        CONTINENTNAME,
        CITYNAME,
        STATEPROVINCENAME,
        REGIONCOUNTRYNAME
    from {{ ref('stg_contoso__geography') }}
),

storedata as (
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

productdata as (
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

promotiondata as (
    select
        PROMOTIONKEY_updated,
        PROMOTIONNAME,
        PROMOTIONTYPE,
        PROMOTIONCATEGORY,
        DISCOUNTPERCENT_updated,
        DAYSPROMOTIONDURATION
    from {{ ref('stg_contoso__promotion')}}
),

channeldata as (
    select
        CHANNELKEY_updated,
        CHANNELNAME
    from {{ ref('stg_contoso__channel') }}
),

completedsalesdata as (
    select
        sales.SALESKEY_updated,
        sales.DATEKEY_updated,
        sales.STOREKEY_updated,
        sales.PRODUCTKEY_updated,
        sales.PROMOTIONKEY_updated,
        sales.CHANNELKEY_updated,
        sales.SALESAMOUNT_updated,
        sales.DISCOUNTAMOUNT_updated,
        sales.RETURNAMOUNT_updated,
        sales.SALESQUANTITY_updated,
        sales.DISCOUNTQUANTITY_updated,
        sales.RETURNQUANTITY_updated,
        sales.UNITCOST_updated,
        sales.UNITPRICE_updated,
        sales.TOTALCOST_updated,
        sales.NET_SALES_AMOUNT,
        sales.TOTAL_PROFIT,
        dates.CALENDARYEAR_updated,
        dates.CALENDARMONTHLABEL,
        dates.CALENDARHALFYEARLABEL,
        dates.CALENDARQUARTERLABEL,
        dates.CALENDARWEEKLABEL,
        dates.CALENDARDAYOFWEEKLABEL,
        dates.ISHOLIDAY_updated,
        dates.ISWORKDAY,
        dates.EUROPESEASON_updated,
        dates.NORTHAMERICASEASON_updated,
        dates.ASIASEASON_updated,
        geog.GEOGRAPHYTYPE,
        geog.CONTINENTNAME,
        geog.CITYNAME,
        geog.STATEPROVINCENAME,
        geog.REGIONCOUNTRYNAME,
        store.STORETYPE,
        store.STORENAME,
        store.STATUS,
        store.CLOSEREASON_UPDATED,
        store.STOREMANAGER_UPDATED,
        store.EMPLOYEECOUNT_UPDATED,
        store.SELLINGAREASIZE_UPDATED,
        store.YEARSSINCEOPEN,
        store.YEARSLEFT,
        store.DAYSSINCELASTREMODEL,
        prod.PRODUCTNAME,
        prod.PRODUCTCATEGORYNAME,
        prod.PRODUCTSUBCATEGORYNAME,
        prod.MANUFACTURER,
        prod.BRANDNAME,
        prod.CLASSNAME,
        prod.COLORNAME,
        prod.STOCKTYPENAME,
        prod.MARKUP,
        prod.DAYSSINCEAVAILABLEFORSALE,
        promo.PROMOTIONNAME,
        promo.PROMOTIONTYPE,
        promo.PROMOTIONCATEGORY,
        promo.DISCOUNTPERCENT_updated,
        promo.DAYSPROMOTIONDURATION,
        chan.CHANNELNAME
    from salesdata sales
    left join datedata dates on sales.DATEKEY_updated = dates.DATEKEY_updated
    left join storedata store on sales.STOREKEY_updated = store.STOREKEY_updated
    left join geographydata geog on store.GEOGRAPHYKEY_updated = geog.GEOGRAPHYKEY_updated
    left join productdata prod on sales.PRODUCTKEY_updated = prod.PRODUCTKEY_updated
    left join promotiondata promo on sales.PROMOTIONKEY_updated = promo.PROMOTIONKEY_updated
    left join channeldata chan on sales.CHANNELKEY_updated = chan.CHANNELKEY_updated
)

select * from completedsalesdata