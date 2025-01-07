with
salesdata as (
    select
        SALESKEY_updated,
        DATEKEY_updated,
        STOREKEY_updated,
        PRODUCTKEY_updated,
        PROMOTIONKEY_updated,
        CHANNELKEY_updated,
        CURRENCYKEY_updated,
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
        FISCALYEAR_updated,
        FISCALHALFYEARLABEL,
        FISCALQUARTERLABEL,
        FISCALMONTHLABEL,
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

currencydata as (
    select
        CURRENCYKEY_updated,
        CURRENCYNAME
    from {{ ref('stg_contoso__currency') }}
),

completedsalesdata as (
    select
        sales.*,
        dates.*,
        geog.*,
        store.*,
        prod.*,
        promo.*,
        chan.*,
        curr.*
    from salesdata sales
    left join datedata dates on sales.DATEKEY_updated = dates.DATEKEY_updated
    left join storedata store on sales.STOREKEY_updated = store.STOREKEY_updated
    left join geographydata geog on store.GEOGRAPHYKEY_updated = geog.GEOGRAPHYKEY_updated
    left join productdata prod on sales.PRODUCTKEY_updated = prod.PRODUCTKEY_updated
    left join promotiondata promo on sales.PROMOTIONKEY_updated = promo.PROMOTIONKEY_updated
    left join channeldata chan on sales.CHANNELKEY_updated = chan.CHANNELKEY_updated
    left join currencydata curr on sales.CURRENCYKEY_updated = curr.CURRENCYKEY_updated
)

select * from completedsalesdata