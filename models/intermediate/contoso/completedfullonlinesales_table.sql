with online_sales as (
    select 
        ONLINESALESKEY_updated,
        STOREKEY_updated,
        DATEKEY_updated,
        PRODUCTKEY_updated,
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
        OPENDATE_UPDATED,
        CLOSEDATE_UPDATED,
        LASTREMODELDATE

    from {{ ref('stg_contoso__store') }}
),

product as (
    select 
        PRODUCTKEY_UPDATED,
        PRODUCTSUBCATEGORYKEY_UPDATED,
        PRODUCTNAME,
        PRODUCTDESCRIPTION,
        BRANDNAME,
        CLASSNAME,
        COLORNAME,
        UNITCOST_UPDATED,
        UNITPRICE_UPDATED

    from {{ ref('stg_contoso__product') }}
),

product_category as (
    select 
    	PRODUCTCATEGORYKEY_UPDATED,
	    PRODUCTCATEGORYNAME,

    from {{ ref('stg_contoso__productcategory') }}
),

product_subcategory as (
    select 
    	PRODUCTSUBCATEGORYKEY_UPDATED,
        PRODUCTCATEGORYKEY_UPDATED,
        PRODUCTSUBCATEGORYNAME,

    from {{ ref('stg_contoso__productsubcategory') }}
),

customer as (
    select 
    CUSTOMERKEY_UPDATED,
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
    select *
    from online_sales
    left join date on online_sales.datekey_updated = date.datekey_updated
    left join store on online_sales.storekey_updated = store.storekey_updated
    left join geography on store.geographykey_updated = geography.geographykey_updated
    left join product on online_sales.PRODUCTKEY_updated = product.PRODUCTKEY_updated
    left join product_subcategory on product.PRODUCTSUBCATEGORYKEY_UPDATED = product_subcategory.PRODUCTSUBCATEGORYKEY_UPDATED
    left join product_category on product_subcategory.PRODUCTCATEGORYKEY_UPDATED = product_category.PRODUCTCATEGORYKEY_UPDATED
    left join customer on online_sales.CUSTOMERKEY_UPDATED = customer.CUSTOMERKEY_UPDATED
)

select *
from joined_data
