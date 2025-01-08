with
salesquota as (
    select * from {{ ref('stg_contoso__salesquota') }}
),
channel as (
    select * from {{ ref('stg_contoso__channel') }}
),
store as (
    select * from {{ ref('stg_contoso__store') }}
),
product as (
    select * from {{ ref('int_product_jointogether') }}
),
date as (
    select * from {{ ref('stg_contoso__date') }}
),
currency as (
    select * from {{ ref('stg_contoso__currency') }}
),
scenario as (
    select * from {{ ref('stg_contoso__scenario') }}
),
geography as (
    select 
        GEOGRAPHYKEY_updated,
        GEOGRAPHYTYPE,
        CONTINENTNAME,
        CITYNAME,
        STATEPROVINCENAME,
        REGIONCOUNTRYNAME,

    from {{ ref('stg_contoso__geography') }}
),
entity as (
    select * from {{ ref('stg_contoso__entity') }}
),

salesquotacombined as (
    select
        s.SALESQUOTAKEY_updated,
        c.CHANNELNAME,
	    st.STORENAME,
        g.GEOGRAPHYTYPE,
        g.CONTINENTNAME,
        g.CITYNAME,
        g.STATEPROVINCENAME,
        g.REGIONCOUNTRYNAME,
        e.ENTITYNAME,
        p.PRODUCTNAME,
        p.PRODUCTCATEGORYNAME,
        p.PRODUCTSUBCATEGORYNAME,
        d.DATEKEY_updated,
        d.CALENDARYEARLABEL,
	    d.CALENDARHALFYEARLABEL,
	    d.CALENDARQUARTERLABEL,
	    d.CALENDARMONTHLABEL,
	    d.CALENDARWEEKLABEL,
	    d.CALENDARDAYOFWEEKLABEL,
        cu.CURRENCYNAME,
        sc.SCENARIONAME,
        s.SALESQUANTITYQUOTA_updated,
	    s.SALESAMOUNTQUOTA_updated,
	    s.GROSSMARGINQUOTA_updated      
    from salesquota s
    left join channel c on s.CHANNELKEY_updated = c.CHANNELKEY_updated
    left join store st on s.STOREKEY_updated = st.STOREKEY_updated
    left join geography g on g.GEOGRAPHYKEY_updated = st.GEOGRAPHYKEY_updated
    left join entity e on e.ENTITYKEY_updated = st.ENTITYKEY_updated
    left join product p on s.PRODUCTKEY_updated = p.PRODUCTKEY_updated
    left join date d on s.DATEKEY_updated = d.DATEKEY_updated
    left join currency cu on s.CURRENCYKEY_updated = cu.CURRENCYKEY_updated
    left join scenario sc on s.SCENARIOKEY_updated = sc.SCENARIOKEY_updated 
)

select * from salesquotacombined