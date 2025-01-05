with

sales as (
    select * from {{ ref('stg_contoso__sales') }}
),
store as (
    select * from {{ ref('stg_contoso__store') }}
),
geography as (
    select * from {{ ref('stg_contoso__geography') }}
),

salesregiondata as (
    select
        s.SALESKEY_UPDATED,
        s.STOREKEY_UPDATED,
        s.DATEKEY_UPDATED,
        s.PRODUCTKEY_UPDATED,
        s.SALESAMOUNT_UPDATED,
        s.NET_SALES_AMOUNT,
        s.TOTAL_PROFIT,
        st.STORETYPE,
        st.STORENAME,
        st.STATUS,
        g.REGIONCOUNTRYNAME
    from sales s
    left join store st on s.STOREKEY_UPDATED = st.STOREKEY_UPDATED
    left join geography g on st.GEOGRAPHYKEY_UPDATED = g.GEOGRAPHYKEY_UPDATED
)

select * from salesregiondata


