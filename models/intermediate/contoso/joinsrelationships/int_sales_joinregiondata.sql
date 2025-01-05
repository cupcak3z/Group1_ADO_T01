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
        s.*,
        st.STORETYPE,
        st.STORENAME,
        st.STATUS,
        g.CONTINENTNAME,
        g.CITYNAME,
        g.STATEPROVINCENAME,
        g.REGIONCOUNTRYNAME
    from sales s
    left join store st on s.STOREKEY_updated = st.STOREKEY_updated
    left join geography g on st.GEOGRAPHYKEY_updated = g.GEOGRAPHYKEY_updated
)

select * from salesregiondata