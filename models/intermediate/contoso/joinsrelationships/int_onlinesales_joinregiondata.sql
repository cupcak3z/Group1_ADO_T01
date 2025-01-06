with
onlinesales as (
    select * from {{ ref('stg_contoso__onlinesales') }}
),
store as (
    select * from {{ ref('stg_contoso__store') }}
),
geography as (
    select * from {{ ref('stg_contoso__geography') }}
),

onlinesalesregiondata as (
    select
        os.*,
        st.STORETYPE,
        st.STORENAME,
        st.STATUS,
        g.CONTINENTNAME,
        g.CITYNAME,
        g.STATEPROVINCENAME,
        g.REGIONCOUNTRYNAME
    from onlinesales os
    left join store st on os.STOREKEY_updated = st.STOREKEY_updated
    left join geography g on st.GEOGRAPHYKEY_updated = g.GEOGRAPHYKEY_updated
)

select * from onlinesalesregiondata