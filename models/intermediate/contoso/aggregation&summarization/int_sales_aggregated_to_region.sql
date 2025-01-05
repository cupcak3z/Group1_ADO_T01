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
        st.*,
        g.*
    from sales s
    left join store st on s.STOREKEY_updated = st.STOREKEY_updated
    left join geography g on st.GEOGRAPHYKEY_updated = g.GEOGRAPHYKEY_updated
)

select * from salesregiondata

