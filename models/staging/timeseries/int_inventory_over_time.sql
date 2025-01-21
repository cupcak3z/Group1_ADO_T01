with aggregated_inventory as (
    select
        i.STOREKEY_UPDATED,
        i.DATEKEY_UPDATED,
        avg(i.ONHANDQUANTITY_UPDATED) as avg_onhand_quantity,
        avg(i.ONORDERQUANTITY_UPDATED) as avg_onorder_quantity
    from {{ ref('stg_contoso__inventory') }} i
    group by i.STOREKEY_UPDATED, i.DATEKEY_UPDATED
),

inventory_with_time as (
    select
        inv.STOREKEY_UPDATED,
        DATE_PART('week', d.DATEKEY_UPDATED) as week_number,  
        d.CALENDARYEARLABEL as year,
        d.CALENDARQUARTERLABEL as quarter,                   
        inv.avg_onhand_quantity,
        inv.avg_onorder_quantity
    from aggregated_inventory inv
    left join {{ ref('stg_contoso__date') }} d
    on inv.DATEKEY_UPDATED = d.DATEKEY_UPDATED
),

sorted_inventory as (
    select
        STOREKEY_UPDATED,
        week_number,
        year,
        quarter,
        avg_onhand_quantity,
        avg_onorder_quantity
    from inventory_with_time
    order by STOREKEY_UPDATED ASC, year ASC, quarter ASC, week_number ASC  
)

select * from sorted_inventory
