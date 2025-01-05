with 
sales as (
    select * 
    from {{ ref('stg_contoso__onlinesales') }}
),

date_data as (
    select *
    from {{ ref('stg_contoso__date') }}
)

select 
    -- Place these columns at the left
    d.CALENDARMONTHLABEL as month_name,
    d.MONTHNUMBER_UPDATED as month_number,
    d.CALENDARQUARTERLABEL as quarter_name,
    d.FISCALQUARTERLABEL as fiscal_quarter_name,
    d.CALENDARYEAR_UPDATED as year_number,

    -- All other columns from sales
    s.*,
    d.*
from sales s
left join date_data d
on s.DATEKEY_UPDATED = d.DATEKEY_UPDATED

order by 
    year_number, 
    quarter_name, 
    month_number
