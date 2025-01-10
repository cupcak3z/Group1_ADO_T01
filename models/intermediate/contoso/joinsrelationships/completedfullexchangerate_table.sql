with exchange_rate as (
    select 
        CURRENCYKEY_UPDATED,
        AVERAGERATE_UPDATED,
        ENDOFDAYRATE_UPDATED,
        DATEKEY_UPDATED
    from {{ ref('stg_contoso__exchangerate') }}
),

currency as (
    select 
        CURRENCYKEY_UPDATED,
        CURRENCYNAME,
        CURRENCYDESCRIPTION
    from {{ ref('stg_contoso__currency') }}
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

joined_data as (
    select 
        e.AVERAGERATE_UPDATED,
        e.ENDOFDAYRATE_UPDATED,
        c.CURRENCYNAME,
        c.CURRENCYDESCRIPTION,
        d.*
    from exchange_rate e
    left join currency c on c.CURRENCYKEY_UPDATED = e.CURRENCYKEY_UPDATED
    left join date d on d.DATEKEY_updated = e.DATEKEY_UPDATED
)


select *
from joined_data
