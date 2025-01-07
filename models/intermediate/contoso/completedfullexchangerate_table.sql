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
    select *
    from exchange_rate
    left join currency on currency.CURRENCYKEY_UPDATED = exchange_rate.CURRENCYKEY_UPDATED
    left join date on date.DATEKEY_updated = exchange_rate.DATEKEY_updated
)

select *
from joined_data
