-- ============================================
-- dim_dates
-- Grain: One row per calendar date
-- Description: Date dimension covering all
-- dates in the orders dataset.
-- Used by fct_orders to provide date context.
-- Allows stakeholders to filter and group
-- by day, week, month, quarter and year
-- in Power BI without writing complex SQL.
-- ============================================

with date_spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('1992-01-01' as date)",
        end_date="cast('2000-12-31' as date)"
    ) }}

),

final as (

    select
        -- SURROGATE KEY
        -- date as integer: 19920101
        -- Fast to join. Compact. Standard.
        cast(
            replace(cast(date_day as varchar), '-', '')
        as integer)                     as date_key,

        -- NATURAL KEY
        date_day                        as full_date,

        -- DAY ATTRIBUTES
        day(date_day)                   as day_of_month,
        dayofweek(date_day)             as day_of_week,
        dayname(date_day)               as day_name,

        -- WEEK ATTRIBUTES
        weekofyear(date_day)            as week_of_year,

        -- MONTH ATTRIBUTES
        month(date_day)                 as month_number,
        monthname(date_day)             as month_name,

        -- QUARTER ATTRIBUTES
        quarter(date_day)               as quarter_number,
        'Q' || quarter(date_day)        as quarter_name,

        -- YEAR ATTRIBUTES
        year(date_day)                  as year_number,

        -- HELPFUL FLAGS
        -- Stakeholders filter by these
        -- constantly in Power BI
        case
            when dayofweek(date_day) in (1, 7)
            then true
            else false
        end                             as is_weekend,

        case
            when dayofweek(date_day) in (1, 7)
            then false
            else true
        end                             as is_weekday

    from date_spine

)

select * from final
