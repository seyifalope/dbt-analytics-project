-- ============================================
-- fct_orders
-- Grain: One row per order
-- Description: Core fact table for orders.
-- Contains all measurable order events.
-- Joins to dim_customers for customer context.
-- Joins to dim_dates for date filtering.
--
-- Business use:
-- Marketing  → revenue by customer segment
-- Finance    → revenue by month and quarter
-- Management → total orders and revenue KPIs
-- ============================================

with orders as (

    select * from {{ ref('stg_orders') }}

),

customers as (

    select * from {{ ref('dim_customers') }}

),

dates as (

    select * from {{ ref('dim_dates') }}

),

final as (

    select
        -- PRIMARY KEY
        -- Every fact table needs a unique
        -- identifier for each row.
        orders.order_id,

        -- FOREIGN KEYS TO DIMENSIONS
        -- These are how we join to dimensions.
        -- We store the surrogate key not
        -- the natural key.
        -- This is the Kimball standard.
        customers.customer_key,
        dates.date_key,

        -- KEEP NATURAL KEYS TOO
        -- For debugging and traceability.
        -- So we can always trace back
        -- to the source system.
        orders.customer_id,
        orders.order_date,

        -- MEASURES
        -- The numbers. The things we
        -- count, sum and average.
        -- These are why the fact table exists.
        orders.order_amount,
        orders.order_amount * 0.79    as order_amount_gbp,
        orders.status,
        orders.priority,

        -- METADATA
        current_timestamp             as dbt_created_at

    from orders

    left join customers
        on orders.customer_id = customers.customer_id

    left join dates
        on orders.order_date = dates.full_date

)

select * from final
