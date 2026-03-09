with orders as (
    select * 
        from {{ ref('stg_orders') }}

),

customers as (
    select *
        from {{ ref('stg_customers') }}
),

final as (
    select 
        orders.order_id,
        orders.order_date,
        orders.status,
        orders.order_amount,
        orders.priority,
        customers.customer_id,
        customers.customer_name,
        customers.market_segment,
        customers.account_balance
    
    from orders
    left join customers
        on orders.customer_id = customers.customer_id

)



select *
    from final