{% snapshot orders_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='order_id',
        strategy='check',
        check_cols=['status']
     )

}}

select 
    order_id,
    customer_id,
    status,
    order_amount,
    order_date


from {{ ref('stg_orders') }}   

{% endsnapshot %}