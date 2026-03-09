with source as (

    select * from {{ source('tpch', 'orders') }}

),

renamed as (

    select
        o_orderkey      as order_id,
        o_custkey       as customer_id,
        o_orderstatus   as status,
        o_totalprice    as order_amount,
        o_orderdate     as order_date,
        o_orderpriority as priority

    from source

)

select * from renamed
