
with source as (

    select * from {{ source('tpch', 'supplier') }}

),

renamed as (

    select
        s_suppkey       as supplier_id,
        s_name          as supplier_name,
        s_nationkey     as nation_id,
        s_acctbal       as account_balance,
        s_phone         as phone

    from source

)

select * from renamed