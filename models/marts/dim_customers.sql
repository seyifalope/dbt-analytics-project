-- ============================================
-- dim_customers
-- Grain: One row per customer
-- Description: Customer dimension table with
-- surrogate key and all descriptive attributes.
-- Used by fct_orders to provide customer context.
-- ============================================

with source as (

        select * 
            from {{ ref('stg_customers') }}


),


final as
        (
           -- SURROGATE KEY
        -- We generate our own key because natural
        -- customer_id from source systems can change
        -- on system migrations or merges.
           
           
            select  {{ dbt_utils.generate_surrogate_key (['customer_id'])  }} as customer_key,
            
             -- NATURAL KEY
        -- Keep the original ID for joining
        -- back to source systems if needed.
            
            customer_id,
            -- DESCRIPTIVE ATTRIBUTES
        -- These give context to the fact table.
        -- Marketing uses market_segment to
        -- slice revenue by customer type.

            customer_name,
            market_segment,
            nation_id,
            account_balance,

             -- METADATA
        -- When was this record created by dbt.

            current_timestamp as dbt_created_at

            from source

        )
select * 
from final            

        