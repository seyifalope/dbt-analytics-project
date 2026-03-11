-- ============================================
-- WINDOW FUNCTIONS PRACTICE
-- Day 6 — SQL Deep Dive
-- Data: SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS
-- ============================================


-- QUERY 1: Customer total spend on every row
-- Shows SUM() OVER (PARTITION BY)
-- Business use: See each order AND customer total together

SELECT
    o_orderkey      AS order_id,
    o_custkey       AS customer_id,
    o_totalprice    AS order_amount,
    SUM(o_totalprice) OVER (
        PARTITION BY o_custkey
    ) AS customer_total_spend
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS
WHERE o_custkey IN (1, 2, 3)
ORDER BY o_custkey;


-- QUERY 2: Rank orders within each customer
-- Shows RANK() OVER (PARTITION BY ORDER BY)
-- Business use: Find each customer's biggest order

SELECT
    o_orderkey      AS order_id,
    o_custkey       AS customer_id,
    o_totalprice    AS order_amount,
    RANK() OVER (
        PARTITION BY o_custkey
        ORDER BY o_totalprice DESC
    ) AS order_rank
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS
WHERE o_custkey IN (1, 2, 3)
ORDER BY o_custkey, order_rank;


-- QUERY 3: Top 5 customers by total spend
-- Shows RANK() without PARTITION BY + CTEs
-- Business use: Identify VIP customers

WITH customer_totals AS (
    SELECT
        o_custkey           AS customer_id,
        SUM(o_totalprice)   AS total_spend,
        COUNT(o_orderkey)   AS total_orders,
        AVG(o_totalprice)   AS avg_order_value
    FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS
    GROUP BY o_custkey
),
ranked_customers AS (
    SELECT
        customer_id,
        total_spend,
        total_orders,
        avg_order_value,
        RANK() OVER (
            ORDER BY total_spend DESC
        ) AS spending_rank
    FROM customer_totals
)
SELECT *
FROM ranked_customers
WHERE spending_rank <= 5
ORDER BY spending_rank;


-- QUERY 4: Order details with rank and spend percentage
-- Shows multiple window functions together
-- Business use: Full customer order analysis

SELECT
    o_orderkey                      AS order_id,
    o_custkey                       AS customer_id,
    o_totalprice                    AS order_amount,
    o_orderstatus                   AS status,
    RANK() OVER (
        PARTITION BY o_custkey
        ORDER BY o_totalprice DESC
    )                               AS order_rank,
    SUM(o_totalprice) OVER (
        PARTITION BY o_custkey
    )                               AS customer_total_spend,
    ROUND(
        o_totalprice /
        SUM(o_totalprice) OVER (
            PARTITION BY o_custkey
        ) * 100, 2
    )                               AS pct_of_customer_spend
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS
WHERE o_custkey IN (1, 2, 3)
ORDER BY o_custkey, order_rank;


-- QUERY 5: Running total of daily revenue
-- Shows SUM() OVER (ORDER BY) — no PARTITION BY
-- Business use: Cumulative revenue over time

WITH daily_revenue AS (
    SELECT
        o_orderdate         AS order_date,
        SUM(o_totalprice)   AS daily_revenue
    FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS
    GROUP BY o_orderdate
)
SELECT
    order_date,
    daily_revenue,
    SUM(daily_revenue) OVER (
        ORDER BY order_date
    ) AS running_total_revenue
FROM daily_revenue
ORDER BY order_date
LIMIT 30;
