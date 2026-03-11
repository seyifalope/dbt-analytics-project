-- ============================================
-- DEDUPLICATION AND DATE LOGIC
-- Day 7 — SQL Deep Dive Part 2
-- Data: SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS
-- ============================================


-- ============================================
-- PART 1: DEDUPLICATION
-- ============================================


-- QUERY 1: Find duplicates using COUNT OVER
-- Business use: Detect if pipeline ran twice

WITH orders_with_duplicates AS (
    SELECT o_orderkey, o_custkey, o_orderdate, o_totalprice
    FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS
    WHERE o_custkey IN (1, 2, 3)

    UNION ALL

    SELECT o_orderkey, o_custkey, o_orderdate, o_totalprice
    FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS
    WHERE o_custkey IN (1, 2, 3)
)

SELECT
    o_orderkey          AS order_id,
    o_custkey           AS customer_id,
    o_orderdate         AS order_date,
    o_totalprice        AS order_amount,
    COUNT(*) OVER (
        PARTITION BY o_orderkey
    )                   AS duplicate_count

FROM orders_with_duplicates
ORDER BY duplicate_count DESC, o_custkey;


-- QUERY 2: Remove duplicates using ROW_NUMBER
-- Business use: Keep one row per order
-- Pattern: PARTITION BY unique key, ORDER BY tiebreaker
-- WHERE row_num = 1 keeps only first occurrence

WITH orders_with_duplicates AS (
    SELECT o_orderkey, o_custkey, o_orderdate, o_totalprice
    FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS
    WHERE o_custkey IN (1, 2, 3)

    UNION ALL

    SELECT o_orderkey, o_custkey, o_orderdate, o_totalprice
    FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS
    WHERE o_custkey IN (1, 2, 3)
),

numbered AS (
    SELECT
        o_orderkey      AS order_id,
        o_custkey       AS customer_id,
        o_orderdate     AS order_date,
        o_totalprice    AS order_amount,

        ROW_NUMBER() OVER (
            PARTITION BY o_orderkey
            ORDER BY o_orderdate
        )               AS row_num

    FROM orders_with_duplicates
)

SELECT
    order_id,
    customer_id,
    order_date,
    order_amount

FROM numbered
WHERE row_num = 1
ORDER BY customer_id, order_date;


-- QUERY 3: Keep most recent record per customer
-- Business use: Customer table loaded twice
-- ORDER BY loaded_at DESC means row_num 1 = most recent

WITH customer_history AS (
    SELECT 1 AS customer_id, 'Alice' AS name, 100.00 AS balance, '2024-01-01'::DATE AS loaded_at
    UNION ALL
    SELECT 1, 'Alice', 150.00, '2024-02-01'::DATE
    UNION ALL
    SELECT 2, 'Bob', 200.00, '2024-01-01'::DATE
    UNION ALL
    SELECT 2, 'Bob', 250.00, '2024-03-01'::DATE
    UNION ALL
    SELECT 3, 'Carol', 300.00, '2024-01-01'::DATE
),

deduped AS (
    SELECT
        customer_id,
        name,
        balance,
        loaded_at,

        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY loaded_at DESC
        ) AS row_num

    FROM customer_history
)

SELECT
    customer_id,
    name,
    balance,
    loaded_at

FROM deduped
WHERE row_num = 1
ORDER BY customer_id;


-- ============================================
-- PART 2: DATE LOGIC
-- ============================================


-- QUERY 4: DATE_TRUNC — truncate dates to period
-- Business use: Group orders by month or year

SELECT
    o_orderdate                                     AS order_date,
    DATE_TRUNC('month', o_orderdate)                AS order_month,
    DATE_TRUNC('year', o_orderdate)                 AS order_year,
    DATE_TRUNC('week', o_orderdate)                 AS order_week

FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS
LIMIT 20;


-- QUERY 5: Monthly revenue trend
-- Business use: Revenue by month for line charts

SELECT
    DATE_TRUNC('month', o_orderdate)    AS order_month,
    COUNT(o_orderkey)                   AS total_orders,
    SUM(o_totalprice)                   AS total_revenue,
    AVG(o_totalprice)                   AS avg_order_value

FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS
GROUP BY DATE_TRUNC('month', o_orderdate)
ORDER BY order_month;


-- QUERY 6: Customer lifespan using DATEDIFF
-- Business use: How long has each customer been active?
-- Used in retention analysis and LTV calculations

SELECT
    o_custkey                               AS customer_id,
    MIN(o_orderdate)                        AS first_order_date,
    MAX(o_orderdate)                        AS last_order_date,
    COUNT(o_orderkey)                       AS total_orders,

    DATEDIFF('day',
        MIN(o_orderdate),
        MAX(o_orderdate)
    )                                       AS customer_lifespan_days,

    DATEDIFF('month',
        MIN(o_orderdate),
        MAX(o_orderdate)
    )                                       AS customer_lifespan_months

FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS
GROUP BY o_custkey
ORDER BY customer_lifespan_days DESC
LIMIT 20;


-- QUERY 7: Month over month revenue growth using LAG
-- Business use: Is revenue growing or shrinking?
-- LAG() gives previous row value
-- Growth % = (this month - last month) / last month * 100

WITH monthly_revenue AS (
    SELECT
        DATE_TRUNC('month', o_orderdate)    AS order_month,
        COUNT(o_orderkey)                   AS total_orders,
        SUM(o_totalprice)                   AS total_revenue

    FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS
    GROUP BY DATE_TRUNC('month', o_orderdate)
),

with_previous AS (
    SELECT
        order_month,
        total_orders,
        total_revenue,

        LAG(total_revenue) OVER (
            ORDER BY order_month
        )                                   AS previous_month_revenue

    FROM monthly_revenue
)

SELECT
    order_month,
    total_orders,
    total_revenue,
    previous_month_revenue,

    ROUND(
        (total_revenue - previous_month_revenue)
        / previous_month_revenue * 100
    , 2)                                    AS revenue_growth_pct

FROM with_previous
WHERE previous_month_revenue IS NOT NULL
ORDER BY order_month;


-- QUERY 8: Days since each order using CURRENT_DATE
-- Business use: How old is each order?

SELECT
    o_orderkey          AS order_id,
    o_custkey           AS customer_id,
    o_orderdate         AS order_date,
    o_totalprice        AS order_amount,

    DATEDIFF('day',
        o_orderdate,
        CURRENT_DATE
    )                   AS days_ago

FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS
ORDER BY o_orderdate DESC
LIMIT 20;


-- QUERY 9: Filter to last 30 days using DATEADD
-- Business use: Recent orders dashboard filter
-- NOTE: TPCH data is old so returns 0 rows
-- In real company data this returns last 30 days

SELECT
    o_orderkey          AS order_id,
    o_custkey           AS customer_id,
    o_orderdate         AS order_date,
    o_totalprice        AS order_amount,

    DATEDIFF('day',
        o_orderdate,
        CURRENT_DATE
    )                   AS days_ago

FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS
WHERE o_orderdate >= DATEADD('day', -30, CURRENT_DATE)
ORDER BY o_orderdate DESC;
