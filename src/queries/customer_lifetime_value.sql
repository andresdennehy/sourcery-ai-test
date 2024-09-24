-- models/customer_lifetime_value.sql

WITH customer_orders AS (
    SELECT
        c.customer_id,
        o.order_id,
        o.order_date,
        o.total_amount,
        SUM(o.total_amount) OVER (PARTITION BY c.customer_id ORDER BY o.order_date) AS cumulative_spend
    FROM {{ ref('customers') }} c
    JOIN {{ ref('orders') }} o ON c.customer_id = o.customer_id
),

-- Calculating average order value per customer
customer_avg_order_value AS (
    SELECT
        customer_id,
        AVG(total_amount) AS avg_order_value
    FROM customer_orders
    GROUP BY customer_id
),

-- Calculating customer order frequency
customer_order_frequency AS (
    SELECT
        customer_id,
        COUNT(order_id) AS order_count,
        DATEDIFF(MAX(order_date), MIN(order_date)) AS customer_lifespan_days
    FROM customer_orders
    GROUP BY customer_id
),

-- Customer lifetime value model (CLV)
customer_lifetime_value AS (
    SELECT
        co.customer_id,
        co.cumulative_spend,
        ao.avg_order_value,
        of.order_count,
        of.customer_lifespan_days,
        (ao.avg_order_value * of.order_count) / (CASE WHEN of.customer_lifespan_days = 0 THEN 1 ELSE of.customer_lifespan_days END) AS customer_lifetime_value
    FROM customer_orders co
    LEFT JOIN customer_avg_order_value ao ON co.customer_id = ao.customer_id
    LEFT JOIN customer_order_frequency of ON co.customer_id = of.customer_id
)

SELECT * FROM customer_lifetime_value