-- models/customer_segmentation.sql

WITH customer_spending AS (
    SELECT
        c.customer_id,
        SUM(o.total_amount) AS total_spend,
        COUNT(o.order_id) AS total_orders,
        MAX(o.order_date) AS last_order_date
    FROM {{ ref('customers') }} c
    JOIN {{ ref('orders') }} o ON c.customer_id = o.customer_id
    GROUP BY c.customer_id
),

-- Classifying customers into segments based on spending
customer_segments AS (
    SELECT
        customer_id,
        total_spend,
        total_orders,
        CASE
            WHEN total_spend > 10000 THEN 'High Value'
            WHEN total_spend BETWEEN 5000 AND 10000 THEN 'Medium Value'
            ELSE 'Low Value'
        END AS customer_segment
    FROM customer_spending
)

SELECT * FROM customer_segments
