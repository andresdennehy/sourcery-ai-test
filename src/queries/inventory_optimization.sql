-- models/inventory_optimization.sql

WITH product_sales AS (
    SELECT
        p.product_id,
        p.product_name,
        p.category,
        SUM(o.quantity) AS total_sales_last_30_days,
        AVG(o.quantity) AS avg_sales_per_day
    FROM {{ ref('products') }} p
    JOIN {{ ref('order_items') }} o ON p.product_id = o.product_id
    WHERE o.order_date >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY p.product_id, p.product_name, p.category
),

inventory_status AS (
    SELECT
        i.product_id,
        i.stock_level,
        ps.avg_sales_per_day,
        CASE
            WHEN ps.avg_sales_per_day > 0 THEN i.stock_level / ps.avg_sales_per_day
            ELSE 9999
        END AS days_of_stock_remaining
    FROM {{ ref('inventory') }} i
    JOIN product_sales ps ON i.product_id = ps.product_id
),

reorder_recommendations AS (
    SELECT
        product_id,
        stock_level,
        avg_sales_per_day,
        days_of_stock_remaining,
        CASE
            WHEN days_of_stock_remaining < 10 THEN 'Reorder Immediately'
            WHEN days_of_stock_remaining BETWEEN 10 AND 20 THEN 'Reorder Soon'
            ELSE 'Sufficient Stock'
        END AS reorder_status
    FROM inventory_status
)

SELECT * FROM reorder_recommendations
