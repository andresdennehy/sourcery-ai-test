-- models/revenue_growth_analysis.sql

WITH revenue_by_date AS (
    SELECT
        o.order_date,
        DATE_TRUNC('month', o.order_date) AS order_month,
        p.product_category,
        r.region_name,
        SUM(o.total_amount) AS monthly_revenue
    FROM {{ ref('orders') }} o
    JOIN {{ ref('products') }} p ON o.product_id = p.product_id
    JOIN {{ ref('regions') }} r ON o.region_id = r.region_id
    GROUP BY 1, 2, 3, 4
),

-- Calculating Year-Over-Year (YoY) growth with window function
revenue_growth AS (
    SELECT
        order_month,
        product_category,
        region_name,
        monthly_revenue,
        LAG(monthly_revenue, 12) OVER (PARTITION BY product_category, region_name ORDER BY order_month) AS prev_year_revenue,
        ((monthly_revenue - LAG(monthly_revenue, 12) OVER (PARTITION BY product_category, region_name ORDER BY order_month)) / NULLIF(LAG(monthly_revenue, 12) OVER (PARTITION BY product_category, region_name ORDER BY order_month), 0)) AS yoy_growth_rate
    FROM revenue_by_date
)

SELECT * FROM revenue_growth
WHERE order_month >= DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '2 years'
ORDER BY order_month DESC