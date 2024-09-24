-- models/sales_funnel_analysis.sql

WITH funnel_data AS (
    SELECT
        l.lead_id,
        l.lead_source,
        l.lead_date,
        CASE WHEN p.prospect_id IS NOT NULL THEN 1 ELSE 0 END AS is_prospect,
        CASE WHEN c.customer_id IS NOT NULL THEN 1 ELSE 0 END AS is_customer
    FROM {{ ref('leads') }} l
    LEFT JOIN {{ ref('prospects') }} p ON l.lead_id = p.lead_id
    LEFT JOIN {{ ref('customers') }} c ON p.prospect_id = c.prospect_id
),

-- Aggregating conversion rates
funnel_aggregates AS (
    SELECT
        lead_source,
        COUNT(lead_id) AS total_leads,
        SUM(is_prospect) AS total_prospects,
        SUM(is_customer) AS total_customers,
        ROUND(SUM(is_prospect) * 100.0 / COUNT(lead_id), 2) AS lead_to_prospect_conversion_rate,
        ROUND(SUM(is_customer) * 100.0 / SUM(is_prospect), 2) AS prospect_to_customer_conversion_rate
    FROM funnel_data
    GROUP BY lead_source
)

SELECT * FROM funnel_aggregates
ORDER BY total_leads DESC
