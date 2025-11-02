SELECT
    rowNumberInAllBlocks() AS activity_type_id,
    lower(tegevusala) AS activity_area,
    'N/A' AS additional_info,
    CASE
        WHEN lower(tegevusala) LIKE '%finants%' THEN 'high'
        WHEN lower(tegevusala) LIKE '%ehitus%' THEN 'medium'
        ELSE 'low'
    END AS risk_level
FROM {{ ref('silver_mtr_clean') }}
GROUP BY tegevusala

