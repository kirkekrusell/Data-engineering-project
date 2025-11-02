SELECT
    staatus AS status_code,
    staatus AS status_label
FROM {{ ref('silver_mtr_clean') }}
GROUP BY staatus
