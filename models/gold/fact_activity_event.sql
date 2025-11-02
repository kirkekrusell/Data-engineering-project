SELECT
    concat(registrikood, '_', alguskuupaev) AS event_id,
    c.company_id,
    a.activity_type_id,
    s.status_code AS status,
    m.alguskuupaev AS start_date,
    m.loppkuupaev AS end_date,
    toYYYYMMDD(m.alguskuupaev) AS start_date_id,
    toYYYYMMDD(m.loppkuupaev) AS end_date_id,
    dateDiff('day', m.alguskuupaev, m.loppkuupaev) AS duration_days,
    a.risk_level
FROM {{ ref('silver_mtr_clean') }} m
LEFT JOIN {{ ref('dim_company') }} c ON m.registrikood = c.registry_code
LEFT JOIN {{ ref('dim_activity_type') }} a ON lower(m.tegevusala) = lower(a.activity_area)
LEFT JOIN {{ ref('dim_status') }} s ON m.staatus = s.status_code
