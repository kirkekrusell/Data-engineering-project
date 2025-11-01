SELECT
    registrikood,
    lower(tegevusala) AS tegevusala,
    alguskuupaev,
    loppkuupaev,
    staatus,
    allikas
FROM {{ ref('bronze_mtr_raw') }}
WHERE staatus = 'aktiivne'
