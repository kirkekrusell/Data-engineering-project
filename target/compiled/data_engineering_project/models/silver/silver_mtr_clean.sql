SELECT
    registrikood,
    lower(tegevusala) AS tegevusala,
    alguskuupaev,
    loppkuupaev,
    staatus,
    allikas
FROM `default`.`bronze_mtr_raw`
WHERE staatus = 'aktiivne'