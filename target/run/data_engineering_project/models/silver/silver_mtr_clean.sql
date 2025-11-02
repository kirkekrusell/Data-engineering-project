
  
    
    
    
        
         


        insert into `default`.`silver_mtr_clean__dbt_backup`
        ("registrikood", "tegevusala", "alguskuupaev", "loppkuupaev", "staatus", "allikas")SELECT
    registrikood,
    lower(tegevusala) AS tegevusala,
    alguskuupaev,
    loppkuupaev,
    staatus,
    allikas
FROM `default`.`bronze_mtr_raw`
WHERE staatus = 'aktiivne'
  