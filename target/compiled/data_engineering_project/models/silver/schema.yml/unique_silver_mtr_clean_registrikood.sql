
    
    

select
    registrikood as unique_field,
    count(*) as n_records

from `default`.`silver_mtr_clean`
where registrikood is not null
group by registrikood
having count(*) > 1


