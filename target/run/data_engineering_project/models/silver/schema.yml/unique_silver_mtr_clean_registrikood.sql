
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    registrikood as unique_field,
    count(*) as n_records

from `default`.`silver_mtr_clean`
where registrikood is not null
group by registrikood
having count(*) > 1



  
  
    ) dbt_internal_test