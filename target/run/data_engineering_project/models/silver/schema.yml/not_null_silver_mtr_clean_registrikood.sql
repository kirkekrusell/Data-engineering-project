
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select registrikood
from `default`.`silver_mtr_clean`
where registrikood is null



  
  
    ) dbt_internal_test