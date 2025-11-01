
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select alguskuupaev
from `default`.`silver_mtr_clean`
where alguskuupaev is null



  
  
    ) dbt_internal_test