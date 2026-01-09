
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select AccountID
from `workspace`.`raw`.`accounts`
where AccountID is null



  
  
      
    ) dbt_internal_test