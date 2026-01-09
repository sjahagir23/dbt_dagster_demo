
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select balance
from `workspace`.`staging`.`stg_accounts`
where balance is null



  
  
      
    ) dbt_internal_test