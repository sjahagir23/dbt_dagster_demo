
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select interest_rate
from `workspace`.`marts`.`account_interest_summary`
where interest_rate is null



  
  
      
    ) dbt_internal_test