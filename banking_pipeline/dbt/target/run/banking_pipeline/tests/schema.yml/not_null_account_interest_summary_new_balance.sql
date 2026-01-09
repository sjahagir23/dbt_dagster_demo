
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select new_balance
from `workspace`.`marts`.`account_interest_summary`
where new_balance is null



  
  
      
    ) dbt_internal_test