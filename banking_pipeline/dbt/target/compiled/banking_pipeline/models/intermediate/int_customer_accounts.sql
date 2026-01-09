select
    a.account_id,
    a.customer_id,
    c.customer_name,
    c.has_loan,
    a.balance,
    a.account_type

from `workspace`.`staging`.`stg_accounts` a
join `workspace`.`staging`.`stg_customers` c
  on a.customer_id = c.customer_id
where a.account_type = 'savings'