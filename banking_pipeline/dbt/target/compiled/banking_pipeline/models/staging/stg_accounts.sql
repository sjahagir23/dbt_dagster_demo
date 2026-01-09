select
    trim(accountid) as account_id,
    cast(trim(customerid) as int) as customer_id,

    cast(coalesce(balance,0) as decimal(12,2)) as balance,

    lower(trim(accounttype)) as account_type

from `workspace`.`raw`.`accounts`
where accountid is not null