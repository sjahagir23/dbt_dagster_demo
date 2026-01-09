{{ config(schema='intermediate') }}
select
    a.account_id,
    a.customer_id,
    c.customer_name,
    c.has_loan,
    a.balance,
    a.account_type

from {{ ref('stg_accounts') }} a
join {{ ref('stg_customers') }} c
  on a.customer_id = c.customer_id
where a.account_type = 'savings'