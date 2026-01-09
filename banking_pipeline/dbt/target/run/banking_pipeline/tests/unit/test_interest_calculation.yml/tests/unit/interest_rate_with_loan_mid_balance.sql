-- Build actual result given inputs
with dbt_internal_unit_test_actual as (
  select
    `customer_id`,`account_id`,`original_balance`,`interest_rate`,`annual_interest_amount`,`new_balance`, 'actual' as `actual_or_expected`
  from (
    with __dbt__cte__int_customer_accounts as (

-- Fixture for int_customer_accounts
select cast('UT001' as string)
 as `account_id`, cast(201 as int)
 as `customer_id`, cast(null as string) as `customer_name`, cast(True as boolean)
 as `has_loan`, cast(15000 as decimal(12, 2))
 as `balance`, cast('savings' as string)
 as `account_type`
) select
    customer_id,
    account_id,
    balance as original_balance,

    case
        when balance < 10000 then 0.01
        when balance between 10000 and 20000 then 0.015
        when balance > 20000 then 0.02
    end
    +
    case when has_loan then 0.005 else 0 end
    as interest_rate,

    balance *
    (
        case
            when balance < 10000 then 0.01
            when balance between 10000 and 20000 then 0.015
            when balance > 20000 then 0.02
        end
        +
        case when has_loan then 0.005 else 0 end
    ) as annual_interest_amount,

    balance +
    (
        balance *
        (
            case
                when balance < 10000 then 0.01
                when balance between 10000 and 20000 then 0.015
                when balance > 20000 then 0.02
            end
            +
            case when has_loan then 0.005 else 0 end
        )
    ) as new_balance

from __dbt__cte__int_customer_accounts
WHERE account_type = 'savings'
  ) _dbt_internal_unit_test_actual
),
-- Build expected result
dbt_internal_unit_test_expected as (
  select
    `customer_id`, `account_id`, `original_balance`, `interest_rate`, `annual_interest_amount`, `new_balance`, 'expected' as `actual_or_expected`
  from (
    select cast(201 as int)
 as `customer_id`, cast('UT001' as string)
 as `account_id`, cast(15000 as decimal(12, 2))
 as `original_balance`, cast(0.02 as decimal(14, 3))
 as `interest_rate`, cast(300 as decimal(27, 5))
 as `annual_interest_amount`, cast(15300 as decimal(28, 5))
 as `new_balance`
  ) _dbt_internal_unit_test_expected
)
-- Union actual and expected results
select * from dbt_internal_unit_test_actual
union all
select * from dbt_internal_unit_test_expected