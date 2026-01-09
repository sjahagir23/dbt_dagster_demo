with __dbt__cte__int_customer_accounts as (

-- Fixture for int_customer_accounts
select cast('UT002' as string)
 as `account_id`, cast(202 as int)
 as `customer_id`, cast(null as string) as `customer_name`, cast(False as boolean)
 as `has_loan`, cast(5000 as decimal(12, 2))
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