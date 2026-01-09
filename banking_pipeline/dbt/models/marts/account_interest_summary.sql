select
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

from {{ ref('int_customer_accounts') }}
WHERE account_type = 'savings'