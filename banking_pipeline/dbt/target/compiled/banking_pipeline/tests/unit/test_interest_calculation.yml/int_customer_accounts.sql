
-- Fixture for int_customer_accounts
select cast('UT003' as string)
 as `account_id`, cast(203 as int)
 as `customer_id`, cast(null as string) as `customer_name`, cast(True as boolean)
 as `has_loan`, cast(20000 as decimal(12, 2))
 as `balance`, cast('checking' as string)
 as `account_type`