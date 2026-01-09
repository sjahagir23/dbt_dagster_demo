select
    cast(trim(customerid) as int) as customer_id,
    initcap(trim(name)) as customer_name,

    case
        when lower(trim(hasloan)) in ('yes', 'y', 'true') then true
        when lower(trim(hasloan)) in ('no', 'n', 'false') then false
        else false
    end as has_loan

from {{ source('raw', 'customers') }}
where customerid is not null