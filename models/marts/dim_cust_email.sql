-- dim_cust_emails.sql
select
    email,
    first_name,
    last_name,
    city,
    phone_number,
    source_system,

    dbt_valid_from as start_ts,
    dbt_valid_to   as end_ts,

    case
        when dbt_valid_to is null then 'Y'
        else 'N'
    end as is_current

from {{ ref('cust_email_snapshot') }}
where dbt_valid_to is null