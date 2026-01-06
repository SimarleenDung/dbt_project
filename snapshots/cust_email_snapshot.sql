{% snapshot cust_email_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='email',
        strategy='check',
        check_cols=[
            'first_name',
            'last_name',
            'city',
            'phone_number'
        ]
    )
}}

select
    email,
    first_name,
    last_name,
    city,
    phone_number
from {{ ref('int_customer_resolved') }}

{% endsnapshot %}
