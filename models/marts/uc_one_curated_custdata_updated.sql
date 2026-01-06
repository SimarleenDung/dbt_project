{{ config(
    materialized='incremental',
    unique_key='customer_id',
    schema='CURATED'
) }}

with ranked_customers as (

    select
        customer_id,

        -- window functions instead of GROUP BY
        max(first_name)  over (partition by customer_id) as first_name,
        max(middle_name) over (partition by customer_id) as middle_name,
        max(last_name)   over (partition by customer_id) as last_name,
        max(city)        over (partition by customer_id) as city,
        max(updated_at)  over (partition by customer_id) as updated_at,

        -- keep only one row per customer
        row_number() over (
            partition by customer_id
            order by updated_at desc
        ) as rn

    from {{ ref('stg_uc_one_customer_data') }}

)

select
    customer_id,
    first_name,
    middle_name,
    last_name,
    city,
    updated_at
from ranked_customers
where rn = 1

{% if is_incremental() %}
  and updated_at > (select max(updated_at) from {{ this }})
{% endif %}
