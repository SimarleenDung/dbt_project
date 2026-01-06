{{ config(
    materialized='incremental',
    unique_key='customer_id',
    schema='CURATED'
) }}

select
    customer_id,
    max(first_name)   as first_name,
    max(middle_name)  as middle_name,
    max(last_name)    as last_name,
    max(city)         as city,
    max(updated_at)   as updated_at
from {{ ref('stg_uc_one_customer_data') }}
group by customer_id

{% if is_incremental() %}
    -- only reprocess changed rows:
    having max(updated_at) > (
        select max(updated_at) from {{ this }}
    )
{% endif %}

order by customer_id
