{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['email', 'valid_from']
) }}

with source_data as (

    select
        customer_id,
        email,
        first_name,
        last_name,
        city,
        phone_number,
        source_system,
        ingestion_ts
    from {{ ref('stg_customers') }}

),

-- 1️⃣ Deduplicate rows arriving at the same timestamp
source_clean as (

    select
        email,
        first_name,
        last_name,
        city,
        phone_number,
        source_system,
        ingestion_ts,
        row_number() over (
            partition by email, ingestion_ts
            order by customer_id desc
        ) as rn
    from source_data

),

-- 2️⃣ Keep only the FIRST occurrence of each attribute combination
--     This is the key step that prevents Bob Green from flipping
dedup_attributes as (

    select
        email,
        first_name,
        last_name,
        city,
        phone_number,
        source_system,
        ingestion_ts,
        row_number() over (
            partition by
                email,
                first_name,
                last_name,
                city,
                phone_number,
                source_system
            order by ingestion_ts
        ) as attr_rn
    from source_clean
    where rn = 1

),

all_source_records as (

    select
        email,
        first_name,
        last_name,
        city,
        phone_number,
        source_system,
        ingestion_ts
    from dedup_attributes
    where attr_rn = 1

),

-- 3️⃣ Emails whose ATTRIBUTES actually changed
changed_emails as (

    {% if is_incremental() %}
    select distinct s.email
    from all_source_records s
    join {{ this }} t
      on s.email = t.email
     and t.is_current = 'Y'
    where
          coalesce(s.first_name, '') <> coalesce(t.first_name, '')
       or coalesce(s.last_name, '')  <> coalesce(t.last_name, '')
       or coalesce(s.city, '')       <> coalesce(t.city, '')
       or coalesce(s.phone_number, '') <> coalesce(t.phone_number, '')
       or coalesce(s.source_system, '') <> coalesce(t.source_system, '')
    {% else %}
    select cast(null as varchar) as email
    from (select 1) x
    where 1 = 0
    {% endif %}

),

-- 4️⃣ New emails
new_emails as (

    {% if is_incremental() %}
    select distinct s.email
    from all_source_records s
    left join {{ this }} t
      on s.email = t.email
    where t.email is null
    {% else %}
    select distinct email from all_source_records
    {% endif %}

),

emails_to_process as (

    select email from changed_emails
    union
    select email from new_emails

),

-- 5️⃣ Build timeline for CHANGED emails
changed_email_timeline as (

    {% if is_incremental() %}
    select
        email,
        first_name,
        last_name,
        city,
        phone_number,
        source_system,
        valid_from
    from {{ this }}
    where email in (select email from changed_emails)

    union all

    select
        email,
        first_name,
        last_name,
        city,
        phone_number,
        source_system,
        ingestion_ts as valid_from
    from all_source_records
    where email in (select email from changed_emails)
    {% else %}
    select
        cast(null as varchar) as email,
        cast(null as varchar) as first_name,
        cast(null as varchar) as last_name,
        cast(null as varchar) as city,
        cast(null as varchar) as phone_number,
        cast(null as varchar) as source_system,
        cast(null as timestamp) as valid_from
    from (select 1) x
    where 1 = 0
    {% endif %}

),

changed_scd2 as (

    select
        email,
        first_name,
        last_name,
        city,
        phone_number,
        source_system,
        valid_from,
        lead(valid_from) over (
            partition by email
            order by valid_from
        ) as valid_to,
        case
            when lead(valid_from) over (
                partition by email
                order by valid_from
            ) is null then 'Y'
            else 'N'
        end as is_current
    from changed_email_timeline

),

-- 6️⃣ New emails SCD2
new_scd2 as (

    select
        email,
        first_name,
        last_name,
        city,
        phone_number,
        source_system,
        ingestion_ts as valid_from,
        lead(ingestion_ts) over (
            partition by email
            order by ingestion_ts
        ) as valid_to,
        case
            when lead(ingestion_ts) over (
                partition by email
                order by ingestion_ts
            ) is null then 'Y'
            else 'N'
        end as is_current
    from all_source_records
    where email in (select email from new_emails)

),

-- 7️⃣ Final union (NO select *)
final as (

    select
        email,
        first_name,
        last_name,
        city,
        phone_number,
        source_system,
        valid_from,
        valid_to,
        is_current
    from changed_scd2

    union all

    select
        email,
        first_name,
        last_name,
        city,
        phone_number,
        source_system,
        valid_from,
        valid_to,
        is_current
    from new_scd2

    {% if is_incremental() %}
    union all

    select
        email,
        first_name,
        last_name,
        city,
        phone_number,
        source_system,
        valid_from,
        valid_to,
        is_current
    from {{ this }}
    where email not in (select email from emails_to_process)
    {% endif %}

),

final_dedup as (

    select
        email,
        first_name,
        last_name,
        city,
        phone_number,
        source_system,
        valid_from,
        valid_to,
        is_current,
        row_number() over (
            partition by email, valid_from
            order by
                case when is_current = 'Y' then 1 else 2 end,
                valid_to desc
        ) as rn
    from final

)

select
    email,
    first_name,
    last_name,
    city,
    phone_number,
    source_system,
    valid_from,
    valid_to,
    is_current
from final_dedup
where rn = 1
