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

-- Deduplicate rows arriving at the same timestamp
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

-- Deduplicate same attributes on same day - keep earliest timestamp
daily_dedup as (

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
                source_system,
                date_trunc('day', ingestion_ts)
            order by ingestion_ts asc
        ) as daily_rn
    from source_clean
    where rn = 1

),

-- Get clean source records (one per day per unique attribute set)
source_records as (
    select
        email,
        first_name,
        last_name,
        city,
        phone_number,
        source_system,
        ingestion_ts
    from daily_dedup
    where daily_rn = 1
),

-- Combine with existing history for changed emails
{% if is_incremental() %}

-- Get emails that have changes
changed_emails as (
    select distinct s.email
    from source_records s
    join {{ this }} t
      on s.email = t.email
     and t.is_current = 'Y'
    where
          trim(coalesce(s.first_name, '')) <> trim(coalesce(t.first_name, ''))
       or trim(coalesce(s.last_name, ''))  <> trim(coalesce(t.last_name, ''))
       or trim(coalesce(s.city, ''))       <> trim(coalesce(t.city, ''))
       or trim(coalesce(s.phone_number, '')) <> trim(coalesce(t.phone_number, ''))
       or trim(coalesce(s.source_system, '')) <> trim(coalesce(t.source_system, ''))
),

-- Get new emails
new_emails as (
    select distinct s.email
    from source_records s
    left join {{ this }} t
      on s.email = t.email
    where t.email is null
),

-- Combine all history for emails being processed
all_records_for_timeline as (
    -- Existing history for changed emails
    select
        email,
        first_name,
        last_name,
        city,
        phone_number,
        source_system,
        valid_from as ingestion_ts
    from {{ this }}
    where email in (select email from changed_emails)
    
    union all
    
    -- New records from source for changed emails
    select
        email,
        first_name,
        last_name,
        city,
        phone_number,
        source_system,
        ingestion_ts
    from source_records
    where email in (select email from changed_emails)
    
    union all
    
    -- All records for new emails
    select
        email,
        first_name,
        last_name,
        city,
        phone_number,
        source_system,
        ingestion_ts
    from source_records
    where email in (select email from new_emails)
),

{% else %}

-- Initial load: use all source records
all_records_for_timeline as (
    select
        email,
        first_name,
        last_name,
        city,
        phone_number,
        source_system,
        ingestion_ts
    from source_records
),

{% endif %}

-- Detect actual changes using LAG
changes_with_history as (
    select
        email,
        first_name,
        last_name,
        city,
        phone_number,
        source_system,
        ingestion_ts,
        lag(first_name) over (partition by email order by ingestion_ts) as prev_first_name,
        lag(last_name) over (partition by email order by ingestion_ts) as prev_last_name,
        lag(city) over (partition by email order by ingestion_ts) as prev_city,
        lag(phone_number) over (partition by email order by ingestion_ts) as prev_phone_number,
        lag(source_system) over (partition by email order by ingestion_ts) as prev_source_system
    from all_records_for_timeline
),

-- Keep only actual changes
actual_changes as (
    select
        email,
        first_name,
        last_name,
        city,
        phone_number,
        source_system,
        ingestion_ts
    from changes_with_history
    where 
        prev_first_name is null  -- First record for this email
        or trim(coalesce(first_name, '')) <> trim(coalesce(prev_first_name, ''))
        or trim(coalesce(last_name, '')) <> trim(coalesce(prev_last_name, ''))
        or trim(coalesce(city, '')) <> trim(coalesce(prev_city, ''))
        or trim(coalesce(phone_number, '')) <> trim(coalesce(prev_phone_number, ''))
        or trim(coalesce(source_system, '')) <> trim(coalesce(prev_source_system, ''))
),

-- Build SCD2 records
new_scd2_records as (
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
    from actual_changes
),

-- Final output
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
    from new_scd2_records
    
    {% if is_incremental() %}
    union all
    
    -- Keep unchanged emails
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
    where email not in (
        select email from new_scd2_records
    )
    {% endif %}
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
from final