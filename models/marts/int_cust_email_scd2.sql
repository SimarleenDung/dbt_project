{{ config(
    materialized = 'incremental',
    unique_key = 'email',
    incremental_strategy = 'merge'
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

    {% if is_incremental() %}
      where ingestion_ts > (select coalesce(max(ingestion_ts), '1900-01-01') from {{ this }})
    {% endif %}
),

-- Deduplicate within batch - keep latest record per email
source_data_deduped as (
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
            order by 
                case 
                    when first_name is not null and last_name is not null then 1
                    else 2 
                end,
                customer_id desc  -- If customer_id exists in staging
        ) as rn
    from source_data
),

source_data_clean as (
    select
        email,
        first_name,
        last_name,
        city,
        phone_number,
        source_system,
        ingestion_ts
    from source_data_deduped
    where rn = 1  -- Keep only the latest/most complete record
),

-- Identify which emails have changes
emails_with_changes as (
    select distinct s.email
    from source_data_clean s  -- Use deduped data
    {% if is_incremental() %}
    inner join {{ this }} t
        on s.email = t.email
        and t.is_current = 'Y'
    where
           coalesce(s.first_name, '')    <> coalesce(t.first_name, '')
        or coalesce(s.last_name, '')     <> coalesce(t.last_name, '')
        or coalesce(s.city, '')          <> coalesce(t.city, '')
        or coalesce(s.phone_number, '')  <> coalesce(t.phone_number, '')
        or coalesce(s.source_system, '') <> coalesce(t.source_system, '')
    {% endif %}
),

-- Close out old current records for changed emails
close_old_records as (
    {% if is_incremental() %}
    select
        t.email,
        t.first_name,
        t.last_name,
        t.city,
        t.phone_number,
        t.source_system,
        t.valid_from,
        min(s.ingestion_ts) as valid_to,
        'N' as is_current
    from {{ this }} t
    inner join emails_with_changes e
        on t.email = e.email
    inner join source_data_clean s  -- Use deduped data
        on t.email = s.email
        and s.ingestion_ts > t.valid_from
    where t.is_current = 'Y'
    group by t.email, t.first_name, t.last_name, t.city, t.phone_number, t.source_system, t.valid_from
    {% else %}
    select
        cast(null as varchar) as email,
        cast(null as varchar) as first_name,
        cast(null as varchar) as last_name,
        cast(null as varchar) as city,
        cast(null as varchar) as phone_number,
        cast(null as varchar) as source_system,
        cast(null as timestamp) as valid_from,
        cast(null as timestamp) as valid_to,
        cast(null as varchar) as is_current
    where 1=0
    {% endif %}
),

-- New records to insert
new_records as (
    select
        s.email,
        s.first_name,
        s.last_name,
        s.city,
        s.phone_number,
        s.source_system,
        s.ingestion_ts as valid_from,
        cast(null as timestamp) as valid_to,
        'Y' as is_current
    from source_data_clean s  -- Use deduped data
    {% if is_incremental() %}
    where 
        -- New emails (never seen before)
        s.email not in (select email from {{ this }})
        -- OR emails with actual changes
        or s.email in (select email from emails_with_changes)
    {% endif %}
)

-- Combine: old records being closed + new records
select * from close_old_records
union all
select * from new_records