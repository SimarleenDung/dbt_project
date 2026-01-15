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
            partition by email 
            order by 
                case 
                    when first_name is not null and last_name is not null then 1
                    else 2 
                end,
                customer_id desc
        ) as rn
    from source_data
),

source_clean as (
    select
        email,
        first_name,
        last_name,
        city,
        phone_number,
        source_system,
        ingestion_ts
    from source_data_deduped
    where rn = 1
),

-- Find which emails changed (only on incremental runs)
changed_emails as (
    select s.email
    from source_clean s
    {% if is_incremental() %}
    inner join {{ this }} t
        on s.email = t.email and t.is_current = 'Y'
    where
           coalesce(s.first_name, '') <> coalesce(t.first_name, '')
        or coalesce(s.last_name, '') <> coalesce(t.last_name, '')
        or coalesce(s.city, '') <> coalesce(t.city, '')
        or coalesce(s.phone_number, '') <> coalesce(t.phone_number, '')
        or coalesce(s.source_system, '') <> coalesce(t.source_system, '')
    {% else %}
    where 1=0  -- On full refresh, no emails have "changed" - all are new
    {% endif %}
),

-- Rows to close (old versions of changed records)
rows_to_close as (
    {% if is_incremental() %}
    select
        t.email,
        t.first_name,
        t.last_name,
        t.city,
        t.phone_number,
        t.source_system,
        t.valid_from,
        (select min(ingestion_ts) from source_clean where email = t.email) as valid_to,
        'N' as is_current
    from {{ this }} t
    inner join changed_emails c on t.email = c.email
    where t.is_current = 'Y'
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
    from source_clean s
    {% if is_incremental() %}
    where 
        s.email not in (select email from {{ this }})
        or s.email in (select email from changed_emails)
    {% endif %}
)

-- Combine: old records being closed + new records
select * from rows_to_close
union all
select * from new_records