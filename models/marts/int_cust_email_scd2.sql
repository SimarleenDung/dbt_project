{{ config(
    materialized = 'incremental',
    unique_key = ['email', 'valid_from'],
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
        batch_id,
        ingestion_ts
    from {{ ref('stg_customers') }}
),

-- dedupe within same timestamp
source_clean as (
    select
        email,
        first_name,
        last_name,
        city,
        phone_number,
        source_system,
        batch_id,
        ingestion_ts,
        row_number() over (
            partition by email, ingestion_ts
            order by customer_id desc
        ) as rn
    from source_data
),

-- keep only 1 row per email PER RUN (very important)
latest_source as (
    select
        email,
        first_name,
        last_name,
        city,
        phone_number,
        source_system,
        batch_id,
        ingestion_ts
    from (
        select
            *,
            row_number() over (
                partition by email
                order by ingestion_ts desc
            ) as rn2
        from source_clean
        where rn = 1
    ) as ranked_source
    where rn2 = 1
),

-- detect changed emails
changed_emails as (
    {% if is_incremental() %}
    select s.email
    from latest_source s
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
    from (select 1 as dummy) dummy_table
    where 1=0
    {% endif %}
),

-- keep rows that are completely untouched
unchanged_history as (
    {% if is_incremental() %}
    select *
    from {{ this }}
    where email not in (select email from changed_emails)
    {% else %}
    select
        cast(null as varchar)   as email,
        cast(null as varchar)   as first_name,
        cast(null as varchar)   as last_name,
        cast(null as varchar)   as city,
        cast(null as varchar)   as phone_number,
        cast(null as varchar)   as source_system,
        cast(null as varchar)   as batch_id,
        cast(null as timestamp) as valid_from,
        cast(null as timestamp) as valid_to,
        cast(null as varchar)   as is_current
    from (select 1 as dummy) dummy_table
    where 1=0
    {% endif %}
),

-- close current rows for changed emails
old_versions as (
    {% if is_incremental() %}
    select
        t.email,
        t.first_name,
        t.last_name,
        t.city,
        t.phone_number,
        t.source_system,
        t.batch_id,
        t.valid_from,
        s.ingestion_ts as valid_to,
        'N' as is_current
    from {{ this }} t
    join changed_emails c on t.email = c.email
    join latest_source s on s.email = t.email
    where t.is_current = 'Y'
    {% else %}
    select
        cast(null as varchar)   as email,
        cast(null as varchar)   as first_name,
        cast(null as varchar)   as last_name,
        cast(null as varchar)   as city,
        cast(null as varchar)   as phone_number,
        cast(null as varchar)   as source_system,
        cast(null as varchar)   as batch_id,
        cast(null as timestamp) as valid_from,
        cast(null as timestamp) as valid_to,
        cast(null as varchar)   as is_current
    from (select 1 as dummy) dummy_table
    where 1=0
    {% endif %}
),

-- new current rows (new emails OR changed emails)
new_records as (
    select
        s.email,
        s.first_name,
        s.last_name,
        s.city,
        s.phone_number,
        s.source_system,
        s.batch_id,
        s.ingestion_ts as valid_from,
        cast(null as timestamp) as valid_to,
        'Y' as is_current
    from latest_source s
    {% if is_incremental() %}
    where s.email not in (select email from {{ this }})
       or s.email in (select email from changed_emails)
    {% endif %}
),

-- final safety net: guarantee ONE current row per email
final as (
    select
        email,
        first_name,
        last_name,
        city,
        phone_number,
        source_system,
        batch_id,
        valid_from,
        valid_to,
        case
            when row_number() over (
                partition by email
                order by valid_from desc
            ) = 1 then 'Y'
            else 'N'
        end as is_current
    from (
        select * from unchanged_history
        union all
        select * from old_versions
        union all
        select * from new_records
    ) as combined_records
)

select * from final
