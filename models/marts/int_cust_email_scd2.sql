{{ config(
    materialized = 'incremental',
    unique_key = 'email',
    incremental_strategy = 'delete+insert'
) }}

with source_data as (

    select
        email,
        first_name,
        last_name,
        city,
        phone_number,
        source_system,
        ingestion_ts
    from {{ ref('stg_customers') }}

    {% if is_incremental() %}
      where ingestion_ts >
            (select coalesce(max(valid_from), '1900-01-01') from {{ this }})
    {% endif %}

),

combined as (

    {% if is_incremental() %}

        -- existing history FOR AFFECTED EMAILS ONLY
        select
            h.email,
            h.first_name,
            h.last_name,
            h.city,
            h.phone_number,
            h.source_system,
            h.valid_from
        from {{ this }} h
        inner join source_data s
            on h.email = s.email

        union all

        -- new incoming data
        select
            email,
            first_name,
            last_name,
            city,
            phone_number,
            source_system,
            ingestion_ts as valid_from
        from source_data

    {% else %}

        -- first run / full refresh
        select
            email,
            first_name,
            last_name,
            city,
            phone_number,
            source_system,
            ingestion_ts as valid_from
        from source_data

    {% endif %}
),

ordered as (

    select
        *,
        lag(first_name)    over (partition by email order by valid_from) as prev_first_name,
        lag(last_name)     over (partition by email order by valid_from) as prev_last_name,
        lag(city)          over (partition by email order by valid_from) as prev_city,
        lag(phone_number)  over (partition by email order by valid_from) as prev_phone_number,
        lag(source_system) over (partition by email order by valid_from) as prev_source_system
    from combined
),

changes_only as (

    select *
    from ordered
    where
          prev_first_name is null
       or coalesce(first_name, '')    <> coalesce(prev_first_name, '')
       or coalesce(last_name, '')     <> coalesce(prev_last_name, '')
       or coalesce(city, '')          <> coalesce(prev_city, '')
       or coalesce(phone_number, '')  <> coalesce(prev_phone_number, '')
       or coalesce(source_system, '') <> coalesce(prev_source_system, '')
)

select
    email,
    first_name,
    last_name,
    city,
    phone_number,
    source_system,

    valid_from,

    lead(valid_from)
        over (partition by email order by valid_from) as valid_to,

    case
        when lead(valid_from)
             over (partition by email order by valid_from) is null
        then 'Y' else 'N'
    end as is_current

from changes_only