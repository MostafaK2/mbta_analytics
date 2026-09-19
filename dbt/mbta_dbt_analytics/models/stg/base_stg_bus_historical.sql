{{
    config(
        materialized='view'
    )
}}

with source as (
        select * from {{ source('stg', 'bus_historical') }}
),
renamed as (
    select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['half_trip_id', 'stop_id', 'time_point_id']) }} as historical_event_id,
    {{ safe_cast('half_trip_id', 'string') }} as half_trip_id,
    {{ safe_cast('route_id', 'string') }} as route_id,
    {{ safe_cast('direction_id', 'string') }} as direction_id,
    {{ safe_cast('stop_id', 'string') }} as stop_id,
    {{ safe_cast('time_point_id', 'string') }} as time_point_id,
    {{ safe_cast('time_point_order', 'integer') }} as time_point_order,

    -- classification
    {{ safe_cast('point_type', 'string') }} as point_type,
    {{ safe_cast('standard_type', 'string') }} as standard_type,

    -- service date
    parse_date('%Y%m%d', service_date) as service_date,
    -- dates & timestamps
    safe_cast(scheduled as datetime) as scheduled_datetime,
    safe_cast(actual as datetime) as actual_datetime,
    
    -- headway metrics (seconds)
    {{ safe_cast('scheduled_headway', 'numeric') }} as scheduled_headway_seconds,
    {{ safe_cast('headway', 'numeric') }} as headway_seconds

    from source
)

select * from renamed
{% if var('is_test_run', default=true) %}
limit 100
{% endif %}