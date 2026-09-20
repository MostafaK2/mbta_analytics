{{
    config(
        materialized='incremental',
        partition_by={
            "field": "batch_loaded_at",
            "data_type": "timestamp",
            "granularity": "day"
        },
        partition_expiration_days=15 
    )
}}

-- do a higher retention policy likr 180 days for month to month analysis
with source as (
  select * from {{ source('stg', 'raw_predictions') }}
  {% if is_incremental() %}
    where batch_loaded_at > (select max(batch_loaded_at) from {{ this }})
  {% endif %}
), 
unnested as (
  select
    -- identifier
    {{ dbt_utils.generate_surrogate_key(['prediction.id', 'batch_loaded_at', 'collected_at', 'prediction.relationships.route.data.id']) }} as table_event_id,
    prediction.id as prediction_id,
    prediction.type as prediction_type,

    -- relationships : ids
    prediction.relationships.vehicle.data.id as vehicle_id,
    prediction.relationships.trip.data.id as trip_id,
    prediction.relationships.stop.data.id as stop_id,
    prediction.relationships.route.data.id as route_id,
    prediction.relationships.schedule.data.id as schedule_id,
    -- Relationship : types
    prediction.relationships.vehicle.data.type as vehicle_type,
    prediction.relationships.trip.data.type as trip_type,
    prediction.relationships.stop.data.type as stop_type,
    prediction.relationships.route.data.type as route_type,
    prediction.relationships.schedule.data.type as schedule_type,

    -- attributes (prediction)
    prediction.attributes.update_type as update_type,
    prediction.attributes.status as status,
    prediction.attributes.schedule_relationship as schedule_relationship,
    prediction.attributes.revenue as revenue,
    prediction.attributes.trip_headsign as trip_headsign,
    prediction.attributes.direction_id as direction_id,
    prediction.attributes.arrival_time as arrival_time,
    prediction.attributes.last_trip as last_trip,
    prediction.attributes.departure_uncertainty as departure_uncertainty,
    prediction.attributes.departure_time as departure_time,
    prediction.attributes.arrival_uncertainty as arrival_uncertainty,
    prediction.attributes.stop_sequence as stop_sequence,

    collected_at as collected_at,
    batch_loaded_at
  from source,
  unnest(data) as prediction
)

select * from unnested
{% if var('is_test_run', default=true) %}
limit 100
{% endif %}