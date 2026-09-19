{{
    config(
        materialized='view'
    )
}}

with source as (
  select * from {{ source('stg', 'raw_predictions') }}
), 
unnested as (
  select
    -- identifier
    prediction.id as prediction_id,
    prediction.type as prediction_type,

    -- relationships : ids
    prediction.relationships.vehicle.data.id as vehicle_id,
    prediction.relationships.trip.data.id as trip_id,
    prediction.relationships.stop.data.id as stop_id,
    prediction.relationships.route.data.id as route_id,
    prediction.relationships.schedule.data.id as schedule_id,
    -- Relationship : types
    prediction.relationships.vehicle.data.type as vehicle_id,
    prediction.relationships.trip.data.type as trip_id,
    prediction.relationships.stop.data.type as stop_id,
    prediction.relationships.route.data.type as route_id,
    prediction.relationships.schedule.data.type as schedule_id,

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
  from source,
  unnest(data) as prediction
)

select * from unnested
{% if var('is_test_run', default=true) %}
limit 100
{% endif %}