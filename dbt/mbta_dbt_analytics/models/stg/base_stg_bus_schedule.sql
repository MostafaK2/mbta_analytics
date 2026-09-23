{{
    config(
        materialized='incremental',
        unique_key='unique_id',
        incremental_strategy='merge',
        partition_by={
            "field": "batch_loaded_at",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by=['unique_id']
    )
}}

-- Note Schedule id is non unique to routes

with source as (
  select * from {{ source('stg', 'raw_schedules') }}
  {% if is_incremental() %}
    where batch_loaded_at > (select max(batch_loaded_at) from {{ this }})
  {% endif %}
),

unnested as (
  select
    -- Identifiers 
    {{ dbt_utils.generate_surrogate_key(['schedule.id', 'route_id']) }} as unique_id,
    cast(schedule.id as string) as schedule_id,
    cast(route_id as string) as route_id,
    cast(schedule.relationships.stop.data.id as string) as stop_id,
    cast(schedule.relationships.trip.data.id as string) as trip_id,

    -- Schdule Attributes
    cast(schedule.attributes.timepoint as BOOLEAN) as timepoint,
    cast(schedule.attributes.stop_headsign as string) as stop_headsign,
    cast(schedule.attributes.drop_off_type as INTEGER) drop_off_type,
    cast(schedule.attributes.direction_id as INTEGER) as direction_id,
    cast(schedule.attributes.stop_sequence as INTEGER) as stop_sequence,
    
    -- Dates
    cast(schedule.attributes.arrival_time as TIMESTAMP) as scheduled_arrival_time,
    cast(schedule.attributes.departure_time as TIMESTAMP) as scheduled_departure_time,
    cast(service_date as TIMESTAMP) as service_date,
    cast(collected_at as TIMESTAMP) as collected_at,
    cast(batch_loaded_at as TIMESTAMP) as batch_loaded_at, 
  from source,
  unnest(data) as schedule
 
)
-- ,
-- count as (
--     select
--     count(*) as total_rows,
--     count(distinct unique_id) as distinct_unique_ids,
--     count(*) = count(distinct unique_id) as is_unique
--     from unnested
-- )


select * from unnested
{% if var('is_test_run', default=true) %}
limit 100
{% endif %}