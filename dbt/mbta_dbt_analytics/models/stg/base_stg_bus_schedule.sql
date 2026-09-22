{# {{
    config(
        materialized='incremental',
        unique_key='schedule_id',
        incremental_strategy='merge',
        partition_by={
            "field": "last_seen_at",
            "data_type": "timestamp",
            "granularity": "day"
        }
    )
}}


with source as (
    select included, batch_loaded_at from {{ source('stg', 'raw_predictions') }}

    {% if is_incremental() %}
    where batch_loaded_at > (select max(last_seen_at) from {{ this }})
    {% endif %}
),

unnested as (
    select
        batch_loaded_at,
        included
    from source,
    unnest(included) as included
    where included.type = 'schedule'
),
deduped as (
    select
        *,
        row_number() over (
            partition by included.id
            order by batch_loaded_at desc
        ) as rn
    from unnested
),
outputsql as (
    select
        included.id as schedule_id,
        included.relationships.route.data.id as route_id,
        included.relationships.trip.data.id as trip_id,
        included.relationships.stop.data.id as stop_id,

        included.attributes.direction_id as direction_id,
        included.attributes.stop_sequence as stop_sequence,
        included.attributes.stop_headsign as stop_headsign,
        included.attributes.pickup_type as pickup_type,
        included.attributes.drop_off_type as drop_off_type,
        included.attributes.timepoint as timepoint,
        included.attributes.arrival_time as scheduled_arrival,
        included.attributes.departure_time as scheduled_departure,

        batch_loaded_at as last_seen_at

    from deduped
    where rn = 1
)

select * from outputsql
{% if var('is_test_run', default=true) %}
limit 100
{% endif %} #}