{{
    config(
        materialized='view'
    )
}}

-- Grain: one row per prediction event (stg_raw_predictions.table_event_id),
-- enriched with the scheduled arrival/departure for that trip/stop and the
-- resulting delay. This is the base table for on-time-performance analysis.
--
-- Predictions carry a direct schedule_id relationship, so that's the primary
-- join key; trip_id/stop_id/route_id are carried through for validation and
-- for the (rarer) predictions that arrive without a schedule_id, e.g. added
-- or unscheduled trips.
--
-- GTFS schedule times can exceed 24:00:00 to represent a trip that departs
-- after midnight but still belongs to the previous service day (e.g.
-- "25:10:00" = 1:10 AM the next day). We parse hours/minutes/seconds out of
-- the HH:MM:SS string ourselves and add the >=24h portion as a day offset,
-- anchored to the calendar date of the prediction itself (predictions land
-- close in time to the real event, so this is a safe proxy for service date
-- without pulling in a separate calendar/service_date model).

with predictions as (
    select * from {{ ref('stg_raw_predictions') }}
),

schedule as (
    select * from {{ ref('stg_bus_schedule') }}
),

joined as (
    select
        p.table_event_id,
        p.prediction_id,
        p.route_id,
        p.trip_id,
        p.stop_id,
        p.vehicle_id,
        p.schedule_id,
        p.direction_id,
        p.stop_sequence,
        p.trip_headsign,
        p.status,
        p.schedule_relationship,
        p.update_type,
        p.revenue,
        p.last_trip,
        p.arrival_time as predicted_arrival,
        p.departure_time as predicted_departure,
        p.arrival_uncertainty,
        p.departure_uncertainty,
        p.collected_at,
        p.batch_loaded_at,

        s.schedule_id is not null as has_schedule,
        s.scheduled_arrival as scheduled_arrival_raw,
        s.scheduled_departure as scheduled_departure_raw,
        s.stop_headsign as scheduled_stop_headsign,
        s.pickup_type,
        s.drop_off_type,
        s.timepoint

    from predictions p
    left join schedule s
        on p.schedule_id = s.schedule_id
),

parsed as (
    select
        *,

        -- anchor date for reconstructing a full scheduled timestamp
        coalesce(date(predicted_arrival), date(predicted_departure)) as service_date_proxy,

        cast(split(scheduled_arrival_raw, ':')[safe_offset(0)] as int64) as sched_arrival_hour,
        cast(split(scheduled_arrival_raw, ':')[safe_offset(1)] as int64) as sched_arrival_minute,
        cast(split(scheduled_arrival_raw, ':')[safe_offset(2)] as int64) as sched_arrival_second,

        cast(split(scheduled_departure_raw, ':')[safe_offset(0)] as int64) as sched_departure_hour,
        cast(split(scheduled_departure_raw, ':')[safe_offset(1)] as int64) as sched_departure_minute,
        cast(split(scheduled_departure_raw, ':')[safe_offset(2)] as int64) as sched_departure_second

    from joined
),

reconstructed as (
    select
        *,

        case when scheduled_arrival_raw is not null then
            timestamp_add(
                timestamp_add(
                    timestamp(service_date_proxy),
                    interval div(sched_arrival_hour, 24) day
                ),
                interval (
                    mod(sched_arrival_hour, 24) * 3600
                    + sched_arrival_minute * 60
                    + sched_arrival_second
                ) second
            )
        end as scheduled_arrival_ts,

        case when scheduled_departure_raw is not null then
            timestamp_add(
                timestamp_add(
                    timestamp(service_date_proxy),
                    interval div(sched_departure_hour, 24) day
                ),
                interval (
                    mod(sched_departure_hour, 24) * 3600
                    + sched_departure_minute * 60
                    + sched_departure_second
                ) second
            )
        end as scheduled_departure_ts

    from parsed
)

select
    table_event_id,
    prediction_id,
    route_id,
    trip_id,
    stop_id,
    vehicle_id,
    schedule_id,
    direction_id,
    stop_sequence,
    trip_headsign,
    scheduled_stop_headsign,
    status,
    schedule_relationship,
    update_type,
    revenue,
    last_trip,
    pickup_type,
    drop_off_type,
    timepoint,
    has_schedule,

    predicted_arrival,
    predicted_departure,
    scheduled_arrival_ts as scheduled_arrival,
    scheduled_departure_ts as scheduled_departure,
    arrival_uncertainty,
    departure_uncertainty,

    timestamp_diff(predicted_arrival, scheduled_arrival_ts, second) as arrival_delay_seconds,
    timestamp_diff(predicted_departure, scheduled_departure_ts, second) as departure_delay_seconds,

    collected_at,
    batch_loaded_at

from reconstructed