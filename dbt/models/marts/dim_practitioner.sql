-- Grain: one row per practitioner_id.
select practitioner_id, full_name, specialty, facility_id from {{ ref('stg_practitioners') }}
