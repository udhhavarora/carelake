-- Grain: one row per encounter_id. Raw is append-only, so the latest version wins
-- (highest updated_at, tie-broken by load time). qualify is Snowflake- and DuckDB-native.
with typed as (
    select
        encounter_id,
        patient_id,
        practitioner_id,
        facility_id,
        encounter_class,
        status,
        nullif(discharge_disposition,'') as discharge_disposition,
        cast(start_ts as timestamp) as start_ts,
        cast(nullif(end_ts,'') as timestamp) as end_ts,
        cast(updated_at as timestamp) as updated_at,
        _loaded_at
    from {{ source('raw', 'encounters') }}
)
select * from typed
qualify row_number() over (partition by encounter_id order by updated_at desc, _loaded_at desc) = 1
