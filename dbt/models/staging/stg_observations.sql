-- Grain: one row per observation_id. Raw is append-only, so the latest version wins
-- (highest updated_at, tie-broken by load time). qualify is Snowflake- and DuckDB-native.
with typed as (
    select
        observation_id,
        encounter_id,
        patient_id,
        loinc_code,
        unit,
        cast(value as double) as value,
        cast(observed_at as timestamp) as observed_at,
        cast(updated_at as timestamp) as updated_at,
        _loaded_at
    from {{ source('raw', 'observations') }}
)
select * from typed
qualify row_number() over (partition by observation_id order by updated_at desc, _loaded_at desc) = 1
