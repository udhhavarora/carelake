-- Grain: one row per condition_id. Raw is append-only, so the latest version wins
-- (highest updated_at, tie-broken by load time). qualify is Snowflake- and DuckDB-native.
with typed as (
    select
        condition_id,
        encounter_id,
        patient_id,
        icd10_code,
        cast(recorded_at as timestamp) as recorded_at,
        cast(updated_at as timestamp) as updated_at,
        _loaded_at
    from {{ source('raw', 'conditions') }}
)
select * from typed
qualify row_number() over (partition by condition_id order by updated_at desc, _loaded_at desc) = 1
