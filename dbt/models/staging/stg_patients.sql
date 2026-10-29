-- Grain: one row per patient_id. Raw is append-only, so the latest version wins
-- (highest updated_at, tie-broken by load time). qualify is Snowflake- and DuckDB-native.
with typed as (
    select
        patient_id,
        gender,
        postcode_area,
        cast(birth_date as date) as birth_date,
        cast(updated_at as timestamp) as updated_at,
        _loaded_at
    from {{ source('raw', 'patients') }}
)
select * from typed
qualify row_number() over (partition by patient_id order by updated_at desc, _loaded_at desc) = 1
