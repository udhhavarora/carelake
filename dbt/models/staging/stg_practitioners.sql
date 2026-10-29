-- Grain: one row per practitioner_id. Raw is append-only, so the latest version wins
-- (highest updated_at, tie-broken by load time). qualify is Snowflake- and DuckDB-native.
with typed as (
    select
        practitioner_id,
        full_name,
        specialty,
        facility_id,
        cast(updated_at as timestamp) as updated_at,
        _loaded_at
    from {{ source('raw', 'practitioners') }}
)
select * from typed
qualify row_number() over (partition by practitioner_id order by updated_at desc, _loaded_at desc) = 1
