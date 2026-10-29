-- Grain: one row per facility_id (a hospital + department combination).
select facility_id, facility_name, department from {{ ref('stg_facilities') }}
