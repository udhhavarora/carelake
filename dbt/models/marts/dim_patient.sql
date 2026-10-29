-- Grain: one row per patient_id (current version; no history kept in this slice).
select patient_id, birth_date, gender, postcode_area from {{ ref('stg_patients') }}
