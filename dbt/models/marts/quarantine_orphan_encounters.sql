-- Encounters whose patient_id has no matching patient. Expected to be empty in production;
-- a non-empty result is surfaced by the reconciliation test, not hidden.
select e.* from {{ ref('stg_encounters') }} e
left join {{ ref('dim_patient') }} p on p.patient_id = e.patient_id
where p.patient_id is null
