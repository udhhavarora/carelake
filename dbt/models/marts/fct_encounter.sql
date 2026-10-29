-- Grain: one row per encounter_id whose patient exists in dim_patient.
-- Encounters pointing at unknown patients are NOT dropped: they are routed to
-- quarantine_orphan_encounters so they stay visible and reconcilable.
-- Diagnoses/observations are deliberately not joined here (would multiply rows);
-- use diagnosis_count for a per-encounter summary.
select
    e.encounter_id, e.patient_id, e.practitioner_id, e.facility_id, e.encounter_class,
    e.start_ts, e.end_ts, e.discharge_disposition,
    date_diff('hour', e.start_ts, e.end_ts) as length_of_stay_hours,
    coalesce(c.diagnosis_count, 0) as diagnosis_count
from {{ ref('stg_encounters') }} e
join {{ ref('dim_patient') }} p on p.patient_id = e.patient_id
left join (
    select encounter_id, count(*) as diagnosis_count from {{ ref('stg_conditions') }} group by 1
) c on c.encounter_id = e.encounter_id
