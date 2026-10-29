-- 30-day readmission, by index discharge month.
-- Index population: inpatient encounters with an end_ts, excluding discharge_disposition
--   'deceased' and 'transfer' (NULL disposition is kept; known limitation for batch-1 data).
-- Readmission: another inpatient encounter for the same patient starting after the index
--   end_ts and within 30 days. Each index is counted once however many readmissions follow.
-- Limitation: readmissions are only observable if the follow-up is already loaded, so the
--   most recent 30 days of discharges are right-censored.
with idx as (
    select * from {{ ref('fct_encounter') }}
    where encounter_class = 'inpatient' and end_ts is not null
      and coalesce(discharge_disposition, 'unknown') not in ('deceased', 'transfer')
),
flagged as (
    select i.encounter_id, i.end_ts,
        exists (
            select 1 from {{ ref('fct_encounter') }} r
            where r.patient_id = i.patient_id and r.encounter_class = 'inpatient'
              and r.start_ts > i.end_ts and r.start_ts <= i.end_ts + interval 30 day
        ) as readmitted_30d
    from idx i
)
select
    date_trunc('month', end_ts) as discharge_month,
    count(*) as index_encounters,
    sum(case when readmitted_30d then 1 else 0 end) as readmissions,
    round(sum(case when readmitted_30d then 1 else 0 end) * 100.0 / count(*), 2) as readmission_rate_pct
from flagged group by 1
