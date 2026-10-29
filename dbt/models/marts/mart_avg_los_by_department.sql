-- Grain: department x discharge month. Population: finished inpatient encounters with an end_ts.
-- Computed from fct_encounter only, so multiple diagnoses cannot double count an encounter.
select
    f.department,
    date_trunc('month', e.end_ts) as discharge_month,
    count(*) as encounters,
    round(avg(e.length_of_stay_hours) / 24.0, 2) as avg_los_days
from {{ ref('fct_encounter') }} e
join {{ ref('dim_facility') }} f on f.facility_id = e.facility_id
where e.encounter_class = 'inpatient' and e.end_ts is not null
group by 1, 2
