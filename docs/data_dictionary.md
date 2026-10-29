# Data dictionary

All data is synthetic. Structures are FHIR-*inspired* (Patient, Encounter, Condition, Observation,
Practitioner, Location-like facility). **Not FHIR conformant and not validated against FHIR.**
Source: `SCHEMAS` in `src/carelake/data_generation/generator.py`. All raw columns land as text
(`all_varchar`); typing happens in dbt staging.

| Entity | Key | Columns (besides key) |
|---|---|---|
| facilities | facility_id | facility_name, department, updated_at |
| practitioners | practitioner_id | full_name, specialty, facility_id, updated_at |
| patients | patient_id | birth_date, gender, postcode_area, updated_at |
| encounters | encounter_id | patient_id, practitioner_id, facility_id, encounter_class, start_ts, end_ts, status, updated_at, *discharge_disposition (added in batch 2)* |
| conditions | condition_id | encounter_id, patient_id, icd10_code, recorded_at, updated_at |
| observations | observation_id | encounter_id, patient_id, loinc_code, value, unit, observed_at, updated_at |

Timestamps are UTC, format `YYYY-MM-DDTHH:MM:SSZ`. `updated_at` is the version marker: the highest
value per key wins in staging (ties broken by `_loaded_at`).

## Generation rules and limitations
- Seeded (`--seed`, default 42) so output is byte-identical (tested).
- Defaults: 200 patients, 20 practitioners, 3 facilities x 5 departments, 400 + 100 encounters.
- Batch 1 spans 60 days before 2025-01-01; batch 2 is loaded the next day.
- Inpatient stays last 1-10 days; other classes 1-8 hours. 1-3 conditions and 1-4 observations per encounter.
- **Limitations:** department is assigned independently of encounter class (so "Emergency" has
  inpatient stays); patients re-visit at random, which inflates readmission rates; ICD-10 and LOINC
  codes are a small fixed list; observation values are uniform random, not clinically plausible.

## Injected anomalies (batch 2; ground truth in `anomalies.json`)
| Anomaly | Handled by |
|---|---|
| Exact duplicate encounter row | Ingestion quarantine (`exact_duplicate`) |
| Invalid ICD-10 code | Ingestion quarantine (`invalid_code:icd10_code`) |
| Null required field (patient_id) | Ingestion quarantine (`null_required:patient_id`) |
| End before start | Ingestion quarantine (`end_before_start`) |
| Corrected encounter / patient | Staging keeps the latest `updated_at` |
| Late-arriving encounter (started 10 days before batch 1) | Appears in the fact table; historical-stability check can flag changed months |
| Orphan encounter (unknown patient) | `quarantine_orphan_encounters`; DQ warning under 1%, block above |
| New column `discharge_disposition` | Reported by ingestion; raw always carries the column |

## Analytical models (`dbt/models/marts`)
| Model | Grain | Notes |
|---|---|---|
| dim_patient / dim_facility / dim_practitioner | one row per id | current version only, no history |
| fct_encounter | one row per encounter with a known patient | no diagnosis join, so no double counting; `diagnosis_count` summarises |
| quarantine_orphan_encounters | one row per orphan encounter | should be empty in a healthy feed |
| mart_avg_los_by_department | department x discharge month | finished inpatient encounters only |
| mart_readmission_rate | discharge month | see definition below |

**30-day readmission:** index = inpatient encounter with an `end_ts`, excluding discharge
disposition `deceased` and `transfer` (NULL kept). Readmitted = same patient has another inpatient
encounter starting after the index end and within 30 days. Each index counts once. Limitation: the
latest 30 days of discharges are right-censored, because follow-ups may not have arrived yet.
