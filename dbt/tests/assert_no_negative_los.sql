select encounter_id from {{ ref('fct_encounter') }} where length_of_stay_hours < 0
