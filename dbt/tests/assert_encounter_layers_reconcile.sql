-- Every staged encounter must land in exactly one place: fct_encounter or the orphan quarantine.
select s.n_stg, f.n_fct, q.n_orph
from (select count(*) n_stg from {{ ref('stg_encounters') }}) s,
     (select count(*) n_fct from {{ ref('fct_encounter') }}) f,
     (select count(*) n_orph from {{ ref('quarantine_orphan_encounters') }}) q
where s.n_stg <> f.n_fct + q.n_orph
