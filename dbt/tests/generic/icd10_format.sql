{% test icd10_format(model, column_name) %}
-- Fails rows whose ICD-10 code does not match the format used at ingestion.
select * from {{ model }} where not regexp_matches({{ column_name }}, '^[A-Z][0-9]{2}(\.[0-9A-Z]{1,4})?$')
{% endtest %}
