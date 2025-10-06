{{ config(materialized="table", schema="sandbox", tags=["dimension","oulad"]) }}

-- Dimension table for module + presentation (simple, with Nullable types)
select
    cast(code_module as Nullable(String))                           as code_module,
    cast(code_presentation as Nullable(String))                     as code_presentation,
    cast(module_presentation_length as Nullable(Int32))             as length_days
from {{ source('clean', 'grp5_stg_oulad_courses') }}
