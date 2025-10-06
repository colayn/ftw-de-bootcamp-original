{{ config(materialized="table", schema="mart", tags=["dimension","oulad"]) }}

select
    concat(cast(code_module as Nullable(String)), cast(code_presentation as Nullable(String))) 
        as id_course,
    cast(code_module as Nullable(String))                           as code_module,
    cast(code_presentation as Nullable(String))                     as code_presentation,
    cast(module_presentation_length as Nullable(Int32))             as module_presentation_length
from {{ source('clean', 'grp5_stg_oulad_courses') }}
