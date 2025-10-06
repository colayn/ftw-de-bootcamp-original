
  
    
    
    
        
         


        insert into `sandbox`.`grp5_oulad_dim_module_presentation__dbt_backup`
        ("code_module", "code_presentation", "length_days")

-- Dimension table for module + presentation (simple, with Nullable types)
select
    cast(code_module as Nullable(String))                           as code_module,
    cast(code_presentation as Nullable(String))                     as code_presentation,
    cast(module_presentation_length as Nullable(Int32))             as length_days
from `clean`.`grp5_stg_oulad_courses`
  