
  
    
    
    
        
         


        insert into `clean`.`grp5_stg_oulad_student_info__dbt_backup`
        ("code_module", "code_presentation", "id_student", "gender", "region", "highest_education", "imd_band", "age_band", "num_of_prev_attempts", "studied_credits", "disability", "final_result")
select
  cast(code_module            as String)     as code_module,
  cast(code_presentation      as String)     as code_presentation,
  cast(id_student             as Int64)      as id_student,
  cast(
    case
      when gender IN ('M', 'F') then gender
      else 'Unknown'
    end as String) as gender,
  cast(region                 as String)     as region,
  cast(highest_education      as String)     as highest_education,
  cast(
    case
        when imd_band = '?' then null
        when imd_band not like '%\%%' then concat(imd_band, '%')
        else imd_band
    end as Nullable(String)) as imd_band,
  cast(age_band               as String)     as age_band,
  cast(num_of_prev_attempts   as Int64)      as num_of_prev_attempts,
  cast(studied_credits        as Int64)      as studied_credits,
  cast(disability             as String)     as disability,
  cast(final_result           as String)     as final_result
from `raw`.`grp5_oulad___student_info`
  