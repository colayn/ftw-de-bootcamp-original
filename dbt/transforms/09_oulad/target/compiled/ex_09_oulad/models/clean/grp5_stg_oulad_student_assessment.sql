 

-- Standardize names and types for student assessment
select
  cast(id_assessment  as Nullable(Int64)) as assessment_id,
  cast(id_student     as Nullable(Int64)) as student_id,
  cast(date_submitted as Nullable(Int64)) as date_submitted,
  cast(is_banked      as Nullable(Int64)) as is_banked,
  cast(score          as Nullable(Int64)) as score
from `raw`.`grp5_oulad___student_assessment`