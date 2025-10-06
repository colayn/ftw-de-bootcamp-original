

-- Dimension table for students (no surrogate key)
select
    cast(id_student as Int64)               as id_student,
    cast(gender as Nullable(String))        as gender,
    cast(age_band as Nullable(String))      as age_band,
    cast(region as Nullable(String))        as region,
    cast(highest_education as Nullable(String)) as highest_education,
    cast(imd_band as Nullable(String))      as imd_band,
    cast(disability as Nullable(String))    as disability
from `clean`.`grp5_stg_oulad_student_info`