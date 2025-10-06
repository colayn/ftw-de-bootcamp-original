
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select id_student
from `clean`.`grp5_stg_oulad_student_info`
where id_student is null



    ) dbt_internal_test