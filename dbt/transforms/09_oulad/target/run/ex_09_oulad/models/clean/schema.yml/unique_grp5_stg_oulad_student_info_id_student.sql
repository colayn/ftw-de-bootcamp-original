
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    id_student as unique_field,
    count(*) as n_records

from `clean`.`grp5_stg_oulad_student_info`
where id_student is not null
group by id_student
having count(*) > 1



    ) dbt_internal_test