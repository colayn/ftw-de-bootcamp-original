
    
    

with all_values as (

    select
        gender as value_field,
        count(*) as n_records

    from `clean`.`grp5_stg_oulad_student_info`
    group by gender

)

select *
from all_values
where value_field not in (
    'M','F','Unknown'
)


