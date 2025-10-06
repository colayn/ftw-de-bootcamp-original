

  create view `mart`.`grp5_dq_oulad_studentVle_anomalies__dbt_tmp` 
  
    
    
  as (
    

with cln as (
    select * from `clean`.`grp5_stg_oulad_studentVle`
),

violations as (
    select
        code_module,
        code_presentation,
        id_student,
        id_site,
        date,
        sum_click,

        multiIf(
            id_student is null,                 'null_id_student',
            sum_click is null,                  'null_sum_click',
            NOT (
                (code_module='AAA' AND code_presentation='2013J') OR
                (code_module='AAA' AND code_presentation='2014J') OR
                (code_module='BBB' AND code_presentation='2013J') OR
                (code_module='BBB' AND code_presentation='2014J') OR
                (code_module='BBB' AND code_presentation='2013B') OR
                (code_module='BBB' AND code_presentation='2014B') OR
                (code_module='CCC' AND code_presentation='2014J') OR
                (code_module='CCC' AND code_presentation='2014B') OR
                (code_module='DDD' AND code_presentation='2013J') OR
                (code_module='DDD' AND code_presentation='2014J') OR
                (code_module='DDD' AND code_presentation='2013B') OR
                (code_module='DDD' AND code_presentation='2014B') OR
                (code_module='EEE' AND code_presentation='2013J') OR
                (code_module='EEE' AND code_presentation='2014J') OR
                (code_module='EEE' AND code_presentation='2014B') OR
                (code_module='FFF' AND code_presentation='2013J') OR
                (code_module='FFF' AND code_presentation='2014J') OR
                (code_module='FFF' AND code_presentation='2013B') OR
                (code_module='FFF' AND code_presentation='2014B') OR
                (code_module='GGG' AND code_presentation='2013J') OR
                (code_module='GGG' AND code_presentation='2014J') OR
                (code_module='GGG' AND code_presentation='2014B')
            ), 'invalid_module_presentation',
            'ok'
        ) as dq_issue
    from cln
)

select *
from violations
where dq_issue != 'ok'
    
  )
      
      
                    -- end_of_sql
                    
                    