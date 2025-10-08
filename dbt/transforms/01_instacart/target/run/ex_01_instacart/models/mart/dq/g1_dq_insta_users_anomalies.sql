

  create view `mart`.`g1_dq_insta_users_anomalies__dbt_tmp` 
  
    
    
  as (
    

-- Row-level Data Quality Violations for Users

with cln as (
  select * from `clean`.`g1_stg_insta_users`
),
violations as (
  select
    user_id,

    multiIf(
      user_id is null, 'null_user_id',
      user_id <= 0, 'invalid_user_id',
      'ok'
    ) as dq_issue
  from cln
)
select *
from violations
where dq_issue != 'ok'
limit 100
    
  )
      
      
                    -- end_of_sql
                    
                    