
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select user_id
from `clean`.`g1_stg_insta_users`
where user_id is null



    ) dbt_internal_test