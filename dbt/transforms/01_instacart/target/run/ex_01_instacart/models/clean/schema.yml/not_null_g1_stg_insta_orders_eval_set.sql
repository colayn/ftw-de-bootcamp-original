
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select eval_set
from `clean`.`g1_stg_insta_orders`
where eval_set is null



    ) dbt_internal_test