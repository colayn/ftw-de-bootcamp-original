
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select order_hour_of_day
from `clean`.`g1_stg_insta_orders`
where order_hour_of_day is null



    ) dbt_internal_test