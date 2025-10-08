
  
    
    
    
        
         


        insert into `clean`.`g1_stg_insta_orders__dbt_backup`
        ("order_id", "user_id", "eval_set", "order_number", "order_dow", "order_hour_of_day", "days_since_prior_order")

-- Clean layer: Orders table
-- Purpose: Standardize data types and preserve NaN values for numeric continuity.
-- Primary Key: order_id
-- Foreign Key: user_id → g1_stg_insta_users.user_id

select
  cast(order_id as Int64)               as order_id,
  cast(user_id as Int64)                as user_id,
  cast(eval_set as String)              as eval_set,
  cast(order_number as Int64)           as order_number,
  cast(order_dow as Int64)              as order_dow,
  cast(order_hour_of_day as Int64)      as order_hour_of_day,
  cast(days_since_prior_order as Float64) as days_since_prior_order
from `raw`.`raw___insta_orders`
  