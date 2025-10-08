

-- Clean layer: Users table  
-- Purpose: Normalize and standardize distinct users extracted from orders.  
-- Primary Key: user_id  
-- Foreign Key: None (referenced by orders.user_id)

select distinct
  cast(user_id as Int64) as user_id
from `clean`.`g1_stg_insta_orders`