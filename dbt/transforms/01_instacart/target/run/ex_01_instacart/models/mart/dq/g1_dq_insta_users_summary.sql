

  create view `mart`.`g1_dq_insta_users_summary` 
  
    
    
  as (
    

-- Data Quality Summary for Users

with 
rawDs as (
    select count(distinct user_id) as row_count_raw
    from `raw`.`raw___insta_orders`
),
cln as (
    select * from `clean`.`g1_stg_insta_users`
),
counts_clean as (
    select count(*) as row_count_clean from cln
),
nulls as (
    select round(100.0 * countIf(user_id is null) / nullif(count(),0), 2) as null_pct_user_id
    from cln
),
dupes as (
    select countIf(cnt > 1) as duplicate_user_ids
    from (
        select user_id, count() as cnt
        from cln
        group by user_id
    )
),
invalid_users as (
    select count(*) as invalid_user_in_orders
    from `clean`.`g1_stg_insta_orders`
    where user_id not in (select user_id from cln)
),
users_without_orders as (
    select count(*) as user_without_orders
    from cln
    where user_id not in (select user_id from `clean`.`g1_stg_insta_orders`)
)

select 
    r.row_count_raw,
    c.row_count_clean,
    (r.row_count_raw - c.row_count_clean) as dropped_rows,
    n.null_pct_user_id,
    d.duplicate_user_ids,
    iu.invalid_user_in_orders,
    uwo.user_without_orders
from (select * from rawDs) r
cross join (select * from counts_clean) c
cross join (select * from nulls) n
cross join (select * from dupes) d
cross join (select * from invalid_users) iu
cross join (select * from users_without_orders) uwo
    
  )
      
      
                    -- end_of_sql
                    
                    