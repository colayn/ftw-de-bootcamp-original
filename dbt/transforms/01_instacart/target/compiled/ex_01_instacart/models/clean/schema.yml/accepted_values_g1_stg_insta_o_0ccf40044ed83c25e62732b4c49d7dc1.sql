
    
    

with all_values as (

    select
        eval_set as value_field,
        count(*) as n_records

    from `clean`.`g1_stg_insta_orders`
    group by eval_set

)

select *
from all_values
where value_field not in (
    'prior','train','test'
)


