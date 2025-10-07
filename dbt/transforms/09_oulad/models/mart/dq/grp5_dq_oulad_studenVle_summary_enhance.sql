{{ config(materialized="view", schema="mart") }}

-- Source raw and cleaned data
with src as (
    select *,
           now() as dq_run_ts
    from {{ source('raw', 'studentVle') }}
),

cln as (
    select *,
           now() as dq_run_ts
    from {{ source('clean', 'grp5_stg_oulad_studentVle') }}
),

-- Row counts for raw and cleaned
counts as (
    select
        (select count(*) from src) as row_count_raw,
        (select count(*) from cln) as row_count_clean
),

-- Null percentage checks
nulls as (
    select
        round(100.0 * countIf(id_student is null) / nullIf(count(),0),2) as pct_null_id_student,
        round(100.0 * countIf(sum_click is null) / nullIf(count(),0),2) as pct_null_sum_click
    from cln
),

-- Invalid module/presentation domain check using seed
domains as (
    select
        countIf(
            not (concat(code_module,'|',code_presentation) in (
                select concat(module,'|',presentation)
                from {{ ref('allowed_module_presentations') }}
            ))
        ) as invalid_module_presentation
    from cln
),

-- Freshness check using interaction date
freshness as (
    select
        max(date) as max_interaction_days,
        case when max(date) >= 0 then 1 else 0 end as is_fresh  -- replace 0 with your threshold if needed
    from cln
),

-- Combine all metrics
joined as (
    select
        counts.row_count_raw,
        counts.row_count_clean,
        (counts.row_count_raw - counts.row_count_clean) as dropped_rows,
        nulls.pct_null_id_student,
        nulls.pct_null_sum_click,
        domains.invalid_module_presentation,
        freshness.max_interaction_days,
        freshness.is_fresh,
        now() as dq_run_ts
    from counts
    cross join nulls
    cross join domains
    cross join freshness
)

select * from joined
