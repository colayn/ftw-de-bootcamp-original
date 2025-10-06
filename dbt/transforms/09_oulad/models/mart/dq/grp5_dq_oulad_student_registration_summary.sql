{{ config(materialized="view", schema="mart") }}

-- Inputs:
--   raw table:   raw.grp5_oulad___student_registration
--   clean table: clean.grp5_stg_oulad_student_registration

with src as (
    select *
    from {{ source('raw', 'grp5_oulad___student_registration') }}
),

cln as (
    select *
    from {{ source('clean', 'grp5_stg_oulad_student_registration') }}
),

counts as (
    select
        (select count() from src) as row_count_raw,
        (select count() from cln) as row_count_clean
),

nulls as (
    select
        round(100.0 * countIf(code_module is null) / nullif(count(),0), 2) as pct_null_code_module,
        round(100.0 * countIf(code_presentation is null) / nullif(count(),0), 2) as pct_null_code_presentation,
        round(100.0 * countIf(id_student is null) / nullif(count(),0), 2) as pct_null_id_student,
        round(100.0 * countIf(date_registration is null) / nullif(count(),0), 2) as pct_null_date_registration,
        round(100.0 * countIf(date_unregistration is null) / nullif(count(),0), 2) as pct_null_date_unregistration
    from cln
),

domains as (
    select
        countIf(code_module not in ('AAA','BBB','CCC','DDD','EEE','FFF','GGG')) as invalid_code_module,
        countIf(code_presentation not in ('2013B','2013J','2014B','2014J')) as invalid_code_presentation
    from cln
),

joined as (
    select
        counts.row_count_raw,
        counts.row_count_clean,
        (counts.row_count_raw - counts.row_count_clean) as dropped_rows,
        nulls.pct_null_code_module,
        nulls.pct_null_code_presentation,
        nulls.pct_null_id_student,
        nulls.pct_null_date_registration,
        nulls.pct_null_date_unregistration,
        domains.invalid_code_module,
        domains.invalid_code_presentation
    from counts
    cross join nulls
    cross join domains
)

select * from joined;
