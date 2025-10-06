{{ config(materialized="view", schema="mart") }}

with src as (
    select * from {{ source('raw', 'studentVle') }}
),
cln as (
    select * from {{ source('clean', 'grp5_stg_oulad_studentVle') }}
),

counts as (
    select
        (select count() from src) as row_count_raw,
        (select count() from cln) as row_count_clean
),

nulls as (
    select
        round(100.0 * countIf(id_student is null) / nullIf(count(),0),2) as pct_null_id_student,
        round(100.0 * countIf(sum_click is null) / nullIf(count(),0),2) as pct_null_sum_click
    from cln
),

domains as (
    select
        countIf(
            NOT (
                (code_module='AAA' AND code_presentation='2013J') OR
                (code_module='AAA' AND code_presentation='2014J') OR
                (code_module='BBB' AND code_presentation='2013J') OR
                (code_module='BBB' AND code_presentation='2014J') OR
                (code_module='BBB' AND code_presentation='2013B') OR
                (code_module='BBB' AND code_presentation='2014B') OR
                (code_module='CCC' AND code_presentation='2014J') OR
                (code_module='CCC' AND code_presentation='2014B') OR
                (code_module='DDD' AND code_presentation='2013J') OR
                (code_module='DDD' AND code_presentation='2014J') OR
                (code_module='DDD' AND code_presentation='2013B') OR
                (code_module='DDD' AND code_presentation='2014B') OR
                (code_module='EEE' AND code_presentation='2013J') OR
                (code_module='EEE' AND code_presentation='2014J') OR
                (code_module='EEE' AND code_presentation='2014B') OR
                (code_module='FFF' AND code_presentation='2013J') OR
                (code_module='FFF' AND code_presentation='2014J') OR
                (code_module='FFF' AND code_presentation='2013B') OR
                (code_module='FFF' AND code_presentation='2014B') OR
                (code_module='GGG' AND code_presentation='2013J') OR
                (code_module='GGG' AND code_presentation='2014J') OR
                (code_module='GGG' AND code_presentation='2014B')
            )
        ) as invalid_module_presentation
    from cln
),

joined as (
    select
        counts.row_count_raw,
        counts.row_count_clean,
        (counts.row_count_raw - counts.row_count_clean) as dropped_rows,
        nulls.pct_null_id_student,
        nulls.pct_null_sum_click,
        domains.invalid_module_presentation
    from counts
    cross join nulls
    cross join domains
)

select * from joined
