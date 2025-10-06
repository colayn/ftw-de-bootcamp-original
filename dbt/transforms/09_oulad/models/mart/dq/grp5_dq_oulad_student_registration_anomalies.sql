{{ config(materialized="view", schema="mart") }}

-- Row-level drilldown of "obviously wrong" records based on simple rules.
-- LIMIT for demo-friendliness; remove in real pipelines.

with cln as (
  select * from {{ source('clean', 'grp5_stg_oulad_student_registration') }}
),
violations as (
  select
    -- NOTE: this dataset has no native PK; include a synthetic row_number if needed.
    -- ClickHouse: use monotonicallyIncreasingId() isn't stable; we'll show columns directly.
    code_module, code_presentation, id_student, date_registration, date_unregistration,

    multiIf(
      code_module not in ('AAA','BBB','CCC','DDD','EEE','FFF','GGG'),  'invalid_code_module',
      code_presentation not in ('2013B', '2013J', '2014B', '2014J'), 'invalid_code_presentation',
      'ok'
    ) as dq_issue
  from cln
)
select *
from violations
where dq_issue != 'ok'
limit 50