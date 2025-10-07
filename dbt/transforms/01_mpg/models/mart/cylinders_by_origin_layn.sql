{{ config(materialized="view", schema="mart") }}

select
  origin,
  avg(cylinders) as avg_cylinders,
  count()        as n
from {{ source('clean','mpg_standardized_layn') }}
group by origin
