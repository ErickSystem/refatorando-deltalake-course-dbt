{{ config(materialized='table') }}

select 
    c.c_custkey,
    c.c_name,
    c.c_address,
    c.c_nationkey,
    c.c_phone,
    c.c_acctbal,
    c.c_mktsegment,
    c.c_comment
from {{ source('tpch', 'customer') }} c
