{{ config(materialized='table') }}

with supplier_details as (
    select 
        s.s_suppkey,
        s.s_name,
        s.s_address,
        s.s_nationkey,
        s.s_phone,
        s.s_acctbal,
        s.s_comment,
        n.n_name as nation_name,
        n.n_regionkey,
        r.r_name as region_name,
        r.r_comment as region_comment
    from {{ source('tpch', 'supplier') }} s
    left join {{ source('tpch', 'nation') }} n 
        on s.s_nationkey = n.n_nationkey
    left join {{ source('tpch', 'region') }} r 
        on n.n_regionkey = r.r_regionkey
)

select * from supplier_details
