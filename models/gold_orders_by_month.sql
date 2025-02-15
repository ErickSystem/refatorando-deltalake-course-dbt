{{ config(materialized='table') }}

with orders_raw as (
    select
        date_trunc('month', o.o_orderdate) as order_month,
        o.o_orderkey,
        o.o_custkey,
        li.l_suppkey,
        o.o_totalprice
    from {{ source('tpch', 'orders') }} o
    inner join {{ source('tpch', 'lineitem') }} li 
        on o.o_orderkey = li.l_orderkey
),

orders_with_dims as (
    select
        r.order_month,
        c.c_name as customer_name,
        s.s_name as supplier_name,
        s.region_name as supplier_region,
        r.o_orderkey,
        r.o_totalprice
    from orders_raw r
    left join {{ ref('dim_customer') }} c 
        on r.o_custkey = c.c_custkey
    left join {{ ref('dim_supplier') }} s 
        on r.l_suppkey = s.s_suppkey
)

select 
    order_month,
    customer_name,
    supplier_name,
    supplier_region,
    count(distinct o_orderkey) as orders_count,
    sum(o_totalprice) as total_revenue
from orders_with_dims
group by order_month, customer_name, supplier_name, supplier_region
order by order_month, customer_name, supplier_name, supplier_region
