{{ config(materialized='view') }}

with pedidos as (
    select
        o_orderkey,
        o_custkey,
        o_orderstatus,
        o_totalprice,
        o_orderdate,
        o_orderpriority,
        o_clerk,
        o_shippriority,
        o_comment
    from {{ source('tpch', 'orders') }}
)

select
    date_trunc('month', o_orderdate) as mes_pedido,
    count(o_orderkey) as total_pedidos
from pedidos
group by date_trunc('month', o_orderdate)
order by date_trunc('month', o_orderdate)
