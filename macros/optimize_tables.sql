{% macro optimize_table(table_relation, zorder_columns) %}
    {# 
    Executa o comando OPTIMIZE com ZORDER para a tabela informada.
    - table_relation: objeto dbt Relation (por exemplo, ref('dim_customer'))
    - zorder_columns: string com as colunas para z-ordering, ex: 'c_custkey'
  #}
    {% set table_name = table_relation.identifier %}
    {% set schema_name = table_relation.schema %}
    {% set catalog_name = table_relation.database %}

    {% set full_table_name = catalog_name ~ "." ~ schema_name ~ "." ~ table_name %}

    {% set sql %}
      OPTIMIZE {{ full_table_name }} ZORDER BY ({{ zorder_columns }})
    {% endset %}

    {{ log("Running: " ~ sql, info=True) }}
    {{ run_query(sql) }}

    {{ log("Optimized table " ~ full_table_name ~ " with ZORDER BY (" ~ zorder_columns ~ ")", info=True) }}
{% endmacro %}

{% macro optimize_all_tables() %}
    {# 
    Define as tabelas a otimizar e seus respectivos zorder columns.
    Adapte conforme a necessidade.
  #}
    {% set tables_to_optimize = [
        {"relation": ref("dim_customer"), "zorder": "c_custkey"},
        {"relation": ref("dim_supplier"), "zorder": "s_suppkey"},
        {"relation": ref("gold_orders_by_month"), "zorder": "order_month"}
    ] %}

    {% for t in tables_to_optimize %}
        {% do optimize_table(t.relation, t.zorder) %}
    {% endfor %}
{% endmacro %}
