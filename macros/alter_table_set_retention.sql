{% macro alter_table_set_retention(table_relation, retention_value='interval 30 days') %}
    {# 
      Executa um comando ALTER TABLE para atualizar a propriedade delta.logRetentionDuration
      na tabela Delta.

      Parâmetros:
      - table_relation: objeto dbt Relation (por exemplo, ref('nome_da_tabela')) ou o nome da tabela como string.
      - retention_value: valor para a propriedade (padrão: 'interval 30 days')
    #}
    {% if table_relation is string %}
         {% set table_relation = ref(table_relation) %}
    {% endif %}

    {% set table_name = table_relation.identifier %}
    {% set schema_name = table_relation.schema %}
    {% set catalog_name = table_relation.database %}
    {% set full_table_name = catalog_name ~ '.' ~ schema_name ~ '.' ~ table_name %}

    {% set sql %}
      ALTER TABLE {{ full_table_name }} 
      SET TBLPROPERTIES ('delta.logRetentionDuration' = '{{ retention_value }}')
    {% endset %}

    {{ log("Running: " ~ sql, info=True) }}
    {{ run_query(sql) }}
    {{ log("Updated table " ~ full_table_name ~ " with delta.logRetentionDuration = " ~ retention_value, info=True) }}
{% endmacro %}
