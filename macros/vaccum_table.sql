{% macro vacuum_table(table_relation, retention_hours=168) %}
    {#
      Executa o comando VACUUM em uma tabela Delta para limpar arquivos antigos.
      
      Parâmetros:
      - table_relation: objeto dbt Relation (ex.: ref('nome_da_tabela')) ou nome da tabela como string.
      - retention_hours: período de retenção para o VACUUM (padrão: 168 horas).
    #}
    {% if table_relation is string %}
         {% set table_relation = ref(table_relation) %}
    {% endif %}

    {% set table_name = table_relation.identifier %}
    {% set schema_name = table_relation.schema %}
    {% set catalog_name = table_relation.database %}
    {% set full_table_name = catalog_name ~ '.' ~ schema_name ~ '.' ~ table_name %}

    {% set sql %}
      VACUUM {{ full_table_name }} RETAIN {{ retention_hours }} HOURS
    {% endset %}

    {{ log("Running: " ~ sql, info=True) }}
    {{ run_query(sql) }}
    {{ log("Executed VACUUM on table " ~ full_table_name ~ " with retention " ~ retention_hours ~ " hours", info=True) }}
{% endmacro %}
