# refatorando-deltalake-course-dbt

## VACCUM

How to Use
For example, to change the ownership of a table referenced by ref('gold_orders_by_month'), you can run the following command via dbt run-operation:

- Set TBLPROPERTIES

```bash
dbt run-operation alter_table_set_retention --args '{"table_relation": "gold_orders_by_month"}'
```

In the dbt Cloud IDE or via terminal, you can run this macro using the dbt run-operation command. For example, to run VACUUM on the gold_orders_by_month table with the default retention period:

- Running vaccum

```bash
dbt run-operation vacuum_table --args '{ "table_relation": "gold_orders_by_month", "retention_hours": 168 }'
```