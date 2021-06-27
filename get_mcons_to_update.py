# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
from utility.utility_functions_mc_script import query_snowflake
from utility.utility_functions_mc_script import log_progress
from utility.utility_functions_mc_script import query_mc_api
import json
from utility.config import mc_queries
from utility.config.read_config import yamlConfig
from utility.snowflake_table import snowflakeTable


# %%
yaml_config = yamlConfig()

query_get_snowflake_tables = f'''
    SELECT
        TABLE_CATALOG || ':' || TABLE_SCHEMA || '.' || TABLE_NAME
            AS TABLE_REF
    FROM  { yaml_config.snowflake_db }.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_CATALOG = '{ yaml_config.snowflake_db }' 
        AND TABLE_SCHEMA IN { yaml_config.snowflake_schemas }
        AND LAST_ALTERED >= CURRENT_DATE - { yaml_config.table_age_limit }
'''
snowflake_tables = query_snowflake(query_get_snowflake_tables)\
    .table_ref.unique()
snowflake_tables = [table.lower() for table in snowflake_tables]

# %%

existing_monitors_response = query_mc_api(
    mc_queries.query_get_existing_monitors,
    query_params={"userDefinedMonitorTypes": ["stats"]})

existing_monitors = []
for edge in existing_monitors_response['getAllUserDefinedMonitorsV2']['edges']:
    existing_monitors.append(edge['node']['customRuleEntities'][0])

# %%
tables_without_monitor = []
for table in snowflake_tables:
    if table not in existing_monitors:
        tables_without_monitor.append(table)

# %%

mc_warehouse_id = (query_mc_api(mc_queries.query_get_warehouse_id)
                   ['getUser']['account']['warehouses'][0]['uuid'])

tables_with_mc_information = {}
table_to_update_count = len(tables_without_monitor[:3])

for enum, table_name in enumerate(tables_without_monitor[:3]):
    log_progress(enum, table_to_update_count, status=f'Working on {table}')

    table = snowflakeTable(table_name=table_name, warehouse_id=mc_warehouse_id)
    table_summary = table.save_table()
    tables_with_mc_information[table_name] = table_summary[table_name]


# %%
with open('utility/data/tables_with_mcons.json', 'w') as file:
    json.dump(tables_with_mc_information, file, indent=2)
