# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
from utility.utility_functions import log_progress
from utility.utility_functions import query_mc_api
import json
from utility.config import mc_queries
from utility.config.read_config import yamlConfig
from utility.monte_carlo_table import monteCarloTable
from utility.database_connection import snowflakeConnection


# Create main() that just has the main functionality and move e.g. the snowflake queries to a different file

# %% Rename everything snowflake with purpose and remove snowflake
yaml_config = yamlConfig()

database_connection = snowflakeConnection(yaml_config=yaml_config)
database_tables = database_connection.get_tables_in_schema()

# %% Writing it as functions or as a class

existing_monitors_response = query_mc_api(
    mc_queries.query_get_existing_monitors,
    query_params={"userDefinedMonitorTypes": ["stats"]})

existing_monitors = []
for edge in existing_monitors_response['getAllUserDefinedMonitorsV2']['edges']:
    existing_monitors.append(edge['node']['customRuleEntities'][0])

# %%
tables_without_monitor = []
for table in database_tables:
    if table not in existing_monitors:
        tables_without_monitor.append(table)

# %%

mc_warehouse_id = (query_mc_api(mc_queries.query_get_warehouse_id)
                   ['getUser']['account']['warehouses'][0]['uuid'])

tables_with_mc_information = {}
table_to_update_count = len(tables_without_monitor[:3])

for enum, table_name in enumerate(tables_without_monitor[:3]):
    log_progress(enum, table_to_update_count, status=f'Working on {table_name}')

    table = monteCarloTable(table_name=table_name)
    table.initialize_monte_carlo(warehouse_id=mc_warehouse_id)
    table_summary = table.save_table()
    tables_with_mc_information[table_name] = table_summary[table_name]


# %%
with open('utility/data/tables_with_mcons.json', 'w') as file:
    json.dump(tables_with_mc_information, file, indent=2)
