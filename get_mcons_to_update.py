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
from utility.monte_carlo_state import monteCarloState

# Create main() that just has the main functionality and move e.g. the snowflake queries to a different file

# %% 
yaml_config = yamlConfig()

database_connection = snowflakeConnection(yaml_config=yaml_config)
database_tables = database_connection.get_tables_in_schema()

# %% 

monte_carlo_state = monteCarloState()
tables_without_monitor = monte_carlo_state.find_tables_without_monitor(
    database_tables=database_tables)

# %%

tables_with_mc_information = {}
table_to_update_count = len(tables_without_monitor[:3])

print('Getting monte carlo data for tables without monitors')
for enum, table_name in enumerate(tables_without_monitor[:3]):
    log_progress(enum, table_to_update_count, status=f'Working on {table_name}')

    table = monteCarloTable(table_name=table_name)
    table.initialize_monte_carlo(warehouse_id=monte_carlo_state.warehouse_id)
    table_summary = table.save_table()
    tables_with_mc_information[table_name] = table_summary[table_name]


# %%
with open('utility/data/tables_with_mcons.json', 'w') as file:
    json.dump(tables_with_mc_information, file, indent=2)
