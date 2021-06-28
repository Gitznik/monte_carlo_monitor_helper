# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import json

from utility.utility_functions import log_progress
from utility.snowflake_table import snowflakeTable

# %%
with open('utility/data/tables_with_mcons.json') as file:
    mcons_to_set = json.load(file)

# %%
create_monitor_manually = []
monitors_to_set_count = len(mcons_to_set)
for enum, table_name in enumerate(mcons_to_set):
    log_progress(enum, monitors_to_set_count, status=f'Working on {table_name}')

    table = snowflakeTable(table_name= table_name)
    table.initialize_saved_state(saved_state= mcons_to_set[table_name])

    if table_to_monitor_manually := table.set_monitor(
            monitor_without_timefield= False):
        create_monitor_manually.append(table_to_monitor_manually)

print(f'Create monitor manually for: {create_monitor_manually}')


# %%
with open('utility/data/set_monitor_manually.json', 'w') as file:
    json.dump(create_monitor_manually, file)
