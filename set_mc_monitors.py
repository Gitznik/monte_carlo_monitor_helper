# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import json

from utility.utility_functions_mc_script import log_progress, query_mc_api
from config import mc_queries

monitorable_time_fields = [
    'servertime',
    'server_time',
    'serverdate',
    'server_date']

set_monitor_blueprint_param = {
    "mcon": None,
    "monitorType": "stats",
    "fields": None,
    "timeAxisType": None,
    "timeAxisName": None,
    "scheduleConfig": {
        "scheduleType": "LOOSE",
        "intervalMinutes": 720
    }
}


def find_timefield_to_monitor(
        table_name,
        mcons_to_set,
        monitorable_time_fields=monitorable_time_fields):

    timefields_in_table = []
    for timefield in mcons_to_set[table_name]['time_fields']:
        timefields_in_table.append(timefield)
    available_timefield_count = len(timefields_in_table)

    timefield_to_monitor = None
    for monitorable_time_field in monitorable_time_fields:
        if monitorable_time_field in timefields_in_table:
            timefield_to_monitor = monitorable_time_field
            break

    time_axis_type = mcons_to_set[table_name]['time_fields'].get(
        timefield_to_monitor)

    return timefield_to_monitor, time_axis_type, available_timefield_count


def set_monitor(
        table,
        timefield_to_monitor,
        time_axis_type,
        available_timefield_count,
        monitor_without_timefield=False,
        set_monitor_blueprint_param=set_monitor_blueprint_param):

    if (available_timefield_count is None and
        monitor_without_timefield is False) or \
        (available_timefield_count > 0 and
         timefield_to_monitor is None):
        return table

    mcon = mcons_to_set[table]['mcon']

    set_monitor_blueprint_param["mcon"] = mcon
    set_monitor_blueprint_param["timeAxisName"] = timefield_to_monitor
    set_monitor_blueprint_param["timeAxisType"] = time_axis_type

    query_mc_api(
        query_string=mc_queries.query_create_monitor,
        query_params=set_monitor_blueprint_param
    )
    print('Monitor created for: ' + table)


# %%
with open('tables_with_mcons.json') as file:
    mcons_to_set = json.load(file)

# %%
create_monitor_manually = []
monitors_to_set_count = len(mcons_to_set)
for enum, table in enumerate(mcons_to_set):
    log_progress(enum, monitors_to_set_count, status=f'Working on {table}')

    timefield_to_monitor, time_axis_type, available_timefield_count = \
        find_timefield_to_monitor(
            table,
            mcons_to_set,
            monitorable_time_fields)

    if table_to_monitor_manually := set_monitor(
        table,
        timefield_to_monitor,
        time_axis_type,
        available_timefield_count,
        monitor_without_timefield=False
    ): create_monitor_manually.append(table_to_monitor_manually)

print(f'Create monitor manually for: {create_monitor_manually}')


# %%
with open('set_monitor_manually.json', 'w') as file:
    json.dump(create_monitor_manually, file)
