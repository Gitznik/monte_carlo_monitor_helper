from datetime import datetime, timezone
import json
import os

from src.utility_functions import log_progress
from src.config.read_config import yamlConfig
from src.monte_carlo_table import monteCarloTable
from src.database_connection import snowflakeConnection
from src.monte_carlo_state import monteCarloState

def main():
    yaml_config = yamlConfig()

    database_tables = snowflakeConnection(
        yaml_config=yaml_config).get_tables_in_schema()

    monte_carlo_state = monteCarloState()
    tables_without_monitor = monte_carlo_state.find_tables_without_monitor(
        database_tables=database_tables,
        table_blacklist=yaml_config.table_blacklist)

    tables_to_monitor = {}
    tables_to_monitor_manually = {}
    table_to_update_count = len(tables_without_monitor)

    print('Getting monte carlo data for tables without monitors')
    for enum, table_name in enumerate(tables_without_monitor):
        log_progress(
            enum, 
            table_to_update_count, 
            status=f'Working on {table_name}')

        table = monteCarloTable(table_name=table_name)
        table.initialize_monte_carlo(
            warehouse_id=monte_carlo_state.warehouse_id)
        table.evaluate_is_monitorable(monitor_without_timefield=False)

        if table.monitorable:
            table_summary = table.save_table()
            tables_to_monitor[table_name] = table_summary
        else:
            tables_to_monitor_manually[table_name] = table.monitor_error

    execution_time = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H-%M-%S")
    with open(
            os.path.join(os.path.dirname(__file__), f'src/data/{execution_time}_tables_to_monitor.json'), 'w'
            ) as file:
        json.dump(tables_to_monitor, file, indent=2)

    if tables_to_monitor_manually:
        print('There are tables to monitor manually, please check the log.')
        with open(
                os.path.join(os.path.dirname(__file__), f'src/data/{execution_time}_tables_to_monitor_manually.json'), 'w'
                ) as file:
            json.dump(tables_to_monitor_manually, file, indent=2)

    tables_to_monitor_count = len(tables_to_monitor)
    if tables_to_monitor_count > 0:
        print('Setting monte carlo monitors')
        for enum, table_name in enumerate(tables_to_monitor):
            log_progress(
                enum, 
                tables_to_monitor_count, 
                status=f'Working on {table_name}')

            table = monteCarloTable(table_name= table_name)
            table.initialize_saved_state(
                saved_state= tables_to_monitor[table_name])
            table.set_monitor()
    else:
        print('No monitors to set')


if __name__ == '__main__':
    main()