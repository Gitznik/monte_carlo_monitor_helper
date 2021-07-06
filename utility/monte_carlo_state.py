from .utility_functions import query_mc_api
from .config import mc_queries


class monteCarloState:
    def __init__(self) -> None:
        self.get_warehouse_id()
        self.find_existing_monitors()

    def get_warehouse_id(self) -> None:
        print('Getting monte carlo warehouse id')
        self.warehouse_id = query_mc_api(
            mc_queries.query_get_warehouse_id)[
                'getUser']['account']['warehouses'][0]['uuid']

    def find_existing_monitors(self) -> list:
        print('Finding existing monitors')
        existing_monitors_api_response = query_mc_api(
            mc_queries.query_get_existing_monitors,
            query_params={"userDefinedMonitorTypes": ["stats"]})

        self.existing_monitors =  [
            edge['node']['customRuleEntities'][0]
            for edge in existing_monitors_api_response[
                'getAllUserDefinedMonitorsV2']['edges']
        ]

    def find_tables_without_monitor(
            self,
            database_tables:list) -> list:
        print('Finding tables without a monitor')

        return [
            table for table in database_tables 
            if table not in self.existing_monitors
        ]
        