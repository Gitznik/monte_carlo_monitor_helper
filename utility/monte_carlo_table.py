from .utility_functions import query_mc_api
from .config import mc_queries
from .config.read_config import yamlConfig


yaml_config = yamlConfig()

class monteCarloTable:
    def __init__(
            self, 
            table_name: str,
            warehouse_id:str = None) -> None:
        
        self.table_name = table_name
        self.warehouse_id =  warehouse_id

    def initialize_monte_carlo(self, warehouse_id: str):
        self.warehouse_id = warehouse_id
        self.get_mc_information()
        self.extract_timefields()
        self.find_timefield_to_monitor(
            monitorable_time_fields= yaml_config.default_timefields)

    def initialize_saved_state(self, saved_state: dict):
        self.timefield_to_monitor_dict = saved_state['time_fields']
        self.mcon = saved_state['mcon']
        self.monitorable = saved_state['monitorable']

    def get_mc_information(self) -> None:
        query_params_table_information = {
            "fullTableId": self.table_name,
            "dwId": self.warehouse_id,
            "isTimeField": True}
        mc_table_information_api_response = query_mc_api(
            mc_queries.query_get_mcons_for_tables,
            query_params=query_params_table_information)
        self.mc_table_information = mc_table_information_api_response
        self.mcon = mc_table_information_api_response['getTable']['mcon']

    def extract_timefields(self) -> None:
        timefield_information = {
            node['node']['name']: node['node']['fieldType']
            for node in self.mc_table_information['getTable']['versions']['edges'][
                0
            ]['node']['fields']['edges']
        }

        self.timefield_information = timefield_information

    def find_timefield_to_monitor(
            self, 
            monitorable_time_fields: list
            ) -> None:

        timefields_in_table = \
            [timefield for timefield in self.timefield_information]
        self.available_timefield_count = len(timefields_in_table)

        timefield_to_monitor = None
        for monitorable_time_field in monitorable_time_fields:
            if monitorable_time_field in timefields_in_table:
                timefield_to_monitor = monitorable_time_field
                break
        
        time_axis_type = self.timefield_information.get(timefield_to_monitor)
        self.timefield_to_monitor_dict = {
            'timefield_to_monitor': timefield_to_monitor,
            'time_axis_type': time_axis_type}

    def evaluate_is_monitorable(
            self,
            monitor_without_timefield:bool = False
            ) -> None:

        self.monitorable = False
        if self.available_timefield_count == 0 and not monitor_without_timefield:
            self.monitor_error = 'No timefield available'
        elif (self.available_timefield_count > 0 and 
                self.timefield_to_monitor_dict['timefield_to_monitor'] is None):
            self.monitor_error = 'No matching standard timefield found'
        else:
            self.monitorable = True

    def save_table(self) -> dict:
        return {
            'mcon': self.mcon,
            'time_fields': self.timefield_to_monitor_dict,
            'monitorable': self.monitorable
            }

    def set_monitor(self):

        set_monitor_query_params = {
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

        try:
            if self.monitorable == False:
                raise ValueError('Table is not automatically monitorable.')
        except:
            raise ValueError('Not known if this table is monitorable. Please make sure to evaluate this before trying to set a monitor')

        set_monitor_query_params["mcon"] = self.mcon
        set_monitor_query_params["timeAxisName"] = self.timefield_to_monitor_dict.get("timefield_to_monitor")
        set_monitor_query_params["timeAxisType"] = self.timefield_to_monitor_dict.get("time_axis_type")

        query_mc_api(
            query_string = mc_queries.query_create_monitor,
            query_params = set_monitor_query_params
        )
        print('Monitor created for: ' + self.table_name)
        
        
