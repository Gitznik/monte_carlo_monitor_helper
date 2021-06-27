from utility_functions_mc_script import query_mc_api
from  config import mc_queries

class snowflake_table:
    def __init__(
            self, 
            table_name: str,
            warehouse_id: str) -> None:
        
        self.table_name = table_name
        self.warehouse_id = warehouse_id
        
        self.get_mc_information()
        self.extract_timefields()

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
        timefields = {}
        for node in (self.mc_table_information['getTable']
                                              ['versions']
                                              ['edges'][0]
                                              ['node']
                                              ['fields']
                                              ['edges']):
            timefields[node['node']['name']] = node['node']['fieldType']
        self.timefields = timefields

    def save_table(self) -> dict:
        summary_dict = {
            self.table_name: {
                'mcon': (self.mcon),
                'time_fields':self.timefields
                }
            }
        return summary_dict
