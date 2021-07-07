import yaml
import os
from ..errors import configError

class yamlConfig:
    def __init__(self):
        self.full_config = self.load_config()
        self.full_database_secrets = self.load_secrets()
        self.extract_table_config()
        self.extract_database_config()
        self.extract_monitor_config()

    def load_config(self):
        filepath = os.path.join(
            os.path.dirname(__file__), 'mc_monitor_config.yaml')
        with open(filepath) as configuration:
            return(yaml.full_load(configuration))

    def load_secrets(self):
        filepath = os.path.join(
            os.path.dirname(__file__), 'database_secrets.yaml')
        with open(filepath) as secrets:
            return(yaml.full_load(secrets))

    def extract_table_config(self):
        table_config = self.full_config['table_config']
        self.table_age_limit = table_config['table_age_limit']
        schemas = tuple(table_config['schemas'])
        if not schemas:
            raise configError(
                config_part='Schemas', 
                message="Not provided")
        elif len(schemas) == 1:
            self.snowflake_schemas = f"('{schemas[0]}')"
        else:
            self.snowflake_schemas = schemas

    def extract_database_config(self):
        database_secrets = self.full_database_secrets['database_config']
        self.snowflake_account = database_secrets['account']
        self.snowflake_db = database_secrets['database']
        self.snowflake_user = database_secrets['user_name']
        self.snowflake_wh = database_secrets['warehouse']
        self.snowflake_role = database_secrets['role']
    
    def extract_monitor_config(self):
        monitor_config = self.full_config['monitor_config']
        self.default_timefields = monitor_config['default_timefields']
