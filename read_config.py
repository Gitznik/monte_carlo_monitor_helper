#%%
import yaml
import os

class yamlConfig():
    def __init__(self):
        self.full_config = self.load_config()
        self.full_snowflake_secrets = self.load_secrets()
        self.extract_snowflake_config()
        self.extract_monitor_config()

    def load_config(self):
        filepath = os.path.join(
            os.path.dirname(__file__), 'mc_monitor_config.yaml')
        with open(filepath) as configuration:
            return(yaml.full_load(configuration))

    def load_secrets(self):
        filepath = os.path.join(
            os.path.dirname(__file__), 'snowflake_secrets.yaml')
        with open(filepath) as secrets:
            return(yaml.full_load(secrets))

    def load_table_config(self):
        table_config = self.full_config['snowflake_config']
        self.table_age_limit = table_config['table_age_limit']
        self.snowflake_schemas = tuple(table_config['schemas'])

    def extract_snowflake_config(self):
        snowflake_secrets = self.full_snowflake_secrets['snowflake_config']
        self.snowflake_account = snowflake_secrets['account']
        self.snowflake_db = snowflake_secrets['database']
        self.snowflake_user = snowflake_secrets['user_name']
        self.snowflake_wh = snowflake_secrets['warehouse']
        self.snowflake_role = snowflake_secrets['role']
    
    def extract_monitor_config(self):
        monitor_config = self.full_config['monitor_config']
        self.default_timefields = monitor_config['default_timefields']

# %%
