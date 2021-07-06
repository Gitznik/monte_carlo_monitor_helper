from abc import ABC, abstractmethod
import os
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import pandas as pd

class databaseConnection(ABC):
    def __init__(self, yaml_config: dict) -> None:
        super().__init__()
        self.yaml_config = yaml_config

    @abstractmethod
    def query_database(self) -> None:
        pass

    @abstractmethod
    def get_tables_in_schema(self) -> None:
        pass


class snowflakeConnection(databaseConnection):
    def __init__(self, yaml_config: dict) -> None:
        super().__init__(yaml_config)
        self.snowflake_pw = os.environ.get('SNOWFLAKE_PW')
        self.query = self.create_query()

    def create_query(self) -> None:
        return f'''
            SELECT
                TABLE_CATALOG || ':' || TABLE_SCHEMA || '.' || TABLE_NAME
                    AS TABLE_REF
            FROM  { self.yaml_config.snowflake_db }.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_CATALOG = '{ self.yaml_config.snowflake_db }' 
                AND TABLE_SCHEMA IN { self.yaml_config.snowflake_schemas }
                AND LAST_ALTERED >= CURRENT_DATE - { self.yaml_config.table_age_limit }
            ''' 

    def query_database(self) -> None:
        """
        Runs the provided query on snowflake. Returns the results as a pandas DataFrame

        :param query: Query to run on snowflake
        :param snowflake_pw: Snowflake of the user
        :param yaml_config: Class containing the yaml config 
        :return: Dataframe with results of given query

        """

        engine = create_engine(URL(
            account = self.yaml_config.snowflake_account,
            user = self.yaml_config.snowflake_user,
            password = self.snowflake_pw,
            database = self.yaml_config.snowflake_db,
            warehouse = self.yaml_config.snowflake_wh,
            role = self.yaml_config.snowflake_role
        ))

        return pd.read_sql(self.query, engine.connect())

    def get_tables_in_schema(self) -> None:
        information_schema = self.query_database().table_ref.unique()
        return [table.lower() for table in information_schema]