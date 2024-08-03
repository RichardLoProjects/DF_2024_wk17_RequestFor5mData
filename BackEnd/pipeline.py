'''Data pipeline for ingestion of source data.'''
import pandas as pd
import os
from dotenv import load_dotenv # type: ignore
import psycopg2 as psql # type: ignore
import warnings
import requests
import numpy as np



class EnvSecrets:
    def __init__(self) -> None:
        '''Fetch all sensitive data and load into variables for later use.'''
        load_dotenv()
        self.db_name = os.getenv('DATABASE_NAME')
        self.db_host = os.getenv('DATABASE_HOST')
        self.db_port = int(os.getenv('DATABASE_PORT'))
        self.sql_user = os.getenv('SQL_USERNAME')
        self.sql_pass = os.getenv('SQL_PASSWORD')
        self.schema = os.getenv('SQL_SCHEMA')
        self.source = os.getenv('DATA_SOURCE')
        self.target = os.getenv('DATA_TARGET')
        self.api_user_agent = os.getenv('API_USER_AGENT')
        self.api_user_id = os.getenv('API_USER_ID')


class DatabaseConnection:
    def __init__(self, credentials:EnvSecrets) -> None:
        '''Open database connection.'''
        self.connection = psql.connect(
            database = credentials.db_name
            , host = credentials.db_host
            , port = credentials.db_port
            , user = credentials.sql_user
            , password = credentials.sql_pass
        )
        self.cursor = self.connection.cursor()
    def close(self) -> None:
        '''Close database connection.'''
        try:
            self.connection.close()
        except:
            pass


class DataPipeline:
    def __init__(
            self
            , primary_key:str
            , connection:DatabaseConnection
            , schema:str
            , data_source:str
            , data_target:str
            , user_agent:str
            , user_id:str
        ) -> None:
        self.df = pd.DataFrame()
        self.source = data_source
        self.target = data_target
        self.pk = primary_key
        self.connection = connection
        self.schema = schema
        self.user_agent = user_agent
        self.user_id = user_id
    def extract(self) -> None:
        '''Extract data from the data source and populate dataframe.'''
        header = {
            'User-Agent': self.user_agent,
            'From': self.user_id
        }
        json_data = requests.get(self.source, headers=header).json()['data']
        rows = [pd.DataFrame.from_dict([item], orient='columns') for item in json_data]
        self.df = pd.concat(rows)
    def transform(self) -> None:
        '''Renaming columns with name appropriate for the database.'''
        self.df.rename(columns={
            'timestamp':self.pk
            , 'avgHighPrice':'avg_high_price'
            , 'avgLowPrice':'avg_low_price'
            , 'highPriceVolume':'high_price_volume'
            , 'lowPriceVolume':'low_price_volume'
        }, inplace=True)
    def load(self) -> None:
        '''Load data into database.'''
        create_table_sql = f'''
CREATE TABLE IF NOT EXISTS {self.schema}.{self.target} (
    {self.pk} int PRIMARY KEY
    , avg_high_price int
    , avg_low_price int
    , high_price_volume int
    , low_price_volume int
);'''
        self.connection.cursor.execute(create_table_sql)
        self.df = self.df.replace({np.nan: None})
        query = f'SELECT {self.pk} FROM {self.schema}.{self.target}'
        existing_data = pd.read_sql(query, self.connection.connection)
        existing_set = set(existing_data[self.pk])
        non_duplicate_data = self.df[~self.df[self.pk].isin(existing_set)]
        if not non_duplicate_data.empty:
            tuples = [tuple(x) for x in non_duplicate_data.to_numpy()]
            columns = ','.join(non_duplicate_data.columns)
            values = ','.join(['%s'] * len(non_duplicate_data.columns))
            insert_query = f'INSERT INTO {self.schema}.{self.target} ({columns}) VALUES ({values})'
            self.connection.cursor.executemany(insert_query, tuples)
        self.connection.connection.commit()





def call_sos(error_message:str) -> None:
        '''Notify errors remotely.'''
        ### Implement email-notifications here.
        print(error_message)

def main() -> None:
    warnings.filterwarnings('ignore', message='.*pandas only supports SQLAlchemy connectable.*')
    try:
        primary_key = 'unix_time'
        sensitive_data = EnvSecrets()
        connection = DatabaseConnection(sensitive_data)
        pipeline = DataPipeline(
            primary_key
            , connection
            , sensitive_data.schema
            , sensitive_data.source
            , sensitive_data.target
            , sensitive_data.api_user_agent
            , sensitive_data.api_user_id
        )
        pipeline.extract()
        pipeline.transform()
        pipeline.load()
    except Exception as error:
        call_sos(error)
    finally:
        connection.close()

if __name__ == '__main__':
    main()

