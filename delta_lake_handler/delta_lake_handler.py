import sys
from collections import OrderedDict
from typing import List, Optional

import pandas as pd
from pyspark.sql import SparkSession
from delta.tables import *

from mindsdb.integrations.handlers.delta_lake_handler.settings import DeltaLakeHandlerConfig
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from mindsdb.integrations.libs.response import RESPONSE_TYPE, HandlerResponse, HandlerStatusResponse as StatusResponse
from mindsdb.integrations.libs.base import DatabaseStoreHandler, FilterCondition
from mindsdb.interfaces.storage.model_fs import HandlerStorage
from mindsdb.utilities import log

logger = log.getLogger(__name__)

def get_spark_session(spark_master: str, spark_conf: dict = {}):
    print(f"Initializing Spark session with spark_master: {spark_master} and spark_conf: {spark_conf}")
    builder = SparkSession.builder.appName("MindsDB-DeltaLake")
    if spark_master:
        builder = builder.master(spark_master)
    for key, value in spark_conf.items():
        builder = builder.config(key, value)
    builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    try:
        spark = builder.getOrCreate()
        print("Spark session initialized successfully")
        return spark
    except Exception as e:
        print(f"Failed to initialize Spark session: {e}")
        raise

class DeltaLakeHandler(DatabaseStoreHandler):
    name = "delta_lake"

    def __init__(self, name: str, **kwargs):
        print(f"Initializing DeltaLakeHandler with name: {name} and kwargs: {kwargs}")
        super().__init__(name)
        self.handler_storage = HandlerStorage(kwargs.get("integration_id"))
        self._spark_session = None
        self.is_connected = False

        config = self.validate_connection_parameters(name, **kwargs)
        self._client_config = config
        self.connect()
        print("DeltaLakeHandler initialized successfully")

    def validate_connection_parameters(self, name, **kwargs):
        print(f"Validating connection parameters for name: {name}")
        _config = kwargs.get("connection_data")
        config = DeltaLakeHandlerConfig(**_config)
        print("Connection parameters validated successfully")
        return config

    def connect(self):
        print("Attempting to connect to Delta Lake")
        if self.is_connected:
            print("Already connected to Delta Lake")
            return self._spark_session

        try:
            self._spark_session = get_spark_session(self._client_config.spark_master, self._client_config.spark_conf)
            self.is_connected = True
            print("Connected to Delta Lake successfully")
        except Exception as e:
            print(f"Error initializing Spark session for Delta Lake: {e}")
            raise

    def disconnect(self):
        print("Attempting to disconnect from Delta Lake")
        if self._spark_session:
            self._spark_session.stop()
            self._spark_session = None
            self.is_connected = False
            print("Disconnected from Delta Lake successfully")

    def check_connection(self):
        print("Checking connection to Delta Lake")
        response = StatusResponse(False)
        try:
            self._spark_session.range(1).collect()
            response.success = True
            print("Connection to Delta Lake is active")
        except Exception as e:
            print(f"Connection check failed for Delta Lake: {e}")
            response.error_message = str(e)
            raise

    def select(self, table_path: str, conditions: List[FilterCondition] = None, columns: List[str] = None) -> HandlerResponse:
        print(f"Selecting from Delta table at path: {table_path} with conditions: {conditions} and columns: {columns}")
        try:
            df = self._spark_session.read.format("delta").load(table_path)
            if conditions:
                for condition in conditions:
                    df = df.filter(condition.to_spark_sql_condition())
            if columns:
                df = df.select(columns)
            print("Selection from Delta table completed successfully")
            return HandlerResponse(resp_type=RESPONSE_TYPE.TABLE, data_frame=df.toPandas())
        except Exception as e:
            print(f"Error selecting data from Delta table at path: {table_path}: {e}")
            raise

    def insert(self, table_path: str, data: pd.DataFrame) -> HandlerResponse:
        print(f"Inserting into Delta table at path: {table_path}")
        try:
            spark_df = self._spark_session.createDataFrame(data)
            spark_df.write.format("delta").mode("append").save(table_path)
            print("Data inserted into Delta table successfully")
            return HandlerResponse(resp_type=RESPONSE_TYPE.OK)
        except Exception as e:
            print(f"Error inserting data into Delta table at path: {table_path}: {e}")
            raise

    def update(self, table_path: str, set: dict, condition: Optional[str] = None) -> HandlerResponse:
        print(f"Updating Delta table at path: {table_path} with set: {set} and condition: {condition}")
        try:
            delta_table = DeltaTable.forPath(self._spark_session, table_path)
            if condition:
                delta_table.update(condition=condition, set=set)
                print("Delta table updated successfully")
                return HandlerResponse(resp_type=RESPONSE_TYPE.OK)
            else:
                print("Update operation requires a condition.")
                raise ValueError("Update operation requires a condition.")
        except Exception as e:
            print(f"Error updating Delta table at path: {table_path}: {e}")
            raise

    def delete(self, table_path: str, condition: str) -> HandlerResponse:
        print(f"Deleting from Delta table at path: {table_path} with condition: {condition}")
        try:
            delta_table = DeltaTable.forPath(self._spark_session, table_path)
            delta_table.delete(condition)
            print("Data deleted from Delta table successfully")
            return HandlerResponse(resp_type=RESPONSE_TYPE.OK)
        except Exception as e:
            print(f"Error deleting data from Delta table at path: {table_path}: {e}")
            raise

    def create_table(self, table_path: str, schema: Optional[str] = None) -> HandlerResponse:
        print(f"Creating Delta table at path: {table_path} with schema: {schema}")
        try:
            if schema:
                self._spark_session.sql(f"CREATE TABLE {table_path} ({schema}) USING delta")
                print("Delta table created successfully")
                return HandlerResponse(resp_type=RESPONSE_TYPE.OK)
            else:
                print("Schema must be provided for table creation.")
                raise ValueError("Schema must be provided for table creation.")
        except Exception as e:
            print(f"Error creating Delta table at path: {table_path}: {e}")
            raise

    def drop_table(self, table_path: str) -> HandlerResponse:
        print(f"Dropping Delta table at path: {table_path}")
        try:
            self._spark_session.sql(f"DROP TABLE IF EXISTS {table_path}")
            print("Delta table dropped successfully")
            return HandlerResponse(resp_type=RESPONSE_TYPE.OK)
        except Exception as e:
            print(f"Error dropping Delta table at path: {table_path}: {e}")
            raise

    def get_tables(self) -> HandlerResponse:
        print("Listing all Delta tables")
        try:
            df = self._spark_session.sql("SHOW TABLES")
            tables_list = [row['tableName'] for row in df.collect()]
            print("Delta tables listed successfully")
            return HandlerResponse(resp_type=RESPONSE_TYPE.TABLE, data_frame=pd.DataFrame({"tableName": tables_list}))
        except Exception as e:
            print("Error listing Delta tables: {e}")
            raise

    def get_columns(self, table_path: str) -> HandlerResponse:
        print(f"Getting columns for Delta table at path: {table_path}")
        try:
            df = self._spark_session.read.format("delta").load(table_path)
            columns_info = [{"COLUMN_NAME": col, "DATA_TYPE": dtype} for col, dtype in zip(df.columns, df.dtypes)]
            print("Columns for Delta table retrieved successfully")
            return HandlerResponse(resp_type=RESPONSE_TYPE.TABLE, data_frame=pd.DataFrame(columns_info))
        except Exception as e:
            print(f"Error getting columns for Delta table at path: {table_path}: {e}")
            raise

connection_args = OrderedDict(
    spark_master={
        "type": ARG_TYPE.STR,
        "description": "Spark master URL, e.g., 'local[*]', 'spark://host:port'",
        "required": False,
    },
    delta_table_path={
        "type": ARG_TYPE.STR,
        "description": "File system path to the Delta table",
        "required": True,
    },
    spark_conf={
        "type": ARG_TYPE.DICT,
        "description": "Additional Spark configuration properties",
        "required": False,
    }
)

connection_args_example = OrderedDict(
    spark_master="local[*]",
    delta_table_path="/path/to/delta/table",
    spark_conf={
        "spark.executor.memory": "4g",
        "spark.driver.memory": "2g",
    }
)