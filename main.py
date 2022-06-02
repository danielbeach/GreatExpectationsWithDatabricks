import datetime
from pyspark.sql import SparkSession, DataFrame
import os
import json

from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.profile.json_schema_profiler import JsonSchemaProfiler
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    FilesystemStoreBackendDefaults,
)


trips_expect = {
  "properties": {},
  "type": 'object',
  "data_asset_type": '',
  "expectation_suite_name": "bikes",
  "expectations": [
    {
      "expectation_type": "expect_table_columns_to_match_ordered_list",
      "kwargs": {
        "column_list": [
          "ride_id", "rideable_type", "started_at", "ended_at", "start_station_name", "start_station_id",
            "end_station_name", "end_station_id", "start_lat", "start_lng", "end_lat", "end_lng", "member_casual"
        ]
      },
      "meta": {}
    },
    {
      "expectation_type": "expect_table_row_count_to_be_between",
      "kwargs": {
        "max_value": 1000000,
        "min_value": 1000
      },
      "meta": {}
    }
  ],
  "ge_cloud_id": '',
  "meta": {
    "citations": [
      {
        "batch_request": {
          "data_asset_name": "trip_data_batch",
          "data_connector_name": "DataFrame_Trips_Data_Connector",
          "datasource_name": "DataFrame_Trips_Source",
          "limit": 1000
        },
        "citation_date": "2022-06-02",
        "comment": "Created suite "
      }
    ],
    "great_expectations_version": "0.14.10"
  }
}



def prepare_ge_context(root_dir: str) -> BaseDataContext:
    data_context_config = DataContextConfig(
        store_backend_defaults=FilesystemStoreBackendDefaults(
            root_directory=root_dir
        ),
    )
    ge_context = BaseDataContext(project_config=data_context_config)
    return ge_context


def prepare_get_datasource(dname: str = 'DataFrame_Trips_Source') -> dict:
    ge_dataframe_datasource = {
        "name": dname,
        "class_name": "Datasource",
        "execution_engine": {"class_name": "SparkDFExecutionEngine"},
        "data_connectors": {
            "DataFrame_Trips_Data_Connector": {
                "module_name": "great_expectations.datasource.data_connector",
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": [
                    "trips_source",
                    "divvy_bike_trips",
                ],
            }
        },
    }
    return ge_dataframe_datasource


def prepare_checkpoint() -> dict:
    ge_trip_data_checkpoint = "trip_check"
    checkpoint_config = {
        "name": ge_trip_data_checkpoint,
        "config_version": 1.0,
        "class_name": "SimpleCheckpoint",
        "run_name_template": "%Y%m%d-%H%M%S-trip-run",
    }
    return checkpoint_config


def prepare_runtime_batch(df: DataFrame):
    batch_request = RuntimeBatchRequest(
        datasource_name="DataFrame_Trips_Source",
        data_connector_name="DataFrame_Trips_Data_Connector",
        data_asset_name="trip_data_batch",  # This can be anything that identifies this data_asset for you
        batch_identifiers={
            "trips_source": "trips_source",
            "divvy_bike_trips": "divvy_bike_trips",
        },
        runtime_parameters={"batch_data": df},  # Your dataframe goes here
    )
    return batch_request


def run_checkpoint(context, batch_request):
    checkpoint_result = context.run_checkpoint(
        checkpoint_name="trip_check",
        validations=[
            {
                "batch_request": batch_request,
                "expectation_suite_name": "bikes",
            }
        ],
    )
    return checkpoint_result

df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("s3a://confessions-of-a-data-guy/*divvy-tripdata.csv")

root_directory = "/dbfs/great_expectations/"
# Prepare Great Expectations / storage on Databricks DBFS
ge_context = prepare_ge_context(root_directory)

# Prepare DataFrame as Data Source Connector for GE.
ge_context.add_datasource(**prepare_get_datasource())

# Prepare Checkpoint
trips_check = prepare_checkpoint()
ge_context.add_checkpoint(**trips_check)

# create and save expectation suite
profiler = JsonSchemaProfiler()
suite = profiler.profile(trips_expect, "bikes")
ge_context.save_expectation_suite(suite)

# Prepare Batch and Validate
trips_batch_request = prepare_runtime_batch(df)
validation_results = run_checkpoint(ge_context, trips_batch_request)
print(validation_results)
