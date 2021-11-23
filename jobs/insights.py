from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from jobs.helper_udfs import *
import logging
import gcsfs
import pyarrow


def _read_data(spark, args):
    logging.info("*******Defining schema for my data************")
    recipe_schema = StructType([StructField('orderDate', StringType(), True),
                                StructField('common_name', StringType(), True),
                                StructField('deliveryType', StringType(), True),
                                StructField('backendOrderId', StringType(), True),
                                StructField('frontendOrderId', StringType(), True),
                                StructField('status_id', StringType(), True),
                                StructField('declinereason_code', StringType(), True),
                                StructField('declinereason_type', StringType(), True),
                                StructField('geopointCustomer', StringType(), True),
                                StructField('geopointDropoff', StringType(), True)])
    logging.info("*******Reading data from source location************")
    #Exception Handling
    try:
        gs = gcsfs.cloud.GCSFileSystem()
        arrow_df = pyarrow.parquet.ParquetDataset(gs_directory_path, filesystem=gs)
        if to_pandas:
            return arrow_df.read_pandas().to_pandas()
            return arrow_df
    except AnalysisException:
        logging.exception('Source data not found, please check the path given exists or not')
        sys.exit()


def _transforming_data(raw_df):

    

    return avg_time_eachDifficulty


def _loading_output(transformed_df, args):
    logging.info('****Saving output to target location****')
    transformed_df.coalesce(1).write.option("inferSchema", "true").csv(args.target_location, mode='overwrite',
                                                                       header=True)


def run_job(spark, args):
    _loading_output(_transforming_data(_read_data(spark, args)), args)