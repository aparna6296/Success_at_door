# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from pyspark.sql import SparkSession
import importlib
import argparse
import logging



def _parse_arguments():
    logging.info("***Parsing arguments given at runtime***")
    parser=argparse.ArgumentParser()
    parser.add_argument("--job",required=True)
    parser.add_argument("--source_location", required=True)
    parser.add_argument("--target_location", required=True)
    parser.add_argument("--app_name", required=True)
    return parser.parse_args()

def main():
    """Entry point of spark-submit command"""
    args=_parse_arguments()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')
    spark = SparkSession.builder.appName(args.app_name).getOrCreate()
    job_module=importlib.import_module("jobs."+args.job)
    job_module.run_job(spark,args)

if __name__ == "__main__":
    main()
