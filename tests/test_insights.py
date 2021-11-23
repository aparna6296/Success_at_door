from jobs.insights import _read_data, _transforming_data, _loading_output
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import argparse


class TestInsights:
    def test_insights(self):

        parser = argparse.ArgumentParser()
        parser.add_argument("--source_location")
        parser.add_argument("--target_location")
        parser.add_argument("--app_name")
        parser.add_argument("--fullvisitorID")
        args = parser

        args.source_location = "test_data/input"
        args.target_location = "test_data/output"
        args.app_name = "GAInsights"

        spark = SparkSession.builder.appName(args.app_name).getOrCreate()
        print(args.app_name)

        """ Reader test """
        extract_df = _read_data(spark, args)
        assert extract_df.count() == 13
        print('***Reader test successful***')


        """ Loader test """
        _loading_output(transform_df, args)
        intermediate_load_df = spark.read.option("header", True).csv(args.target_location + "/*.csv").toDF("difficulty", "avg_total_cooking_time").select("difficulty", F.round(F.col("avg_total_cooking_time"), 2))
        loader_df = intermediate_load_df.withColumnRenamed("round(avg_total_cooking_time, 2)","avg_total_cooking_time(minutes)")
        assert sorted(loader_df.collect()) == sorted(transform_df.collect())
        print('***Loader test successful***')


def main():
    p1=TestInsights()
    p1.test_insights()



if __name__ == "__main__":
    main()
