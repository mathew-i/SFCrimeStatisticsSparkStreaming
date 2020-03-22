import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf

# small improvement, easier to run program, just type command "python data_stream.py"
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4 pyspark-shell'

schema = StructType([
    StructField("crime_id", StringType(), True),
    StructField("original_crime_type_name", StringType(), True),
    StructField("report_date", StringType(), True),
    StructField("call_date", StringType(), True),
    StructField("offense_date", StringType(), True),
    StructField("call_time", StringType(), True),
    StructField("call_date_time", StringType(), True),
    StructField("disposition", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("agency_id", StringType(), True),
    StructField("address_type", StringType(), True),
    StructField("common_location", StringType(), True)
])

def run_spark_job(spark):

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "com.udacity.police.calls.new") \
        .option("startingOffset", "earliest") \
        .option("maxRatePerPartition", 100) \
        .option("maxOffsetPerTrigger", 200) \
        .load()

    df.printSchema()

    kafka_df = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS timestamp)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"), psf.col('timestamp'))\
        .select("timestamp", "DF.*")
    
    service_table.printSchema()

    distinct_table = service_table.select('original_crime_type_name', 'disposition', psf.to_timestamp('timestamp').alias('call_date_time2'))
    #.dropDuplicates()

    agg_df = distinct_table.select('call_date_time2', 'original_crime_type_name', 'disposition') \
    .withWatermark('call_date_time2', '1 day') \
    .groupBy(
             psf.window('call_date_time2', "5 minutes", "2 minutes"),
             #'call_date_time2',
             'original_crime_type_name', 
             'disposition') \
    .count() \
#    .orderBy(psf.desc('count'))

#    query = agg_df \
#    .writeStream \
#    .outputMode("complete") \
#    .format("console") \
#    .start()
#    query.awaitTermination()

    radio_code_json_filepath = "/home/workspace/radio_code.json"
    radio_code_df = spark.read.option("multiLine", True).json(radio_code_json_filepath)
    
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # join on disposition column, I was experimenting with different types of joins inner, left_outer...
    join_query = agg_df.join(radio_code_df, radio_code_df.disposition==agg_df.disposition, "left_outer")
    
    query2 = join_query.writeStream.outputMode("complete").format("console").start()    
    query2.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
