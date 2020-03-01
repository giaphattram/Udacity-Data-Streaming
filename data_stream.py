import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf
from dateutil.parser import parse as parse_date

schema = StructType([StructField("crime_id", StringType(), True),
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
    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
    .readStream \
    .format('kafka') \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "project2") \
    .option("maxRatePerPartition", 300) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 500) \
    .option("stopGracefullyOnShutdown", "true") \
    .load()
        
    # Show schema for the incoming resources for checks
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    kafka_df.printSchema()
        
    service_table = kafka_df\
    .select(psf.from_json(psf.col('value'), schema).alias("DF")).select("DF.*")
        
    service_table.printSchema()
    
    #select original_crime_type_name and disposition
    distinct_table = service_table \
        .select(psf.col('original_crime_type_name'),psf.col('disposition'), psf.to_timestamp(psf.col('call_date_time')).alias("call_date_time"))    

    # count the number of original crime type
    agg_df = distinct_table \
        .dropna() \
        .withWatermark("call_date_time", "60 minutes") \
        .groupby(
            psf.window(distinct_table.call_date_time, "10 minutes", "5 minutes"),
            distinct_table.original_crime_type_name,
            distinct_table.disposition
        ).count()

    agg_df.isStreaming
    agg_df.printSchema()

#     # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
#     # TODO write output stream    
#     query = agg_df \
#             .writeStream \
#             .format("console") \
#             .queryName("counts") \
#             .outputMode("Complete") \
#             .trigger(processingTime="100 seconds")\
#             .start()  
#     query.awaitTermination()  # TODO attach a ProgressReporter
    
         
    # get the right radio code json path
    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath, multiLine = True)
    
    print('radio_code schema')
    radio_code_df.printSchema()
    radio_code_df.show()
        
    # rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")
    
    
    # join on disposition column
    join_query = agg_df.join(radio_code_df, "disposition")
    
    output_query = join_query.writeStream.format("console").queryName("join").outputMode("Complete").trigger(processingTime="5 seconds").start()
    output_query.awaitTermination()
    
if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .config("spark.ui.port", 3000) \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    print("Spark Session createdc")
    logger.info("Spark started")

    run_spark_job(spark)

    logger.info("Spark stoped")
    spark.stop()