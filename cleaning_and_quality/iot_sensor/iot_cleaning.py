from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,FloatType,StructField,StructType
from pyspark.sql.functions import col,sum,when,isnan,lit,concat_ws,to_timestamp
import logging
import os
import shutil

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)

#Spark configuration
spark=SparkSession\
        .builder\
        .appName("IOT cleaner")\
        .config("spark.executor.memory","2g")\
        .config("spark.driver.memory","1g")\
        .getOrCreate() 

schema=StructType([
    StructField("Date", StringType(), True),
    StructField("Time",StringType(), True),
    StructField("Global_active_power", FloatType(), True),
    StructField("Global_reactive_power", FloatType(), True),
    StructField("Voltage", FloatType(), True),
    StructField("Global_intensity", FloatType(), True),
    StructField("Sub_metering_1", FloatType(), True),
    StructField("Sub_metering_2", FloatType(), True),
    StructField("Sub_metering_3", FloatType(), True)
])

#Read data
df=spark.read.csv("cleaning_and_quality/iot_sensor/data/household_power_consumption.txt",
                  sep=";",
                  header=True,
                  schema=schema)

#Check read operation
df.show()
df.printSchema()

#Null and Nan count check
null_count=df.select([
    sum(when(col(c).isNull() | isnan(col(c)),1).otherwise(0)).alias(c)
    for c in df.columns]
)
logging.info("Initial null count:")
null_count.show()

#This columns should not contain empty string, nulls and nans (assumed for practice)
critical_number_columns = ["Global_active_power", "Global_reactive_power", "Voltage", "Global_intensity"]
critical_string_columns = ["Date", "Time"]

clean_df=df

logging.info(f"Initial row count: {clean_df.count()}")

for column in critical_number_columns:
    clean_df=clean_df.filter(col(column).isNotNull() & ~isnan(col(column)))

logging.info(f"Row count after removing null and nans: {clean_df.count()}")

for column in critical_string_columns:
    clean_df = clean_df.filter(col(column).isNotNull() & (col(column) != ''))

logging.info(f"Row count after removing empty strings: {clean_df.count()}")

#Double check nulls and nans
final_null_count=clean_df.select([
    sum(when(col(c).isNull(),1).otherwise(0)).alias(c)
    for c in clean_df.columns]
)
logging.info("Final null count:")

final_null_count.show()

#Remove outliers (assumed)
clean_df = clean_df.filter(col("Voltage") <= 320)

#Timestamp conversions
clean_df = clean_df.withColumn("Timestamp", to_timestamp(concat_ws(" ",col("Date"),col("Time")), "d/M/yyyy H:mm:ss"))
clean_df.show()
clean_df=clean_df.drop("Time","Date")

final_df=clean_df.withColumn("power_label",
                             when(col("Global_reactive_power")<5000,"weak")
                            .when((col("Global_reactive_power") >= 5000) & (col("Global_reactive_power") <= 7000), "powerful")
                            .when(col("Global_reactive_power")>7000,"extreme")
                            .otherwise("unknown")
                    )

#Validate quality outside of production
if spark.sparkContext.getConf().get("spark.master") == "local[*]":
    final_df.summary().show()


#Write output
output_path = "cleaning_and_quality/iot_sensor/data/cleaned_iot"

# delete old data
if os.path.exists(output_path):
    shutil.rmtree(output_path)

final_df.write \
    .option("header", True) \
    .option("sep", ";") \
    .mode("overwrite") \
    .csv(output_path)

logging.info("Cleaning completed")