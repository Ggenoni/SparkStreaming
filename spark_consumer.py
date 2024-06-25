import sys
sys.path.append('/opt/bitnami/spark/scripts')
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, MapType, ArrayType
import json
import numpy as np
#from fwi_calculator import total_fwi
import pyarrow as pa

def calculate_ffmc(temperature, relative_humidity, rain):
    ffmc0 = 85.0
    a = 0.03
    b = 0.0003
    k1 = 1.0
    k2 = 0.0006
    k3 = 0.0012
    k4 = 0.000246
    k5 = 0.000034

    if rain > 0.5:
        er = rain - 0.5
    else:
        er = 0

    if ffmc0 > 90:
        mr = ((ffmc0 - 91.0) * k5 * np.exp(k4 * temperature)) + (10.0 * k2 * (1.0 - np.exp(-k3 * rain))) + (20.0 * (1.0 - np.exp(-k1 * er)))
    else:
        mr = ((ffmc0 - 91.0) * k5 * np.exp(k4 * temperature)) + (10.0 * k2 * (1.0 - np.exp(-k3 * rain * k1 * er)))

    ffmc = 91.9 * (np.exp((mr - 15.0) / (10.0 * (mr + 10.0))) - np.exp(-b * (temperature + 20.0)))
    return ffmc

def calculate_dmc(temperature, rain, ffmc):
    k1 = 0.036
    k2 = 1.0
    if ffmc <= 33.0:
        dmc0 = (0.155 * ffmc) - 43.27
    else:
        dmc0 = (0.305 * ffmc) - 15.87

    if rain > 1.5:
        pr = rain - 1.5
    else:
        pr = 0.0

    dmc = dmc0 + (100.0 * pr)
    return dmc

def calculate_dc(temperature, rain, dmc):
    k1 = 0.27
    k2 = 1.5
    if dmc <= 0.4 * dmc:
        dc0 = (0.27 * dmc) - 2.0
    else:
        dc0 = (0.9 * dmc) - 1.5

    if rain > 2.8:
        pr = rain - 2.8
    else:
        pr = 0.0

    dc = dc0 + (pr * k1)
    return dc

def calculate_isi(wind_speed, ffmc):
    k1 = 0.208
    k2 = 0.0000613
    if wind_speed > 0.0:
        isi = k2 * np.exp(k1 * ffmc * (1.0 - np.exp(-0.5 * wind_speed)))
    else:
        isi = 0.0
    return isi

def calculate_bui(dmc, dc):
    k1 = 0.0322
    k2 = 0.014
    if dc <= 0.4 * dmc:
        bui = (1.0 - k1) * dc
    else:
        bui = ((1.0 - np.exp(-k1 * dmc)) * (0.4 * dmc)) + (((1.0 - (1.0 - k1) * np.exp(-k1 * dmc)) * (dc - 0.4 * dmc)) / (1.0 - np.exp(-k1 * dmc)))
    return bui

def calculate_fwi_from_subindices(isi, bui):
    k1 = 0.4
    k2 = 0.4
    if bui <= 80.0:
        fwi = (0.8 * isi * bui) / (isi + (0.1 * bui))
    else:
        fwi = (2.0 * isi) + (0.1 * bui)
    return fwi

def evaluate_fwi(fwi):
    if fwi < 11.2:
        return "Low"
    elif 11.2 <= fwi < 21.3:
        return "Moderate"
    elif 21.3 <= fwi < 38:
        return "High"
    elif 38 <= fwi < 50:
        return "Very-High"
    else:
        return "Extreme"

def total_fwi(temperature, relative_humidity, rain, wind_speed):
    temperature = temperature  # Celsius to Celsius (no conversion needed)
    wind_speed = wind_speed * 3.6  # m/s to km/h
    ffmc = calculate_ffmc(temperature, relative_humidity, rain)
    dmc = calculate_dmc(temperature, rain, ffmc)
    dc = calculate_dc(temperature, rain, dmc)
    isi = calculate_isi(wind_speed, ffmc)
    bui = calculate_bui(dmc, dc)
    fwi = calculate_fwi_from_subindices(isi, bui)
    risk_level = evaluate_fwi(fwi)
    return fwi, risk_level

# Define the schema for the incoming JSON data
schema = StructType([
    StructField("main", StructType([
        StructField("temp", FloatType(), True),
        StructField("humidity", FloatType(), True),
        StructField("temp_min", FloatType(), True),
        StructField("temp_max", FloatType(), True),
        StructField("pressure", IntegerType(), True),
        StructField("sea_level", IntegerType(), True),
        StructField("grnd_level", IntegerType(), True)
    ]), True),
    StructField("visibility", IntegerType(), True),
    StructField("wind", StructType([
        StructField("speed", FloatType(), True),
        StructField("deg", IntegerType(), True),
        StructField("gust", FloatType(), True)
    ]), True),
    StructField("rain", MapType(StringType(), FloatType()), True),
    StructField("clouds", StructType([
        StructField("all", IntegerType(), True)
    ]), True),
    StructField("weather", ArrayType(StructType([
        StructField("id", IntegerType(), True),
        StructField("main", StringType(), True),
        StructField("description", StringType(), True),
        StructField("icon", StringType(), True)
    ])), True),
    StructField("coord", StructType([
        StructField("lon", FloatType(), True),
        StructField("lat", FloatType(), True)
    ]), True),
    StructField("dt", IntegerType(), True),
    StructField("sys", StructType([
        StructField("type", IntegerType(), True),
        StructField("id", IntegerType(), True),
        StructField("country", StringType(), True),
        StructField("sunrise", IntegerType(), True),
        StructField("sunset", IntegerType(), True)
    ]), True),
    StructField("timezone", IntegerType(), True),
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("cod", IntegerType(), True)
])

# Initialize Spark session
spark = SparkSession.builder.appName("WeatherAlerts").getOrCreate()

# Create a custom deserializer function using pyarrow
def custom_deserializer(value):
    try:
        if isinstance(value, bytes):
            return pa.deserialize(value)
        else:
            return pa.deserialize(value.encode('utf-8'))
    except Exception as e:
        print(f"Deserialization error: {e}")
        return None

# Register the custom deserializer as a UDF
deserialize_udf = udf(lambda x: custom_deserializer(x), schema)

# Create a streaming DataFrame by reading from Kafka
raw_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "weather-data") \
    .load()

# Parse the JSON data and extract fields
parsed_df = raw_df.selectExpr("CAST(value AS BINARY)").select(deserialize_udf(col("value")).alias("data")).select("data.*")

# Perform the FWI calculation
def calculate_fwi_udf(temp, hum, rain, wind_speed):
    try:
        fwi, fwi_level = total_fwi(temp, hum, rain, wind_speed)
        return (fwi, fwi_level)
    except Exception as e:
        print(f"FWI calculation error: {e}")
        return (None, None)

#ADDED
parsed=parsed_df.writeStream.outputMode("append").format("console").start()


# Register the UDF
fwi_schema = StructType([
    StructField("fwi", FloatType(), True),
    StructField("fwi_level", StringType(), True)
])

calculate_fwi = udf(calculate_fwi_udf, fwi_schema)

# Add FWI and FWI level to the DataFrame
alerts_df = parsed_df.withColumn("rain_1h", col("rain")["1h"].cast(FloatType())).fillna(0, subset=["rain_1h"]) \
    .withColumn("fwi_data", calculate_fwi(col("main.temp"), col("main.humidity"), col("rain_1h"), col("wind.speed"))) \
    .select(col("name"), col("fwi_data.fwi_level"))

#print("!!!!!ALERTS!!!!!!")
#ADDED
al1=alerts_df.writeStream.outputMode("append").format("console").start()


# Filter alerts
alerts_df = alerts_df.filter(col("fwi_level").isin("Extreme", "Very-High", "High"))

#ADDED
al2=alerts_df.writeStream.outputMode("append").format("console").start()


# Output the alerts to the console
query = alerts_df.writeStream.outputMode("append").format("console").start()

parsed.awaitTermination()
al1.awaitTermination()
al2.awaitTermination()

query.awaitTermination()
