from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Uber_ETL").getOrCreate()

# -------------------- INPUT PATHS --------------------
india_path = "/workspace/trips_india.csv"
europe_path = "/workspace/trips_europe.csv"
vehicles_path = "/workspace/vehicles_big.csv"
cities_path = "/workspace/cities_big.txt"
drivers_path = "/workspace/drivers.json"   # JSON array

# -------------------- LOAD INDIA CSV --------------------
india = spark.read.option("header", True).csv(india_path)
india = india.withColumn("Region", lit("India"))

# -------------------- LOAD EUROPE CSV --------------------
europe = spark.read.option("header", True).csv(europe_path)
europe = europe.withColumn("Region", lit("Europe"))

# -------------------- COMBINE INDIA + EUROPE --------------------
uber = india.unionByName(europe, allowMissingColumns=True)

# Convert numeric columns
uber = uber.withColumn("Booking Value", col("Booking Value").cast("double"))
uber = uber.withColumn("Ride Distance", col("Ride Distance").cast("double"))

# Create datetime
uber = uber.withColumn(
    "Booking_Datetime",
    to_timestamp(concat_ws(" ", col("Date"), col("Time")))
)

# Flags
uber = uber.withColumn("Is_Cancelled",
                when(col("Booking Status").contains("Cancelled"), 1).otherwise(0))

# -------------------- SAVE MAIN CSV OUTPUTS --------------------
uber.write.mode("overwrite").csv("/workspace/final_output/uber_final", header=True)
india.write.mode("overwrite").csv("/workspace/final_output/final_india", header=True)
europe.write.mode("overwrite").csv("/workspace/final_output/final_europe", header=True)

# -------------------- LOAD VEHICLES CSV --------------------
vehicles = spark.read.option("header", True).csv(vehicles_path)

# -------------------- LOAD CITIES TXT --------------------
cities = spark.read.text(cities_path).withColumnRenamed("value", "city_name")

# -------------------- LOAD DRIVERS JSON (MULTILINE FIX) --------------------
drivers = (
    spark.read
    .option("multiline", "true")
    .option("mode", "PERMISSIVE")
    .json(drivers_path)
)

# -------------------- SAVE OTHER SOURCES --------------------
drivers.write.mode("overwrite").json("/workspace/final_output/drivers_final")
vehicles.write.mode("overwrite").csv("/workspace/final_output/vehicles_final", header=True)
cities.write.mode("overwrite").csv("/workspace/final_output/cities_final", header=True)

print("ETL Completed ✓")
