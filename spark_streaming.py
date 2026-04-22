from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ----------------------------
# SESIÓN SPARK
# ----------------------------
spark = SparkSession.builder \
    .appName("StreamingSensores") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ----------------------------
# ESQUEMA
# ----------------------------
schema = StructType([
    StructField("sensor_id", StringType()),
    StructField("tipo", StringType()),
    StructField("valor", DoubleType()),
    StructField("timestamp", LongType())
])

# ----------------------------
# LEER DESDE KAFKA
# ----------------------------
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:29092") \
    .option("subscribe", "sensores") \
    .option("startingOffsets", "latest") \
    .load()

# ----------------------------
# PROCESAR JSON
# ----------------------------
data = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# ----------------------------
# CONVERTIR TIEMPO
# ----------------------------
data = data.withColumn("event_time", to_timestamp(col("timestamp")))

# ----------------------------
# GUARDAR A ARCHIVOS (CLAVE)
# ----------------------------
query = data.writeStream \
    .format("json") \
    .outputMode("append") \
    .option("path", "output_sensores") \
    .option("checkpointLocation", "checkpoint_sensores") \
    .trigger(processingTime="5 seconds") \
    .start()

# ----------------------------
# MANTENER STREAM ACTIVO
# ----------------------------
query.awaitTermination()