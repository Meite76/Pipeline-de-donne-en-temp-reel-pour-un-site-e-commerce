from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType

# Création de la session Spark
spark = SparkSession.builder \
    .appName("EcommerceKafkaStreaming") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schéma du JSON attendu
schema = StructType([
    StructField("user_id", StringType()),
    StructField("event_type", StringType()),
    StructField("product_id", StringType()),
    StructField("timestamp", LongType()),
    StructField("price", DoubleType())
])

# Lecture du flux Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ecommerce_events") \
    .option("startingOffsets", "latest") \
    .load()

# Les données sont dans la colonne 'value' en bytes, on cast en string
events = df.selectExpr("CAST(value AS STRING) as json_str")

# Parse le JSON avec le schéma
events_parsed = events.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

# Exemple de transformation : filtrer uniquement les achats (event_type='purchase')
purchases = events_parsed.filter(col("event_type") == "purchase")

# Écriture dans un dossier local (simulant un Data Lake)
query = purchases.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "data_lake/purchases") \
    .option("checkpointLocation", "data_lake/checkpoint") \
    .start()

query.awaitTermination()
