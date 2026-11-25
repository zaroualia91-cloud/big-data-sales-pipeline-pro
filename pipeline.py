"""
Big Data Sales Pipeline - PRO Version
Author: NABIL
100k rows | PySpark Optimized
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, desc, countDistinct
import random
from datetime import datetime

# PRO CONFIG - Spark Optimization
spark = SparkSession.builder \
    .appName("BigData_PRO_NABIL") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

print("🚀 Starting Big Data Pipeline PRO - By NABIL")

# Generate 100k rows - Distributed
data = []
for i in range(100000):
    data.append((
        i,
        f"client_{random.randint(1,20000)}",
        f"produit_{random.randint(1,500)}",
        random.randint(1,20),
        random.randint(50,5000),
        f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
    ))

schema = ["id", "client", "produit", "quantite", "prix", "date"]
df = spark.createDataFrame(data, schema)

# PRO OPTIMIZATION
df.cache()  # In-memory caching
df = df.withColumn("total", col("quantite") * col("prix"))

# ANALYTICS
print("\n💰 Total Revenue:")
df.agg(_sum("total").alias("revenue")).show()

print("\n📦 Top 10 Products:")
df.groupBy("produit").agg(_sum("quantite").alias("total_qty")).orderBy(desc("total_qty")).show(10)

print("\n👥 Client Stats:")
df.agg(countDistinct("client").alias("unique_clients")).show()

print(f"\n✅ Pipeline completed - {datetime.now()}")
