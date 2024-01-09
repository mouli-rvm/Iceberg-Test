from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import StringType, StructField, StructType, IntegerType


warehouse_path = "./warehouse"
iceberg_spark_jar = "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.0"
catalog_name = "demo"

# Setup iceberg config
conf = SparkConf().setAppName("IceBergApp")\
    .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")\
    .set(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")\
    .set('spark.jars.packages', iceberg_spark_jar)\
    .set(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse_path)\
    .set(f"spark.sql.catalog.{catalog_name}.type", "hadoop")\
    .set("spark.sql.defaultCatalog", catalog_name)

# Create spark session
spark = SparkSession.builder.config(conf=conf).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
table_name = "db.persons"

"""
# Create a data frame
schema = StructType([
    StructField('name', StringType(), True),
    StructField('age', IntegerType(), True),
    StructField('job_title', StringType(), True)
])
data = [("person1", 28, "Doctor"), ("person2", 35, "Singer"), ("person3", 42, "Teacher")]
df = spark.createDataFrame(data, schema)
df.printSchema()
df.show()

#Write and read iceberg table
df.write.format("iceberg").mode("overwrite").saveAsTable(f"{table_name}")
iceberg_df = spark.read.format("iceberg").load(f"{table_name}")
iceberg_df.printSchema()
iceberg_df.show()

#Schema Evolution
spark.sql(f"ALTER TABLE {table_name} RENAME COLUMN job_title TO job")
spark.sql(f"ALTER TABLE {table_name} ADD COLUMN salary FLOAT after job")

iceberg_df = spark.read.format("iceberg").load(f"{table_name}")
iceberg_df.printSchema()
iceberg_df.show()

spark.sql(f"SELECT * FROM {table_name}.snapshots").show()
spark.sql(f"UPDATE {table_name} SET salary = 100")
spark.sql(f"DELETE FROM {table_name} WHERE age=42")
spark.sql(f"INSERT INTO {table_name} VALUES ('person4',50,'Engineer',200)")
spark.sql(f"SELECT * FROM {table_name}.snapshots").show()
"""

iceberg_df = spark.read.format("iceberg").load(f"{table_name}")
iceberg_df.printSchema()
iceberg_df.show()

#spark.sql(f"ALTER table {table_name} ADD PARTITION FIELD age")
#spark.read.format("iceberg").load(f"{table_name}").where("age=28").show()

spark.sql(f"SELECT * FROM {table_name}.snapshots").show()
spark.sql(f"SELECT * FROM {table_name}.snapshots").show(1, truncate=False)
spark.read.option("snapshot-id", "3346351941241183008").table(table_name).show()

spark.read.option("as-of-timestamp", "3346351941241183008").table(table_name).show()

