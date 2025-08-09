from pyspark.sql import SparkSession

# Start Spark session
spark = SparkSession.builder \
    .appName("Convert PFOS - No Schema") \
    .config("spark.driver.memory", "64g") \
    .getOrCreate()

# Read tab-delimited text file without schema
df = spark.read.option("delimiter", "\t").csv("/home/emcj/data/MAG/PaperFieldsOfStudy.txt")

# Rename columns
df = df.toDF("PaperId", "FieldOfStudyId", "Score")

# Save as CSV with header
df.coalesce(1).write.option("header", True).csv("PaperFieldsOfStudy_converted.csv")

print("[DONE] CSV written.")


