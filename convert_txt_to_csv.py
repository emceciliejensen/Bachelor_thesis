from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Convert PFOS - No Schema") \
    .config("spark.driver.memory", "64g") \
    .getOrCreate()

df = spark.read.option("delimiter", "\t").csv("/home/emcj/data/MAG/PaperFieldsOfStudy.txt")

df = df.toDF("PaperId", "FieldOfStudyId", "Score")

df.coalesce(1).write.option("header", True).csv("PaperFieldsOfStudy_converted.csv")



