import pandas as pd
import numpy as np 
import functions as aux
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

spark = SparkSession.builder.getOrCreate()
print("--------------------------------------------------------------------------------")

data = spark.read.csv("./data/preprocessing/GIT_COMMITS_DATA.csv", header=True, sep=",", multiLine=True)

data = data.withColumn("FILE", f.split(f.col("FILE"), "/")).withColumn("FILE", f.element_at(f.col("FILE"), -1))
commit_files = data.groupBy("COMMIT_HASH").agg(f.collect_set("FILE").alias("FILE")).toPandas()

dict = commit_files.set_index("COMMIT_HASH")["FILE"].to_dict()
data = data.toPandas()

data["COMMIT_MESSAGE"] = data.apply(aux.clean_messages, file_list=dict, axis=1)

data.to_csv(r"./data/preprocessing/DATA_PRE1.csv", index=False)

