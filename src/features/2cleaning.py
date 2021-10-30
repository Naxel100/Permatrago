import pandas as pd
import numpy as np
import functions as aux
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import StringType

def main():
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext
    print("--------------------------------------------------------------------------------")

    data = spark.read.csv("../../data/interim/GIT_COMMITS_DATA.csv", header=True, sep=",", multiLine=True)

    data = data.withColumn("FILE", f.split(f.col("FILE"), "/")).withColumn("FILE", f.element_at(f.col("FILE"), -1))
    commit_files = data.groupBy("COMMIT_HASH").agg(f.collect_set("FILE").alias("FILE_LIST"))#.toPandas()
    data = data.drop("FILE")

    data_join = data.join(commit_files, "COMMIT_HASH")
    del commit_files
    cleanMessageUDF = f.udf(lambda x: str(aux.cleanMessage(x)), StringType())
    data_join = data_join.withColumn("COMMIT_MESSAGE", cleanMessageUDF(f.col("COMMIT_MESSAGE")))
    tokenizeFileUDF = f.udf(lambda x, y: str(aux.tokenizeFile(x, y)), StringType())
    data_join = data_join.withColumn("COMMIT_MESSAGE", tokenizeFileUDF(f.col("COMMIT_MESSAGE"), f.col("FILE_LIST")))
    data_join.toPandas().to_csv(r"../../data/processed/DATA_PRE1.csv", index=False)

if __name__ == "__main__":
    main()
