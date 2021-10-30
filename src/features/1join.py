import pandas as pd
import numpy as np 
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
print("--------------------------------------------------------------------------------")

git_commits = spark.read.csv("./data/GIT_COMMITS.csv", header=True, sep=",", multiLine=True).drop("PROJECT_ID","AUTHOR","AUTHOR_DATE","AUTHOR_TIMEZONE","COMMITTER","COMMITER_DATE","COMMITTER_TIMEZONE","BRANCHES","IN_MAIN_BRANCH","MERGE")
git_commits_changes = spark.read.csv("./data/GIT_COMMITS_CHANGES.csv", header=True, sep=",", multiLine=True).drop("PROJECT_ID","DATE","COMMITTER_ID","NOTE")

git_commits.show()
git_commits_changes.show()

inner_join = git_commits.join(git_commits_changes, "COMMIT_HASH")

inner_join = inner_join.toPandas()
inner_join.to_csv(r"./data/preprocessing/GIT_COMMITS_DATA.csv", index=False)
