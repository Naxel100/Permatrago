import pandas as pd
pd.options.display.max_columns = 999
pd.options.display.max_rows = 999
pd.options.display.max_colwidth = 50
import numpy as np 
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
print("--------------------------------------------------------------------------------")

git_commits = spark.read.csv("./data/GIT_COMMITS.csv", header=True, sep=",", multiLine=True).drop("PROJECT_ID","AUTHOR","AUTHOR_DATE","AUTHOR_TIMEZONE","COMMITTER","COMMITER_DATE","COMMITTER_TIMEZONE","BRANCHES","IN_MAIN_BRANCH","MERGE")
git_commits_changes = spark.read.csv("./data/GIT_COMMITS_CHANGES.csv", header=True, sep=",", multiLine=True).drop("PROJECT_ID","DATE","COMMITER_ID")

git_commits.show()
git_commits_changes.show()

git_commits.join(git_commits_changes, git_commits.COMMIT_HASH == git_commits_changes.COMMIT_HASH)
git_commits.printSchema()
git_commits_changes.printSchema()

git_commits = git_commits.toPandas()
git_commits.to_csv(r"./data/preprocessing/GIT_COMMIT_DATA.csv", index=False)

'''

valuesA = [('Pirate',1),('Monkey',2),('Ninja',3),('Spaghetti',4)]
TableA = spark.createDataFrame(valuesA,['name','id'])
 
valuesB = [('Rutabaga',1),('Pirate',2),('Ninja',3),('Darth Vader',4)]
TableB = spark.createDataFrame(valuesB,['name','id'])

inner_join = TableA.join(TableB, TableA.name == TableB.name)
inner_join.printSchema()

'''