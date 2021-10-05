import pandas as pd
pd.options.display.max_columns = 999
pd.options.display.max_rows = 999
pd.options.display.max_colwidth = 50
import numpy as np 
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
print("--------------------------------------------------------------------------------")

git_commit = pd.read_csv('./data/GIT_COMMITS.csv')
#git_commit = spark.read.csv('./data/GIT_COMMITS.csv',header=True)
#git_commit = git_commit.toPandas()

commit_dict = {}
i = 0
for idx, row in git_commit.iterrows():
    if row.COMMIT_HASH in commit_dict.keys():
        print(row.COMMIT_MESSAGE, commit_dict[row.COMMIT_HASH])
    commit_dict[row.COMMIT_HASH] = row.COMMIT_MESSAGE
    if i > 10: break
    i += 1

#print(len(commit_dict), git_commit.count())



print("aaaaa")