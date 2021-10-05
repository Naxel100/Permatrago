import pandas as pd
pd.options.display.max_columns = 999
pd.options.display.max_rows = 999
pd.options.display.max_colwidth = 50
import numpy as np 

print("--------------------------------------------------------------------------------")


git_commits = pd.read_csv("./data/GIT_COMMITS.csv").drop(["PROJECT_ID","AUTHOR","AUTHOR_DATE","AUTHOR_TIMEZONE","COMMITTER","COMMITER_DATE","COMMITTER_TIMEZONE","BRANCHES","IN_MAIN_BRANCH","MERGE"], axis=1)
git_commits_changes = pd.read_csv("./data/GIT_COMMITS_CHANGES.csv").drop(["PROJECT_ID","DATE","COMMITER_ID"],axis=1)

git_commits.set_index("COMMIT_HASH").join(git_commits_changes.set_index("COMMIT_HASH"))
#git_commits = pd.merge(git_commits, git_commits_changes, on="COMMIT_HASH")
git_commits.to_csv("./data/preprocessing/GIT_COMMIT_DATA", index=False)

commit_dict = {}
i = 0
for idx, row in git_commits.iterrows():
    #if row.COMMIT_HASH in commit_dict.keys():
    #    print(row.COMMIT_MESSAGE, commit_dict[row.COMMIT_HASH])
    commit_dict[row.COMMIT_HASH] = row.COMMIT_MESSAGE

    print(row)
    i+=1
    if i > 0: break

