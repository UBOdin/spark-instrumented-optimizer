from collections import defaultdict
import json
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import os
import re

# this script is expecting to be invoked by `make` in the root directory of the project.

# the parent directory for batches of runs is expected to be laid out like so:
#############################
#                           #
# parent                    #
#   |                       #
#   |--run1                 # 
#   |   |--runDataFile1     #
#   |   |--runDataFile2     #
#   |   |--runDataFile`n`   #
#   |                       #
#   |--run2                 #
#   |   |--runDataFile1     #
#   |   |--runDataFile2     #
#   |   |--runDataFile`n`   #
#   |                       #
#   |--run`n`               #
#   |   |--runDataFile1     #
#   |   |--runDataFile2     #
#   |   |--runDataFile`n`   #
#                           #
#############################
# where you could have an arbitrary number of sub directories and data files with arbitrary names
dataOutputDirectory = "./scripts/timing/output/"

# graph output directory
graphOutputDirectory = "./scripts/plotting/output/"

# regular expression string to match the JSON data string
reDataString = '{"data":\D.*}}'

# regular expression string to match and identify the query number
reQueryString = r'Query.(?P<number>\d\d|\d)."'

# number of runs is calculated by the amount of subdirectories traversed after the parent directory, this is just for initialization
numberOfRuns = -1

# graph output names
stackedGraphOutputFile = "stackedGraph.pdf"

# storage for totals, then the averages of the data from runs
queryDataDict = defaultdict(dict)

for root, dirs, files in os.walk(dataOutputDirectory):
    numberOfRuns += 1
    if files != None:
        for name in files:
            with open(os.path.join(root, name)) as dataFile:
                jsonData = defaultdict(float)
                queryNumber = "0"
                for line in dataFile:
                    queryMatch = re.search(reQueryString, line)
                    if queryMatch:
                        queryNumber = str(queryMatch.group("number"))
                    if re.match(reDataString, line):
                        cleanedString = line.replace("\n","")
                        jsonData = json.loads(cleanedString)
                        for keyName in jsonData["data"]:
                            if keyName not in queryDataDict[queryNumber]:
                                queryDataDict[queryNumber][keyName] = 0
                            queryDataDict[queryNumber][keyName] += jsonData["data"][keyName]

# manipulating data
xAxis = []
search = []
ineffective = []
effective = []
execution = []
leftover = []
for rootKey in queryDataDict:
    for subKey in queryDataDict[rootKey]:
        queryDataDict[rootKey][subKey] = queryDataDict[rootKey][subKey] / numberOfRuns
for i in range(1,23):
    xAxis.append(("Q" + str(i)))
    search.append(queryDataDict[str(i)]["transformTime"] / 1000000000.0)
    ineffective.append(queryDataDict[str(i)]["ineffectiveMatchTime"] /1000000000.0)
    effective.append(queryDataDict[str(i)]["effectiveMatchTime"] / 1000000000.0)
    execution.append(queryDataDict[str(i)]["executorTime"] / 1000000000.0)
    leftover.append(abs(queryDataDict[str(i)]["applyTime"] - queryDataDict[str(i)]["transformTime"]) / 1000000000.0)

# stacked graph generation
plt.figure(figsize=(10, 6))
plt.rcParams.update({'font.size': 18})
plt.xticks(rotation = 90, label = "")
plt.tick_params(axis = 'x', which = 'major', labelsize = 15.0)
plt.ylim(ymin = 0, ymax = 3)
plt.xlabel('TPC-H Query #')
plt.ylabel('Total Time Spent Optimizing (sec)')
plt.bar(xAxis, search, label='Search', color = '#00263E')
plt.bar(xAxis, ineffective, label='Ineffective Rewrites', color = '#0062A0', bottom = search)
plt.bar(xAxis, effective, label='Effective Rewrites', color = '#409EDA', bottom = np.array(search) + np.array(ineffective))
plt.bar(xAxis, execution, label='Fixpoint Loop', color = '#76C8FC', bottom = np.array(search) + np.array(ineffective) + np.array(effective))
plt.bar(xAxis, leftover, label='Untracked', color = '#7A0097', bottom = np.array(search) + np.array(ineffective) + np.array(effective) + np.array(execution))
plt.legend(loc="upper left")

plt.savefig(graphOutputDirectory + stackedGraphOutputFile ,bbox_inches='tight')
