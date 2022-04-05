#!/bin/bash

# trap for exiting the loop(s).
trap break INT

# paths.
OUTPUT="./output/"
SHELLCOMMANDS="./shellCommands/"
SPARKSHELL="../../bin/spark-shell"

RUNS=5

# sanity checks.

if [ -f "../../bin/spark-shell" ]; then
    echo "spark-shell found!" 
else
    echo "spark-shell not found! ../../bin/spark-shell"
    echo "Please run from the \`scripts/timing\` folder"
    exit 1
fi

if [ -d "./shellCommands/" ]; then
    echo "spark-shell commands found!"
else
    echo "spark-shell commands not found! ./shellCmmands"
fi

if [ -d "./output" ]; then
   echo "output folder found!"
else
    echo "output folder not found! creating now..."
    mkdir "./output"
fi

# loop over commands, and write output to both `stdout` and
# specific output files.
for run in $(seq 1 $RUNS); do
    mkdir ${OUTPUT}run${run}
    for file in $SHELLCOMMANDS*; do
        echo $run
        cat $file | $SPARKSHELL | tee ${OUTPUT}run${run}/$(basename "$file" .scala).txt
    done
done
