#!/bin/bash

# trap for exiting the loop.
trap break INT

# paths.
OUTPUT="./output/"
SHELLCOMMANDS="./shellCommands/"
SPARKSHELL="../../bin/spark-shell"

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

for file in $SHELLCOMMANDS*; do
    cat $file | $SPARKSHELL | tee ${OUTPUT}$(basename $file)_output.txt
done
