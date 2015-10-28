#!/bin/bash
#Script to run the code.

mvn clean package -e
spark-submit --class BoschAnalysis --master yarn-client  --num-executors 4 --driver-memory 4g     --executor-memory 4g --executor-cores 3 target/simple-project-1.0.jar 10 > Output.txt
echo "Output File Output.txt Written"
#cat Output.txt
