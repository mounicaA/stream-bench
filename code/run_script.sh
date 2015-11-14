#!/bin/bash
#Script to run the code.

mvn clean package -e
spark-submit --class BoschAnalysis --master yarn-client  --num-executors 4 --driver-memory 4g --executor-memory 4g --executor-cores 10 target/simple-project-1.0.jar > $i'Output.txt'
echo "Output File Output.txt Written"
