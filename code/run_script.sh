#!/bin/bash
#Script to run the code.

mvn clean package
$SPARK_HOME/bin/spark-submit --class "BoschAnalysis" --master local[4] target/simple-project-1.0.jar > Output.txt
echo "Output File Output.txt Written"
cat Output.txt
