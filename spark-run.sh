#!/bin/bash

# Description: This script is used to run a Spark application
APP_NAME="Flight_v2"
MAIN_CLASS="Main"
DEPLOYED_MODE="client"
SPARK_MASTER="local[*]"
JAR_FILE="target/scala-2.12/flight_2_2.12-0.1.0-SNAPSHOT.jar"

# Run the Spark application
spark-submit \
    --class $MAIN_CLASS \
    --deploy-mode $DEPLOYED_MODE \
    --name $APP_NAME \
    --master $SPARK_MASTER \
    $JAR_FILE \
    "data/"