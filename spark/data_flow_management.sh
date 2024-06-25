#!/bin/bash


BRONZE_SCRIPT="/workspace/process_bronze.py"
SILVER_SCRIPT="/workspace/process_silver.py"
GOLD_SCRIPT="/workspace/process_gold.py"


echo "Starting bronze process..."
python3 $BRONZE_SCRIPT &
BRONZE_PID=$!
echo "Bronze process started with PID $BRONZE_PID"


START_TIME=$(date +%s)

while true; do
    CURRENT_TIME=$(date +%s)
    ELAPSED_TIME=$((CURRENT_TIME - START_TIME))
    
    if (( ELAPSED_TIME % 150 == 0 )); then
        echo "Running silver process..."
        python3 $SILVER_SCRIPT &
        SILVER_PID=$!
        echo "Silver process started with PID $SILVER_PID"
    fi
    
    if (( ELAPSED_TIME % 300 == 0 )); then
        echo "Running gold process..."
        spark-submit --jars /opt/spark/jars/postgresql-42.5.0.jar $GOLD_SCRIPT &
        GOLD_PID=$!
        echo "Gold process started with PID $GOLD_PID"
    fi
    
    sleep 1
done