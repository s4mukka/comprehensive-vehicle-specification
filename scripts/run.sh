#! /bin/bash

file=$1

rm -f /app/datalib.zip
zip -r /app/datalib.zip datalib
# --properties-file $SPARK_HOME/conf/spark-defaults.conf
spark-submit $1
