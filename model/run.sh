#!/usr/bin/env bash
export PATH=/usr/local/anaconda3/bin:$PATH
export PYSPARK_PYTHON=/usr/local/anaconda3/bin/python3.6 && \
export PYSPARK_DRIVER_PYTHON=/usr/local/anaconda3/bin/python3.6 && \

dir=`pwd`
echo "#####current-project-dir:${dir}#####"

cd ${dir}/coupon_recommend/model

WORK_DIR=`pwd`
echo "WORK_DIR = $WORK_DIR"

NDAY=$1 ###必填项，模型训练计划时间 2021-06-17

execute_dt=`date --date="$NDAY" +%Y-%m-%d`

spark-submit \
  --master yarn \
  --archives hdfs://ns9/user/jdw_dwm_bqyf/pyenvs/python36.zip#clv_py_env \
  --driver-memory 6G --executor-memory 6G \
  --num-executors 255 --executor-cores 4 --driver-cores 4 \
  --conf spark.default.parallelism=2500 \
  --conf spark.sql.shuffle.partitions=2500 \
  --conf spark.network.timeout=4800s \
  --conf spark.driver.maxResultSize=20g \
  --conf spark.sql.hive.mergeFiles=true \
  --conf spark.sql.auto.repartition=true \
  --conf spark.shuffle.service.enabled=true \
  --jars ../price_elasticity/pyspark-xgboost_xgboost4j-spark-0.90.jar,../price_elasticity/pyspark-xgboost_xgboost4j-0.90.jar \
  coupon_before_model_train.py --execute_dt ${execute_dt}