#!/usr/bin/env python3
# coding: utf-8

import argparse
import datetime
import os

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window

from sku_deal_feature import first_day_of_last_month

os.environ['PYSPARK_PYTHON'] = "/usr/local/anaconda3/bin/python3.6"
os.environ['PYSPARK_DRIVER_PYTHON'] = "/usr/local/anaconda3/bin/python3.6"
os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--jars pyspark-xgboost_xgboost4j-spark-0.90.jar,pyspark-xgboost_xgboost4j-0.90.jar pyspark-shell'

'''
CREATE EXTERNAL TABLE IF NOT EXISTS adm.adm_sku_price_elasticity_month_mid(
item_sku_id string COMMENT '商品SKU编号',
item_name string COMMENT '商品名称',
item_first_cate_cd string COMMENT '商品一级分类代码',
item_second_cate_cd string COMMENT '商品二级分类代码',
item_third_cate_cd string COMMENT '商品三级分类代码',
brand_cd string COMMENT '品牌代码',
sale_ord_dt string COMMENT '销售订单订购日期',
u_hat double COMMENT 'dLnQ残差',
v_hat double COMMENT 'dLnP残差',
theta double COMMENT '价格弹性')
COMMENT 'sku价格弹性结果中间表'
PARTITIONED BY (
  dt string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns1/user/dd_edw/adm.db/adm_sku_price_elasticity_month_mid';
'''
'''
CREATE EXTERNAL TABLE IF NOT EXISTS adm.adm_sku_price_elasticity_month(
item_sku_id string COMMENT '商品SKU编号',
item_name string COMMENT '商品名称',
item_first_cate_cd string COMMENT '商品一级分类代码',
item_second_cate_cd string COMMENT '商品二级分类代码',
item_third_cate_cd string COMMENT '商品三级分类代码',
brand_cd string COMMENT '品牌代码',
theta double COMMENT '价格弹性')
COMMENT 'sku价格弹性结果表'
PARTITIONED BY (
  dt string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns1/user/dd_edw/adm.db/adm_sku_price_elasticity_month';
'''

LOG_PREFIX = "[■■□□ ■□■■ □■■□]"

FEATURE_COLS = ['id', 'avg_offer_amt', 'avg_coupon_amt', 'day_of_month', 'week_of_month', 'day_of_week', 'month',
                'weekend', 'sale_qtty_lim', 'sku_inventory_status', 'real_jd_ratio', 'real_before_ratio',
                'offer_discount_ratio', 'coupon_discount_ratio', 'diff_real_ratio', 'sku_median_prc', 'dLnP', 'dLnQ',
                'c3d_vec0', 'c3d_vec1', 'c3d_vec2', 'c3d_vec3', 'c3d_vec4', 'c3d_vec5', 'c3d_vec6', 'c3d_vec7',
                'c3d_vec8', 'c3d_vec9', 'c3d_vec10', 'c3d_vec11', 'c3d_vec12', 'c3d_vec13', 'c3d_vec14', 'c3d_vec15',
                'c3d_vec16', 'c3d_vec17', 'c3d_vec18', 'c3d_vec19', 'c3d_vec20', 'c3d_vec21', 'c3d_vec22', 'c3d_vec23',
                'c3d_vec24', 'c3d_vec25', 'c3d_vec26', 'c3d_vec27', 'c3d_vec28', 'c3d_vec29', 'c3d_vec30', 'c3d_vec31',
                'c3d_vec32', 'c3d_vec33', 'c3d_vec34', 'c3d_vec35', 'c3d_vec36', 'c3d_vec37', 'c3d_vec38', 'c3d_vec39',
                'c3d_vec40', 'c3d_vec41', 'c3d_vec42', 'c3d_vec43', 'c3d_vec44', 'c3d_vec45', 'c3d_vec46', 'c3d_vec47',
                'c3d_vec48', 'c3d_vec49', 'c3d_vec50', 'c3d_vec51', 'c3d_vec52', 'c3d_vec53', 'c3d_vec54', 'c3d_vec55',
                'c3d_vec56', 'c3d_vec57', 'c3d_vec58', 'c3d_vec59', 'c3d_vec60', 'c3d_vec61', 'c3d_vec62', 'c3d_vec63']

INT_COLS = ['day_of_month', 'week_of_month', 'day_of_week', 'month']

spark = SparkSession \
    .builder \
    .appName("train_price_elasticity_model") \
    .config("spark.sql.crossJoin.enabled", "true") \
    .config("hive.exec.dynamic.partition", "true") \
    .config("spark.driver.memory", "20g") \
    .config("spark.executor.memory", "20g") \
    .config("spark.driver.cores", 4) \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .config("spark.kryoserializer.buffer.max", "2048m") \
    .config("spark.debug.maxToStringFields", "10000") \
    .config("spark.default.parallelism", "2500") \
    .config("spark.sql.shuffle.partitions", "2500") \
    .config("spark.speculation", "true") \
    .config("spark.executor.instances", "400") \
    .config("spark.yarn.appMasterEnv.yarn.nodemanager.container-executor.class", "DockerLinuxContainer") \
    .config("spark.executorEnv.yarn.nodemanager.container-executor.class", "DockerLinuxContainer") \
    .config("spark.yarn.appMasterEnv.yarn.nodemanager.docker-container-executor.image-name",
            "bdp-docker.jd.com:5000/wise_mart_bag:latest") \
    .config("spark.executorEnv.yarn.nodemanager.docker-container-executor.image-name",
            "bdp-docker.jd.com:5000/wise_mart_bag:latest") \
    .config("spark.yarn.nodemanager.vmem-check-enabled", "false") \
    .config("spark.yarn.executor.memoryOverhead", "50g") \
    .config("spark.driver.maxResultSize", "20g") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
spark.sparkContext.addPyFile("pyspark-xgboost_sparkxgb.zip")

from dml import LinearDML


def dataframe_add_id(df):
    """
        给spark的dataframe添加从1开始自增的id列
        :param df: spark的dataframe
        :return: spark.DataFrame
    """
    schema = df.schema.add(StructField("id", LongType()))
    rdd = df.rdd.zipWithIndex()

    def flat(l):
        for k in l:
            if not isinstance(k, (list, tuple)):
                yield k
            else:
                yield from flat(k)

    rdd = rdd.map(lambda x: list(flat(x)))
    df_with_id = spark.createDataFrame(rdd, schema).fillna(0)

    return df_with_id


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--execute_dt', type=str, default='', help='date of script execution')

    args = parser.parse_args()
    execute_dt = args.execute_dt
    write_dt = first_day_of_last_month(execute_dt)
    start_dt = (datetime.datetime.strptime(execute_dt, '%Y-%m-%d').replace(day=1) - datetime.timedelta(
        days=370)).strftime("%Y-%m-%d")

    print(LOG_PREFIX, 'execute_dt=%s,start_dt=%s,write_dt=%s' % (execute_dt, start_dt, write_dt))

    cate_vec_df = spark.createDataFrame(pd.read_csv('./cate_vec.csv'))
    feature_df = spark.sql("""
    SELECT
        *
    FROM
        adm.adm_sku_deal_ord_feature_month
    WHERE
        dt >= '{start_dt}'
    """.format(start_dt=start_dt)) \
        .withColumn('sku_prc_cnt', F.count('real_price').over(Window.partitionBy('item_sku_id'))).where(
        'sku_prc_cnt > 2') \
        .withColumn('sku_prc_std', F.stddev('real_price').over(Window.partitionBy('item_sku_id'))).where(
        'sku_prc_std > 0') \
        .withColumn('sku_mean_prc', F.mean('real_price').over(Window.partitionBy('item_sku_id'))) \
        .withColumn('sku_mean_qtty', F.mean('sale_qtty').over(Window.partitionBy('item_sku_id'))) \
        .withColumn('sku_median_prc',
                    F.expr('percentile_approx(real_price, 0.5, 9999)').over(Window.partitionBy('item_sku_id'))) \
        .withColumn('dLnP', F.col('LnP') - F.log(F.col('sku_mean_prc'))) \
        .withColumn('dLnQ', F.col('LnQ') - F.log(F.col('sku_mean_qtty'))) \
        .join(cate_vec_df, ['item_third_cate_cd'], 'left').fillna(0)

    for col in INT_COLS:
        feature_df = feature_df.withColumn(col, F.col(col).cast('int'))

    feature_df = dataframe_add_id(feature_df).cache()
    dml_df = feature_df.select(FEATURE_COLS)

    print(LOG_PREFIX, '当前时间', datetime.datetime.now().strftime('%H:%M:%S'), ' 模型开始训练！')

    est = LinearDML(spark, dml_df, model_y='rf', model_t='rf', discrete_treatment=False, cv=2)
    est.fit(Y='dLnQ', T='dLnP', ite_stat=False)

    print(LOG_PREFIX, '当前时间', datetime.datetime.now().strftime('%H:%M:%S'), ' 模型训练完成！')
    print(LOG_PREFIX)
    print(est.get_ate().show())

    ITE_mid_df = est.spark_data.selectExpr('id', 'u_hat', 'v_hat', 'ite as theta') \
        .join(feature_df, ['id'], 'inner') \
        .select('item_sku_id', 'item_name', 'item_first_cate_cd', 'item_second_cate_cd', 'item_third_cate_cd',
                'brand_cd', 'sale_ord_dt', 'u_hat', 'v_hat', 'theta')

    ITE_mid_df.registerTempTable('price_elasticity_mid')

    spark.sql("""
        INSERT OVERWRITE TABLE adm.adm_sku_price_elasticity_month_mid PARTITION(dt='{dt}')
            SELECT
                item_sku_id,item_name,item_first_cate_cd,item_second_cate_cd,item_third_cate_cd,brand_cd,
                sale_ord_dt,u_hat,v_hat,theta
            FROM price_elasticity_mid
        """.format(dt=write_dt))

    print(LOG_PREFIX, '写adm_sku_price_elasticity_month_mid表完成')

    ITE_df = ITE_mid_df.groupBy('item_sku_id', 'item_name', 'item_first_cate_cd', 'item_second_cate_cd',
                                'item_third_cate_cd', 'brand_cd') \
        .agg(F.mean('theta').alias('theta'))

    ITE_df.registerTempTable('price_elasticity')

    spark.sql("""
        INSERT OVERWRITE TABLE adm.adm_sku_price_elasticity_month PARTITION(dt='{dt}')
            SELECT
                item_sku_id,item_name,item_first_cate_cd,item_second_cate_cd,item_third_cate_cd,brand_cd,theta
            FROM price_elasticity
        """.format(dt=write_dt))

    print(LOG_PREFIX, '写adm_sku_price_elasticity_month表完成')
