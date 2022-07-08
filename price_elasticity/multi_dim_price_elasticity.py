#!/usr/bin/env python3
# coding: utf-8

import argparse
import datetime
import os

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window

os.environ['PYSPARK_PYTHON'] = "/usr/local/anaconda3/bin/python3.6"
os.environ['PYSPARK_DRIVER_PYTHON'] = "/usr/local/anaconda3/bin/python3.6"

'''
CREATE EXTERNAL TABLE IF NOT EXISTS adm.adm_brand_c3d_price_elasticity_month(
item_third_cate_cd string COMMENT '商品三级分类代码',
brand_cd string COMMENT '品牌代码',
theta double COMMENT '价格弹性')
COMMENT '品牌*三级类目价格弹性结果表'
PARTITIONED BY (
  dt string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns1/user/dd_edw/adm.db/adm_brand_c3d_price_elasticity_month';
'''
'''
CREATE EXTERNAL TABLE IF NOT EXISTS adm.adm_c3d_price_elasticity_month(
item_third_cate_cd string COMMENT '商品三级分类代码',
theta double COMMENT '价格弹性')
COMMENT '三级类目价格弹性结果表'
PARTITIONED BY (
  dt string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns1/user/dd_edw/adm.db/adm_c3d_price_elasticity_month';
'''

LOG_PREFIX = "[■■□□ ■□■■ □■■□]"

spark = SparkSession \
    .builder \
    .appName("sku_deal_feature") \
    .enableHiveSupport() \
    .config("spark.executor.instances", "255") \
    .config("spark.executor.memory", "6g") \
    .config("spark.executor.cores", "4") \
    .config("spark.driver.memory", "8g") \
    .config("spark.driver.maxResultSize", "10g") \
    .config("spark.sql.shuffle.partitions", "2500") \
    .config("spark.sql.broadcastTimeout", "36000") \
    .config("spark.shuffle.service.enabled", "true") \
    .config("spark.sql.auto.repartition", "true") \
    .config("spark.yarn.appMasterEnv.yarn.nodemanager.container-executor.class", "DockerLinuxContainer") \
    .config("spark.executorEnv.yarn.nodemanager.container-executor.class", "DockerLinuxContainer") \
    .config("spark.yarn.appMasterEnv.yarn.nodemanager.docker-container-executor.image-name",
            "bdp-docker.jd.com:5000/wise_mart_bag:latest") \
    .config("spark.executorEnv.yarn.nodemanager.docker-container-executor.image-name",
            "bdp-docker.jd.com:5000/wise_mart_bag:latest") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

def first_day_of_last_month(any_day):
    """
        获取前一个月中的第一天
        :param any_day: 任意日期
        :return: string
    """
    any_day = datetime.datetime.strptime(any_day, '%Y-%m-%d')
    last_month = any_day.replace(day=1) + datetime.timedelta(days=-1)
    return (last_month - datetime.timedelta(days=last_month.day-1)).strftime("%Y-%m-%d")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--execute_dt', type=str, default='', help='date of script execution')

    args = parser.parse_args()
    execute_dt = args.execute_dt
    start_dt = first_day_of_last_month(execute_dt)
    print(LOG_PREFIX, 'execute_dt=%s,start_dt=%s' % (execute_dt, start_dt))

    brand_c3d_pe_df = spark.sql("""
	SELECT
		item_third_cate_cd,
		brand_cd,
		MEAN(theta) AS theta
	FROM
		adm.adm_sku_price_elasticity_month
	WHERE
		dt = '{start_dt}'
	GROUP BY
		item_third_cate_cd,
		brand_cd    
    """.format(start_dt=start_dt))

    brand_c3d_pe_df.registerTempTable('brand_c3d_pe')

    spark.sql("""
    INSERT OVERWRITE TABLE adm.adm_brand_c3d_price_elasticity_month PARTITION(dt='{dt}')
        SELECT
            item_third_cate_cd,brand_cd,theta
        FROM brand_c3d_pe
    """.format(dt=start_dt))

    print(LOG_PREFIX, '写adm.adm_brand_c3d_price_elasticity_month表完成')


    c3d_pe_df = spark.sql("""
	SELECT
		item_third_cate_cd,
		MEAN(theta) AS theta
	FROM
		adm.adm_sku_price_elasticity_month
	WHERE
		dt = '{start_dt}'
	GROUP BY
		item_third_cate_cd
    """.format(start_dt=start_dt))

    c3d_pe_df.registerTempTable('c3d_pe')

    spark.sql("""
    INSERT OVERWRITE TABLE adm.adm_c3d_price_elasticity_month PARTITION(dt='{dt}')
        SELECT
            item_third_cate_cd,theta
        FROM c3d_pe
    """.format(dt=start_dt))

    print(LOG_PREFIX, '写adm.adm_c3d_price_elasticity_month表完成')