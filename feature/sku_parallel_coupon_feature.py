#!/usr/bin/env python3
# coding: utf-8

import argparse
import os

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *

os.environ['PYSPARK_PYTHON'] = "/usr/local/anaconda3/bin/python3.6"
os.environ['PYSPARK_DRIVER_PYTHON'] = "/usr/local/anaconda3/bin/python3.6"

"""
CREATE EXTERNAL TABLE IF NOT EXISTS adm.adm_sku_cps_pal_feature(
item_sku_id string COMMENT '商品SKU编号',
pal_cps_cnt bigint COMMENT '平行券数量',
pal_hour_coupon_cnt bigint COMMENT '平行小时券数量',
pal_shufang_cnt bigint COMMENT '平行数坊券数量',
pal_shangxiang_cnt bigint COMMENT '平行商详券数量',
pal_new_crowd_cnt bigint COMMENT '平行拉新券数量',
pal_second_crowd_cnt bigint COMMENT '平行一转二券数量',
pal_old_crowd_cnt bigint COMMENT '平行复购券数量',
pal_mean_gmv double COMMENT '平行券平均GMV',
pal_max_gmv double COMMENT '平行券最大GMV',
pal_min_gmv double COMMENT '平行券最小GMV',
pal_std_gmv double COMMENT '平行券GMV标准差',
pal_mean_sale_qtty double COMMENT '平行券平均销量',
pal_max_sale_qtty double COMMENT '平行券最大销量',
pal_min_sale_qtty double COMMENT '平行券最小销量',
pal_std_sale_qtty double COMMENT '平行券销量标准差',
pal_mean_user_cnt double COMMENT '平行券平均用券人数',
pal_max_user_cnt double COMMENT '平行券最大用券人数',
pal_min_user_cnt double COMMENT '平行券最小用券人数',
pal_std_user_cnt double COMMENT '平行券用券人数标准差',
pal_mean_consume_lim double COMMENT '平行券平均限额',
pal_max_consume_lim double COMMENT '平行券最大限额',
pal_min_consume_lim double COMMENT '平行券最小限额',
pal_std_consume_lim double COMMENT '平行券限额标准差',
pal_mean_cps_face_value double COMMENT '平行券平均面额',
pal_max_cps_face_value double COMMENT '平行券最大面额',
pal_min_cps_face_value double COMMENT '平行券最小面额',
pal_std_cps_face_value double COMMENT '平行券面额标准差',
pal_mean_discount_rt double COMMENT '平行券平均折扣率',
pal_max_discount_rt double COMMENT '平行券最大折扣率',
pal_min_discount_rt double COMMENT '平行券最小折扣率',
pal_std_discount_rt double COMMENT '平行券折扣率标准差',
pal_mean_batch_cps_total_qtty double COMMENT '平行券平均发放数量',
pal_max_batch_cps_total_qtty double COMMENT '平行券最大发放数量',
pal_min_batch_cps_total_qtty double COMMENT '平行券最小发放数量',
pal_std_batch_cps_total_qtty double COMMENT '平行券发放数量标准差',
pal_10p_consume_lim double COMMENT '平行券限额10%分位点',
pal_20p_consume_lim double COMMENT '平行券限额20%分位点',
pal_30p_consume_lim double COMMENT '平行券限额30%分位点',
pal_40p_consume_lim double COMMENT '平行券限额40%分位点',
pal_50p_consume_lim double COMMENT '平行券限额50%分位点',
pal_60p_consume_lim double COMMENT '平行券限额60%分位点',
pal_70p_consume_lim double COMMENT '平行券限额70%分位点',
pal_80p_consume_lim double COMMENT '平行券限额80%分位点',
pal_90p_consume_lim double COMMENT '平行券限额90%分位点',
pal_10p_cps_face_value double COMMENT '平行券面额10%分位点',
pal_20p_cps_face_value double COMMENT '平行券面额20%分位点',
pal_30p_cps_face_value double COMMENT '平行券面额30%分位点',
pal_40p_cps_face_value double COMMENT '平行券面额40%分位点',
pal_50p_cps_face_value double COMMENT '平行券面额50%分位点',
pal_60p_cps_face_value double COMMENT '平行券面额60%分位点',
pal_70p_cps_face_value double COMMENT '平行券面额70%分位点',
pal_80p_cps_face_value double COMMENT '平行券面额80%分位点',
pal_90p_cps_face_value double COMMENT '平行券面额90%分位点',
pal_10p_discount_rt double COMMENT '平行券折扣率10%分位点',
pal_20p_discount_rt double COMMENT '平行券折扣率20%分位点',
pal_30p_discount_rt double COMMENT '平行券折扣率30%分位点',
pal_40p_discount_rt double COMMENT '平行券折扣率40%分位点',
pal_50p_discount_rt double COMMENT '平行券折扣率50%分位点',
pal_60p_discount_rt double COMMENT '平行券折扣率60%分位点',
pal_70p_discount_rt double COMMENT '平行券折扣率70%分位点',
pal_80p_discount_rt double COMMENT '平行券折扣率80%分位点',
pal_90p_discount_rt double COMMENT '平行券折扣率90%分位点',
pal_sx_mean_gmv double COMMENT '平行商详券平均GMV',
pal_sx_max_gmv double COMMENT '平行商详券最大GMV',
pal_sx_min_gmv double COMMENT '平行商详券最小GMV',
pal_sx_std_gmv double COMMENT '平行商详券GMV标准差',
pal_sx_mean_sale_qtty double COMMENT '平行商详券平均销量',
pal_sx_max_sale_qtty double COMMENT '平行商详券最大销量',
pal_sx_min_sale_qtty double COMMENT '平行商详券最小销量',
pal_sx_std_sale_qtty double COMMENT '平行商详券销量标准差',
pal_sx_mean_user_cnt double COMMENT '平行商详券平均用券人数',
pal_sx_max_user_cnt double COMMENT '平行商详券最大用券人数',
pal_sx_min_user_cnt double COMMENT '平行商详券最小用券人数',
pal_sx_std_user_cnt double COMMENT '平行商详券用券人数标准差',
pal_sx_mean_consume_lim double COMMENT '平行商详券平均限额',
pal_sx_max_consume_lim double COMMENT '平行商详券最大限额',
pal_sx_min_consume_lim double COMMENT '平行商详券最小限额',
pal_sx_std_consume_lim double COMMENT '平行商详券限额标准差',
pal_sx_mean_cps_face_value double COMMENT '平行商详券平均面额',
pal_sx_max_cps_face_value double COMMENT '平行商详券最大面额',
pal_sx_min_cps_face_value double COMMENT '平行商详券最小面额',
pal_sx_std_cps_face_value double COMMENT '平行商详券面额标准差',
pal_sx_mean_discount_rt double COMMENT '平行商详券平均折扣率',
pal_sx_max_discount_rt double COMMENT '平行商详券最大折扣率',
pal_sx_min_discount_rt double COMMENT '平行商详券最小折扣率',
pal_sx_std_discount_rt double COMMENT '平行商详券折扣率标准差',
pal_sx_mean_batch_cps_total_qtty double COMMENT '平行商详券平均发放数量',
pal_sx_max_batch_cps_total_qtty double COMMENT '平行商详券最大发放数量',
pal_sx_min_batch_cps_total_qtty double COMMENT '平行商详券最小发放数量',
pal_sx_std_batch_cps_total_qtty double COMMENT '平行商详券发放数量标准差',
pal_sx_10p_consume_lim double COMMENT '平行商详券限额10%分位点',
pal_sx_20p_consume_lim double COMMENT '平行商详券限额20%分位点',
pal_sx_30p_consume_lim double COMMENT '平行商详券限额30%分位点',
pal_sx_40p_consume_lim double COMMENT '平行商详券限额40%分位点',
pal_sx_50p_consume_lim double COMMENT '平行商详券限额50%分位点',
pal_sx_60p_consume_lim double COMMENT '平行商详券限额60%分位点',
pal_sx_70p_consume_lim double COMMENT '平行商详券限额70%分位点',
pal_sx_80p_consume_lim double COMMENT '平行商详券限额80%分位点',
pal_sx_90p_consume_lim double COMMENT '平行商详券限额90%分位点',
pal_sx_10p_cps_face_value double COMMENT '平行商详券面额10%分位点',
pal_sx_20p_cps_face_value double COMMENT '平行商详券面额20%分位点',
pal_sx_30p_cps_face_value double COMMENT '平行商详券面额30%分位点',
pal_sx_40p_cps_face_value double COMMENT '平行商详券面额40%分位点',
pal_sx_50p_cps_face_value double COMMENT '平行商详券面额50%分位点',
pal_sx_60p_cps_face_value double COMMENT '平行商详券面额60%分位点',
pal_sx_70p_cps_face_value double COMMENT '平行商详券面额70%分位点',
pal_sx_80p_cps_face_value double COMMENT '平行商详券面额80%分位点',
pal_sx_90p_cps_face_value double COMMENT '平行商详券面额90%分位点',
pal_sx_10p_discount_rt double COMMENT '平行商详券折扣率10%分位点',
pal_sx_20p_discount_rt double COMMENT '平行商详券折扣率20%分位点',
pal_sx_30p_discount_rt double COMMENT '平行商详券折扣率30%分位点',
pal_sx_40p_discount_rt double COMMENT '平行商详券折扣率40%分位点',
pal_sx_50p_discount_rt double COMMENT '平行商详券折扣率50%分位点',
pal_sx_60p_discount_rt double COMMENT '平行商详券折扣率60%分位点',
pal_sx_70p_discount_rt double COMMENT '平行商详券折扣率70%分位点',
pal_sx_80p_discount_rt double COMMENT '平行商详券折扣率80%分位点',
pal_sx_90p_discount_rt double COMMENT '平行商详券折扣率90%分位点')
COMMENT 'sku平行券特征表'
PARTITIONED BY (
  dt string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns1/user/dd_edw/adm.db/adm_sku_cps_pal_feature';
"""

LOG_PREFIX = "[■■□□ ■□■■ □■■□]"

spark = SparkSession \
    .builder \
    .appName("sku_parallel_coupon_feature") \
    .enableHiveSupport() \
    .config("spark.executor.instances", "400") \
    .config("spark.executor.memory", "20g") \
    .config("spark.executor.cores", "4") \
    .config("spark.driver.memory", "20g") \
    .config("spark.driver.maxResultSize", "20g") \
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


def crowd_type(cps_name, crowd_type):
    """
        判断券的人群类型，0:高潜，1:拉新，2:一转二，3:复购
        :param str cps_name: 优惠券名称
        :param int crowd_type: 人群类型
        :return: int
    """
    new_crowd = ['拉新', '新用户', '新客', '全站新', '品类新', '首购', '品牌新', '站外新', '站内新', '招新', '平台新', '新人', '开卡', '经典新', '首单']
    old_crowd = ['复购', '老客', '召回', '老用户']
    second_crowd = ['一转二', '二单']
    try:
        # 如果已知是数坊身份券或已知底表人群类型为空时：
        if crowd_type == 4200 or crowd_type == 0:
            for val in new_crowd:  # 拉新
                if val in cps_name:
                    return 1
            for val in old_crowd:  # 复购
                if val in cps_name:
                    return 3
            for val in second_crowd:  # 一转二
                if val in cps_name:
                    return 2
            return 0
    except:
        return 0


def generate_sku_cps_feature(df, suffix):
    """
        判断券的人群类型，0:高潜，1:拉新，2:一转二，3:复购
        :param df: dataframe
        :param str suffix: 人群类型
        :return: DataFrame
    """
    return df.groupBy('item_sku_id') \
        .agg(
        F.mean('gmv').alias('%s_mean_gmv' % suffix),
        F.max('gmv').alias('%s_max_gmv' % suffix),
        F.min('gmv').alias('%s_min_gmv' % suffix),
        F.stddev('gmv').alias('%s_std_gmv' % suffix),
        F.mean('sale_qtty').alias('%s_mean_sale_qtty' % suffix),
        F.max('sale_qtty').alias('%s_max_sale_qtty' % suffix),
        F.min('sale_qtty').alias('%s_min_sale_qtty' % suffix),
        F.stddev('sale_qtty').alias('%s_std_sale_qtty' % suffix),
        F.mean('user_cnt').alias('%s_mean_user_cnt' % suffix),
        F.max('user_cnt').alias('%s_max_user_cnt' % suffix),
        F.min('user_cnt').alias('%s_min_user_cnt' % suffix),
        F.stddev('user_cnt').alias('%s_std_user_cnt' % suffix),
        F.mean('consume_lim').alias('%s_mean_consume_lim' % suffix),
        F.max('consume_lim').alias('%s_max_consume_lim' % suffix),
        F.min('consume_lim').alias('%s_min_consume_lim' % suffix),
        F.stddev('consume_lim').alias('%s_std_consume_lim' % suffix),
        F.mean('cps_face_value').alias('%s_mean_cps_face_value' % suffix),
        F.max('cps_face_value').alias('%s_max_cps_face_value' % suffix),
        F.min('cps_face_value').alias('%s_min_cps_face_value' % suffix),
        F.stddev('cps_face_value').alias('%s_std_cps_face_value' % suffix),
        F.mean('discount_rt').alias('%s_mean_discount_rt' % suffix),
        F.max('discount_rt').alias('%s_max_discount_rt' % suffix),
        F.min('discount_rt').alias('%s_min_discount_rt' % suffix),
        F.stddev('discount_rt').alias('%s_std_discount_rt' % suffix),
        F.mean('batch_cps_total_qtty').alias('%s_mean_batch_cps_total_qtty' % suffix),
        F.max('batch_cps_total_qtty').alias('%s_max_batch_cps_total_qtty' % suffix),
        F.min('batch_cps_total_qtty').alias('%s_min_batch_cps_total_qtty' % suffix),
        F.stddev('batch_cps_total_qtty').alias('%s_std_batch_cps_total_qtty' % suffix),
        F.expr('percentile(consume_lim, 0.1, 9999)').alias('%s_10p_consume_lim' % suffix),
        F.expr('percentile(consume_lim, 0.2, 9999)').alias('%s_20p_consume_lim' % suffix),
        F.expr('percentile(consume_lim, 0.3, 9999)').alias('%s_30p_consume_lim' % suffix),
        F.expr('percentile(consume_lim, 0.4, 9999)').alias('%s_40p_consume_lim' % suffix),
        F.expr('percentile(consume_lim, 0.5, 9999)').alias('%s_50p_consume_lim' % suffix),
        F.expr('percentile(consume_lim, 0.6, 9999)').alias('%s_60p_consume_lim' % suffix),
        F.expr('percentile(consume_lim, 0.7, 9999)').alias('%s_70p_consume_lim' % suffix),
        F.expr('percentile(consume_lim, 0.8, 9999)').alias('%s_80p_consume_lim' % suffix),
        F.expr('percentile(consume_lim, 0.9, 9999)').alias('%s_90p_consume_lim' % suffix),
        F.expr('percentile(cps_face_value, 0.1, 9999)').alias('%s_10p_cps_face_value' % suffix),
        F.expr('percentile(cps_face_value, 0.2, 9999)').alias('%s_20p_cps_face_value' % suffix),
        F.expr('percentile(cps_face_value, 0.3, 9999)').alias('%s_30p_cps_face_value' % suffix),
        F.expr('percentile(cps_face_value, 0.4, 9999)').alias('%s_40p_cps_face_value' % suffix),
        F.expr('percentile(cps_face_value, 0.5, 9999)').alias('%s_50p_cps_face_value' % suffix),
        F.expr('percentile(cps_face_value, 0.6, 9999)').alias('%s_60p_cps_face_value' % suffix),
        F.expr('percentile(cps_face_value, 0.7, 9999)').alias('%s_70p_cps_face_value' % suffix),
        F.expr('percentile(cps_face_value, 0.8, 9999)').alias('%s_80p_cps_face_value' % suffix),
        F.expr('percentile(cps_face_value, 0.9, 9999)').alias('%s_90p_cps_face_value' % suffix),
        F.expr('percentile(discount_rt, 0.1, 9999)').alias('%s_10p_discount_rt' % suffix),
        F.expr('percentile(discount_rt, 0.2, 9999)').alias('%s_20p_discount_rt' % suffix),
        F.expr('percentile(discount_rt, 0.3, 9999)').alias('%s_30p_discount_rt' % suffix),
        F.expr('percentile(discount_rt, 0.4, 9999)').alias('%s_40p_discount_rt' % suffix),
        F.expr('percentile(discount_rt, 0.5, 9999)').alias('%s_50p_discount_rt' % suffix),
        F.expr('percentile(discount_rt, 0.6, 9999)').alias('%s_60p_discount_rt' % suffix),
        F.expr('percentile(discount_rt, 0.7, 9999)').alias('%s_70p_discount_rt' % suffix),
        F.expr('percentile(discount_rt, 0.8, 9999)').alias('%s_80p_discount_rt' % suffix),
        F.expr('percentile(discount_rt, 0.9, 9999)').alias('%s_90p_discount_rt' % suffix)

    )


def get_newest_partition(spark, table, dt):
    """
        返回hive表的最近dt分区
        :param spark: spark
        :param str table: 表名
        :param str dt: 当前dt
        :return: str
    """
    return spark.sql('show partitions %s' % table).where("partition < 'dt=%s'" % dt) \
               .orderBy(F.desc('partition')).first()[0][3:]


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--execute_dt', type=str, default='', help='date of script execution')

    args = parser.parse_args()
    execute_dt = args.execute_dt

    newest_dt = get_newest_partition(spark, 'adm.adm_sku_with_bundle_parallel_coupon_indicator_data', execute_dt)
    print(LOG_PREFIX, 'execute_dt=%s, newest_dt=%s' % (execute_dt, newest_dt))

    sku_pal_coupon_df = spark.sql("""
    SELECT
		item_sku_id,
		t1.batch_id,
		consume_lim,
		cps_face_value,
		discount_rt,
		sku_daily_gmv * valid_days AS gmv,
		sku_daily_use_cps_cnt * valid_days AS user_cnt,
		sku_daily_sale * valid_days AS sale_qtty,
		batch_cps_total_qtty,
		COALESCE(hour_coupon_flag, 0) hour_coupon_flag,
		COALESCE(shufang_flag, 0) shufang_flag,
		COALESCE(crowd_type, 0) crowd_type,
		COALESCE(shangxiang_flag, 0) shangxiang_flag
	FROM
		(
			SELECT
				item_sku_id,
				batch_id,
				consume_lim,
				cps_face_value,
				start_tm,
				end_tm,
				DATEDIFF(end_tm, start_tm) + 1 AS valid_days,
				discount_ratio AS discount_rt,
				open_flag AS shangxiang_flag,
				shufang_flag,
				crowd_type,
				sku_daily_use_cps_cnt,
				sku_daily_gmv,
				sku_daily_sale,
				dt
			FROM
				adm.adm_sku_with_bundle_parallel_coupon_indicator_data
			WHERE
				dt = '{newest_dt}'
		)
		t1
	LEFT JOIN
		(
			SELECT
				batch_id,
				batch_cps_total_qtty,
				IF(hour_coupon_flag = '2', 1, 0) AS hour_coupon_flag
			FROM
				gdm.gdm_m07_cps_batch_da
			WHERE
				dt = '{dt}'
		)
		t2
	ON
		t1.batch_id = t2.batch_id
    """.format(dt=execute_dt, newest_dt=newest_dt))

    sku_pal_coupon_df = sku_pal_coupon_df.fillna(0).distinct().cache()

    pal_df = sku_pal_coupon_df.groupBy('item_sku_id').agg(
        F.count('batch_id').alias('pal_cps_cnt'),
        F.sum('hour_coupon_flag').alias('pal_hour_coupon_cnt'),
        F.sum('shufang_flag').alias('pal_shufang_cnt'),
        F.sum('shangxiang_flag').alias('pal_shangxiang_cnt'),
        F.sum(F.when(F.col('crowd_type') == F.lit(1), 1).otherwise(0)).alias('pal_new_crowd_cnt'),
        F.sum(F.when(F.col('crowd_type') == F.lit(2), 1).otherwise(0)).alias('pal_second_crowd_cnt'),
        F.sum(F.when(F.col('crowd_type') == F.lit(3), 1).otherwise(0)).alias('pal_old_crowd_cnt')) \
        .join(generate_sku_cps_feature(sku_pal_coupon_df, 'pal'), ['item_sku_id'], 'inner') \
        .join(generate_sku_cps_feature(sku_pal_coupon_df.where('shangxiang_flag=1'), 'pal_sx'), ['item_sku_id'],
              'inner')

    pal_df.registerTempTable('sku_pal_coupon')

    spark.sql("""
        INSERT OVERWRITE TABLE adm.adm_sku_cps_pal_feature PARTITION(dt='{dt}')
            SELECT
                item_sku_id,pal_cps_cnt,pal_hour_coupon_cnt,pal_shufang_cnt,pal_shangxiang_cnt,pal_new_crowd_cnt,
                pal_second_crowd_cnt,pal_old_crowd_cnt,pal_mean_gmv,pal_max_gmv,pal_min_gmv,pal_std_gmv,
                pal_mean_sale_qtty,pal_max_sale_qtty,pal_min_sale_qtty,pal_std_sale_qtty,pal_mean_user_cnt,
                pal_max_user_cnt,pal_min_user_cnt,pal_std_user_cnt,pal_mean_consume_lim,pal_max_consume_lim,
                pal_min_consume_lim,pal_std_consume_lim,pal_mean_cps_face_value,pal_max_cps_face_value,
                pal_min_cps_face_value,pal_std_cps_face_value,pal_mean_discount_rt,pal_max_discount_rt,
                pal_min_discount_rt,pal_std_discount_rt,pal_mean_batch_cps_total_qtty,pal_max_batch_cps_total_qtty,
                pal_min_batch_cps_total_qtty,pal_std_batch_cps_total_qtty,pal_10p_consume_lim,pal_20p_consume_lim,
                pal_30p_consume_lim,pal_40p_consume_lim,pal_50p_consume_lim,pal_60p_consume_lim,pal_70p_consume_lim,
                pal_80p_consume_lim,pal_90p_consume_lim,pal_10p_cps_face_value,pal_20p_cps_face_value,
                pal_30p_cps_face_value,pal_40p_cps_face_value,pal_50p_cps_face_value,pal_60p_cps_face_value,
                pal_70p_cps_face_value,pal_80p_cps_face_value,pal_90p_cps_face_value,pal_10p_discount_rt,
                pal_20p_discount_rt,pal_30p_discount_rt,pal_40p_discount_rt,pal_50p_discount_rt,
                pal_60p_discount_rt,pal_70p_discount_rt,pal_80p_discount_rt,pal_90p_discount_rt,pal_sx_mean_gmv,
                pal_sx_max_gmv,pal_sx_min_gmv,pal_sx_std_gmv,pal_sx_mean_sale_qtty,pal_sx_max_sale_qtty,
                pal_sx_min_sale_qtty,pal_sx_std_sale_qtty,pal_sx_mean_user_cnt,pal_sx_max_user_cnt,
                pal_sx_min_user_cnt,pal_sx_std_user_cnt,pal_sx_mean_consume_lim,pal_sx_max_consume_lim,
                pal_sx_min_consume_lim,pal_sx_std_consume_lim,pal_sx_mean_cps_face_value,pal_sx_max_cps_face_value,
                pal_sx_min_cps_face_value,pal_sx_std_cps_face_value,pal_sx_mean_discount_rt,pal_sx_max_discount_rt,
                pal_sx_min_discount_rt,pal_sx_std_discount_rt,pal_sx_mean_batch_cps_total_qtty,
                pal_sx_max_batch_cps_total_qtty,pal_sx_min_batch_cps_total_qtty,pal_sx_std_batch_cps_total_qtty,
                pal_sx_10p_consume_lim,pal_sx_20p_consume_lim,pal_sx_30p_consume_lim,pal_sx_40p_consume_lim,
                pal_sx_50p_consume_lim,pal_sx_60p_consume_lim,pal_sx_70p_consume_lim,pal_sx_80p_consume_lim,
                pal_sx_90p_consume_lim,pal_sx_10p_cps_face_value,pal_sx_20p_cps_face_value,
                pal_sx_30p_cps_face_value,pal_sx_40p_cps_face_value,pal_sx_50p_cps_face_value,
                pal_sx_60p_cps_face_value,pal_sx_70p_cps_face_value,pal_sx_80p_cps_face_value,
                pal_sx_90p_cps_face_value,pal_sx_10p_discount_rt,pal_sx_20p_discount_rt,pal_sx_30p_discount_rt,
                pal_sx_40p_discount_rt,pal_sx_50p_discount_rt,pal_sx_60p_discount_rt,pal_sx_70p_discount_rt,
                pal_sx_80p_discount_rt,pal_sx_90p_discount_rt
            FROM sku_pal_coupon
        """.format(dt=execute_dt))

    print(LOG_PREFIX, '写adm_sku_cps_pal_feature表完成')
