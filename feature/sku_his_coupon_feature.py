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

"""
CREATE EXTERNAL TABLE IF NOT EXISTS adm.adm_sku_cps_his_feature(
item_sku_id string COMMENT '商品SKU编号',
his_cps_cnt bigint COMMENT '历史券数量',
his_hour_coupon_cnt bigint COMMENT '历史小时券数量',
his_shufang_cnt bigint COMMENT '历史数坊券数量',
his_shangxiang_cnt bigint COMMENT '历史商详券数量',
his_new_crowd_cnt bigint COMMENT '历史拉新券数量',
his_second_crowd_cnt bigint COMMENT '历史一转二券数量',
his_old_crowd_cnt bigint COMMENT '历史复购券数量',
his_mean_gmv double COMMENT '历史券平均GMV',
his_max_gmv double COMMENT '历史券最大GMV',
his_min_gmv double COMMENT '历史券最小GMV',
his_std_gmv double COMMENT '历史券GMV标准差',
his_mean_sale_qtty double COMMENT '历史券平均销量',
his_max_sale_qtty double COMMENT '历史券最大销量',
his_min_sale_qtty double COMMENT '历史券最小销量',
his_std_sale_qtty double COMMENT '历史券销量标准差',
his_mean_user_cnt double COMMENT '历史券平均用券人数',
his_max_user_cnt double COMMENT '历史券最大用券人数',
his_min_user_cnt double COMMENT '历史券最小用券人数',
his_std_user_cnt double COMMENT '历史券用券人数标准差',
his_mean_consume_lim double COMMENT '历史券平均限额',
his_max_consume_lim double COMMENT '历史券最大限额',
his_min_consume_lim double COMMENT '历史券最小限额',
his_std_consume_lim double COMMENT '历史券限额标准差',
his_mean_cps_face_value double COMMENT '历史券平均面额',
his_max_cps_face_value double COMMENT '历史券最大面额',
his_min_cps_face_value double COMMENT '历史券最小面额',
his_std_cps_face_value double COMMENT '历史券面额标准差',
his_mean_discount_rt double COMMENT '历史券平均折扣率',
his_max_discount_rt double COMMENT '历史券最大折扣率',
his_min_discount_rt double COMMENT '历史券最小折扣率',
his_std_discount_rt double COMMENT '历史券折扣率标准差',
his_mean_batch_cps_total_qtty double COMMENT '历史券平均发放数量',
his_max_batch_cps_total_qtty double COMMENT '历史券最大发放数量',
his_min_batch_cps_total_qtty double COMMENT '历史券最小发放数量',
his_std_batch_cps_total_qtty double COMMENT '历史券发放数量标准差',
his_10p_consume_lim double COMMENT '历史券限额10%分位点',
his_20p_consume_lim double COMMENT '历史券限额20%分位点',
his_30p_consume_lim double COMMENT '历史券限额30%分位点',
his_40p_consume_lim double COMMENT '历史券限额40%分位点',
his_50p_consume_lim double COMMENT '历史券限额50%分位点',
his_60p_consume_lim double COMMENT '历史券限额60%分位点',
his_70p_consume_lim double COMMENT '历史券限额70%分位点',
his_80p_consume_lim double COMMENT '历史券限额80%分位点',
his_90p_consume_lim double COMMENT '历史券限额90%分位点',
his_10p_cps_face_value double COMMENT '历史券面额10%分位点',
his_20p_cps_face_value double COMMENT '历史券面额20%分位点',
his_30p_cps_face_value double COMMENT '历史券面额30%分位点',
his_40p_cps_face_value double COMMENT '历史券面额40%分位点',
his_50p_cps_face_value double COMMENT '历史券面额50%分位点',
his_60p_cps_face_value double COMMENT '历史券面额60%分位点',
his_70p_cps_face_value double COMMENT '历史券面额70%分位点',
his_80p_cps_face_value double COMMENT '历史券面额80%分位点',
his_90p_cps_face_value double COMMENT '历史券面额90%分位点',
his_10p_discount_rt double COMMENT '历史券折扣率10%分位点',
his_20p_discount_rt double COMMENT '历史券折扣率20%分位点',
his_30p_discount_rt double COMMENT '历史券折扣率30%分位点',
his_40p_discount_rt double COMMENT '历史券折扣率40%分位点',
his_50p_discount_rt double COMMENT '历史券折扣率50%分位点',
his_60p_discount_rt double COMMENT '历史券折扣率60%分位点',
his_70p_discount_rt double COMMENT '历史券折扣率70%分位点',
his_80p_discount_rt double COMMENT '历史券折扣率80%分位点',
his_90p_discount_rt double COMMENT '历史券折扣率90%分位点',
his_sx_mean_gmv double COMMENT '历史商详券平均GMV',
his_sx_max_gmv double COMMENT '历史商详券最大GMV',
his_sx_min_gmv double COMMENT '历史商详券最小GMV',
his_sx_std_gmv double COMMENT '历史商详券GMV标准差',
his_sx_mean_sale_qtty double COMMENT '历史商详券平均销量',
his_sx_max_sale_qtty double COMMENT '历史商详券最大销量',
his_sx_min_sale_qtty double COMMENT '历史商详券最小销量',
his_sx_std_sale_qtty double COMMENT '历史商详券销量标准差',
his_sx_mean_user_cnt double COMMENT '历史商详券平均用券人数',
his_sx_max_user_cnt double COMMENT '历史商详券最大用券人数',
his_sx_min_user_cnt double COMMENT '历史商详券最小用券人数',
his_sx_std_user_cnt double COMMENT '历史商详券用券人数标准差',
his_sx_mean_consume_lim double COMMENT '历史商详券平均限额',
his_sx_max_consume_lim double COMMENT '历史商详券最大限额',
his_sx_min_consume_lim double COMMENT '历史商详券最小限额',
his_sx_std_consume_lim double COMMENT '历史商详券限额标准差',
his_sx_mean_cps_face_value double COMMENT '历史商详券平均面额',
his_sx_max_cps_face_value double COMMENT '历史商详券最大面额',
his_sx_min_cps_face_value double COMMENT '历史商详券最小面额',
his_sx_std_cps_face_value double COMMENT '历史商详券面额标准差',
his_sx_mean_discount_rt double COMMENT '历史商详券平均折扣率',
his_sx_max_discount_rt double COMMENT '历史商详券最大折扣率',
his_sx_min_discount_rt double COMMENT '历史商详券最小折扣率',
his_sx_std_discount_rt double COMMENT '历史商详券折扣率标准差',
his_sx_mean_batch_cps_total_qtty double COMMENT '历史商详券平均发放数量',
his_sx_max_batch_cps_total_qtty double COMMENT '历史商详券最大发放数量',
his_sx_min_batch_cps_total_qtty double COMMENT '历史商详券最小发放数量',
his_sx_std_batch_cps_total_qtty double COMMENT '历史商详券发放数量标准差',
his_sx_10p_consume_lim double COMMENT '历史商详券限额10%分位点',
his_sx_20p_consume_lim double COMMENT '历史商详券限额20%分位点',
his_sx_30p_consume_lim double COMMENT '历史商详券限额30%分位点',
his_sx_40p_consume_lim double COMMENT '历史商详券限额40%分位点',
his_sx_50p_consume_lim double COMMENT '历史商详券限额50%分位点',
his_sx_60p_consume_lim double COMMENT '历史商详券限额60%分位点',
his_sx_70p_consume_lim double COMMENT '历史商详券限额70%分位点',
his_sx_80p_consume_lim double COMMENT '历史商详券限额80%分位点',
his_sx_90p_consume_lim double COMMENT '历史商详券限额90%分位点',
his_sx_10p_cps_face_value double COMMENT '历史商详券面额10%分位点',
his_sx_20p_cps_face_value double COMMENT '历史商详券面额20%分位点',
his_sx_30p_cps_face_value double COMMENT '历史商详券面额30%分位点',
his_sx_40p_cps_face_value double COMMENT '历史商详券面额40%分位点',
his_sx_50p_cps_face_value double COMMENT '历史商详券面额50%分位点',
his_sx_60p_cps_face_value double COMMENT '历史商详券面额60%分位点',
his_sx_70p_cps_face_value double COMMENT '历史商详券面额70%分位点',
his_sx_80p_cps_face_value double COMMENT '历史商详券面额80%分位点',
his_sx_90p_cps_face_value double COMMENT '历史商详券面额90%分位点',
his_sx_10p_discount_rt double COMMENT '历史商详券折扣率10%分位点',
his_sx_20p_discount_rt double COMMENT '历史商详券折扣率20%分位点',
his_sx_30p_discount_rt double COMMENT '历史商详券折扣率30%分位点',
his_sx_40p_discount_rt double COMMENT '历史商详券折扣率40%分位点',
his_sx_50p_discount_rt double COMMENT '历史商详券折扣率50%分位点',
his_sx_60p_discount_rt double COMMENT '历史商详券折扣率60%分位点',
his_sx_70p_discount_rt double COMMENT '历史商详券折扣率70%分位点',
his_sx_80p_discount_rt double COMMENT '历史商详券折扣率80%分位点',
his_sx_90p_discount_rt double COMMENT '历史商详券折扣率90%分位点')
COMMENT 'sku历史券特征表'
PARTITIONED BY (
  dt string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns1/user/dd_edw/adm.db/adm_sku_cps_his_feature';
"""

LOG_PREFIX = "[■■□□ ■□■■ □■■□]"

spark = SparkSession \
    .builder \
    .appName("sku_his_coupon_feature") \
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


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--execute_dt', type=str, default='', help='date of script execution')

    args = parser.parse_args()
    execute_dt = args.execute_dt
    print(LOG_PREFIX, 'execute_dt=%s' % execute_dt)

    sku_his_coupon_df = spark.sql("""
    SELECT
		t1.item_sku_id,
		t1.batch_id,
		cps_name,
		cps_cate_cd,
		coupon_style,
		consume_lim,
		cps_face_value,
		CASE
			WHEN coupon_style = 0
			THEN cps_face_value / GREATEST(jd_prc, consume_lim)
			ELSE cps_face_value / consume_lim
		END AS discount_rt,
		gmv,
		sale_qtty,
		user_cnt,
		batch_cps_total_qtty,
		open_flag,
		hour_coupon_flag,
		COALESCE(shufang_flag, 0) AS shufang_flag,
		COALESCE(crowd_type, 0) AS crowd_type,
		IF(open_flag = 1
		AND ARRAY_CONTAINS(open_type_cd_list, '1'), 1, 0) AS shangxiang_flag
	FROM
		(
			SELECT
				sku_id AS item_sku_id,
				batch_id,
				cps_name,
				cps_cate_cd,
				coupon_style,
				consume_lim,
				cps_face_value,
				activity_id,
				activity_key,
				batch_cps_total_qtty,
				IF(hour_coupon_flag = '2', 1, 0) AS hour_coupon_flag,
				SUM(after_prefr_amount_1) AS gmv,
				SUM(sale_qtty) AS sale_qtty,
				COUNT(DISTINCT user_log_acct) AS user_cnt,
				MAX(SUBSTR(cps_valid_end_tm, 0, 10)) AS end_dt
			FROM
				adm.adm_d07_cps_batch_ord_sku_det
			WHERE
				dt >= date_sub('{dt}', 365)
				AND dt <= '{dt}'
				AND cps_state_cd = 3
				AND cps_type_cd = 1
				AND cps_cate_cd = 1
				AND allow_overlap = 1
				AND coupon_style IN(0, 3, 28)
				AND after_prefr_amount_1 > 0.1
				AND cps_name NOT LIKE CONCAT('%', '运费券', '%')
				AND cps_name NOT LIKE CONCAT('%', '福利券', '%')
				AND cps_name NOT LIKE CONCAT('福利券', '%')
				AND cps_name NOT LIKE CONCAT('%', '赔付', '%')
				AND cps_name NOT LIKE CONCAT('员工申请', '%')
				AND cps_name NOT LIKE CONCAT('申请京券', '%')
				AND cps_name NOT LIKE CONCAT('%', '京贴', '%')
				AND cps_name != '咚咚砍价优惠券'
				AND activity_applicant_erp_acct NOT LIKE 'pop-%' --去除pop优惠券中大面额的券
				AND consume_lim > 0
				AND valid_flag = 1
			GROUP BY
				item_sku_id,
				batch_id,
				cps_name,
				cps_cate_cd,
				coupon_style,
				consume_lim,
				cps_face_value,
				activity_id,
				activity_key,
				batch_cps_total_qtty,
				hour_coupon_flag
			HAVING
				end_dt <= '{dt}'
		)
		t1
	LEFT JOIN
		(
			SELECT
				item_sku_id,
				jd_prc
			FROM
				adm.adm_coupon_recommend_with_sku_dimension_state_attribute
			WHERE
				dt = '{dt}'
		)
		t2
	ON
		t1.item_sku_id = t2.item_sku_id
	LEFT JOIN
		(
			SELECT
			    activity_id, --活动编号
				activity_key, --批次活动key
				open_flag, --公开标志 0-不开启 1-开启
				SPLIT(open_type_cd_scope, ',') AS open_type_cd_list
				--公开类型代码范围 1-公开到单品页 2-公开到领券中心 3-公开到乡村电商推广 4-公开到联盟 5-公开到中台券池（此标示的优惠券可以用于中台券池投放
				--6-全网推广（此标示的优惠券可以用于所有投放场景）8-数坊智能投放（首购礼金模板）999-未知
			FROM
				gdm.gdm_m07_cps_active_coupon_da
			WHERE
				dt = '{dt}'
				AND plat_type_cd_scope = - 1 --平台类型代码范围 -1-不限
				--AND user_lv_cd IN(10000, 20000, 30000, 92000)
		)
		t3
	ON
		t1.activity_key = t3.activity_key
	LEFT JOIN
		(
			SELECT
				DISTINCT(equity_id) AS batch_id,
				1 AS shufang_flag
			FROM
				app.app_leo_growth_strategy_plan_task_equity_channel_info lateral VIEW explode(equity_ids) new_table AS equity_id
			WHERE
				dt <= '{dt}'
				AND dt >= date_sub('{dt}', 365)
				AND strategy_version = '内部版'
				AND business_type = 'couponNew'
		)
		t4
	ON
		t1.batch_id = t4.batch_id
	LEFT JOIN
		(
			SELECT
				role_id,
				CASE
					WHEN channel_type = '4200'
					THEN 4200 --数坊身份
					WHEN channel_type IN('2005', '2045', '2006', '2008', '2009', '2007', '2080')
					THEN 1 --拉新
					WHEN channel_type IN('2050', '2070')
					THEN 2 --一转二
				END AS crowd_type
			FROM
				fdm.fdm_active_coupon_coupon_active_rule_ext_chain
			WHERE
				dp = 'ACTIVE'
				AND channel_type IN('4200', '2005', '2006', '2007', '2008', '2009', '2007', '2045', '2050', '2070', '2060', '2080')
			GROUP BY
				role_id,
				channel_type
		)
		t5
	ON
		t3.activity_id = t5.role_id
    """.format(dt=execute_dt))

    udf_crowd = F.udf(crowd_type, IntegerType())
    sku_his_coupon_df = sku_his_coupon_df.withColumn("crowd_type", udf_crowd('cps_name', 'crowd_type')).fillna(
        0).distinct().cache()

    his_df = sku_his_coupon_df.groupBy('item_sku_id').agg(
        F.count('batch_id').alias('his_cps_cnt'),
        F.sum('hour_coupon_flag').alias('his_hour_coupon_cnt'),
        F.sum('shufang_flag').alias('his_shufang_cnt'),
        F.sum('shangxiang_flag').alias('his_shangxiang_cnt'),
        F.sum(F.when(F.col('crowd_type') == F.lit(1), 1).otherwise(0)).alias('his_new_crowd_cnt'),
        F.sum(F.when(F.col('crowd_type') == F.lit(2), 1).otherwise(0)).alias('his_second_crowd_cnt'),
        F.sum(F.when(F.col('crowd_type') == F.lit(3), 1).otherwise(0)).alias('his_old_crowd_cnt')) \
        .join(generate_sku_cps_feature(sku_his_coupon_df, 'his'), ['item_sku_id'], 'inner') \
        .join(generate_sku_cps_feature(sku_his_coupon_df.where('shangxiang_flag=1'), 'his_sx'), ['item_sku_id'],
              'inner')

    his_df.registerTempTable('sku_his_coupon')

    spark.sql("""
        INSERT OVERWRITE TABLE adm.adm_sku_cps_his_feature PARTITION(dt='{dt}')
            SELECT
                item_sku_id,his_cps_cnt,his_hour_coupon_cnt,his_shufang_cnt,his_shangxiang_cnt,his_new_crowd_cnt,
                his_second_crowd_cnt,his_old_crowd_cnt,his_mean_gmv,his_max_gmv,his_min_gmv,his_std_gmv,
                his_mean_sale_qtty,his_max_sale_qtty,his_min_sale_qtty,his_std_sale_qtty,his_mean_user_cnt,
                his_max_user_cnt,his_min_user_cnt,his_std_user_cnt,his_mean_consume_lim,his_max_consume_lim,
                his_min_consume_lim,his_std_consume_lim,his_mean_cps_face_value,his_max_cps_face_value,
                his_min_cps_face_value,his_std_cps_face_value,his_mean_discount_rt,his_max_discount_rt,
                his_min_discount_rt,his_std_discount_rt,his_mean_batch_cps_total_qtty,his_max_batch_cps_total_qtty,
                his_min_batch_cps_total_qtty,his_std_batch_cps_total_qtty,his_10p_consume_lim,his_20p_consume_lim,
                his_30p_consume_lim,his_40p_consume_lim,his_50p_consume_lim,his_60p_consume_lim,his_70p_consume_lim,
                his_80p_consume_lim,his_90p_consume_lim,his_10p_cps_face_value,his_20p_cps_face_value,
                his_30p_cps_face_value,his_40p_cps_face_value,his_50p_cps_face_value,his_60p_cps_face_value,
                his_70p_cps_face_value,his_80p_cps_face_value,his_90p_cps_face_value,his_10p_discount_rt,
                his_20p_discount_rt,his_30p_discount_rt,his_40p_discount_rt,his_50p_discount_rt,his_60p_discount_rt,
                his_70p_discount_rt,his_80p_discount_rt,his_90p_discount_rt,his_sx_mean_gmv,his_sx_max_gmv,
                his_sx_min_gmv,his_sx_std_gmv,his_sx_mean_sale_qtty,his_sx_max_sale_qtty,his_sx_min_sale_qtty,
                his_sx_std_sale_qtty,his_sx_mean_user_cnt,his_sx_max_user_cnt,his_sx_min_user_cnt,
                his_sx_std_user_cnt,his_sx_mean_consume_lim,his_sx_max_consume_lim,his_sx_min_consume_lim,
                his_sx_std_consume_lim,his_sx_mean_cps_face_value,his_sx_max_cps_face_value,
                his_sx_min_cps_face_value,his_sx_std_cps_face_value,his_sx_mean_discount_rt,his_sx_max_discount_rt,
                his_sx_min_discount_rt,his_sx_std_discount_rt,his_sx_mean_batch_cps_total_qtty,
                his_sx_max_batch_cps_total_qtty,his_sx_min_batch_cps_total_qtty,his_sx_std_batch_cps_total_qtty,
                his_sx_10p_consume_lim,his_sx_20p_consume_lim,his_sx_30p_consume_lim,his_sx_40p_consume_lim,
                his_sx_50p_consume_lim,his_sx_60p_consume_lim,his_sx_70p_consume_lim,his_sx_80p_consume_lim,
                his_sx_90p_consume_lim,his_sx_10p_cps_face_value,his_sx_20p_cps_face_value,
                his_sx_30p_cps_face_value,his_sx_40p_cps_face_value,his_sx_50p_cps_face_value,
                his_sx_60p_cps_face_value,his_sx_70p_cps_face_value,his_sx_80p_cps_face_value,
                his_sx_90p_cps_face_value,his_sx_10p_discount_rt,his_sx_20p_discount_rt,his_sx_30p_discount_rt,
                his_sx_40p_discount_rt,his_sx_50p_discount_rt,his_sx_60p_discount_rt,his_sx_70p_discount_rt,
                his_sx_80p_discount_rt,his_sx_90p_discount_rt
            FROM sku_his_coupon
        """.format(dt=execute_dt))

    print(LOG_PREFIX, '写adm_sku_cps_his_feature表完成')
