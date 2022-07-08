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
CREATE EXTERNAL TABLE IF NOT EXISTS adm.adm_cps_bef_all_pred(
batch_id string COMMENT '批次id',
cvr double COMMENT 'CVR',
use_num bigint COMMENT '用券用户数',
roi double COMMENT 'ROI',
sale_amt double COMMENT 'GMV',
ord_qtty bigint COMMENT '销量',
cvr_pred double COMMENT 'CVR预测',
use_num_pred bigint COMMENT '用券用户数预测',
roi_pred double COMMENT 'ROI预测',
sale_amt_pred double COMMENT 'GMV预测',
ord_qtty_pred bigint COMMENT '销量预测',
flag string COMMENT '券范围：after-投后，before-投前，yhzz-用增')
COMMENT '投前优惠券预测表'
PARTITIONED BY (
  dt string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns9/user/mart_scr/adm.db/adm_cps_bef_all_pred';
"""
"""
CREATE EXTERNAL TABLE adm.adm_before_coupon_evaluate_monitor(
  cvr string COMMENT 'CVR',
  use_num string COMMENT '用券用户数',
  roi string COMMENT 'ROI',
  sale_amt string COMMENT 'GMV',
  ord_qtty string COMMENT '销量',
  qty_score string COMMENT '总分')
COMMENT '优惠券投前模型评估监控表'
PARTITIONED BY (
  dt string COMMENT '日期',
  evaluate_type string COMMENT '评估类型, 0:模型, 1:质量分-高潜, 2:质量分-拉新, 3:质量分-一转二, 4:质量分-复购, 5:质量分-商详, 6:质量分-数坊')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns9/user/mart_scr/adm.db/adm_before_coupon_evaluate_monitor';
"""

LOG_PREFIX = "[■■□□ ■□■■ □■■□]"

spark = SparkSession \
    .builder \
    .appName("coupon_before_model") \
    .enableHiveSupport() \
    .config("spark.executor.instances", "255") \
    .config("spark.executor.memory", "6g") \
    .config("spark.executor.cores", "4") \
    .config("spark.driver.memory", "6g") \
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
spark.sparkContext.addPyFile('../price_elasticity/pyspark-xgboost_sparkxgb.zip')

from sparkxgb import XGBoostRegressor

from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator

sparse_cols = ['month', 'cps_cate_cd', 'coupon_style', 'source_type', 'limit_rule_type', 'limit_organization',
               'user_tag_cd',
               'biz_tag_cd', 'activity_type_cd', 'user_lv_cd', 'open_flag', 'hour_coupon_flag', 'new_user_flag',
               'cate_new_user_flag', 'shop_new_user_flag', 'shufang_flag', 'crowd_type', 'shangxiang_flag',
               'open_type_1_2',
               'open_type_1_3', 'open_type_1_4', 'open_type_1_5', 'open_type_1_6', 'open_type_1_7', 'open_type_1_8',
               'open_type_1_999', 'open_type_0_1', 'open_type_0_999']

dense_cols = ['cps_face_value', 'consume_lim', 'batch_cps_total_qtty', 'valid_days', 'jd_prc', 'theta',
              'sale_qtty_sum_30d',
              'sale_qtty_mean_30d', 'sale_qtty_min_30d', 'sale_qtty_max_30d', 'sale_qtty_std_30d', 'gmv_sum_30d',
              'gmv_mean_30d', 'gmv_min_30d', 'gmv_max_30d', 'gmv_std_30d', 'user_cnt_sum_30d', 'user_cnt_mean_30d',
              'user_cnt_min_30d', 'user_cnt_max_30d', 'user_cnt_std_30d', 'order_cnt_sum_30d', 'order_cnt_mean_30d',
              'order_cnt_min_30d', 'order_cnt_max_30d', 'order_cnt_std_30d', 'before_price_sum_30d',
              'before_price_mean_30d', 'before_price_min_30d', 'before_price_max_30d', 'before_price_std_30d',
              'avg_offer_amt_sum_30d', 'avg_offer_amt_mean_30d', 'avg_offer_amt_min_30d', 'avg_offer_amt_max_30d',
              'avg_offer_amt_std_30d', 'avg_coupon_amt_sum_30d', 'avg_coupon_amt_mean_30d', 'avg_coupon_amt_min_30d',
              'avg_coupon_amt_max_30d', 'avg_coupon_amt_std_30d', 'real_price_sum_30d', 'real_price_mean_30d',
              'real_price_min_30d', 'real_price_max_30d', 'real_price_std_30d', 'max_unit_price_30d',
              'min_unit_price_30d',
              'sale_qtty_lim_30d', 'sku_inventory_rt_30d', 'real_jd_ratio_mean_30d', 'real_before_ratio_mean_30d',
              'offer_discount_ratio_mean_30d', 'coupon_discount_ratio_mean_30d', 'diff_real_ratio_mean_30d',
              'cps_sale_qtty_sum_30d', 'cps_sale_qtty_mean_30d', 'cps_sale_qtty_min_30d', 'cps_sale_qtty_max_30d',
              'cps_sale_qtty_std_30d', 'cps_gmv_sum_30d', 'cps_gmv_mean_30d', 'cps_gmv_min_30d', 'cps_gmv_max_30d',
              'cps_gmv_std_30d', 'cps_user_cnt_sum_30d', 'cps_user_cnt_mean_30d', 'cps_user_cnt_min_30d',
              'cps_user_cnt_max_30d', 'cps_user_cnt_std_30d', 'cps_order_cnt_sum_30d', 'cps_order_cnt_mean_30d',
              'cps_order_cnt_min_30d', 'cps_order_cnt_max_30d', 'cps_order_cnt_std_30d', 'cps_avg_offer_amt_sum_30d',
              'cps_avg_offer_amt_mean_30d', 'cps_avg_offer_amt_min_30d', 'cps_avg_offer_amt_max_30d',
              'cps_avg_offer_amt_std_30d', 'cps_avg_coupon_amt_sum_30d', 'cps_avg_coupon_amt_mean_30d',
              'cps_avg_coupon_amt_min_30d', 'cps_avg_coupon_amt_max_30d', 'cps_avg_coupon_amt_std_30d',
              'cps_real_price_sum_30d', 'cps_real_price_mean_30d', 'cps_real_price_min_30d', 'cps_real_price_max_30d',
              'cps_real_price_std_30d', 'cps_max_unit_price_30d', 'cps_min_unit_price_30d',
              'cps_real_jd_ratio_mean_30d',
              'cps_real_before_ratio_mean_30d', 'cps_offer_discount_ratio_mean_30d',
              'cps_coupon_discount_ratio_mean_30d',
              'cps_diff_real_ratio_mean_30d', 'shelve_days', 'pv_cvr_1d', 'uv_cvr_1d', 'pv_cvr_7d', 'uv_cvr_7d',
              'pv_cvr_30d',
              'uv_cvr_30d', 'sku_quality_score', 'sku_good_comment_rate', 'sku_comment_num', 'sku_follow_1d_cnt',
              'sku_follow_7d_cnt', 'sku_follow_30d_cnt', 'sku_ord_7d_pnum', 'sku_ord_30d_pnum', 'sku_comm_30d_cnt',
              'sku_good_comm_30d_cnt', 'sku_good_comm_ratio', 'sku_exp_1d_cnt', 'sku_exp_3d_cnt', 'sku_exp_ads_cnt',
              'sku_exp_rec_cnt', 'sku_exp_ser_cnt', 'sku_exp_act_cnt', 'sku_ord_unit_price', 'sku_ord_user_price',
              'sku_pv_cvr_7d', 'sku_uv_cvr_7d', 'sku_ctr_3d', 'sku_ctr_ads', 'sku_ctr_rec', 'sku_ctr_ser',
              'sku_ctr_act',
              'sku_prc_quantile', 'sku_ord_quantile', 'sku_view_quantile', 'sku_cart_quantile', 'view_cnt90d',
              'view_cnt60d',
              'view_cnt30d', 'view_cnt14d', 'view_cnt7d', 'view_cnt3d', 'view_cnt1d', 'avg_view_count', 'avg_view_uv',
              'search_click15d', 'search_click_cnt6d', 'search_click_cnt3d', 'cart_cnt90d', 'cart_cnt60d',
              'cart_cnt30d',
              'cart_cnt14d', 'cart_cnt7d', 'cart_cnt3d', 'cart_cnt1d', 'avg_cart_count', 'buy_cnt90d', 'buy_cnt60d',
              'buy_cnt30d', 'buy_cnt14d', 'buy_cnt7d', 'buy_cnt3d', 'buy_cnt1d', 'avg_buy_count', 'gmv_90d', 'gmv_60d',
              'gmv_30d', 'gmv_14d', 'gmv_7d', 'gmv_3d', 'gmv_1d', 'avg_gmv', 'cart_prob_90d', 'cart_prob_60d',
              'cart_prob_30d',
              'cart_prob_14d', 'cart_prob_7d', 'buy_prob_90d', 'buy_prob_60d', 'buy_prob_30d', 'buy_prob_14d',
              'buy_prob_7d',
              'gpm3d', 'gpm2d', 'gpm1d', 'cart_uv_90d', 'cart_uv_60d', 'cart_uv_30d', 'cart_uv_15d', 'cart_uv_7d',
              'buy_uv_90d',
              'buy_uv_60d', 'buy_uv_30d', 'buy_uv_15d', 'buy_uv_7d', 'has_coupon_use_days', 'coupon_use_cnt90d',
              'coupon_use_cnt60d', 'coupon_use_cnt30d', 'coupon_use_cnt15d', 'coupon_use_cnt7d', 'avg_coupon_use_count',
              'avg_coupon_use_uv', 'min_pay_price_30d', 'min_pay_price_15d', 'min_pay_price_7d', 'max_pay_price_30d',
              'max_pay_price_15d', 'max_pay_price_7d', 'current_mid_pay_price', 'coupon_use_prob_90d',
              'coupon_use_prob_60d', 'coupon_use_prob_30d', 'coupon_use_prob_15d', 'coupon_use_prob_7d', 'his_cps_cnt',
              'his_hour_coupon_cnt', 'his_shufang_cnt', 'his_shangxiang_cnt', 'his_new_crowd_cnt',
              'his_second_crowd_cnt',
              'his_old_crowd_cnt', 'his_mean_gmv', 'his_max_gmv', 'his_min_gmv', 'his_std_gmv', 'his_mean_sale_qtty',
              'his_max_sale_qtty', 'his_min_sale_qtty', 'his_std_sale_qtty', 'his_mean_user_cnt', 'his_max_user_cnt',
              'his_min_user_cnt', 'his_std_user_cnt', 'his_mean_consume_lim', 'his_max_consume_lim',
              'his_min_consume_lim',
              'his_std_consume_lim', 'his_mean_cps_face_value', 'his_max_cps_face_value', 'his_min_cps_face_value',
              'his_std_cps_face_value', 'his_mean_discount_rt', 'his_max_discount_rt', 'his_min_discount_rt',
              'his_std_discount_rt', 'his_mean_batch_cps_total_qtty', 'his_max_batch_cps_total_qtty',
              'his_min_batch_cps_total_qtty', 'his_std_batch_cps_total_qtty', 'his_10p_consume_lim',
              'his_20p_consume_lim',
              'his_30p_consume_lim', 'his_40p_consume_lim', 'his_50p_consume_lim', 'his_60p_consume_lim',
              'his_70p_consume_lim', 'his_80p_consume_lim', 'his_90p_consume_lim', 'his_10p_cps_face_value',
              'his_20p_cps_face_value', 'his_30p_cps_face_value', 'his_40p_cps_face_value', 'his_50p_cps_face_value',
              'his_60p_cps_face_value', 'his_70p_cps_face_value', 'his_80p_cps_face_value', 'his_90p_cps_face_value',
              'his_10p_discount_rt', 'his_20p_discount_rt', 'his_30p_discount_rt', 'his_40p_discount_rt',
              'his_50p_discount_rt', 'his_60p_discount_rt', 'his_70p_discount_rt', 'his_80p_discount_rt',
              'his_90p_discount_rt', 'his_sx_mean_gmv', 'his_sx_max_gmv', 'his_sx_min_gmv', 'his_sx_std_gmv',
              'his_sx_mean_sale_qtty', 'his_sx_max_sale_qtty', 'his_sx_min_sale_qtty', 'his_sx_std_sale_qtty',
              'his_sx_mean_user_cnt', 'his_sx_max_user_cnt', 'his_sx_min_user_cnt', 'his_sx_std_user_cnt',
              'his_sx_mean_consume_lim', 'his_sx_max_consume_lim', 'his_sx_min_consume_lim', 'his_sx_std_consume_lim',
              'his_sx_mean_cps_face_value', 'his_sx_max_cps_face_value', 'his_sx_min_cps_face_value',
              'his_sx_std_cps_face_value', 'his_sx_mean_discount_rt', 'his_sx_max_discount_rt',
              'his_sx_min_discount_rt',
              'his_sx_std_discount_rt', 'his_sx_mean_batch_cps_total_qtty', 'his_sx_max_batch_cps_total_qtty',
              'his_sx_min_batch_cps_total_qtty', 'his_sx_std_batch_cps_total_qtty', 'his_sx_10p_consume_lim',
              'his_sx_20p_consume_lim', 'his_sx_30p_consume_lim', 'his_sx_40p_consume_lim', 'his_sx_50p_consume_lim',
              'his_sx_60p_consume_lim', 'his_sx_70p_consume_lim', 'his_sx_80p_consume_lim', 'his_sx_90p_consume_lim',
              'his_sx_10p_cps_face_value', 'his_sx_20p_cps_face_value', 'his_sx_30p_cps_face_value',
              'his_sx_40p_cps_face_value', 'his_sx_50p_cps_face_value', 'his_sx_60p_cps_face_value',
              'his_sx_70p_cps_face_value', 'his_sx_80p_cps_face_value', 'his_sx_90p_cps_face_value',
              'his_sx_10p_discount_rt', 'his_sx_20p_discount_rt', 'his_sx_30p_discount_rt', 'his_sx_40p_discount_rt',
              'his_sx_50p_discount_rt', 'his_sx_60p_discount_rt', 'his_sx_70p_discount_rt', 'his_sx_80p_discount_rt',
              'his_sx_90p_discount_rt', 'his_pos_consume_lim', 'his_pos_cps_face_value', 'his_pos_discount_rt',
              'his_sx_pos_consume_lim', 'his_sx_pos_cps_face_value', 'his_sx_pos_discount_rt', 'pal_cps_cnt',
              'pal_hour_coupon_cnt', 'pal_shufang_cnt', 'pal_shangxiang_cnt', 'pal_new_crowd_cnt',
              'pal_second_crowd_cnt',
              'pal_old_crowd_cnt', 'pal_mean_gmv', 'pal_max_gmv', 'pal_min_gmv', 'pal_std_gmv', 'pal_mean_sale_qtty',
              'pal_max_sale_qtty', 'pal_min_sale_qtty', 'pal_std_sale_qtty', 'pal_mean_user_cnt', 'pal_max_user_cnt',
              'pal_min_user_cnt', 'pal_std_user_cnt', 'pal_mean_consume_lim', 'pal_max_consume_lim',
              'pal_min_consume_lim',
              'pal_std_consume_lim', 'pal_mean_cps_face_value', 'pal_max_cps_face_value', 'pal_min_cps_face_value',
              'pal_std_cps_face_value', 'pal_mean_discount_rt', 'pal_max_discount_rt', 'pal_min_discount_rt',
              'pal_std_discount_rt', 'pal_mean_batch_cps_total_qtty', 'pal_max_batch_cps_total_qtty',
              'pal_min_batch_cps_total_qtty', 'pal_std_batch_cps_total_qtty', 'pal_10p_consume_lim',
              'pal_20p_consume_lim',
              'pal_30p_consume_lim', 'pal_40p_consume_lim', 'pal_50p_consume_lim', 'pal_60p_consume_lim',
              'pal_70p_consume_lim', 'pal_80p_consume_lim', 'pal_90p_consume_lim', 'pal_10p_cps_face_value',
              'pal_20p_cps_face_value', 'pal_30p_cps_face_value', 'pal_40p_cps_face_value', 'pal_50p_cps_face_value',
              'pal_60p_cps_face_value', 'pal_70p_cps_face_value', 'pal_80p_cps_face_value', 'pal_90p_cps_face_value',
              'pal_10p_discount_rt', 'pal_20p_discount_rt', 'pal_30p_discount_rt', 'pal_40p_discount_rt',
              'pal_50p_discount_rt', 'pal_60p_discount_rt', 'pal_70p_discount_rt', 'pal_80p_discount_rt',
              'pal_90p_discount_rt', 'pal_sx_mean_gmv', 'pal_sx_max_gmv', 'pal_sx_min_gmv', 'pal_sx_std_gmv',
              'pal_sx_mean_sale_qtty', 'pal_sx_max_sale_qtty', 'pal_sx_min_sale_qtty', 'pal_sx_std_sale_qtty',
              'pal_sx_mean_user_cnt', 'pal_sx_max_user_cnt', 'pal_sx_min_user_cnt', 'pal_sx_std_user_cnt',
              'pal_sx_mean_consume_lim', 'pal_sx_max_consume_lim', 'pal_sx_min_consume_lim', 'pal_sx_std_consume_lim',
              'pal_sx_mean_cps_face_value', 'pal_sx_max_cps_face_value', 'pal_sx_min_cps_face_value',
              'pal_sx_std_cps_face_value', 'pal_sx_mean_discount_rt', 'pal_sx_max_discount_rt',
              'pal_sx_min_discount_rt',
              'pal_sx_std_discount_rt', 'pal_sx_mean_batch_cps_total_qtty', 'pal_sx_max_batch_cps_total_qtty',
              'pal_sx_min_batch_cps_total_qtty', 'pal_sx_std_batch_cps_total_qtty', 'pal_sx_10p_consume_lim',
              'pal_sx_20p_consume_lim', 'pal_sx_30p_consume_lim', 'pal_sx_40p_consume_lim', 'pal_sx_50p_consume_lim',
              'pal_sx_60p_consume_lim', 'pal_sx_70p_consume_lim', 'pal_sx_80p_consume_lim', 'pal_sx_90p_consume_lim',
              'pal_sx_10p_cps_face_value', 'pal_sx_20p_cps_face_value', 'pal_sx_30p_cps_face_value',
              'pal_sx_40p_cps_face_value', 'pal_sx_50p_cps_face_value', 'pal_sx_60p_cps_face_value',
              'pal_sx_70p_cps_face_value', 'pal_sx_80p_cps_face_value', 'pal_sx_90p_cps_face_value',
              'pal_sx_10p_discount_rt', 'pal_sx_20p_discount_rt', 'pal_sx_30p_discount_rt', 'pal_sx_40p_discount_rt',
              'pal_sx_50p_discount_rt', 'pal_sx_60p_discount_rt', 'pal_sx_70p_discount_rt', 'pal_sx_80p_discount_rt',
              'pal_sx_90p_discount_rt', 'pal_pos_consume_lim', 'pal_pos_cps_face_value', 'pal_pos_discount_rt',
              'pal_sx_pos_consume_lim', 'pal_sx_pos_cps_face_value', 'pal_sx_pos_discount_rt']


def round_4(val):
    """
    round value by 4 decimal
    """
    try:
        return round(val, 4)
    except:
        return 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--execute_dt', type=str, default='', help='date of script execution')

    args = parser.parse_args()
    execute_dt = args.execute_dt
    print(LOG_PREFIX, 'execute_dt=%s' % execute_dt)

    target_cols = ['cvr', 'use_num', 'roi', 'sale_amt', 'ord_qtty']
    indexers = [
        StringIndexer(inputCol=c, outputCol="{0}_indexed".format(c), handleInvalid='keep')
        for c in sparse_cols
    ]

    encoders = [OneHotEncoder(dropLast=False, inputCol=indexer.getOutputCol(),
                              outputCol="{0}_encoded".format(indexer.getOutputCol()))
                for indexer in indexers
                ]

    assembler = VectorAssembler(inputCols=[encoder.getOutputCol() for encoder in encoders] + dense_cols,
                                outputCol="features")

    pipeline_1 = Pipeline(stages=indexers + encoders + [assembler])

    all_feature = spark.sql("""
        SELECT
            *
        FROM
            adm.adm_cps_bef_all_feat
        WHERE
            dt = '{dt}'
        """.format(dt=execute_dt)).fillna(0).fillna('-1')

    model1 = pipeline_1.fit(all_feature)
    all_df = model1.transform(all_feature).cache()

    train_df, test_df = all_df.where("flag = 'after'").randomSplit([8.0, 2.0], 2022)

    result = []

    for target_col in target_cols:

        if target_col != 'cvr':
            train_df = train_df.withColumn(target_col, F.log1p(target_col))
            test_df = test_df.withColumn(target_col, F.log1p(target_col))
            all_df = all_df.withColumn(target_col, F.log1p(target_col))

        estimator = XGBoostRegressor(missing=float(0.0), featuresCol='features', labelCol=target_col,
                                     predictionCol='%s_pred' % target_col, numRound=30, numWorkers=100)

        pipeline_2 = Pipeline(stages=[estimator])

        model2 = pipeline_2.fit(train_df)
        predict_train = model2.transform(train_df)
        predict_test = model2.transform(test_df)
        all_df = model2.transform(all_df)

        rmse_evaluator = RegressionEvaluator(labelCol=target_col, metricName="rmse",
                                             predictionCol='%s_pred' % target_col)
        mae_evaluator = RegressionEvaluator(labelCol=target_col, metricName="mae",
                                            predictionCol='%s_pred' % target_col)

        train_rmse = round_4(rmse_evaluator.evaluate(predict_train))
        test_rmse = round_4(rmse_evaluator.evaluate(predict_test))
        train_mae = mae_evaluator.evaluate(predict_train)
        test_mae = mae_evaluator.evaluate(predict_test)
        train_mean = \
            predict_train.select(F.mean('%s_pred' % target_col).alias('mean_%s_pred' % target_col)).collect()[
                0].asDict()[
                'mean_%s_pred' % target_col]
        test_mean = \
            predict_test.select(F.mean('%s_pred' % target_col).alias('mean_%s_pred' % target_col)).collect()[
                0].asDict()[
                'mean_%s_pred' % target_col]

        train_w_mape = round_4(train_mae / float(train_mean))
        test_w_mape = round_4(test_mae / float(test_mean))

        result.append(test_rmse)
        result.append(test_w_mape)

        print(LOG_PREFIX, target_col, ' train RMSE: ', train_rmse)
        print(LOG_PREFIX, target_col, 'test RMSE: ', test_rmse)
        print(LOG_PREFIX, target_col, ' train W-Mape: ', train_w_mape)
        print(LOG_PREFIX, target_col, 'test W-Mape: ', test_w_mape)

        if target_col != 'cvr':
            all_df = all_df.withColumn('%s_pred' % target_col, F.exp('%s_pred' % target_col) - F.lit(1.0))

    all_df.registerTempTable('predict')

    spark.sql("""
        INSERT OVERWRITE TABLE adm.adm_cps_bef_all_pred PARTITION(dt='{dt}')
            SELECT
                batch_id,cvr,use_num,roi,sale_amt,ord_qtty,cvr_pred,use_num_pred,roi_pred,sale_amt_pred,ord_qtty_pred,
                flag
            FROM predict
        """.format(dt=execute_dt))

    print(LOG_PREFIX, '写adm.adm_cps_bef_all_pred表完成')

    s = "\"{'rmse':%.4f,'w-mape':%.4f}\" cvr,\"{'rmse':%.4f,'w-mape':%.4f}\" use_num,\"{'rmse':%.4f,'w-mape':%.4f}\" roi,\"{'rmse':%.4f,'w-mape':%.4f}\" sale_amt,\"{'rmse':%.4f,'w-mape':%.4f}\" ord_qtty" % (
        result[0], result[1], result[2], result[3], result[4], result[5], result[6], result[7],
        result[8], result[9])

    spark.sql("""
        INSERT OVERWRITE TABLE adm.adm_before_coupon_evaluate_monitor PARTITION(dt='{0}', evaluate_type=0)
            SELECT
                {1}, "" qty_score
        """.format(execute_dt, s))

    print(LOG_PREFIX, '写adm.adm_before_coupon_evaluate_monitor表完成')
