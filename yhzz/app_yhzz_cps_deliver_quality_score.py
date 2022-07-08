#!/usr/bin/env python3
# coding: utf-8

import argparse
import datetime
import math
import os

import numpy as np
import scipy.stats as spstats
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from scipy.special import inv_boxcox

os.environ['PYSPARK_PYTHON'] = "/usr/local/anaconda3/bin/python3.6"
os.environ['PYSPARK_DRIVER_PYTHON'] = "/usr/local/anaconda3/bin/python3.6"

"""
CREATE TABLE app.app_yhzz_cps_deliver_quality_score(
  batch_id bigint COMMENT 'batch_id',
  use_num_score double COMMENT '用户量(质量分)',
  smooth_avg_bayes_cvr_score double COMMENT '贝叶斯平滑CVR转化率(质量分)',
  roi_score double COMMENT 'ROI(质量分)',
  valid_sale_amt_score double COMMENT '有效下单金额(质量分)',
  valid_parent_ord_qtty_score double COMMENT '有效下单父单量(质量分)',
  quality_score double COMMENT '综合质量分')
COMMENT '优惠券投后质量分'
PARTITIONED BY (
  dt string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns9/user/mart_scr/app.db/app_yhzz_cps_deliver_quality_score';
"""

LOG_PREFIX = "[■■□□ ■□■■ □■■□]"

spark = SparkSession \
    .builder \
    .appName("app_yhzz_cps_deliver_quality_score") \
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

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--execute_dt', type=str, default='', help='date of script execution')

    args = parser.parse_args()
    execute_dt = args.execute_dt
    two_month_bef_dt = (datetime.datetime.strptime(execute_dt, '%Y-%m-%d') - datetime.timedelta(days=61)).strftime(
        "%Y-%m-%d")
    print(LOG_PREFIX, 'execute_dt=%s, two_month_bef_dt=%s' % (execute_dt, two_month_bef_dt))

    param = eval(spark.sql("""
    SELECT
	    param
    FROM
	    app.app_cps_deliver_aft_quality_score_param
    WHERE
	    dt = '{execute_dt}'
    """.format(execute_dt=execute_dt)).collect()[0][0])

    data = spark.sql("""
    SELECT
    	t1.batch_id,
    	t1.use_num,
    	t1.use_cnt,
    	t1.get_cnt,
    	t1.get_use_cvr,
    	t1.smooth_avg_bayes_cvr,
    	t1.roi,
    	t1.valid_sale_amt,
    	t1.valid_parent_ord_qtty,
    	t1.use_num_aft_scale,
    	t1.smooth_avg_bayes_cvr_aft_scale,
    	t1.roi_aft_scale,
    	t1.valid_sale_amt_aft_scale,
    	t1.valid_parent_ord_qtty_aft_scale,
    	t1.quality_score,
    	t2.cvr_pred,
    	t2.use_num_pred,
    	t2.roi_pred,
    	t2.sale_amt_pred,
    	t2.ord_qtty_pred,
    	IF(t3.valid_start_dt >= '{execute_dt}', 'before', IF(t3.valid_end_dt <= '{execute_dt}', 'after', 'duration')) status
    FROM
    	(
    		SELECT
    			*
    		FROM
    			app.app_cps_deliver_aft_quality_score_v2
    		WHERE
    			dt = '{execute_dt}'
    			AND sf_type = 0
    	)
    	t1
    INNER JOIN
    	(
    		SELECT
    			batch_id,
    			cvr_pred,
    			use_num_pred,
    			roi_pred,
    			sale_amt_pred,
    			ord_qtty_pred
    		FROM
    			adm.adm_cps_bef_all_pred
    		WHERE
    			dt = '{execute_dt}'
    			AND flag = 'yhzz'
    	)
    	t2
    ON
    	t1.batch_id = t2.batch_id
    INNER JOIN
    	(
    		SELECT
    			batch_id,
    			valid_start_dt,
    			valid_end_dt
    		FROM
    			(
    				SELECT
    					*,
    					MAX(dt) over(partition BY batch_id) max_dt
    				FROM
    					app.app_coupon_score_label_sum
    				WHERE
    					dt >= '{two_month_bef_dt}'
    					AND dt <= '{execute_dt}'
    					AND sf_type = 0
    			)
    			aa
    		WHERE
    			dt = max_dt
    	)
    	t3
    ON
    	t1.batch_id = t3.batch_id
    """.format(execute_dt=execute_dt, two_month_bef_dt=two_month_bef_dt)).toPandas()

    data['status'] = data.apply(
        lambda x: 'before_' if (x['status'] == 'duration' and x['use_cnt'] == 0) else x['status'], axis=1)

    df = data.copy(deep=True)

    cols = ['use_num_aft', 'smooth_avg_bayes_cvr_aft', 'roi_aft', 'valid_sale_amt_aft', 'valid_parent_ord_qtty_aft']
    pred_cols = ['use_num_pred', 'cvr_pred', 'roi_pred', 'sale_amt_pred', 'ord_qtty_pred']
    cols_boxcox_minmax = [i + '_scale' for i in cols]

    df['use_num_aft'] = df['use_num_pred'].apply(lambda x: 1 if x <= 0 else x)
    df['smooth_avg_bayes_cvr_aft'] = df['cvr_pred'].apply(lambda x: 0.000001 if x <= 0 else x)
    df['roi_aft'] = df['roi_pred'].apply(lambda x: 0.0000001 if x <= 0 else x)
    df['valid_sale_amt_aft'] = df['sale_amt_pred'].apply(lambda x: 1 if x <= 0 else x)
    df['valid_parent_ord_qtty_aft'] = df['ord_qtty_pred'].apply(lambda x: 1 if x <= 0 else x)

    for col_aft, col, col_after in zip(cols, pred_cols, cols_boxcox_minmax):

        scale_min, scale_max = 1, 99
        l = []
        a = []
        left_delta, right_delta = 0.1, 0.1

        opt_lambda = float(param.get(col_aft + '_bc'))

        for c_aft, c in zip(spstats.boxcox(df[col_aft], lmbda=opt_lambda), df[col]):
            if col_aft in ('use_num_aft', 'valid_parent_ord_qtty_aft'):
                if c > 1:
                    l.append(c_aft)
                    a.append(c_aft)
                elif c == 1:
                    l.append(1)
                else:
                    l.append(0)
            else:
                if c > 0:
                    l.append(c_aft)
                    a.append(c_aft)
                else:
                    l.append(0)
        a = np.array(a)

        max1, min1 = param.get(col_aft + '_max'), param.get(col_aft + '_min')
        diff = max1 - min1
        res = []
        for i in l:
            if i == 0:
                res.append(0)
            elif i == 1 and col_aft in ('use_num_aft', 'valid_parent_ord_qtty_aft'):
                res.append(1)
            elif i > max1:
                try:
                    if col_aft in ('valid_parent_ord_qtty_aft'):
                        i = inv_boxcox(np.array([i]), opt_lambda)[0]
                    else:
                        i = math.log(inv_boxcox(np.array([i]), opt_lambda)[0])
                    if col_aft in ('smooth_avg_bayes_cvr_aft'):
                        max_buffer = i ** 6
                    else:
                        corr = 0.01
                        max_buffer = (math.exp(corr * i) - math.exp(-corr * i)) / (
                                math.exp(corr * i) + math.exp(-corr * i))
                    res.append(scale_max + max_buffer)
                except:
                    res.append(scale_max)
            elif i < min1:
                try:
                    if col_aft in ('smooth_avg_bayes_cvr_aft'):
                        i = inv_boxcox(np.array([i]), opt_lambda)[0]
                        corr = 300
                        min_buffer = (math.exp(corr * i) - math.exp(-corr * i)) / (
                                math.exp(corr * i) + math.exp(-corr * i))
                    else:
                        min_buffer = scale_min
                    res.append(min_buffer)
                except:
                    res.append(scale_min)
            else:
                res.append(scale_min + (scale_max - scale_min) * (i - min1) / diff)
        df[col_after] = res

    df['quality_score'] = 0
    for c in cols_boxcox_minmax:
        df['quality_score'] += df[c]

    col = ['batch_id', 'use_num', 'use_cnt', 'get_cnt', 'get_use_cvr', 'smooth_avg_bayes_cvr', 'roi', 'valid_sale_amt',
           'valid_parent_ord_qtty',
           'use_num_aft_scale', 'smooth_avg_bayes_cvr_aft_scale', 'roi_aft_scale', 'valid_sale_amt_aft_scale',
           'valid_parent_ord_qtty_aft_scale', 'quality_score', 'status']

    spark_df = spark.createDataFrame(
        data[(data['status'] != 'before') & (data['status'] != 'before_')][col].append(
            df[(df['status'] == 'before') | (df['status'] == 'before_')][col],
            ignore_index=True), col)
    spark_df.createOrReplaceTempView('temp1')

    sql = """ 
    INSERT
	    overwrite TABLE app.app_yhzz_cps_deliver_quality_score partition
	    (
		    dt = '{execute_dt}'
	    )
    SELECT
	    batch_id,
	    use_num_aft_scale,
	    smooth_avg_bayes_cvr_aft_scale,
	    roi_aft_scale,
	    valid_sale_amt_aft_scale,
	    valid_parent_ord_qtty_aft_scale,
	    quality_score
    FROM
	    temp1 """.format(execute_dt=execute_dt)

    spark.sql(sql)
    print(LOG_PREFIX, '写app_yhzz_cps_deliver_quality_score表完成')
