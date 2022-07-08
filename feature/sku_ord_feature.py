#!/usr/bin/env python3
# coding: utf-8

import argparse
import datetime
import os

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *

os.environ['PYSPARK_PYTHON'] = "/usr/local/anaconda3/bin/python3.6"
os.environ['PYSPARK_DRIVER_PYTHON'] = "/usr/local/anaconda3/bin/python3.6"

"""
CREATE EXTERNAL TABLE IF NOT EXISTS adm.adm_sku_ord_feature(
item_sku_id string COMMENT '商品SKU编号',
item_first_cate_cd string COMMENT '商品一级分类代码',
item_second_cate_cd string COMMENT '商品二级分类代码',
item_third_cate_cd string COMMENT '商品三级分类代码',
brand_cd string COMMENT '品牌代码',
jd_prc double COMMENT '京东价',
theta double COMMENT '价格弹性',
sale_qtty_sum_30d double COMMENT '近30天总计销量',
sale_qtty_mean_30d double COMMENT '近30天平均销量',
sale_qtty_min_30d double COMMENT '近30天最小销量',
sale_qtty_max_30d double COMMENT '近30天最大销量',
sale_qtty_std_30d double COMMENT '近30天销量标准差',
gmv_sum_30d double COMMENT '近30天总计gmv',
gmv_mean_30d double COMMENT '近30天平均gmv',
gmv_min_30d double COMMENT '近30天最小gmv',
gmv_max_30d double COMMENT '近30天最大gmv',
gmv_std_30d double COMMENT '近30天gmv标准差',
user_cnt_sum_30d double COMMENT '近30天总计购买人数',
user_cnt_mean_30d double COMMENT '近30天平均购买人数',
user_cnt_min_30d double COMMENT '近30天最小购买人数',
user_cnt_max_30d double COMMENT '近30天最大购买人数',
user_cnt_std_30d double COMMENT '近30天购买人数标准差',
order_cnt_sum_30d double COMMENT '近30天总计单量',
order_cnt_mean_30d double COMMENT '近30天平均单量',
order_cnt_min_30d double COMMENT '近30天最小单量',
order_cnt_max_30d double COMMENT '近30天最大单量',
order_cnt_std_30d double COMMENT '近30天单量标准差',
before_price_sum_30d double COMMENT '近30天总计优惠前价格',
before_price_mean_30d double COMMENT '近30天平均优惠前价格',
before_price_min_30d double COMMENT '近30天最小优惠前价格',
before_price_max_30d double COMMENT '近30天最大优惠前价格',
before_price_std_30d double COMMENT '近30天优惠前价格标准差',
avg_offer_amt_sum_30d double COMMENT '近30天总计促销优惠金额',
avg_offer_amt_mean_30d double COMMENT '近30天平均促销优惠金额',
avg_offer_amt_min_30d double COMMENT '近30天最小促销优惠金额',
avg_offer_amt_max_30d double COMMENT '近30天最大促销优惠金额',
avg_offer_amt_std_30d double COMMENT '近30天促销优惠金额标准差',
avg_coupon_amt_sum_30d double COMMENT '近30天总计优惠券优惠金额',
avg_coupon_amt_mean_30d double COMMENT '近30天平均优惠券优惠金额',
avg_coupon_amt_min_30d double COMMENT '近30天最小优惠券优惠金额',
avg_coupon_amt_max_30d double COMMENT '近30天最大优惠券优惠金额',
avg_coupon_amt_std_30d double COMMENT '近30天优惠券优惠金额标准差',
real_price_sum_30d double COMMENT '近30天总计到手价',
real_price_mean_30d double COMMENT '近30天平均到手价',
real_price_min_30d double COMMENT '近30天最小到手价',
real_price_max_30d double COMMENT '近30天最大到手价',
real_price_std_30d double COMMENT '近30天到手价标准差',
max_unit_price_30d double COMMENT '近30天最大到手价',
min_unit_price_30d double COMMENT '近30天最小到手价',
sale_qtty_lim_30d double COMMENT '近30天平均限购数',
sku_inventory_rt_30d double COMMENT '近30天缺货率',
real_jd_ratio_mean_30d double COMMENT '近30天平均到手价与京东价之比',
real_before_ratio_mean_30d double COMMENT '近30天平均到手价与优惠前单价之比',
offer_discount_ratio_mean_30d double COMMENT '近30天平均促销占总优惠比例',
coupon_discount_ratio_mean_30d double COMMENT '近30天平均优惠券占总优惠比例',
diff_real_ratio_mean_30d double COMMENT '近30天平均到手价极差',
cps_sale_qtty_sum_30d double COMMENT '近30天总计用券销量',
cps_sale_qtty_mean_30d double COMMENT '近30天平均用券销量',
cps_sale_qtty_min_30d double COMMENT '近30天最小用券销量',
cps_sale_qtty_max_30d double COMMENT '近30天最大用券销量',
cps_sale_qtty_std_30d double COMMENT '近30天用券销量标准差',
cps_gmv_sum_30d double COMMENT '近30天总计用券gmv',
cps_gmv_mean_30d double COMMENT '近30天平均用券gmv',
cps_gmv_min_30d double COMMENT '近30天最小用券gmv',
cps_gmv_max_30d double COMMENT '近30天最大用券gmv',
cps_gmv_std_30d double COMMENT '近30天用券gmv标准差',
cps_user_cnt_sum_30d double COMMENT '近30天总计用券购买人数',
cps_user_cnt_mean_30d double COMMENT '近30天平均用券购买人数',
cps_user_cnt_min_30d double COMMENT '近30天最小用券购买人数',
cps_user_cnt_max_30d double COMMENT '近30天最大用券购买人数',
cps_user_cnt_std_30d double COMMENT '近30天用券购买人数标准差',
cps_order_cnt_sum_30d double COMMENT '近30天总计用券单量',
cps_order_cnt_mean_30d double COMMENT '近30天平均用券单量',
cps_order_cnt_min_30d double COMMENT '近30天最小用券单量',
cps_order_cnt_max_30d double COMMENT '近30天最大用券单量',
cps_order_cnt_std_30d double COMMENT '近30天用券单量标准差',
cps_avg_offer_amt_sum_30d double COMMENT '近30天总计用券促销优惠金额',
cps_avg_offer_amt_mean_30d double COMMENT '近30天平均用券促销优惠金额',
cps_avg_offer_amt_min_30d double COMMENT '近30天最小用券促销优惠金额',
cps_avg_offer_amt_max_30d double COMMENT '近30天最大用券促销优惠金额',
cps_avg_offer_amt_std_30d double COMMENT '近30天用券促销优惠金额标准差',
cps_avg_coupon_amt_sum_30d double COMMENT '近30天总计用券优惠券优惠金额',
cps_avg_coupon_amt_mean_30d double COMMENT '近30天平均用券优惠券优惠金额',
cps_avg_coupon_amt_min_30d double COMMENT '近30天最小用券优惠券优惠金额',
cps_avg_coupon_amt_max_30d double COMMENT '近30天最大用券优惠券优惠金额',
cps_avg_coupon_amt_std_30d double COMMENT '近30天用券优惠券优惠金额标准差',
cps_real_price_sum_30d double COMMENT '近30天总计用券到手价',
cps_real_price_mean_30d double COMMENT '近30天平均用券到手价',
cps_real_price_min_30d double COMMENT '近30天最小用券到手价',
cps_real_price_max_30d double COMMENT '近30天最大用券到手价',
cps_real_price_std_30d double COMMENT '近30天用券到手价标准差',
cps_max_unit_price_30d double COMMENT '近30天最大用券到手价',
cps_min_unit_price_30d double COMMENT '近30天最小用券到手价',
cps_real_jd_ratio_mean_30d double COMMENT '近30天平均用券到手价与京东价之比',
cps_real_before_ratio_mean_30d double COMMENT '近30天平均用券到手价与优惠前单价之比',
cps_offer_discount_ratio_mean_30d double COMMENT '近30天平均用券促销占总优惠比例',
cps_coupon_discount_ratio_mean_30d double COMMENT '近30天平均用券优惠券占总优惠比例',
cps_diff_real_ratio_mean_30d double COMMENT '近30天平均用券到手价极差')
COMMENT 'sku订单相关特征表'
PARTITIONED BY (
  dt string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns1/user/dd_edw/adm.db/adm_sku_ord_feature';
"""

LOG_PREFIX = "[■■□□ ■□■■ □■■□]"

spark = SparkSession \
    .builder \
    .appName("sku_ord_feature") \
    .enableHiveSupport() \
    .config("spark.sql.crossJoin.enabled", "true") \
    .config("spark.executor.instances", "255") \
    .config("spark.executor.memory", "10g") \
    .config("spark.executor.cores", "4") \
    .config("spark.driver.memory", "10g") \
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
    start_dt = (datetime.datetime.strptime(execute_dt, '%Y-%m-%d') - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
    newest_dt = get_newest_partition(spark, 'adm.adm_sku_price_elasticity_month', '2022-05-09')
    print(LOG_PREFIX, 'execute_dt=%s,start_dt=%s,newest_dt=%s' % (execute_dt, start_dt, newest_dt))

    sku_ord_df = spark.sql("""
	SELECT
		*
	FROM
		(
			SELECT
				parent_sale_ord_id,
				IF((SUM(dq_and_jq_pay_amount) / SUM(sale_qtty)) > 0, 1, 0) AS cps_flag,
				SUBSTR(sale_ord_dt, 0, 10) AS sale_ord_dt,
				user_log_acct,
				item_sku_id,
				item_first_cate_cd,
				item_second_cate_cd,
				item_third_cate_cd,
				brand_cd,
				SUM(sale_qtty) AS sale_qtty, --销量
				SUM(after_prefr_amount) AS gmv,
				MEAN(sku_jd_prc) AS sku_jd_prc, --京东价
				(SUM(before_prefr_amount) / SUM(sale_qtty)) AS before_unit_price, --优惠前单价
				(SUM(total_offer_amount) / SUM(sale_qtty)) AS avg_offer_amt, --促销优惠金额
				(SUM(dq_and_jq_pay_amount) / SUM(sale_qtty)) AS avg_coupon_amt, --优惠券优惠金额
				(SUM(after_prefr_amount - dq_and_jq_pay_amount) / SUM(sale_qtty)) AS final_unit_price, --用户到手单价
				CAST(MAX(sale_qtty_lim) AS INT) AS sale_qtty_lim, --限购情况
				MAX(IF(sku_inventory_status_cd IN(0, 5, 33), 1, 0)) AS sku_inventory_status --库存状态
			FROM
				adm.adm_th04_deal_ord_det_sum
			WHERE
				dt >= '{start_dt}'
				AND dt <= '{dt}'
				AND sale_ord_dt >= '{start_dt}'
				AND sale_ord_dt <= '{dt}'
				AND is_deal_ord = 1
				AND item_third_cate_name <> '特殊商品' -- 排除特殊商品
				AND split_status_cd IN(2, 3) -- 2拆分后子单，3不需要拆分
				AND sale_ord_cate_cd IN(10, 20) -- 10 普通订单 20 POP实物订单 30 团购订单 (会剔除大量数据)
				AND sale_ord_valid_flag = 1 -- 加工来源为orders.yn ，默认为1，即有效订单
				AND after_prefr_unit_price > 0.1 -- 排除赠品，和赠送状态的订单
				AND user_lv_cd != 90 -- 排除企业购
				AND
				(
					free_goods_flag = 0
					OR free_goods_flag IS NULL
				)
				AND user_log_acct IS NOT NULL
				AND parent_sale_ord_id <> 0
				AND parent_sale_ord_id IS NOT NULL
			GROUP BY
				parent_sale_ord_id,
				sale_ord_dt,
				user_log_acct,
				item_sku_id,
				item_first_cate_cd,
				item_second_cate_cd,
				item_third_cate_cd,
				brand_cd
		)
		tmp LEFT ANTI
	JOIN
		(
			SELECT
				user_log_acct
			FROM
				adm.adm_m01_lable_user_enterprise_info_d
			WHERE
				dt = '{dt}'
		)
		t1 --剔除企业用户
	ON
		LOWER(TRIM(tmp.user_log_acct)) = LOWER(TRIM(t1.user_log_acct))
    """.format(dt=execute_dt, start_dt=start_dt))

    sku_ord_df.registerTempTable('sku_ord')

    sku_ord_day_df = spark.sql("""
	SELECT
		t1.item_sku_id,
		t1.item_first_cate_cd,
		t1.item_second_cate_cd,
		t1.item_third_cate_cd,
		t1.brand_cd,
		MEAN(jd_prc) AS jd_prc,
		IFNULL(MEAN(theta), IFNULL(MEAN(theta_brand_c3d), IFNULL(MEAN(theta_c3d), MEAN(theta_mean)))) AS theta,
		SUM(sale_qtty) AS sale_qtty_sum_30d,
		MEAN(sale_qtty) AS sale_qtty_mean_30d,
		MIN(sale_qtty) AS sale_qtty_min_30d,
		MAX(sale_qtty) AS sale_qtty_max_30d,
		STD(sale_qtty) AS sale_qtty_std_30d,
		SUM(gmv) AS gmv_sum_30d,
		MEAN(gmv) AS gmv_mean_30d,
		MIN(gmv) AS gmv_min_30d,
		MAX(gmv) AS gmv_max_30d,
		STD(gmv) AS gmv_std_30d,
		SUM(user_cnt) AS user_cnt_sum_30d,
		MEAN(user_cnt) AS user_cnt_mean_30d,
		MIN(user_cnt) AS user_cnt_min_30d,
		MAX(user_cnt) AS user_cnt_max_30d,
		STD(user_cnt) AS user_cnt_std_30d,
		SUM(order_cnt) AS order_cnt_sum_30d,
		MEAN(order_cnt) AS order_cnt_mean_30d,
		MIN(order_cnt) AS order_cnt_min_30d,
		MAX(order_cnt) AS order_cnt_max_30d,
		STD(order_cnt) AS order_cnt_std_30d,
		SUM(before_price) AS before_price_sum_30d,
		MEAN(before_price) AS before_price_mean_30d,
		MIN(before_price) AS before_price_min_30d,
		MAX(before_price) AS before_price_max_30d,
		STD(before_price) AS before_price_std_30d,
		SUM(avg_offer_amt) AS avg_offer_amt_sum_30d,
		MEAN(avg_offer_amt) AS avg_offer_amt_mean_30d,
		MIN(avg_offer_amt) AS avg_offer_amt_min_30d,
		MAX(avg_offer_amt) AS avg_offer_amt_max_30d,
		STD(avg_offer_amt) AS avg_offer_amt_std_30d,
		SUM(avg_coupon_amt) AS avg_coupon_amt_sum_30d,
		MEAN(avg_coupon_amt) AS avg_coupon_amt_mean_30d,
		MIN(avg_coupon_amt) AS avg_coupon_amt_min_30d,
		MAX(avg_coupon_amt) AS avg_coupon_amt_max_30d,
		STD(avg_coupon_amt) AS avg_coupon_amt_std_30d,
		SUM(real_price) AS real_price_sum_30d,
		MEAN(real_price) AS real_price_mean_30d,
		MIN(real_price) AS real_price_min_30d,
		MAX(real_price) AS real_price_max_30d,
		STD(real_price) AS real_price_std_30d,
		MAX(max_unit_price) AS max_unit_price_30d,
		MIN(min_unit_price) AS min_unit_price_30d,
		MEAN(sale_qtty_lim) AS sale_qtty_lim_30d,
		MEAN(sku_inventory_status) AS sku_inventory_rt_30d,
		MEAN(real_jd_ratio) AS real_jd_ratio_mean_30d,
		MEAN(real_before_ratio) AS real_before_ratio_mean_30d,
		MEAN(offer_discount_ratio) AS offer_discount_ratio_mean_30d,
		MEAN(coupon_discount_ratio) AS coupon_discount_ratio_mean_30d,
		MEAN(diff_real_ratio) AS diff_real_ratio_mean_30d,
		SUM(cps_sale_qtty) AS cps_sale_qtty_sum_30d,
		MEAN(cps_sale_qtty) AS cps_sale_qtty_mean_30d,
		MIN(cps_sale_qtty) AS cps_sale_qtty_min_30d,
		MAX(cps_sale_qtty) AS cps_sale_qtty_max_30d,
		STD(cps_sale_qtty) AS cps_sale_qtty_std_30d,
		SUM(cps_gmv) AS cps_gmv_sum_30d,
		MEAN(cps_gmv) AS cps_gmv_mean_30d,
		MIN(cps_gmv) AS cps_gmv_min_30d,
		MAX(cps_gmv) AS cps_gmv_max_30d,
		STD(cps_gmv) AS cps_gmv_std_30d,
		SUM(cps_user_cnt) AS cps_user_cnt_sum_30d,
		MEAN(cps_user_cnt) AS cps_user_cnt_mean_30d,
		MIN(cps_user_cnt) AS cps_user_cnt_min_30d,
		MAX(cps_user_cnt) AS cps_user_cnt_max_30d,
		STD(cps_user_cnt) AS cps_user_cnt_std_30d,
		SUM(cps_order_cnt) AS cps_order_cnt_sum_30d,
		MEAN(cps_order_cnt) AS cps_order_cnt_mean_30d,
		MIN(cps_order_cnt) AS cps_order_cnt_min_30d,
		MAX(cps_order_cnt) AS cps_order_cnt_max_30d,
		STD(cps_order_cnt) AS cps_order_cnt_std_30d,
		SUM(cps_avg_offer_amt) AS cps_avg_offer_amt_sum_30d,
		MEAN(cps_avg_offer_amt) AS cps_avg_offer_amt_mean_30d,
		MIN(cps_avg_offer_amt) AS cps_avg_offer_amt_min_30d,
		MAX(cps_avg_offer_amt) AS cps_avg_offer_amt_max_30d,
		STD(cps_avg_offer_amt) AS cps_avg_offer_amt_std_30d,
		SUM(cps_avg_coupon_amt) AS cps_avg_coupon_amt_sum_30d,
		MEAN(cps_avg_coupon_amt) AS cps_avg_coupon_amt_mean_30d,
		MIN(cps_avg_coupon_amt) AS cps_avg_coupon_amt_min_30d,
		MAX(cps_avg_coupon_amt) AS cps_avg_coupon_amt_max_30d,
		STD(cps_avg_coupon_amt) AS cps_avg_coupon_amt_std_30d,
		SUM(cps_real_price) AS cps_real_price_sum_30d,
		MEAN(cps_real_price) AS cps_real_price_mean_30d,
		MIN(cps_real_price) AS cps_real_price_min_30d,
		MAX(cps_real_price) AS cps_real_price_max_30d,
		STD(cps_real_price) AS cps_real_price_std_30d,
		MAX(cps_max_unit_price) AS cps_max_unit_price_30d,
		MIN(cps_min_unit_price) AS cps_min_unit_price_30d,
		MEAN(cps_real_jd_ratio) AS cps_real_jd_ratio_mean_30d,
		MEAN(cps_real_before_ratio) AS cps_real_before_ratio_mean_30d,
		MEAN(cps_offer_discount_ratio) AS cps_offer_discount_ratio_mean_30d,
		MEAN(cps_coupon_discount_ratio) AS cps_coupon_discount_ratio_mean_30d,
		MEAN(cps_diff_real_ratio) AS cps_diff_real_ratio_mean_30d
	FROM
		(
			SELECT
				sale_ord_dt,
				item_sku_id,
				item_first_cate_cd,
				item_second_cate_cd,
				item_third_cate_cd,
				brand_cd,
				SUM(sale_qtty) AS sale_qtty,
				SUM(gmv) AS gmv,
				COUNT(DISTINCT user_log_acct) AS user_cnt,
				COUNT(DISTINCT parent_sale_ord_id) AS order_cnt,
				SUM(before_unit_price * sale_qtty) / SUM(sale_qtty) AS before_price,
				SUM(avg_offer_amt * sale_qtty) / SUM(sale_qtty) AS avg_offer_amt,
				SUM(avg_coupon_amt * sale_qtty) / SUM(sale_qtty) AS avg_coupon_amt,
				SUM(final_unit_price * sale_qtty) / SUM(sale_qtty) AS real_price,
				MAX(final_unit_price) AS max_unit_price,
				MIN(final_unit_price) AS min_unit_price,
				MAX(sale_qtty_lim) AS sale_qtty_lim,
				MIN(sku_inventory_status) AS sku_inventory_status, --是否有缺货情况
				(SUM(final_unit_price * sale_qtty) / SUM(sale_qtty)) / MEAN(sku_jd_prc) AS real_jd_ratio,
				SUM(final_unit_price * sale_qtty) / SUM(before_unit_price * sale_qtty) AS real_before_ratio,
				SUM(avg_offer_amt * sale_qtty) /(SUM(before_unit_price * sale_qtty) - SUM(final_unit_price * sale_qtty)) AS offer_discount_ratio,
				SUM(avg_coupon_amt * sale_qtty) /(SUM(before_unit_price * sale_qtty) - SUM(final_unit_price * sale_qtty)) AS coupon_discount_ratio,
				(MAX(final_unit_price) - MIN(final_unit_price)) /(SUM(final_unit_price * sale_qtty) / SUM(sale_qtty)) AS diff_real_ratio
			FROM
				sku_ord
			GROUP BY
				sale_ord_dt,
				item_sku_id,
				item_first_cate_cd,
				item_second_cate_cd,
				item_third_cate_cd,
				brand_cd
			HAVING
				SUM(final_unit_price * sale_qtty) / SUM(sale_qtty) > 0.1
		)
		t1
	LEFT JOIN
		(
			SELECT
				sale_ord_dt,
				item_sku_id,
				item_first_cate_cd,
				item_second_cate_cd,
				item_third_cate_cd,
				brand_cd,
				SUM(sale_qtty) AS cps_sale_qtty,
				SUM(gmv) AS cps_gmv,
				COUNT(DISTINCT user_log_acct) AS cps_user_cnt,
				COUNT(DISTINCT parent_sale_ord_id) AS cps_order_cnt,
				SUM(avg_offer_amt * sale_qtty) / SUM(sale_qtty) AS cps_avg_offer_amt,
				SUM(avg_coupon_amt * sale_qtty) / SUM(sale_qtty) AS cps_avg_coupon_amt,
				SUM(final_unit_price * sale_qtty) / SUM(sale_qtty) AS cps_real_price,
				MAX(final_unit_price) AS cps_max_unit_price,
				MIN(final_unit_price) AS cps_min_unit_price,
				(SUM(final_unit_price * sale_qtty) / SUM(sale_qtty)) / MEAN(sku_jd_prc) AS cps_real_jd_ratio,
				SUM(final_unit_price * sale_qtty) / SUM(before_unit_price * sale_qtty) AS cps_real_before_ratio,
				SUM(avg_offer_amt * sale_qtty) /(SUM(before_unit_price * sale_qtty) - SUM(final_unit_price * sale_qtty)) AS cps_offer_discount_ratio,
				SUM(avg_coupon_amt * sale_qtty) /(SUM(before_unit_price * sale_qtty) - SUM(final_unit_price * sale_qtty)) AS cps_coupon_discount_ratio,
				(MAX(final_unit_price) - MIN(final_unit_price)) /(SUM(final_unit_price * sale_qtty) / SUM(sale_qtty)) AS cps_diff_real_ratio
			FROM
				sku_ord
			WHERE
				cps_flag = 1
			GROUP BY
				sale_ord_dt,
				item_sku_id,
				item_first_cate_cd,
				item_second_cate_cd,
				item_third_cate_cd,
				brand_cd
			HAVING
				SUM(final_unit_price * sale_qtty) / SUM(sale_qtty) > 0.1
		)
		t2
	ON
		t1.sale_ord_dt = t2.sale_ord_dt
		AND t1.item_sku_id = t2.item_sku_id
		AND t1.item_first_cate_cd = t2.item_first_cate_cd
		AND t1.item_second_cate_cd = t2.item_second_cate_cd
		AND t1.item_third_cate_cd = t2.item_third_cate_cd
		AND t1.brand_cd = t2.brand_cd
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
		t3
	ON
		t1.item_sku_id = t3.item_sku_id
	LEFT JOIN
		(
			SELECT
				item_sku_id,
				theta
			FROM
				adm.adm_sku_price_elasticity_month
			WHERE
				dt = '{newest_dt}'
		)
		t4
	ON
		t1.item_sku_id = t4.item_sku_id
	LEFT JOIN
		(
			SELECT
				item_third_cate_cd,
				brand_cd,
				theta AS theta_brand_c3d
			FROM
				adm.adm_brand_c3d_price_elasticity_month
			WHERE
				dt = '{newest_dt}'
		)
		t5
	ON
		t1.item_third_cate_cd = t5.item_third_cate_cd
		AND t1.brand_cd = t2.brand_cd
	LEFT JOIN
		(
			SELECT
				item_third_cate_cd,
				theta AS theta_c3d
			FROM
				adm.adm_c3d_price_elasticity_month
			WHERE
				dt = '{newest_dt}'
		)
		t6
	ON
		t1.item_third_cate_cd = t6.item_third_cate_cd
	JOIN
		(
			SELECT
				MEAN(theta) AS theta_mean
			FROM
				adm.adm_sku_price_elasticity_month
			WHERE
				dt = '{newest_dt}'
		)
	GROUP BY
		t1.item_sku_id,
		t1.item_first_cate_cd,
		t1.item_second_cate_cd,
		t1.item_third_cate_cd,
		t1.brand_cd
    """.format(dt=execute_dt, newest_dt=newest_dt))

    sku_ord_day_df.registerTempTable('sku_ord_day')

    spark.sql("""
        INSERT OVERWRITE TABLE adm.adm_sku_ord_feature PARTITION(dt='{dt}')
            SELECT
                item_sku_id,item_first_cate_cd,item_second_cate_cd,item_third_cate_cd,brand_cd,jd_prc,theta,
                sale_qtty_sum_30d,sale_qtty_mean_30d,sale_qtty_min_30d,sale_qtty_max_30d,sale_qtty_std_30d,
                gmv_sum_30d,gmv_mean_30d,gmv_min_30d,gmv_max_30d,gmv_std_30d,user_cnt_sum_30d,user_cnt_mean_30d,
                user_cnt_min_30d,user_cnt_max_30d,user_cnt_std_30d,order_cnt_sum_30d,order_cnt_mean_30d,
                order_cnt_min_30d,order_cnt_max_30d,order_cnt_std_30d,before_price_sum_30d,before_price_mean_30d,
                before_price_min_30d,before_price_max_30d,before_price_std_30d,avg_offer_amt_sum_30d,
                avg_offer_amt_mean_30d,avg_offer_amt_min_30d,avg_offer_amt_max_30d,avg_offer_amt_std_30d,
                avg_coupon_amt_sum_30d,avg_coupon_amt_mean_30d,avg_coupon_amt_min_30d,avg_coupon_amt_max_30d,
                avg_coupon_amt_std_30d,real_price_sum_30d,real_price_mean_30d,real_price_min_30d,real_price_max_30d,
                real_price_std_30d,max_unit_price_30d,min_unit_price_30d,sale_qtty_lim_30d,sku_inventory_rt_30d,
                real_jd_ratio_mean_30d,real_before_ratio_mean_30d,offer_discount_ratio_mean_30d,
                coupon_discount_ratio_mean_30d,diff_real_ratio_mean_30d,cps_sale_qtty_sum_30d,
                cps_sale_qtty_mean_30d,cps_sale_qtty_min_30d,cps_sale_qtty_max_30d,cps_sale_qtty_std_30d,
                cps_gmv_sum_30d,cps_gmv_mean_30d,cps_gmv_min_30d,cps_gmv_max_30d,cps_gmv_std_30d,
                cps_user_cnt_sum_30d,cps_user_cnt_mean_30d,cps_user_cnt_min_30d,cps_user_cnt_max_30d,
                cps_user_cnt_std_30d,cps_order_cnt_sum_30d,cps_order_cnt_mean_30d,cps_order_cnt_min_30d,
                cps_order_cnt_max_30d,cps_order_cnt_std_30d,cps_avg_offer_amt_sum_30d,cps_avg_offer_amt_mean_30d,
                cps_avg_offer_amt_min_30d,cps_avg_offer_amt_max_30d,cps_avg_offer_amt_std_30d,
                cps_avg_coupon_amt_sum_30d,cps_avg_coupon_amt_mean_30d,cps_avg_coupon_amt_min_30d,
                cps_avg_coupon_amt_max_30d,cps_avg_coupon_amt_std_30d,cps_real_price_sum_30d,
                cps_real_price_mean_30d,cps_real_price_min_30d,cps_real_price_max_30d,cps_real_price_std_30d,
                cps_max_unit_price_30d,cps_min_unit_price_30d,cps_real_jd_ratio_mean_30d,
                cps_real_before_ratio_mean_30d,cps_offer_discount_ratio_mean_30d,cps_coupon_discount_ratio_mean_30d,
                cps_diff_real_ratio_mean_30d
            FROM sku_ord_day
        """.format(dt=execute_dt))

    print(LOG_PREFIX, '写adm_sku_ord_feature表完成')
