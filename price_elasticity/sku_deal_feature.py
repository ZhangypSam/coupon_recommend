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
CREATE EXTERNAL TABLE IF NOT EXISTS adm.adm_sku_deal_ord_feature_month(
item_sku_id string COMMENT '商品SKU编号',
item_name string COMMENT '商品名称',
item_first_cate_cd string COMMENT '商品一级分类代码',
item_second_cate_cd string COMMENT '商品二级分类代码',
item_third_cate_cd string COMMENT '商品三级分类代码',
brand_cd string COMMENT '品牌代码',
sale_ord_dt string COMMENT '销售订单订购日期',
day_of_month string COMMENT '销售日期日号',
week_of_month string COMMENT '销售日期第几周',
day_of_week string COMMENT '销售日期周几',
month string COMMENT '销售日期月份',
weekend int COMMENT '是否周末',
sale_qtty bigint COMMENT '销售数量',
real_price double COMMENT '到手价',
LnP double COMMENT 'LnP',
LnQ double COMMENT 'LnQ',
sku_jd_prc double COMMENT '京东价',
before_price double COMMENT '优惠前单价',
avg_offer_amt double COMMENT '促销优惠金额',
avg_coupon_amt double COMMENT '优惠券优惠金额',
max_unit_price double COMMENT '最大到手价',
min_unit_price double COMMENT '最小到手价',
sale_qtty_lim int COMMENT '是否有限购',
sku_inventory_status int COMMENT '是否有缺货',
real_jd_ratio double COMMENT '到手价与京东价之比',
real_before_ratio double COMMENT '到手价与优惠前单价之比',
offer_discount_ratio double COMMENT '促销占总优惠比例',
coupon_discount_ratio double COMMENT '优惠券占总优惠比例',
diff_real_ratio double COMMENT '到手价极差',
order_cnt bigint COMMENT '日单量')
COMMENT 'sku成交特征表'
PARTITIONED BY (
  dt string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns1/user/dd_edw/adm.db/adm.adm_sku_deal_ord_feature_month';
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


def last_day_of_last_month(any_day):
    """
        获取一前个月中的最后一天
        :param any_day: 任意日期
        :return: string
    """
    any_day = datetime.datetime.strptime(any_day, '%Y-%m-%d')
    return (any_day.replace(day=1) - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

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
    end_dt = last_day_of_last_month(execute_dt)
    print(LOG_PREFIX, 'execute_dt=%s,start_dt=%s,end_dt=%s' % (execute_dt, start_dt, end_dt))

    deal_df = spark.sql("""
    SELECT
    	sale_ord_dt,
    	item_sku_id,
    	MAX(item_name) AS item_name,
    	item_first_cate_cd,
    	item_second_cate_cd,
    	item_third_cate_cd,
    	brand_cd,
    	SUM(sale_qtty) AS sale_qtty,
    	MEAN(sku_jd_prc) AS sku_jd_prc,
    	SUM(before_unit_price * sale_qtty) / SUM(sale_qtty) AS before_price,
    	SUM(avg_offer_amt * sale_qtty) / SUM(sale_qtty) AS avg_offer_amt,
    	SUM(avg_coupon_amt * sale_qtty) / SUM(sale_qtty) AS avg_coupon_amt,
    	SUM(final_unit_price * sale_qtty) / SUM(sale_qtty) AS real_price,
    	LOG(SUM(sale_qtty)) AS LnQ,
    	LOG(SUM(final_unit_price * sale_qtty) / SUM(sale_qtty)) AS LnP,
    	COUNT(DISTINCT parent_sale_ord_id) AS order_cnt,
    	MAX(day_of_month) AS day_of_month,
    	MAX(week_of_month) AS week_of_month,
    	MAX(day_of_week) AS day_of_week,
    	MAX(month) AS month,
    	MAX(weekend) AS weekend,
    	MAX(big_sale) AS big_sale,
    	MAX(final_unit_price) AS max_unit_price,
    	MIN(final_unit_price) AS min_unit_price,
    	MAX(sale_qtty_lim) AS sale_qtty_lim,
    	MIN(sku_inventory_status_cd) AS sku_inventory_status --是否有缺货情况
    FROM
    	(
    		SELECT
    			parent_sale_ord_id,
    			user_log_acct,
    			SUBSTR(sale_ord_dt, 0, 10) AS sale_ord_dt,
    			item_sku_id,
    			MAX(item_name) AS item_name,
    			item_first_cate_cd,
    			item_second_cate_cd,
    			item_third_cate_cd,
    			brand_cd,
    			SUM(sale_qtty) AS sale_qtty, --销量
    			MEAN(sku_jd_prc) AS sku_jd_prc, --京东价
    			(SUM(before_prefr_amount) / SUM(sale_qtty)) AS before_unit_price, --优惠前单价
    			(SUM(total_offer_amount) / SUM(sale_qtty)) AS avg_offer_amt, --促销优惠金额
    			(SUM(dq_and_jq_pay_amount) / SUM(sale_qtty)) AS avg_coupon_amt, --优惠券优惠金额
    			(SUM(after_prefr_amount - dq_and_jq_pay_amount) / SUM(sale_qtty)) AS final_unit_price, --用户到手单价
    			DAY(sale_ord_dt) AS day_of_month,
    			CAST(DATE_FORMAT(sale_ord_dt, 'W') AS INT) AS week_of_month,
    			CAST(DATE_FORMAT(sale_ord_dt, 'u') AS INT) AS day_of_week,
    			MONTH(sale_ord_dt) AS month,
    			CASE
    				WHEN DATE_FORMAT(sale_ord_dt, 'u') IN(6, 7)
    				THEN 1
    				ELSE 0
    			END AS weekend, --是否工作日/周末
    			CASE
    				WHEN
    					(
    						SUBSTR(sale_ord_dt, 6, 10) >= '06-01'
    						AND SUBSTR(sale_ord_dt, 6, 10) <= '06-18'
    					)
    					OR
    					(
    						SUBSTR(sale_ord_dt, 6, 10) >= '11-01'
    						AND SUBSTR(sale_ord_dt, 6, 10) <= '11-11'
    					)
    				THEN 1
    				ELSE 0
    			END AS big_sale, --是否大促
    			CAST(MAX(sale_qtty_lim) AS INT) AS sale_qtty_lim, --限购情况
    			MAX(IF(sku_inventory_status_cd IN(0, 5, 33), 1, 0)) AS sku_inventory_status_cd --库存状态
    		FROM
    			adm.adm_th04_deal_ord_det_sum
    		WHERE
    			dt >= '{start_dt}'
    			AND dt < '{execute_dt}'
    			AND sale_ord_dt >= '{start_dt}'
    			AND sale_ord_dt <= '{end_dt}'
    			AND is_deal_ord = 1
    			AND item_third_cate_name <> '特殊商品' -- 排除特殊商品
    			AND split_status_cd IN(2, 3) -- # 2拆分后子单，3不需要拆分
    			AND sale_ord_cate_cd IN(10,20) --# 10 普通订单 20 POP实物订单 30 团购订单 
    			AND sale_ord_valid_flag = 1 --# 加工来源为orders.yn ，默认为1，即有效订单
    			AND after_prefr_unit_price > 0.1 -- 排除赠品，和赠送状态的订单
    			AND user_lv_cd != 90 -- 排除企业购
    			AND
    			(
    				free_goods_flag = 0
    				OR free_goods_flag IS NULL
    			)
    			AND item_name NOT LIKE CONCAT('%', '非商品', '%')
    			AND item_name NOT LIKE CONCAT('%', '非实物', '%')
    			AND item_name NOT LIKE CONCAT('%', '虚拟', '%')
    			AND item_name NOT LIKE CONCAT('%', '测试', '%')
    			AND item_name NOT LIKE CONCAT('%', '不发货', '%')
    			AND item_name NOT LIKE CONCAT('%', '补差链接', '%')
    			AND item_name NOT LIKE CONCAT('%', '特权', '%')
    			AND item_name NOT LIKE CONCAT('%', '福利包', '%')
    			AND item_name NOT LIKE CONCAT('%', '权益', '%')
    			AND user_log_acct IS NOT NULL
    			AND parent_sale_ord_id <> 0
    			AND parent_sale_ord_id IS NOT NULL
    		GROUP BY
    			parent_sale_ord_id,
    			user_log_acct,
    			sale_ord_dt,
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
    			dt = '{end_dt}'
    	)
    	t1 --剔除企业用户
    ON
    	LOWER(TRIM(tmp.user_log_acct)) = LOWER(TRIM(t1.user_log_acct))
    WHERE
    	big_sale = 0
    GROUP BY
    	sale_ord_dt,
    	item_sku_id,
    	item_first_cate_cd,
    	item_second_cate_cd,
    	item_third_cate_cd,
    	brand_cd
    HAVING
    	SUM(final_unit_price * sale_qtty) / SUM(sale_qtty) > 0.1
    """.format(execute_dt=execute_dt, start_dt=start_dt, end_dt=end_dt))

    deal_df = deal_df \
        .withColumn('real_jd_ratio', F.col('real_price') / F.col('sku_jd_prc')) \
        .withColumn('real_before_ratio', F.col('real_price') / F.col('before_price')) \
        .withColumn('offer_discount_ratio', F.col('avg_offer_amt') / (F.col('before_price') - F.col('real_price'))) \
        .withColumn('coupon_discount_ratio', F.col('avg_coupon_amt') / (F.col('before_price') - F.col('real_price'))) \
        .withColumn('diff_real_ratio', (F.col('max_unit_price') - F.col('min_unit_price')) / F.col('real_price')) \
        .fillna(0)

    deal_df.registerTempTable('sku_deal_feature')

    spark.sql("""
    INSERT OVERWRITE TABLE adm.adm_sku_deal_ord_feature_month PARTITION(dt='{dt}')
        SELECT
            item_sku_id,item_name,item_first_cate_cd,item_second_cate_cd,item_third_cate_cd,brand_cd,sale_ord_dt,
            day_of_month,week_of_month,day_of_week,month,weekend,sale_qtty,real_price,LnP,LnQ,sku_jd_prc,before_price,
            avg_offer_amt,avg_coupon_amt,max_unit_price,min_unit_price,sale_qtty_lim,sku_inventory_status,
            real_jd_ratio,real_before_ratio,offer_discount_ratio,coupon_discount_ratio,diff_real_ratio,order_cnt
        FROM sku_deal_feature
    """.format(dt=start_dt))

    print(LOG_PREFIX, '写表完成')
