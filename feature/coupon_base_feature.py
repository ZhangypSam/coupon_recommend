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
CREATE EXTERNAL TABLE IF NOT EXISTS adm.adm_cps_base_feature(
batch_id bigint COMMENT '批次id',
cps_name string COMMENT '优惠券名称',
cps_face_value double COMMENT '面额',
consume_lim double COMMENT '消费限额',
batch_cps_total_qtty bigint COMMENT '该批次优惠券数量',
start_tm string COMMENT '优惠券开始时间',
end_tm string COMMENT '优惠券结束时间',
valid_days int COMMENT '有效期天数',
month int COMMENT '优惠券开始月份',
valid_flag int COMMENT '批次有效标志',
allow_overlap int COMMENT '是否允许叠加',
cps_cate_cd int COMMENT '优惠券种类',
coupon_style int COMMENT '优惠券分类',
source_type int COMMENT '来源类型',
limit_rule_type int COMMENT '限品类规则类型',
limit_area_type int COMMENT '限区域类型',
limit_organization int COMMENT '限制的体系',
user_tag_cd string COMMENT '用户标签代码',
biz_tag_cd string COMMENT '业务标签代码',
activity_type_cd string COMMENT '活动优惠券领取限制类型代码',
user_lv_cd string COMMENT '用户级别代码',
open_flag int COMMENT '公开标志',
hour_coupon_flag int COMMENT '小时券标志',
new_user_flag int COMMENT '全站拉新标志',
cate_new_user_flag int COMMENT '分类拉新标志',
shop_new_user_flag int COMMENT '店铺拉新标志',
shufang_flag int COMMENT '数坊标志',
crowd_type int COMMENT '人群类型 0:高潜，1:拉新，2:一转二，3:复购',
shangxiang_flag int COMMENT '商详标志',
open_type_1_2 int COMMENT '公开到领券中心',
open_type_1_3 int COMMENT '公开到乡村电商推广',
open_type_1_4 int COMMENT '公开到联盟',
open_type_1_5 int COMMENT '公开到中台券池',
open_type_1_6 int COMMENT '全网推广',
open_type_1_7 int COMMENT '订单列表&详情',
open_type_1_8 int COMMENT '数坊智能投放',
open_type_1_999 int COMMENT '公开未知',
open_type_0_1 int COMMENT '其他推广',
open_type_0_999 int COMMENT '不公开未知')
COMMENT '近一年cps基础特征表'
PARTITIONED BY (
  dt string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns1/user/dd_edw/adm.db/adm_cps_base_feature';
"""

LOG_PREFIX = "[■■□□ ■□■■ □■■□]"

spark = SparkSession \
    .builder \
    .appName("coupon_base_feature") \
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


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--execute_dt', type=str, default='', help='date of script execution')

    args = parser.parse_args()
    execute_dt = args.execute_dt
    print(LOG_PREFIX, 'execute_dt=%s' % execute_dt)

    coupon_base_df = spark.sql("""
    SELECT
		t1.batch_id,
		cps_name,
		cps_face_value,
		consume_lim,
		batch_cps_total_qtty,
		start_tm,
		end_tm,
		valid_days,
		month,
		valid_flag,
		allow_overlap,
		cps_cate_cd,
		coupon_style,
		source_type,
		limit_rule_type,
		limit_area_type,
		limit_organization,
		user_tag_cd,
		biz_tag_cd,
		COALESCE(activity_type_cd, '-1') AS activity_type_cd,
		COALESCE(user_lv_cd, '-1') AS user_lv_cd,
		COALESCE(open_flag, '-1') AS open_flag,
		IF(open_flag = 1
		AND ARRAY_CONTAINS(open_type_cd_list, '1'), 1, 0) AS shangxiang_flag,
		IF(open_flag = 1
		AND ARRAY_CONTAINS(open_type_cd_list, '2'), 1, 0) AS open_type_1_2,
		IF(open_flag = 1
		AND ARRAY_CONTAINS(open_type_cd_list, '3'), 1, 0) AS open_type_1_3,
		IF(open_flag = 1
		AND ARRAY_CONTAINS(open_type_cd_list, '4'), 1, 0) AS open_type_1_4,
		IF(open_flag = 1
		AND ARRAY_CONTAINS(open_type_cd_list, '5'), 1, 0) AS open_type_1_5,
		IF(open_flag = 1
		AND ARRAY_CONTAINS(open_type_cd_list, '6'), 1, 0) AS open_type_1_6,
		IF(open_flag = 1
		AND ARRAY_CONTAINS(open_type_cd_list, '7'), 1, 0) AS open_type_1_7,
		IF(open_flag = 1
		AND ARRAY_CONTAINS(open_type_cd_list, '8'), 1, 0) AS open_type_1_8,
		IF(open_flag = 1
		AND ARRAY_CONTAINS(open_type_cd_list, '999'), 1, 0) AS open_type_1_999,
		IF(open_flag = 0
		AND ARRAY_CONTAINS(open_type_cd_list, '1'), 1, 0) AS open_type_0_1,
		IF(open_flag = 0
		AND ARRAY_CONTAINS(open_type_cd_list, '999'), 1, 0) AS open_type_0_999,
		hour_coupon_flag,
		COALESCE(new_user_flag, '-1') AS new_user_flag,
		COALESCE(cate_new_user_flag, '-1') AS cate_new_user_flag,
		COALESCE(shop_new_user_flag, '-1') AS shop_new_user_flag,
		shufang_flag,
		crowd_type
	FROM
		(
			SELECT
				batch_id,
				cps_name,
				cps_face_value,
				consume_lim,
				batch_cps_total_qtty,
				activity_key,
				valid_flag,
				allow_overlap,
				cps_cate_cd,
				coupon_style,
				source_type,
				limit_rule_type,
				limit_area_type,
				limit_organization,
				start_tm,
				end_tm,
				IF(hour_coupon_flag = '2', 1, 0) AS hour_coupon_flag,
				DATEDIFF(SUBSTR(end_tm, 0, 10), SUBSTR(start_tm, 0, 10)) + 1 AS valid_days, --优惠券有效时间
				MONTH(SUBSTR(start_tm, 0, 10)) AS month, --优惠券有效期开始月份
				user_tag_cd,
				biz_tag_cd
			FROM
				(
					SELECT
						batch_id,
						cps_name,
						cps_face_value,
						consume_lim,
						batch_cps_total_qtty,
						activity_key, --批次活动key
						valid_flag, --批次有效标志, 0-否 1-是
						allow_overlap, --是否允许叠加, 1-不允许（默认）2-允许
						cps_cate_cd, --优惠券种类, 1-限品类 2-限店铺（POP）3-店铺限商品券（POP店铺商品）
						coupon_style, --优惠券分类, 0-京东券 3-折扣券（满折券，仅cps_type_cd=1时有） 28-每满减券
						source_type, --来源类型 1-jd（默认）3-pop 4-三全（自营店铺化）
						limit_rule_type, --限品类规则类型 -1-未定义（默认） 5-店铺白+sku白+sku黑 组合 6-分类白+店铺黑+sku黑 组合
						limit_area_type, --限区域类型 -1-未定义（默认） 1-仅限某些区域可以使用 2-除某些区域以外可以使用
						limit_organization, --限制的体系 1-全部（兼容老数据）（默认）2-京东商城
						user_tag_cd, --用户标签代码 https://cf.jd.com/pages/viewpage.action?pageId=133241210
						biz_tag_cd, --业务标签代码 https://cf.jd.com/pages/viewpage.action?pageId=133241210
						hour_coupon_flag, --小时券标记 1-普通券（默认）2-小时券
						CASE
							WHEN val_time_limit IN(0, 1, 8)
							THEN SUBSTR(activity_start_tm, 0, 10)
							WHEN val_time_limit = 5
							THEN SUBSTR(valid_start_tm, 0, 10)
							WHEN val_time_limit = 6
							THEN SUBSTR(DATE_ADD(activity_start_tm, delay_days), 0, 10)
							ELSE SUBSTR(valid_start_tm, 0, 10)
						END AS start_tm, --优惠券有效期开始时间
						CASE
							WHEN val_time_limit IN(0, 5, 8)
							THEN SUBSTR(valid_end_tm, 0, 10)
							WHEN val_time_limit = 1
							THEN SUBSTR(DATE_ADD(activity_start_tm, valid_days), 0, 10)
							WHEN val_time_limit = 6
							THEN SUBSTR(DATE_ADD(activity_start_tm, delay_days + valid_days), 0, 10)
							ELSE SUBSTR(valid_end_tm, 0, 10)
						END AS end_tm --优惠券有效期结束时间
					FROM
						gdm.gdm_m07_cps_batch_da
					WHERE
						dt = '{dt}'
						--AND check_status_cd = 3
						--AND valid_flag = 1
						--AND consume_lim > 0
						--AND SUBSTR(activity_end_tm, 0, 10) <= '{dt}'
						--AND SUBSTR(activity_start_tm, 0, 10) >= date_sub('{dt}', 365)
						--AND cps_type_cd = 1 --优惠券类型, 1-东券（满减券和折扣券）：coupon_style值为0,3,28；
						--AND coupon_style IN(0, 3, 28)
						--AND cps_cate_cd IN(1)
						--AND allow_overlap = 1
						--AND source_type IN(1, 3)
						--AND limit_rule_type IN(5)
						--AND limit_area_type IN( - 1)
						--AND limit_organization IN(1, 2)
						--AND cps_name NOT LIKE concat('%', '运费券', '%')
						--AND cps_name NOT LIKE concat('%', '福利券', '%')
						--AND cps_name NOT LIKE concat('福利券', '%')
						--AND cps_name NOT LIKE concat('%', '赔付', '%')
						--AND cps_name NOT LIKE concat('员工申请', '%')
						--AND cps_name NOT LIKE concat('申请京券', '%')
						--AND cps_name NOT LIKE concat('%', '京贴', '%')
						--AND batch_cps_total_qtty >= 100
				)
		)
		t1
	LEFT JOIN
		(
			SELECT
				activity_id, --活动编号
				activity_key, --批次活动key
				activity_type_cd, --活动优惠券领取限制类型代码 4-活动期间每天领取一张 5-活动期间领取一张 6-用户领多张 8-手机号+pin活动期间领取一张
				user_lv_cd, --用户级别代码 10000 不限 20000 普通会员 30000 PLUS会员（付费会员）92000 品牌会员
				open_flag, --公开标志 0-不开启 1-开启
				SPLIT(open_type_cd_scope, ',') AS open_type_cd_list,
				--公开类型代码范围 1-公开到单品页 2-公开到领券中心 3-公开到乡村电商推广 4-公开到联盟 5-公开到中台券池（此标示的优惠券可以用于中台券池投放
				--6-全网推广（此标示的优惠券可以用于所有投放场景）8-数坊智能投放（首购礼金模板）999-未知
				new_user_flag, --全站拉新标志 1-是 0-否
				cate_new_user_flag, --分类拉新标志 1-是 0-否
				shop_new_user_flag --店铺拉新标志 1-是 0-否
			FROM
				gdm.gdm_m07_cps_active_coupon_da
			WHERE
				dt = '{dt}'
				AND plat_type_cd_scope = - 1 --平台类型代码范围 -1-不限
				--AND user_lv_cd IN(10000, 20000, 30000, 92000)
		)
		t2
	ON
		t1.activity_key = t2.activity_key
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
		t3
	ON
		t1.batch_id = t3.batch_id
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
				END AS crowd_type,
				collect_list(channel_param) AS channel_param
			FROM
				fdm.fdm_active_coupon_coupon_active_rule_ext_chain
			WHERE
				dp = 'ACTIVE'
				AND channel_type IN('4200', '2005', '2006', '2007', '2008', '2009', '2007', '2045', '2050', '2070', '2060', '2080')
			GROUP BY
				role_id,
				channel_type
		)
		t4
	ON
		t2.activity_id = t4.role_id
    """.format(dt=execute_dt))

    udf_crowd = F.udf(crowd_type, IntegerType())
    coupon_base_df = coupon_base_df.withColumn("crowd_type", udf_crowd('cps_name', 'crowd_type')).fillna(0).distinct()

    coupon_base_df.registerTempTable('coupon_base')

    spark.sql("""
        INSERT OVERWRITE TABLE adm.adm_cps_base_feature PARTITION(dt='{dt}')
            SELECT
                batch_id,cps_name,cps_face_value,consume_lim,batch_cps_total_qtty,start_tm,end_tm,valid_days,month,
                valid_flag,allow_overlap,cps_cate_cd,coupon_style,source_type,limit_rule_type,limit_area_type,
                limit_organization,user_tag_cd,biz_tag_cd,activity_type_cd,user_lv_cd,open_flag,hour_coupon_flag,
                new_user_flag,cate_new_user_flag,shop_new_user_flag,shufang_flag,crowd_type,shangxiang_flag,
                open_type_1_2,open_type_1_3,open_type_1_4,open_type_1_5,open_type_1_6,open_type_1_7,open_type_1_8,
                open_type_1_999,open_type_0_1,open_type_0_999
            FROM coupon_base
        """.format(dt=execute_dt))

    print(LOG_PREFIX, '写adm_cps_base_feature表完成')
