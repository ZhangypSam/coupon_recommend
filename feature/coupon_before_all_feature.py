#!/usr/bin/env python3
# coding: utf-8

import argparse
import os

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window

os.environ['PYSPARK_PYTHON'] = "/usr/local/anaconda3/bin/python3.6"
os.environ['PYSPARK_DRIVER_PYTHON'] = "/usr/local/anaconda3/bin/python3.6"

"""
CREATE EXTERNAL TABLE IF NOT EXISTS adm.adm_cps_bef_all_feat(
batch_id string COMMENT '批次id',
flag string COMMENT '券范围：after-投后，before-投前，yhzz-用增',
month int COMMENT '优惠券开始月份',
cps_cate_cd int COMMENT '优惠券种类',
coupon_style int COMMENT '优惠券分类',
source_type int COMMENT '来源类型',
limit_rule_type int COMMENT '限品类规则类型',
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
open_type_0_999 int COMMENT '不公开未知',
cps_face_value double COMMENT '面额',
consume_lim double COMMENT '消费限额',
batch_cps_total_qtty double COMMENT '该批次优惠券数量',
valid_days double COMMENT '有效期天数',
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
cps_diff_real_ratio_mean_30d double COMMENT '近30天平均用券到手价极差',
shelve_days double COMMENT '上架天数',
pv_cvr_1d double COMMENT '1天sku pv的cvr',
uv_cvr_1d double COMMENT '1天sku uv的cvr',
pv_cvr_7d double COMMENT '7天sku pv的cvr',
uv_cvr_7d double COMMENT '7天sku uv的cvr',
pv_cvr_30d double COMMENT '30天sku pv的cvr',
uv_cvr_30d double COMMENT '30天sku uv的cvr',
sku_quality_score double COMMENT '商品质量分',
sku_good_comment_rate double COMMENT '商品好评率',
sku_comment_num double COMMENT '商品评论数',
sku_follow_1d_cnt double COMMENT 'sku下1日关注数',
sku_follow_7d_cnt double COMMENT 'sku下7日关注数',
sku_follow_30d_cnt double COMMENT 'sku下30日关注数',
sku_ord_7d_pnum double COMMENT 'sku下7日下单人数',
sku_ord_30d_pnum double COMMENT 'sku下30日下单人数',
sku_comm_30d_cnt double COMMENT 'sku下30日评价数',
sku_good_comm_30d_cnt double COMMENT 'sku下30日好评数',
sku_good_comm_ratio double COMMENT 'sku下好评率',
sku_exp_1d_cnt double COMMENT 'sku下1日曝光量',
sku_exp_3d_cnt double COMMENT 'sku下3日曝光量',
sku_exp_ads_cnt double COMMENT 'sku下广告曝光量',
sku_exp_rec_cnt double COMMENT 'sku下推荐曝光量',
sku_exp_ser_cnt double COMMENT 'sku下搜索曝光量',
sku_exp_act_cnt double COMMENT 'sku下活动曝光量',
sku_ord_unit_price double COMMENT 'sku下30日下单件单价',
sku_ord_user_price double COMMENT 'sku下30日下单客单价',
sku_pv_cvr_7d double COMMENT 'sku下7日pv转化率',
sku_uv_cvr_7d double COMMENT 'sku下7日uv转化率',
sku_ctr_3d double COMMENT 'sku下3日点击率',
sku_ctr_ads double COMMENT 'sku下广告点击率',
sku_ctr_rec double COMMENT 'sku下推荐点击率',
sku_ctr_ser double COMMENT 'sku下搜索点击率',
sku_ctr_act double COMMENT 'sku下活动点击率',
sku_prc_quantile double COMMENT 'sku价格百分位',
sku_ord_quantile double COMMENT 'sku订单百分位',
sku_view_quantile double COMMENT 'sku浏览百分位',
sku_cart_quantile double COMMENT 'sku加购百分位',
view_cnt90d double COMMENT '过去 90 天总浏览次数',
view_cnt60d double COMMENT '过去 60 天总浏览次数',
view_cnt30d double COMMENT '过去 30 天总浏览次数',
view_cnt14d double COMMENT '过去 14 天总浏览次数',
view_cnt7d double COMMENT '过去 7 天总浏览次数',
view_cnt3d double COMMENT '过去 3 天总浏览次数',
view_cnt1d double COMMENT '过去 1 天总浏览次数',
avg_view_count double COMMENT '过去 90 天日均浏览次数',
avg_view_uv double COMMENT '过去 90 天日均 UV',
search_click15d double COMMENT '过去 15 天搜索点击次数',
search_click_cnt6d double COMMENT '过去 6 天搜索点击次数',
search_click_cnt3d double COMMENT '过去 3 天搜索点击次数',
cart_cnt90d double COMMENT '过去 90 天加购次数',
cart_cnt60d double COMMENT '过去 60 天加购次数',
cart_cnt30d double COMMENT '过去 30 天加购次数',
cart_cnt14d double COMMENT '过去 14 天加购次数',
cart_cnt7d double COMMENT '过去 7 天加购次数',
cart_cnt3d double COMMENT '过去 3 天加购次数',
cart_cnt1d double COMMENT '过去 1 天加购次数',
avg_cart_count double COMMENT '过去 90 天日均加购次数',
buy_cnt90d double COMMENT '过去 90 天订单量',
buy_cnt60d double COMMENT '过去 60 天订单量',
buy_cnt30d double COMMENT '过去 30 天订单量',
buy_cnt14d double COMMENT '过去 14 天订单量',
buy_cnt7d double COMMENT '过去 7 天订单量',
buy_cnt3d double COMMENT '过去 3 天订单量',
buy_cnt1d double COMMENT '过去 1 天订单量',
avg_buy_count double COMMENT '过去 90 天日均订单量',
gmv_90d double COMMENT '过去 90 天 GMV',
gmv_60d double COMMENT '过去 60 天 GMV',
gmv_30d double COMMENT '过去 30 天 GMV',
gmv_14d double COMMENT '过去 14 天 GMV',
gmv_7d double COMMENT '过去 7 天 GMV',
gmv_3d double COMMENT '过去 3 天 GMV',
gmv_1d double COMMENT '过去 1 天 GMV',
avg_gmv double COMMENT '过去 90 天日均 GMV',
cart_prob_90d double COMMENT '过去 90 天浏览->加购概率',
cart_prob_60d double COMMENT '过去 60 天浏览->加购概率',
cart_prob_30d double COMMENT '过去 30 天浏览->加购概率',
cart_prob_14d double COMMENT '过去 14 天浏览->加购概率',
cart_prob_7d double COMMENT '过去 7 天浏览->加购概率',
buy_prob_90d double COMMENT '过去 90 天浏览->购买概率',
buy_prob_60d double COMMENT '过去 60 天浏览->购买概率',
buy_prob_30d double COMMENT '过去 30 天浏览->购买概率',
buy_prob_14d double COMMENT '过去 14 天浏览->购买概率',
buy_prob_7d double COMMENT '过去 7 天浏览->购买概率',
gpm3d double COMMENT '过去 3 天 GPM',
gpm2d double COMMENT '过去 2 天 GPM',
gpm1d double COMMENT '过去 1 天 GPM',
cart_uv_90d double COMMENT '过去 90 天加购UV',
cart_uv_60d double COMMENT '过去 60 天加购UV',
cart_uv_30d double COMMENT '过去 30 天加购UV',
cart_uv_15d double COMMENT '过去 15 天加购UV',
cart_uv_7d double COMMENT '过去 7 天加购UV',
buy_uv_90d double COMMENT '过去 90 天下单UV',
buy_uv_60d double COMMENT '过去 60 天下单UV',
buy_uv_30d double COMMENT '过去 30 天下单UV',
buy_uv_15d double COMMENT '过去 15 天下单UV',
buy_uv_7d double COMMENT '过去 7 天下单UV',
has_coupon_use_days double COMMENT '过去 90 天使用优惠券天数',
coupon_use_cnt90d double COMMENT '过去 90 天使用优惠券次数',
coupon_use_cnt60d double COMMENT '过去 60 天使用优惠券次数',
coupon_use_cnt30d double COMMENT '过去 30 天使用优惠券次数',
coupon_use_cnt15d double COMMENT '过去 15 天使用优惠券次数',
coupon_use_cnt7d double COMMENT '过去 7 天使用优惠券次数',
avg_coupon_use_count double COMMENT '过去 90 天日均使用优惠券次数',
avg_coupon_use_uv double COMMENT '过去 90 天日均使用优惠券UV',
min_pay_price_30d double COMMENT '过去 30 天最低下单价',
min_pay_price_15d double COMMENT '过去 15 天最低下单价',
min_pay_price_7d double COMMENT '过去 7 天最低下单价',
max_pay_price_30d double COMMENT '过去 30 天最高下单价',
max_pay_price_15d double COMMENT '过去 15 天最高下单价',
max_pay_price_7d double COMMENT '过去 7 天最高下单价',
current_mid_pay_price double COMMENT '最近 1 天下单价中位数',
coupon_use_prob_90d double COMMENT '过去 90 天使用优惠券次数',
coupon_use_prob_60d double COMMENT '过去 60 天使用优惠券次数',
coupon_use_prob_30d double COMMENT '过去 30 天使用优惠券次数',
coupon_use_prob_15d double COMMENT '过去 15 天使用优惠券次数',
coupon_use_prob_7d double COMMENT '过去 7 天使用优惠券次数',
his_cps_cnt double COMMENT '历史券数量',
his_hour_coupon_cnt double COMMENT '历史小时券数量',
his_shufang_cnt double COMMENT '历史数坊券数量',
his_shangxiang_cnt double COMMENT '历史商详券数量',
his_new_crowd_cnt double COMMENT '历史拉新券数量',
his_second_crowd_cnt double COMMENT '历史一转二券数量',
his_old_crowd_cnt double COMMENT '历史复购券数量',
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
his_sx_90p_discount_rt double COMMENT '历史商详券折扣率90%分位点',
his_pos_consume_lim double COMMENT '该券在历史券限额分位位置',
his_pos_cps_face_value double COMMENT '该券在历史券面额分位位置',
his_pos_discount_rt double COMMENT '该券在历史券折扣率分位位置',
his_sx_pos_consume_lim double COMMENT '该券在历史商详券限额分位位置',
his_sx_pos_cps_face_value double COMMENT '该券在历史商详券面额分位位置',
his_sx_pos_discount_rt double COMMENT '该券在历史商详券折扣率分位位置',
pal_cps_cnt double COMMENT '平行券数量',
pal_hour_coupon_cnt double COMMENT '平行小时券数量',
pal_shufang_cnt double COMMENT '平行数坊券数量',
pal_shangxiang_cnt double COMMENT '平行商详券数量',
pal_new_crowd_cnt double COMMENT '平行拉新券数量',
pal_second_crowd_cnt double COMMENT '平行一转二券数量',
pal_old_crowd_cnt double COMMENT '平行复购券数量',
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
pal_sx_90p_discount_rt double COMMENT '平行商详券折扣率90%分位点',
pal_pos_consume_lim double COMMENT '该券在平行券限额分位位置',
pal_pos_cps_face_value double COMMENT '该券在平行券面额分位位置',
pal_pos_discount_rt double COMMENT '该券在平行券折扣率分位位置',
pal_sx_pos_consume_lim double COMMENT '该券在平行商详券限额分位位置',
pal_sx_pos_cps_face_value double COMMENT '该券在平行商详券面额分位位置',
pal_sx_pos_discount_rt double COMMENT '该券在平行商详券折扣率分位位置',
cvr double COMMENT 'CVR',
use_num bigint COMMENT '用券用户数',
roi double COMMENT 'ROI',
sale_amt double COMMENT 'GMV',
ord_qtty bigint COMMENT '销量')
COMMENT '投前优惠券特征表'
PARTITIONED BY (
  dt string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://ns9/user/mart_scr/adm.db/adm_cps_bef_all_feat';
"""

LOG_PREFIX = "[■■□□ ■□■■ □■■□]"

spark = SparkSession \
    .builder \
    .appName("coupon_before_model") \
    .enableHiveSupport() \
    .config("spark.executor.instances", "400") \
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

sparse_cols = [
    'month', 'cps_cate_cd', 'coupon_style', 'source_type', 'limit_rule_type', 'limit_organization',
    'user_tag_cd', 'biz_tag_cd', 'activity_type_cd', 'user_lv_cd', 'open_flag', 'hour_coupon_flag',
    'new_user_flag', 'cate_new_user_flag', 'shop_new_user_flag', 'shufang_flag', 'crowd_type',
    'shangxiang_flag', 'open_type_1_2', 'open_type_1_3', 'open_type_1_4', 'open_type_1_5', 'open_type_1_6',
    'open_type_1_7', 'open_type_1_8', 'open_type_1_999', 'open_type_0_1', 'open_type_0_999']

cps_dense_cols = ['cps_face_value', 'consume_lim', 'batch_cps_total_qtty', 'valid_days']

dense_cols = [
    'cps_face_value', 'consume_lim', 'batch_cps_total_qtty', 'valid_days', 'jd_prc', 'theta',
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

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--execute_dt', type=str, default='', help='date of script execution')

    args = parser.parse_args()
    execute_dt = args.execute_dt
    print(LOG_PREFIX, 'execute_dt=%s' % execute_dt)

    ##############################
    ########## 标签及先验特征开发
    ##############################
    label_after = spark.sql("""
    SELECT
    	t1.batch_id,
    	t2.cvr,
    	t1.use_num,
    	t1.roi,
    	t1.sale_amt,
    	t1.ord_qtty,
    	t1.feature_dt,
    	t1.flag
    FROM
    	(
    		SELECT
    			batch_id,
    			IF(get_use_cvr > 1, 1, get_use_cvr) AS cvr,
    			use_num,
    			IF(roi < 0, 0, roi) AS roi,
    			IF(valid_sale_amt < 0, 0, valid_sale_amt) AS sale_amt,
    			IF(valid_parent_ord_qtty < 0, 0, valid_parent_ord_qtty) AS ord_qtty,
    			valid_start_dt AS feature_dt,
    			'after' AS flag
    		FROM
    			app.app_coupon_score_label_sum
    		WHERE
    			dt <= '{dt}'
    			AND dt >= date_sub('{dt}', 62)
    			AND sf_type = 0
    			AND valid_end_type = 1
    			AND valid_start_dt >= date_sub('{dt}', 62)
    	)
    	t1
    LEFT JOIN
    	(
    		SELECT
    			batch_id,
    			smooth_avg_bayes_cvr cvr
    		FROM
    			app.app_cps_deliver_aft_quality_score_v2
    		WHERE
    			dt = '{dt}'
    			AND sf_type = 0
    	)
    	t2
    ON
    	t1.batch_id = t2.batch_id
    """.format(dt=execute_dt)).sample(0.2)
    yhzz_cps = spark.sql("""
    SELECT
    	benefit_id as batch_id,
    	'yhzz' as flag
    FROM
    	app.app_yhzz_groot_discount_info_da
    WHERE
    	dt = '{dt}'
        AND benefit_id is not null
    	AND yn = 1
    	AND type = 1
    GROUP BY
    	benefit_id
    """.format(dt=execute_dt))
    label_before = spark.sql("""
    SELECT
    	batch_id,
    	IF(get_use_cvr > 1, 1, get_use_cvr) AS cvr,
    	use_num,
    	IF(roi < 0, 0, roi) AS roi,
    	IF(valid_sale_amt < 0, 0, valid_sale_amt) AS sale_amt,
    	IF(valid_parent_ord_qtty < 0, 0, valid_parent_ord_qtty) AS ord_qtty,
        '{dt}' AS feature_dt
    FROM
    	app.app_coupon_score_label_sum
    WHERE
    	dt = '{dt}'
    	AND dt >= date_sub('{dt}', 62)
    	AND sf_type = 0
    	AND valid_start_dt >= '{dt}'
    """.format(dt=execute_dt)).join(yhzz_cps, 'batch_id', 'full').fillna(
        {'flag': 'before', 'feature_dt': execute_dt}).fillna(0)

    label_all = label_after.union(label_before).where("batch_id != '-1'").cache()

    label_all.registerTempTable('label_all')

    ##############################
    ########## 先验特征
    ##############################
    prior_feature = spark.sql("""
    SELECT
    	batch_id,
    	cate_use_cnt,
    	crowd_use_cnt,
    	shangxiang_use_cnt,
    	all_use_cnt,
    	cate_get_cnt,
    	crowd_get_cnt,
    	shangxiang_get_cnt,
    	all_get_cnt,
    	all_cvr,
    	p_cate_ord,
    	p_crowd_ord,
    	p_shangxiang_ord,
    	p_cate,
    	p_crowd,
    	p_shangxiang,
    	bayes_cate_cvr,
    	bayes_shangxiang_cvr,
    	bayes_cvr,
    	cate_crowd_shangxiang_get_mean,
    	dt
    FROM
    	app.app_cps_deliver_aft_quality_score_v2
    WHERE
    	dt <= '{dt}'
    	AND dt >= date_sub('{dt}', 63)
    	AND sf_type = 0
    """.format(dt=execute_dt))

    ##############################
    ########## 券特征
    ##############################
    coupon_feature = spark.sql("""
    SELECT
    	t2.*,
    	t1.flag
    FROM
    	(
    		SELECT * FROM label_all
    	)
    	t1
    LEFT JOIN
    	(
    		SELECT * FROM adm.adm_cps_base_feature WHERE dt = '{dt}'
    	)
    	t2
    ON
    	t1.batch_id = t2.batch_id
    """.format(dt=execute_dt)).fillna(0)

    ##############################
    ########## 券绑定SKU
    ##############################
    sku_bundle = spark.sql("""
    SELECT
    	t1.batch_id,
    	t2.item_sku_id,
    	t1.feature_dt,
    	t1.flag
    FROM
    	(
    		SELECT batch_id, flag, feature_dt FROM label_all
    	)
    	t1
    LEFT JOIN
    	(
    		SELECT
    			batch_id,
    			sku_id item_sku_id,
    			dt
    		FROM
    			app.app_coupon_score_batch_sku
    		WHERE
    			dt <= '{dt}'
    			AND dt >= date_sub('{dt}', 63)
    	)
    	t2
    ON
    	t1.batch_id = t2.batch_id
    	AND t1.feature_dt = t2.dt
    """.format(dt=execute_dt)) \
        .withColumn('rand', F.rand(2022)) \
        .withColumn('rate', F.lit(2000) / F.count('item_sku_id').over(Window.partitionBy('batch_id'))) \
        .where('rand < rate').drop('rand', 'rate').cache()

    sku_bundle.registerTempTable('sku_bundle')

    ##############################
    ########## SKU交易特征
    ##############################
    sku_ord_feature = spark.sql("""
    SELECT
    	t1.batch_id,
    	t2.*,
    	t3.shelve_days,
    	t3.pv_cvr_1d,
    	t3.uv_cvr_1d,
    	t3.pv_cvr_7d,
    	t3.uv_cvr_7d,
    	t3.pv_cvr_30d,
    	t3.uv_cvr_30d,
    	t3.sku_quality_score,
    	t3.sku_good_comment_rate,
    	t3.sku_comment_num,
    	t3.sku_follow_1d_cnt,
    	t3.sku_follow_7d_cnt,
    	t3.sku_follow_30d_cnt,
    	t3.sku_ord_7d_pnum,
    	t3.sku_ord_30d_pnum,
    	t3.sku_comm_30d_cnt,
    	t3.sku_good_comm_30d_cnt,
    	t3.sku_good_comm_ratio,
    	t3.sku_exp_1d_cnt,
    	t3.sku_exp_3d_cnt,
    	t3.sku_exp_ads_cnt,
    	t3.sku_exp_rec_cnt,
    	t3.sku_exp_ser_cnt,
    	t3.sku_exp_act_cnt,
    	t3.sku_ord_unit_price,
    	t3.sku_ord_user_price,
    	t3.sku_pv_cvr_7d,
    	t3.sku_uv_cvr_7d,
    	t3.sku_ctr_3d,
    	t3.sku_ctr_ads,
    	t3.sku_ctr_rec,
    	t3.sku_ctr_ser,
    	t3.sku_ctr_act,
    	t3.sku_prc_quantile,
    	t3.sku_ord_quantile,
    	t3.sku_view_quantile,
    	t3.sku_cart_quantile,
    	t3.view_cnt90d,
    	t3.view_cnt60d,
    	t3.view_cnt30d,
    	t3.view_cnt14d,
    	t3.view_cnt7d,
    	t3.view_cnt3d,
    	t3.view_cnt1d,
    	t3.avg_view_count,
    	t3.avg_view_uv,
    	t3.search_click15d,
    	t3.search_click_cnt6d,
    	t3.search_click_cnt3d,
    	t3.cart_cnt90d,
    	t3.cart_cnt60d,
    	t3.cart_cnt30d,
    	t3.cart_cnt14d,
    	t3.cart_cnt7d,
    	t3.cart_cnt3d,
    	t3.cart_cnt1d,
    	t3.avg_cart_count,
    	t3.buy_cnt90d,
    	t3.buy_cnt60d,
    	t3.buy_cnt30d,
    	t3.buy_cnt14d,
    	t3.buy_cnt7d,
    	t3.buy_cnt3d,
    	t3.buy_cnt1d,
    	t3.avg_buy_count,
    	t3.gmv_90d,
    	t3.gmv_60d,
    	t3.gmv_30d,
    	t3.gmv_14d,
    	t3.gmv_7d,
    	t3.gmv_3d,
    	t3.gmv_1d,
    	t3.avg_gmv,
    	t3.cart_prob_90d,
    	t3.cart_prob_60d,
    	t3.cart_prob_30d,
    	t3.cart_prob_14d,
    	t3.cart_prob_7d,
    	t3.buy_prob_90d,
    	t3.buy_prob_60d,
    	t3.buy_prob_30d,
    	t3.buy_prob_14d,
    	t3.buy_prob_7d,
    	t3.gpm3d,
    	t3.gpm2d,
    	t3.gpm1d,
    	t3.cart_uv_90d,
    	t3.cart_uv_60d,
    	t3.cart_uv_30d,
    	t3.cart_uv_15d,
    	t3.cart_uv_7d,
    	t3.buy_uv_90d,
    	t3.buy_uv_60d,
    	t3.buy_uv_30d,
    	t3.buy_uv_15d,
    	t3.buy_uv_7d,
    	t3.has_coupon_use_days,
    	t3.coupon_use_cnt90d,
    	t3.coupon_use_cnt60d,
    	t3.coupon_use_cnt30d,
    	t3.coupon_use_cnt15d,
    	t3.coupon_use_cnt7d,
    	t3.avg_coupon_use_count,
    	t3.avg_coupon_use_uv,
    	t3.min_pay_price_30d,
    	t3.min_pay_price_15d,
    	t3.min_pay_price_7d,
    	t3.max_pay_price_30d,
    	t3.max_pay_price_15d,
    	t3.max_pay_price_7d,
    	t3.current_mid_pay_price,
    	t3.coupon_use_prob_90d,
    	t3.coupon_use_prob_60d,
    	t3.coupon_use_prob_30d,
    	t3.coupon_use_prob_15d,
    	t3.coupon_use_prob_7d,
    	t1.feature_dt,
    	t1.flag
    FROM
    	(
    		SELECT batch_id, item_sku_id, flag, feature_dt FROM sku_bundle
    	)
    	t1
    LEFT JOIN
    	(
    		SELECT
    			*
    		FROM
    			adm.adm_sku_ord_feature
    		WHERE
    			dt <= '{dt}'
    			AND dt >= date_sub('{dt}', 63)
    	)
    	t2
    ON
    	t1.item_sku_id = t2.item_sku_id
    	AND t1.feature_dt = t2.dt
    LEFT JOIN
    	(
    		SELECT
    			*
    		FROM
    			app.app_coupon_quality_score_sku_feat
    		WHERE
    			dt <= '{dt}'
    			AND dt >= date_sub('{dt}', 63)
    	)
    	t3
    ON
    	t1.item_sku_id = t3.item_sku_id
    	AND t1.feature_dt = t3.dt
    """.format(dt=execute_dt)).fillna(0).cache()

    ##############################
    ########## SKU历史券特征
    ##############################
    history_coupon = spark.sql("""
    SELECT
    	t2.*,
    	t4.*,
    	t3.jd_prc,
    	t1.feature_dt,
    	t1.flag
    FROM
    	(
    		SELECT batch_id, item_sku_id, flag, feature_dt FROM sku_bundle
    	)
    	t1
    LEFT JOIN
    	(
    		SELECT
    			batch_id,
    			coupon_style,
    			consume_lim,
    			cps_face_value
    		FROM
    			adm.adm_cps_base_feature
    		WHERE
    			dt = '{dt}'
    	)
    	t2
    ON
    	t1.batch_id = t2.batch_id
    LEFT JOIN
    	(
    		SELECT item_sku_id, jd_prc FROM adm.adm_sku_ord_feature WHERE dt = '{dt}'
    	)
    	t3
    ON
    	t1.item_sku_id = t3.item_sku_id
    LEFT JOIN
    	(
    		SELECT
    			*
    		FROM
    			adm.adm_sku_cps_his_feature
    		WHERE
    			dt <= '{dt}'
    			AND dt >= date_sub('{dt}', 63)
    	)
    	t4
    ON
    	t1.item_sku_id = t4.item_sku_id
    	AND t1.feature_dt = t4.dt
    """.format(dt=execute_dt))

    coupon_history_interact = history_coupon \
        .withColumn('discount_rt', F.when(F.col('coupon_style') == F.lit(0),
                                          F.col('cps_face_value') / F.greatest('jd_prc', 'consume_lim')) \
                    .otherwise(F.col('cps_face_value') / F.col('consume_lim'))
                    ) \
        .withColumn('his_pos_consume_lim',
                    F.when(F.col('consume_lim') < F.col('his_10p_consume_lim'), F.lit(0)) \
                    .when(F.col('consume_lim') < F.col('his_20p_consume_lim'), F.lit(1)) \
                    .when(F.col('consume_lim') < F.col('his_30p_consume_lim'), F.lit(2)) \
                    .when(F.col('consume_lim') < F.col('his_40p_consume_lim'), F.lit(3)) \
                    .when(F.col('consume_lim') < F.col('his_50p_consume_lim'), F.lit(4)) \
                    .when(F.col('consume_lim') < F.col('his_60p_consume_lim'), F.lit(5)) \
                    .when(F.col('consume_lim') < F.col('his_70p_consume_lim'), F.lit(6)) \
                    .when(F.col('consume_lim') < F.col('his_80p_consume_lim'), F.lit(7)) \
                    .when(F.col('consume_lim') < F.col('his_90p_consume_lim'), F.lit(8)) \
                    .otherwise(F.lit(9))
                    ) \
        .withColumn('his_pos_cps_face_value',
                    F.when(F.col('cps_face_value') < F.col('his_10p_cps_face_value'), F.lit(0)) \
                    .when(F.col('cps_face_value') < F.col('his_20p_cps_face_value'), F.lit(1)) \
                    .when(F.col('cps_face_value') < F.col('his_30p_cps_face_value'), F.lit(2)) \
                    .when(F.col('cps_face_value') < F.col('his_40p_cps_face_value'), F.lit(3)) \
                    .when(F.col('cps_face_value') < F.col('his_50p_cps_face_value'), F.lit(4)) \
                    .when(F.col('cps_face_value') < F.col('his_60p_cps_face_value'), F.lit(5)) \
                    .when(F.col('cps_face_value') < F.col('his_70p_cps_face_value'), F.lit(6)) \
                    .when(F.col('cps_face_value') < F.col('his_80p_cps_face_value'), F.lit(7)) \
                    .when(F.col('cps_face_value') < F.col('his_90p_cps_face_value'), F.lit(8)) \
                    .otherwise(F.lit(9))
                    ) \
        .withColumn('his_pos_discount_rt',
                    F.when(F.col('discount_rt') < F.col('his_10p_discount_rt'), F.lit(0)) \
                    .when(F.col('discount_rt') < F.col('his_20p_discount_rt'), F.lit(1)) \
                    .when(F.col('discount_rt') < F.col('his_30p_discount_rt'), F.lit(2)) \
                    .when(F.col('discount_rt') < F.col('his_40p_discount_rt'), F.lit(3)) \
                    .when(F.col('discount_rt') < F.col('his_50p_discount_rt'), F.lit(4)) \
                    .when(F.col('discount_rt') < F.col('his_60p_discount_rt'), F.lit(5)) \
                    .when(F.col('discount_rt') < F.col('his_70p_discount_rt'), F.lit(6)) \
                    .when(F.col('discount_rt') < F.col('his_80p_discount_rt'), F.lit(7)) \
                    .when(F.col('discount_rt') < F.col('his_90p_discount_rt'), F.lit(8)) \
                    .otherwise(F.lit(9))
                    ) \
        .withColumn('his_sx_pos_consume_lim',
                    F.when(F.col('consume_lim') < F.col('his_sx_10p_consume_lim'), F.lit(0)) \
                    .when(F.col('consume_lim') < F.col('his_sx_20p_consume_lim'), F.lit(1)) \
                    .when(F.col('consume_lim') < F.col('his_sx_30p_consume_lim'), F.lit(2)) \
                    .when(F.col('consume_lim') < F.col('his_sx_40p_consume_lim'), F.lit(3)) \
                    .when(F.col('consume_lim') < F.col('his_sx_50p_consume_lim'), F.lit(4)) \
                    .when(F.col('consume_lim') < F.col('his_sx_60p_consume_lim'), F.lit(5)) \
                    .when(F.col('consume_lim') < F.col('his_sx_70p_consume_lim'), F.lit(6)) \
                    .when(F.col('consume_lim') < F.col('his_sx_80p_consume_lim'), F.lit(7)) \
                    .when(F.col('consume_lim') < F.col('his_sx_90p_consume_lim'), F.lit(8)) \
                    .otherwise(F.lit(9))
                    ) \
        .withColumn('his_sx_pos_cps_face_value',
                    F.when(F.col('cps_face_value') < F.col('his_sx_10p_cps_face_value'), F.lit(0)) \
                    .when(F.col('cps_face_value') < F.col('his_sx_20p_cps_face_value'), F.lit(1)) \
                    .when(F.col('cps_face_value') < F.col('his_sx_30p_cps_face_value'), F.lit(2)) \
                    .when(F.col('cps_face_value') < F.col('his_sx_40p_cps_face_value'), F.lit(3)) \
                    .when(F.col('cps_face_value') < F.col('his_sx_50p_cps_face_value'), F.lit(4)) \
                    .when(F.col('cps_face_value') < F.col('his_sx_60p_cps_face_value'), F.lit(5)) \
                    .when(F.col('cps_face_value') < F.col('his_sx_70p_cps_face_value'), F.lit(6)) \
                    .when(F.col('cps_face_value') < F.col('his_sx_80p_cps_face_value'), F.lit(7)) \
                    .when(F.col('cps_face_value') < F.col('his_sx_90p_cps_face_value'), F.lit(8)) \
                    .otherwise(F.lit(9))
                    ) \
        .withColumn('his_sx_pos_discount_rt',
                    F.when(F.col('discount_rt') < F.col('his_sx_10p_discount_rt'), F.lit(0)) \
                    .when(F.col('discount_rt') < F.col('his_sx_20p_discount_rt'), F.lit(1)) \
                    .when(F.col('discount_rt') < F.col('his_sx_30p_discount_rt'), F.lit(2)) \
                    .when(F.col('discount_rt') < F.col('his_sx_40p_discount_rt'), F.lit(3)) \
                    .when(F.col('discount_rt') < F.col('his_sx_50p_discount_rt'), F.lit(4)) \
                    .when(F.col('discount_rt') < F.col('his_sx_60p_discount_rt'), F.lit(5)) \
                    .when(F.col('discount_rt') < F.col('his_sx_70p_discount_rt'), F.lit(6)) \
                    .when(F.col('discount_rt') < F.col('his_sx_80p_discount_rt'), F.lit(7)) \
                    .when(F.col('discount_rt') < F.col('his_sx_90p_discount_rt'), F.lit(8)) \
                    .otherwise(F.lit(9))
                    ).fillna(0).drop('jd_prc')

    ##############################
    ########## SKU平行券特征
    ##############################
    parallel_coupon = spark.sql("""
    SELECT
    	t2.*,
    	t4.*,
    	t3.jd_prc,
    	t1.feature_dt,
    	t1.flag
    FROM
    	(
    		SELECT batch_id, item_sku_id, flag, feature_dt FROM sku_bundle
    	)
    	t1
    LEFT JOIN
    	(
    		SELECT
    			batch_id,
    			coupon_style,
    			consume_lim,
    			cps_face_value
    		FROM
    			adm.adm_cps_base_feature
    		WHERE
    			dt = '{dt}'
    	)
    	t2
    ON
    	t1.batch_id = t2.batch_id
    LEFT JOIN
    	(
    		SELECT item_sku_id, jd_prc FROM adm.adm_sku_ord_feature WHERE dt = '{dt}'
    	)
    	t3
    ON
    	t1.item_sku_id = t3.item_sku_id
    LEFT JOIN
    	(
    		SELECT
    			*
    		FROM
    			adm.adm_sku_cps_pal_feature
    		WHERE
    			dt <= '{dt}'
    			AND dt >= date_sub('{dt}', 63)
    	)
    	t4
    ON
    	t1.item_sku_id = t4.item_sku_id
    	AND t1.feature_dt = t4.dt
    """.format(dt=execute_dt))

    coupon_parallel_interact = parallel_coupon \
        .withColumn('discount_rt', F.when(F.col('coupon_style') == F.lit(0),
                                          F.col('cps_face_value') / F.greatest('jd_prc', 'consume_lim')) \
                    .otherwise(F.col('cps_face_value') / F.col('consume_lim'))
                    ) \
        .withColumn('pal_pos_consume_lim',
                    F.when(F.col('consume_lim') < F.col('pal_10p_consume_lim'), F.lit(0)) \
                    .when(F.col('consume_lim') < F.col('pal_20p_consume_lim'), F.lit(1)) \
                    .when(F.col('consume_lim') < F.col('pal_30p_consume_lim'), F.lit(2)) \
                    .when(F.col('consume_lim') < F.col('pal_40p_consume_lim'), F.lit(3)) \
                    .when(F.col('consume_lim') < F.col('pal_50p_consume_lim'), F.lit(4)) \
                    .when(F.col('consume_lim') < F.col('pal_60p_consume_lim'), F.lit(5)) \
                    .when(F.col('consume_lim') < F.col('pal_70p_consume_lim'), F.lit(6)) \
                    .when(F.col('consume_lim') < F.col('pal_80p_consume_lim'), F.lit(7)) \
                    .when(F.col('consume_lim') < F.col('pal_90p_consume_lim'), F.lit(8)) \
                    .otherwise(F.lit(9))
                    ) \
        .withColumn('pal_pos_cps_face_value',
                    F.when(F.col('cps_face_value') < F.col('pal_10p_cps_face_value'), F.lit(0)) \
                    .when(F.col('cps_face_value') < F.col('pal_20p_cps_face_value'), F.lit(1)) \
                    .when(F.col('cps_face_value') < F.col('pal_30p_cps_face_value'), F.lit(2)) \
                    .when(F.col('cps_face_value') < F.col('pal_40p_cps_face_value'), F.lit(3)) \
                    .when(F.col('cps_face_value') < F.col('pal_50p_cps_face_value'), F.lit(4)) \
                    .when(F.col('cps_face_value') < F.col('pal_60p_cps_face_value'), F.lit(5)) \
                    .when(F.col('cps_face_value') < F.col('pal_70p_cps_face_value'), F.lit(6)) \
                    .when(F.col('cps_face_value') < F.col('pal_80p_cps_face_value'), F.lit(7)) \
                    .when(F.col('cps_face_value') < F.col('pal_90p_cps_face_value'), F.lit(8)) \
                    .otherwise(F.lit(9))
                    ) \
        .withColumn('pal_pos_discount_rt',
                    F.when(F.col('discount_rt') < F.col('pal_10p_discount_rt'), F.lit(0)) \
                    .when(F.col('discount_rt') < F.col('pal_20p_discount_rt'), F.lit(1)) \
                    .when(F.col('discount_rt') < F.col('pal_30p_discount_rt'), F.lit(2)) \
                    .when(F.col('discount_rt') < F.col('pal_40p_discount_rt'), F.lit(3)) \
                    .when(F.col('discount_rt') < F.col('pal_50p_discount_rt'), F.lit(4)) \
                    .when(F.col('discount_rt') < F.col('pal_60p_discount_rt'), F.lit(5)) \
                    .when(F.col('discount_rt') < F.col('pal_70p_discount_rt'), F.lit(6)) \
                    .when(F.col('discount_rt') < F.col('pal_80p_discount_rt'), F.lit(7)) \
                    .when(F.col('discount_rt') < F.col('pal_90p_discount_rt'), F.lit(8)) \
                    .otherwise(F.lit(9))
                    ) \
        .withColumn('pal_sx_pos_consume_lim',
                    F.when(F.col('consume_lim') < F.col('pal_sx_10p_consume_lim'), F.lit(0)) \
                    .when(F.col('consume_lim') < F.col('pal_sx_20p_consume_lim'), F.lit(1)) \
                    .when(F.col('consume_lim') < F.col('pal_sx_30p_consume_lim'), F.lit(2)) \
                    .when(F.col('consume_lim') < F.col('pal_sx_40p_consume_lim'), F.lit(3)) \
                    .when(F.col('consume_lim') < F.col('pal_sx_50p_consume_lim'), F.lit(4)) \
                    .when(F.col('consume_lim') < F.col('pal_sx_60p_consume_lim'), F.lit(5)) \
                    .when(F.col('consume_lim') < F.col('pal_sx_70p_consume_lim'), F.lit(6)) \
                    .when(F.col('consume_lim') < F.col('pal_sx_80p_consume_lim'), F.lit(7)) \
                    .when(F.col('consume_lim') < F.col('pal_sx_90p_consume_lim'), F.lit(8)) \
                    .otherwise(F.lit(9))
                    ) \
        .withColumn('pal_sx_pos_cps_face_value',
                    F.when(F.col('cps_face_value') < F.col('pal_sx_10p_cps_face_value'), F.lit(0)) \
                    .when(F.col('cps_face_value') < F.col('pal_sx_20p_cps_face_value'), F.lit(1)) \
                    .when(F.col('cps_face_value') < F.col('pal_sx_30p_cps_face_value'), F.lit(2)) \
                    .when(F.col('cps_face_value') < F.col('pal_sx_40p_cps_face_value'), F.lit(3)) \
                    .when(F.col('cps_face_value') < F.col('pal_sx_50p_cps_face_value'), F.lit(4)) \
                    .when(F.col('cps_face_value') < F.col('pal_sx_60p_cps_face_value'), F.lit(5)) \
                    .when(F.col('cps_face_value') < F.col('pal_sx_70p_cps_face_value'), F.lit(6)) \
                    .when(F.col('cps_face_value') < F.col('pal_sx_80p_cps_face_value'), F.lit(7)) \
                    .when(F.col('cps_face_value') < F.col('pal_sx_90p_cps_face_value'), F.lit(8)) \
                    .otherwise(F.lit(9))
                    ) \
        .withColumn('pal_sx_pos_discount_rt',
                    F.when(F.col('discount_rt') < F.col('pal_sx_10p_discount_rt'), F.lit(0)) \
                    .when(F.col('discount_rt') < F.col('pal_sx_20p_discount_rt'), F.lit(1)) \
                    .when(F.col('discount_rt') < F.col('pal_sx_30p_discount_rt'), F.lit(2)) \
                    .when(F.col('discount_rt') < F.col('pal_sx_40p_discount_rt'), F.lit(3)) \
                    .when(F.col('discount_rt') < F.col('pal_sx_50p_discount_rt'), F.lit(4)) \
                    .when(F.col('discount_rt') < F.col('pal_sx_60p_discount_rt'), F.lit(5)) \
                    .when(F.col('discount_rt') < F.col('pal_sx_70p_discount_rt'), F.lit(6)) \
                    .when(F.col('discount_rt') < F.col('pal_sx_80p_discount_rt'), F.lit(7)) \
                    .when(F.col('discount_rt') < F.col('pal_sx_90p_discount_rt'), F.lit(8)) \
                    .otherwise(F.lit(9))
                    ).fillna(0).drop('jd_prc')

    ##############################
    ########## 合并所有特征
    ##############################
    all_feature = sku_bundle.join(sku_ord_feature, ['batch_id', 'item_sku_id', 'flag'], 'left') \
        .join(coupon_history_interact,
              ['batch_id', 'item_sku_id', 'flag'], 'left') \
        .join(coupon_parallel_interact,
              ['batch_id', 'item_sku_id', 'flag'], 'left') \
        .groupBy(*(['batch_id', 'flag'])) \
        .agg(*[F.mean(F.col(col)).alias(col) for col in list(set(dense_cols) ^ set(cps_dense_cols))]) \
        .join(label_all, ['batch_id', 'flag'], 'right') \
        .join(coupon_feature, ['batch_id', 'flag'], 'left').fillna(0).fillna('-1')

    all_feature.registerTempTable('all_feature')

    spark.sql("""
        INSERT OVERWRITE TABLE adm.adm_cps_bef_all_feat PARTITION(dt='{dt}')
            SELECT
                batch_id,flag,month,cps_cate_cd,coupon_style,source_type,limit_rule_type,limit_organization,user_tag_cd,
                biz_tag_cd,activity_type_cd,user_lv_cd,open_flag,hour_coupon_flag,new_user_flag,cate_new_user_flag,
                shop_new_user_flag,shufang_flag,crowd_type,shangxiang_flag,open_type_1_2,open_type_1_3,open_type_1_4,
                open_type_1_5,open_type_1_6,open_type_1_7,open_type_1_8,open_type_1_999,open_type_0_1,open_type_0_999,
                cps_face_value,consume_lim,batch_cps_total_qtty,
                valid_days,jd_prc,theta,sale_qtty_sum_30d,sale_qtty_mean_30d,sale_qtty_min_30d,sale_qtty_max_30d,
                sale_qtty_std_30d,gmv_sum_30d,gmv_mean_30d,gmv_min_30d,gmv_max_30d,gmv_std_30d,user_cnt_sum_30d,
                user_cnt_mean_30d,user_cnt_min_30d,user_cnt_max_30d,user_cnt_std_30d,order_cnt_sum_30d,
                order_cnt_mean_30d,order_cnt_min_30d,order_cnt_max_30d,order_cnt_std_30d,before_price_sum_30d,
                before_price_mean_30d,before_price_min_30d,before_price_max_30d,before_price_std_30d,
                avg_offer_amt_sum_30d,avg_offer_amt_mean_30d,avg_offer_amt_min_30d,avg_offer_amt_max_30d,
                avg_offer_amt_std_30d,avg_coupon_amt_sum_30d,avg_coupon_amt_mean_30d,avg_coupon_amt_min_30d,
                avg_coupon_amt_max_30d,avg_coupon_amt_std_30d,real_price_sum_30d,real_price_mean_30d,
                real_price_min_30d,real_price_max_30d,real_price_std_30d,max_unit_price_30d,min_unit_price_30d,
                sale_qtty_lim_30d,sku_inventory_rt_30d,real_jd_ratio_mean_30d,real_before_ratio_mean_30d,
                offer_discount_ratio_mean_30d,coupon_discount_ratio_mean_30d,diff_real_ratio_mean_30d,
                cps_sale_qtty_sum_30d,cps_sale_qtty_mean_30d,cps_sale_qtty_min_30d,cps_sale_qtty_max_30d,
                cps_sale_qtty_std_30d,cps_gmv_sum_30d,cps_gmv_mean_30d,cps_gmv_min_30d,cps_gmv_max_30d,cps_gmv_std_30d,
                cps_user_cnt_sum_30d,cps_user_cnt_mean_30d,cps_user_cnt_min_30d,cps_user_cnt_max_30d,
                cps_user_cnt_std_30d,cps_order_cnt_sum_30d,cps_order_cnt_mean_30d,cps_order_cnt_min_30d,
                cps_order_cnt_max_30d,cps_order_cnt_std_30d,cps_avg_offer_amt_sum_30d,cps_avg_offer_amt_mean_30d,
                cps_avg_offer_amt_min_30d,cps_avg_offer_amt_max_30d,cps_avg_offer_amt_std_30d,
                cps_avg_coupon_amt_sum_30d,cps_avg_coupon_amt_mean_30d,cps_avg_coupon_amt_min_30d,
                cps_avg_coupon_amt_max_30d,cps_avg_coupon_amt_std_30d,cps_real_price_sum_30d,cps_real_price_mean_30d,
                cps_real_price_min_30d,cps_real_price_max_30d,cps_real_price_std_30d,cps_max_unit_price_30d,
                cps_min_unit_price_30d,cps_real_jd_ratio_mean_30d,cps_real_before_ratio_mean_30d,
                cps_offer_discount_ratio_mean_30d,cps_coupon_discount_ratio_mean_30d,cps_diff_real_ratio_mean_30d,
                shelve_days,pv_cvr_1d,uv_cvr_1d,pv_cvr_7d,uv_cvr_7d,pv_cvr_30d,uv_cvr_30d,sku_quality_score,
                sku_good_comment_rate,sku_comment_num,sku_follow_1d_cnt,sku_follow_7d_cnt,sku_follow_30d_cnt,
                sku_ord_7d_pnum,sku_ord_30d_pnum,sku_comm_30d_cnt,sku_good_comm_30d_cnt,sku_good_comm_ratio,
                sku_exp_1d_cnt,sku_exp_3d_cnt,sku_exp_ads_cnt,sku_exp_rec_cnt,sku_exp_ser_cnt,sku_exp_act_cnt,
                sku_ord_unit_price,sku_ord_user_price,sku_pv_cvr_7d,sku_uv_cvr_7d,sku_ctr_3d,sku_ctr_ads,sku_ctr_rec,
                sku_ctr_ser,sku_ctr_act,sku_prc_quantile,sku_ord_quantile,sku_view_quantile,sku_cart_quantile,
                view_cnt90d,view_cnt60d,view_cnt30d,view_cnt14d,view_cnt7d,view_cnt3d,view_cnt1d,avg_view_count,
                avg_view_uv,search_click15d,search_click_cnt6d,search_click_cnt3d,cart_cnt90d,cart_cnt60d,cart_cnt30d,
                cart_cnt14d,cart_cnt7d,cart_cnt3d,cart_cnt1d,avg_cart_count,buy_cnt90d,buy_cnt60d,buy_cnt30d,buy_cnt14d,
                buy_cnt7d,buy_cnt3d,buy_cnt1d,avg_buy_count,gmv_90d,gmv_60d,gmv_30d,gmv_14d,gmv_7d,gmv_3d,gmv_1d,
                avg_gmv,cart_prob_90d,cart_prob_60d,cart_prob_30d,cart_prob_14d,cart_prob_7d,buy_prob_90d,buy_prob_60d,
                buy_prob_30d,buy_prob_14d,buy_prob_7d,gpm3d,gpm2d,gpm1d,cart_uv_90d,cart_uv_60d,cart_uv_30d,cart_uv_15d,
                cart_uv_7d,buy_uv_90d,buy_uv_60d,buy_uv_30d,buy_uv_15d,buy_uv_7d,has_coupon_use_days,coupon_use_cnt90d,
                coupon_use_cnt60d,coupon_use_cnt30d,coupon_use_cnt15d,coupon_use_cnt7d,avg_coupon_use_count,
                avg_coupon_use_uv,min_pay_price_30d,min_pay_price_15d,min_pay_price_7d,max_pay_price_30d,
                max_pay_price_15d,max_pay_price_7d,current_mid_pay_price,coupon_use_prob_90d,coupon_use_prob_60d,
                coupon_use_prob_30d,coupon_use_prob_15d,coupon_use_prob_7d,his_cps_cnt,his_hour_coupon_cnt,
                his_shufang_cnt,his_shangxiang_cnt,his_new_crowd_cnt,his_second_crowd_cnt,his_old_crowd_cnt,
                his_mean_gmv,his_max_gmv,his_min_gmv,his_std_gmv,his_mean_sale_qtty,his_max_sale_qtty,his_min_sale_qtty,
                his_std_sale_qtty,his_mean_user_cnt,his_max_user_cnt,his_min_user_cnt,his_std_user_cnt,
                his_mean_consume_lim,his_max_consume_lim,his_min_consume_lim,his_std_consume_lim,
                his_mean_cps_face_value,his_max_cps_face_value,his_min_cps_face_value,his_std_cps_face_value,
                his_mean_discount_rt,his_max_discount_rt,his_min_discount_rt,his_std_discount_rt,
                his_mean_batch_cps_total_qtty,his_max_batch_cps_total_qtty,his_min_batch_cps_total_qtty,
                his_std_batch_cps_total_qtty,his_10p_consume_lim,his_20p_consume_lim,his_30p_consume_lim,
                his_40p_consume_lim,his_50p_consume_lim,his_60p_consume_lim,his_70p_consume_lim,his_80p_consume_lim,
                his_90p_consume_lim,his_10p_cps_face_value,his_20p_cps_face_value,his_30p_cps_face_value,
                his_40p_cps_face_value,his_50p_cps_face_value,his_60p_cps_face_value,his_70p_cps_face_value,
                his_80p_cps_face_value,his_90p_cps_face_value,his_10p_discount_rt,his_20p_discount_rt,
                his_30p_discount_rt,his_40p_discount_rt,his_50p_discount_rt,his_60p_discount_rt,his_70p_discount_rt,
                his_80p_discount_rt,his_90p_discount_rt,his_sx_mean_gmv,his_sx_max_gmv,his_sx_min_gmv,his_sx_std_gmv,
                his_sx_mean_sale_qtty,his_sx_max_sale_qtty,his_sx_min_sale_qtty,his_sx_std_sale_qtty,
                his_sx_mean_user_cnt,his_sx_max_user_cnt,his_sx_min_user_cnt,his_sx_std_user_cnt,
                his_sx_mean_consume_lim,his_sx_max_consume_lim,his_sx_min_consume_lim,his_sx_std_consume_lim,
                his_sx_mean_cps_face_value,his_sx_max_cps_face_value,his_sx_min_cps_face_value,
                his_sx_std_cps_face_value,his_sx_mean_discount_rt,his_sx_max_discount_rt,his_sx_min_discount_rt,
                his_sx_std_discount_rt,his_sx_mean_batch_cps_total_qtty,his_sx_max_batch_cps_total_qtty,
                his_sx_min_batch_cps_total_qtty,his_sx_std_batch_cps_total_qtty,his_sx_10p_consume_lim,
                his_sx_20p_consume_lim,his_sx_30p_consume_lim,his_sx_40p_consume_lim,his_sx_50p_consume_lim,
                his_sx_60p_consume_lim,his_sx_70p_consume_lim,his_sx_80p_consume_lim,his_sx_90p_consume_lim,
                his_sx_10p_cps_face_value,his_sx_20p_cps_face_value,his_sx_30p_cps_face_value,his_sx_40p_cps_face_value,
                his_sx_50p_cps_face_value,his_sx_60p_cps_face_value,his_sx_70p_cps_face_value,his_sx_80p_cps_face_value,
                his_sx_90p_cps_face_value,his_sx_10p_discount_rt,his_sx_20p_discount_rt,his_sx_30p_discount_rt,
                his_sx_40p_discount_rt,his_sx_50p_discount_rt,his_sx_60p_discount_rt,his_sx_70p_discount_rt,
                his_sx_80p_discount_rt,his_sx_90p_discount_rt,his_pos_consume_lim,his_pos_cps_face_value,
                his_pos_discount_rt,his_sx_pos_consume_lim,his_sx_pos_cps_face_value,his_sx_pos_discount_rt,pal_cps_cnt,
                pal_hour_coupon_cnt,pal_shufang_cnt,pal_shangxiang_cnt,pal_new_crowd_cnt,pal_second_crowd_cnt,
                pal_old_crowd_cnt,pal_mean_gmv,pal_max_gmv,pal_min_gmv,pal_std_gmv,pal_mean_sale_qtty,pal_max_sale_qtty,
                pal_min_sale_qtty,pal_std_sale_qtty,pal_mean_user_cnt,pal_max_user_cnt,pal_min_user_cnt,
                pal_std_user_cnt,pal_mean_consume_lim,pal_max_consume_lim,pal_min_consume_lim,pal_std_consume_lim,
                pal_mean_cps_face_value,pal_max_cps_face_value,pal_min_cps_face_value,pal_std_cps_face_value,
                pal_mean_discount_rt,pal_max_discount_rt,pal_min_discount_rt,pal_std_discount_rt,
                pal_mean_batch_cps_total_qtty,pal_max_batch_cps_total_qtty,pal_min_batch_cps_total_qtty,
                pal_std_batch_cps_total_qtty,pal_10p_consume_lim,pal_20p_consume_lim,pal_30p_consume_lim,
                pal_40p_consume_lim,pal_50p_consume_lim,pal_60p_consume_lim,pal_70p_consume_lim,pal_80p_consume_lim,
                pal_90p_consume_lim,pal_10p_cps_face_value,pal_20p_cps_face_value,pal_30p_cps_face_value,
                pal_40p_cps_face_value,pal_50p_cps_face_value,pal_60p_cps_face_value,pal_70p_cps_face_value,
                pal_80p_cps_face_value,pal_90p_cps_face_value,pal_10p_discount_rt,pal_20p_discount_rt,
                pal_30p_discount_rt,pal_40p_discount_rt,pal_50p_discount_rt,pal_60p_discount_rt,pal_70p_discount_rt,
                pal_80p_discount_rt,pal_90p_discount_rt,pal_sx_mean_gmv,pal_sx_max_gmv,pal_sx_min_gmv,pal_sx_std_gmv,
                pal_sx_mean_sale_qtty,pal_sx_max_sale_qtty,pal_sx_min_sale_qtty,pal_sx_std_sale_qtty,
                pal_sx_mean_user_cnt,pal_sx_max_user_cnt,pal_sx_min_user_cnt,pal_sx_std_user_cnt,
                pal_sx_mean_consume_lim,pal_sx_max_consume_lim,pal_sx_min_consume_lim,pal_sx_std_consume_lim,
                pal_sx_mean_cps_face_value,pal_sx_max_cps_face_value,pal_sx_min_cps_face_value,
                pal_sx_std_cps_face_value,pal_sx_mean_discount_rt,pal_sx_max_discount_rt,pal_sx_min_discount_rt,
                pal_sx_std_discount_rt,pal_sx_mean_batch_cps_total_qtty,pal_sx_max_batch_cps_total_qtty,
                pal_sx_min_batch_cps_total_qtty,pal_sx_std_batch_cps_total_qtty,pal_sx_10p_consume_lim,
                pal_sx_20p_consume_lim,pal_sx_30p_consume_lim,pal_sx_40p_consume_lim,pal_sx_50p_consume_lim,
                pal_sx_60p_consume_lim,pal_sx_70p_consume_lim,pal_sx_80p_consume_lim,pal_sx_90p_consume_lim,
                pal_sx_10p_cps_face_value,pal_sx_20p_cps_face_value,pal_sx_30p_cps_face_value,pal_sx_40p_cps_face_value,
                pal_sx_50p_cps_face_value,pal_sx_60p_cps_face_value,pal_sx_70p_cps_face_value,pal_sx_80p_cps_face_value,
                pal_sx_90p_cps_face_value,pal_sx_10p_discount_rt,pal_sx_20p_discount_rt,pal_sx_30p_discount_rt,
                pal_sx_40p_discount_rt,pal_sx_50p_discount_rt,pal_sx_60p_discount_rt,pal_sx_70p_discount_rt,
                pal_sx_80p_discount_rt,pal_sx_90p_discount_rt,pal_pos_consume_lim,pal_pos_cps_face_value,
                pal_pos_discount_rt,pal_sx_pos_consume_lim,pal_sx_pos_cps_face_value,pal_sx_pos_discount_rt,
                cvr,use_num,roi,sale_amt,ord_qtty
            FROM all_feature
        """.format(dt=execute_dt))

    print(LOG_PREFIX, '写adm.adm_cps_bef_all_feat表完成')
