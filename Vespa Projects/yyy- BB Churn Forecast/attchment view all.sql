CREATE OR REPLACE  VIEW pitteloudj.attach_view_all
AS
SELECT View_attachments_201411.account_number
	, View_attachments_201411.observation_dt
	, View_attachments_201411.product_holding
	, View_attachments_201411.bb_type
	, View_attachments_201411.h_AGE_coarse_description
	, View_attachments_201411.h_fss_v3_group
	, View_attachments_201411.BB_latest_act_date
	, View_attachments_201411.affluence
	, View_attachments_201411.life_stage
	, View_attachments_201411.monthyear
FROM sharmaa.View_attachments_201411

UNION ALL

SELECT View_attachments_201412.account_number
	, View_attachments_201412.observation_dt
	, View_attachments_201412.product_holding
	, View_attachments_201412.bb_type
	, View_attachments_201412.h_AGE_coarse_description
	, View_attachments_201412.h_fss_v3_group
	, View_attachments_201412.BB_latest_act_date
	, View_attachments_201412.affluence
	, View_attachments_201412.life_stage
	, View_attachments_201412.monthyear, broadband
FROM sharmaa.View_attachments_201412


UNION ALL

SELECT View_attachments_201501.account_number
	, View_attachments_201501.observation_dt
	, View_attachments_201501.product_holding
	, View_attachments_201501.bb_type
	, View_attachments_201501.h_AGE_coarse_description
	, View_attachments_201501.h_fss_v3_group
	, View_attachments_201501.BB_latest_act_date
	, View_attachments_201501.affluence
	, View_attachments_201501.life_stage
	, View_attachments_201501.monthyear, broadband
FROM sharmaa.View_attachments_201501


UNION ALL

SELECT View_attachments_201502.account_number
	, View_attachments_201502.observation_dt
	, View_attachments_201502.product_holding
	, View_attachments_201502.bb_type
	, View_attachments_201502.h_AGE_coarse_description
	, View_attachments_201502.h_fss_v3_group
	, View_attachments_201502.BB_latest_act_date
	, View_attachments_201502.affluence
	, View_attachments_201502.life_stage
	, View_attachments_201502.monthyear, broadband
FROM sharmaa.View_attachments_201502


UNION ALL

SELECT View_attachments_201503.account_number
	, View_attachments_201503.observation_dt
	, View_attachments_201503.product_holding
	, View_attachments_201503.bb_type
	, View_attachments_201503.h_AGE_coarse_description
	, View_attachments_201503.h_fss_v3_group
	, View_attachments_201503.BB_latest_act_date
	, View_attachments_201503.affluence
	, View_attachments_201503.life_stage
	, View_attachments_201503.monthyear, broadband
FROM sharmaa.View_attachments_201503


UNION ALL

SELECT View_attachments_201504.account_number
	, View_attachments_201504.observation_dt
	, View_attachments_201504.product_holding
	, View_attachments_201504.bb_type
	, View_attachments_201504.h_AGE_coarse_description
	, View_attachments_201504.h_fss_v3_group
	, View_attachments_201504.BB_latest_act_date
	, View_attachments_201504.affluence
	, View_attachments_201504.life_stage
	, View_attachments_201504.monthyear, broadband
FROM sharmaa.View_attachments_201504


UNION ALL

SELECT View_attachments_201505.account_number
	, View_attachments_201505.observation_dt
	, View_attachments_201505.product_holding
	, View_attachments_201505.bb_type
	, View_attachments_201505.h_AGE_coarse_description
	, View_attachments_201505.h_fss_v3_group
	, View_attachments_201505.BB_latest_act_date
	, View_attachments_201505.affluence
	, View_attachments_201505.life_stage
	, View_attachments_201505.monthyear, broadband
FROM sharmaa.View_attachments_201505


UNION ALL

SELECT View_attachments_201506.account_number
	, View_attachments_201506.observation_dt
	, View_attachments_201506.product_holding
	, View_attachments_201506.bb_type
	, View_attachments_201506.h_AGE_coarse_description
	, View_attachments_201506.h_fss_v3_group
	, View_attachments_201506.BB_latest_act_date
	, View_attachments_201506.affluence
	, View_attachments_201506.life_stage
	, View_attachments_201506.monthyear, broadband
FROM sharmaa.View_attachments_201506


UNION ALL

SELECT View_attachments_201507.account_number
	, View_attachments_201507.observation_dt
	, View_attachments_201507.product_holding
	, View_attachments_201507.bb_type
	, View_attachments_201507.h_AGE_coarse_description
	, View_attachments_201507.h_fss_v3_group
	, View_attachments_201507.BB_latest_act_date
	, View_attachments_201507.affluence
	, View_attachments_201507.life_stage
	, View_attachments_201507.monthyear, broadband
FROM sharmaa.View_attachments_201507


UNION ALL

SELECT View_attachments_201508.account_number
	, View_attachments_201508.observation_dt
	, View_attachments_201508.product_holding
	, View_attachments_201508.bb_type
	, View_attachments_201508.h_AGE_coarse_description
	, View_attachments_201508.h_fss_v3_group
	, View_attachments_201508.BB_latest_act_date
	, View_attachments_201508.affluence
	, View_attachments_201508.life_stage
	, View_attachments_201508.monthyear, broadband
FROM sharmaa.View_attachments_201508


UNION ALL

SELECT View_attachments_201509.account_number
	, View_attachments_201509.observation_dt
	, View_attachments_201509.product_holding
	, View_attachments_201509.bb_type
	, View_attachments_201509.h_AGE_coarse_description
	, View_attachments_201509.h_fss_v3_group
	, View_attachments_201509.BB_latest_act_date
	, View_attachments_201509.affluence
	, View_attachments_201509.life_stage
	, View_attachments_201509.monthyear, broadband
FROM sharmaa.View_attachments_201509


UNION ALL

SELECT View_attachments_201510.account_number
	, View_attachments_201510.observation_dt
	, View_attachments_201510.product_holding
	, View_attachments_201510.bb_type
	, View_attachments_201510.h_AGE_coarse_description
	, View_attachments_201510.h_fss_v3_group
	, View_attachments_201510.BB_latest_act_date
	, View_attachments_201510.affluence
	, View_attachments_201510.life_stage
	, View_attachments_201510.monthyear, broadband
FROM sharmaa.View_attachments_201510


UNION ALL

SELECT View_attachments_201511.account_number
	, View_attachments_201511.observation_dt
	, View_attachments_201511.product_holding
	, View_attachments_201511.bb_type
	, View_attachments_201511.h_AGE_coarse_description
	, View_attachments_201511.h_fss_v3_group
	, View_attachments_201511.BB_latest_act_date
	, View_attachments_201511.affluence
	, View_attachments_201511.life_stage
	, View_attachments_201511.monthyear, broadband
FROM sharmaa.View_attachments_201511


UNION ALL

SELECT View_attachments_201512.account_number
	, View_attachments_201512.observation_dt
	, View_attachments_201512.product_holding
	, View_attachments_201512.bb_type
	, View_attachments_201512.h_AGE_coarse_description
	, View_attachments_201512.h_fss_v3_group
	, View_attachments_201512.BB_latest_act_date
	, View_attachments_201512.affluence
	, View_attachments_201512.life_stage
	, View_attachments_201512.monthyear, broadband
FROM sharmaa.View_attachments_201512


UNION ALL

SELECT View_attachments_201601.account_number
	, View_attachments_201601.observation_dt
	, View_attachments_201601.product_holding
	, View_attachments_201601.bb_type
	, View_attachments_201601.h_AGE_coarse_description
	, View_attachments_201601.h_fss_v3_group
	, View_attachments_201601.BB_latest_act_date
	, View_attachments_201601.affluence
	, View_attachments_201601.life_stage
	, View_attachments_201601.monthyear, broadband
FROM sharmaa.View_attachments_201601


UNION ALL

SELECT attachments_201602.account_number
	, attachments_201602.observation_dt
	, attachments_201602.product_holding
	, attachments_201602.bb_type
	, attachments_201602.h_AGE_coarse_description
	, attachments_201602.h_fss_v3_group
	, attachments_201602.BB_latest_act_date
	, attachments_201602.affluence
	, attachments_201602.life_stage
	, attachments_201602.monthyear, broadband
FROM sharmaa.attachments_201602

UNION ALL

SELECT View_attachments_201603.account_number
	, View_attachments_201603.observation_dt
	, View_attachments_201603.product_holding
	, View_attachments_201603.bb_type
	, View_attachments_201603.h_AGE_coarse_description
	, View_attachments_201603.h_fss_v3_group
	, View_attachments_201603.BB_latest_act_date
	, View_attachments_201603.affluence
	, View_attachments_201603.life_stage
	, View_attachments_201603.monthyear, broadband
FROM sharmaa.View_attachments_201603


UNION ALL

SELECT View_attachments_201604.account_number
	, View_attachments_201604.observation_dt
	, View_attachments_201604.product_holding
	, View_attachments_201604.bb_type
	, View_attachments_201604.h_AGE_coarse_description
	, View_attachments_201604.h_fss_v3_group
	, View_attachments_201604.BB_latest_act_date
	, View_attachments_201604.affluence
	, View_attachments_201604.life_stage
	, View_attachments_201604.monthyear, broadband
FROM sharmaa.View_attachments_201604


UNION ALL

SELECT View_attachments_201605.account_number
	, View_attachments_201605.observation_dt
	, View_attachments_201605.product_holding
	, View_attachments_201605.bb_type
	, View_attachments_201605.h_AGE_coarse_description
	, View_attachments_201605.h_fss_v3_group
	, View_attachments_201605.BB_latest_act_date
	, View_attachments_201605.affluence
	, View_attachments_201605.life_stage
	, View_attachments_201605.monthyear, broadband
FROM sharmaa.View_attachments_201605


UNION ALL

SELECT View_attachments_201606.account_number
	, View_attachments_201606.observation_dt
	, View_attachments_201606.product_holding
	, View_attachments_201606.bb_type
	, View_attachments_201606.h_AGE_coarse_description
	, View_attachments_201606.h_fss_v3_group
	, View_attachments_201606.BB_latest_act_date
	, View_attachments_201606.affluence
	, View_attachments_201606.life_stage
	, View_attachments_201606.monthyear, broadband
FROM sharmaa.View_attachments_201606


UNION ALL

SELECT View_attachments_201607.account_number
	, View_attachments_201607.observation_dt
	, View_attachments_201607.product_holding
	, View_attachments_201607.bb_type
	, View_attachments_201607.h_AGE_coarse_description
	, View_attachments_201607.h_fss_v3_group
	, View_attachments_201607.BB_latest_act_date
	, View_attachments_201607.affluence
	, View_attachments_201607.life_stage
	, View_attachments_201607.monthyear, broadband
FROM sharmaa.View_attachments_201607


UNION ALL

SELECT View_attachments_201608.account_number
	, View_attachments_201608.observation_dt
	, View_attachments_201608.product_holding
	, View_attachments_201608.bb_type
	, View_attachments_201608.h_AGE_coarse_description
	, View_attachments_201608.h_fss_v3_group
	, View_attachments_201608.BB_latest_act_date
	, View_attachments_201608.affluence
	, View_attachments_201608.life_stage
	, View_attachments_201608.monthyear, broadband
FROM sharmaa.View_attachments_201608


UNION ALL

SELECT View_attachments_201609.account_number
	, View_attachments_201609.observation_dt
	, View_attachments_201609.product_holding
	, View_attachments_201609.bb_type
	, View_attachments_201609.h_AGE_coarse_description
	, View_attachments_201609.h_fss_v3_group
	, View_attachments_201609.BB_latest_act_date
	, View_attachments_201609.affluence
	, View_attachments_201609.life_stage
	, View_attachments_201609.monthyear, broadband
FROM sharmaa.View_attachments_201609


UNION ALL

SELECT View_attachments_201610.account_number
	, View_attachments_201610.observation_dt
	, View_attachments_201610.product_holding
	, View_attachments_201610.bb_type
	, View_attachments_201610.h_AGE_coarse_description
	, View_attachments_201610.h_fss_v3_group
	, View_attachments_201610.BB_latest_act_date
	, View_attachments_201610.affluence
	, View_attachments_201610.life_stage
	, View_attachments_201610.monthyear, broadband
FROM sharmaa.View_attachments_201610


UNION ALL

SELECT View_attachments_201611.account_number
	, View_attachments_201611.observation_dt
	, View_attachments_201611.product_holding
	, View_attachments_201611.bb_type
	, View_attachments_201611.h_AGE_coarse_description
	, View_attachments_201611.h_fss_v3_group
	, View_attachments_201611.BB_latest_act_date
	, View_attachments_201611.affluence
	, View_attachments_201611.life_stage
	, View_attachments_201611.monthyear, broadband
FROM sharmaa.View_attachments_201611

