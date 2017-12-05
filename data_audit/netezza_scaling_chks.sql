select 1.0 * (select count(distinct account_number) from dis_prepare.DIS_ETL.TD_CUSTOMER_ATTRIBUTES) / 
(select sky_base from dis_reference.DEACONA.SCALING_METADATA where current_flag = 1)

select * from dis_reference.DEACONA.SCALING_METADATA

select sum(weight_scaled_value) from DIS_PREPARE.DIS_ETL.TD_SCALING_RIM_WEIGHTING
limit 100

select count(1) from DIS_REFERENCE.DIS_ETL.FINAL_SCALING_HOUSEHOLD

TD_SCALING_RIM_WEIGHTING.WEIGHT_SCALED_VALUE

select * from dis_prepare..TD_SCALING_RIM_WEIGHTING
limit 100

select a.*, b.weight_scaled_value 
from dis_prepare.DIS_ETL.TD_SCALING_HOUSEHOLDS_2 a,
dis_prepare..TD_SCALING_RIM_WEIGHTING b
where a.scaling_segment_key = b.scaling_segment_key

select count(1) from 

22.2135105226704


3197254132327210788	22.2135105226704
-6549150237113388916	74.606165738011

83.5972168512953

select 1.0 * 83.5972168512953 / 74.606165738011

select 1.0 * 24.8905387084662 / 22.2135105226704

1.1205135128490253033215
1.120513512849025391926


select * from dis_prepare.DIS_ETL.TD_SCALING_HOUSEHOLDS_2
where scaling_segment_key = 3197254132327210788

select 103000

select count(1) from dis_prepare.DIS_ETL.TD_CUSTOMER_ATTRIBUTES

limit 100

select sum(weight_sample_value) from DIS_PREPARE.DIS_ETL.TD_SCALING_HOUSEHOLDS_1
limit 100

Get the value for that segment based on the SCALING_SEGMENT_KEY, 
TD_SCALING_RIM_WEIGHTING.WEIGHT_SCALED_VALUE 
* (SCALING_METADATA.SKY_BASE / ( total value of TD_SCALING_HOUSEHOLDS_1.WEIGHT_SAMPLE_VALUE * ))
 
 select sum(weight_sample_value) from dis_prepare..TD_SCALING_HOUSEHOLDS_1
 
select 1.0 * (10300000/(select sum(weight_sample_value) from dis_prepare..TD_SCALING_HOUSEHOLDS_1))


(SELECT distinct a.event_start_date,a.hh_composition, a.tv_region, a.dtv_package, 
a.box_type, a.tenure, a.hh_composition_scaling_value 
  --FROM DIS_REFERENCE.DIS_ETL.FINAL_SCALING_HOUSEHOLD_HISTORY
  from DIS_PREPARE.DIS_ETL.TD_SCALING_RIM_WEIGHTING a,
  DIS_REFERENCE.DIS_ETL.FINAL_SCALING_HOUSEHOLD b
where a.scaling_segment_key = b.scaling_segment_key
and a.event_start_date = '2013-05-23 00:00:00')

 select count(1) from 
 
 group by a.event_start_date,a.hh_composition, a.tv_region, a.dtv_package, a.box_type, a.tenure)

from DIS_PREPARE.DIS_ETL.TD_SCALING_RIM_WEIGHTING



select a.*, b.acct_cnt daily_panel_cnt, b.sum_of_weights, 
--b.sum_of_weights * (1.0 * (select count(distinct account_number) from dis_prepare.DIS_ETL.TD_CUSTOMER_ATTRIBUTES) / 
--(select sky_base from dis_reference.DEACONA.SCALING_METADATA where current_flag = 1)) weights_pre_upscaling
b.sum_of_weights * (1.0 * (select count(distinct account_number) from dis_prepare.DIS_ETL.TD_CUSTOMER_ATTRIBUTES) / 10300000)
weights_pre_upscaling
from
(SELECT hh_composition,tv_region,dtv_package,box_type, tenure, count(distinct account_number) sky_base
  FROM DIS_PREPARE.DIS_ETL.TD_CUSTOMER_ATTRIBUTES
 group by hh_composition,tv_region,dtv_package,box_type, tenure) a
 --left outer join 
 full outer join 
  (SELECT event_start_date,hh_composition, tv_region, dtv_package, box_type, tenure, 
count(distinct account_number) acct_cnt, sum(weight_scaled_value) sum_of_weights
  --FROM DIS_REFERENCE.DIS_ETL.FINAL_SCALING_HOUSEHOLD_HISTORY
  from DIS_PREPARE.DIS_ETL.TD_SCALING_RIM_WEIGHTING
  --FROM DIS_REFERENCE.DIS_ETL.FINAL_SCALING_HOUSEHOLD
where event_start_date = '2013-05-23 00:00:00'
 group by event_start_date,hh_composition, tv_region, dtv_package, box_type, tenure) b
 on a.hh_composition = b.hh_composition
 and a.tv_region = b.tv_region
  and a.dtv_package = b.dtv_package
 and a.box_type = b.box_type
 and a.tenure = b.tenure
 
 

 select * from dis_prepare..TD_SCALING_RIM_WEIGHTING 
 limit 100
 
 select count(distinct account_number) from DIS_PREPARE.DIS_ETL.TD_CUSTOMER_ATTRIBUTES
 limit 100
 
 select * from dis_prepare..TD_SCALING_RIM_WEIGHTING
 limit 100
 
 select sum(sky_base)
 from 
 (SELECT a.hh_composition,a.tv_region,a.dtv_package,a.box_type, a.tenure, count(distinct a.account_number) sky_base
  FROM DIS_PREPARE.DIS_ETL.TD_CUSTOMER_ATTRIBUTES a,DIS_PREPARE..HOUSEHOLD_STB_COUNT b
  where a.household_key = b.household_key
 group by hh_composition,tv_region,dtv_package,box_type, tenure) t
 
 select b.event_start_date,a.hh_composition, a.tv_region, a.dtv_package, a.box_type,a.tenure, a.sky_base, b.scaling_segment_key,
 b.acct_num,b.post_upscale_weight, b.pre_upscale_weight_scaled_value


from
 (SELECT a.hh_composition,a.tv_region,a.dtv_package,a.box_type, a.tenure, count(distinct a.account_number) sky_base
  FROM DIS_PREPARE.DIS_ETL.TD_CUSTOMER_ATTRIBUTES a,DIS_PREPARE..HOUSEHOLD_STB_COUNT b
  where a.household_key = b.household_key
 group by hh_composition,tv_region,dtv_package,box_type, tenure) a
 left outer join 
 --full outer join
  (select a.scaling_segment_key, b.hh_composition,b.tv_region,b.dtv_package,b.box_type,b.tenure, b.event_start_date,
  count(distinct a.account_number) acct_num,sum(a.weight_scaled_value) post_upscale_weight, sum(b.weight_scaled_value) pre_upscale_weight_scaled_value
from dis_prepare.DIS_ETL.TD_SCALING_HOUSEHOLDS_2 a,
dis_prepare..TD_SCALING_RIM_WEIGHTING b
where a.scaling_segment_key = b.scaling_segment_key
group by a.scaling_segment_key, b.hh_composition,b.tv_region,b.dtv_package,b.box_type,b.tenure,b.event_start_date) b
 on a.hh_composition = b.hh_composition
 and a.tv_region = b.tv_region
  and a.dtv_package = b.dtv_package
 and a.box_type = b.box_type
 and a.tenure = b.tenure


select * from DIS_PREPARE..TD_SCALING_POPULATION_ATTRIBUTES
limit 100

----------------------FINAL NETEZZA QUERY-------------------------------------
select b.event_start_date,a.scaling_universe_key,a.hh_composition, a.tv_region, a.dtv_package, a.box_type,a.tenure, a.weight_sample_value sky_base, b.scaling_segment_key,
 b.acct_num,b.post_upscale_weight, b.pre_upscale_weight_scaled_value
from
 DIS_PREPARE..TD_SCALING_POPULATION_ATTRIBUTES a
 left outer join 
 (select a.scaling_segment_key, a.scaling_universe_key, b.hh_composition,b.tv_region,b.dtv_package,b.box_type,b.tenure, b.event_start_date,
  count(distinct a.account_number) acct_num,sum(a.weight_scaled_value) post_upscale_weight, sum(b.weight_scaled_value) pre_upscale_weight_scaled_value
from dis_prepare.DIS_ETL.TD_SCALING_HOUSEHOLDS_2 a,
dis_prepare..TD_SCALING_RIM_WEIGHTING b
where a.scaling_segment_key = b.scaling_segment_key
group by a.scaling_segment_key, a.scaling_universe_key,b.hh_composition,b.tv_region,b.dtv_package,b.box_type,b.tenure,b.event_start_date) b
 on a.hh_composition = b.hh_composition
 and a.tv_region = b.tv_region
  and a.dtv_package = b.dtv_package
 and a.box_type = b.box_type
 and a.tenure = b.tenure
 and a.scaling_universe_key = b.scaling_universe_key

----------------------FINAL NETEZZA QUERY END-------------------------------------

select a.*, b.weight_scaled_value pre_upscale_weight_scaled_value
from dis_prepare.DIS_ETL.TD_SCALING_HOUSEHOLDS_2 a,
dis_prepare..TD_SCALING_RIM_WEIGHTING b
where a.scaling_segment_key = b.scaling_segment_key

