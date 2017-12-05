
-- reviewing the tables...
select * from smi_dw..AGENCY_DIM limit 10
select * from smi_dw..SLOT_COPY_DIM limit 100000
select * from dis_reference..FINAL_SLOT_COPY limit 10
select * from dis_reference..SLOT_TIMETABLE_HIST limit 10

-- checking uniqueness on PKs...

select	count(1) as a
		,count(distinct PK_AGENCY_DIM) as b
		,case when a<>b then 1 else 0 end as diff
from 	smi_dw..AGENCY_DIM 
union	all
select	count(1) as a
		,count(distinct PK_SLOT_COPY_DIM) as b
		,case when a<>b then 1 else 0 end as diff
from	smi_dw..SLOT_COPY_DIM
union	all
select	count(1) as a
		,count(distinct PK_SLOT_REFERENCE_DIM)as b
		,case when a<>b then 1 else 0 end as diff
from	smi_dw..SLOT_REFERENCE_DIM

-- Integrity checks for relevant fields...

select	'nc1' as index_
		,count(1)																				as hits
		,sum(case when trim(advertiser_code) in (null,'','-1') then 1 else 0 end) 				as nc1_1
		,sum(case when trim(advertiser_name) in (null,'','-1') then 1 else 0 end) 				as nc1_2
		,sum(case when trim(buyer_code) in (null,'','-1') then 1 else 0 end)					as nc1_3
		,sum(case when trim(buyer_name) in (null,'','-1') then 1 else 0 end)					as nc1_4
		,sum(case when trim(BARB_SALES_HOUSE_NAME) in (null,'','-1') then 1 else 0 end)			as nc1_5
		,sum(case when trim(BARB_SALES_HOUSE_short_NAME) in (null,'','-1') then 1 else 0 end)	as nc1_6
		,sum(case when trim(lower(BARB_SALES_HOUSE_NAME)) = 'unknown' then 1 else 0 end)		as nc1_7
		,sum(case when trim(lower(BARB_SALES_HOUSE_short_NAME)) = 'unknown' then 1 else 0 end)	as nc1_8
from	smi_dw..AGENCY_DIM


select	'nc2' as index_
		,count(1)																						as hits
		,sum(case when trim(lower(clearcast_commercial_number)) in (null,'','-1') then 1 else 0 end)	as nc2_1
		,sum(case when trim(slot_type) in (null,'','-1') then 1 else 0 end)								as nc2_2
		,sum(case when slot_copy_duration_seconds <= 0 then 1 else 0 end)								as nc2_3 --
		,sum(case when product_code in (null,'','-1') then 1 else 0 end)								as nc2_4
		,sum(case when trim(lower(product_code)) = 'unknown' then 1 else 0 end)							as nc2_42
		,sum(case when product_name in (null,'','-1') then 1 else 0 end)								as nc2_5 --
		,sum(case when trim(lower(product_name)) = 'unknown' then 1 else 0 end)							as nc2_52
		,sum(case when product_category in (null,'','-1') then 1 else 0 end)							as nc2_6
		,sum(case when trim(lower(product_category)) = 'unknown' then 1 else 0 end)						as nc2_62 --
		,sum(case when product_master_category in (null,'','-1') then 1 else 0 end)						as nc2_7
		,sum(case when trim(lower(product_master_category)) = 'unknown' then 1 else 0 end)				as nc2_72 --
		,sum(case when HOLDING_COMPANY_CODE in (null,'','-1') then 1 else 0 end)						as nc2_8
		,sum(case when trim(lower(HOLDING_COMPANY_CODE)) = 'unknown' then 1 else 0 end)					as nc2_82 --
		,sum(case when HOLDING_COMPANY_NAME in (null,'','-1') then 1 else 0 end)						as nc2_9
		,sum(case when trim(lower(HOLDING_COMPANY_NAME)) = 'unknown' then 1 else 0 end)					as nc2_92 --
		,sum(case when advertiser_code in (null,'','-1') then 1 else 0 end)								as nc2_10
		,sum(case when trim(lower(advertiser_code)) = 'unknown' then 1 else 0 end)						as nc2_102 --
		,sum(case when advertiser_name in (null,'','-1') then 1 else 0 end)								as nc2_11
		,sum(case when trim(lower(advertiser_name)) = 'unknown' then 1 else 0 end)						as nc2_112 --
		,sum(case when buyer_code in (null,'','-1') then 1 else 0 end)									as nc2_12
		,sum(case when trim(lower(buyer_code)) = 'unknown' then 1 else 0 end)							as nc2_122 --
		,sum(case when buyer_name in (null,'','-1') then 1 else 0 end)									as nc2_13
		,sum(case when trim(lower(buyer_name)) = 'unknown' then 1 else 0 end)							as nc2_132 --
		,sum(case when slot_copy_source in (null,'','-1') then 1 else 0 end)							as nc2_14
		,sum(case when trim(lower(slot_copy_source)) = 'unknown' then 1 else 0 end)						as nc2_142 --
from	dis_reference..FINAL_SLOT_COPY


select	'nc3' as index_
		,count(1)		as hits
		,sum(case when slot_type in (null,'','-1') then 1 else 0 end)									as nc3_1
		,sum(case when trim(lower(slot_type)) = 'unknown' then 1 else 0 end)							as nc3_12
		,sum(case when slot_sub_type in (null,'','-1') then 1 else 0 end)								as nc3_2
		,sum(case when trim(lower(slot_sub_type)) = 'unknown' then 1 else 0 end)						as nc3_22 --
		,sum(case when SPOT_POSITION_IN_BREAK_DESCRIPTION in (null,'','-1') then 1 else 0 end)			as nc3_3
		,sum(case when trim(lower(SPOT_POSITION_IN_BREAK_DESCRIPTION)) = 'unknown' then 1 else 0 end)	as nc3_32
		,sum(case when break_position in (null,'','-1') then 1 else 0 end)								as nc3_4
		,sum(case when SLOT_INSTANCE_POSITION_IN_BREAK in (null,-1) then 1 else 0 end ) 				as nc_5
		,sum(case when adsmart_status in (null,'','-1') then 1 else 0 end)								as nc3_6
		,sum(case when adsmart_action in (null,'','-1') then 1 else 0 end)								as nc3_7
		,sum(case when trim(lower(adsmart_action)) = 'n/a' then 1 else 0 end)							as nc3_72 --
		,sum(case when slot_duration_seconds <= 0 then 1 else 0 end) 									as nc_8
from 	smi_dw..SLOT_REFERENCE_DIM
