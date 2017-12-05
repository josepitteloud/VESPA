------------------------- C&S S&P...

-- Overview...
select count(1) from greenj.barb_spot_data;         -- 133503
select count(1) from greenj.barb_promo_data;        -- 86105
select top 10 * from greenj.landmark_data;          -- permission / We don't have landmark data yet...
select count(1) from greenj.bss_data;               -- 28906
select count(1) from greenj.attribution_data;       -- 0
select count(1) from greenj.barb_data_amends;       -- 266
select top 10 * from greenj.attribution_audit_data; -- not found / No attribution audit data either...



------------------------ Checking matches for dimension key values to their source tables...
select count(1)from (
select distinct DK_VIEWING_SLOT_INSTANCE_FACT
from SMI_ETL.VIEWING_SLOT_INSTANCE_FACT_STATIC
minus
select distinct PK_CHANNEL_DIM
from SMI_ETL.CHANNEL_DIM) as b


------------------------ Checking Slot Attribution as expected (BASE Table)...

create table AD_SNP_BASE (
	key_				integer unique primary key
	,channel			integer
	,life_flag			varchar(10)
	,duration			integer
	,prec_prog			integer
	,prec_prog_start	datetime
	,prec_prog_end		datetime
	,slot				integer
	,slot_start			datetime
	,succ_prog			integer
	,succ_prog_start	datetime
	,succ_prog_end  	datetime
)
commit


insert into	AD_SNP_BASE
select	base.key_
		,base.channel
		,base.life_flag
		,base.duration
		,base.prec_prog
		,null
		,null
		,base.slot
		,cast(substring(tdate.BROADCAST_DAY_DATE,1,10) || ' ' || ttime.LOCAL_TIME as datetime) as slot_start
		,base.succ_prog
		,null
		,null
from	(
			select	a.PK_VIEWING_SLOT_INSTANCE_FACT				as key_
					,a.DK_CHANNEL_DIM							as channel
					,lp.LIVE_OR_RECORDED						as life_flag
					,a.duration									as duration						
					,a.DK_PRECEDING_PROGRAMME_INSTANCE_DIM		as prec_prog
					,a.DK_SLOT_INSTANCE_DIM						as slot
					,a.DK_INSTANCE_START_DATEHOUR_DIM			as slot_start_date
					,a.DK_INSTANCE_START_TIME_DIM				as slot_start_time
					--,a.DK_EVENT_START_DATEHOUR_DIM				as event_start_date
					--,a.DK_EVENT_START_TIME_DIM					as event_start_time
					,a.DK_SUCCEEDING_PROGRAMME_INSTANCE_DIM		as succ_prog
			from	(select	PK_VIEWING_SLOT_INSTANCE_FACT
							,DK_PRECEDING_PROGRAMME_INSTANCE_DIM
							,DK_SLOT_INSTANCE_DIM
							,DK_BROADCAST_START_DATEHOUR_DIM
							,DK_INSTANCE_START_DATEHOUR_DIM
							,DK_INSTANCE_START_TIME_DIM
							,DK_EVENT_START_DATEHOUR_DIM
							,DK_EVENT_START_TIME_DIM
							,DK_SUCCEEDING_PROGRAMME_INSTANCE_DIM
							,DK_CHANNEL_DIM
							,DURATION
							,DK_PLAYBACK_DIM
					from	SMI_ETL.VIEWING_SLOT_INSTANCE_FACT_STATIC
					)												as a
					LEFT join SMI_ETL.PROGRAMME_INSTANCE_DIM_ASOC	as presc
					on	a.DK_PRECEDING_PROGRAMME_INSTANCE_DIM 	= presc.PK_PROGRAMME_INSTANCE_DIM_ASOC
					
					LEFT join SMI_ETL.PROGRAMME_INSTANCE_DIM_ASOC	as succ
					on	a.DK_SUCCEEDING_PROGRAMME_INSTANCE_DIM 	= succ.PK_PROGRAMME_INSTANCE_DIM_ASOC
					
					LEFT join SMI_ETL.PLAYBACK_DIM					as lp
					on	a.DK_PLAYBACK_DIM						= lp.PK_PLAYBACK_DIM
		) 								as base
		inner join	MDS..DATEHOUR_DIM 	as tdate
		on base.slot_start_date	= tdate.PK_DATEHOUR_DIM
		inner join 	MDS..TIME_DIM		as ttime
		on base.slot_start_time	= ttime.PK_TIME_DIM -- 10760065 rows affected

commit;

-------------------------------Q: is there any preceding programme <> succeding programme where starting times are the same?
-------------------------------A: No

select 	count(1)
from 	SMI_ETL.AD_SNP_BASE as a
where 	a.PREC_PROG <> a.SUCC_PROG
and 	a.PREC_PROG_START = a.SUCC_PROG_START

-------------------------------Q: is there preceding programme = succeding programme where starting times are different?
-------------------------------A: No

select 	count(1)
from 	SMI_ETL.AD_SNP_BASE as a
where 	a.PREC_PROG = a.SUCC_PROG
and 	a.PREC_PROG_START <> a.SUCC_PROG_START


-------------------------------Q: is there any null or -1 for preceding or succeeding programme dim and programme Instance Dim?
-------------------------------A: Yes there are and it shouldn't be like that...

select 	count(1)
from	SMI_ETL.VIEWING_SLOT_INSTANCE_FACT_STATIC
where	DK_SUCCEEDING_PROGRAMME_DIM <=0 -- 754779

select 	count(1)
from	SMI_ETL.VIEWING_SLOT_INSTANCE_FACT_STATIC
where	DK_PRECEDING_PROGRAMME_DIM <=0 -- 754779

select 	count(1)
from	SMI_ETL.VIEWING_SLOT_INSTANCE_FACT_STATIC
where	DK_PRECEDING_PROGRAMME_INSTANCE_DIM <=0 -- 754779

select 	count(1)
from	SMI_ETL.VIEWING_SLOT_INSTANCE_FACT_STATIC
where	DK_SUCCEEDING_PROGRAMME_INSTANCE_DIM <=0 -- 787119 -- Diff: 32340

-------------------------------Q: For a single programme are slots falling in between start and end time as expected?
-------------------------------A: It seems to be a defect here...

select 	count(1)
from 	SMI_ETL.AD_SNP_BASE as a
where 	a.PREC_PROG <> a.SUCC_PROG
and 	a.SLOT_START between a.PREC_PROG_START and a.SUCC_PROG_START


-- total records where prec <> succ: 	1,797,328
-- Total slots as expected:				1,612,169 
-- Diff:								185,159

/* how are this outliers? */

