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
-------------------------------A: OK...

select	base.key_
		,base.channel
		,base.life_flag
		,base.duration
		,base.prec_prog
		,base.prec_start
		,base.prec_stime
		,base.slot
		,cast(substring(tdate.UTC_DAY_DATE,1,10) || ' ' || ttime.UTC_TIME as datetime) as slot_start
		,base.succ_prog
		,base.succ_end
		,base.succ_etime
from	(
			select	a.PK_VIEWING_SLOT_INSTANCE_FACT				as key_
					,a.DK_CHANNEL_DIM							as channel
					,lp.LIVE_OR_RECORDED						as life_flag
					,a.duration									as duration						
					,a.DK_PRECEDING_PROGRAMME_INSTANCE_DIM		as prec_prog
					,presc.DK_BROADCAST_START_DATEHOUR_DIM		as prec_start
					,presc.DK_BROADCAST_START_TIME_DIM			as prec_stime
					,a.DK_SLOT_INSTANCE_DIM						as slot
					,a.DK_BROADCAST_START_DATEHOUR_DIM			as slot_start_date
					,a.DK_BROADCAST_START_TIME_DIM				as slot_start_time
					,a.DK_SUCCEEDING_PROGRAMME_INSTANCE_DIM		as succ_prog
					,succ.DK_BROADCAST_END_DATEHOUR_DIM			as succ_end
					,succ.DK_BROADCAST_END_TIME_DIM				as succ_etime
			from	(select	PK_VIEWING_SLOT_INSTANCE_FACT
							,DK_PRECEDING_PROGRAMME_INSTANCE_DIM
							,DK_SLOT_INSTANCE_DIM
							,DK_BROADCAST_START_DATEHOUR_DIM
							,DK_BROADCAST_START_TIME_DIM
							,DK_INSTANCE_START_DATEHOUR_DIM
							,DK_INSTANCE_START_TIME_DIM
							,DK_EVENT_START_DATEHOUR_DIM
							,DK_EVENT_START_TIME_DIM
							,DK_SUCCEEDING_PROGRAMME_INSTANCE_DIM
							,DK_CHANNEL_DIM
							,DURATION
							,DK_PLAYBACK_DIM
					from	SMI_ETL.VIEWING_SLOT_INSTANCE_FACT_STATIC
					where 	DK_PRECEDING_PROGRAMME_INSTANCE_DIM = DK_SUCCEEDING_PROGRAMME_INSTANCE_DIM
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
		on base.slot_start_time	= ttime.PK_TIME_DIM


-------------------------------Q: For different programmes are slots falling in between start and end time as expected?
-------------------------------A: OK...


-------------------------------Q: Checking is Slots overflown into another...
-------------------------------A: Based on the sample this is OK...

/*Sample:

unique dk_viewing_event_dim, dk_dth_active_viewing_card_dim
1200071	29481312
6400025	27675445
3800013	28933888
7400034	27926687
1200071	40038390
3800013	40209039
1200071	39616693
400020	16096205
3400014	14895873
1200071	29045062

*/

select count(1) 
from (
		select 	slot_start
				,min(slot_start) 
					over( partition by 	DK_VIEWING_EVENT_DIM
										,dk_dth_active_viewing_card_dim
						order 	by	slot_start
						rows 		between 1 following and 1 following) 	as endtime
		from(			
				select	vsifs.PK_VIEWING_SLOT_INSTANCE_FACT														AS key_
						,vsifs.DK_VIEWING_EVENT_DIM
						,vsifs.dk_dth_active_viewing_card_dim
						,cast(substring(tdate.BROADCAST_DAY_DATE,1,10) || ' ' || ttime.LOCAL_TIME as datetime) 	as slot_start						
				from 	smi_dw..VIEWING_SLOT_INSTANCE_FACT_STATIC as vsifs
						inner join	MDS..DATEHOUR_DIM 	as tdate
						on vsifs.DK_BROADCAST_START_DATEHOUR_DIM	= tdate.PK_DATEHOUR_DIM
						inner join 	MDS..TIME_DIM		as ttime
						on vsifs.DK_BROADCAST_START_TIME_DIM		= ttime.PK_TIME_DIM
				where 	vsifs.DK_VIEWING_EVENT_DIM = 3800013		
				and		vsifs.dk_dth_active_viewing_card_dim = 40209039
				and 	vsifs.slot_status = 'INSERT'
				and		vsifs.BATCH_NUMBER = 237
				--and 	vsifs.DK_BROADCAST_START_DATEHOUR_DIM between 2012110100 and 2012110123
				order	by slot_start
			) as x	
		) as N
where 	slot_start >= endtime