/* Generate report data for Persistent AMS / Eco-mode trial of R7 software release

This script has been written to be run on the Netezza database, and tests for AMS persistence by the existence of viewing events prior to the 3am Eco-mode window.

~~~
20/03/2014	Author	:	Hoi Yu Tang, hoiyu.tang@skyiq.co.uk

*/

-------------------------------------------
-- 1. Generate summary data for pivot table
-------------------------------------------
select 
	t.SCMS_SUBSCRIBER_ID
	, t.DAY_PK
	, min(r.log_received_datetime)		as earliest_log_received_dt
	, 	case
			when min(r.log_received_datetime) is not null then 1
			else 0
		end 							as log_returned_flag
	, min(r.event_time)					as earliest_event_in_logs
	, 	case
			when min(r.event_time) < (t.DAY_PK + cast('1 hour' as interval)) then 1
			else 0
		end 							as ams_persisted_flag
	, case 
			when (log_returned_flag = 1 and ams_persisted_flag = 1) then 1
			else 0
		end 							as log_with_AMS
	, case 
			when (log_returned_flag = 1 and ams_persisted_flag = 0) then 1
			else 0
		end 							as log_no_AMS
	, case 
			when (log_returned_flag = 0 and ams_persisted_flag = 0) then 1
			else 0
		end 							as no_log_no_AMS
	, case 
			when (log_returned_flag = 0 and ams_persisted_flag = 1) then 1
			else 0
		end 							as AMS_no_log
	, sum(r.log_events)					as number_of_events
	, sum(r.adsmart_events)				as number_of_adsmart_events
from 
	(	-- First create all date vs subID combinations
	select 
		DD.DAY_PK
		, SUB.*
	from 
		rocket_prepare..DATE_DIM 	as DD 	-- Start from calendar to create time base
		cross join ( 						-- cross join to get all combinations of subID and date
			select '26893552' as scms_subscriber_id union
			select '26893544' as scms_subscriber_id union
			select '26893576' as scms_subscriber_id union
			select '26893522' as scms_subscriber_id union
			select '26893521' as scms_subscriber_id union
			select '26893509' as scms_subscriber_id union
			select '26893519' as scms_subscriber_id union
			select '26893546' as scms_subscriber_id union
			select '26893209' as scms_subscriber_id union
			select '26893210' as scms_subscriber_id union
			select '26893573' as scms_subscriber_id union
			select '26893571' as scms_subscriber_id union
			select '26893495' as scms_subscriber_id union
			select '26893507' as scms_subscriber_id union
			select '26893510' as scms_subscriber_id union
			select '26893541' as scms_subscriber_id union
			select '26893498' as scms_subscriber_id union
			select '26893499' as scms_subscriber_id union
			select '26893504' as scms_subscriber_id union
			select '26893289' as scms_subscriber_id union
			select '26893293' as scms_subscriber_id union
			select '26893294' as scms_subscriber_id union
			select '26894594' as scms_subscriber_id union
			select '26894597' as scms_subscriber_id union
			select '26894596' as scms_subscriber_id union
			select '26894599' as scms_subscriber_id union
			select '26894598' as scms_subscriber_id union
			select '26894612' as scms_subscriber_id union
			select '26894611' as scms_subscriber_id union
			select '26894610' as scms_subscriber_id union
			select '26894616' as scms_subscriber_id union
			select '26894614' as scms_subscriber_id union
			select '26894578' as scms_subscriber_id union
			select '26894623' as scms_subscriber_id union
			select '26894621' as scms_subscriber_id union
			select '26894591' as scms_subscriber_id union
			select '26894589' as scms_subscriber_id union
			select '26894593' as scms_subscriber_id union
			select '26894588' as scms_subscriber_id union
			select '26894587' as scms_subscriber_id union
			select '26894586' as scms_subscriber_id union
			select '26894585' as scms_subscriber_id union
			select '26894600' as scms_subscriber_id union
			select '26894626' as scms_subscriber_id union
			select '26894602' as scms_subscriber_id union
			select '26894601' as scms_subscriber_id union
			select '26894604' as scms_subscriber_id union
			select '26893324' as scms_subscriber_id union
			select '26893306' as scms_subscriber_id union
			select '26893456' as scms_subscriber_id union
			select '26893493' as scms_subscriber_id union
			select '26893472' as scms_subscriber_id union
			select '26893470' as scms_subscriber_id union
			select '26893479' as scms_subscriber_id union
			select '26893437' as scms_subscriber_id union
			select '26893388' as scms_subscriber_id union
			select '26893446' as scms_subscriber_id union
			select '26893445' as scms_subscriber_id union
			select '26893444' as scms_subscriber_id union
			select '26893452' as scms_subscriber_id union
			select '26893449' as scms_subscriber_id union
			select '26893448' as scms_subscriber_id union
			select '26893467' as scms_subscriber_id union
			select '26893457' as scms_subscriber_id union
			select '26893345' as scms_subscriber_id union
			select '26893400' as scms_subscriber_id union
			select '26893402' as scms_subscriber_id union
			select '26893393' as scms_subscriber_id union
			select '26893373' as scms_subscriber_id union
			select '26893374' as scms_subscriber_id union
			select '26893359' as scms_subscriber_id union
			select '26893352' as scms_subscriber_id union
			select '26893429' as scms_subscriber_id union
			select '26893424' as scms_subscriber_id union
			select '26893425' as scms_subscriber_id union
			select '26893412' as scms_subscriber_id union
			select '26893407' as scms_subscriber_id union
			select '26893414' as scms_subscriber_id union
			select '26893422' as scms_subscriber_id union
			select '26893411' as scms_subscriber_id union
			select '26893207' as scms_subscriber_id union
			select '26893621' as scms_subscriber_id union
			select '26893597' as scms_subscriber_id union
			select '26893585' as scms_subscriber_id union
			select '26893601' as scms_subscriber_id union
			select '26893599' as scms_subscriber_id union
			select '26893196' as scms_subscriber_id union
			select '26893613' as scms_subscriber_id union
			select '26893617' as scms_subscriber_id union
			select '26893620' as scms_subscriber_id union
			select '26893191' as scms_subscriber_id union
			select '26893622' as scms_subscriber_id union
			select '26893201' as scms_subscriber_id union
			select '26893612' as scms_subscriber_id union
			select '26893608' as scms_subscriber_id union
			select '26893606' as scms_subscriber_id union
			select '26893266' as scms_subscriber_id union
			select '26893269' as scms_subscriber_id union
			select '26893272' as scms_subscriber_id union
			select '26893281' as scms_subscriber_id union
			select '26893283' as scms_subscriber_id union
			select '26893286' as scms_subscriber_id union
			select '26893246' as scms_subscriber_id union
			select '26847721' as scms_subscriber_id union
			select '26847722' as scms_subscriber_id union
			select '26847776' as scms_subscriber_id union
			select '26893217' as scms_subscriber_id union
			select '26897496' as scms_subscriber_id union
			select '26893223' as scms_subscriber_id union
			select '26893221' as scms_subscriber_id union
			select '26893219' as scms_subscriber_id union
			select '26893225' as scms_subscriber_id union
			select '26847710' as scms_subscriber_id union
			select '26847713' as scms_subscriber_id union
			select '26847714' as scms_subscriber_id union
			select '26847699' as scms_subscriber_id union
			select '26847689' as scms_subscriber_id union
			select '26847688' as scms_subscriber_id union
			select '26847639' as scms_subscriber_id union
			select '26847642' as scms_subscriber_id union
			select '26847644' as scms_subscriber_id union
			select '26847646' as scms_subscriber_id union
			select '26847647' as scms_subscriber_id union
			select '26847651' as scms_subscriber_id union
			select '26847656' as scms_subscriber_id union
			select '26847657' as scms_subscriber_id union
			select '26847659' as scms_subscriber_id union
			select '26847660' as scms_subscriber_id union
			select '26847799' as scms_subscriber_id union
			select '26847801' as scms_subscriber_id union
			select '26847806' as scms_subscriber_id union
			select '26847841' as scms_subscriber_id union
			select '26847844' as scms_subscriber_id union
			select '26847847' as scms_subscriber_id union
			select '26847835' as scms_subscriber_id union
			select '26847831' as scms_subscriber_id union
			select '26847829' as scms_subscriber_id union
			select '26847821' as scms_subscriber_id union
			select '26847823' as scms_subscriber_id union
			select '26847818' as scms_subscriber_id union
			select '26847725' as scms_subscriber_id union
			select '26847728' as scms_subscriber_id union
			select '26847731' as scms_subscriber_id union
			select '26893395' as scms_subscriber_id
			)						as SUB
		where 
			DD.DAY_PK between '2014-02-15' and now()
		order by DD.DAY_PK
		) as t
	left join (				-- Finally, left join to overlay aggregated log and AMS analysis
		select
			scms_subscriber_id
			, log_received_datetime
			, min(DTH.EVENT_START_DATETIME)		as event_time
			, sum(
				case 
					when event_action not in ('AdSmart Substitution','AdSmart No Substitution') then 1
					else 0
				end
				)	as log_events
			, sum(
				case 
					when event_action in ('AdSmart Substitution','AdSmart No Substitution') then 1
					else 0
				end
				)	as adsmart_events
		from DIS_REFERENCE..FINAL_DTH_VIEWING_EVENT_HISTORY	as DTH
		where  
			DTH.log_received_datetime between '2014-02-15' and now()
			and time(DTH.log_received_datetime) > '03:00:00'
			and panel_id_reported = 4
		group by 
			scms_subscriber_id
			, DTH.log_received_datetime
		order by
			scms_subscriber_id
			, DTH.log_received_datetime
		) as r 	on 	r.scms_subscriber_id = t.scms_subscriber_id
				and	date(r.log_received_datetime) = t.DAY_PK
group by
	t.SCMS_SUBSCRIBER_ID
	, t.DAY_PK
order by
	t.SCMS_SUBSCRIBER_ID
	, t.DAY_PK
;



---------------------------------------------
-- 2. Individual subscriber log interrogation
---------------------------------------------
select 
	LOG_RECEIVED_DATETIME
	, LOG_CREATION_DATETIME
	, EVENT_START_DATETIME
--	, EVENT_END_DATETIME
	, EVENT_ACTION
from DIS_REFERENCE..FINAL_DTH_VIEWING_EVENT_HISTORY
where 
	SCMS_SUBSCRIBER_ID = '26847725'
	and EVENT_START_DATETIME >= '2014-02-24'
order by
	LOG_RECEIVED_DATETIME
	, LOG_CREATION_DATETIME
	, EVENT_START_DATETIME
--	, EVENT_END_DATETIME
--	, EVENT_ACTION
;
