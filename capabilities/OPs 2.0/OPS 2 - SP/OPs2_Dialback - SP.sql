/*


                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$= ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$  ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$   ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=   ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$    ODD   ZDDDDDDDN
                                      $$$      .      $DDZ
                                     $$$             ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES

--------------------------------------------------------------------------------------------------------------
**Project Name:							OPS 2.0 Dialback Report Complement
**Analysts:                             Angel Donnarumma 	(angel.donnarumma_mirabel@skyiq.co.uk)
										Berwyn Cort			(Berwyn.Cort@skyiq.co.uk)
										James McKane		(James.McKane@skyiq.co.uk)
**Lead(s):                              Angel Donnarumma 	(angel.donnarumma_mirabel@skyiq.co.uk)
**Stakeholder:                          Operational Reports / SIG
**Due Date:                             28/03/2014
**Project Code (Insight Collation):     
**Sharepoint Folder:                    
                                                                        
**Business Brief:

	This Procedure complements MASVG units to help building KPIs for the Dialback Report...

**Sections:

	S01: OPs 2.0 Dialback Report Complement
		S01.0 - Initialising environment
		S01.1 - Sampling boxes interaction with the panel
        S01.2 - Aggregate components for KPIs
		S01.3 - Setting Privileges
--------------------------------------------------------------------------------------------------------------
*/

-----------------------------------
-- S01.0 - Initialising environment
-----------------------------------

create or replace dsd_ops2_dialback_complement
as begin

-- Variables
declare @latest_full_date	date
declare @event_from_date    integer
declare @event_to_date      integer
declare @bst_start 			date
declare @gmt_start 			date

-- Getting past Saturday... Vespa weeks goes from Saturday to Sunday...
execute vespa_analysts.Regulars_Get_report_end_date @latest_full_date output

-- Shaping the dates to the Dim representation to optimise the query on viewing tables...
set @event_from_date    = convert(integer,dateformat(dateadd(day, -30, @latest_full_date),'yyyymmddhh'))        -- YYYYMMDD00
set @event_to_date      = convert(integer,dateformat(dateadd(day,1,@latest_full_date),'yyyymmdd')+'23')         -- YYYYMMDD23

-- Constructing bases to evaluate shift of hour in the year due to seasons...
set @bst_start = dateadd(dy, -(datepart(dw, datepart(year, today()) || '-03-31') -1),datepart(year, today()) || '-03-31')  -- to get last Sunday in March
set @gmt_start = dateadd(dy, -(datepart(dw, datepart(year, today()) || '-10-31') -1),datepart(year, today()) || '-10-31')  -- to get last Sunday in October


----------------------------------------------------
-- S01.1 - Sampling boxes interaction with the panel
----------------------------------------------------

IF today()  >= @bst_start and today() < @gmt_start
Begin

	-- Taking a snapshot of the viewing tables for last month activity for all boxes involved...
	insert	into vespa_Dialback_log_collection_dump	(
														 subscriber_id
														,stb_log_creation_date
														,doc_creation_date_from_9am
														,first_event_mark
														,last_event_mark
														,log_event_count
														,hour_received
														,panel_id
													)
	select	subscriber_id
			,dateadd(hour,1, LOG_START_DATE_TIME_UTC)
			,case	when convert(integer,dateformat(min(dateadd(hour,1, LOG_RECEIVED_START_DATE_TIME_UTC)),'hh')) <23 then cast(min(dateadd(hour,1, LOG_RECEIVED_START_DATE_TIME_UTC)) as date)-1
					else cast(min(dateadd(hour,1, LOG_RECEIVED_START_DATE_TIME_UTC)) as date)
			end 	as doc_creation_date_from_9am
			,min(dateadd(hour,1, EVENT_START_DATE_TIME_UTC))
			,max(dateadd(hour,1, EVENT_END_DATE_TIME_UTC))
			,count(1)
			,datepart(hh, min(dateadd(hour,1, LOG_RECEIVED_START_DATE_TIME_UTC)))
			,min(panel_id)
	from  	sk_prod.VESPA_DP_PROG_VIEWED_CURRENT
	where  	panel_id 							in (4,11,12)
	and     dk_event_start_datehour_dim 		between @event_from_date and @event_to_date
	and     LOG_RECEIVED_START_DATE_TIME_UTC	is not null
	and     LOG_START_DATE_TIME_UTC 			is not null
	and     subscriber_id 						is not null
	group 	by	subscriber_id
				,LOG_START_DATE_TIME_UTC
	having	doc_creation_date_from_9am is not null
End

ELSE
Begin

	-- Taking a snapshot of the viewing tables for last month activity for all boxes involved...
	insert	into vespa_Dialback_log_collection_dump	(
														 subscriber_id
														,stb_log_creation_date
														,doc_creation_date_from_9am
														,first_event_mark
														,last_event_mark
														,log_event_count
														,hour_received
														,panel_id
													)
	select	subscriber_id
            ,LOG_START_DATE_TIME_UTC
            ,case	when convert(integer,dateformat(min(LOG_RECEIVED_START_DATE_TIME_UTC),'hh')) <23 then cast(min(LOG_RECEIVED_START_DATE_TIME_UTC) as date)-1
                    else cast(min(LOG_RECEIVED_START_DATE_TIME_UTC) as date)
            end 	as doc_creation_date_from_9am
            ,min(EVENT_START_DATE_TIME_UTC)
            ,max(EVENT_END_DATE_TIME_UTC)
            ,count(1)
            ,datepart(hh, min(LOG_RECEIVED_START_DATE_TIME_UTC))
            ,min(panel_id)
	from   	sk_prod.VESPA_DP_PROG_VIEWED_CURRENT
	where  	panel_id 							in (4,11,12)
    and     dk_event_start_datehour_dim 		between @event_from_date and @event_to_date
    and     LOG_RECEIVED_START_DATE_TIME_UTC	is not null
    and     LOG_START_DATE_TIME_UTC 			is not null
    and     subscriber_id 						is not null
	group 	by	subscriber_id
				,LOG_START_DATE_TIME_UTC
	having  doc_creation_date_from_9am is not null
End

create index panel_id on vespa_Dialback_log_collection_dump (panel_id)
commit


----------------------------------------
-- S01.2 - Aggregate components for KPIs
----------------------------------------

truncate table vespa_Dialback_log_daily_summary
commit

-- Aggregating components to be use later on on the derivation of KPIs for Dialback...
insert  into vespa_Dialback_log_daily_summary
select	subscriber_id
		,convert(date, doc_creation_date_from_9am) as log_date
		,count(distinct doc_creation_date_from_9am) -- we never check that this isn't 1?
		,min(first_event_mark)
		,max(last_event_mark)
		,sum(log_event_count) -- we're still not doing anything with these coverage numbers?
		,min(hour_received)
from 	vespa_Dialback_log_collection_dump
where 	panel_id in (4,12,11) -- split out phone DP from BB panel
group 	by	subscriber_id, log_date -- 19,180,935 row(s) inserted

commit

-- Axing outlier dates if any...
delete from vespa_Dialback_log_daily_summary
where dateadd(day, 30, log_date) <= @latest_full_date
or log_date > @latest_full_date

commit


-----------------------------
-- S01.3 - Setting Privileges
-----------------------------

grant select on vespa_Dialback_log_collection_dump 	to vespa_group_low_security
grant select on vespa_Dialback_log_daily_summary 	to vespa_group_low_security
commit

end;
commit;


grant execute on dsd_ops2_dialback_complement to vespa_group_low_security;
commit;