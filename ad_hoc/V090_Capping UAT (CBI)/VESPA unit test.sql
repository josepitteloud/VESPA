/*

start date: 15/10/2012
due date:	19/10/2012
analyst:	Angel Donnarumma, Tony Kenniard, Patrick Igonor
lead:		Jose Loureda

Project name: Capping UAT (CBI)  
Project code: 90

** Description:

We need to have a unit test to apply on both Vespa and CBI data to compare the capping process
and make sure everything that outcomes are similars, here we are extracting the measures to compare
from the Vespa side focusing on sk_prod.VESPA_EVENTS_ALL ??? ( or should it be VESPA_VIEWING_EVENTS_ALL 
as we cap only on viewing events...[Checking with Jose] )

The Test Unit is being constructed over a period of time going from the 10th - 19th of August, hence measures will be
counted for these dates...

Patrick is working on the CBI leg to do the comparison...

** Section:

		A: Checking on Completness of data in source table...
			
			A01: Checking on Key fields for capping process...
			A02: Checking on Number of events captured per day...
			A03: Checking on total duration of events captured per day (in minutes)...
			A04: Checking distribution across segments for capping...

		
			
		B: Checking Capping figures and proportions...
			
			B01: Number/% of events with short duration cap applied (<6 sec of viewing)... +Distribution check (A04)
			B02: Duration/% of events minimun cap applied...+Distribution check (A04)
			B03: Number/% Events with long duration cap applied (random process, first programme)...+Distribution check (A04)
			B04: Duration/% Total viewing with long duration cap applied (random process, first programme)...+Distribution check (A04)
			B05: Number/% of Capped/uncapped events...+Distribution check (A04)
			B06: Duration/% of viewing before capping...+Distribution check (A04)
			B07: Ntiles distribution...
			B08: Segments' thresholds (in Minutes)...
			B09: Num of thresholds above and below the max and min...
		
		
		C: Creating Process tables for Section B...
			
			C01: Creating table statements...
			C02: Creating indexes statements...
			
			
		D: Generic Distribution check for section B...
			
			D01: Generic check for distribution accross segments for capping...
			
NOTES:

+ CURRENT ANALYSIS IS ONLY BEEN RAN FOR THE 10TH OF AUGUST... THAT'S A FRIDAY... AND THIS IS THE TABLE OUTPUT TABLE
	Vespa_daily_augs_20120810
*/


/*			A: Checking on Completness of data in source table...			*/
			
--	A01: Checking on Key fields for capping process...

--	Checking number of null panel id 
	select 
			count(1) as hits 
	from    sk_prod.vespa_events_viewed_all 
	where   dk_event_start_datehour_dim >= 2012081000 
	  and   dk_event_start_datehour_dim<= 2012081023 
	  and 	panel_id is null -- 36

-- we have a very low amount of null values in here, that is 36 cases in 63,711,244 records

-- Checking number of null Account numbers...
	select	count(1) as CountNullAccountN 
	  from	sk_prod.vespa_events_viewed_all 
	 where	dk_event_start_datehour_dim >= 2012081000 
	   and  dk_event_start_datehour_dim<= 2012081023 
	   and 	panel_id in (4,12)
	   and  account_number is null -- 2707013
	   
-- So we have 2,707,013 null account_numbers in 35,778,265 records, that is 8% of records...

-- Checking number of null subscriber_id...
	select  count(1) as CountNullSubsN
	from    sk_prod.vespa_events_viewed_all 
	where   dk_event_start_datehour_dim >= 2012081000 
	  and   dk_event_start_datehour_dim<= 2012081023 
	  and 	panel_id in (4,12)
	  and   subscriber_id is null -- 27129  

-- 27,129 representing 0.08% or records...

-- Checking on Nulls for dk_programme_instance_dim
	select  count(1) as CountNullProg
	from    sk_prod.vespa_events_viewed_all 
	where   dk_event_start_datehour_dim >= 2012081000 
	  and   dk_event_start_datehour_dim<= 2012081023 
	  and 	panel_id in (4,12)
	  and   dk_programme_instance_dim is null -- 0

-- No nulls in this field...

-- Checking on Nulls for event types...
	select  count(1) as CountNullEventType
	from    sk_prod.vespa_events_viewed_all 
	where   dk_event_start_datehour_dim >= 2012081000 
	  and   dk_event_start_datehour_dim<= 2012081023 
	  and   type_of_viewing_event is null
	  and 	panel_id in (4,12) --  0
	  
-- No nulls in this field...

-- Checking nulls on event_start_time...
	select  count(1) as hits 
	from    sk_prod.vespa_events_viewed_all 
	where   event_start_date_time_utc is null 
	and     dk_event_start_datehour_dim >= 2012081000 
	and     dk_event_start_datehour_dim<= 2012081023 
	and 	panel_id in (4,12) -- 0
	
-- No nulls in this field...

-- Checking nulls on event_end_time...
	select  count(1) as hits 
	from    sk_prod.vespa_events_viewed_all 
	where   event_end_date_time_utc is null
	and     dk_event_start_datehour_dim >= 2012081000 
	and     dk_event_start_datehour_dim<= 2012081023 
	and 	panel_id in (4,12)-- 0
	
-- No nulls in this field...	

-- Checking nulls on viewing start and end times...
/*
	By definition all viewing events refer to recorded events being played, hence the condition here is to check that if for all
	cases of Recordings there are null values in the event_start_time or event_end_time, fortunately on previous checks we saw that 
	there is no single record having missing values for this two date fields... meaning that all recorded events have a viewing time...
	Wohooo!!!!
*/

-- Checking on null values for Live_recorded field [NEW CHECK]
	select  count(1) as hits 
	from    sk_prod.vespa_events_viewed_all 
	where   live_recorded is null
	and     dk_event_start_datehour_dim >= 2012081000 
	and     dk_event_start_datehour_dim<= 2012081023
	and 	panel_id in (4,12)-- 0
	
-- No nulls in this field...

-- Checking on year <=1970 values for Recorded UTC...
	select  count(1) as hits 
	from    sk_prod.VESPA_EVENTS_VIEWED_ALL 
	where   datepart(year,event_start_date_time_utc) <=1970 
	and     upper(live_recorded) = 'RECORDED'
	and     dk_event_start_datehour_dim >= 2012081000 
	and     dk_event_start_datehour_dim<= 2012081023
	and 	panel_id in (4,12)

	
-- No recorded time under the year of 1970...

-- Checking any clash on gendre and channel pack... [NEW CHECK]
/*	
	Basically is to compare the values that are coming directly in vespa_events_viewed_all
	against the consolidated view for channel map -> vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES
*/
--	So far the query I've written is getting stuck, yahoooo... so moving on meanwhile IT kills my stucked session...


--	Sensitive_channel: Checking number of null values during the 10th of Aug.

	select  count(1) as hits
	from    sk_prod.vespa_events_viewed_all
	where   sensitive_channel is null
	and     dk_event_start_datehour_dim >= 2012081000 
	and     dk_event_start_datehour_dim<= 2012081023
	and 	panel_id in (4,12)
	
--	827,796 (2%) records with null values in this field...



--A02: Checking on Number of events captured per day...

/*for the 10th*/
select 	count(1) as hits
from 	sk_prod.VESPA_EVENTS_VIEWED_ALL 
where 	dk_event_start_datehour_dim >= 2012081000 
and 	dk_event_start_datehour_dim<= 2012081023 
and 	panel_id in (4,12) -- 35742844


/*for the 11th*/
--select 	count(1) as hits
--from 	sk_prod.VESPA_EVENTS_VIEWED_ALL 
--where 	dk_event_start_datehour_dim >= 2012081100 
--and 	dk_event_start_datehour_dim<= 2012081123 -- 63337769

/*for the 12th*/
--select 	count(1) as hits
--from 	sk_prod.VESPA_EVENTS_VIEWED_ALL 
--where 	dk_event_start_datehour_dim >= 2012081200 
--and 	dk_event_start_datehour_dim<= 2012081223 -- 68361419

/*for the 13th*/
--select 	count(1) as hits
--from 	sk_prod.VESPA_EVENTS_VIEWED_ALL 
--where 	dk_event_start_datehour_dim >= 2012081300 
--and 	dk_event_start_datehour_dim<= 2012081323 -- 64957210

/*for the 14th*/
--select 	count(1) as hits
--from 	sk_prod.VESPA_EVENTS_VIEWED_ALL 
--where 	dk_event_start_datehour_dim >= 2012081400 
--and 	dk_event_start_datehour_dim<= 2012081423 -- 64213243

/*for the 15th*/
--select 	count(1) as hits
--from 	sk_prod.VESPA_EVENTS_VIEWED_ALL 
--where 	dk_event_start_datehour_dim >= 2012081500 
--and 	dk_event_start_datehour_dim<= 2012081523 -- 66464024

/*for the 16th*/
--select 	count(1) as hits
--from 	sk_prod.VESPA_EVENTS_VIEWED_ALL 
--where 	dk_event_start_datehour_dim >= 2012081600 
--and 	dk_event_start_datehour_dim<= 2012081623 -- 65674749

/*for the 17th*/
--select 	count(1) as hits
--from 	sk_prod.VESPA_EVENTS_VIEWED_ALL 
--where 	dk_event_start_datehour_dim >= 2012081700 
--and 	dk_event_start_datehour_dim<= 2012081723 -- 65019347

/*for the 18th*/
--select 	count(1) as hits
--from 	sk_prod.VESPA_EVENTS_VIEWED_ALL 
--where 	dk_event_start_datehour_dim >= 2012081800 
--and 	dk_event_start_datehour_dim<= 2012081823 -- 65176105

/*for the 19th*/
--select 	count(1) as hits
--from 	sk_prod.VESPA_EVENTS_VIEWED_ALL 
--where 	dk_event_start_datehour_dim >= 2012081900 
--and 	dk_event_start_datehour_dim<= 2012081923 -- 67550914

-- We have Approx. 64k events per day...



--A03: Checking on total duration of events captured per day (in minutes)...

	select  
			case when (dk_event_start_datehour_dim >= 2012081000 and dk_event_start_datehour_dim<= 2012081023) then sum(duration)/60 end as _10thmin
			--,case when (dk_event_start_datehour_dim >= 2012081100 and dk_event_start_datehour_dim<= 2012081123) then sum(duration)/60 end as _11thmin
			--,case when (dk_event_start_datehour_dim >= 2012081200 and dk_event_start_datehour_dim<= 2012081223) then sum(duration)/60 end as _12thmin
			--,case when (dk_event_start_datehour_dim >= 2012081300 and dk_event_start_datehour_dim<= 2012081323) then sum(duration)/60 end as _13thmin
			--,case when (dk_event_start_datehour_dim >= 2012081400 and dk_event_start_datehour_dim<= 2012081423) then sum(duration)/60 end as _14thmin
			--,case when (dk_event_start_datehour_dim >= 2012081500 and dk_event_start_datehour_dim<= 2012081523) then sum(duration)/60 end as _15thmin
			--,case when (dk_event_start_datehour_dim >= 2012081600 and dk_event_start_datehour_dim<= 2012081623) then sum(duration)/60 end as _16thmin
			--,case when (dk_event_start_datehour_dim >= 2012081700 and dk_event_start_datehour_dim<= 2012081723) then sum(duration)/60 end as _17thmin
			--,case when (dk_event_start_datehour_dim >= 2012081800 and dk_event_start_datehour_dim<= 2012081823) then sum(duration)/60 end as _18thmin
			--,case when (dk_event_start_datehour_dim >= 2012081900 and dk_event_start_datehour_dim<= 2012081923) then sum(duration)/60 end as _19thmin 
	from    sk_prod.vespa_events_viewed_all 
	where   dk_event_start_datehour_dim >= 2012081000 
	and     dk_event_start_datehour_dim<= 2012081923 -- <amend the date to get all the cases you want>
	and 	panel_id in (4,12)
	group by dk_event_start_datehour_dim
	
-- for the 10TH: we have 29,233,469,767 minutes...


--A04: Checking distribution across segments for capping...

/*
	So what's going on here is that the capping process based on a hierarchy on the capping rules to check what events 
	get filtered and which ones stand... the hierarchy is made up from the following fields (in the same order):
	
	Live_recorded
	,day of the week (when the event started, hence using the day part of event_start_date_time_utc)
	,event start hour
	,ps_flag
	,channel_pack
	,genre
	
	lets see how the events are distributed amongst the population of events per day... (yeah, quite long)...
*/

-- Checking distribution for Live_recorded...
	
	-- Before Capping... 
	select  substr(convert(varchar(10),dk_event_start_datehour_dim),7,2) as _day
			,case   when lower(live_recorded) = 'live'      then count(1) end as liveHits
			,case   when lower(live_recorded) = 'live'      then sum(duration)/60 end as liveMins
			,case   when lower(live_recorded) = 'recorded'  then count(1) end as RecHits
			,case   when lower(live_recorded) = 'recorded'  then sum(duration)/60 end as RecMin
	from 	sk_prod.vespa_events_viewed_all
	where   dk_event_start_datehour_dim >= 2012081000 
	and     dk_event_start_datehour_dim<= 2012081023
	and 	panel_id = 12
	group by substr(convert(varchar(10),dk_event_start_datehour_dim),7,2), live_recorded
	
	-- Before capping V2 ... now measuring over the exact table used by capping...
	select  live
            ,case   when live = 1      then count(1) end as liveHits
			,case   when live = 1      then sum(x_event_duration)/60 end as liveMins
			,case   when live = 0  then count(1) end as RecHits
			,case   when live = 0  then sum(x_event_duration)/60 end as RecMin
	from 	Capping2_01_Viewing_Records
    group   by live
	
	-- After Capping...
	select  
		    case   when upper(timeshifting) = 'LIVE'                                then count(1) end as liveHits
			,case   when upper(timeshifting) = 'LIVE'                               then sum(viewing_duration)/60 end as liveMins
			,case   when upper(timeshifting) in ('PLAYBACK28','VOSDAL','PLAYBACK7') then count(1) end as RecHits
			,case   when upper(timeshifting) in ('PLAYBACK28','VOSDAL','PLAYBACK7') then sum(viewing_duration)/60 end as RecMin
	from 	angeld.Vespa_daily_augs_20120810
	group   by timeshifting


-- Checking distribution for Day of the Week...
	
/*
When the event started, hence using the day part of event_start_date_time_utc

If I would have to write a query it would looks like below, however a simple way of doing this is just adding up the number of events 
on Section A02 for weekdays and weekends...

But because at the moment we are only checking on the 10Th and this is a Friday so is a 100% on weekday...

*/

	select	
			case    when datepart(dw,event_start_date_time_utc) between 2 and 6 then count(1) end as weekday
			,case    when datepart(dw,event_start_date_time_utc) between 2 and 6 then sum(duration)/6 end as weekdayMin
			,case   when datepart(dw,event_start_date_time_utc) in(1,7) then count(1) end as weekend
			,case   when datepart(dw,event_start_date_time_utc) in(1,7) then sum(duration)/6 end as weekendMin
	from 	sk_prod.vespa_events_viewed_all
	where   dk_event_start_datehour_dim >= 2012081000 
	and     dk_event_start_datehour_dim<= 2012081923
	and 	panel_id in (4,12)
	group by substr(convert(varchar(10),dk_event_start_datehour_dim),7,2)
	
	
	
-- Checking distribution for event start hour...

	--Before Capping...
	select 
			datepart(hour,event_start_date_time_utc) as _hour
			,count(1) as hits
			,sum(duration)/60 as TotalMin
	from	sk_prod.vespa_events_viewed_all
	where   dk_event_start_datehour_dim >= 2012081000 
	and     dk_event_start_datehour_dim<= 2012081023
	and 	panel_id in (12)
	group 	by _hour
	
	--BEFORE CAPPING V2...
	select 
			event_start_hour as _hour
			,count(1) as hits
			,sum(x_event_duration)/60 as TotalMin
	from	Capping2_01_Viewing_Records
	group   by event_start_hour
	
	
	--After Capping..
	select 
			datepart(hour,viewing_starts) as _hour
			,count(1) as hits
			,sum(viewing_duration)/60 as TotalMin
	from	angeld.Vespa_daily_augs_20120810
	group by datepart(hour,viewing_starts)
	


-- Checking on PS_flag distributtion...

	select  si_external_identifier
			,account_number
			,rank() over(partition by si_external_identifier order by account_number desc) as rank_
			,left(si_service_instance_type,1) as ps_flag
	  into  csi
	  from  sk_prod.cust_service_instance
	  where si_service_instance_type like '%DTV%'

	-- Choose one between Before or After Capping...
	-- Before Capping...
	select  distinct account_number
			,sum(duration) as duration
	into    #accountlookup
	from    sk_prod.vespa_events_viewed_all
	where   dk_event_start_datehour_dim >= 2012081000 
	and     dk_event_start_datehour_dim<= 2012081023
	and 	panel_id in (4,12)
	group by account_number
	--
	-- Or After Capping...
	select  csi.ps_flag
			,count(1) as hits
			,sum(aug.time_) as totalMin
	from    (  
				select  subscriber_id as box
						,sum(Viewing_Duration) as time_
				from    bkp_Vespa_daily_augs_20120810
				group   by subscriber_id
			) as aug
			inner join ( 
				select  convert(bigint,si_external_identifier)  as box
						,left(si_service_instance_type,1)       as ps_flag
				from    sk_prod.cust_service_instance
				where   si_service_instance_type like '%DTV%'
				group   by  box
							,ps_flag
			)as csi
			on aug.box = csi.box
	group   by csi.ps_flag
	--
	
	select  ps_flag
			,count(1) as hits
			,sum(duration)/60 as TotalMin
	from    #accountlookup as aclp
			inner join csi as csi
			on aclp.account_number = csi.account_number
			and csi.rank_=1
	group by ps_flag
	
	-- OR BEFORE CAPPING V2... ONLY THIS PIECE OF CODE...
	
	select	box_subscription
			,count(1) as hits
			,sum(x_event_duration)/60 as TotalMin
	from	Capping2_01_Viewing_Records
	group   by box_subscription

	
	
-- Checking Channel_pack distribution...

	-- Before Capping...
	select 	ska.channel_pack
			,count(1) as hits
			,sum(eva.duration)/60 as TotalMin
	from 	sk_prod.vespa_events_viewed_all as eva
			inner join vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES as ska
			on eva.service_key = ska.service_key
	where   eva.dk_event_start_datehour_dim >= 2012081000 
	and     eva.dk_event_start_datehour_dim<= 2012081023
	and 	eva.panel_id in (12)
	group 	by ska.channel_pack
	
	-- BEFORE CAPPING V2...
	
	select 	pack
			,count(1) as hits
			,sum(X_EVENT_duration)/60 as TotalMin
	from 	Capping2_01_Viewing_Records
	group   by pack
	
	-- After Capping...
	select 	ska.channel_pack
			,count(1) as hits
			,sum(aug.viewing_duration)/60 as TotalMin
	from 	angeld.Vespa_daily_augs_20120810 as aug
			inner join sk_prod.vespa_events_viewed_all as eva
			on aug.cb_row_id = eva.pk_viewing_prog_instance_fact
			inner join vespa_analysts.CHANNEL_MAP_CBI_PROD_SERVICE_KEY_ATTRIBUTES as ska
			on eva.service_key = ska.service_key
	group by ska.channel_pack
	


-- Checking genre distribution...

	-- Before Capping...
	select	genre_description
			,count(1) as hits
			,sum(duration)/60 as TotalMin
	from	sk_prod.vespa_events_viewed_all
	where   dk_event_start_datehour_dim >= 2012081000 
	and     dk_event_start_datehour_dim<= 2012081023
	and 	panel_id in (4,12)
	group by genre_description

	-- BEFORE CAPPING V2...
	
	select	genre
			,count(1) as hits
			,sum(x_event_duration)/60 as TotalMin
	from	Capping2_01_Viewing_Records
	group   by genre
	
	-- After Capping...
	select	eva.genre_description
			,count(1) as hits
			,sum(aug.viewing_duration)/60 as TotalMin
	from	angeld.Vespa_daily_augs_20120810 as aug
			inner join sk_prod.vespa_events_viewed_all as eva
			on aug.cb_row_id = eva.pk_viewing_prog_instance_fact
	group by eva.genre_description


	
	
/*			B: Checking Capping figures and proportions...			*/
	

-- B01: Number/% of events with short duration cap applied (<6 sec of viewing)... +Distribution check (A04)
	Insert into b01
		select  dk_event_start_datehour_dim
				,live_recorded
				,event_start_date_time_utc
				,account_number
				,service_key
				,genre_description
				,duration
		from    sk_prod.vespa_events_viewed_all
		where   dk_event_start_datehour_dim >= 2012081000 
		and     dk_event_start_datehour_dim<= 2012081023
		and 	panel_id in (4,12)
		and     duration < 6 -- 2,628,454 records
		
		/* Live_recorded distribution... generic query: D01-1			*/ 
			-- 57% of Live events		(1,503,654 events)
			-- 43% of Recorded events	(1,124,800 events)
			
		/* Day of the week distribution... generic query: none  		*/
			-- 100% falling in week day..
			
	    /* Event Start hour distribution... generic query: D01-2		*/
		
		/* PS_flag distribution... generic query: D01-3					*/
		
		/* Channel Pack distribution... generic query: D0-4				*/
		
		
-- Worth to mention: 	The nature of the capping process on Vespa is to cut off all events lasting less thatn 6 seconds on the very begining
--						Hence, the only place to check at this bit is on events_viewed_all

		

-- B02: Duration/% of events minimun cap applied...+Distribution check (A04)
	
	insert into b02
		select  convert(varchar(23),adjusted_event_start_time)
				,live
				,box_subscription
				,pack
				,initial_genre
				,event_dur_mins
		from    CP2_event_listing
		where   capped_event = 0 --angeld: shouldn't this be 1...
		and     max_dur_mins = 20
	
	/* Live_Recorded distribution... generic query:	D01-1.2				*/
	
	/* Day of the week distribution... generic query: none				*/
	
	/* event start hour distribution... generic query: D01-2			*/
	
	/* ps flag*/
	select 
			pack
			,count(1) as hits
			,sum(duration) as TotalMin
	from    b02
	group by pack
	
	/* genre */
	select
			genre_description
			,count(1) as hits
			,sum(duration) as TotalMins
	from b02
	group by genre_description
	

-- B03 - B04 : Number/% Events with long duration cap applied (random process, first programme)...+Distribution check (A04)


	-- Creating the Base table...
		select  eva.*
				,case   when
							cewe.capped_event_end_time is not null
							and firstrow is null
						then 1 else 0
						end as FirstProgrammeRule
				,case   when
							cewe.capped_event_end_time is not null
							and firstrow is not null
						then 1 else 0
						end as RandomeProcessRule  
				,cewe.max_dur_mins
		into    b0304 -- 
		from    (
					select  live_recorded
							,event_start_date_time_utc
							,subscriber_id
							,account_number
							,service_key
							,genre_description
					from    sk_prod.VESPA_EVENTS_viewed_ALL 
					where	dk_event_start_datehour_dim >= 2012081000 
					and     dk_event_start_datehour_dim <= 2012081023 
					and 	panel_id in (4,12)
				)as eva
				inner join CP2_capped_events_with_endpoints as cewe
				on  eva.subscriber_id = cewe.subscriber_id
				and eva.event_start_date_time_utc = cewe.adjusted_event_start_time-- 8966349 row(s) affected
	
	/* 	live_recorded 		*/
		select  live_recorded
				,case when firstprogrammerule = 1 then count(1)             end as FPhits
				,case when firstprogrammerule = 1 then sum(max_dur_mins)    end as FPTotalMin
				,case when RandomeProcessRule = 1 then count(1)             end as RPhits
				,case when RandomeProcessRule = 1 then sum(max_dur_mins)    end as RPTotalMin
		from    b0304
		group   by live_recorded, firstprogrammerule, RandomeProcessRule
	
	/* 	event start hour 	*/
		select 
				datepart(hour,event_start_date_time_utc) as _hour
				,case when firstprogrammerule = 1 then count(1)             end as FPhits
				,case when firstprogrammerule = 1 then sum(max_dur_mins)    end as FPTotalMin
				,case when RandomeProcessRule = 1 then count(1)             end as RPhits
				,case when RandomeProcessRule = 1 then sum(max_dur_mins)    end as RPTotalMin
		from	b0304
		group by datepart(hour,event_start_date_time_utc), firstprogrammerule, RandomeProcessRule
		
	/*	ps_flag				*/
		select 
				csi.ps_flag
				,case when firstprogrammerule = 1 then count(1)             end as FPhits
				,case when firstprogrammerule = 1 then sum(max_dur_mins)    end as FPTotalMin
				,case when RandomeProcessRule = 1 then count(1)             end as RPhits
				,case when RandomeProcessRule = 1 then sum(max_dur_mins)    end as RPTotalMin
		from    b0304 as b
				inner join csi
				on  b.subscriber_id = convert(integer,csi.si_external_identifier)
				and csi.rank_ = 1 
		group   by csi.ps_flag, b.firstprogrammerule, b.RandomeProcessRule

	
	/*	Channel Pack		*/
	    select 	ska.channel_pack
				,case when firstprogrammerule = 1 then count(1)             end as FPhits
				,case when firstprogrammerule = 1 then sum(max_dur_mins)    end as FPTotalMin
				,case when RandomeProcessRule = 1 then count(1)             end as RPhits
				,case when RandomeProcessRule = 1 then sum(max_dur_mins)    end as RPTotalMin
		from 	B0304 as eva
				inner join vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES as ska
				on eva.service_key = ska.service_key
		group by ska.channel_pack,firstprogrammerule,RandomeProcessRule

		
	/*	genre				*/
	    select	genre_description
				,case when firstprogrammerule = 1 then count(1)             end as FPhits
				,case when firstprogrammerule = 1 then sum(max_dur_mins)    end as FPTotalMin
				,case when RandomeProcessRule = 1 then count(1)             end as RPhits
				,case when RandomeProcessRule = 1 then sum(max_dur_mins)    end as RPTotalMin
		from	b0304
		group by genre_description,firstprogrammerule,RandomeProcessRule	

		
		
-- B05: Number/% of Capped/uncapped events...+Distribution check (A04)

		select  
				viewing_starts 
				,timeshifting             
				,account_number
				--,pack              
				,genre          
				,viewing_duration	
				,capped_flag
        into b05	
		from CP2_capped_data_holding_pen
		
	/* Live_Recorded distribution... generic query:	D01-1.2				*/
		select  substr(convert(varchar(10),viewing_starts),9,2) as _day
				,case   when lower(timeshifting) = 'live'      then count(1) end as liveHits
				,case   when lower(timeshifting) = 'live'      then sum(viewing_duration)/60 end as liveMins
				,case   when lower(timeshifting) in ('playback7','playback28','vosdal')  then count(1) end as RecHits
				,case   when lower(timeshifting) in ('playback7','playback28','vosdal')  then sum(viewing_duration)/60 end as RecMin
		from 	b05
		group by substr(convert(varchar(10),viewing_starts),9,2), timeshifting
	
	
	/* Day of the week distribution... generic query: none				*/
	
		--None Applicable...
		
		
	/* event start hour distribution... generic query: D01-2			*/
		select 
				datepart(hour,viewing_starts) as _hour
				,count(1) as hits
				,sum(viewing_duration)/60 as TotalMin
		from	b05
		group by datepart(hour,viewing_starts)
		
	/* ps flag 															*/
		-- Seems to be not applicable...
		
	/*channel pack ... generic query: D01-3								*/
	
	/* genre 															*/
		select 
				genre
				,count(1) as hits
				,sum(viewing_duration) as TotalMin
		from 	b05
		group by genre
		
-- B06: Duration/% of viewing before capping...+Distribution check (A04)

	/*killed with section A*/

	
-- B07: Ntiles Distribution...

	--angeld: this shouldn't be a live playback entiles (ntile_lp)...

	/* live_recorded */
		select 
				ntile_lp
				,case when live = 0 then count(1) end as RecHits
				,case when live = 1 then count(1) end as LiveHits
		from    CP2_ntiles_week
		group   by ntile_lp, live
		order   by ntile_lp
	
	/* Day of Week */
		
		--None Applicable...
		
		
	/*	Event start Hour */
		select 
				ntile_lp
				,event_start_hour
				,count(1) as hits
		from    CP2_ntiles_week
		group   by ntile_lp, event_start_hour
		order   by ntile_lp
	
	/* ps_flag */
		select 
				ntile_lp
				,box_subscription
				,count(1) as hits
		from    CP2_ntiles_week
		group   by ntile_lp, box_subscription
		order   by ntile_lp, box_subscription
	
	/* Channel pack */
		select 
				ntile_lp
				,pack_grp
				,count(1) as hits
		from    CP2_ntiles_week
		group   by ntile_lp, pack_grp
		order   by ntile_lp, pack_grp
	
	/* genre */			
		select 
				ntile_lp
				,initial_genre
				,count(1) as hits
		from    CP2_ntiles_week
		group   by ntile_lp, initial_genre
		order   by ntile_lp, initial_genre
		

-- B08: Segments' thresholds (in Minutes)...

		select  live,event_start_day,event_start_hour,pack_grp,initial_genre,cap_ntile,max(min_dur_mins)
		from    CP2_h23_3
		group   by live,event_start_day,event_start_hour,pack_grp,initial_genre,cap_ntile
		order   by live,event_start_day,event_start_hour,pack_grp,initial_genre,cap_ntile

		select  live,event_start_day,event_start_hour,pack_grp,initial_genre,cap_ntile,max(min_dur_mins)
		from    CP2_h20_22
		group   by live,event_start_day,event_start_hour,pack_grp,initial_genre,cap_ntile
		order   by live,event_start_day,event_start_hour,pack_grp,initial_genre,cap_ntile

		select  live,event_start_day,event_start_hour,pack_grp,initial_genre,cap_ntile,max(min_dur_mins)
		from    CP2_h15_19
		group   by live,event_start_day,event_start_hour,pack_grp,initial_genre,cap_ntile
		order   by live,event_start_day,event_start_hour,pack_grp,initial_genre,cap_ntile
		
		select  live,event_start_day,event_start_hour,pack_grp,initial_genre,cap_ntile,max(min_dur_mins)
		from    CP2_h20_22
		group   by live,event_start_day,event_start_hour,pack_grp,initial_genre,cap_ntile
		order   by live,event_start_day,event_start_hour,pack_grp,initial_genre,cap_ntile


-- B09: Num of thresholds above and below the max and min...

	-- Out of aboves queries, what I've done is to merge the results into a single table called "cap"... then

		-- Number of thresholds below the minimum...
		select  count(1) as hits
		from    cap
		where   expression <20 -- 203

		-- Number of thresholds above the maximum...
		select  count(1) as hits
		from    cap
		where   expression>120 -- 1

		-- Total number of capping ntiles...
		select count(1) from cap -- 495




/*			C: Creating Process tables for Section B...			*/

-- C01: Creating table statements...

	--B01 table (events with less than 6 seconds duration)...
	if object_id('b01') is not null
		drop table b01;
	commit;
	
	create table b01(
		dk_event_start_datehour_dim integer
		,live_recorded              varchar(20)
		,event_start_date_time_utc  timestamp
		,account_number             varchar(20)
		,service_key                bigint
		,genre_description          varchar(20)
		,duration					integer
	);
	commit;
	
	-- B02 table(events with minimun capped applied)...
	if object_id('b02') is not null
		drop table b02;
	commit;
	
	create table b02(
		event_start_date_time_utc	varchar(23)
		,live_recorded				bit
		,PS_flag					varchar(1)
		,pack						varchar(100)
		,genre_description			varchar(25)
		,duration					integer
	);
	commit;
	
	
	
-- C02: Creating indexes statements...

	-- b01 indexes...
	create lf   index i_b01_1_lf    on b01(dk_event_start_datehour_dim);
	create lf   index i_b01_2_lf    on b01(live_recorded);
	create hg   index i_b01_1_hg    on b01(event_start_date_time_utc);
	create hg   index i_b01_2_hg    on b01(account_number);
	create lf   index i_b01_3_lf    on b01(service_key);
	create lf   index i_b01_4_lf    on b01(genre_description);
	commit;
	
	-- b02 indexes
	create hg index i_b02_1_hg	on b02(adjusted_event_start_time);
	create lf index i_b02_2_lf	on b02(box_subscription);
	create lf index i_b02_3_lf	on b02(pack);
	create lf index i_b02_4_lf	on b02(initial_genre);
	commit;
	
	
	
	
	
	
	
	
	
/*			D: Generic Distribution check for section B...			*/
			
-- D01: Generic check for distribution accross segments for capping...

-- Checking distribution for Live_recorded... 			D01-1
	
	select  substr(convert(varchar(10),dk_event_start_datehour_dim),7,2) as _day
			,case   when lower(live_recorded) = 'live'      then count(1) end as liveHits
			,case   when lower(live_recorded) = 'live'      then sum(duration)/60 end as liveMins
			,case   when lower(live_recorded) = 'recorded'  then count(1) end as RecHits
			,case   when lower(live_recorded) = 'recorded'  then sum(duration)/60 end as RecMins
	from 	<B0N table of B section you want to check>
	group by substr(convert(varchar(10),dk_event_start_datehour_dim),7,2), live_recorded
	
-- D01-1.2
	select  substr(convert(varchar(10),event_start_date_time_utc),9,2) as _day
			,case   when lower(live_recorded) = 'live'      then count(1) end as liveHits
			,case   when lower(live_recorded) = 'live'      then sum(duration)/60 end as liveMins
			,case   when lower(live_recorded) = 'recorded'  then count(1) end as RecHits
			,case   when lower(live_recorded) = 'recorded'  then sum(duration)/60 end as RecMins
	from 	<B0N table of B section you want to check>
	group by substr(convert(varchar(10),event_start_date_time_utc),9,2), live_recorded
	

-- Checking distribution for event start hour... 		D01-2
	select 
			datepart(hour,event_start_date_time_utc) as _hour
			,count(1) as hits
			,sum(duration)/60 as TotalMin
	from	<B0N table of B section you want to check>
	group by datepart(hour,event_start_date_time_utc)

-- Checking distribution for ps_flag...					D01-3

	select  si_external_identifier
			,account_number
			,rank() over(partition by si_external_identifier order by effective_from_dt desc) as rank_
			,left(si_service_instance_type,1) as ps_flag
	  into  csi
	  from  sk_prod.cust_service_instance
	  where si_service_instance_type like '%DTV%'

	select  distinct account_number
			,sum(duration) as duration
	into    #accountlookup
	from    <B0N table of B section you want to check>
	group by account_number
	
	select  ps_flag
			,count(1) as hits
			,sum(duration)/60 as TotalMin
	from    #accountlookup as aclp
			inner join csi as csi
			on aclp.account_number = csi.account_number
			and csi.rank_=1
	group by ps_flag

-- D01-3.2
	select 
			ps_flag
			, count(1) as hits
			, sume(duration) as TotalMin
	from b02
	group by ps_flag
	
-- Checking distribution for channel pack				D01-4

	select 	ska.channel_pack
			,count(1) as hits
			,sum(eva.duration)/60 as TotalMin
	from 	<B0N table of B section you want to check> as eva
			inner join vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES as ska
			on eva.service_key = ska.service_key
	group by ska.channel_pack
	

-- Checking distribution for Genre						D01-5

	select	genre_description
			,count(1) as hits
			,sum(duration)/60 as TotalMin
	from	<B0N table of B section you want to check>
	group by genre_description
	
	
	
	
-- New test ran...
/*
Checking the proportion of minutes capped against none capped
vespa 11%, CBI 28%
*/


-- this is giving me the total minutes capped...
select sum(datediff(ss,capped_event_end_time, x_adjusted_event_end_time))/60 cappedMinutes
from CP2_capped_data_holding_pen
where capped_event_end_time is not null

select capped/uncaped as proportion 
from(
        select  sum(datediff(ss, adjusted_event_start_time,x_adjusted_event_end_time))/60 as uncaped
                ,sum(datediff(ss, adjusted_event_start_time, capped_event_end_time))/60   as capped
        from    CP2_capped_data_holding_pen
        where   capped_event_end_time is not null
) as n