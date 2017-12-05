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

**Project Name: 					PRODUCTION DEFECT 065
**Analysts:							Angel Donnarumma (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):							Jose Loureda
**Stakeholder:						Vespa Team

--------------------------------------------------------------------------------------------------------------
*/
---------------------------------------------------------------
/* MEASURING SAMPLE SIZE FOR THE 28TH OF APRIL ON FACT TABLE */
---------------------------------------------------------------
/*
	AD: need to confirm with someone at cbi (Kumar) about seeing so few records for dates...
	AD: so yeah it was me pointing to wrong table... now we have couple of them sufixed
	  	with _PRE_2_6 which I'm assuming they have everything...
*/
-- smi_dw..VIEWING_PROGRAMME_INSTANCE_FACT
-- smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE

select	count(1)
from	smi_dw..VIEWING_PROGRAMME_INSTANCE_FACT -- 147,842,690
where	dk_event_start_datehour_dim like '20130428%' -- 62,129,757


-- checking the same for slots

select	count(1)
from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE -- 144,886,288
where	dk_event_start_datehour_dim between 2013042800 and 2013042823 -- 46,709,960


-- now how many cases do we have missing scaling weights...

-- PROGRAMMES:
select	count(1)
from	smi_dw..VIEWING_PROGRAMME_INSTANCE_FACT -- 147,842,690
where	dk_event_start_datehour_dim like '20130428%' -- 62,129,757
and		weight_scaled is null -- 38,979,005 (62%)

-- SLOTS:
select	count(1)
from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE -- 144,886,288
where	dk_event_start_datehour_dim between 2013042800 and 2013042823 -- 46,709,960
and		actual_weight is null -- 16,646,141 (36%)


-- Checking Programmes first at has the biggest slice and is already in use by VESPA...

/*
Both Programmes and Slots should be linked to this table through viewing_event_id as this value is assigned to anything that belongs
to a given event from a box...
*/

-- how does this looks like?...
select * from DIS_REFERENCE..FINAL_SCALING_EVENT_HISTORY limit 10

-- So in theory all dth_viewing_event_id in scaling table should be in programme fact table... is that true?

select	count(1) as hits														
from	(
			select	distinct(DTH_VIEWING_EVENT_ID) as theid						
			from	dis_reference..FINAL_SCALING_EVENT_HISTORY
			where	event_start_date = '2013-04-28 00:00:00' 					-- 19,391,766
		) as scaling
		inner join	(
						select	distinct DTH_VIEWING_EVENT_ID 	as theid		 
						from	smi_dw..VIEWING_PROGRAMME_INSTANCE_FACT 		
						where	dk_event_start_datehour_dim like '20130428%' 	-- 50,147,206
					) as progs
		on	scaling.theid = progs.theid											-- 19,382,632 (missing around 10k)
		
		
-- what's the impact, so how many records in average those 10k represent in programme fact table?... 
-- is this reason enough to cope with the 38M records laking weights... I don't think so but let's check...

select	count(1) 	as hits		 
from	smi_dw..VIEWING_PROGRAMME_INSTANCE_FACT 		
where	dk_event_start_datehour_dim like '20130428%'
and		dth_viewing_event_id not in	(
										select	distinct(DTH_VIEWING_EVENT_ID) as theid						
										from	dis_reference..FINAL_SCALING_EVENT_HISTORY
										where	event_start_date = '2013-04-28 00:00:00'
									) -- 38,979,005 = 38,979,005 mmmm... so the issue gotta be one stage before this scaling table...
									
/*
	Pulling up a sample of this DTH_VIEWING_EVENT_ID missing in the Scaling table to check if they should have been dropped on first instance
	based on what required...
	
	Filters are based on various things, though I think most important is date-time and panel hence, now comparing a sample of weight attributed vs
	weight not attributed viewing_ids...
*/


select	distinct(DTH_VIEWING_EVENT_ID) as theids
from	smi_dw..VIEWING_PROGRAMME_INSTANCE_FACT	
where	dk_event_start_datehour_dim like '20130428%'
and		DTH_VIEWING_EVENT_ID not in	(
										select	distinct(DTH_VIEWING_EVENT_ID) as theid						
										from	dis_reference..FINAL_SCALING_EVENT_HISTORY
										where	event_start_date = '2013-04-28 00:00:00'
									) -- 38,979,005 = 38,979,005 mmmm... so the issue gotta be one stage before this scaling table...
									
									
select	DTH_VIEWING_EVENT_ID,*
from	smi_dw..VIEWING_PROGRAMME_INSTANCE_FACT	
where	dk_event_start_datehour_dim like '20130428%'
and		DTH_VIEWING_EVENT_ID not in	(
										select	distinct(DTH_VIEWING_EVENT_ID) as theid						
										from	dis_reference..FINAL_SCALING_EVENT_HISTORY
										where	event_start_date = '2013-04-28 00:00:00'
									) -- 38,979,005 = 38,979,005 mmmm... so the issue gotta be one stage before this scaling table...
limit 	100
									
						