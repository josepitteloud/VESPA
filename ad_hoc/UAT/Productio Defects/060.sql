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

**Project Name: 					PRODUCTION DEFECT 060
**Analysts:							Angel Donnarumma (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):							Jose Loureda
**Stakeholder:						Vespa Team

--------------------------------------------------------------------------------------------------------------
*/

 ------------------------------------------------------------------------------------------------------
/* CHECKING SCALING WEIGHS ASSIGNED TO EVENTS ARE CORRECT AND CONSISTENT FROM SOURCES ON NETEZZA PROD*/

-- sample taken for 28th of April

-------------------------------------------------------------------------------------------------------

/*
	AD: The card talks about Slot only but I think would be good to check this on programme level as well...
		but just after I finish with SLOT...
	AD: Confirmed with CBI, on Netezza side the Scaling logic aggregates everything into a single output table
		that sources the fact table with respect to allocate scaling weights, hence just one point of comparison...
	AD: All tests where made on the 27th, 28th and 29th of April (Dates when 2.6 quicked off)...
*/

-- smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE
-- dis_reference..FINAL_SCALING_EVENT_HISTORY


-- lets checks the join and capture of weights here...

select	count(1) as mismaches
from	(
			select	slots.DTH_VIEWING_EVENT_ID		as theid
					,slots.ACTUAL_WEIGHT			as slotweight
					,scaling.WEIGHT_SCALED_VALUE	as scaling_weight
			from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE as slots
					inner join	(
									select 	dth_viewing_event_id
											,weight_scaled_value
									from	dis_reference..FINAL_SCALING_EVENT_HISTORY
									where	event_start_date = '2013-04-28 00:00:00'
								) as scaling
					on	slots.DTH_VIEWING_EVENT_ID = scaling.DTH_VIEWING_EVENT_ID
			where	dk_event_start_datehour_dim like '20130428%'
		) as base
where 	slotweight <> scaling_weight -- 0 hits (this is good)...


-- Also tested for Slots...

select	count(1) as mismaches
from	(
			select	slots.DTH_VIEWING_EVENT_ID		as theid
					,slots.ACTUAL_WEIGHT			as slotweight
					,scaling.WEIGHT_SCALED_VALUE	as scaling_weight
			from	smi_dw..VIEWING_SLOT_INSTANCE_FACT_VOLATILE as slots
					inner join	(
									select 	dth_viewing_event_id
											,weight_scaled_value
									from	dis_reference..FINAL_SCALING_EVENT_HISTORY
									where	event_start_date = '2013-04-28 00:00:00'
								) as scaling
					on	slots.DTH_VIEWING_EVENT_ID = scaling.DTH_VIEWING_EVENT_ID
			where	dk_event_start_datehour_dim like '20130428%'
		) as base
where 	slotweight <> scaling_weight -- 0 hits (this is good)...



-----------------------------------------------------------------
/* CHECKING CONGRUENCE OF WEIGHTS ON TABLES ON OLIVE PROD SIDE */
-----------------------------------------------------------------


describe sk_prod.slot_data
describe sk_prod.VIQ_VIEWING_DATA_SCALING

-- GENERAL CHECKS OF INTEGRITY...

select  left(cast(viewed_start_date_key as varchar(10)),8) as thedate
        ,count(1)
from    sk_prod.slot_data
where   viewed_start_date_key between 2013042700 and 2013050723
group   by  thedate

/*
thedate,count(1)
'20130507',52
'20130506',3408883 --> checking here first...
'20130505',2534338
'20130504',3702074
'20130503',3767665
'20130502',13519287
'20130501',5767635
'20130430',5344072
'20130429',4198229
'20130428',15281249
'20130427',4016655
*/

select  count(1)
from    sk_prod.VIQ_VIEWING_DATA_SCALING
where   scaling_date_key = 2013050600 --  in AVG 300K

-------------------------

-- checking how to link both tables... I think we should be using houshold_key

select  count(distinct household_key)
from    sk_prod.slot_data
where   viewed_start_date_key between 2013042700 and 2013050723 -- 410961 (what expected to be maching...)

select  count(distinct slot.household_key) as hits
from    sk_prod.slot_data as slot
        inner join sk_prod.viq_viewing_data_scaling as scaling
        on  slot.household_key = scaling.household_key
        and scaling.scaling_date_key = 2013050600
where   slot.viewed_start_date_key between 2013050600 and 2013050623 -- 219512 (only 53% matching, seems low)


select  *
from    (
            select  distinct
                    slot.household_key
                    ,slot.scaling_factor
                    ,scaling.calculated_scaling_weight
                    ,left(cast(scaling.scaling_date_key as varchar(10)),8) as dateA
                    ,left(cast(slot.viewed_start_date_key as varchar(10)),8) as dateB
            from    sk_prod.slot_data as slot
                    inner join sk_prod.viq_viewing_data_scaling as scaling
                    on  slot.household_key = scaling.household_key
                    and scaling.scaling_date_key = 2013050600
            where   slot.viewed_start_date_key between 2013050600 and 2013050623
            and     slot.household_key > 0
        ) as base
where   scaling_factor <> calculated_scaling_weight -- around 5k events for this date don't have the right weights

-- it seems they are mapping these tables in a way that is not allowing to capture the right weights for all the households...
-- a way different from what done above...