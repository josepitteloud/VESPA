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
**Project Name:                                                 OPS 2.0
**Analysts:                             James McKane (james.mckane@skyiq.co.uk)
**Lead(s):                              Angel Donnarumma
**Stakeholder:                          Operational Reports / SIG
**Due Date:                             03/03/2014
**Project Code (Insight Collation):
**Sharepoint Folder:                    
                                                                        
**Business Brief:

        This module assembles the Panel balance Traffic Lights fur use in PanMan and Xdash reports

**Modules:

        M11: MASVG Panel Measurements
        M11.0 - Initialising environment
        M11.1 - VESPA Traffic Lights Created
        M11.2 - QAing results
        M11.3 - Setting Access Privileges
        M11.4 - Returning Results

**Stats:
	
	6 Minutes run... End-to-End...
--------------------------------------------------------------------------------------------------------------
*/

-----------------------------------
-- M11.0 - Initialising environment
-----------------------------------

create or replace procedure sig_masvg_m11_panel_measurement_generator
as begin


    MESSAGE cast(now() as timestamp)||' | Beginig M11.0 - Initialising environment' TO CLIENT
    

	-- Accounts and profiling:
	if object_id('vespa_analysts.Vespa_all_households') is not null
		drop table Vespa_all_households
		
	-- This guy eventually holds all households, not just those which have returned data,
	-- though it happens to get data return metrics on it as well.
	create table Vespa_all_households(
		account_number                  varchar(20)         not null primary key
		,hh_box_count                   tinyint             not null
		,most_recent_enablement         date                not null
		,reporting_categorisation       varchar(20)
		,panel                          varchar(10)
		,scaling_segment_ID             int
		,non_scaling_segment_ID         int
		,reporting_quality              float
	)

	commit
	grant select on Vespa_all_households to vespa_group_low_security
	commit
	
	MESSAGE cast(now() as timestamp)||' | @ M11.0: Table ''Vespa_all_households'' creation DONE' TO CLIENT    

	/*-*********** Segmentation management (except the names and lookups) **********-*/

	if object_id('vespa_analysts.Vespa_Scaling_Segment_Profiling') is not null
		drop table Vespa_Scaling_Segment_Profiling

	create table Vespa_Scaling_Segment_Profiling (
		Panel                                               varchar(10)
		,scaling_segment_id                                 int             -- All combinations of variables used in scaling
		,scaling_segment_name                               varchar(150)
		,non_scaling_segment_id                             int             -- All combinations of other variables that aren't used in scaling
		,non_scaling_segment_name                           varchar(100)
		,Sky_Base_Households                                int             -- duplicated across panels, but that's okay
		,Panel_households                                   int
		,Acceptably_reliable_households                     int             -- Some denormalisation in here, this is closer to the format in which results are delivered
		,Unreliable_households                              int
		,Zero_reporting_households                          int
		,Recently_enabled_households                        int
		,Acceptably_reporting_index                         decimal(6,2)  default null
		,primary key (scaling_segment_id, non_scaling_segment_id, panel)
	)

	-- These guys needs their own indexes because we'll need to join through them:
	create hg index for_joining on Vespa_Scaling_Segment_Profiling (non_scaling_segment_id)
	commit
	grant select on Vespa_Scaling_Segment_Profiling to vespa_group_low_security
	commit

	MESSAGE cast(now() as timestamp)||' | @ M11.0: Table ''Vespa_Scaling_Segment_Profiling'' creation DONE' TO CLIENT

	/*-*************** QUASI-RESULTS STRUCTURES! ***************-*/

	-- So this table holds all the single variable aggregation results and for
	-- the result pluss, we just filter on the panel and aggregation variable
	-- to pull out what we need in each instance.
	if object_id('vespa_analysts.Vespa_all_aggregated_results') is not null
		drop table Vespa_all_aggregated_results
		
	create table Vespa_all_aggregated_results (
		panel                                               varchar(10)
		,aggregation_variable                               varchar(30)
		,scaling_or_not                                     bit
		,variable_value                                     varchar(60)
		,Sky_Base_Households                                int
		,Panel_Households                                   int
		,Acceptable_Households                              int
		,Unreliable_Households                              int
		,Zero_reporting_Households                          int
		,Recently_enabled_households                        int
		,Good_Household_Index                               decimal(6,2)
		,primary key (panel, aggregation_variable, variable_value)
	)

	commit
	grant select on Vespa_Scaling_Segment_Profiling to vespa_group_low_security
	commit

	MESSAGE cast(now() as timestamp)||' | @ M11.0: Table ''Vespa_all_aggregated_results'' creation DONE' TO CLIENT    

	/*-*************** Traffic Light Hist Table ***************-*/
	if object_id('vespa_analysts.vespa_traffic_lights_hist') is null
	create table vespa_traffic_lights_hist (
		 panel                                              varchar(10)
		,variable_name                                      varchar(30)
		,sequencer                                          int
		,imbalance_rating                                   float
		,weekending                                         date
	)

	commit
	grant select on vespa_traffic_lights_hist to vespa_group_low_security
	commit
	
	declare @profiling_day date
	-- so we're going to set this to last Thursday when everything was updated?
	-- SAV refresh permitting of course. Oh, hey, there's a cheap way of doing it;

	select @profiling_day = max(sbv.weekending)from SIG_SINGLE_BOX_VIEW as SBV

	declare @weekending date

	select @weekending =	case	when datepart(weekday,@profiling_day) = 7 then @profiling_day
									else (@profiling_day + (7 - datepart(weekday,@profiling_day))) 
							end

	if exists	(
					select	first *
					from 	vespa_traffic_lights_hist
					where 	weekending = @weekending
				)
	begin
		delete  from vespa_traffic_lights_hist where weekending = @weekending
		commit
	end

	MESSAGE cast(now() as timestamp)||' | @ M11.0: Initialisation DONE' TO CLIENT

	---------------------------------------
	-- M11.1 - VESPA Traffic Lights Created
	---------------------------------------


	--Acceptable Reporting - VESPA Panel
	insert into Vespa_all_households	(
											account_number
											,hh_box_count       -- not directly used? but might be interesting
											,most_recent_enablement
											,reporting_categorisation
											,reporting_quality
											,panel
											,scaling_segment_id
											,non_scaling_segment_id
									   )
	select	distinct sbv.account_number
			,count(1)
			,max(Enablement_date)
			,sav.reporting_performance
			,min(sbv.reporting_quality)   -- Used much later in the box selection bit, but may as well build it now
			,'DP'               -- This guy should be unique per account, we test for that coming off SBV
			,sav.scaling_segment_id
			,sav.non_scaling_segment_id
	from 	SIG_SINGLE_BOX_VIEW as SBV
			inner join SIG_SINGLE_ACCOUNT_VIEW as SAV
			on	sbv.account_number = sav.account_number
	where 	sav.panel in ('VESPA', 'VESPA11')
	and 	sbv.status_vespa = 'Enabled'
	group 	by	sbv.account_number
				,sav.reporting_performance
				,sav.panel
				,sav.scaling_segment_id
				,sav.non_scaling_segment_id

	MESSAGE cast(now() as timestamp)||' | @ M11.1: Acceptable Reporting - VESPA Panel DONE' TO CLIENT

	--Acceptable Reporting - ALT Panels
	insert into Vespa_all_households(
										account_number
										,hh_box_count       -- not directly used? but might be interesting
										,most_recent_enablement
										,reporting_categorisation
										,reporting_quality
										,panel
										,scaling_segment_id
										,non_scaling_segment_id
									)
	select	distinct sbv.account_number
			,count(1)
			,max(Enablement_date)
			,sav.reporting_performance
			,min(sbv.reporting_quality)   -- Used much later in the box selection bit, but may as well build it now
			,'AP'               -- This guy should be unique per account, we test for that coming off SBV
			,sav.scaling_segment_id
			,sav.non_scaling_segment_id
	from 	SIG_SINGLE_BOX_VIEW as SBV
			inner join SIG_SINGLE_ACCOUNT_VIEW as SAV
			on	sbv.account_number = sav.account_number
	where 	sav.panel in ('ALT5', 'ALT6', 'ALT7')
	and 	sbv.status_vespa = 'Enabled'
	group 	by	sbv.account_number
				,sav.reporting_performance
				,sav.panel
				,sav.scaling_segment_id
				,sav.non_scaling_segment_id

	MESSAGE cast(now() as timestamp)||' | @ M11.1: Acceptable Reporting - ALT Panels DONE' TO CLIENT

	-- So we need to start off with a view of the whole Sky base, and then add in the details for the stuff on each panel...
	select 	scaling_segment_id
			,non_scaling_segment_id
			,count(1) as Sky_Base_Households
	into 	#sky_base_segmentation
	from 	SIG_SINGLE_ACCOUNT_VIEW
	group 	by	scaling_segment_ID
				,non_scaling_segment_id
				
	-- It has to go into a temp table because we duplicate all these number for each panel

	commit

	--Panels Totals
	select	panel
			,count(1) as panel_reporters
	into 	#panel_totals
	from 	Vespa_all_households
	where 	reporting_categorisation = 'Acceptable'
	group 	by 	panel

	commit


	--Scaling Segment Profiling
	insert into Vespa_Scaling_Segment_Profiling	(
													panel
												   ,scaling_segment_id
												   ,non_scaling_segment_id
												   ,Sky_Base_Households
												)
	select 	pt.panel
			,sb.*
	from 	#sky_base_segmentation as sb
			cross join	(
							select  distinct panel
							from    Vespa_all_households
						)   as pt
	where 	scaling_segment_id is not null            -- segment data missing from approx. 250k accounts
	and 	non_scaling_segment_id is not null          -- segment data missing from approx. 250k accounts
	
	commit

	MESSAGE cast(now() as timestamp)||' | @ M11.1: Scaling Segment Profiling CREATED' TO CLIENT

	--Panel Segmentation
	select	panel
			,scaling_segment_id
			,non_scaling_segment_id
			,count(1) as Panel_Households
			,sum(case when reporting_categorisation = 'Acceptable'       then 1 else 0 end) as Acceptably_reliable_households
			,sum(case when reporting_categorisation = 'Unreliable'       then 1 else 0 end) as Unreliable_households
			,sum(case when reporting_categorisation = 'Zero reporting'   then 1 else 0 end) as Zero_reporting_households
			,sum(case when reporting_categorisation = 'Recently enabled' then 1 else 0 end) as Recently_enabled_households
	into 	#panel_segmentation
	from 	Vespa_all_households as hr
	where 	scaling_segment_ID is not null and non_scaling_segment_id is not null
	group 	by	panel
				,scaling_segment_ID
				,non_scaling_segment_id

	commit
	create unique index fake_pk on #panel_segmentation (panel, scaling_segment_id, non_scaling_segment_id)
	commit

	-- Now with the totals built for each panel, we can throw them into the table with the Sky base:
	update	Vespa_Scaling_Segment_Profiling
	set 	Panel_Households                = ps.Panel_Households
			,Acceptably_reliable_households = ps.Acceptably_reliable_households
			,Unreliable_households          = ps.Unreliable_households
			,Zero_reporting_households      = ps.Zero_reporting_households
			,Recently_enabled_households    = ps.Recently_enabled_households
	from 	Vespa_Scaling_Segment_Profiling
			inner join #panel_segmentation as ps 
			on	Vespa_Scaling_Segment_Profiling.panel                    = ps.panel
			and Vespa_Scaling_Segment_Profiling.scaling_segment_id       = ps.scaling_segment_id
			and Vespa_Scaling_Segment_Profiling.non_scaling_segment_id   = ps.non_scaling_segment_id

	commit
	drop table #sky_base_segmentation
	drop table #panel_segmentation


	-- We need the size of the sky base for indexing calculations
	declare @total_sky_base                 int

	select	@total_sky_base     = sum(Sky_Base_Households)
	from 	Vespa_Scaling_Segment_Profiling
	where 	panel ='DP'

	commit

	-- Patch in the scaling segment name from the lookup...
	update	Vespa_Scaling_Segment_Profiling
	set 	scaling_segment_name = ssl.scaling_segment_name
	from 	Vespa_Scaling_Segment_Profiling
			inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl 
			on	Vespa_Scaling_Segment_Profiling.scaling_segment_ID = ssl.scaling_segment_ID

	update	Vespa_Scaling_Segment_Profiling
	set 	non_scaling_segment_name = nss.non_scaling_segment_name
	from 	Vespa_Scaling_Segment_Profiling
			inner join vespa_analysts.sig_non_scaling_segments_lookup as nss 
			on	Vespa_Scaling_Segment_Profiling.non_scaling_segment_ID = nss.non_scaling_segment_ID

	MESSAGE cast(now() as timestamp)||' | @ M11.1: Scaling Segment Profiling DONE' TO CLIENT

	--Aggregated Results
	insert	into Vespa_all_aggregated_results
	select	ssp.panel
			,'UNIVERSE' -- Name of variable being profiled
			,1          -- Whether the variable is used for scaling or not (determintes the results sheet pull)
			,ssl.universe
			,sum(Sky_Base_Households)
			,sum(Panel_households)
			,sum(Acceptably_reliable_households)
			,sum(Unreliable_households)
			,sum(Zero_reporting_households)
			,sum(Recently_enabled_households)
			,null
	from 	Vespa_Scaling_Segment_Profiling as ssp
			inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl 
			on	ssp.scaling_segment_ID = ssl.scaling_segment_ID
	group 	by	ssp.panel
				,ssl.universe

	commit

	insert	into Vespa_all_aggregated_results
	select 	ssp.panel
			,'REGION'   -- Name of variable being profiled
			,1          -- Whether the variable is used for scaling or not (determintes the results sheet pull)
			,ssl.isba_tv_region
			,sum(Sky_Base_Households)
			,sum(Panel_households)
			,sum(Acceptably_reliable_households)
			,sum(Unreliable_households)
			,sum(Zero_reporting_households)
			,sum(Recently_enabled_households)
			,null
	from 	Vespa_Scaling_Segment_Profiling as ssp
			inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl 
			on	ssp.scaling_segment_ID = ssl.scaling_segment_ID
	group 	by	ssp.panel
				,ssl.isba_tv_region

	commit

	insert	into Vespa_all_aggregated_results
	select 	ssp.panel
			,'HHCOMP'
			,1
			,ssl.hhcomposition
			,sum(Sky_Base_Households)
			,sum(Panel_households)
			,sum(Acceptably_reliable_households)
			,sum(Unreliable_households)
			,sum(Zero_reporting_households)
			,sum(Recently_enabled_households)
			,null
	from 	Vespa_Scaling_Segment_Profiling as ssp
			inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl 
			on	ssp.scaling_segment_ID = ssl.scaling_segment_ID
	group 	by	ssp.panel
				,ssl.hhcomposition

	commit

	insert	into Vespa_all_aggregated_results
	select	ssp.panel
			,'PACKAGE'
			,1
			,ssl.package
			,sum(Sky_Base_Households)
			,sum(Panel_households)
			,sum(Acceptably_reliable_households)
			,sum(Unreliable_households)
			,sum(Zero_reporting_households)
			,sum(Recently_enabled_households)
			,null
	from 	Vespa_Scaling_Segment_Profiling as ssp 
			inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl 
			on	ssp.scaling_segment_ID = ssl.scaling_segment_ID
	group 	by	ssp.panel
				,ssl.package

	commit

	insert	into Vespa_all_aggregated_results
	select	ssp.panel
			,'TENURE'
			,1
			,ssl.tenure
			,sum(Sky_Base_Households)
			,sum(Panel_households)
			,sum(Acceptably_reliable_households)
			,sum(Unreliable_households)
			,sum(Zero_reporting_households)
			,sum(Recently_enabled_households)
			,null
	from 	Vespa_Scaling_Segment_Profiling as ssp
			inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl 
			on	ssp.scaling_segment_ID = ssl.scaling_segment_ID
	group 	by	ssp.panel
				,ssl.tenure

	commit

	insert	into Vespa_all_aggregated_results
	select 	ssp.panel
			,'BOXTYPE'
			,1
			,ssl.boxtype
			,sum(Sky_Base_Households)
			,sum(Panel_households)
			,sum(Acceptably_reliable_households)
			,sum(Unreliable_households)
			,sum(Zero_reporting_households)
			,sum(Recently_enabled_households)
			,null
	from 	Vespa_Scaling_Segment_Profiling as ssp
			inner join vespa_analysts.SC2_Segments_Lookup_v2_1 as ssl on ssp.scaling_segment_ID = ssl.scaling_segment_ID
	group 	by	ssp.panel
				,ssl.boxtype

	commit
	
	MESSAGE cast(now() as timestamp)||' | @ M11.1: Scaling Segment Aggregated Results DONE' TO CLIENT 

	-- Then other things that we're not scaling by, but we'd still like for panel balance:
	insert	into Vespa_all_aggregated_results
	select 	ssp.panel
			,'VALUESEG'
			,0          -- indicates we're not scaling by this, because these variables are pulled onto a different sheet
			,nss.value_segment
			,sum(Sky_Base_Households)
			,sum(Panel_households)
			,sum(Acceptably_reliable_households)
			,sum(Unreliable_households)
			,sum(Zero_reporting_households)
			,sum(Recently_enabled_households)
			,null
	from 	Vespa_Scaling_Segment_Profiling as ssp
			inner join vespa_analysts.sig_non_scaling_segments_lookup as nss 
			on	ssp.non_scaling_segment_ID = nss.non_scaling_segment_ID
	group 	by	ssp.panel
				,nss.value_segment

	commit

	insert	into Vespa_all_aggregated_results
	select 	ssp.panel
			,'MOSAIC'
			,0
			,nss.Mosaic_segment -- Special treatment for the MOSAIC segment names gets handled at the end
			,sum(Sky_Base_Households)
			,sum(Panel_households)
			,sum(Acceptably_reliable_households)
			,sum(Unreliable_households)
			,sum(Zero_reporting_households)
			,sum(Recently_enabled_households)
			,null
	from 	Vespa_Scaling_Segment_Profiling as ssp
			inner join vespa_analysts.sig_non_scaling_segments_lookup as nss 
			on	ssp.non_scaling_segment_ID = nss.non_scaling_segment_ID
	group 	by	ssp.panel
				,nss.Mosaic_segment

	commit

	insert	into Vespa_all_aggregated_results
	select 	ssp.panel
			,'FINANCIALSTRAT'
			,0
			,nss.Financial_strategy_segment
			,sum(Sky_Base_Households)
			,sum(Panel_households)
			,sum(Acceptably_reliable_households)
			,sum(Unreliable_households)
			,sum(Zero_reporting_households)
			,sum(Recently_enabled_households)
			,null
	from 	Vespa_Scaling_Segment_Profiling as ssp
			inner join vespa_analysts.sig_non_scaling_segments_lookup as nss 
			on	ssp.non_scaling_segment_ID = nss.non_scaling_segment_ID
	group 	by	ssp.panel
				,nss.Financial_strategy_segment

	commit

	insert	into Vespa_all_aggregated_results
	select 	ssp.panel
			,'ONNET'
			,0
			,case when nss.is_OnNet = 1 then '1.) OnNet' else '2.) OffNet' end
			,sum(Sky_Base_Households)
			,sum(Panel_households)
			,sum(Acceptably_reliable_households)
			,sum(Unreliable_households)
			,sum(Zero_reporting_households)
			,sum(Recently_enabled_households)
			,null
	from 	Vespa_Scaling_Segment_Profiling as ssp
			inner join vespa_analysts.sig_non_scaling_segments_lookup as nss 
			on	ssp.non_scaling_segment_ID = nss.non_scaling_segment_ID
	group 	by	ssp.panel
				,nss.is_OnNet

	commit

	insert	into Vespa_all_aggregated_results
	select 	ssp.panel
			,'SKYGO'
			,0
			,case when nss.uses_sky_go = 1 then '1.) Uses Sky Go' else '2.) No Sky Go' end
			,sum(Sky_Base_Households)
			,sum(Panel_households)
			,sum(Acceptably_reliable_households)
			,sum(Unreliable_households)
			,sum(Zero_reporting_households)
			,sum(Recently_enabled_households)
			,null
	from 	Vespa_Scaling_Segment_Profiling as ssp
			inner join vespa_analysts.sig_non_scaling_segments_lookup as NSS 
			on	ssp.non_scaling_segment_ID = nss.non_scaling_segment_ID
	group 	by	ssp.panel
				,nss.uses_sky_go

	commit

	MESSAGE cast(now() as timestamp)||' | @ M11.1: Non-Scaling Segment Aggregated Results DONE' TO CLIENT

	--Good Household Index
	update	Vespa_all_aggregated_results
	set 	Good_Household_Index =
							case    when pt.panel_reporters > 0  then 	(
																			case    when 200 < 100 * (Acceptable_Households) * @total_sky_base / convert(float, Sky_Base_Households) / pt.panel_reporters then 200
																					else       100 * (Acceptable_Households) * @total_sky_base / convert(float, Sky_Base_Households) / pt.panel_reporters
																			end
																		)
									else 0
							end
	from 	Vespa_all_aggregated_results
			left join #panel_totals as pt 
			on	Vespa_all_aggregated_results.panel = pt.panel

	MESSAGE cast(now() as timestamp)||' | @ M11.1: Good Household Index DONE' TO CLIENT


	--Traffic Lights Table
	insert	into vespa_traffic_lights_hist
	select  panel -- it gets denormalised in the extraction query though...
			,case aggregation_variable
				   when 'UNIVERSE'         then 'Universe'
				   when 'REGION'           then 'Region'
				   when 'HHCOMP'           then 'Household composition'
				   when 'PACKAGE'          then 'Package'
				   when 'TENURE'           then 'Tenure'
				   when 'BOXTYPE'          then 'Box type'
				   when 'VALUESEG'         then 'Value segment'
				   when 'MOSAIC'           then 'MOSAIC'
				   when 'FINANCIALSTRAT'   then 'FSS'
				   when 'ONNET'            then 'OnNet / Offnet'
				   when 'SKYGO'            then 'Sky Go users'
				   else 'FAIL!'
			end
			,case aggregation_variable
				   when 'UNIVERSE'         then 1
				   when 'REGION'           then 2
				   when 'HHCOMP'           then 3
				   when 'PACKAGE'          then 4
				   when 'TENURE'           then 5
				   when 'BOXTYPE'          then 6
				   when 'VALUESEG'         then 7
				   when 'MOSAIC'           then 8
				   when 'FINANCIALSTRAT'   then 9
				   when 'ONNET'            then 10
				   when 'SKYGO'            then 11
				   else -1
			end -- so the results go out into the excel thing in the right order
			,sqrt(avg((Good_Household_Index - 100) * (Good_Household_Index - 100)))
			,@weekending
	from 	Vespa_all_aggregated_results
	group 	by	panel
				,aggregation_variable

	commit

	if exists	(
					select	first *
					from  	vespa_traffic_lights_hist
					where 	weekending = @weekending
				)
		MESSAGE cast(now() as timestamp)||' | @ M11.1: Traffic Lights HIST COMPLETED' TO CLIENT
	else
		MESSAGE cast(now() as timestamp)||' | @ M11.1: Traffic Lights HIST INCOMPLETE' TO CLIENT

	commit

	MESSAGE cast(now() as timestamp)||' | @ M11.1: Traffic Lights DONE' TO CLIENT


	------------------------
	-- M11.2 - QAing results
	------------------------


		 

	------------------------------------
	-- M11.3 - Setting Access Privileges
	------------------------------------

	grant select on Vespa_all_households            to vespa_group_low_security
	grant select on Vespa_Scaling_Segment_Profiling to vespa_group_low_security
	grant select on Vespa_all_aggregated_results    to vespa_group_low_security
	grant select on vespa_traffic_lights_hist		to vespa_group_low_security
	commit
	

	MESSAGE cast(now() as timestamp)||' | @ M11.3: Setting Access Privileges DONE' TO CLIENT

	----------------------------
	-- M11.4 - Returning results
	----------------------------

	-- Project Vespa: Panel Management Report - traffic lights, showing balance of panel over each single variable
	
	/*
	select	weekending
			,variable_name
			,sum(case when panel = 'DP' then imbalance_rating else 0 end) 	as DP_Imbalance
			,sum(case when panel = 'AP' then imbalance_rating else 0 end)	as AP_Imbalance
	from 	vespa_traffic_lights_hist
	where 	weekending = (select max(weekending) from vespa_traffic_lights_hist)
	group 	by	variable_name, weekending
	order 	by 	min(sequencer)
	*/

    MESSAGE cast(now() as timestamp)||' | M11 Finished' TO CLIENT

    commit

end;

commit;
grant execute on sig_masvg_m11_panel_measurement_generator to vespa_group_low_security;
commit;
