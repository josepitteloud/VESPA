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

**Project Name:                                         XDash Overview Variables
**Analysts:                                                     Berwyn Cort        (berwyn.cort@skyiq.co.uk)
**Lead(s):
**Stakeholder:                                          SIG and Executive Team


**Business Brief:

        A method to use 241 - Panel Balancing 2.0 created by Jon Green and list of accounts with the number
        of boxes they have and are returning built by Angel Donnarumma with other tables to pull together variables
        based on Scaling 3.0 to show Sky base, Panel base, average Scaling Weight Reportin Quality and formulae to
        produce Panel Balancing indices.

**Sections:

                A00: Initialising the environment

        A: POPULATE PANEL BALANCE TABLES FOR VARIABLE DATA

                A01: Run stored procedure
                     The SP is in the following location in the repository (...GIT\Vespa\Vespa Projects\241 - Panel Balancing 2.0\panbal_segments).


        B: REFRESH DAILY PANEL ACCOUNT POPULATION WITH RETURNING BOXES

                B01: Accounts and thier boxes
                B02: Number of boxes dialled against thier account in the past 30 days

        C: POPULATE VARIABLES METRICS TABLE

                C01: Initialisation
                C02: Set the temp tables
                C03: Set the high level variables
                C04: Construct the variable table, repeating per variable

--------------------------------------------------------------------------------------------------------------------------------------------
USEFUL NOTE:    first the PanBal_segmentation SP is run to get the latest snapshot of variables against accounts and
                segment_id then the population is refreshed with their box details.  Then variables are populated with
                Sky and Panel base for index calculations.  Queries then pull together the metrics and repeat for every variable.
--------------------------------------------------------------------------------------------------------------------------------------------

*/

------------------------------------
-- A00: Initialising the environment
------------------------------------

create or replace procedure xdash_ov_variable_metrics
	@weekending	date	= null
as begin

	MESSAGE now() ||' | Beginig  XDASH_OV_VARIABLE_METRICS' TO CLIENT
	
	MESSAGE now() ||' | Beginig  A00: Initialising the environment' TO CLIENT

	delete from xdash_overview_variables
    commit

	MESSAGE now() ||' | Beginig  A00: Initialising the environment DONE' TO CLIENT
	
------------------------------------------
-- A01: Run Panel Balance stored procedure
------------------------------------------

	MESSAGE now() ||' | Beginig  A01: Run Panel Balance stored procedure (ETA: 30 mins APROX)' TO CLIENT

	-- To populate tables for variable data
    execute /*vespa_analysts.*/V306_M03_PanBal_Segments_adapted @weekending

	MESSAGE now() ||' | Beginig  A01: Run Panel Balance stored procedure DONE' TO CLIENT

-----------------------------------------------------
-- B01: Account population with their number of boxes
-----------------------------------------------------

	MESSAGE now() ||' | Beginig  B01: Account population with thier number of boxes' TO CLIENT

	-- declare and inititialise variables
	
	MESSAGE now() ||' | ONLY TO FIT RSMB AUDIT!!!!!' TO CLIENT
	
	if object_id('acview') is not null
		drop table acview
		
	commit
	
	-- a list of accounts and how many boxes each has...
	select  panel_id	as panel	
			,account_number
			,count(distinct subscriber_id) 	as num_boxes
	into	acview
	from    vespa_analysts.vespa_sbv_hist_qualitycheck
	where	panel is not null
	and		weekending = @weekending
	group   by  panel
				,account_number

	commit
	create hg index hg1 on acview(account_number)
	create lf index lf1 on acview(panel)
	commit

	MESSAGE now() ||' | Beginig  B01: Account population with thier number of boxes DONE' TO CLIENT
	
-----------------------------------------------------
-- B02: Boxes dialled per account in the past 30 days
-----------------------------------------------------

	MESSAGE now() ||' | Beginig  B02: Boxes dialled per account in the past 30 days' TO CLIENT
	
	MESSAGE now() ||' | OOP!!!!!' TO CLIENT
	
	-- counting for each day on the past 30 days the number of boxes that dialed
	-- for every single account...

	if object_id('panel_data_summary') is not null
		drop table panel_data_summary
	
	commit
	
    select  perf.dt
			,boxview.account_number
			,count(distinct perf.subscriber_id) as dialling_b
	into	panel_data_summary
	from    (
				select	dt
						,subscriber_id
				from	vespa_analysts.panel_data
				where	data_received = 1
				and		dt between @weekending-29 and @weekending
				and		panel in (5,6,7)
			)	as perf
			inner join  vespa_analysts.vespa_sbv_hist_qualitycheck	as boxview
			on  perf.subscriber_id = boxview.subscriber_id
			and	boxview.panel_id in (5,6,7)
			and	boxview.weekending = @weekending
	group   by  perf.dt 
				,boxview.account_number

	commit
	create date index date1 on panel_data_summary(dt)
	create hg index hg1     on panel_data_summary(account_number)
	grant select on panel_data_summary to vespa_group_low_security
	commit 

	MESSAGE now() ||' | Beginig  B02: Boxes dialled per account in the past 30 days DONE' TO CLIENT
	
----------------------
-- C01: Initialisation
----------------------

	MESSAGE now() ||' | Beginig  C01: Initialisation' TO CLIENT

	-- declare the variables
	declare	@Sky_Base decimal(8,1)
			,@Panel_Base decimal(8,1)
			,@todtMinus1 date
			,@todtMinus2 date
			,@todtMinus3 date
			,@HLAvgReturners integer
			,@HLBalanceIndex decimal(16,6)
			,@var_count      tinyint
			,@thevariable    varchar(20)

	-- Populate the variables
	select @Sky_Base   =    count(distinct account_number)
							from    PanBal_segment_snapshots

	select @Panel_Base =	count(distinct account_number)
							from    vespa_analysts.vespa_sbv_hist_qualitycheck
							where   weekending = @weekending
							and		panel_id in (11,12)

	select  @todtMinus1 = @weekending - 7
	select  @todtMinus2 = @todtMinus1 - 7
	select  @todtMinus3 = @todtMinus2 - 7

	select  @var_count = min(id) from panbal_variables

	MESSAGE now() ||' | Beginig  C01: Initialisation DONE' TO CLIENT
	
---------------------------------------------------
-- C02: Set the temporary tables most commonly used
---------------------------------------------------

	MESSAGE now() ||' | Beginig  C02: Set the temporary tables most commonly used' TO CLIENT
	
	MESSAGE now() ||' | Changed to VIQ!!!!!' TO CLIENT

	-- Scaling Weight Reporting Quality population
	/* select  case	when dial.dt <= @todt       and dial.dt > @todtMinus1   then @todt
					when dial.dt <= @todtMinus1 and dial.dt > @todtMinus2   then @todtMinus1
					when dial.dt <= @todtMinus2 and dial.dt > @todtMinus3   then @todtMinus2
					when dial.dt <= @todtMinus3 and dial.dt > @todtMinus3-7 then @todtMinus3
			end 	as dt
			,acview.account_number
	into    #SWRQ
	from    #panel_data         as dial
			inner join #acview  as acview                    --*
			on  dial.account_number = acview.account_number
			and dial.dialling_b >= acview.num_boxes -- This is the condition that flags whether an account returned data or not
	where   dt > @todtMinus3-7 */
	
	select  case	when adjusted_event_start_date_vespa <= @weekending       and adjusted_event_start_date_vespa > @weekending-7   then @weekending
					when adjusted_event_start_date_vespa <= @weekending-7     and adjusted_event_start_date_vespa > @weekending-14  then @weekending-7
					when adjusted_event_start_date_vespa <= @weekending-14    and adjusted_event_start_date_vespa > @weekending-21  then @weekending-14
					when adjusted_event_start_date_vespa <= @weekending-21    and adjusted_event_start_date_vespa > @weekending-28  then @weekending-21
			end 	as dt
			,account_number
	into    #SWRQ
	from    sk_prod.VIQ_VIEWING_DATA_SCALING
	where   adjusted_event_start_date_vespa between @weekending-27 and @weekending
	
	commit
	create hg index idx_accountnumber on #SWRQ(account_number)
	commit

	Message now()||' | Building SWRQ DONE' to client

	-- Vespa panel households population
	select  distinct account_number
	into    #PanBaseHH
	from    vespa_analysts.vespa_sbv_hist_qualitycheck
	where   weekending = @weekending
	and		panel_id in (11,12)

	commit
	create hg index idx_accountnumber on #PanBaseHH(account_number)
	commit

	Message now()||' | Building PanBaseHH DONE' to client
	MESSAGE now() ||' | Beginig  C02: Set the temporary tables most commonly used DONE' TO CLIENT
	
------------------------------------
-- C03: Set the high level variables
------------------------------------

	MESSAGE now() ||' | Beginig  C03: Set the high level variables' TO CLIENT

	-- Variable to get average returning households at high level.
	select  @HLAvgReturners = avg(AvgRet)
	from	(
				select  swrq.dt
						,count(swrq.account_number) / 7 as AvgRet
				from    PanBal_segment_snapshots as pss
						left join #SWRQ as swrq
						on swrq.account_number = pss.account_number
				group   by swrq.dt
				having dt is not null
			) 	as HLReturners

	MESSAGE now() ||' | Beginig  C03: Set the high level variables DONE'  TO CLIENT
			
------------------------------------------------------------
-- C04: Construct the variable table, repeating per variable
------------------------------------------------------------

	MESSAGE now() ||' | Beginig  C04: Construct the variable table, repeating per variable' TO CLIENT

	while @var_count <= (select max(id) from panbal_variables)
	begin

		select  @thevariable = aggregation_variable from panbal_variables where id = @var_count

		MESSAGE now() ||' | @ C04: Looping for '|| @thevariable to client
		
		-- Variable to get the the Balance Index at high level.
		select  @HLBalanceIndex = min(Balance_Index)
		from(
				select  psl.value
						,count( distinct pss.account_number) as Sky_Base_Households
						,count(distinct pbhh.account_number) as Panel_base_Households
						,sqrt(
								avg(
										(
											(
												(Panel_base_Households * @Sky_Base
																				/ Sky_Base_Households
																										/ @Panel_Base
												) * 100
											) - 100
										) * (
												(
													(Panel_base_Households * @Sky_Base
																					/ Sky_Base_Households
																											/ @Panel_Base
													) * 100
												) - 100
											)
								   ) over(partition by Part)
							  ) as Balance_Index --formula to get high level balance index
						,'Partition' as Part
				from    PanBal_segment_snapshots as pss
						inner join PanBal_segments_lookup_normalised as psl             --*
						on  pss.segment_id = psl.segment_id
						and psl.aggregation_variable = @var_count
						left join #PanBaseHH as pbhh
						on  pbhh.account_number = pss.account_number
				group   by  psl.value
			)HLBalanceIndex
		
		Message now()||' | C04: Calculating Balance Index at high level DONE' to client

		-- To get low level sub variable metrics
		select  psl.value
				,count(distinct pss.account_number) as Sky_Base_Households
				,count(distinct pbhh.account_number) as Panel_base_Households
				,cast(0 as integer) Returning_Households
				,(
					(Panel_base_Households * @Sky_Base
												/ Sky_Base_Households
																		/ @Panel_Base
					) * 100 
				)-100	as Balance_Index
		into    #TempLowLevel
		from    PanBal_segment_snapshots as pss
				inner join PanBal_segments_lookup_normalised as psl             --*
				on	pss.segment_id = psl.segment_id
				and	psl.aggregation_variable = @var_count
				left join #PanBaseHH as pbhh
				on	pbhh.account_number = pss.account_number
		group   by 	psl.value
		
		Message now()||' | C04: Calculating Low level sub variable metrics DONE' to client

		-- To get average returning households at low level sub variables
		select  value
				,cast(avg(AvgRet) as integer)	as AvgReturners
		into    #TempAvgRetLL
		from    (
					select  psl.value
							,count(swrq.account_number) / 7 AvgRet
					from    PanBal_segment_snapshots 						as pss
							inner join PanBal_segments_lookup_normalised	as psl
							on	pss.segment_id = psl.segment_id
							and	psl.aggregation_variable = @var_count
							left join #SWRQ 								as swrq
							on	swrq.account_number = pss.account_number
					group   by	psl.value
								,swrq.dt
					having  AvgRet > 0
				) 	as LLAvgReturners
		group	by	value
		
		Message now()||' | C04: Calculating average returning households at low level sub variables DONE' to client

		--To update low level average Returning households.
		update  #TempLowLevel as tll
		set     tll.Returning_Households = tarll.AvgReturners
		from    #TempAvgRetLL as tarll
		where   tll.value = tarll.value
		and     tarll.AvgReturners is not null
		
		Message now()||' | C04: Calculating update low level average Returning households DONE' to client

		-- To get variables with Sky base, Panel base at high level joined with low level split per sub variable with Balance indices
		
		insert into xdash_overview_variables	(  
													aggregation_variable
													,categories
													,sky_base_households
													,panel_base_households
													,avg_returning_households
													,balance_index
												)
		select  right(('0'||@var_count),2)||' - '||@thevariable	as aggregation_variable
				,'Summary'										as categories
				,count(pss.account_number) 						as Sky_Base_Households
				,count(distinct pbhh.account_number)			as Panel_base_Households
				,@HLAvgReturners 								as Returning_Households
				,@HLBalanceIndex 								as Balance_Index
		from    PanBal_segment_snapshots	as pss
				left join #PanBaseHH 		as pbhh
				on pbhh.account_number = pss.account_number
		UNION
		select  right(('0'||@var_count),2)||' - '||@thevariable	as aggregation_variable
				,coalesce(lk.friendlyname,ll.value) as category
				,ll.Sky_Base_Households
				,ll.Panel_base_Households
				,ll.Returning_Households
				,ll.Balance_Index
		from 	#TempLowLevel   as ll
				left join category_lookup as lk
				on  lower(lk.category_techname) = lower(ll.value)
				and lk.aggregation_index        = @var_count
		where   trim(value) <> ''
		commit

		Message now()||' | C04: Saving Overview Variables DONE' to client

		drop table #TempLowLevel
		drop table #TempAvgRetLL
		commit
		
		set @var_count = @var_count + 1

		Message now()||' | Looping through '|| @thevariable ||' DONE' to client    

	end

	grant select  on xdash_overview_variables  to vespa_group_low_security
	commit
	
	MESSAGE now() ||' | Beginig  C04: Construct the variable table, repeating per variable DONE' TO CLIENT
	MESSAGE now() ||' | XDASH_OV_VARIABLE_METRICS DONE' TO CLIENT
	commit
	
end;
commit;

grant execute on xdash_ov_variable_metrics to vespa_group_low_security;
commit;
