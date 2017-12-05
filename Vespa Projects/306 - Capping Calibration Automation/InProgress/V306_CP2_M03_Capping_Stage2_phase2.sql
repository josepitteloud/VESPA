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

-----------------------------------------------------------------------------------

**Project Name:                         Capping Calibration Automation
**Analysts:                             Leonardo Ripoli  (Leonardo.Ripoli@sky.uk)
                                        Jonathan Green   (Jonathan.Green2@sky.uk)
                                        
**Lead(s):                              Hoi Yu Tang (hoiyu.tang@sky.uk)
**Stakeholder:                          Jose Loureda
**Project Code (Insight Collation):     V306
**SharePoint Folder:                    http://sp-department.bskyb.com/sites/IQSKY/SIG/Insight%20Collation%20Documents/Forms/AllItems.aspx?RootFolder=%2Fsites%2FIQSKY%2FSIG%2FInsight%20Collation%20Documents%2F01%20Analysis%20Requests%2FV306%20-%20Foundation%20-%20Platform%20Maintenance%2FPhase%202%2FCapping%20Calibration%20Automation

**Business Brief:

Capping Calibration Automation ultimately aims to deliver improved Vespa viewing consumption through monthly alignment to BARB reference data. 
The Capping algorithm was developed in order to truncate the length of viewing events where there is no actual audience present (e.g. TV set off, but STB on and registering viewing).

Up to this point, the parameters and thresholds that feed into the Capping algorithm have been largely static since the time of the original algorithm development by IQ and ultimate implementation within EDM. 
Although a recent capping calibration exercise addressed exactly the issue realigning Vespa viewing to BARB, this was a highly manual process that required much resource to construct and perform the analyses and assessment. 
Capping Calibration Automation will adopt those calculations and fundamental approach, but delivered as a self-contained and automated process that calculates the best set of capping thresholds and parameters in IQ/Olive 
for ingest within into the EDM production environment

This project will also aim to increase the precision over which the Capping thresholds and parameters operate. For example, the current parameters are defined at the day-part level, 
each of which spans a number of hours in the day. The intention is to explore the possibility of redefining the parameters at the hourly level in order to give greater control over the alignment process against BARB. 
In theory, there should be little to no adjustment required to the actual flow of the Capping algorithm since the thresholds and parameters are contained in an external lookup table rather than being hard-coded in the SQL.




**Module:                               V306_CP2_M03_Capping_Stage2_phase2

create AUG tables - CUSTOM

*/

create or replace procedure V306_CP2_M03_Capping_Stage2_phase2
											@capping_date date
										,	@sample_size  tinyint
										,	@VESPA_table_name	varchar(150)
as begin

	-- ########################################################################
	-- #### Capping State 2 - create AUG tables - CUSTOM                   ####
	-- ########################################################################
	-- Change months according to range of run

	execute M00_2_output_to_logger '@ M03 : V306_CP2_M03_Capping_Stage2_phase2'
	COMMIT

	--------------------------------------------------------------
	-- Initialise
	--------------------------------------------------------------
	
	declare @QA_catcher   integer	commit

	declare @sql_	varchar(5000)	commit
	
	--------------------------------------------------------------
	-- Select sample of accounts
	--------------------------------------------------------------

	execute M00_2_output_to_logger '@ M03 : V306_CP2_M03_Capping_Stage2_phase2 : take sample selection @ '	||	@sample_size ||	'%'
	COMMIT


	--------------------------------------------------------------
	-- Identify non-duplicated VPIF keys
	--------------------------------------------------------------
	
	execute M00_2_output_to_logger '@ M03 : V306_CP2_M03_Capping_Stage2_phase2 : Identify and retain non-duplicated VPIF keys'
	COMMIT


	-- Clear table if necessary
	execute DROP_LOCAL_TABLE 'CP2_unique_vpif_keys'
	commit	
	
	-- Get non-duplicated VPIF keys for the date of interest
	set	@sql_	=	'
		select
				pk_viewing_prog_instance_fact
			,	account_number
			,	cast(NULL as tinyint)		as	tx_day
			,	count()	as	cnt
		into	CP2_unique_vpif_keys
		from
						' || @VESPA_table_name	|| '	as	dpp
		where
                dpp.log_received_start_date_time_utc    between	dateadd(day,-3,''' || @capping_date || ''')
                                                        and     dateadd(hour,30,''' || @capping_date || ''')
			and	panel_id							in	(11,12)
			and	type_of_viewing_event				is not null
			and	type_of_viewing_event				<>	''Non viewing event''
		group by
				pk_viewing_prog_instance_fact
			,	account_number
		having	cnt	=	1
		commit
		'
	commit
	
/*
	TX definition required - (log received date - event start date + 1)? Include TX+0 if there are any.
	TX+1 -> ntiling process
	TX+2/3 -> capping applied -- is this actually needed? how many get scaling weights? - shouldn't expect many
	MbM - still isolate to scaled accounts for reporting/TE
*/
	
	execute(@sql_)
	commit
	
	create unique hg index uhg on CP2_unique_vpif_keys(pk_viewing_prog_instance_fact)
	commit
	create hg index idx1 on CP2_unique_vpif_keys(account_number)
	commit
	create lf index idx2 on CP2_unique_vpif_keys(tx_day)
	commit

	
	execute M00_2_output_to_logger '@ M03 : V306_CP2_M03_Capping_Stage2_phase2 : Non-duplicated VPIF instances identified.'
	COMMIT



	

	
	
	-- Calculate TX day for each instance/event. Do this separately from the initial VPIF deduping so as not to re-introduce duplicates at that level.

	execute M00_2_output_to_logger '@ M03 : V306_CP2_M03_Capping_Stage2_phase2 : Calculate TX Day for each instance'
	COMMIT

	set	@sql_	=	'
		update	CP2_unique_vpif_keys
		set		tx_day	=	datediff	(
												day
											,	dpp.event_start_date_time_utc
											,	dpp.log_received_start_date_time_utc
										)
		from
						CP2_unique_vpif_keys			as	a
			inner join	' || @VESPA_table_name	|| '	as	dpp		on  dpp.pk_viewing_prog_instance_fact		=		a.pk_viewing_prog_instance_fact
																	and	dpp.account_number						=		a.account_number
																	and	dpp.log_received_start_date_time_utc    between	dateadd(day,-3,''' || @capping_date || ''')
																												and     dateadd(hour,30,''' || @capping_date || ''')
																	and	dpp.panel_id							in		(11,12)
																	and	dpp.type_of_viewing_event				is 		not null
																	and	dpp.type_of_viewing_event				<>		''Non viewing event''
		commit
		'
	commit
							
	execute(@sql_)
	commit
	


	-- Retain only those events up to TX+3
	execute M00_2_output_to_logger '@ M03 : V306_CP2_M03_Capping_Stage2_phase2 : Retain only those events up to TX+3'
	COMMIT

	delete from	CP2_unique_vpif_keys
	where	tx_day	not between	0
						and		3
	commit

	
	-- TEMPORARY FILTER
	execute M00_2_output_to_logger '@ M03 : V306_CP2_M03_Capping_Stage2_phase2 : Retain only those events at TX+1'
	COMMIT
	delete from	CP2_unique_vpif_keys
	where	tx_day	<>	1
	commit
	
	

	-----------------------------------------------------------------------------------------------------------------------------------------------------
	-- Now also apply sample trimming. This was previously performed by joining into the above, but we now need to factoring in unscaled accounts as well
	-----------------------------------------------------------------------------------------------------------------------------------------------------

	execute DROP_LOCAL_TABLE 'CP2_unscaled_accounts'
	commit
	
	select
			a.account_number
		,	rand(number())	as	rand_num
	into	CP2_unscaled_accounts
	from
					CP2_unique_vpif_keys	a
		left join	CP2_accounts			b 	on	a.account_number	=	b.account_number
	where	b.account_number	is null
	group by	a.account_number	-- shouldn't need this, but deduping just in case!
	commit
	


	-- Apply sample trimming on unscaled accounts (the scaled version was performed earlier in V306_CP2_M03_Capping_Stage2_phase1)
	-- For now, this sample can change between iterations, whereas the scaled account sample is fixed per capping date.
	delete from CP2_unscaled_accounts
	where	rand_num	>	(@sample_size/ 100.0)
	commit
	

	delete from 	CP2_unique_vpif_keys
	from
					CP2_unique_vpif_keys	a
		left join	(
						select	account_number
						from	CP2_accounts
						union all
						select	account_number
						from	CP2_unscaled_accounts
					)						b	on	a.account_number	=	b.account_number
		where	a.account_number	is null
	commit

	



	----------------------------------------------------------------------------
	-- Create view to the appropriate monthly viewing table for the capping date
	----------------------------------------------------------------------------
		
	set @sql_	=	'
		create or replace view	Capping2_00_Raw_Uncapped_Events as
		select	dpp.*
		from
						' || @VESPA_table_name	|| '			as	dpp
			inner join	CP2_unique_vpif_keys					as	keys		on	dpp.pk_viewing_prog_instance_fact	=	keys.pk_viewing_prog_instance_fact
		where
                dpp.log_received_start_date_time_utc    between	dateadd(day,-3,''' || @capping_date || ''')
                                                        and     dateadd(hour,30,''' || @capping_date || ''')
--				(
--						(
--							dk_event_start_datehour_dim	between	cast(dateformat(''' || @capping_date || ''', ''yyyymmdd00'') as int)
--														and		cast(dateformat(''' || @capping_date || ''', ''yyyymmdd23'') as int)
--						)
--					or	(
--							dk_event_end_datehour_dim	between	cast(dateformat(''' || @capping_date || ''', ''yyyymmdd00'') as int)
--														and		cast(dateformat(''' || @capping_date || ''', ''yyyymmdd23'') as int)
--						)
--				)
			and	panel_id							in	(11,12)
			and	type_of_viewing_event				is not null
			and	type_of_viewing_event				<>	''Non viewing event''
		commit
	'
	commit
	
	execute (@sql_)
	commit
	
	
	execute M00_2_output_to_logger '@ M03 : V306_CP2_M03_Capping_Stage2_phase2 : Working view created : Capping2_00_Raw_Uncapped_Events'
	COMMIT
	

/*
commit								
execute logger_create_run 'Capping2.x CUSTOM', 'Weekly capping run', @varBuildId output

select @QA_catcher = count(1)
from CP2_duplicated_keys

execute logger_add_event @varBuildId, 3, 'Number of duplicated keys: ', coalesce(@QA_catcher, -1)
*/

end;
commit;

grant execute on V306_CP2_M03_Capping_Stage2_phase2 to vespa_group_low_security;
commit;

