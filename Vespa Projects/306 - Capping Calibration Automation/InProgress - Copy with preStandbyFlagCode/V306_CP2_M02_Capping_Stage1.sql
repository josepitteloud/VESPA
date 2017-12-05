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




**Module:                               V306_CP2_M02_Capping_Stage1

create AUG tables - CUSTOM

*/

create or replace procedure V306_CP2_M02_Capping_Stage1
													@target_date       	date = NULL     -- Date of daily table caps to cache
												,	@VESPA_table_name	varchar(150)	output
as begin

	execute M00_2_output_to_logger '@ M02 : Initial data check : '
	COMMIT
			
	declare @query varchar(1000)
	commit --; --^^ to be removed
	-- declare @VESPA_table_name varchar(150)
	-- commit --; --^^ to be removed

	execute M00_2_output_to_logger 'M02: CP2 checking data availability for date ' || convert(varchar(10),@target_date,123)
	-- Check that there is data in viewing table
	commit --; --^^ to be removed
	declare @cust_subs_hist_IsOK bit
	commit --; --^^ to be removed
	declare @vespa_table_min_check_IsOK bit		
	commit --; --^^ to be removed
	declare @vespa_table_max_check_IsOK bit		
	commit --; --^^ to be removed

	-- set @cust_subs_hist_IsOK=case when (select max(effective_from_dt) from cust_subs_hist)>=@target_date then 1 else 0 end
	-- commit --; --^^ to be removed

	set @VESPA_table_name = 'VESPA_DP_PROG_VIEWED_' || convert(varchar(6),@target_date,112)
	commit --; --^^ to be removed

	execute M00_2_output_to_logger '@ M02 : Source table identified : ' || @VESPA_table_name
	COMMIT

	-- set @query='
	-- set @@vespa_table_min_check_IsOK=case when (select min(dk_event_start_datehour_dim)/100  from ###tableName###) <= cast(@target_date as integer) then 1 else 0 end
	-- commit --; --^^ to be removed
	-- set @@vespa_current_max_IsOK=case when (select max(dk_event_start_datehour_dim)/100  from ###tableName###) >= cast(@target_date as integer) then 1 else 0 end
	-- commit --; --^^ to be removed
	-- '
	-- commit --; --^^ to be removed
	
	-- select @@vespa_table_min_check_IsOK

	-- execute(replace(@query,'###tableName###',@VESPA_table_name))
	-- commit --; --^^ to be removed

	-- set @dateIsOK=case when (@cust_subs_hist_IsOK!=0) and (@vespa_table_min_check_IsOK!=0) and (@vespa_current_max_IsOK!=0) then 1 else 0 end
	-- commit --; --^^ to be removed

	-- execute logger_add_event @CP2_build_ID, 3, 'M02: CP2 check results: cust_subs_hist ' || cast(@cust_subs_hist_IsOK as varchar(1)) || ', ' || @VESPA_table_name || ' min ' || cast(@cust_subs_hist_IsOK as varchar(1)) ||' max ' || cast(@cust_subs_hist_IsOK as varchar(1))

	
end;
commit;

grant execute on V306_CP2_M02_Capping_Stage1 to vespa_group_low_security;
commit;





