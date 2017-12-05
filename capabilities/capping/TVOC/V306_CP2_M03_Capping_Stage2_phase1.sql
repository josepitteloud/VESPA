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




**Module:                               V306_CP2_M03_Capping_Stage2_phase1

create AUG tables - CUSTOM

*/

create or replace procedure V306_CP2_M03_Capping_Stage2_phase1
											@capping_date date
										,	@sample_size  tinyint
										,	@VESPA_table_name	varchar(150)
										,	@lag_days		smallint		=	0
as begin

	-- ########################################################################
	-- #### Capping State 2 - create AUG tables - CUSTOM                   ####
	-- ########################################################################
	-- Change months according to range of run

	execute M00_2_output_to_logger '@ M03 : V306_CP2_M03_Capping_Stage2_phase1'
	COMMIT

	--------------------------------------------------------------
	-- Initialise
	--------------------------------------------------------------
	
	declare @QA_catcher   integer	commit
	
	--------------------------------------------------------------
	-- Select sample of accounts
	--------------------------------------------------------------

	execute M00_2_output_to_logger '@ M03 : V306_CP2_M03_Capping_Stage2_phase1 : Apply sample selection @ '	||	@sample_size
	COMMIT
	
	-- Get Vespa panel accounts for capping date of interest and the scaling weight
	insert into	CP2_accounts
	select
			account_number
		,	adsmart_scaling_weight
		,	rand(number())	as	rand_num
		,	@capping_date
	from	sk_prod.VIQ_viewing_data_scaling
	where	adjusted_event_start_date_vespa	=	@capping_date
	commit
	
	-- Apply sample trimming
	delete from CP2_accounts
	where	rand_num	>	(@sample_size/ 100.0)
	and  reference_date = @capping_date
	commit
	
		insert into	CP2_accounts_lag
	select
			account_number
		,	adsmart_scaling_weight
		,	rand(number())	as	rand_num
		,	dateadd(dd,@lag_days,@capping_date)
	from	sk_prod.VIQ_viewing_data_scaling
	where	adjusted_event_start_date_vespa	=	dateadd(dd,@lag_days,@capping_date)
	commit
	
	-- Apply sample trimming
	delete from CP2_accounts_lag
	where	rand_num	>	(@sample_size/ 100.0)
	and  reference_date = dateadd(dd,@lag_days,@capping_date)
	commit
	
	
	
end;
commit;

grant execute on V306_CP2_M03_Capping_Stage2_phase1 to vespa_group_low_security;
commit;

