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




**Module:                               V306_CP2_M08_Clean_Up

										-- The capping build generates a whole bunch of junk that clutters up a schema. This
										-- guy will clear out all the transient objects, which might be cute as some of them
										-- are pretty big - full daily viewing data dumps etc.

*/

create or replace procedure V306_CP2_M08_Clean_Up
	as begin
			execute M00_2_output_to_logger '@ M08 : V306_CP2_M08_Clean_Up start'


			--------------------------------------------------------------------------------
			-- R01) TRANSIENT TABLE RESET PROCEDURE
			--------------------------------------------------------------------------------


						-- Tables that are reset and built in the middle of the script:
			if object_id('CP2_capped_events_with_endpoints')    is not null drop table CP2_capped_events_with_endpoints
			if object_id('CP2_event_listing')                   is not null drop table CP2_event_listing
			if object_id('CP2_First_Programmes_In_Event')       is not null drop table CP2_First_Programmes_In_Event
			if object_id('CP2_h15_19')                          is not null drop table CP2_h15_19
			if object_id('CP2_h20_22')                          is not null drop table CP2_h20_22
			if object_id('CP2_h23_3')                           is not null drop table CP2_h23_3
			if object_id('CP2_h4_14')                           is not null drop table CP2_h4_14
			if object_id('CP2_lp')                              is not null drop table CP2_lp
			if object_id('CP2_nt_20_3')                         is not null drop table CP2_nt_20_3
			if object_id('CP2_nt_4_19')                         is not null drop table CP2_nt_4_19
			if object_id('CP2_nt_lp')                           is not null drop table CP2_nt_lp
			if object_id('CP2_ntiles_week')                     is not null drop table CP2_ntiles_week
			if object_id('Capping2_01_Viewing_Records')         is not null drop table Capping2_01_Viewing_Records

						-- Tables that exist eslewhere in the table creation script:
			truncate table CP2_box_lookup
			truncate table CP2_calculated_viewing_caps
			truncate table CP2_capped_data_holding_pen
			truncate table CP2_capping_buckets
			truncate table CP2_relevant_boxes
			truncate table VESPAvsBARB_metrics_table


			execute M00_2_output_to_logger '@ M08 : V306_CP2_M08_Clean_Up end'


	end;
commit;

grant execute on V306_CP2_M08_Clean_Up to vespa_group_low_security;
commit;





