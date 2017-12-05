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




**Module:                               M00_2_output_to_logger

This module installs a procedure to output debug messages both to logger and to client


*/

create or replace procedure M00_2_output_to_logger              
        @output_msg             varchar(2000)
    ,@CP2_build_ID      bigint = NULL   -- Logger ID (so all builds end up in same queue)
        ,@logger_level          tinyint = 3  -- event level
as begin

        declare @output_with_timestamp varchar(2048)
        commit

        set @output_with_timestamp	=	CAST(NOW() AS TIMESTAMP) || ' | ' || @output_msg
        commit

        if @CP2_build_ID is not null
        begin
                execute logger_add_event @CP2_build_ID, @logger_level, cast(@output_with_timestamp as varchar(200))
                        -- Check that there is data in viewing table
                commit --; --^^ to be removed
        end
        
        MESSAGE @output_with_timestamp TO CLIENT
        commit

-- check if the debug output table exists, if not create it.
        if not  exists  (
							select  1
							from    sysobjects
							where
											[name]                  =       'CP2_debug_output_table'
									and uid                         =       user_id()
									and     upper([type])   =       'U'
						)
        begin
                                        -- table does not exist, let's create it!!!
        create table CP2_debug_output_table (
                        row_id          int                     primary key identity -- primary key
                ,       msg                     varchar(2048)
        )
        commit

        end



        insert into CP2_debug_output_table(msg)
        select @output_with_timestamp
        commit

end

commit;

grant execute on M00_2_output_to_logger to vespa_group_low_security;
commit;
