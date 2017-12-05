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




**Module:                               V306_CP2_M05_1_Add_Scaling

create AUG tables - CUSTOM

*/

create or replace procedure V306_CP2_M05_1_Add_Scaling
as begin

        execute M00_2_output_to_logger '@ M05_1 : V306_CP2_M05_1_Add_Scaling start...'
        commit
        declare @input_table varchar(40)
        commit                                                                  
         -- input table with VESPA capped data
        set @input_table='CP2_capped_data_holding_pen'
        commit                                                                  
        

        declare @query varchar(10000)
        commit                                                                  
        
        declare @cntr_days integer
        commit                                                                  
        
        declare @nr_of_total_days integer
        commit                                                                  
        
        declare @curr_date date
        commit                                                                  
        
        declare @end_date date
        commit                                                                  
        
        declare @current_hour int
        commit                                                                  
        
        --create or replace variable @batch_size_in_hours=8 -- update of the daily table is done in batches of this number of hours (starting event time is the reference)
        

        SET @query='
        set @curr_date=(select cast(min(adjusted_event_start_time) as date) from ###input_table###)
                commit
                set @end_date=(select cast(max(adjusted_event_start_time) as date) from ###input_table###)
                commit
        '
        commit                                                                  
        execute(replace(@query,'###input_table###',@input_table))
        commit                                                                  

        while @curr_date <= @end_date
        BEGIN

                execute M00_2_output_to_logger '@ M05_1 : V306_CP2_M05_1_Add_Scaling - while loop, executing day ' || cast(@curr_date as varchar(20))

                SET @query='
                update ###input_table### ves
                        set ves.scaling_weighting=sca.adsmart_scaling_weight
                        --,ves.scaling_segment_id=sca.scaling_segment_key
                        from
                        ###input_table### ves
                        inner join
                        sk_prod.viq_viewing_data_scaling sca
                -- where
                        on
                        sca.adjusted_event_start_date_vespa=''###batch_date###''
                        and
                        ves.account_number=sca.account_number
                        and
                        date(ves.adjusted_event_start_time) = sca.adjusted_event_start_date_vespa
                '

                commit

                execute (   
							replace(
										replace(@query,'###batch_date###', @curr_date)
										,'###input_table###',@input_table)   )

                commit

                --set @current_hour= @current_hour+@batch_size_in_hours
                --commit

                --end -- hour while loop

                --commit

                set @curr_date=dateadd(dd,1,@curr_date)

                commit


        END -- date while loop
        
        execute M00_2_output_to_logger '@ M05_1 : V306_CP2_M05_1_Add_Scaling end'

end;
commit;

grant execute on V306_CP2_M05_1_Add_Scaling to vespa_group_low_security;
commit;
