/*###############################################################################
# Created on:   08/10/2012
# Created by:   Tony Kinnaird
# Description:  Minute attribution Phase 2 data - Run for Daily Augs tables
#
# List of steps:
#               STEP 0.1 - Drop variables if running multiple times in same session (commented out for ease)
#               STEP 0.2 - creating variables for use in process
#               STEP 0.3 - Define variable values for this run
#               STEP 0.4 - Create min_attrib table for this run
#               STEP 1.0 - While stmt to loop around the days worth of data you wish to run
#               STEP 1.1 - Delete from tables where this relates to the target_date
#               STEP 1.2 - Set sql_stmt to get day's worth of events to minute attribute
#               STEP 2.1 - Call Minute Attribution procedure on events to be processed
#               STEP 3.1 - Update Daily Augs Table with BARB Minute Values
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# (none)
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 08/10/2012  TKD   v01 - initial version
#
###############################################################################*/

        -- ##############################################################################################################
        -- ##### STEP 0.1 - Drop Variables if running more than once                            		    #####
        -- ##############################################################################################################

/*
drop variable @varBuildId;
drop variable @min_attrib_start_dt;
drop variable @min_attrib_end_dt;
drop variable @var_sql;
drop variable @target_date;
drop variable @qa_catcher;
*/

        -- ##############################################################################################################
        -- ##### STEP 0.2 - creating variables for use in process                            		    	    #####
        -- ##############################################################################################################

CREATE VARIABLE @varBuildId bigint; -- the Run_ID to be used for events
EXECUTE citeam.logger_create_run 'Phase2MinAttr','Build starting ' || dateformat(now(), 'yyyy-mm-dd hh:mm:ss') , @varBuildId output; -- This sets up the run AND gets the RunID

create variable @min_attrib_start_dt date;
create variable @min_attrib_end_dt date;
create variable @var_sql varchar(16000);
create variable @target_date date;
create variable @qa_catcher bigint;

        -- ##############################################################################################################
        -- ##### STEP 0.3 - Define variable values for this run                            		    	    #####
        -- ##############################################################################################################


set @min_attrib_start_dt = '2012-08-24'
set @min_attrib_end_dt = '2012-08-26'
set @target_date = @min_attrib_start_dt



        -- ##############################################################################################################
        -- ##### STEP 0.4 - Create Min Attrib table for this run                            		    	    #####
        -- ##############################################################################################################


IF object_id('min_attrib_data_table') IS NOT NULL DROP TABLE min_attrib_data_table;
-- For the commented out columns we don't have rules yet / havent implemented
-- the flag population.
        create table min_attrib_data_table (
        pk_viewing_prog_instance_fact bigint,
        Subscriber_Id decimal,
        instance_start_date_time_utc timestamp,
        instance_end_date_time_utc timestamp,
        dk_programme_dim bigint,
        dk_channel_dim int,
        time_in_seconds_since_recording int,
        type_of_viewing_event varchar(40),
        playback_speed decimal,
        service_type_description varchar(40),
        video_playing_flag bit,
        barb_minute_start timestamp,
        barb_minute_end timestamp)

create hg index min_attrib_idx1_hg on min_attrib_data_table (pk_viewing_prog_instance_fact)


        -- ##############################################################################################################
        -- ##### STEP 1.0 - While stmt to loop around the days worth of data you wish to run                        #####
        -- ##############################################################################################################


while @min_attrib_end_dt >= @target_date

--execute citeam.logger_add_event @varBuildId, 3, 'Step 1.0: Pull Data for Minute Attribution for dt '|| convert(varchar(10),@target_date,123)

BEGIN

        -- ##############################################################################################################
        -- ##### STEP 1.1 - Delete from tables where this relates to the target_date                        	    #####
        -- ##############################################################################################################


truncate table min_attrib_data_table ----remove records from the staging minute attribution tables

--delete relevant records from surf_constitutents table as you do not want multiples for any processed day, if you have to process more than once.
--need to happen before delete from VESPA_SURF_MINUTES_PHASE2  table

/*
delete from VESPA_SURF_CONSTITUENTS_PHASE2 ve_ph
from
VESPA_SURF_CONSTITUENTS_PHASE2 ph2, VESPA_SURF_MINUTES_PHASE2 surf
where ph2.surf_id = surf.surf_id
and date(surf.surf_minute_start) = date(@target_date)
*/

--In QA was suggested to make the delete above clearer with the following code

delete from VESPA_SURF_CONSTITUENTS_PHASE2
where surf_id in (select surf_id 
from VESPA_SURF_MINUTES_PHASE2 
where date(surf_minute_start) = date(@target_date))


--delete relevant records from VESPA_SURF_MINUTES_PHASE2  table as you do not want multiples for any processed day, if you have to process more than once.

delete from VESPA_SURF_MINUTES_PHASE2 where date(surf_minute_start) = date(@target_date)

--set sql script for insert into minute attribution table


        -- ##############################################################################################################
        -- ##### STEP 1.2 - Set sql_stmt to get day's worth of events to minute attribute			    #####
        -- ##############################################################################################################


    SET @var_sql = '
        insert into min_attrib_data_table
        select distinct vea.pk_viewing_prog_instance_fact,
                        vea.Subscriber_Id ,
                        vea.instance_start_date_time_utc ,
                        vea.instance_end_date_time_utc ,
                        vea.dk_programme_dim ,
                        vea.dk_channel_dim ,
                        vea.time_in_seconds_since_recording ,
                        vea.type_of_viewing_event ,
                        vea.playback_speed ,
                        vea.service_type_description ,
                        vea.video_playing_flag ,
                        null as barb_minute_start ,
                        null as barb_minute_end
         from sk_prod.vespa_events_all vea, vespa_daily_augs_##^^*^*## augs
        where vea.pk_viewing_prog_instance_fact = augs.cb_row_id
        and convert( char(8), vea.instance_start_date_time_utc, 112 ) = cast(##^^*^*## as char(8))' 

--execute sql stmt replacing target date with the relevant value

    EXECUTE(replace(@var_sql,'##^^*^*##', dateformat(@target_date, 'yyyymmdd')))   -- formatting for name of daily table)

--select @qa_catcher = count(1) from min_attrib_data_table

--execute citeam.logger_add_event @varBuildId, 3, 'Step 2.1: Pull Data for Minute Attribution for dt '|| convert(varchar(10),@target_date,123), @qa_catcher 

--execute minute attribution script for the date in question

        -- ##############################################################################################################
        -- ##### STEP 2.1 - Call Minute Attribution procedure on events to be processed			            #####
        -- ##############################################################################################################

--delete any duplicates between this table and what has been run before?-----

--delete from min_attrib_data_table
--where pk_viewing_prog_instance_fact in
--(select instance_id from VESPA_SURF_CONSTITUENTS_PHASE2)

--commit

---- delete ended

---execute minute attribution script on remaining data---

exec Minute_Attribution_Phase2_v03 'min_attrib_data_table', now(), 0,'', 0 -----procedure to execute minute attribution on daily set of tables

commit

--execute citeam.logger_add_event @varBuildId, 3, 'Step 2.1: Update Min Attribution Values into Daily Augs Table for dt '|| convert(varchar(10),@target_date,123)

--set sql script for update of the BARB minute values into Daily Augs tables


        -- ##############################################################################################################
        -- ##### STEP 3.1 - Update Daily Augs Table with BARB Minute Values			                    #####
        -- ##############################################################################################################


SET @var_sql = '
        update vespa_daily_augs_##^^*^*##  base
         set base.BARB_minute_start = det.BARB_Minute_Start,
             base.BARB_minute_end   = det.BARB_Minute_End
from min_attrib_data_table det
where base.cb_row_id = det.pk_viewing_prog_instance_fact'    ------update the daily augs table utilising the staging table that is now populated with BARB timings

--execute sql stmt replacing target date with the relevant value

    EXECUTE(replace(@var_sql,'##^^*^*##', dateformat(@target_date, 'yyyymmdd')))   -- formatting for name of daily table)  execute update statement

commit

--execute citeam.logger_add_event @varBuildId, 3, 'Step 3.2: Update Completed on Daily Augs Table for dt '|| convert(varchar(10),@target_date,123)

set @target_date = @target_date + 1  ------set date to 1 > so that you can modify multiple days

END

commit

