
/*###############################################################################
# Created on:   03/03/2014
# Created by:   Tony Kinnaird (TKD)
# Description:  Where clause to define valid viewing from Monthly tables) .
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 03/03/2014  TKD   v01 - initial version
# 05/03/2014  TKD   v02 - added some viewing back to cope with VOSDAL
# 06/03/2014  TKD   v03 - added 2nd variation with wider scope to cope with capped flag discrepancy
#
###############################################################################*/


-- ##############################################################################################################
-- ##### STEP 1.0 - Sample Code  									    #####
-- ##############################################################################################################


select dk_instance_start_datehour_dim/100,type_of_viewing_event, count(1)
from sk_prod.vespa_dp_prog_viewed_201308
                         where capped_full_flag = 0 -- only those instances that have not been fully capped
                           and instance_start_date_time_utc < instance_end_date_time_utc              -- Remove 0sec instances
                           and (reported_playback_speed is null or reported_playback_speed = 2) -- Live or Recorded Records
                           and account_number is not null --remove instances we do not know the account for
                           and subscriber_id is not null --remove instances we do not know the subscriber_id for
                           and (type_of_viewing_event in ('HD Viewing Event', 'Sky+ time-shifted viewing event', 'TV Channel Viewing')-- limit to keep out 
                            --interactive viewing and other service viewing event i.e. where it could not identify viewing event type it was
                            or (type_of_viewing_event = 'Other Service Viewing Event' 
                            and service_type_description in ('NVOD service','High Definition TV test service','Digital TV channel')))
                           and capping_end_date_time_utc is not null -- only those records where the event has been given a capped event end time
                            and dk_instance_start_datehour_dim/100 = 20130815
group by dk_instance_start_datehour_dim/100,type_of_viewing_event


-- ##############################################################################################################
-- ##### STEP 1.0 Ended									                    #####
-- ##############################################################################################################


-- ##############################################################################################################
-- ##### STEP 2.0 - Sample revised Code  									    #####
-- ##############################################################################################################


select pk_viewing_prog_instance_fact, 
case when (capping_end_date_time_utc is not null and capping_end_date_time_utc < instance_start_date_time_utc) then 1 else 0 end capped_full_flag_supplement
,
case when (capping_end_date_time_utc is not null and (capping_end_date_time_utc > instance_start_date_time_utc and capping_end_date_time_utc < instance_end_date_time_utc)) then 1 else 0 end capped_partial_flag_supplement
,
case when capping_end_date_time_utc is not null and capping_end_date_time_utc > instance_end_date_time_utc then 1 else 0 end capped_no_flag_supplement,
subscriber_id,a.dk_channel_dim,a.live_recorded,event_start_date_time_utc, a.dk_programme_instance_dim,
lag (event_end_date_time_utc) over (partition by a.dk_channel_dim, a.live_recorded order by a.subscriber_id,a.instance_start_date_time_utc) prev_end_event_time,
lag (event_start_date_time_utc) over (partition by a.dk_channel_dim, a.live_recorded order by a.subscriber_id,a.instance_start_date_time_utc) prev_start_event_time,
event_end_date_time_utc,instance_start_date_time_utc, instance_end_date_time_utc,
(case when capped_partial_flag = 1 and capped_partial_flag_supplement = 1 then capping_end_date_time_utc else instance_end_date_time_utc end) instance_end_time,
account_number,
log_start_date_time_utc, application_id, audio_track_tag, log_received_start_date_time_utc,
data_track_tag, producer_id, video_playing_flag,capping_end_date_time_utc,
video_track_tag,service_key, channel_genre, duration,dk_programme_dim,
programme_name, capped_full_flag, capped_partial_flag
 from sk_prod.vespa_dp_prog_viewed_201308 a
where capped_full_flag = 0 -- only those instances that have not been fully capped
and case when (capping_end_date_time_utc is not null and capping_end_date_time_utc < instance_start_date_time_utc) then 1 else 0 end = 0 ---instances 
--where capped_full_flag not applied even though capping applied
                           and instance_start_date_time_utc < instance_end_date_time_utc              -- Remove 0sec instances
                           and (reported_playback_speed is null or reported_playback_speed = 2) -- Live or Recorded Records
                           and account_number is not null --remove instances we do not know the account for
                           and subscriber_id is not null --remove instances we do not know the subscriber_id for
                           and (type_of_viewing_event in ('HD Viewing Event', 'Sky+ time-shifted viewing event', 'TV Channel Viewing')-- limit to keep out 
                            --interactive viewing and other service viewing event i.e. where it could not identify viewing event type it was
                            or (type_of_viewing_event = 'Other Service Viewing Event' 
                            and service_type_description in ('NVOD service','High Definition TV test service','Digital TV channel'))) --need other service viewing where relates to TV viewing
                           and capping_end_date_time_utc is not null -- only those records where the event has been given a capped event end time
and DATEDIFF(second,instance_start_date_time_utc, instance_end_date_time_utc) > 6 --capping we do removes all records with viewing of 6 secs or less 
and dk_channel_dim > 0 --only interested in where we have a channel
and panel_id = 12 --currently panel 12 but should be using panel_id in (11,12) for data from November 2013 onwards to allow for 
and cast (instance_start_date_time_utc as date) = '2013-08-15'
and service_key = 1520

-- ##############################################################################################################
-- ##### STEP 2.0 Ended									                    #####
-- ##############################################################################################################
