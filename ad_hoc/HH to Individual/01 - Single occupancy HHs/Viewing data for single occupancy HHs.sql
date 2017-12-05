/*

ETL for the learning dataset of single occupancy households

Lead: Claudio Lima
11/09/2013

*/

/*
Vespa HHs
hhcomposition	age_band	count()
4	16-19	 14 
4	20-24	 324 
4	25-34	 5,387 
4	35-44	 8,578 
4	45-64	 17,318 
4	65+	     15,938 
5	16-19	 15 
5	20-24	 464 
5	25-34	 5,759 
5	35-44	 9,536 
5	45-64	 15,890 
5	65+	     22,328 
4 - male 
5 - female
*/

select top 100 * from glasera.vespa_single_occupancy_type

-- Number of HHs in Vespa by gender/age
select hhcomposition,age_band,count(*)
from glasera.vespa_single_occupancy_type
group by hhcomposition,age_band
order by hhcomposition,age_band

-- For a representation of Winter TV viewing we'll pick week 11-17 Mar
-- Checks performed:
-- Max temperature for Birmingham varied between 1-9C
-- No bank holidays 
-- Premier league was on during the weekend
select date(event_start_date_time_utc),count(*)
from sk_prod.vespa_dp_prog_viewed_201303
where date(event_start_date_time_utc) >= '2013-03-11'
and date(event_start_date_time_utc) <= '2013-03-17'
group by date(event_start_date_time_utc)
order by date(event_start_date_time_utc)
/*
'2013-03-11',29921336
'2013-03-12',22842714
'2013-03-13',21321057
'2013-03-14',21400377
'2013-03-15',21518264
'2013-03-16',26723510
'2013-03-17',26896723
*/

-- Get account number for winter viewing
select distinct account_number
into #accounts_winter_viewing
from vespa_analysts.SC2_Intervals
where (reporting_starts < '2013-03-11' and reporting_ends > '2013-03-17')
or (reporting_starts >= '2013-03-11' and reporting_starts <= '2013-03-17')
or (reporting_ends >= '2013-03-11' and reporting_ends <= '2013-03-17')
-- 455,436

-- For a representation of Summer TV viewing we'll pick week 22-28 Jul
-- Checks performed:
-- Max temperature for Birmingham varied between 23-29C
-- No holiday banks
-- Premier league was NOT on during the weekend
select date(event_start_date_time_utc),count(*)
from sk_prod.vespa_dp_prog_viewed_201307
where date(event_start_date_time_utc) >= '2013-07-22'
and date(event_start_date_time_utc) <= '2013-07-28'
group by date(event_start_date_time_utc)
order by date(event_start_date_time_utc)
/*
2013-07-22,27305369
2013-07-23,28545069
2013-07-24,26119211
2013-07-25,27114157
2013-07-26,26388095
2013-07-27,26939544
2013-07-28,29613002
*/

-- Get account number for summer viewing
select distinct account_number
into #accounts_summer_viewing
from vespa_analysts.SC2_Intervals
where (reporting_starts < '2013-07-22' and reporting_ends > '2013-07-28')
or (reporting_starts >= '2013-07-22' and reporting_starts <= '2013-07-28')
or (reporting_ends >= '2013-07-22' and reporting_ends <= '2013-07-28')
-- 559,420

-- Select accounts that have watched TV in both winter and summer weeks
select w.account_number
into #Vespa_Accounts_Winter_Summer_Viewing
from #accounts_winter_viewing w
inner join #accounts_summer_viewing s
on w.account_number = s.account_number
-- 338,398

-- Single occupancy accounts with viewing data for both winter and summer weeks
select so.*
into vespa_single_occupancy_w_viewing_data
from glasera.vespa_single_occupancy_type so
inner join #Vespa_Accounts_Winter_Summer_Viewing v
on so.account_number = v.account_number
-- 63,315

-- Number of HHs in Vespa by gender/age with viewing data for both winter and summer weeks
select hhcomposition,age_band,count(*)
from vespa_single_occupancy_w_viewing_data
group by hhcomposition,age_band
order by hhcomposition,age_band
/*
04,16-19,6
04,20-24,124
04,25-34,2738
04,35-44,4781
04,45-64,10277
04,65+,9273

05,16-19,2
05,20-24,164
05,25-34,2793
05,35-44,5126
05,45-64,9208
05,65+,12665
*/

/*
If we pick 10% of HHs (age 25+) we have almost 6K HHS
Therefore, 6K HHs x (25M events /400K HHs) x 14 days = 5.25M events
*/

-- Learning dataset
select *
into vespa_single_occupancy_dataset_accounts
from vespa_single_occupancy_w_viewing_data
where hhcomposition in ('04','05')
and ( 
    (age_band = '20-24') 
    or
    (age_band in ('25-34','35-44','45-64','65+') and right(account_number,1)='0')
)
-- 6043


-- Number of HHs in Vespa by gender/age with viewing data for both winter and summer weeks
select hhcomposition,age_band,count(*)
from vespa_single_occupancy_dataset_accounts
group by hhcomposition,age_band
order by hhcomposition,age_band
/*
04,20-24,124
04,25-34,267
04,35-44,503
04,45-64,1079
04,65+,913
05,20-24,164
05,25-34,290
05,35-44,497
05,45-64,888
05,65+,1318
*/

---------------------------------------------
-- Get viewing data for accounts in dataset
---------------------------------------------

-- Look at varibles to decide which ones to include in the training/test dataset
select top 100 * from sk_prod.vespa_dp_prog_viewed_201307

-- Get viewiwing data

select *
into Single_HH_Viewing_Data_2weeks
from (
select --top 1000 
        case
            when left(right(ac.account_number,2),1) in ('0','1','2','3','4') -- second digit from the right
            then 1
            else 0
        end as Training_Dataset 
        ,case
            when ac.hhcomposition = '04' then 'M'
            when ac.hhcomposition = '05' then 'F'
        end as Gender
        ,ac.age_band
        ,Gender || ' ' || ac.age_band as Gender_Age
        ,ac.account_number
        ,subscriber_id
        ,cb_key_household
        -- Input variables
        ,month(     case
                        when (EVENT_START_DATE_TIME_UTC <  '2013-03-31 01:00:00') then EVENT_START_DATE_TIME_UTC                      -- Oct 12-Mar 13 => UTC = Local
                        when (EVENT_START_DATE_TIME_UTC <  '2013-10-27 02:00:00') then dateadd(hour, 1, EVENT_START_DATE_TIME_UTC)    -- Mar 13-Oct 13 => DST, add 1 hour to UTC
                        else NULL  -- the scrippt will have to be updated past Mar 2014
                    end) as event_start_month
        ,day(     case
                        when (EVENT_START_DATE_TIME_UTC <  '2013-03-31 01:00:00') then EVENT_START_DATE_TIME_UTC                      -- Oct 12-Mar 13 => UTC = Local
                        when (EVENT_START_DATE_TIME_UTC <  '2013-10-27 02:00:00') then dateadd(hour, 1, EVENT_START_DATE_TIME_UTC)    -- Mar 13-Oct 13 => DST, add 1 hour to UTC
                        else NULL  -- the scrippt will have to be updated past Mar 2014
                    end) as event_start_day
        ,datepart(dw,
                    case
                        when (EVENT_START_DATE_TIME_UTC <  '2013-03-31 01:00:00') then EVENT_START_DATE_TIME_UTC                      -- Oct 12-Mar 13 => UTC = Local
                        when (EVENT_START_DATE_TIME_UTC <  '2013-10-27 02:00:00') then dateadd(hour, 1, EVENT_START_DATE_TIME_UTC)    -- Mar 13-Oct 13 => DST, add 1 hour to UTC
                        else NULL  -- the scrippt will have to be updated past Mar 2014
                    end) as event_start_dow
          ,datepart(hour,
                    case
                        when (EVENT_START_DATE_TIME_UTC <  '2013-03-31 01:00:00') then EVENT_START_DATE_TIME_UTC                      -- Oct 12-Mar 13 => UTC = Local
                        when (EVENT_START_DATE_TIME_UTC <  '2013-10-27 02:00:00') then dateadd(hour, 1, EVENT_START_DATE_TIME_UTC)    -- Mar 13-Oct 13 => DST, add 1 hour to UTC
                        else NULL  -- the script will have to be updated past Mar 2014
                      end) as event_start_hour
          ,datepart(hour,
                    case
                        when (instance_START_DATE_TIME_UTC <  '2013-03-31 01:00:00') then instance_START_DATE_TIME_UTC                      -- Oct 12-Mar 13 => UTC = Local
                        when (instance_START_DATE_TIME_UTC <  '2013-10-27 02:00:00') then dateadd(hour, 1, instance_START_DATE_TIME_UTC)    -- Mar 13-Oct 13 => DST, add 1 hour to UTC
                        else NULL                                                                                                   -- the scrippt will have to be updated past Mar 2014
                      end) as instance_start_hour
        ,duration
        ,case 
            when capped_partial_flag = 1
            then datediff(minute,event_start_date_time_utc,capping_end_date_time_utc)
            else datediff(minute,event_start_date_time_utc,event_end_date_time_utc)
         end as event_duration_min_capped
        ,case 
            when capped_partial_flag = 1
            then datediff(minute,instance_start_date_time_utc,capping_end_date_time_utc)
            else datediff(minute,instance_start_date_time_utc,instance_end_date_time_utc)
         end as instance_duration_min_capped
        ,duration_since_last_viewing_event
        ,time_in_seconds_since_recording
        ,black_and_white_flag
        ,closed_caption_flag
        ,episode_broadcast_count
        ,language
        ,last_in_series_indicator_flag
        ,new_series_indicator_flag
        ,new_show_flag
        ,parental_rating_description
        ,premiere_indicator_flag
        ,programme_instance_duration/60 as programme_instance_duration_min
        ,case 
            when coalesce(programme_instance_duration_min,0) > 0 
            then round(instance_duration_min_capped*1.0/programme_instance_duration_min,1) 
            else NULL
        end as Proportion_Programme_Watched
        ,repeat_flag
        ,series_inclusion_flag
        ,sound_type_description
        ,stereo_flag
        ,genre_description
        ,sub_genre_description
        ,barb_name
        ,channel_genre
        ,channel_name
        ,epg_group_name
        ,grouping_indicator
        ,network_indicator
        ,pay_free_indicator
        ,sensitive_channel
        ,service_key
        ,service_type_description
        ,type_of_viewing_event
        ,multiroom_indicator
        ,personalisation_flag
        ,live_recorded
        ,platform
        ,playback_speed
        ,playback_type
        ,datediff(day,broadcast_start_date_time_utc,event_start_date_time_utc) as air_view_lag_days
--        ,broadcast_start_date_time_utc
--        ,broadcast_end_date_time_utc
--        ,event_start_date_time_utc
--        ,event_end_date_time_utc
--        ,instance_start_date_time_utc
--        ,instance_end_date_time_utc
        ,next_genre_description
        ,next_sub_genre_description
        ,next_channel_name
        ,previous_genre_description
        ,previous_sub_genre_description
        ,previous_channel_name
        ,subtitle_flag
        ,surround_flag
        ,three_d_flag
        ,true_hd_flag
        ,wide_screen_flag
        ,broadcast_time_of_day
--        ,capping_end_date_time_utc
from vespa_single_occupancy_dataset_accounts ac
inner join sk_prod.vespa_dp_prog_viewed_201303 ve
on ac.account_number = ve.account_number
where ve.event_start_date_time_utc >= '2013-03-11 00:00:00'
and ve.event_start_date_time_utc <= '2013-03-17 23:59:59'
and ve.capped_full_flag = 0
UNION ALL
select --top 1000 
        case
            when left(right(ac.account_number,2),1) in ('0','1','2','3','4') -- second digit from the right
            then 1
            else 0
        end as Training_Dataset 
        ,case
            when ac.hhcomposition = '04' then 'M'
            when ac.hhcomposition = '05' then 'F'
        end as Gender
        ,ac.age_band
        ,Gender || ' ' || ac.age_band as Gender_Age
        ,ac.account_number
        ,subscriber_id
        ,cb_key_household
        -- Input variables
        ,month(     case
                        when (EVENT_START_DATE_TIME_UTC <  '2013-03-31 01:00:00') then EVENT_START_DATE_TIME_UTC                      -- Oct 12-Mar 13 => UTC = Local
                        when (EVENT_START_DATE_TIME_UTC <  '2013-10-27 02:00:00') then dateadd(hour, 1, EVENT_START_DATE_TIME_UTC)    -- Mar 13-Oct 13 => DST, add 1 hour to UTC
                        else NULL  -- the scrippt will have to be updated past Mar 2014
                    end) as event_start_month
        ,day(     case
                        when (EVENT_START_DATE_TIME_UTC <  '2013-03-31 01:00:00') then EVENT_START_DATE_TIME_UTC                      -- Oct 12-Mar 13 => UTC = Local
                        when (EVENT_START_DATE_TIME_UTC <  '2013-10-27 02:00:00') then dateadd(hour, 1, EVENT_START_DATE_TIME_UTC)    -- Mar 13-Oct 13 => DST, add 1 hour to UTC
                        else NULL  -- the scrippt will have to be updated past Mar 2014
                    end) as event_start_day
        ,datepart(dw,
                    case
                        when (EVENT_START_DATE_TIME_UTC <  '2013-03-31 01:00:00') then EVENT_START_DATE_TIME_UTC                      -- Oct 12-Mar 13 => UTC = Local
                        when (EVENT_START_DATE_TIME_UTC <  '2013-10-27 02:00:00') then dateadd(hour, 1, EVENT_START_DATE_TIME_UTC)    -- Mar 13-Oct 13 => DST, add 1 hour to UTC
                        else NULL  -- the scrippt will have to be updated past Mar 2014
                    end) as event_start_dow
          ,datepart(hour,
                    case
                        when (EVENT_START_DATE_TIME_UTC <  '2013-03-31 01:00:00') then EVENT_START_DATE_TIME_UTC                      -- Oct 12-Mar 13 => UTC = Local
                        when (EVENT_START_DATE_TIME_UTC <  '2013-10-27 02:00:00') then dateadd(hour, 1, EVENT_START_DATE_TIME_UTC)    -- Mar 13-Oct 13 => DST, add 1 hour to UTC
                        else NULL  -- the script will have to be updated past Mar 2014
                      end) as event_start_hour
          ,datepart(hour,
                    case
                        when (instance_START_DATE_TIME_UTC <  '2013-03-31 01:00:00') then instance_START_DATE_TIME_UTC                      -- Oct 12-Mar 13 => UTC = Local
                        when (instance_START_DATE_TIME_UTC <  '2013-10-27 02:00:00') then dateadd(hour, 1, instance_START_DATE_TIME_UTC)    -- Mar 13-Oct 13 => DST, add 1 hour to UTC
                        else NULL                                                                                                   -- the scrippt will have to be updated past Mar 2014
                      end) as instance_start_hour
        ,duration
        ,case 
            when capped_partial_flag = 1
            then datediff(minute,event_start_date_time_utc,capping_end_date_time_utc)
            else datediff(minute,event_start_date_time_utc,event_end_date_time_utc)
         end as event_duration_min_capped
        ,case 
            when capped_partial_flag = 1
            then datediff(minute,instance_start_date_time_utc,capping_end_date_time_utc)
            else datediff(minute,instance_start_date_time_utc,instance_end_date_time_utc)
         end as instance_duration_min_capped
        ,duration_since_last_viewing_event
        ,time_in_seconds_since_recording
        ,black_and_white_flag
        ,closed_caption_flag
        ,episode_broadcast_count
        ,language
        ,last_in_series_indicator_flag
        ,new_series_indicator_flag
        ,new_show_flag
        ,parental_rating_description
        ,premiere_indicator_flag
        ,programme_instance_duration/60 as programme_instance_duration_min
        ,case 
            when coalesce(programme_instance_duration_min,0) > 0 
            then round(instance_duration_min_capped*1.0/programme_instance_duration_min,1) 
            else NULL
        end as Proportion_Programme_Watched
        ,repeat_flag
        ,series_inclusion_flag
        ,sound_type_description
        ,stereo_flag
        ,genre_description
        ,sub_genre_description
        ,barb_name
        ,channel_genre
        ,channel_name
        ,epg_group_name
        ,grouping_indicator
        ,network_indicator
        ,pay_free_indicator
        ,sensitive_channel
        ,service_key
        ,service_type_description
        ,type_of_viewing_event
        ,multiroom_indicator
        ,personalisation_flag
        ,live_recorded
        ,platform
        ,playback_speed
        ,playback_type
        ,datediff(day,broadcast_start_date_time_utc,event_start_date_time_utc) as air_view_lag_days
--        ,broadcast_start_date_time_utc
--        ,broadcast_end_date_time_utc
--        ,event_start_date_time_utc
--        ,event_end_date_time_utc
--        ,instance_start_date_time_utc
--        ,instance_end_date_time_utc
        ,next_genre_description
        ,next_sub_genre_description
        ,next_channel_name
        ,previous_genre_description
        ,previous_sub_genre_description
        ,previous_channel_name
        ,subtitle_flag
        ,surround_flag
        ,three_d_flag
        ,true_hd_flag
        ,wide_screen_flag
        ,broadcast_time_of_day
--        ,capping_end_date_time_utc
from vespa_single_occupancy_dataset_accounts ac
inner join sk_prod.vespa_dp_prog_viewed_201307 ve
on ac.account_number = ve.account_number
where ve.event_start_date_time_utc >= '2013-07-22 00:00:00'
and ve.event_start_date_time_utc <= '2013-07-28 23:59:59'
and ve.capped_full_flag = 0
) t
-- END
-- 3601632 row(s) affected
commit

select top 100 * from Single_HH_Viewing_Data_2weeks


-- Add Experian household variables
select top 100 * from SK_PROD.EXPERIAN_CONSUMERVIEW

select ve.*
        ,exp.h_affluence_v2
        ,exp.h_income_band
        ,exp.h_equivalised_income_band
        ,exp.h_fss_group
        ,exp.h_lifestage
        ,exp.h_mosaic_ni_group
        ,exp.h_mosaic_uk_group
        ,exp.h_presence_of_child_aged_0_4_2011
        ,exp.h_presence_of_child_aged_12_17_2011
        ,exp.h_presence_of_child_aged_5_11_2011
into Single_HH_Age_Gender_Dataset
from Single_HH_Viewing_Data_2weeks ve
left join ( 
            select cb_key_household
                    ,max(h_affluence_v2) as h_affluence_v2
                    ,max(h_income_band) as h_income_band
                    ,max(h_equivalised_income_band) as h_equivalised_income_band
                    ,max(h_fss_group) as h_fss_group
                    ,max(h_lifestage) as h_lifestage
                    ,max(h_mosaic_ni_group) as h_mosaic_ni_group
                    ,max(h_mosaic_uk_group) as h_mosaic_uk_group
                    ,max(h_presence_of_child_aged_0_4_2011) as h_presence_of_child_aged_0_4_2011
                    ,max(h_presence_of_child_aged_12_17_2011) as h_presence_of_child_aged_12_17_2011
                    ,max(h_presence_of_child_aged_5_11_2011) as h_presence_of_child_aged_5_11_2011
            from SK_PROD.EXPERIAN_CONSUMERVIEW 
            group by cb_key_household
           ) exp
on ve.cb_key_household = exp.cb_key_household
-- 3601632 row(s) affected

-- Look at the distribution
select Gender_Age
        ,count(distinct account_number) as num_accounts
        ,count(distinct subscriber_id) as num_boxes
        ,count(*) as num_events
        ,count(*)*1.0/count(distinct account_number) as avg_num_events
from Single_HH_Age_Gender_Dataset
group by Gender_Age
order by Gender_Age

-- Get a 1% sample for model prototyping
select *
into Single_HH_Age_Gender_Dataset_1perc_sample
from Single_HH_Age_Gender_Dataset
where left(right(account_number,4),2)='00'
-- 36481 row(s) affected

select gender_age
        ,count(distinct account_number) as num_aacounts
        ,count(*) as num_events
from Single_HH_Age_Gender_Dataset_1perc_sample
group by gender_age
order by gender_age
/*
gender_age,num_aacounts,num_events
'F 25-34',3,1359
'F 35-44',4,2170
'F 45-64',13,6817
'F 65+',18,9968
'M 20-24',1,644
'M 25-34',3,748
'M 35-44',11,4612
'M 45-64',10,7396
'M 65+',6,2767
*/

-- Get 10 % sample
select *
into Single_HH_Age_Gender_Dataset_10perc_sample
from Single_HH_Age_Gender_Dataset
where left(right(account_number,2),1)='0'
-- 362024 row(s) affected

select gender_age
        ,count(distinct account_number) as num_aacounts
        ,count(*) as num_events
from Single_HH_Age_Gender_Dataset_10perc_sample
group by gender_age
order by gender_age
/*
gender_age,num_aacounts,num_events
'F 20-24',15,6828
'F 25-34',25,12876
'F 35-44',50,25925
'F 45-64',80,46965
'F 65+',139,85358
'M 20-24',11,8024
'M 25-34',21,9184
'M 35-44',51,34352
'M 45-64',116,81446
'M 65+',89,51066
*/

-- Give permissions
grant all on Single_HH_Age_Gender_Dataset to angeld;
grant all on Single_HH_Age_Gender_Dataset_1perc_sample to angeld;
grant all on Single_HH_Age_Gender_Dataset_10perc_sample to angeld;

grant all on Single_HH_Age_Gender_Dataset to tanghoi;
grant all on Single_HH_Age_Gender_Dataset_1perc_sample to tanghoi;
grant all on Single_HH_Age_Gender_Dataset_10perc_sample to tanghoi;



