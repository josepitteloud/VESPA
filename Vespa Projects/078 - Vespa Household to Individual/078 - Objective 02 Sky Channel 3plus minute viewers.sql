

---This code creates a summary of all viewing of at least 3 minutes continuous Live viewing of any programme on Sky1/Sky Arts 1/Sky Living channels (SD and HD)
---between 13th and 19th July (Broadcast days)---

---Uses Panel 4 and 12 as dates used are where migration of Panels being undertaken

---Run Uncapped and Unscaled Live viewing for 3 sky channels for 1 week to get profile details

---Currently issues with  sk_prod.VESPA_EVENTS_VIEWED_ALL in that subscriber_id and account_number fields not always populated - these records are excluded in query
--drop table dbarnett.project078_summary_sky_channels_programmes_viewed_3plus_minutes;
select subscriber_id
,account_number
,channel_name
,programme_name
,broadcast_start_date_time_utc
,broadcast_end_date_time_utc
,cb_key_household
,instance_start_date_time_utc
,instance_end_date_time_utc
,event_start_date_time_utc
,event_end_date_time_utc
into dbarnett.project078_summary_sky_channels_programmes_viewed_3plus_minutes
from  sk_prod.VESPA_EVENTS_VIEWED_ALL
where live_recorded = 'LIVE' and broadcast_start_date_time_utc between '2012-06-13 05:00:00' and '2012-06-20 04:59:59'
and datediff(second,event_start_date_time_utc,event_end_date_time_utc)>=180
and panel_id in (4,12)
and  channel_name in (
'Sky Arts 1'
,'Sky Arts 1 HD'
,'Sky1'
,'Sky1 HD'
,'Sky Living'
,'Sky Living HD')
and subscriber_id is not null and account_number is not null
;
commit;

-- add indexes to improve performance
create hg index idx1 on dbarnett.project078_summary_sky_channels_programmes_viewed_3plus_minutes(subscriber_id);
--create hg index idx2 on dbarnett.project078_summary_sky_channels_programmes_viewed_3plus_minutes(account_number);
create hg index idx3 on dbarnett.project078_summary_sky_channels_programmes_viewed_3plus_minutes(channel_name);
create hg index idx4 on dbarnett.project078_summary_sky_channels_programmes_viewed_3plus_minutes(programme_name);
create dttm index idx5 on dbarnett.project078_summary_sky_channels_programmes_viewed_3plus_minutes(broadcast_start_date_time_utc);
create dttm index idx6 on dbarnett.project078_summary_sky_channels_programmes_viewed_3plus_minutes(broadcast_end_date_time_utc);
create hg index idx7 on dbarnett.project078_summary_sky_channels_programmes_viewed_3plus_minutes(cb_key_household);
---Dedupe to one record per programme per subscriber_id
commit;

--drop table dbarnett.project078_summary_sky_channels_programmes_viewed_deduped;
select subscriber_id
,account_number 
,case when channel_name = 'Sky Arts 1 HD' then 'Sky Arts 1' 
      when channel_name = 'Sky1 HD' then 'Sky1' 
      when channel_name = 'Sky Living HD' then 'Sky Living' else channel_name end as channel_name_grouped
,programme_name
,broadcast_start_date_time_utc
,broadcast_end_date_time_utc
,cb_key_household
into dbarnett.project078_summary_sky_channels_programmes_viewed_deduped
from dbarnett.project078_summary_sky_channels_programmes_viewed_3plus_minutes
group by subscriber_id
,account_number 
, channel_name_grouped
,programme_name
,broadcast_start_date_time_utc
,broadcast_end_date_time_utc
,cb_key_household
;
commit;


---Output - Boxes watching per programme - Note all Times are currently UTC which is one hour behing British Summer Time

select channel_name_grouped
,programme_name
,broadcast_start_date_time_utc
,broadcast_end_date_time_utc

,count(*) as boxes
from dbarnett.project078_summary_sky_channels_programmes_viewed_deduped
group by channel_name_grouped
,programme_name
,broadcast_start_date_time_utc
,broadcast_end_date_time_utc

order by channel_name_grouped
,broadcast_start_date_time_utc
,broadcast_end_date_time_utc
;
commit;
grant all on dbarnett.project078_summary_sky_channels_programmes_viewed_deduped to public;
---Next Steps - Match viewing summary to consumerview to get profile

select cb_key_household
,sum(case when p_gender = '0' then 1 else 0 end) as males
,sum(case when p_gender = '1' then 1 else 0 end) as females
,sum(case when p_gender = 'U' then 1 else 0 end) as unknown_gender
,sum(case when p_gender = '0' and person_age = '0'  then 1 else 0 end) as males_aged_18_25
,sum(case when p_gender = '0' and person_age = '1'  then 1 else 0 end) as males_aged_26_35
,sum(case when p_gender = '0' and person_age = '2'  then 1 else 0 end) as males_aged_36_45
,sum(case when p_gender = '0' and person_age = '3'  then 1 else 0 end) as males_aged_46_55
,sum(case when p_gender = '0' and person_age = '4'  then 1 else 0 end) as males_aged_56_65
,sum(case when p_gender = '0' and person_age = '5'  then 1 else 0 end) as males_aged_66_plus
,sum(case when p_gender = '0' and person_age = 'U'  then 1 else 0 end) as males_aged_unk

,sum(case when p_gender = '1' and person_age = '0'  then 1 else 0 end) as females_aged_18_25
,sum(case when p_gender = '1' and person_age = '1'  then 1 else 0 end) as females_aged_26_35
,sum(case when p_gender = '1' and person_age = '2'  then 1 else 0 end) as females_aged_36_45
,sum(case when p_gender = '1' and person_age = '3'  then 1 else 0 end) as females_aged_46_55
,sum(case when p_gender = '1' and person_age = '4'  then 1 else 0 end) as females_aged_56_65
,sum(case when p_gender = '1' and person_age = '5'  then 1 else 0 end) as females_aged_66_plus
,sum(case when p_gender = '1' and person_age = 'U'  then 1 else 0 end) as females_aged_unk

,sum(case when p_gender = 'U' and person_age = '0'  then 1 else 0 end) as unknown_gender_aged_18_25
,sum(case when p_gender = 'U' and person_age = '1'  then 1 else 0 end) as unknown_gender_aged_26_35
,sum(case when p_gender = 'U' and person_age = '2'  then 1 else 0 end) as unknown_gender_aged_36_45
,sum(case when p_gender = 'U' and person_age = '3'  then 1 else 0 end) as unknown_gender_aged_46_55
,sum(case when p_gender = 'U' and person_age = '4'  then 1 else 0 end) as unknown_gender_aged_56_65
,sum(case when p_gender = 'U' and person_age = '5'  then 1 else 0 end) as unknown_gender_aged_66_plus
,sum(case when p_gender = 'U' and person_age = 'U'  then 1 else 0 end) as unknown_gender_aged_unk

,max(case when family_lifestage in ('02','03','06','07','10') then 1 else 0 end) as presence_of_children

into #consumerview_data_one_record_per_hh
from sk_prod.experian_consumerview as a
where cb_address_status = '1' and cb_address_dps is not null 
group by cb_key_household
;
commit;
create hg index idx1 on #consumerview_data_one_record_per_hh (cb_key_household);
commit;


select a.*
,b.males as consumerview_males
,b.females as consumerview_females 
,b.unknown_gender as consumerview_unknown_gender
,b.males_aged_18_25 as consumerview_males_aged_18_25
,b.males_aged_26_35 as consumerview_males_aged_26_35
,b.males_aged_36_45 as consumerview_males_aged_36_45
,b.males_aged_46_55 as consumerview_males_aged_46_55
,b.males_aged_56_65 as consumerview_males_aged_56_65
,b.males_aged_66_plus as consumerview_males_aged_66_plus
,b.males_aged_unk as consumerview_males_aged_unk

,b.females_aged_18_25 as consumerview_females_aged_18_25
,b.females_aged_26_35 as consumerview_females_aged_26_35
,b.females_aged_36_45 as consumerview_females_aged_36_45
,b.females_aged_46_55 as consumerview_females_aged_46_55
,b.females_aged_56_65 as consumerview_females_aged_56_65
,b.females_aged_66_plus as consumerview_females_aged_66_plus
,b.females_aged_unk as consumerview_females_aged_unk

,b.unknown_gender_aged_18_25 as consumerview_unknown_gender_aged_18_25
,b.unknown_gender_aged_26_35 as consumerview_unknown_gender_aged_26_35
,b.unknown_gender_aged_36_45 as consumerview_unknown_gender_aged_36_45
,b.unknown_gender_aged_46_55 as consumerview_unknown_gender_aged_46_55
,b.unknown_gender_aged_56_65 as consumerview_unknown_gender_aged_56_65
,b.unknown_gender_aged_66_plus as consumerview_unknown_gender_aged_66_plus
,b.unknown_gender_aged_unk as consumerview_unknown_gender_aged_unk
,b.presence_of_children as consumerview_presence_of_children
into dbarnett.project078_objective2_sky_channels_watched_with_consumerview_details_added
from dbarnett.project078_summary_sky_channels_programmes_viewed_deduped as a
left outer join #consumerview_data_one_record_per_hh as b
on a.cb_key_household=b.cb_key_household;
commit;

grant all on dbarnett.project078_objective2_sky_channels_watched_with_consumerview_details_added to public;

commit;

--select top 100 *   from sk_prod.VESPA_EVENTS_VIEWED_ALL;

---Updated 20120821 ----

---Create Capping Rules for viewing Using Phase II data and Phase 1b Capping----


----Tables Used/Created-----
--------------------------------------------------------------------------------
-- PART A - Capping
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- A01 - Identify extreme viewing and populate max and min daily caps
-- Table produced for 10% capping (Live)
--------------------------------------------------------------------------------
--select * from dbarnett.project078_vespa_max_caps_13_20_jun_2012;
-- Max Caps:
--select * from dbarnett.project078_vespa_max_caps_13_20_jun_2012;
IF object_id('dbarnett.project078_vespa_max_caps_13_20_jun_2012') 
IS NOT NULL DROP TABLE dbarnett.project078_vespa_max_caps_13_20_jun_2012;

create table dbarnett.project078_vespa_max_caps_13_20_jun_2012
(
    event_start_day as date
    , event_start_hour as integer
    , live as smallint
    , ntile_100 as integer
    , min_dur_mins as integer
);

--drop table gm_ntile_temph_db;
    select
        account_number
        , subscriber_id
        , event_start_date_time_utc
        , duration
        , 1 as live    ---Only looking at Live Events
        , date(event_start_date_time_utc) as event_start_day
        , datepart(hour, event_start_date_time_utc) as event_start_hour
        , cast(duration/ 60 as int) as dur_mins
    into
        gm_ntile_temph_db
    from sk_prod.VESPA_EVENTS_VIEWED_ALL
where   live_recorded = 'LIVE' 
        and broadcast_start_date_time_utc between '2012-06-13 05:00:00' and '2012-06-20 04:59:59'
        and panel_id in (4,12)
        and video_playing_flag = 1
        and cast(duration/ 86400 as int) = 0
group by account_number
        , subscriber_id
        , event_start_date_time_utc
        , duration
        , live    ---Only looking at Live Events
        , event_start_day
        , event_start_hour
        , dur_mins
;

--select @var_sql_capping_1b


    -- create indexes to speed up the ntile creation
    create hng index idx1 on gm_ntile_temph_db(event_start_day);
    create hng index idx2 on gm_ntile_temph_db(event_start_hour);
    create hng index idx3 on gm_ntile_temph_db(live);
    create hng index idx4 on gm_ntile_temph_db(dur_mins);

    -- query ntiles for given date and insert into the persistent table
    insert into dbarnett.project078_vespa_max_caps_13_20_jun_2012
    (
    select
            event_start_day
            , event_start_hour
            , live
            , ntile_100
            , min(dur_mins) as min_dur_mins
        from
        (
            select
                event_start_day
                ,event_start_hour
                ,live
                ,dur_mins
                ,ntile(100) over (partition by event_start_day, event_start_hour, live order by dur_mins) as ntile_100
            into ntilesh
            from gm_ntile_temph_db
        ) a
        where ntile_100 = 91 -- modify this to adapt aggressiveness of capping, 91 means exclude top 10% of values
        group by
            event_start_day
            , event_start_hour
            , live
            , ntile_100
    );
    commit;

--IF object_id('gm_ntile_temph_db') IS NOT NULL DROP TABLE gm_ntile_temph_db;
-- add indexes
create hng index idx1 on dbarnett.project078_vespa_max_caps_13_20_jun_2012(event_start_day);
create hng index idx2 on dbarnett.project078_vespa_max_caps_13_20_jun_2012(event_start_hour);
create hng index idx3 on dbarnett.project078_vespa_max_caps_13_20_jun_2012(live);

---Min Cap set to 1 so no viewing will be removed but code kept consistent--

-- Min Caps
IF object_id('dbarnett.project078_vespa_min_cap') IS NOT NULL DROP TABLE  dbarnett.project078_vespa_min_cap;

create table  dbarnett.project078_vespa_min_cap (
    cap_secs as integer
);
insert into  dbarnett.project078_vespa_min_cap (cap_secs) values (6);

commit;


--select * from dbarnett.project078_vespa_max_caps_13_20_jun_2012;


alter table dbarnett.project078_summary_sky_channels_programmes_viewed_3plus_minutes
    add (
        capped_instance_start_date_time_utc datetime
        , capped_instance_end_date_time_utc   datetime
        , capped_x_programme_viewed_duration integer
        , capped_flag integer
    )
;
-- update the viewing start and end times for playback records

-- update table to create capped start and end times        
update dbarnett.project078_summary_sky_channels_programmes_viewed_3plus_minutes
    set capped_instance_start_date_time_utc =
        case  
            -- if start of viewing_time is beyond start_time + cap then flag as null
            when dateadd(minute, min_dur_mins, event_start_date_time_utc) < instance_start_date_time_utc then null
            -- else leave start of viewing time unchanged
            else instance_start_date_time_utc
        end
        , capped_instance_end_date_time_utc =
        case
            -- if start of viewing_time is beyond start_time + cap then flag as null
            when dateadd(minute, min_dur_mins, event_start_date_time_utc) < instance_start_date_time_utc then null
            -- if start_time+ cap is beyond end time then leave end time unchanged
            when dateadd(minute, min_dur_mins, event_start_date_time_utc) > event_end_date_time_utc then event_end_date_time_utc
            -- otherwise set end time to start_time + cap
            else dateadd(minute, min_dur_mins, event_start_date_time_utc)
        end
from
        dbarnett.project078_summary_sky_channels_programmes_viewed_3plus_minutes base left outer join dbarnett.project078_vespa_max_caps_13_20_jun_2012 caps
    on (
        date(base.event_start_date_time_utc) = caps.event_start_day
        and datepart(hour, base.event_start_date_time_utc) = caps.event_start_hour
--        and base.live = caps.live
    )  
;
commit;

--select * from vespa_analysts.vespa_201111_max_caps order by event_start_day, event_start_hour , live ;

-- calculate capped_x_programme_viewed_duration
update dbarnett.project078_summary_sky_channels_programmes_viewed_3plus_minutes
    set capped_x_programme_viewed_duration = datediff(second, capped_instance_start_date_time_utc, capped_instance_end_date_time_utc)
;

-- set capped_flag based on nature of capping
--      0 programme view not affected by capping
--      1 if programme view has been shortened by a long duration capping rule
--      2 if programme view has been excluded by a long duration capping rule

update dbarnett.project078_summary_sky_channels_programmes_viewed_3plus_minutes
    set capped_flag = 
        case
            when capped_instance_end_date_time_utc < event_end_date_time_utc then 1
            when capped_instance_start_date_time_utc is null then 2
            else 0
        end
;
commit;

-- cap based on min duration of seconds (from min_cap) and set capping flag
-- this nullifies capped_x times as for long duration cap and sets capped_flag = 3
-- note that some capped_flag = 1 records may also be updated if the capping of the end of
-- a long view resulted in a very short view
update dbarnett.project078_summary_sky_channels_programmes_viewed_3plus_minutes
    set capped_instance_start_date_time_utc = null
        , capped_instance_end_date_time_utc = null
        , capped_x_programme_viewed_duration = null
        , capped_flag = 3
    from
         dbarnett.project072_vespa_min_cap
    where
        capped_x_programme_viewed_duration < cap_secs 
;
commit;



select subscriber_id
,account_number 
,case when channel_name = 'Sky Arts 1 HD' then 'Sky Arts 1' 
      when channel_name = 'Sky1 HD' then 'Sky1' 
      when channel_name = 'Sky Living HD' then 'Sky Living' else channel_name end as channel_name_grouped
,programme_name
,broadcast_start_date_time_utc
,broadcast_end_date_time_utc
,cb_key_household
into dbarnett.project078_summary_sky_channels_programmes_viewed_capped_deduped
from dbarnett.project078_summary_sky_channels_programmes_viewed_3plus_minutes
where capped_flag not in (2,3) and datediff(second,capped_instance_start_date_time_utc,capped_instance_end_date_time_utc)>=180
group by subscriber_id
,account_number 
, channel_name_grouped
,programme_name
,broadcast_start_date_time_utc
,broadcast_end_date_time_utc
,cb_key_household
;
commit;

--select top 100 * from dbarnett.project078_summary_sky_channels_programmes_viewed_3plus_minutes where capped_flag =3


select channel_name_grouped
,programme_name
,broadcast_start_date_time_utc
,broadcast_end_date_time_utc
,case 
when dateformat(broadcast_start_date_time_utc,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,broadcast_start_date_time_utc) 
when dateformat(broadcast_start_date_time_utc,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,broadcast_start_date_time_utc) 
when dateformat(broadcast_start_date_time_utc,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,broadcast_start_date_time_utc) 
                    else broadcast_start_date_time_utc  end as broadcast_start_date_time_local

,case 
when dateformat(broadcast_end_date_time_utc,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,broadcast_end_date_time_utc) 
when dateformat(broadcast_end_date_time_utc,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,broadcast_end_date_time_utc) 
when dateformat(broadcast_end_date_time_utc,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,broadcast_end_date_time_utc) 
                    else broadcast_end_date_time_utc  end as broadcast_end_date_time_local
,count(*) as boxes
from dbarnett.project078_summary_sky_channels_programmes_viewed_capped_deduped
group by channel_name_grouped
,programme_name
,broadcast_start_date_time_utc
,broadcast_end_date_time_utc
,broadcast_start_date_time_local
,broadcast_end_date_time_local
order by channel_name_grouped
,broadcast_start_date_time_utc
,broadcast_end_date_time_utc
;
commit;
grant all on dbarnett.project078_summary_sky_channels_programmes_viewed_capped_deduped to public;
commit;

--select top 100 * from dbarnett.project078_summary_sky_channels_programmes_viewed_capped_deduped


---Add HH Profile Information to viewing 



