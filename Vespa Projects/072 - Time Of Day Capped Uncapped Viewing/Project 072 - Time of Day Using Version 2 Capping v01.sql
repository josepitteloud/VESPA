
----Capping Phase 2 Comparison - Viewing 5th-18th feb Inclusive
--drop table dbarnett.project072_capping_phase_2_feb05_18;
CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(15000);
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @var_num_days           smallint;

  -- If your programme listing is by a date range...
SET @var_prog_period_start  = '2012-02-05';
SET @var_prog_period_end    = '2012-02-19';


SET @var_cntr = 0;
SET @var_num_days = 14;

create table dbarnett.project072_capping_phase_2_feb05_18
(subscriber_id bigint
,account_number varchar(20)
,programme_trans_sk bigint
,scaling_segment_id bigint  
,scaling_weighting  real
,viewing_starts_local datetime
,viewing_stops_local datetime
);

SET @var_sql = '
insert into dbarnett.project072_capping_phase_2_feb05_18
select subscriber_id
,account_number
,programme_trans_sk
,scaling_segment_id
,scaling_weighting
--,viewing_starts as viewing_starts_utc
,case 
when dateformat(viewing_starts,''YYYY-MM-DD-HH'') between ''2010-03-28-02'' and ''2010-10-31-02'' then dateadd(hh,1,viewing_starts) 
when dateformat(viewing_starts,''YYYY-MM-DD-HH'') between ''2011-03-27-02'' and ''2011-10-30-02'' then dateadd(hh,1,viewing_starts) 
when dateformat(viewing_starts,''YYYY-MM-DD-HH'') between ''2012-03-25-02'' and ''2012-10-28-02'' then dateadd(hh,1,viewing_starts) 
                    else viewing_starts  end as viewing_starts_local
--,viewing_stops as viewing_stops_utc
,case 
when dateformat(viewing_stops,''YYYY-MM-DD-HH'') between ''2010-03-28-02'' and ''2010-10-31-02'' then dateadd(hh,1,viewing_stops) 
when dateformat(viewing_stops,''YYYY-MM-DD-HH'') between ''2011-03-27-02'' and ''2011-10-30-02'' then dateadd(hh,1,viewing_stops) 
when dateformat(viewing_stops,''YYYY-MM-DD-HH'') between ''2012-03-25-02'' and ''2012-10-28-02'' then dateadd(hh,1,viewing_stops) 
                    else viewing_stops  end as viewing_stops_local
--into #capped_phase_2_viewing
from vespa_analysts.VESPA_DAILY_AUGS_##^^*^*##
where timeshifting = ''LIVE''
'     ;

while @var_cntr < @var_num_days
begin
    EXECUTE(replace(@var_sql,'##^^*^*##',dateformat(dateadd(day, @var_cntr, @var_prog_period_start), 'yyyymmdd')))

    commit

    set @var_cntr = @var_cntr + 1
end;


commit;

--select count(*) from dbarnett.project072_capping_phase_2_feb05_18;

--Update scaling segment and weighting and add programme channel details---

update dbarnett.project072_capping_phase_2_feb05_18
set scaling_segment_id=b.scaling_segment_id
from dbarnett.project072_capping_phase_2_feb05_18  as a
left outer join vespa_analysts.scaling_dialback_intervals as b
on a.account_number = b.account_number
where cast (viewing_starts_local as date)  between b.reporting_starts and b.reporting_ends
commit;


update dbarnett.project072_capping_phase_2_feb05_18
set scaling_weighting=b.weighting
from dbarnett.project072_capping_phase_2_feb05_18  as a
left outer join vespa_analysts.scaling_weightings as b
on cast (viewing_starts_local as date) = b.scaling_day and a.scaling_segment_id=b.scaling_segment_id
commit;

alter table dbarnett.project072_capping_phase_2_feb05_18 add Channel_Name    varchar(30);

update dbarnett.project072_capping_phase_2_feb05_18
set channel_name = b.Channel_Name
from dbarnett.project072_capping_phase_2_feb05_18 as a
left outer join sk_prod.vespa_epg_dim as b
on a.programme_trans_sk=b.programme_trans_sk

;

alter table dbarnett.project072_capping_phase_2_feb05_18 add channel_name_inc_hd varchar(40);

update dbarnett.project072_capping_phase_2_feb05_18
set channel_name_inc_hd = case when det.channel_name_inc_hd is not null then det.channel_name_inc_hd else base.Channel_Name end
from dbarnett.project072_capping_phase_2_feb05_18 as base
left outer join vespa_analysts.channel_name_lookup_old  det
 on base.Channel_Name = det.Channel
;
commit;

--select * from vespa_analysts.channel_name_lookup_old;

---Create Adsmartable Box Lookup

select subscriber_id
,max(adsmartable_box) as adsmartable_subscriber_id
into dbarnett.project072_adsmartable_box_lookup
from dbarnett.project072_all_viewing
group by subscriber_id
;
commit;
create hg index idx1 on dbarnett.project072_adsmartable_box_lookup(subscriber_id);
commit;

alter table dbarnett.project072_capping_phase_2_feb05_18 add adsmartable_box tinyint;

update dbarnett.project072_capping_phase_2_feb05_18
set adsmartable_box = case when b.adsmartable_subscriber_id is null then 0 else b.adsmartable_subscriber_id end
from dbarnett.project072_capping_phase_2_feb05_18 as a
left outer join dbarnett.project072_adsmartable_box_lookup  b
 on a.subscriber_id = b.subscriber_id
;
commit;

create lf index idx1 on dbarnett.project072_capping_phase_2_feb05_18(adsmartable_box);
create hg index idx2 on dbarnett.project072_capping_phase_2_feb05_18(channel_name_inc_hd);
create dttm index idx3 on dbarnett.project072_capping_phase_2_feb05_18(viewing_starts_local);
create dttm index idx4 on dbarnett.project072_capping_phase_2_feb05_18(viewing_stops_local);

--select count(*) , sum(adsmartable_box) from dbarnett.project072_all_viewing
--select count(*) , sum(adsmartable) from #adsmartable_box_detail_by_sub_id

------------------------------------
--select day_viewing , hour_viewing , count(*) from dbarnett.project072_seconds_viewed_capped_uncapped_by_channel group by day_viewing , hour_viewing order by day_viewing , hour_viewing;
--drop table  dbarnett.project072_seconds_viewed_capped_uncapped_by_channel;


create table dbarnett.project072_seconds_viewed_capping_v2
(channel_name_inc_hd varchar (90)
,day_viewing date
,hour_viewing varchar(2)
,box_seconds_watched_capped_v2 double
);
--@hour
--'2012-02-09 01:00:00.000'
create variable @hour datetime;
set @hour = '2012-02-05 00:00:00';
--set @hour = '2012-02-14 05:00:00';

WHILE @hour <= '2012-02-19 07:00:00'

BEGIN

insert into dbarnett.project072_seconds_viewed_capping_v2

select channel_name_inc_hd
,cast(@hour as date) as day_viewing
,dateformat(@hour,'HH') as hour_viewing
,sum(case when viewing_stops_local is null then 0
          when viewing_starts_local >=dateadd(minute,60,@hour) then 0
          when viewing_stops_local <@hour then 0
            when viewing_starts_local <= @hour and viewing_stops_local >= dateadd(minute,60,@hour) then 3600*scaling_weighting

            when viewing_starts_local <= @hour and viewing_stops_local >= @hour then 
                    datediff(ss,@hour,viewing_stops_local)*scaling_weighting

            when viewing_starts_local >= @hour and viewing_stops_local <= dateadd(minute,60,@hour) then 
            datediff(ss,viewing_starts_local,viewing_stops_local)*scaling_weighting

            when viewing_starts_local >= @hour and viewing_stops_local >= dateadd(minute,60,@hour) then
         datediff(ss,viewing_starts_local,dateadd(minute,60,@hour))*scaling_weighting
     else 0 end) as box_seconds_watched_capped_v2
from dbarnett.project072_capping_phase_2_feb05_18
where adsmartable_box=1
group by channel_name_inc_hd
,day_viewing
, hour_viewing


SET @hour  = dateadd(hour,1,@hour)
commit

end;

--select @hour
commit;









commit;





/*
--select top 500 * from dbarnett.project072_capping_phase_2_feb05_18 ;
--select top 500 * from vespa_analysts.VESPA_DAILY_AUGS_20120208 ;
select * from vespa_analysts.VESPA_DAILY_AUGS_20120208 where subscriber_id = 17828608 order by viewing_starts , viewing_stops;

select 
        vw.cb_row_ID,vw.Account_Number,vw.Subscriber_Id,vw.Cb_Key_Household,vw.Cb_Key_Family
        ,vw.Cb_Key_Individual,vw.Event_Type,vw.X_Type_Of_Viewing_Event,vw.Adjusted_Event_Start_Time
        ,vw.X_Adjusted_Event_End_Time,vw.X_Viewing_Start_Time,vw.X_Viewing_End_Time
        ,prog.Tx_Start_Datetime_UTC,prog.Tx_End_Datetime_UTC,vw.Recorded_Time_UTC,vw.Play_Back_Speed
        ,vw.X_Event_Duration,vw.X_Programme_Duration,vw.X_Programme_Viewed_Duration
        ,vw.X_Programme_Percentage_Viewed,vw.X_Viewing_Time_Of_Day,vw.Programme_Trans_Sk
        ,prog.Channel_Name,prog.Epg_Title,prog.Genre_Description,prog.Sub_Genre_Description
, sum(x_programme_viewed_duration) over (partition by subscriber_id, adjusted_event_start_time order by cb_row_id) as x_cumul_programme_viewed_duration 
 ,vw.service_key                
    ,vw.original_network_id        
    ,vw.transport_stream_id        
    ,vw.si_service_id                 

 from sk_prod.VESPA_STB_PROG_EVENTS_20120208 as vw
          left outer join   sk_prod.VESPA_EPG_DIM as prog
          on vw.programme_trans_sk = prog.programme_trans_sk
        -- Filter for viewing events during extraction
    where 
video_playing_flag = 1 and    
      adjusted_event_start_time <> x_adjusted_event_end_time
     and (    x_type_of_viewing_event in ('TV Channel Viewing','Sky+ time-shifted viewing event','HD Viewing Event')
          or (x_type_of_viewing_event = ('Other Service Viewing Event')
              and x_si_service_type = 'High Definition TV test service'))
     and panel_id in (4,5) and upper(prog.Channel_Name) = 'ANYTIME'
--subscriber_id = 17828608
order by Adjusted_Event_Start_Time ,X_Adjusted_Event_End_Time
;

commit;

--Select * from sk_prod.VESPA_EPG_DIM where upper(Channel_Name) = 'ANYTIME' and tx_date = '20120710' order by tx_start_datetime_utc
*/