CREATE VARIABLE @var_prog_period_start datetime;
CREATE VARIABLE @var_date_counter      datetime;
CREATE VARIABLE @var_prog_period_end   datetime;
CREATE VARIABLE @dt                    char(8);
CREATE VARIABLE @var_sql               varchar(15000);

SET @var_prog_period_start  = '2012-03-20';
SET @var_prog_period_end    = '2012-04-30';

-- To store all the viewing records:
create table V055_viewing_records(
             cb_row_ID                             bigint       not null primary key
            ,Account_Number                        varchar(20)  not null
            ,Subscriber_Id                         decimal(8,0) not null
            ,Cb_Key_Household                      bigint
            ,Cb_Key_Family                         bigint
            ,Cb_Key_Individual                     bigint
            ,Event_Type                            varchar(20)  not null
            ,X_Type_Of_Viewing_Event               varchar(40)  not null
            ,Adjusted_Event_Start_Time             datetime
            ,X_Adjusted_Event_End_Time             datetime
            ,X_Viewing_Start_Time                  datetime
            ,X_Viewing_End_Time                    datetime
            ,Tx_Start_Datetime_UTC                 datetime
            ,Tx_End_Datetime_UTC                   datetime
            ,Recorded_Time_UTC                     datetime
            ,Play_Back_Speed                       decimal(4,0)
            ,X_Event_Duration                      decimal(10,0)
            ,X_Programme_Duration                  decimal(10,0)
            ,X_Programme_Viewed_Duration           decimal(10,0)
            ,X_Programme_Percentage_Viewed         decimal(3,0)
            ,X_Viewing_Time_Of_Day                 varchar(15)
            ,Programme_Trans_Sk                    bigint       not null
            ,Channel_Name                          varchar(30)
            ,service_key                           int
            ,Epg_Title                             varchar(50)
            ,Genre_Description                     varchar(30)
            ,Sub_Genre_Description                 varchar(30)
            ,x_cumul_programme_viewed_duration     bigint
            ,live                                  bit         default 0
            ,channel_name_inc_hd                   varchar(40)
            ,capped_x_programme_viewed_duration    int
            ,capped_flag                           tinyint     default 0
            ,viewing_record_start_time_utc         datetime
            ,viewing_record_start_time_local       datetime
            ,viewing_record_end_time_utc           datetime
            ,viewing_record_end_time_local         datetime
            ,viewing_category                      varchar(20)
            ,HD_channel                            bit         default 0
            ,Pay_Channel                           bit         default 0
            ,capped_x_viewing_start_time           datetime
            ,capped_x_viewing_end_time             datetime
            ,epl_sunday                            bit         default 0
);

select distinct(account_number) into V055_epl_base_accounts from epl_base;

-- Build string with placeholder for changing daily table reference
SET @var_sql = '
    insert into V055_viewing_records
    select vw.cb_row_ID,vw.Account_Number,vw.Subscriber_Id,vw.Cb_Key_Household,vw.Cb_Key_Family
          ,vw.Cb_Key_Individual,vw.Event_Type,vw.X_Type_Of_Viewing_Event,vw.Adjusted_Event_Start_Time
          ,vw.X_Adjusted_Event_End_Time,vw.X_Viewing_Start_Time,vw.X_Viewing_End_Time
          ,prog.Tx_Start_Datetime_UTC,prog.Tx_End_Datetime_UTC,vw.Recorded_Time_UTC,vw.Play_Back_Speed
          ,vw.X_Event_Duration,vw.X_Programme_Duration,vw.X_Programme_Viewed_Duration
          ,vw.X_Programme_Percentage_Viewed,vw.X_Viewing_Time_Of_Day,vw.Programme_Trans_Sk
          ,prog.channel_name,prog.service_key
          ,prog.Epg_Title,prog.Genre_Description,prog.Sub_Genre_Description
          ,sum(x_programme_viewed_duration) over (partition by subscriber_id, adjusted_event_start_time order by cb_row_id) as x_cumul_programme_viewed_duration
          ,0,'''',0,0,'''','''','''','''','''',0,0,'''','''',0
     from sk_prod.VESPA_STB_PROG_EVENTS_##^^*^*##      as vw
          left  join sk_prod.VESPA_EPG_DIM             as prog on vw.programme_trans_sk = prog.programme_trans_sk
          inner join v055_epl_base_accounts                 as acc on vw.account_number = acc.account_number
        -- Filter for viewing events during extraction
    where video_playing_flag = 1
      and adjusted_event_start_time <> x_adjusted_event_end_time
      and (    x_type_of_viewing_event in (''TV Channel Viewing'',''Sky+ time-shifted viewing event'',''HD Viewing Event'')
           or (    x_type_of_viewing_event = (''Other Service Viewing Event'')
               and x_si_service_type = ''High Definition TV test service''))
      and panel_id in (4,5)'
;

   update V055_viewing_records
      set live = case when play_back_speed is null then 1 else 0 end
;


--we only need live
delete from V055_viewing_records where live=0;

    set @var_date_counter = @var_prog_period_start;

  while @var_date_counter <= @var_prog_period_end
  begin
      set @dt = left(@var_date_counter,4) || substr(@var_date_counter,6,2) || substr(@var_date_counter,9,2)
      EXECUTE(replace(@var_sql,'##^^*^*##',@dt))
      commit
      set @var_date_counter = dateadd(day, 1, @var_date_counter)
  end;

  update V055_viewing_records
     set viewing_category = case when lower(Epg_Title) in ('football first - game of the day'
                                                   ,'ford football special'
                                                   ,'ford football-aston villa v bolton'
                                                   ,'ford monday night football'
                                                   ,'live ford football special'
                                                   ,'live ford monday night football'
                                                   ,'live ford super sunday'
                                                   ,'live: barclays premier league...'
                                                   ,'live: barclays premier league:...'
                                                   ,'live: norwich v liverpool -...'
                                                   ,'live: qpr v tottenham -...'
                                                   ,'live: stoke v manchester city -...'
                                                   ,'live: stoke v manchester city -...'
                                                   ,'live: stoke v wolves - barclays...'
                                                   ,'norwich v liverpool - barclays...'
                                                   ,'qpr v tottenham - barclays...'
                                                   ,'stoke v manchester city -...'
                                                   ,'stoke v wolves - barclays...')     then 'EPL Matches'
                                when epg_title in ('Football First - Match Choice'
                                                    ,'Gillette Soccer Special'
                                                    ,'Gillette Soccer Saturday'
                                                    ,'Match of the Day'
                                                   ,'Barclays Premier League Review'
                                                   ,'ESPN Kicks - Barclays Premier...') then 'EPL Highlights'
                                when upper(Sub_Genre_Description) = 'FOOTBALL'          then 'Other Football'
                                when upper(Genre_Description) = 'SPORTS'                then 'Other Sports'
                                when channel_name like '%Sky Atlantic%'                 then 'Sky Atlantic'
                                else 'Other' end
;

  update V055_viewing_records
     set hd_channel = 1
   where channel_name like '%HD%'
; --

  update V055_viewing_records as bas
     set Pay_channel = 1
    from vespa_analysts.channel_name_and_techedge_channel as det
   where det.Channel_Name_Inc_Hd in
          ('Animal Planet','Animal Plnt+1','Attheraces','BET','BET+1','Bio','Bio HD','Blighty','Bliss','Boomerang',
           'Boomerang +1','Bravo','Bravo Player','Bravo+1','Cartoon Network','Cartoonito','CBS Action','CBS Drama',
           'CBS Reality','CBS Reality +1','Challenge TV','Challenge TV+1','Channel AKA','Channel One','Channel One+1',
           'Chart Show TV','Clubland TV','CNTOO','Comedy Central','Comedy Central Extra','Comedy Central Extra+1',
           'ComedyCentral+1','Crime & Investigation','Crime & Investigation +1','Current TV','DanceNationTV','Dave ja vu',
           'Disc.Knowldge','Disc.knowledge +1','Disc.RealTime','Disc.RT+1','Disc.Sci+1','Disc.Science','Discovery',
           'Discovery Shed','Discovery Turbo','Discovery+1','Disney','Disney +1','Disney Cinemagic','Disney Cinemagic +1',
           'Disney Playhouse','Disney Playhouse+','Disney XD','Disney XD+1','Diva TV','Diva TV +1','DMAX','DMAX +1',
           'DMAX+2','E! Entertainment','Eden','Eden+1','ESPN','ESPN Classic','Euronews','Extreme Sports','Film24',
           'Flava','Food Network','Food Network+1','Fox FX','FX','FX+','Good Food','History','History +1','Home and Health',
           'Home&Health+','horror ch+1','horror channel','Horror Channel','Horse & Country TV','Kerrang','Kiss TV',
           'Kix!','Liverpool FCTV','LIVING','Living','LIVING +1','LIVING2','Living2+1','LIVINGit','LIVINGit+1','Magic',
           'MGM','Military History','Motors TV','Mov4Men2 +1','movies 24','Movies 24','Movies 24+1','Movies4Men +1',
           'Movies4Men.','Movies4Men2','MTV','MTV Base','MTV CLASSIC','MTV Dance','MTV Hits','MTV ROCKS','MTV SHOWS',
           'MTV+1','NatGeo + 1hr','NatGeo Wild','National Geo','Nick Jr','Nick Jr 2','Nick Toons','Nick Toonster',
           'Nickelodeon','Nickelodeon Replay','NME TV','N''Toons Replay','POP','Pop Girl','Pop Girl+1','Q Channel',
           'Quest','QUEST +1','Really','Scuzz NEW','Sky Arts 1','Sky Arts 2','Sky Movies Action & Adventure','Sky Movies Classics',
           'Sky Movies Comedy','Sky Movies Crime & Thriller','Sky Movies Drama & Romance','Sky Movies Family',
           'Sky Movies Indie','Sky Movies Modern','Sky Movies Modern Greats','Sky Movies Premiere','Sky Movies Premiere +1',
           'Sky Movies Sci-Fi / Horror','Sky Movies Sci-Fi/Horror','Sky Movies Showcase','Sky News','Sky News Enhanced',
           'Sky Sports 1','Sky Sports 2','Sky Sports 3','Sky Sports 4','Sky Sports News','Sky Spts News','Sky Thriller',
           'Sky Thriller HD','Sky1','Sky2','Sky3','Sky3+1','Smash Hits','Sunrise TV','Syfy','Syfy +1','TCM_UK','TCM2',
           'The Box','The Music Factory','The Vault','Tiny Pop','Tiny Pop +1','Travel','Travel & Living','Travel Ch +1',
           'True Ent','True Movies','True Movies 2','UKTV Alibi','UKTV Alibi+1','UKTV Dave','UKTV Dave+1','UKTV Food',
           'UKTV Food+1','UKTV G.O.L.D','UKTV G.O.L.D +1','UKTV GOLD','UKTV Style','UKTV Style Plus','Universal',
           'Universal +1','VH1','VH1 Classic','Vintage TV','Virgin 1','Virgin 1 +1','VIVA','Watch','Watch +1','Wedding TV',
           'wedding tv','Nat Geo','MTV Music','Sky Movies Mdn Greats','GOLD  (TV)','Sky DramaRom','Good Food +1',
           'Sky Living +1','Discovery +1hr','Premier Sports','Discovery RealTime +1','Nat Geo+1hr','Nick Replay',
           'Football First 4','Challenge','Football First 6','MUTV','Showcase','ESPN America','Chelsea TV','Alibi',
           'YeSTERDAY +1','Sky Movies Thriller','Eden +1','CineMoi Movies','Sky 1','Sky Living Loves','5* +1','Challenge +1',
           'Home+1','Home & Health +1','HD Retail Info','Home & Health','FX +','Disc. Shed','Discovery RealTime',
           'Sky Premiere','Sky Prem+1','Football First 7','Disney XD +1','Playhouse Disney','YeSTERDAY','Nat Geo Wild',
           'DMax','Home','HD MTV','Sky Movies Action','SBO','MGM HD','Animal Planet +1','Sky Box Office','TCM 2',
           'Sky Livingit +1','Dave','At The Races','History +1 hour','Sky 3D','horror channel +1','TCM','Anytime',
           'Comedy Central Extra +1','PopGirl+1','Smash Hits!','Nicktoons TV','Comedy Central +1','5*','Football First 2',
           'Alibi +1','MTV BASE','Sky Atlantic','Sky 2','MTV HITS','Disc. History','Disc. History+1','Sky Livingit',
           'Football First 3','Racing UK','DMax +2','MTV DANCE','Disc.Science +1','DMax +1','GOLD +1','Sky Living',
           'Ideal & More','CNToo')
        and bas.Channel_Name = det.Channel
; --5m

-- --skygo checks
-- select top 10 * from sk_prod.SKY_PLAYER_USAGE_DETAIL
-- select count(1) from sk_prod.SKY_PLAYER_USAGE_DETAIL
-- where video_title in ('Football First - Game of the Day'
--                                                    ,'Ford Football Special'
--                                                    ,'Ford Football-Aston Villa v Bolton'
--                                                    ,'Ford Monday Night Football'
--                                                    ,'Live Ford Football Special'
--                                                    ,'Live Ford Monday Night Football'
--                                                    ,'Live Ford Super Sunday'
--                                                    ,'Live: Norwich v Liverpool -...'
--                                                    ,'Live: QPR v Tottenham -...'
--                                                    ,'Live: Stoke v Manchester City -...'
--                                                    ,'LIVE: Stoke v Manchester City -...'
--                                                    ,'Live: Stoke v Wolves - Barclays...'
--                                                    ,'Norwich v Liverpool - Barclays...'
--                                                    ,'QPR v Tottenham - Barclays...'
--                                                    ,'Stoke v Manchester City -...'
--                                                    ,'Stoke v Wolves - Barclays...'
--                                                    ,'Football First - Match Choice'
--                                                    ,'Barclays Premier League Review'
--                                                    ,'ESPN Kicks - Barclays Premier...')
--

--Capping starts here (using Phase 1 10% limit)
  select event_start_day
        ,event_start_hour
        ,live
        ,min(dur_mins) as min_dur_mins
    into V055_max_caps
    from (select cast(Adjusted_Event_Start_Time as date) as event_start_day
                ,datepart(hour,Adjusted_Event_Start_Time) as event_start_hour
                ,live
                ,datediff(minute,Adjusted_Event_Start_Time,x_Adjusted_Event_end_Time) as dur_mins
                ,ntile(100) over (partition by event_start_day, event_start_hour, live
                                      order by dur_mins) as ntile_100
            from V055_viewing_records) as sub
   where ntile_100 = 91
group by event_start_day
        ,event_start_hour
        ,live
; --2m

---Create Capping rules limits
---Add on derived variables for viewing
  update V055_viewing_records as bas
     set channel_name_inc_hd = case when det.channel_name_inc_hd is not null then det.channel_name_inc_hd else bas.Channel_Name end
    from vespa_analysts.channel_name_and_techedge_channel as det
   where bas.Channel_Name = det.Channel
; --

commit;
create hg   index idx1 on V055_viewing_records(subscriber_id);
create dttm index idx2 on V055_viewing_records(adjusted_event_start_time);
create dttm index idx3 on V055_viewing_records(recorded_time_utc);
create dttm index idx5 on V055_viewing_records(x_viewing_start_time);
create dttm index idx6 on V055_viewing_records(x_viewing_end_time);
create hng  index idx7 on V055_viewing_records(x_cumul_programme_viewed_duration);
create hg   index idx8 on V055_viewing_records(programme_trans_sk);
create hg   index idx9 on V055_viewing_records(channel_name_inc_hd)
; --12m

-- -- update        the viewing                            start and end times for playback records
--   update V055_viewing_records
--      set x_viewing_end_time = dateadd(second,x_cumul_programme_viewed_duration,adjusted_event_start_time)
--    where recorded_time_utc is not null
-- ; --6m
-- commit;
-- 
--   update V055_viewing_records
--      set x_viewing_start_time = dateadd(second,-x_programme_viewed_duration,x_viewing_end_time)
--    where recorded_time_utc is not null
-- ; --6m
-- commit;

-- update table to create capped start and end times
  update V055_viewing_records as bas
     set capped_x_viewing_start_time = case
            -- if start of viewing_time is beyond start_time + cap then flag as null
            when dateadd(minute, min_dur_mins, adjusted_event_start_time) < x_viewing_start_time then null
            -- else leave start of viewing time unchanged
            else x_viewing_start_time
         end
        ,capped_x_viewing_end_time = case
            -- if start of viewing_time is beyond start_time + cap then flag as null
            when dateadd(minute, min_dur_mins, adjusted_event_start_time) < x_viewing_start_time then null
            -- if start_time+ cap is beyond end time then leave end time unchanged
            when dateadd(minute, min_dur_mins, adjusted_event_start_time) > x_viewing_end_time then x_viewing_end_time
            -- otherwise set end time to start_time + cap
            else dateadd(minute, min_dur_mins, adjusted_event_start_time)
         end
  from V055_max_caps as caps
 where date(bas.adjusted_event_start_time) = caps.event_start_day
                                           and datepart(hour, bas.adjusted_event_start_time) = caps.event_start_hour
; --1m
commit;


-- calculate capped_x_programme_viewed_duration
  update V055_viewing_records
     set capped_x_programme_viewed_duration = datediff(second, capped_x_viewing_start_time, capped_x_viewing_end_time)
; --1m

-- set capped_flag based on nature of capping
  update V055_viewing_records
     set capped_flag =
        case
            when capped_x_viewing_start_time is null then 2
            when capped_x_viewing_end_time < x_viewing_end_time then 1
            else 0
        end
; --1m

--This is 6 seconds
  update V055_viewing_records as bas
     set capped_x_viewing_start_time        = null
        ,capped_x_viewing_end_time          = null
        ,capped_x_programme_viewed_duration = null
        ,capped_flag                        = 3
    from vespa_201111_201112_min_cap
   where capped_x_programme_viewed_duration < cap_secs
; --0m

  update V055_viewing_records
     set capped_flag =
        case
            when capped_x_viewing_start_time is null then 2
            when capped_x_viewing_end_time < x_viewing_end_time then 1
            else 0
        end
; --1m
commit;

--QA:
select capped_flag,count(1) from v055_viewing_records group by capped_flag;

delete from V055_viewing_records where capped_flag in (2,3); --4m

---Add in Event start and end time and add in local time activity---
update V055_viewing_records
   set viewing_record_start_time_utc = case when recorded_time_utc         <  tx_start_datetime_utc then tx_start_datetime_utc
                                            when recorded_time_utc         >= tx_start_datetime_utc then recorded_time_utc
                                            when adjusted_event_start_time <  tx_start_datetime_utc then tx_start_datetime_utc
                                            when adjusted_event_start_time >= tx_start_datetime_utc then adjusted_event_start_time else null end
      ,viewing_record_end_time_utc   = dateadd(second, capped_x_programme_viewed_duration, viewing_record_start_time_utc)
;  --0m

update V055_viewing_records
   set viewing_record_start_time_local = case when dateformat(viewing_record_start_time_utc, 'YYYY-MM-DD-HH') between '2010-03-28-01' and '2010-10-31-02'
                                                or dateformat(viewing_record_start_time_utc, 'YYYY-MM-DD-HH') between '2011-03-27-01' and '2011-10-30-02'
                                                or dateformat(viewing_record_start_time_utc, 'YYYY-MM-DD-HH') between '2012-03-25-01' and '2012-10-28-02' then dateadd(hh,1,viewing_record_start_time_utc)
                                              else viewing_record_start_time_utc  end
      ,viewing_record_end_time_local   = case when dateformat(viewing_record_end_time_utc,   'YYYY-MM-DD-HH') between '2010-03-28-01' and '2010-10-31-02'
                                                or dateformat(viewing_record_end_time_utc,   'YYYY-MM-DD-HH') between '2011-03-27-01' and '2011-10-30-02'
                                                or dateformat(viewing_record_end_time_utc,   'YYYY-MM-DD-HH') between '2012-03-25-01' and '2012-10-28-02' then dateadd(hh,1,viewing_record_end_time_utc)
                                              else viewing_record_end_time_utc  end
; --1m

--add scaling variables (from wiki)
   alter table V055_viewing_records
     add (weighting_date        date
         ,scaling_segment_ID    int
         ,weightings            float default 0
);

commit;
create index for_weightings on V055_viewing_records(account_number);

  update V055_viewing_records
     set weighting_date = cast(viewing_record_start_time_local as date)
; --0m

-- First, get the segmentation for the account at the time of viewing
  update V055_viewing_records as bas
     set bas.scaling_segment_ID = wei.scaling_segment_ID
    from vespa_analysts.scaling_dialback_intervals as wei
   where bas.account_number = wei.account_number
     and bas.weighting_date between wei.reporting_starts and wei.reporting_ends
; --1m

commit;

-- Find out the weight for that segment on that day
  update V055_viewing_records as bas
     set bas.weightings = wei.weighting
    from vespa_analysts.scaling_weightings as wei
   where bas.weighting_date     = wei.scaling_day
     and bas.scaling_segment_ID = wei.scaling_segment_ID
;
commit;

---queires to create output including number of possible viewings
  update V055_viewing_records
     set viewing_category = case when lower(Epg_Title) in ('football first - game of the day'
                                                   ,'ford football special'
                                                   ,'ford football-aston villa v bolton'
                                                   ,'ford monday night football'
                                                   ,'live ford football special'
                                                   ,'live ford monday night football'
                                                   ,'live ford super sunday'
                                                   ,'live: barclays premier league...'
                                                   ,'live: barclays premier league:...'
                                                   ,'live: norwich v liverpool -...'
                                                   ,'live: qpr v tottenham -...'
                                                   ,'live: stoke v manchester city -...'
                                                   ,'live: stoke v manchester city -...'
                                                   ,'live: stoke v wolves - barclays...'
                                                   ,'norwich v liverpool - barclays...'
                                                   ,'qpr v tottenham - barclays...'
                                                   ,'stoke v manchester city -...'
                                                   ,'stoke v wolves - barclays...')     then 'EPL Matches'
                                when lower(epg_title) in ('football first - match choice'
                                                   ,'gillette soccer special'
                                                   ,'gillette soccer saturday'
                                                   ,'match of the day'
                                                   ,'barclays premier league review'
                                                   ,'espn kicks - barclays premier...') then 'EPL Highlights'
                                when upper(Sub_Genre_Description) = 'FOOTBALL'          then 'Other Football'
                                when upper(Sub_Genre_Description) = 'CRICKET'           then 'Cricket'
                                when upper(Sub_Genre_Description) = 'RUGBY'             then 'Rugby'
                                when upper(Sub_Genre_Description) = 'GOLF'              then 'Golf'
                                when upper(Sub_Genre_Description) = 'MOTOR SPORT'       then 'Motor Sport'
                                when upper(Genre_Description) = 'SPORTS'                then 'Other Sports'
                                when channel_name like '%Sky Atlantic%'                 then 'Sky Atlantic'
                                when channel_name in ('Sky Action'
                                                     ,'Sky Action HD'
                                                     ,'Sky Classics'
                                                     ,'Sky Classics HD'
                                                     ,'Sky Comedy'
                                                     ,'Sky Comedy HD'
                                                     ,'Sky DramaRom'
                                                     ,'Sky DraRomHD'
                                                     ,'Sky Family'
                                                     ,'Sky Family HD'
                                                     ,'Sky Indie'
                                                     ,'Sky Indie HD'
                                                     ,'Sky Mdn Greats'
                                                     ,'Sky MdnGrtsHD'
                                                     ,'Sky Prem+1'
                                                     ,'Sky Premiere'
                                                     ,'Sky ScFi/HorHD'
                                                     ,'Sky ScFi/Horror'
                                                     ,'Sky Showcase'
                                                     ,'Sky Thriller'
                                                     ,'Sky Thriller HD'
                                                     ,'SkyPremiereHD'
                                                     ,'SkyShowcseHD'
                                                     ) then 'Sky Movies'
when hd_channel = 1 then 'Other HD'
when pay_channel = 1 then 'Other Pay TV'
                                else 'Other Free TV' end
;

  update V055_viewing_records
     set epl_sunday = 1
   where weighting_date in ('2012-03-25', '2012-04-01', '2012-04-08', '2012-04-15', '2012-04-22', '2012-04-29')
     and viewing_category ='EPL Matches'
;

  select account_number
        ,Tx_Start_Datetime_UTC
        ,cast(Tx_Start_Datetime_UTC as date) as dt
        ,viewing_category
        ,lower(epg_title) as title
        ,epl_sunday
        ,max(weightings) as weightings
        ,pay_channel
    into v055_temp
    from V055_viewing_records
   where x_programme_percentage_viewed >= 20
group by account_number
        ,Tx_Start_Datetime_UTC
        ,dt
        ,viewing_category
        ,title
        ,epl_sunday
        ,pay_channel
;

  select max(programme_trans_sk) as ky
        ,case when lower(Epg_Title) in ('football first - game of the day'
                                                   ,'ford football special'
                                                   ,'ford football-aston villa v bolton'
                                                   ,'ford monday night football'
                                                   ,'live ford football special'
                                                   ,'live ford monday night football'
                                                   ,'live ford super sunday'
                                                   ,'live: barclays premier league...'
                                                   ,'live: barclays premier league:...'
                                                   ,'live: norwich v liverpool -...'
                                                   ,'live: qpr v tottenham -...'
                                                   ,'live: stoke v manchester city -...'
                                                   ,'live: stoke v manchester city -...'
                                                   ,'live: stoke v wolves - barclays...'
                                                   ,'norwich v liverpool - barclays...'
                                                   ,'qpr v tottenham - barclays...'
                                                   ,'stoke v manchester city -...'
                                                   ,'stoke v wolves - barclays...')     then 'EPL Matches'
                                when lower(epg_title) in ('football first - match choice'
                                                   ,'gillette soccer special'
                                                   ,'gillette soccer saturday'
                                                   ,'match of the day'
                                                   ,'barclays premier league review'
                                                   ,'espn kicks - barclays premier...') then 'EPL Highlights'
                                when upper(Sub_Genre_Description) = 'FOOTBALL'          then 'Other Football'
                                when upper(Sub_Genre_Description) = 'CRICKET'           then 'Cricket'
                                when upper(Sub_Genre_Description) = 'RUGBY'             then 'Rugby'
                                when upper(Sub_Genre_Description) = 'GOLF'              then 'Golf'
                                when upper(Sub_Genre_Description) = 'MOTOR SPORT'       then 'Motor Sport'
                                when upper(Genre_Description) = 'SPORTS'                then 'Other Sports'
                                when channel_name like '%Sky Atlantic%'                 then 'Sky Atlantic'
                                when channel_name in ('Sky Action'
                                                     ,'Sky Action HD'
                                                     ,'Sky Classics'
                                                     ,'Sky Classics HD'
                                                     ,'Sky Comedy'
                                                     ,'Sky Comedy HD'
                                                     ,'Sky DramaRom'
                                                     ,'Sky DraRomHD'
                                                     ,'Sky Family'
                                                     ,'Sky Family HD'
                                                     ,'Sky Indie'
                                                     ,'Sky Indie HD'
                                                     ,'Sky Mdn Greats'
                                                     ,'Sky MdnGrtsHD'
                                                     ,'Sky Prem+1'
                                                     ,'Sky Premiere'
                                                     ,'Sky ScFi/HorHD'
                                                     ,'Sky ScFi/Horror'
                                                     ,'Sky Showcase'
                                                     ,'Sky Thriller'
                                                     ,'Sky Thriller HD'
                                                     ,'SkyPremiereHD'
                                                     ,'SkyShowcseHD')                 then 'Sky Movies'
when channel_name like '%HD%' then 'Other HD'
   when det.Channel_Name_Inc_Hd in
          ('Animal Planet','Animal Plnt+1','Attheraces','BET','BET+1','Bio','Bio HD','Blighty','Bliss','Boomerang',
           'Boomerang +1','Bravo','Bravo Player','Bravo+1','Cartoon Network','Cartoonito','CBS Action','CBS Drama',
           'CBS Reality','CBS Reality +1','Challenge TV','Challenge TV+1','Channel AKA','Channel One','Channel One+1',
           'Chart Show TV','Clubland TV','CNTOO','Comedy Central','Comedy Central Extra','Comedy Central Extra+1',
           'ComedyCentral+1','Crime & Investigation','Crime & Investigation +1','Current TV','DanceNationTV','Dave ja vu',
           'Disc.Knowldge','Disc.knowledge +1','Disc.RealTime','Disc.RT+1','Disc.Sci+1','Disc.Science','Discovery',
           'Discovery Shed','Discovery Turbo','Discovery+1','Disney','Disney +1','Disney Cinemagic','Disney Cinemagic +1',
           'Disney Playhouse','Disney Playhouse+','Disney XD','Disney XD+1','Diva TV','Diva TV +1','DMAX','DMAX +1',
           'DMAX+2','E! Entertainment','Eden','Eden+1','ESPN','ESPN Classic','Euronews','Extreme Sports','Film24',
           'Flava','Food Network','Food Network+1','Fox FX','FX','FX+','Good Food','History','History +1','Home and Health',
           'Home&Health+','horror ch+1','horror channel','Horror Channel','Horse & Country TV','Kerrang','Kiss TV',
           'Kix!','Liverpool FCTV','LIVING','Living','LIVING +1','LIVING2','Living2+1','LIVINGit','LIVINGit+1','Magic',
           'MGM','Military History','Motors TV','Mov4Men2 +1','movies 24','Movies 24','Movies 24+1','Movies4Men +1',
           'Movies4Men.','Movies4Men2','MTV','MTV Base','MTV CLASSIC','MTV Dance','MTV Hits','MTV ROCKS','MTV SHOWS',
           'MTV+1','NatGeo + 1hr','NatGeo Wild','National Geo','Nick Jr','Nick Jr 2','Nick Toons','Nick Toonster',
           'Nickelodeon','Nickelodeon Replay','NME TV','N''Toons Replay','POP','Pop Girl','Pop Girl+1','Q Channel',
           'Quest','QUEST +1','Really','Scuzz NEW','Sky Arts 1','Sky Arts 2','Sky Movies Action & Adventure','Sky Movies Classics',
           'Sky Movies Comedy','Sky Movies Crime & Thriller','Sky Movies Drama & Romance','Sky Movies Family',
           'Sky Movies Indie','Sky Movies Modern','Sky Movies Modern Greats','Sky Movies Premiere','Sky Movies Premiere +1',
           'Sky Movies Sci-Fi / Horror','Sky Movies Sci-Fi/Horror','Sky Movies Showcase','Sky News','Sky News Enhanced',
           'Sky Sports 1','Sky Sports 2','Sky Sports 3','Sky Sports 4','Sky Sports News','Sky Spts News','Sky Thriller',
           'Sky Thriller HD','Sky1','Sky2','Sky3','Sky3+1','Smash Hits','Sunrise TV','Syfy','Syfy +1','TCM_UK','TCM2',
           'The Box','The Music Factory','The Vault','Tiny Pop','Tiny Pop +1','Travel','Travel & Living','Travel Ch +1',
           'True Ent','True Movies','True Movies 2','UKTV Alibi','UKTV Alibi+1','UKTV Dave','UKTV Dave+1','UKTV Food',
           'UKTV Food+1','UKTV G.O.L.D','UKTV G.O.L.D +1','UKTV GOLD','UKTV Style','UKTV Style Plus','Universal',
           'Universal +1','VH1','VH1 Classic','Vintage TV','Virgin 1','Virgin 1 +1','VIVA','Watch','Watch +1','Wedding TV',
           'wedding tv','Nat Geo','MTV Music','Sky Movies Mdn Greats','GOLD  (TV)','Sky DramaRom','Good Food +1',
           'Sky Living +1','Discovery +1hr','Premier Sports','Discovery RealTime +1','Nat Geo+1hr','Nick Replay',
           'Football First 4','Challenge','Football First 6','MUTV','Showcase','ESPN America','Chelsea TV','Alibi',
           'YeSTERDAY +1','Sky Movies Thriller','Eden +1','CineMoi Movies','Sky 1','Sky Living Loves','5* +1','Challenge +1',
           'Home+1','Home & Health +1','HD Retail Info','Home & Health','FX +','Disc. Shed','Discovery RealTime',
           'Sky Premiere','Sky Prem+1','Football First 7','Disney XD +1','Playhouse Disney','YeSTERDAY','Nat Geo Wild',
           'DMax','Home','HD MTV','Sky Movies Action','SBO','MGM HD','Animal Planet +1','Sky Box Office','TCM 2',
           'Sky Livingit +1','Dave','At The Races','History +1 hour','Sky 3D','horror channel +1','TCM','Anytime',
           'Comedy Central Extra +1','PopGirl+1','Smash Hits!','Nicktoons TV','Comedy Central +1','5*','Football First 2',
           'Alibi +1','MTV BASE','Sky Atlantic','Sky 2','MTV HITS','Disc. History','Disc. History+1','Sky Livingit',
           'Football First 3','Racing UK','DMax +2','MTV DANCE','Disc.Science +1','DMax +1','GOLD +1','Sky Living',
           'Ideal & More','CNToo') then 'Other Pay TV'
                                else 'Other Free TV' end as viewing_category
        ,tx_date_utc as dt
        ,lower(epg_title) as title
        ,tx_start_datetime_utc as tm
    into #temp
    from sk_prod.vespa_epg_dim as bas
         left join vespa_analysts.channel_name_and_techedge_channel as det on bas.Channel_Name = det.Channel
   where tx_date between '20120320' and '20120430'
group by viewing_category
        ,dt
        ,title
        ,tm
;

  select count(distinct ky) as cow
        ,dt
        ,viewing_category
    into v055_progs
    from #temp
group by dt
        ,viewing_category
;

  select dt
        ,account_number
        ,max(weightings) as weightings
        ,sum(case when viewing_category = 'EPL Matches'    then 1 else 0 end) as epl_matches
        ,sum(case when viewing_category = 'EPL Highlights' then 1 else 0 end) as epl_highlights
        ,sum(case when viewing_category = 'Other Football' then 1 else 0 end) as other_football
        ,sum(case when viewing_category = 'Cricket'        then 1 else 0 end) as cricket
        ,sum(case when viewing_category = 'Rugby'          then 1 else 0 end) as rugby
        ,sum(case when viewing_category = 'Golf'           then 1 else 0 end) as golf
        ,sum(case when viewing_category = 'Motor Sport'    then 1 else 0 end) as motorsport
        ,sum(case when viewing_category = 'Other Sports'   then 1 else 0 end) as other_sports
        ,sum(case when viewing_category = 'Sky Atlantic'   then 1 else 0 end) as sky_atlantic
        ,sum(case when viewing_category = 'Sky Movies'     then 1 else 0 end) as sky_movies
        ,sum(case when viewing_category = 'Other HD'       then 1 else 0 end) as other_hd
        ,sum(case when viewing_category = 'Other Pay TV'   then 1 else 0 end) as other_pay_tv
        ,sum(case when viewing_category = 'Other Free TV'  then 1 else 0 end) as other_free_tv
        ,sum(epl_sunday)                                                      as epl_sunday
        ,sum(pay_tv)                                                          as all_pay_tv
        ,sum(1 - pay_tv)                                                      as all_free_tv
    into v055_summ
    from v055_temp
group by dt
        ,account_number
;

  select smm.dt,account_number,weightings,epl_matches,epl_highlights,other_football,cricket,rugby,golf,motorsport,other_sports,sky_atlantic,sky_movies,other_hd,other_pay_tv,other_free_tv,epl_sunday,all_pay_tv,all_free_tv
        ,sum(case when prg.viewing_category='EPL Matches'    then cow else 0 end) as total_epl_matches
        ,sum(case when prg.viewing_category='EPL Highlights' then cow else 0 end) as total_epl_highlights
        ,sum(case when prg.viewing_category='Other Football' then cow else 0 end) as total_other_football
        ,sum(case when prg.viewing_category='Cricket'        then cow else 0 end) as total_cricket
        ,sum(case when prg.viewing_category='Rugby'          then cow else 0 end) as total_rugby
        ,sum(case when prg.viewing_category='Golf'           then cow else 0 end) as total_golf
        ,sum(case when prg.viewing_category='Motor Sport'    then cow else 0 end) as total_motorsport
        ,sum(case when prg.viewing_category='Other Sports'   then cow else 0 end) as total_other_sports
        ,sum(case when prg.viewing_category='Sky Atlantic'   then cow else 0 end) as total_sky_atlantic
        ,sum(case when prg.viewing_category='Sky Movies'     then cow else 0 end) as total_sky_movies
        ,sum(case when prg.viewing_category='Other HD'       then cow else 0 end) as total_other_hd
        ,sum(case when prg.viewing_category='Other Pay TV'   then cow else 0 end) as total_other_pay_tv
        ,sum(case when prg.viewing_category='Other Free TV'  then cow else 0 end) as total_other_free_tv
        ,sum(epl_sunday)                                                          as total_epl_matches_sunday
    into v055_allinone
    from v055_summ as smm
         inner join v055_progs as prg on smm.dt = prg.dt
group by smm.dt,account_number,weightings,epl_matches,epl_highlights,other_football,cricket,rugby,golf,motorsport,other_sports,sky_atlantic,sky_movies,other_hd,other_pay_tv,other_free_tv,epl_sunday,all_pay_tv,all_free_tv
;

commit;

-----------------------------------------------------------------------------------------------------------2. PROFILING

----------------------------------------------------------------------------------2a Create the profiling variables from OLIVE
--These are created in table EPL_Base and can be matched to viewing data accounts

CREATE variable @snapshot_start_dt DATE;
CREATE variable @snapshot_end_dt DATE ;

SET @snapshot_start_dt = '2012-03-20' ;
SET @snapshot_end_dt = '2012-04-30' ;


---------------------------------------------------------------------------------------------------------------------------------------------------------
---- Create Table
---------------------------------------------------------------------------------------------------------------------------------------------------------

IF object_ID ('EPL_Base') IS NOT NULL THEN
            DROP TABLE EPL_Base
END IF;

CREATE TABLE EPL_Base

        (account_number                         VARCHAR(12)
        ,cb_key_household                       BIGINT
        ,effective_from_dt                      DATE
        ,effective_to_dt                        DATE
        ,value_segment                          VARCHAR(20)
        ,products                               VARCHAR(20)
        ,past_churn                             tinyint
        ,past_downgrade                         tinyint
        ,sports_tenure                          int
        ,SkyGo_EPL                              tinyint
        ,Atlantic                               tinyint
        ,PayTV                                  tinyint
        ,Football_Type                          varchar(20) --EPL / Highlights / Other
        ,Cable                                  tinyint
        ,HH_composition                         varchar(60)
        ,Region                                 varchar(30)
        ,Affluence                              varchar(10)
        ,EoS_Churn_Return                       tinyint
        ,sports_package                         tinyint
)

;

COMMIT;

CREATE HG INDEX idx_account_number_hg ON EPL_Base(account_number);
create hg index idx_hh  on EPL_Base(cb_key_household);


GRANT SELECT ON EPL_Base TO PUBLIC;

--drop table EPL_Base


------------------------------------------------------------------------------------------------------------------------------------------------------------
---- Populate with active customers in the last 6 weeks with a sports subscription
------------------------------------------------------------------------------------------------------------------------------------------------------------

SELECT   csh.account_number
        ,csh.cb_key_household
        ,csh.effective_from_dt
        ,csh.effective_to_dt
        ,MAX(cel.prem_sports) as sports_package
        ,rank() over(partition by csh.account_number  order by csh.effective_from_dt desc) as drank
INTO #accounts
FROM sk_prod.cust_subs_hist AS csh INNER JOIN sk_prod.cust_entitlement_lookup AS cel
                ON csh.current_short_description = cel.short_description
WHERE subscription_sub_type = 'DTV Primary Viewing'
      and status_code IN ('AC','AB','PC')
      and (( effective_from_dt  <= @snapshot_start_dt and effective_to_dt > @snapshot_start_dt)
      and effective_to_dt > @snapshot_end_dt)
      AND cel.prem_sports > 0
GROUP BY csh.account_number
        ,csh.cb_key_household
        ,csh.effective_from_dt
        ,csh.effective_to_dt

;
--4756436 Row(s) affected




--dedupe
delete
  from #accounts
 where drank > 1
;
--304 Row(s) affected


INSERT INTO EPL_Base (account_number,cb_key_household,effective_from_dt,effective_to_dt,sports_package)

SELECT   account_number
        ,cb_key_household
        ,effective_from_dt
        ,effective_to_dt
        ,sports_package

FROM #accounts
COMMIT;


--4756132 Row(s) affectedd


------------------------------------------------------------------------------------------------------------------------------------------------------------
---- Populate with profiling variables
------------------------------------------------------------------------------------------------------------------------------------------------------------

---Value Segment

UPDATE EPL_Base
SET value_segment = tgt.value_seg
FROM EPL_Base AS base
       INNER JOIN sk_prod.VALUE_SEGMENTS_DATA AS tgt ON base.account_number = tgt.account_number
;
--4712016 Row(s) affected

--- CABLE

ALTER TABLE EPL_Base
ADD postcode_no_space VARCHAR(10);

UPDATE EPL_Base
      SET  postcode_no_space = REPLACE(sav.cb_address_postcode,' ','')
     FROM EPL_Base AS base
          INNER JOIN sk_prod.cust_single_account_view AS sav ON base.account_number = sav.account_number
;
--4756087 Row(s) affected



   UPDATE EPL_Base
      SET cable = 1
     FROM EPL_Base AS base
          INNER JOIN sk_prod.broadband_postcode_exchange as bb
                ON base.postcode_no_space = replace(bb.cb_address_postcode,' ','')
                    AND UPPER(bb.cable_postcode) = 'Y'
;
--1658485 Row(s) affected

--select count(account_number) from EPL_Base where cable = 1;


--- Box Type & Attachments



-- Attachments

SELECT TOP 10 * FROM EPL_BASE;

ALTER TABLE EPL_Base
ADD (wlr tinyint)
;

UPDATE EPL_Base
   SET HD        = tgt.hdtv
      ,MR        = tgt.multiroom
      ,SP        = tgt.skyplus
      ,BB        = tgt.broadband
      ,talk      = tgt.skytalk
      ,WLR       = tgt.wlr
      ,movies    = tgt.movies
 FROM EPL_Base AS base
      INNER JOIN (
                    SELECT  csh.account_number
                           ,MAX(CASE  WHEN csh.subscription_sub_type ='DTV Sky+'
                                       AND csh.status_code in  ('AC','AB','PC') THEN 1 ELSE 0  END) AS skyplus
                           ,MAX(CASE  WHEN csh.subscription_sub_type ='DTV Extra Subscription'
                                       AND csh.status_code in  ('AC','AB','PC') THEN 1 ELSE 0  END) AS multiroom
                           ,MAX(CASE  WHEN csh.subscription_sub_type ='DTV HD'
                                       AND csh.status_code in  ('AC','AB','PC') THEN 1 ELSE 0 END)  AS hdtv
                           ,MAX(CASE  WHEN csh.subscription_sub_type ='Broadband DSL Line'
                                       AND (       status_code in ('AC','AB')
                                               OR (status_code='PC' AND prev_status_code not in ('?','RQ','AP','UB','BE','PA') )
                                               OR (status_code='CF' AND prev_status_code='PC'                                  )
                                               OR (status_code='AP' AND sale_type='SNS Bulk Migration'                         )
                                            )                                    THEN 1 ELSE 0 END)  AS broadband
                           ,MAX(CASE  WHEN csh.subscription_sub_type = 'SKY TALK SELECT'
                                       AND (     csh.status_code = 'A'
                                             OR (csh.status_code = 'FBP' AND prev_status_code in ('PC','A'))
                                             OR (csh.status_code = 'RI'  AND prev_status_code in ('FBP','A'))
                                             OR (csh.status_code = 'PC'  AND prev_status_code = 'A')
                                            )                                  THEN 1 ELSE 0 END)   AS skytalk
                           ,MAX(CASE  WHEN csh.subscription_sub_type ='SKY TALK LINE RENTAL'
                                       AND csh.status_code in ('A','CRQ','R')  THEN 1 ELSE 0 END) AS wlr
                           ,MAX(cel.prem_movies)      AS movies
                      FROM sk_prod.cust_subs_hist AS csh
                           INNER JOIN EPL_base AS base ON csh.account_number = base.account_number
                           LEFT OUTER JOIN sk_prod.cust_entitlement_lookup cel on csh.current_short_description = cel.short_description
                     WHERE csh.effective_from_dt <= @snapshot_end_dt
                       AND csh.effective_to_dt    > @snapshot_end_dt
                       AND csh.subscription_sub_type  IN ( 'DTV Primary Viewing'
                                                          ,'DTV Sky+'
                                                          ,'DTV Extra Subscription'
                                                          ,'DTV HD'
                                                          ,'Broadband DSL Line'
                                                          ,'SKY TALK SELECT'
                                                          ,'SKY TALK LINE RENTAL'  )  --< Optimises the code, limit to what is needed
                       AND csh.effective_from_dt <> csh.effective_to_dt
                  GROUP BY csh.account_number
        )AS tgt ON base.account_number = tgt.account_number;

COMMIT;

--4780437 Row(s) affected
--4756125 Row(s) affected


-- HD Box

ALTER TABLE EPL_Base
ADD HD_box tinyint;

UPDATE EPL_Base
   SET HD_box = 1
  FROM EPL_Base AS base
       INNER JOIN (
                    SELECT DISTINCT stb.account_number
                      FROM sk_prod.CUST_SET_TOP_BOX AS stb
                           INNER JOIN EPL_base AS acc on stb.account_number = acc.account_number
                     WHERE box_installed_dt   <= @snapshot_end_dt
                       AND box_replaced_dt    >  @snapshot_end_dt
                       AND current_product_description like '%HD%'
       ) AS tgt ON base.account_number = tgt.account_number;

COMMIT;

--3118389 Row(s) affected
--3104289 Row(s) affected

-- Derive Box Type



ALTER TABLE EPL_Base
ADD box_type varchar(15) ;

UPDATE EPL_Base
   SET box_type = CASE WHEN HD =1 AND MR = 1            THEN 'HD_Combi'
                       WHEN HD =1                       THEN 'HD'
                       WHEN HD_box =1 AND MR = 1        THEN 'HDx_Combi'
                       WHEN HD_box =1                   THEN 'HDx'
                       WHEN SP =1 AND MR = 1            THEN 'SkyPlus_Combi'
                       WHEN SP =1                       THEN 'SkyPlus'
                       WHEN MR =1                       THEN 'Multiroom'
                       ELSE                                  'FDB'
                    END;
--4780444 Row(s) affected
--4756132 Row(s) affected

--====
-- QA
--====

--   SELECT box_type,SUM(HD) AS hd,SUM(HD_box) AS hdBox,SUM(MR) AS mr,SUM(SP)AS sp,SUM(BB) AS bb, sum(talk) as talk, SUM(WLR) AS wlr
--         ,SUM(CASE WHEN movies >0 THEN 1 ELSE 0 END) as movies
--         ,SUM(CASE WHEN sports >0 THEN 1 ELSE 0 END) as sports
--     FROM EPL_Base
-- GROUP BY box_type


---Affluence

select          sav.account_number,
                max(CASE WHEN ilu_affluence in ('01','02','03','04')    THEN 'Very Low'
                     WHEN ilu_affluence in ('05','06')                  THEN 'Low'
                     WHEN ilu_affluence in ('07','08')                  THEN 'Mid Low'
                     WHEN ilu_affluence in ('09','10')                  THEN 'Mid'
                     WHEN ilu_affluence in ('11','12')                  THEN 'Mid High'
                     WHEN ilu_affluence in ('13','14','15')             THEN 'High'
                     WHEN ilu_affluence in ('16','17')                  THEN 'Very High'
                ELSE                                                         'Unknown'
                END) as sav_affluence,
                max(cb_key_household) as cb_key_household
into            #sav_dem                                                --drop table #sav_dem
from            sk_prod.CUST_SINGLE_ACCOUNT_VIEW as sav
where           sav.account_number in (select distinct account_number from EPL_BASE)
group by        sav.account_number;
commit;
--4780093 Row(s) affected
--4756087 Row(s) affected

create hg index indx1 on #sav_dem(account_number);

update          EPL_BASE base
set             base.affluence = sav.sav_affluence
from            #sav_dem sav
where           base.account_number = sav.account_number;
commit;
--4780398 Row(s) affected
--4756087 Row(s) affected

--Region

update          EPL_BASE base
set             Region = CASE WHEN sav.isba_tv_region = 'Not Defined'
                                       THEN 'UNKNOWN'
                                       ELSE sav.isba_tv_region
                                   END
FROM            EPL_Base base INNER JOIN sk_prod.cust_single_account_view AS sav
                        ON base.account_number = sav.account_number;

--4780398 Row(s) affected
--4756087 Row(s) affected

select count(account_number), value_segment, eos_churn_return from EPL_BAse
group by value_segment, eos_churn_return;

select count(*),value_segment from epl_base
group by value_segment

------------------------------------------------------------------------End of Season Churn and Returners (Identified from end of 2010/11 season)

/*EoS time period is defined following the def used by Trading Analytics Team to ensure consistency*/

--Downgrade events in EoS 2010/11 ('2011-04-01' to '2011-06-30')


--Make a working table
CREATE TABLE  #prem_movement (
         account_number         varchar(20)     not null
        ,premiums_at_start      integer         null
        ,premiums_at_end        integer         null
        ,next_movement_date     date            null
        ,next_movement_premiums integer         null
        );

--Populate with Sports accounts
SELECT   csh.account_number
        ,CASE WHEN cel.prem_sports > 0 THEN 1 ELSE 0 END AS premiums
        ,RANK() OVER (PARTITION BY csh.account_number ORDER BY effective_from_dt, csh.cb_row_id DESC) AS 'RANK'
  INTO  #start_accounts
  FROM  sk_prod.cust_subs_hist AS csh
        inner join sk_prod.cust_entitlement_lookup AS cel
                   ON csh.current_short_description = cel.short_description
 WHERE  subscription_sub_type = 'DTV Primary Viewing'
   AND  csh.status_code IN ('AC','AB','PC')
   AND  effective_from_dt <= '2011-03-31' AND effective_to_dt > '2011-03-31'
   AND  cel.prem_sports > 0          --select those with sports prems before start of 'EoS'

;


--5215764 Row(s) affected

INSERT INTO #prem_movement  (account_number,premiums_at_start)
SELECT account_number,premiums
  FROM #start_accounts
 WHERE rank = 1;

CREATE UNIQUE hg INDEX idx1 ON #prem_movement(account_number); --Unique to make sure no dupes!


--5215410 Row(s) affected


--Update with info at EoS period
UPDATE #prem_movement
   SET premiums_at_end = tgt.premiums
  FROM #prem_movement AS pm
       inner join (
                        SELECT   csh.account_number
                                ,cel.prem_sports AS premiums
                          FROM  sk_prod.cust_subs_hist AS csh
                                inner join sk_prod.cust_entitlement_lookup AS cel
                                           ON csh.current_short_description = cel.short_description
                         WHERE  subscription_sub_type = 'DTV Primary Viewing'
                           AND  csh.status_code IN ('AC','AB','PC')
                           AND  csh.effective_from_dt BETWEEN '20110401' and '20110630'  --'EoS' period
                           AND csh.effective_to_dt > csh.effective_from_dt
                           AND  account_number IN (SELECT account_number FROM #prem_movement)
       )as tgt ON pm.account_number = tgt.account_number;



--663843 Row(s) affected

-----RESULTS EoS-------
--Pre 'EoS' vs 'EoS'



SELECT account_number
INTO #churners
FROM #prem_movement
WHERE premiums_at_end = 0;

--227384 Row(s) affected

select top 100 * from #prem_movement
drop table #churners

----------Upgrade events in SoS 2011/12 ('2011-08-05' to '2011-11-03')

--Update with upgrade events during SoS period


  SELECT  csh.Account_number
         ,csh.effective_from_dt as Upgrade_date
         ,csh.current_short_description
         ,ncel.prem_sports as current_premiums
         ,ocel.prem_sports as old_premiums
  INTO #EoS_churners
    FROM #churners AS CH INNER JOIN sk_prod.cust_subs_hist as csh
                    on ch.account_number = csh.account_number
         inner join sk_prod.cust_entitlement_lookup as ncel
                    on csh.current_short_description = ncel.short_description
         inner join sk_prod.cust_entitlement_lookup as ocel
                    on csh.previous_short_description = ocel.short_description
  WHERE csh.effective_from_dt BETWEEN '20110805' and '20111103'
    AND csh.effective_to_dt > csh.effective_from_dt
    AND subscription_sub_type='DTV Primary Viewing'
    AND status_code in ('AC','PC','AB')   -- Active records
    AND current_premiums > old_premiums   -- Increase in premiums
    AND csh.ent_cat_prod_changed = 'Y'    -- The package has changed - VERY IMPORTANT
    AND csh.prev_ent_cat_product_id<>'?'  -- This is not an Aquisition
;

--59329 Row(s) affected

UPDATE EPL_Base
SET EPL.EoS_Churn_Return = 1
FROM #EoS_churners ch INNER JOIN EPL_Base EPL
        on ch.account_number = epl.account_number

--37392 Row(s) affected

---------------------------------------------------------------------------------------------------Add Olive profiling to viewing table



Alter table v055_allinone
ADD (value_segment varchar(20), EoS_Churn_Return tinyint, cable tinyint, region varchar (20), affluence varchar (20)
        ,sports_package tinyint, movies tinyint, HD tinyint
        );

Update v055_allinone
SET     summ.value_segment = epl.value_segment
        ,summ.EoS_churn_return = epl.EoS_churn_return
        ,summ.cable = epl.cable
        ,summ.region = epl.region
        ,summ.affluence = epl.affluence
        ,summ.sports_package = epl.sports_package
        ,summ.movies = epl.movies
        ,summ.HD = epl.HD

FROM  v055_allinone summ INNER JOIN EPL_Base EPL
        on summ.account_number = epl.account_number

--5762411 Row(s) affected

-----------------------------------------------------------------------------------------------------------ESPN sub

Alter table v055_allinone
ADD ESPN tinyint;

select          one.account_number
INTO            #espn
from            v055_allinone one INNER JOIN sk_prod.cust_subs_hist csh
                                ON one.account_number = csh.account_number
        where subscription_type = 'A-LA-CARTE'
        and subscription_sub_type = 'ESPN'
        and effective_from_dt  <= '2012-03-20'
        and effective_to_dt > '2012-04-30'

;

--1683745 Row(s) affected
--1680900 Row(s) affected

Update v055_allinone a
SET a.ESPN = 1
FROM #espn e
WHERE e.account_number = a.account_number
;

--1657680 Row(s) affected

/* QA
select count(distinct(account_number)) from #espn; --46706
select count(distinct(account_number)) from v055_allinone; --163866
select top 10 (account_number) from #espn  --200000847620

select subscription_type ,subscription_sub_type, effective_from_dt, effective_to_dt
from sk_prod.cust_subs_hist
where account_number = '200000847620'
*/




--select sum(ESPN) from v055_allinone one


-------------------------------------------------------------------Previous sports sub downgrades



 SELECT  csh.Account_number
         ,csh.effective_from_dt as downgrade_date
         ,csh.current_short_description
         ,ncel.prem_sports as current_sports
         ,ocel.prem_sports as old_sports
         ,RANK() OVER (PARTITION BY csh.account_number ORDER BY csh.effective_from_dt, csh.cb_row_id DESC) AS 'RANK'
    INTO #down
    FROM EPL_Base as base
         inner join sk_prod.cust_subs_hist as csh
                    on base.account_number = csh.account_number
         inner join sk_prod.cust_entitlement_lookup as ncel
                    on csh.current_short_description = ncel.short_description
         inner join sk_prod.cust_entitlement_lookup as ocel
                    on csh.previous_short_description = ocel.short_description
    AND subscription_sub_type='DTV Primary Viewing'
    AND status_code in ('AC','PC','AB')   -- Active records
    AND current_sports < old_sports   -- sports downgrade
    AND csh.ent_cat_prod_changed = 'Y'    -- The package has changed - VERY IMPORTANT
    AND csh.prev_ent_cat_product_id<>'?'  -- This is not an Aquisition
;
--2159859 Row(s) affected
--select top 100 * from #down;


SELECT *
INTO #down2
FROM #down
where rank = 1;
--1080910 Row(s) affected

Alter Table v055_allinone
ADD Sport_downgrade tinyint;

Update v055_allinone base
SET Sport_downgrade = 1
FROM #down d
WHERE Base.account_number = d.account_number;

--1617982 Row(s) affected


--------------------------------------------------------------------------------------OUTPUT Viewing profiles

--select top 100* from v055_allinone;
/*
Value = actual # progs viewed / # possible programs to view

The value variable will then be output and the distribution will be used to bucket into value groups (lots/some/little viewing)

TABLES

v055_allinone   - summarised viewing data
EPL_2           -  old universe accounts, weightings and viewing for EPL and other sports
*/

--1. original code to get output data

select  sum(epl_matches) as ACT_epl ,sum(total_epl_matches) as POSS_epl
        ,sum(epl_highlights) as ACT_EPLhigh , sum(total_epl_highlights) as POSS_EPLhigh
        ,sum(other_sports) as ACT_sports ,sum(total_other_sports) as POSS_sports
        ,sum(sky_movies) as ACT_movies ,sum(total_sky_movies) as POSS_movies
        ,sum(other_football) as ACT_football, sum(total_other_football) as POSS_football
        ,sum(sky_atlantic) as ACT_Atlantic, sum(total_sky_atlantic) as POSS_Atlantic
        ,sum(other_hd) as ACT_otherHD, sum(total_other_hd) as POSS_otherHD
        ,sum(other_pay_tv) as ACT_otherPAY ,sum(total_other_pay_tv) as POSS_otherPAY
        ,sum(other_free_tv) as ACT_otherFREE ,sum(total_other_free_tv) as POSS_otherFREE
        ,sum(all_pay_tv) as ACT_allPAY
        ,sum(all_free_tv) as ACT_allFREE
        ,max(epl_sunday) as EPL_Sunday
        ,account_number, avg(weightings) as weighting, EoS_churn_return, cable
        ,region, affluence, sports_package, movies, ESPN, sport_downgrade
        ,max(case when (value_segment = 'Platinum' or value_segment = 'Gold' or value_segment = 'Silver' or value_segment = 'Bronze')
                THEN 'Loyal' Else value_segment END) as value_segment
INTO #value
from v055_allinone
group by account_number, EoS_churn_return, cable, region, affluence, sports_package, movies, ESPN, sport_downgrade
;
--163866 Row(s) affected
--163899 Row(s) affected

--drop table #value

/*
select count(account_number) from #value
where value_segment is not null;
--163066


select count(b.account_number)
from #value a inner join epl_2 b
        ON a.account_number = b.account_number
where a.value_segment is not null;
--163033

select count(account_number)
from epl_2;
--163033

select a.account_number, b.account_number, a.weightings, b.weighting
from v055_allinone a inner join epl_2 b
                        on a.account_number = b.account_number
where a.value_segment is not null;
*/

--2. new code to get OUTPUT data

/* Includes extra variables and uses EPL_2.  Viewing data was re-run a week later and data had been updated.  Therefore old data in EPL_2 was used to maintain
consistency in the universe)
*/


select  a.act_epl
        ,a.poss_epl
        ,a.act_sports
        ,a.poss_sports
        ,a.account_number
        ,a.weighting
        ,max(case when (a.value_segment = 'Platinum' or a.value_segment = 'Gold' or a.value_segment = 'Silver' or a.value_segment = 'Bronze')
                THEN 'Loyal' Else a.value_segment END) as value_segment
        ,a.eos_churn_return
        ,a.cable
        ,a.region
        ,a.affluence
        ,a.sports_package
        ,a.movies
        ,a.HD
        ,a.TT
        ,a.ESPN
        ,a.sport_downgrade
        ,sum(epl_highlights) as ACT_EPLhigh , sum(total_epl_highlights) as POSS_EPLhigh
        ,sum(sky_movies) as ACT_movies ,sum(total_sky_movies) as POSS_movies
        ,sum(sky_atlantic) as ACT_Atlantic, sum(total_sky_atlantic) as POSS_Atlantic
        ,sum(other_hd) as ACT_otherHD, sum(total_other_hd) as POSS_otherHD
        ,sum(other_pay_tv) as ACT_otherPAY ,sum(total_other_pay_tv) as POSS_otherPAY
        ,sum(other_free_tv) as ACT_otherFREE ,sum(total_other_free_tv) as POSS_otherFREE
        ,sum(all_pay_tv) as ACT_allPAY
        ,sum(all_free_tv) as ACT_allFREE
        ,sum(epl_sunday) as EPL_Sunday
        ,case when epl_sunday > 0 then 1 else 0 end as EPL_Sunday_ever
        ,sum(other_football) as ACT_football
        ,sum(motorsport) as ACT_motorsport
        ,sum(golf) as ACT_golf
        ,sum(rugby) as ACT_rugby
        ,sum(cricket) as ACT_cricket
INTO #combi1
from epl_2 a inner join v055_allinone b
                on a.account_number = b.account_number
where a.value_segment is not null
group by a.act_epl
        ,a.poss_epl
        ,a.act_sports
        ,a.poss_sports
        ,a.account_number
        ,a.weighting
        ,a.eos_churn_return
        ,a.cable
        ,a.region
        ,a.affluence
        ,a.sports_package
        ,a.movies
        ,a.HD
        ,a.TT
        ,a.ESPN
        ,a.sport_downgrade

;

--drop table #combi1
--select top 100 * from v055_allinone;

select * from #combi1;


-------------------------------------------------------------------------------------------------------------------------------------------------
/*
select account_number, avg(weightings) as weighting, value_segment, EoS_churn_return, cable, region, affluence, sports_package, movies, ESPN,
from #value
where value_segment is not null;

select  (ACT_epl+ACT_movies+ACT_ATlantic+ACT_otherPAY) as ACT_PAY
        ,ACT_otherFREE as ACT_FREE
        ,ACT_epl
        ,poss_epl
        ,ACT_sports
        ,poss_sports
        ,act_movies
        ,poss_movies
        ,account_number
INTO    #pay
from    #value
----select top 100 * from #pay;
--drop table #pay

select one.account_number
        ,avg(weightings) as weighting
        ,value_segment
        ,EoS_churn_return
        ,cable
        ,region
        ,affluence
        ,sports_package
        ,movies
        ,ACT_PAY
        ,ACT_FREE
        ,ACT_epl
        ,poss_epl
        ,ACT_sports
        ,poss_sports
        ,act_movies
        ,poss_movies

INTO v055_output
FROM v055_allinone one INNER JOIN #pay p
                        ON   one.account_number = p.account_number
group by one.account_number, value_segment, EoS_churn_return, cable, region, affluence, sports_package, movies, ACT_PAY, ACT_FREE ,ACT_epl
        ,poss_epl ,ACT_sports
        ,poss_sports
        ,act_movies
        ,poss_movies
;

--drop table v055_output;
--select top 100 * from v055_output;



*/

-----------------------------------------------------------------------------------------------------------EPL Matches ONLY

/* EPL_Match variable includes highlight programmes as it is difficult to identify just the actual matches from the meta data

However, subsequent request for just the EPL matches, we manually identified the matches and playout time from the EPG and hard coded below

*/


select top 100 * from #v055_temp_EPLmatchDay;

select account_number
        ,Tx_Start_Datetime_UTC
        ,dt
        ,viewing_category
        ,title

into #v055_temp_EPLmatchDay
from V055_temp
where viewing_category = 'EPL Matches'
        AND  ( dt = '2012-03-21'
        or dt = '2012-03-24'
        or dt = '2012-03-25'
        or dt = '2012-03-26'
        or dt = '2012-04-01'
        or dt = '2012-04-02'
        or dt between '2012-04-06' and '2012-04-11'
        or dt between '2012-04-14' and '2012-04-16'
        or dt between '2012-04-21' and '2012-04-22'
        or dt between '2012-04-28' and '2012-04-30')
;
--1,075,029 Row(s) affected

drop table #v055_temp_EPLmatchDay;

select count (distinct(account_number)) from #v055_temp_EPLmatchDay --count(distinct(#v055_temp_EPLmatchDay.account_number))138503
select count (distinct(account_number)) from #v055_EPLmatchDays

Select  a.account_number
        ,b.tm
        ,a.dt
        ,a.viewing_category
        ,a.title
into    #v055_EPLmatchDays
from    #v055_temp_EPLmatchDay a INNER JOIN v055_progs_EPLmatchDays b
                ON (a.title = b.title AND  a.dt = b.dt)
;
--1,625,823 Row(s) affected

select  account_number
        ,dt
        ,tm
        ,viewing_category
into    #v055_summ
from    #v055_EPLmatchDays
where      tm = '2012-03-21 19:00:00.000000'
        or tm = '2012-03-24 12:00:00.000000'
        or tm = '2012-03-24 16:30:00.000000'
        or tm = '2012-03-25 15:30:00.000000'
        or tm = '2012-03-26 19:00:00.000000'
        or tm = '2012-04-01 14:30:00.000000'
        or tm = '2012-04-01 12:00:00.000000'
        or tm = '2012-04-02 18:00:00.000000'
        or tm = '2012-04-06 15:00:00.000000'
        or tm = '2012-04-07 11:30:00.000000'
        or tm = '2012-04-08 14:30:00.000000'
        or tm = '2012-04-08 12:00:00.000000'
        or tm = '2012-04-09 18:30:00.000000'
        or tm = '2012-04-10 18:30:00.000000'
        or tm = '2012-04-11 18:30:00.000000'
        or tm = '2012-04-14 11:30:00.000000'
        or tm = '2012-04-15 14:00:00.000000'
        or tm = '2012-04-16 18:00:00.000000'
        or tm = '2012-04-21 11:00:00.000000'
        or tm = '2012-04-21 15:30:00.000000'
        or tm = '2012-04-22 11:00:00.000000'
        or tm = '2012-04-28 15:30:00.000000'
        or tm = '2012-04-29 14:30:00.000000'
        or tm = '2012-04-29 13:00:00.000000'
        or tm = '2012-04-30 18:00:00.000000'
        or tm = '2012-04-22 14:00:00.000000'
group  by account_number
        ,dt
        ,tm
        ,viewing_category
;
--1093428 Row(s) affected

-- select  dt
--         ,tm
--         ,viewing_category
-- from    #v055_EPLmatchDays
-- group by        dt
--         ,tm
--         ,viewing_category
-- ;

 select account_number
        ,sum(case when viewing_category = 'EPL Matches'    then 1 else 0 end) as epl_MatchTime
    into v055_EPLmatchday
    from #v055_summ
group by account_number

;
--137473 Row(s) affected


--drop table v055_EPLmatchday

Alter table epl_2
add EPL_Match_Only int;

Update epl_2 a
set EPL_Match_Only = epl_MatchTime
from v055_EPLmatchday b
where a.account_number = b.account_number
;

---------------------------------------------------------------------------------------------------------------SKY Go streaming data
select MAX (case when (
                 broadcast_channel = 'StreamSkySports1'
                 or broadcast_channel = 'StreamSkySports2'
                 --or broadcast_channel = 'StreamSkySports3'
                 --or broadcast_channel = 'StreamSkySports4'
                 )
                 THEN 1 ELSE 0 END) as SkyGoSports
,activity_dt
,site_name
,account_number
into #channel_summary
from sk_prod.SKY_PLAYER_USAGE_DETAIL
where activity_dt BETWEEN '2012-03-20' AND '2012-04-30'
GROUP BY activity_dt
,site_name
,account_number
order by account_number
,activity_dt
,site_name
;

--19,397,094 Row(s) affected

-- select count(account_number) from #channel_summary  --count(#channel_summary.account_number) 19,396,842
-- order by broadcast_channel
-- ,activity_dt
-- ,site_name;
-- commit;

-- drop table #channel_summary;
--
-- select top 1000 * from v055_skygo order by account_number, activity_dt;
-- where account_number is not null;

--select top 100 * from v055_allinone;

Select  a.account_number
        ,a.activity_dt
        ,a.SkyGoSports
INTO    v055_SkyGo
FROM    #channel_summary a INNER JOIN v055_allinone b
        ON a.account_number = b.account_number
where   skygosports = 1
group by        a.account_number
                ,a.activity_dt
                ,a.skygosports
;
--337800 Row(s) affected


select  account_number
        ,count(activity_dt) as SkyGo_Days
        ,skygosports
into #epl
from v055_skygo
where      activity_dt = '2012-03-21'
        or activity_dt = '2012-03-24'
        or activity_dt = '2012-03-25'
        or activity_dt = '2012-03-26'
        or activity_dt = '2012-04-01'
        or activity_dt = '2012-04-02'
        or activity_dt between '2012-04-06' and '2012-04-11'
        or activity_dt between '2012-04-14' and '2012-04-16'
        or activity_dt between '2012-04-21' and '2012-04-22'
        or activity_dt between '2012-04-28' and '2012-04-30'
group by account_number, skygosports
;

--51637 Row(s) affected

Alter table EPL_2
add EPL_SkyGO int;

Update epl_2 a
SET EPL_SkyGo = SkyGo_Days
FROM #epl b
WHERE a.account_number = b.account_number
;

--51374 Row(s) affected

-- select top 1000 * from epl_2;
--
-- select * from v055_skygo
-- where account_number = '200000940813';

select  account_number
        ,weighting
        ,epl_skygo
from epl_2
order by account_number
;



--136848 Row(s) affected


select  account_number
        ,ACT_epl
        ,EPL_Match_Only
from epl_2
order by account_number
;

