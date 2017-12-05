/*###############################################################################
# Created on:   28/02/2014
# Created by:   Sebastian Bednaszynski(SBE)
# Description:  EPL rights project - EPG lookup
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 28/02/2014  SBE   Initial version
#
###############################################################################*/


  -- ##############################################################################################################
  -- ##### Get an EPG lookup                                                                                  #####
  -- ##############################################################################################################
  -- Clean-up, table created by someone else (James McKane)
if object_id('EPL_01_EPG') is not null then drop table EPL_01_EPG end if;
create table EPL_01_EPG (
    Pk_Identifier                           bigint            identity,
    Dk_Programme_Instance_Dim               bigint            not null  default 0,
    Service_Key                             smallint          null      default null,
    Broadcast_Date                          date              null      default null,
    Broadcast_Start_Time_UTC                datetime          null      default null,
    Broadcast_End_Time_UTC                  datetime          null      default null,
    Broadcast_Duration                      bigint            null      default 0,
    Channel                                 varchar(20)       null      default null,
    Programme                               varchar(100)      null      default null,
    Programme_Instance_Name                 varchar(80)       null      default null,

    Teams                                   varchar(50)       null      default null,
    Game                                    tinyint           null      default null,
    Season                                  varchar(10)       null      default null,
    Match_Date                              date              null      default null,
    Kick_Off_Time                           time              null      default null,
    Day_Of_Week                             varchar(10)       null      default null,
    Live_Game_Flag                          bit               null      default 0,
    EPL_Pack                                varchar(30)       null      default null,

    Data_Source                             varchar(50)       null      default null,
    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name()
);
create unique hg   index idx01 on EPL_01_EPG(Dk_Programme_Instance_Dim);
create        lf   index idx02 on EPL_01_EPG(Service_Key);
create        hg   index idx03 on EPL_01_EPG(Programme_Instance_Name);
create        date index idx04 on EPL_01_EPG(Broadcast_Date);
create        dttm index idx05 on EPL_01_EPG(Broadcast_Start_Time_UTC);
create        lf   index idx06 on EPL_01_EPG(Channel);
grant select on EPL_01_EPG to vespa_group_low_security;


/*
v269_EPL_Live_Matches2              for live matches
v269_EPL_Non_Live                   for non live shows

v269_EPL_Non_Live_Feb_Jan           for non live
and v269_EPL_Live_Matches_Feb_Jan2  for live
*/


insert into EPL_01_EPG
      (Dk_Programme_Instance_Dim, Service_Key, Broadcast_Start_Time_UTC, Broadcast_End_Time_UTC, Broadcast_Duration,
       Channel, Programme, Teams, Game, Season, Match_Date, Kick_Off_Time, Day_Of_Week, Live_Game_Flag, EPL_Pack, Data_Source)
select
      Dk_Programme_Instance_Dim,
      Service_Key,
      Broadcast_Start_Date_Time_UTC,
      Broadcast_End_Date_Time_UTC,
      datediff(second, Broadcast_Start_Date_Time_UTC, Broadcast_End_Date_Time_UTC),
      Channel_Name,
      Programme_Instance_Name,
      Teams,
      Game,
      Season,
      Match_Date,
      Kick_Off,
      Day,
      1,
      Pack,
      'mckanej.v269_EPL_Live_Matches2'
  from mckanej.v269_EPL_Live_Matches2;
commit;


insert into EPL_01_EPG
      (Dk_Programme_Instance_Dim, Service_Key, Broadcast_Start_Time_UTC, Broadcast_End_Time_UTC, Broadcast_Duration,
       Channel, Programme, Live_Game_Flag, EPL_Pack, Data_Source)
select
      Dk_Programme_Instance_Dim,
      Service_Key,
      Broadcast_Start_Date_Time_UTC,
      Broadcast_End_Date_Time_UTC,
      datediff(second, Broadcast_Start_Date_Time_UTC, Broadcast_End_Date_Time_UTC),
      Channel_Name,
      Programme_Instance_Name,
      0,
      Pack,
      'mckanej.v269_EPL_Non_Live'
  from mckanej.v269_EPL_Non_Live;
commit;



insert into EPL_01_EPG
      (Dk_Programme_Instance_Dim, Service_Key, Broadcast_Start_Time_UTC, Broadcast_End_Time_UTC, Broadcast_Duration,
       Channel, Programme, Teams, Game, Season, Match_Date, Kick_Off_Time, Day_Of_Week, Live_Game_Flag, EPL_Pack, Data_Source)
select
      Dk_Programme_Instance_Dim,
      Service_Key,
      Broadcast_Start_Date_Time_UTC,
      Broadcast_End_Date_Time_UTC,
      datediff(second, Broadcast_Start_Date_Time_UTC, Broadcast_End_Date_Time_UTC),
      Channel_Name,
      Programme_Instance_Name,
      Teams,
      Game,
      Season,
      Match_Date,
      Kick_Off,
      Day,
      1,
      Pack,
      'mckanej.v269_EPL_Live_Matches_Feb_Jan2'
  from mckanej.v269_EPL_Live_Matches_Feb_Jan2
 where date(Broadcast_Start_Date_Time_UTC) < '2013-08-01 00:00:00';
commit;


insert into EPL_01_EPG
      (Dk_Programme_Instance_Dim, Service_Key, Broadcast_Start_Time_UTC, Broadcast_End_Time_UTC, Broadcast_Duration,
       Channel, Programme, Live_Game_Flag, EPL_Pack, Data_Source)
select
      Dk_Programme_Instance_Dim,
      Service_Key,
      Broadcast_Start_Date_Time_UTC,
      Broadcast_End_Date_Time_UTC,
      datediff(second, Broadcast_Start_Date_Time_UTC, Broadcast_End_Date_Time_UTC),
      Channel_Name,
      Programme_Instance_Name,
      0,
      Pack,
      'mckanej.v269_EPL_Non_Live_Feb_Jan'
  from mckanej.v269_EPL_Non_Live_Feb_Jan
 where date(Broadcast_Start_Date_Time_UTC) < '2013-08-01 00:00:00';
commit;


  -- Drop v long programmes
delete from EPL_01_EPG
 where Broadcast_Duration > 16200;
commit;


  -- Manual hacks & updates
update EPL_01_EPG base
   set base.Channel = trim(replace( replace(det.Vespa_Name, '+ 1', ''), '+1', '' ))
  from VESPA_Analysts.Channel_Map_Prod_Service_Key_Attributes det
 where base.Service_Key = det.Service_Key
   and date(det.Effective_To) = '2999-12-31';
commit;

update EPL_01_EPG base
   set base.Broadcast_Date = date(Broadcast_Start_Time_UTC),
       base.Channel       = case
                              when base.Service_Key in (4052, 4054) then '3D channel'
                              when base.Service_Key in (4005, 1341, 1361, 1311) then 'Sky Sports 1'
                              when base.Service_Key in (3662) then 'BT Sport 1'
                              when base.Service_Key in (3142, 4041) then 'ESPN'
                              when base.Service_Key in (5490) then 'BBC Red Button'
                              when base.Service_Key in (1320, 1362, 1702, 2051,4085) then 'Pub channel/Other'
                                else base.Channel
                            end,
       base.Programme     = case
                              when Live_Game_Flag = 1 then 'Live game (' || Teams || ')'
                              when Programme like 'Soccer A.%' then 'Soccer AM'
                              when Programme like 'Football First%' then 'Football First'
                                else Programme
                            end,
       base.EPL_Pack      = case
                              when EPL_Pack = 'BBC Non Live' then 'BBC (non-live)'
                              when EPL_Pack = 'BT Non Live' then 'BT Sport (non-live)'
                              when EPL_Pack = 'Sky Non Live' then 'Sky Sports (non-live)'

                                -- Remove new line characters
                              when EPL_Pack like 'Festive Early Afternoon%' then 'Festive Early Afternoon'
                              when EPL_Pack like 'Festive Late Afternoon%' then 'Festive Late Afternoon'
                              when EPL_Pack like 'Festive Lunchtime%' then 'Festive Lunchtime'
                              when EPL_Pack like 'Midweek Evening%' then 'Midweek Evening'
                              when EPL_Pack like 'Monday Evening%' then 'Monday Evening'
                              when EPL_Pack like 'Saturday Afternoon%' then 'Saturday Afternoon'
                              when EPL_Pack like 'Saturday Lunchtime%' then 'Saturday Lunchtime'
                              when EPL_Pack like 'Sunday Early Afternoon%' then 'Sunday Early Afternoon'
                              when EPL_Pack like 'Sunday Late Afternoon%' then 'Sunday Late Afternoon'
                              when EPL_Pack like 'Sunday Lunchtime%' then 'Sunday Lunchtime'

                                else EPL_Pack
                            end,
       base.Day_Of_Week   = case
                              when datepart(weekday, Broadcast_Start_Time_UTC) = 1 then 'Sunday'
                              when datepart(weekday, Broadcast_Start_Time_UTC) = 2 then 'Monday'
                              when datepart(weekday, Broadcast_Start_Time_UTC) = 3 then 'Tuesday'
                              when datepart(weekday, Broadcast_Start_Time_UTC) = 4 then 'Wednesday'
                              when datepart(weekday, Broadcast_Start_Time_UTC) = 5 then 'Thursday'
                              when datepart(weekday, Broadcast_Start_Time_UTC) = 6 then 'Friday'
                              when datepart(weekday, Broadcast_Start_Time_UTC) = 7 then 'Saturday'
                                else null
                            end;
commit;

update EPL_01_EPG base
   set base.Programme_Instance_Name =
                            case
                              when base.Dk_Programme_Instance_Dim is null then null
                                    else base.Programme || ' [' || dateformat(base.Broadcast_Start_Time_UTC, 'yyyy-mm-dd @hh:mm') || ']'
                            end,
       base.EPL_Pack      = case
                              when EPL_Pack = 'Sunday Lunchtime' then 'Sunday Early Afternoon'
                              when EPL_Pack = 'Festive Lunchtime' then 'Sunday Early Afternoon'
                              when EPL_Pack = 'Festive Early Afternoon' then 'Sunday Early Afternoon'
                              when EPL_Pack = 'Festive Late Afternoon' then 'Sunday Late Afternoon'
                                else EPL_Pack
                            end;
commit;

  -- Negligible viewing, a few programme only - causes issues with overlapping programmes
delete from EPL_01_EPG
 where Channel = 'Pub channel/Other';
commit;



  -- ##############################################################################################################
  -- ##### Audit                                                                                              #####
  -- ##############################################################################################################

select Data_Source, Broadcast_Duration , count(*) as Cnt from EPL_01_EPG group by Data_Source, Broadcast_Duration order by 1, 2;
select Data_Source, Programme          , count(*) as Cnt from EPL_01_EPG group by Data_Source, Programme          order by 1, 2;
select Data_Source, Channel            , count(*) as Cnt from EPL_01_EPG group by Data_Source, Channel            order by 1, 2;
select Data_Source, Game               , count(*) as Cnt from EPL_01_EPG group by Data_Source, Game               order by 1, 2;
select Data_Source, Season             , count(*) as Cnt from EPL_01_EPG group by Data_Source, Season             order by 1, 2;
select Data_Source, Match_Date         , count(*) as Cnt from EPL_01_EPG group by Data_Source, Match_Date         order by 1, 2;
select Data_Source, Kick_Off_Time      , count(*) as Cnt from EPL_01_EPG group by Data_Source, Kick_Off_Time      order by 1, 2;
select Data_Source, Day_Of_Week        , count(*) as Cnt from EPL_01_EPG group by Data_Source, Day_Of_Week        order by 1, 2;
select Data_Source, Live_Game_Flag     , count(*) as Cnt from EPL_01_EPG group by Data_Source, Live_Game_Flag     order by 1, 2;
select Data_Source, EPL_Pack           , count(*) as Cnt from EPL_01_EPG group by Data_Source, EPL_Pack           order by 1, 2;


  -- ##############################################################################################################
  -- ##############################################################################################################


























