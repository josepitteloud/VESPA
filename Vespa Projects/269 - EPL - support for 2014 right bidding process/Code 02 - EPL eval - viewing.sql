/*###############################################################################
# Created on:   03/03/2014
# Created by:   Sebastian Bednaszynski(SBE)
# Description:  EPL rights project - viewing aggregation
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 03/03/2014  SBE   Initial version
# 25/03/2014  SBE   Current table archived as "v01"
#                   Dimensions added:
#                     - Sky Atlantic
#                     - Sky Exclusive channels (Sky News HD, Sky Atlantic,
#                         Sky Sports News HD, CI HD, History HD, E! HD, Disney HD,
#                         More 4 HD, Disney XD HD, Universal HD, Star Plus HD,
#                         Lifetime HD, Disney Junior HD)
# 07/07/2014  SBE   Additional extract created for Sky Atlantic
# 14/07/2014  SBE   Additional extract created for Sky branded channels
# 20/11/2014  SBE   Viewing extract rebuilt, different channels & cuts added. Removed
#                   redundant sections
#
###############################################################################*/


  -- ##############################################################################################################
  -- ##### Get a list of daily accounts available within the period                                           #####
  -- ##############################################################################################################
if object_id('EPL_02_Viewing_Summary') is not null then drop table EPL_02_Viewing_Summary end if;
create table EPL_02_Viewing_Summary (
    Pk_Identifier                           bigint            identity,
    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name(),

      -- Account
    Account_Number                          varchar(20)       null      default null,
    Days_Data_Available                     smallint          null      default 0,
    Period                                  tinyint           null      default 0,
    First_BT_Viewing                        date              null      default null,

      -- Viewing
    Viewing_Day                             date              null      default null,
    Instance_Start_Date                     date              null      default null,
    Instance_Start_Hour                     tinyint           null      default null,

    Event_Start_Time                        datetime          null      default null,
    Event_End_Time                          datetime          null      default null,
    Instance_Start_Time                     datetime          null      default null,
    Instance_End_Time                       datetime          null      default null,
    Event_Duration                          bigint            null      default 0,
    Instance_Duration                       bigint            null      default 0,

    Dk_Programme_Instance_Dim               bigint            null      default 0,
    Service_Key                             smallint          null      default null,
    Sports_Genre_Flag                       bit               null      default 0,
    Sports_Channel_Type                     tinyint           null      default 0,      -- 1: Sky Sports, 2: BT Sports, 3: ESPN, 4: All other
    Pay_TV_Type                             tinyint           null      default 0,      -- 0: FTA, 1: Pay package, 2: Pay Sports, 3: Pay Movies, 4: 3rd party, 5: A'La Carte, 6: PPV/other
    Sky_Branded_Channel                     smallint          null      default 0,      -- 0: All other, 1: Sky Atlantic (all), 2: Other Sky Branded channels, 3: Sky Sports News
    Third_Party_Channel                     smallint          null      default 0,      -- 0: All other, 1: FOX, 2: Universal, 3: Comedy Central, 4: Eurosport, 5: Discovery, 6: Syfy
    Viewing_Type                            tinyint           null      default 0,      -- 1: Live, 2: Live pause (up to 15 mins), 3: Playback
    Channel_Version_1hr                     bit               null      default 0,

      -- EPG
    EPL_Content                             tinyint           null      default 0,
    Broadcast_Start_Time_UTC                datetime          null      default null,
    Broadcast_End_Time_UTC                  datetime          null      default null,
    Broadcast_Duration                      bigint            null      default 0,
    Channel                                 varchar(20)       null      default null,
    Programme                               varchar(100)      null      default null,
    Programme_Instance_Name                 varchar(80)       null      default null,
    Live_Game_Flag                          bit               null      default null,
    EPL_Pack                                varchar(30)       null      default null

);
create        hg   index idx01 on EPL_02_Viewing_Summary(Account_Number);
create        date index idx02 on EPL_02_Viewing_Summary(First_BT_Viewing);
create        date index idx03 on EPL_02_Viewing_Summary(Viewing_Day);
create        hg   index idx04 on EPL_02_Viewing_Summary(Dk_Programme_Instance_Dim);
create        hg   index idx05 on EPL_02_Viewing_Summary(Service_Key);
create        hg   index idx06 on EPL_02_Viewing_Summary(Programme_Instance_Name);
create        hg   index idx07 on EPL_02_Viewing_Summary(Event_Start_Time);
create        hg   index idx08 on EPL_02_Viewing_Summary(Event_End_Time);
create        hg   index idx09 on EPL_02_Viewing_Summary(Instance_Start_Date);
create        hg   index idx10 on EPL_02_Viewing_Summary(Instance_End_Time);
create        dttm index idx11 on EPL_02_Viewing_Summary(Broadcast_Start_Time_UTC);
create        dttm index idx12 on EPL_02_Viewing_Summary(Broadcast_End_Time_UTC);


if object_id('EPL_1_Extract_Viewing') is not null then drop procedure EPL_1_Extract_Viewing end if;
create procedure EPL_1_Extract_Viewing
      @parStartDate             date = null,
      @parEndDate               date = null,      -- Must be the same month as Start Date!!!!
      @parPeriod                tinyint = 0,
      @parDestTable             varchar(50)
as
begin
      declare @varTableSuffix                 varchar(6)
      declare @varSQL                         varchar(25000)

      set @varTableSuffix       = (dateformat(@parStartDate, 'yyyymm'))


      execute logger_add_event 0, 0, '##### Processing period: ' || dateformat(@parStartDate, 'dd/mm/yyyy') || ' - '  || dateformat(@parEndDate, 'dd/mm/yyyy') || ' #####', null
      execute logger_add_event 0, 0, 'Source table used: sk_prod.vespa_dp_prog_viewed_' || @varTableSuffix, null
      execute logger_add_event 0, 0, 'Destination table used: ' || @parDestTable, null



      set @varSQL = '
                      delete from ' || @parDestTable || '
                       where Viewing_Day between ''' || @parStartDate || ''' and ''' || @parEndDate || '''
                      commit

                      execute logger_add_event 0, 0, ''Viewing data rows for current period removed'', @@rowcount


                      insert into ' || @parDestTable || '
                             (Account_Number, Period, First_BT_Viewing, Viewing_Day, Instance_Start_Date, Instance_Start_Hour, Event_Start_Time, Event_End_Time, Instance_Start_Time,
                              Instance_End_Time, Event_Duration, Instance_Duration, Dk_Programme_Instance_Dim, Service_Key, Sports_Genre_Flag, Sports_Channel_Type, Pay_TV_Type,
                              Sky_Branded_Channel, Third_Party_Channel, Viewing_Type, Channel_Version_1hr, EPL_Content, Broadcast_Start_Time_UTC, Broadcast_End_Time_UTC,
                              Broadcast_Duration, Channel, Programme, Programme_Instance_Name, Live_Game_Flag, EPL_Pack)
                      select
                            un.Account_Number,
                            ' || @parPeriod || ',
                            un.First_BT_Viewing,
                            un.Data_Day,                                                                      -- Viewing_Day

                            date(Instance_Start_Date_Time_UTC),                                               -- Instance_Start_Date
                            hour(Instance_Start_Date_Time_UTC),                                               -- Instance_Start_Hour
                            vw.Event_Start_Date_Time_UTC,                                                     -- Event_Start_Time
                            vw.Capping_End_Date_Time_UTC,                                                     -- Event_End_Time
                            Instance_Start_Date_Time_UTC,                                                     -- Instance_Start_Time
                            case
                              when vw.Capped_Partial_Flag = 1 then vw.Capping_End_Date_Time_UTC
                                else vw.Instance_End_Date_Time_UTC
                            end,                                                                              -- Instance_End_Time
                            datediff(second, vw.Event_Start_Date_Time_UTC, vw.Capping_End_Date_Time_UTC),     -- Event_Duration
                            case
                              when vw.Capped_Partial_Flag = 1 then datediff(second, vw.Instance_Start_Date_Time_UTC, vw.Capping_End_Date_Time_UTC)
                                else datediff(second, vw.Instance_Start_Date_Time_UTC, vw.Instance_End_Date_Time_UTC)
                            end,                                                                              -- Instance_Duration

                            vw.Dk_Programme_Instance_Dim,                                                     -- Dk_Programme_Instance_Dim
                            vw.Service_Key,                                                                   -- Service_Key

                            case
                              when vw.Genre_Description = ''Sports'' then 1
                                else 0
                            end,                                                                              -- Sports_Genre_Flag

                              -- 1: Sky Sports, 2: BT Sports, 3: ESPN, 4: All other
                            case
                              when vw.Service_Key in (1301, 1701, 4002, 1302, 4081, 1306, 3835,
                                                      1333, 4022, 1322, 4026, 1302, 4081) then 1              -- Sky Sports 1/2/F1/3/4/Ashes
                              when vw.Service_Key in (3625, 3661, 3627, 3663) then 2                          -- BT Sports 1/2
                              when vw.Service_Key in (3141, 4040) then 3                                      -- ESPN
                                else 4
                            end,                                                                              -- Sports_Channel_Type

                              -- 0: FTA, 1: Pay package, 2: Pay Sports, 3: Pay Movies, 4: 3rd party, 5: A-La Carte, 6: PPV/other
                            case
                              when cm.Channel_Type in (''Retail - Entertainment'', ''Retail - Entertainment Extra'', ''Retail - Entertainment+'',
                                                       ''Retail - Entertainment (NI only)'', ''Retail - 3D'') then 1
                              when cm.Channel_Type in (''Retail - Sports'', ''Retail - Sports + HD Pack'') then 2
                              when cm.Channel_Type in (''Retail - Movies'', ''Retail - Movies + HD Pack'') then 3
                              when cm.Channel_Type in (''NR - Conditional Access'', ''NR - Pay-per-view'') then 4
                              when cm.Channel_Type in (''Retail - ALC'', ''Retail - ALC + Ent Extra+'', ''Retail - ALC / Movies Pack + Ent Extra+'') then 5
                              when cm.Channel_Type in (''Retail - Adult Nightly'', ''Retail - Pay-per-night'', ''Retail - Pay-per-view'', ''Retail - PPV HD'',
                                                       ''Retail - ROI Bonus'') then 6
                                else 0
                            end,                                                                              -- Pay_TV_Type


                              -- 0: All other, 1: Sky Atlantic (all), 2: Other Sky Branded channels, 3: Sky Sports News
                            case
                              when vw.Service_Key in (
                                                      1412,       -- Sky Atlantic (SD)
                                                      4053,       -- Sky Atlantic (HD)
                                                      1414,       -- Sky Atlantic (HD)
                                                      1413        -- Sky Atlantic (HD)
                                                     ) then 1

                              when vw.Service_Key in (
                                                      1752, 4063, -- Sky Arts 1
                                                      1753, 4064, -- Sky Arts 2
                                                      2201, 4066, -- Sky Living
                                                      2205,       -- Sky Living+1
                                                      2207,       -- Sky Livingit
                                                      4330,       -- Sky Livingit+1
                                                      1402, 4061, -- Sky1
                                                      1403,       -- Sky1+1
                                                      1833,       -- Sky2
                                                      4052, 4054  -- 3D channel
                                                     ) then 2

                              when vw.Service_Key in (
                                                      1314,       -- Sky Sports News (SD)
                                                      1319,       -- Sky Sports News (SD)
                                                      4035,       -- Sky Sports News (HD)
                                                      4049        -- Sky Sports News (HD)
                                                     ) then 3
                                else 0
                            end,                                                                              -- Sky_Branded_Channel

                              -- 0: All other, 1: FOX, 2: Universal, 3: Comedy Central, 4: Eurosport, 5: Discovery, 6: Syfy
                            case
                              when vw.Service_Key in (
                                                      1305,       -- FX SD
                                                      1791,       -- FX+ SD
                                                      4023        -- FX HD
                                                     ) then 1

                              when vw.Service_Key in (
                                                      1842,       -- Universal SD
                                                      3207,       -- Universal+1 SD
                                                      3745,       -- Universal (ROI) SD
                                                      4080        -- Universal HD
                                                     ) then 2

                              when vw.Service_Key in (
                                                      1813,       -- Comedy Central Extra SD
                                                      2510,       -- Comedy Central SD
                                                      2709,       -- Comedy Central X Roi SD
                                                      3607,       -- Comedy Central Extra+1 SD
                                                      3802,       -- Comedy Central+1 SD
                                                      4056        -- Comedy Central HD
                                                     ) then 3

                              when vw.Service_Key in (
                                                      1726,       -- Eurosport UK SD
                                                      1841,       -- Eurosport2 UK SD
                                                      4004,       -- Eurosport HD
                                                      4009        -- Eurosport2 HD
                                                     ) then 4

                              when vw.Service_Key in (
                                                      1351,       -- Discovery Science+1 SD
                                                      1353,       -- Discovery Real Time +1
                                                      2401,       -- Discovery SD
                                                      2403,       -- Discovery Real Time
                                                      2404,       -- Discovery+1 SD
                                                      2405,       -- Discovery Travel & Living
                                                      2406,       -- Discovery Science
                                                      2407,       -- Discovery History SD
                                                      2408,       -- Discovery Home & Health
                                                      2409,       -- Discovery Turbo
                                                      2410,       -- Discovery Home & Health +1
                                                      3760,       -- Discovery History+1
                                                      4003,       -- Discovery
                                                      4548        -- Discovery Shed SD
                                                     ) then 5

                              when vw.Service_Key in (
                                                      2505,       -- Syfy SD
                                                      2513,       -- Syfy +1 SD
                                                      4074        -- Syfy HD
                                                     ) then 6

                                else 0
                            end,                                                                              -- Third_Party_Channel

                              -- 1: Live, 2: Live pause (up to 15 mins), 3: Playback
                            case
                              when vw.live_recorded = ''LIVE'' then 1
                              when vw.time_in_seconds_since_recording <= 15 * 60 then 2
                                else 3
                            end,                                                                              -- Viewing_Type

                            case
                              when cm.Vespa_Name like ''%+1%'' then 1
                                else 0
                            end,                                                                              -- Channel_Version_1hr


                            case
                              when epg.Dk_Programme_Instance_Dim is null then 0
                                else 1
                            end,                                                                              -- EPL_Content
                            epg.Broadcast_Start_Time_UTC,
                            epg.Broadcast_End_Time_UTC,
                            epg.Broadcast_Duration,
                            epg.Channel,
                            epg.Programme,
                            case
                              when epg.Dk_Programme_Instance_Dim is null then ''''
                                else epg.Programme || '' ['' || dateformat(epg.Broadcast_Start_Time_UTC, ''yyyy-mm-dd @hh:mm'') || '']''
                            end,
                            epg.Live_Game_Flag,
                            epg.EPL_Pack

                        from EPL_01_Universe un,
                             sk_prod.vespa_dp_prog_viewed_' || @varTableSuffix || ' vw
                                left join EPL_01_EPG epg                                                  on vw.Dk_Programme_Instance_Dim = epg.Dk_Programme_Instance_Dim
                                left join VESPA_Analysts.Channel_Map_Prod_Service_Key_Attributes cm       on vw.Service_Key = cm.Service_Key
                                                                                                         and cm.Effective_From < date(broadcast_start_date_time_utc)
                                                                                                         and cm.Effective_To >= date(broadcast_start_date_time_utc)

                       where un.Account_Number = vw.Account_Number
                         and un.Data_Day between ''' || @parStartDate || ''' and ''' || @parEndDate || '''
                         and un.Valid_Account_Flag = 1
                         and un.Period = ' || @parPeriod || '

                         and vw.dk_capping_end_datehour_dim > 0                                     -- Events received on time for capping
                         and vw.capped_full_flag = 0
                         and vw.panel_id = 12                                                       -- Panel 12 only
                         and vw.Instance_Start_Date_Time_UTC < vw.Instance_End_Date_Time_UTC        -- Remove 0sec instances
                         and vw.account_number is not null
                         and vw.subscriber_id is not null
                         and vw.broadcast_start_date_time_utc >= dateadd(hour, -(24*28), vw.event_start_date_time_utc)
                         and (vw.reported_playback_speed is null or vw.reported_playback_speed = 2)
                         and (
                               vw.type_of_viewing_event in (''HD Viewing Event'', ''TV Channel Viewing'')
                               or
                               (
                                 vw.type_of_viewing_event = ''Other Service Viewing Event''
                                 and
                                 cm.Channel_Type in (''Retail - Pay-per-night'', ''Retail - Pay-per-view'',
                                                     ''Retail - PPV HD'', ''NR - Pay-per-view'')
                               )
                               or
                               (
                                 vw.type_of_viewing_event = ''Sky+ time-shifted viewing event''
                                 and
                                 cm.Channel_Type <> ''NR - FTA - Radio''
                               )
                             )

                         and un.Data_Day = date(vw.event_start_date_time_utc)
                      commit

                      execute logger_add_event 0, 0, ''Data has been processed'', @@rowcount
                    '

      execute(@varSQL)


end;


if object_id('EPL_1_Extract_Viewing_Assemble') is not null then drop procedure EPL_1_Extract_Viewing_Assemble end if;
create procedure EPL_1_Extract_Viewing_Assemble
      @parSrcTableId            varchar(50)
as
begin
      declare @varSQL                         varchar(25000)

      execute logger_add_event 0, 0, '##### Assembling viewing data #####', null
      execute logger_add_event 0, 0, 'Source table used: EPL_02_Viewing_Summary_' || @parSrcTableId, null
      execute logger_add_event 0, 0, 'Destination table used: EPL_02_Viewing_Summary', null


      set @varSQL = '
                      insert into EPL_02_Viewing_Summary
                             (Account_Number, Period, First_BT_Viewing, Viewing_Day, Instance_Start_Date, Instance_Start_Hour, Event_Start_Time, Event_End_Time, Instance_Start_Time,
                              Instance_End_Time, Event_Duration, Instance_Duration, Dk_Programme_Instance_Dim, Service_Key, Sports_Genre_Flag, Sports_Channel_Type, Pay_TV_Type,
                              Sky_Branded_Channel, Third_Party_Channel, Viewing_Type, Channel_Version_1hr, EPL_Content, Broadcast_Start_Time_UTC, Broadcast_End_Time_UTC,
                              Broadcast_Duration, Channel, Programme, Programme_Instance_Name, Live_Game_Flag, EPL_Pack)
                        select
                              Account_Number, Period, First_BT_Viewing, Viewing_Day, Instance_Start_Date, Instance_Start_Hour, Event_Start_Time, Event_End_Time, Instance_Start_Time,
                              Instance_End_Time, Event_Duration, Instance_Duration, Dk_Programme_Instance_Dim, Service_Key, Sports_Genre_Flag, Sports_Channel_Type, Pay_TV_Type,
                              Sky_Branded_Channel, Third_Party_Channel, Viewing_Type, Channel_Version_1hr, EPL_Content, Broadcast_Start_Time_UTC, Broadcast_End_Time_UTC,
                              Broadcast_Duration, Channel, Programme, Programme_Instance_Name, Live_Game_Flag, EPL_Pack
                         from EPL_02_Viewing_Summary_' || @parSrcTableId || '

                      commit
                      execute logger_add_event 0, 0, ''Data has been uploaded to the main table'', @@rowcount
                    '

      execute(@varSQL)


end;



  -- ##############################################################################################################
  -- Run extract - period 1 (Aug '13 - Feb '14)
execute EPL_1_Extract_Viewing '2013-08-01', '2013-08-15', 1, 'EPL_02_Viewing_Summary_1';
execute EPL_1_Extract_Viewing '2013-08-16', '2013-08-31', 1, 'EPL_02_Viewing_Summary_1';

execute EPL_1_Extract_Viewing '2013-09-01', '2013-09-15', 1, 'EPL_02_Viewing_Summary_1';
execute EPL_1_Extract_Viewing '2013-09-16', '2013-09-30', 1, 'EPL_02_Viewing_Summary_1';

execute EPL_1_Extract_Viewing '2013-10-01', '2013-10-15', 1, 'EPL_02_Viewing_Summary_2';
execute EPL_1_Extract_Viewing '2013-10-16', '2013-10-31', 1, 'EPL_02_Viewing_Summary_2';

execute EPL_1_Extract_Viewing '2013-11-01', '2013-11-15', 1, 'EPL_02_Viewing_Summary_2';
execute EPL_1_Extract_Viewing '2013-11-16', '2013-11-30', 1, 'EPL_02_Viewing_Summary_2';

execute EPL_1_Extract_Viewing '2013-12-01', '2013-12-15', 1, 'EPL_02_Viewing_Summary_3';
execute EPL_1_Extract_Viewing '2013-12-16', '2013-12-31', 1, 'EPL_02_Viewing_Summary_3';

execute EPL_1_Extract_Viewing '2014-01-01', '2014-01-15', 1, 'EPL_02_Viewing_Summary_3';
execute EPL_1_Extract_Viewing '2014-01-16', '2014-01-31', 1, 'EPL_02_Viewing_Summary_4';

execute EPL_1_Extract_Viewing '2014-02-01', '2014-02-14', 1, 'EPL_02_Viewing_Summary_4';
execute EPL_1_Extract_Viewing '2014-02-15', '2014-02-28', 1, 'EPL_02_Viewing_Summary_4';

  -- Assemble data
execute EPL_1_Extract_Viewing_Assemble '1';
execute EPL_1_Extract_Viewing_Assemble '2';
execute EPL_1_Extract_Viewing_Assemble '3';
execute EPL_1_Extract_Viewing_Assemble '4';

-- select Viewing_Day, count(*) as Cnt from EPL_02_Viewing_Summary group by Viewing_Day order by Viewing_Day;

-- truncate table EPL_02_Viewing_Summary_1;
-- truncate table EPL_02_Viewing_Summary_2;
-- truncate table EPL_02_Viewing_Summary_3;
-- truncate table EPL_02_Viewing_Summary_4;


  -- ##############################################################################################################
  -- Run extract - period 2 (Feb '13 - Jul '13)
execute EPL_1_Extract_Viewing '2013-02-01', '2013-02-14', 2, 'EPL_02_Viewing_Summary_4';
execute EPL_1_Extract_Viewing '2013-02-15', '2013-02-28', 2, 'EPL_02_Viewing_Summary_4';

execute EPL_1_Extract_Viewing '2013-03-01', '2013-03-15', 2, 'EPL_02_Viewing_Summary_1';
execute EPL_1_Extract_Viewing '2013-03-16', '2013-03-31', 2, 'EPL_02_Viewing_Summary_1';

execute EPL_1_Extract_Viewing '2013-04-01', '2013-04-15', 2, 'EPL_02_Viewing_Summary_2';
execute EPL_1_Extract_Viewing '2013-04-16', '2013-04-30', 2, 'EPL_02_Viewing_Summary_2';

execute EPL_1_Extract_Viewing '2013-05-01', '2013-05-15', 2, 'EPL_02_Viewing_Summary_3';
execute EPL_1_Extract_Viewing '2013-05-16', '2013-05-31', 2, 'EPL_02_Viewing_Summary_3';

execute EPL_1_Extract_Viewing '2013-06-01', '2013-06-15', 2, 'EPL_02_Viewing_Summary_4';
execute EPL_1_Extract_Viewing '2013-06-16', '2013-06-30', 2, 'EPL_02_Viewing_Summary_4';

execute EPL_1_Extract_Viewing '2013-07-01', '2013-07-15', 2, 'EPL_02_Viewing_Summary_1';
execute EPL_1_Extract_Viewing '2013-07-16', '2013-07-31', 2, 'EPL_02_Viewing_Summary_2';

-- Assemble data
execute EPL_1_Extract_Viewing_Assemble '1';
execute EPL_1_Extract_Viewing_Assemble '2';
execute EPL_1_Extract_Viewing_Assemble '3';
execute EPL_1_Extract_Viewing_Assemble '4';
-- truncate table EPL_02_Viewing_Summary_1;
-- truncate table EPL_02_Viewing_Summary_2;
-- truncate table EPL_02_Viewing_Summary_3;
-- truncate table EPL_02_Viewing_Summary_4;


  -- ##############################################################################################################
  -- ##### Append number of days                                                                              #####
  -- ##############################################################################################################
update EPL_02_Viewing_Summary base
   set base.Days_Data_Available = det.Days_Data_Available
  from (select
              Period,
              Account_Number,
              max(Days_Data_Available) as Days_Data_Available
          from EPL_01_Universe
         group by Period, Account_Number) det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period;
commit;


  -- ##############################################################################################################
  -- ##### Check data                                                                                         #####
  -- ##############################################################################################################
  -- Basic counts
select Sports_Genre_Flag   , count(*) as Cnr  from EPL_02_Viewing_Summary group by Sports_Genre_Flag   order by 1;
select Sports_Channel_Type , count(*) as Cnr  from EPL_02_Viewing_Summary group by Sports_Channel_Type order by 1;
select Pay_TV_Type         , count(*) as Cnr  from EPL_02_Viewing_Summary group by Pay_TV_Type         order by 1;
select Viewing_Type        , count(*) as Cnr  from EPL_02_Viewing_Summary group by Viewing_Type        order by 1;
select Sky_Exclusive_Channel_Type         , count(*) as Cnr  from EPL_02_Viewing_Summary group by Sky_Exclusive_Channel_Type          order by 1;
select Channel             , count(*) as Cnr  from EPL_02_Viewing_Summary group by Channel             order by 1;
select Programme           , count(*) as Cnr  from EPL_02_Viewing_Summary group by Programme           order by 1;
select EPL_Pack            , count(*) as Cnr  from EPL_02_Viewing_Summary group by EPL_Pack            order by 1;


  -- Check if any programmes are missing
select
      Dk_Programme_Instance_Dim,
      Broadcast_Start_Time_UTC,
      Broadcast_End_Time_UTC,
      Broadcast_Duration,
      Channel,
      Programme,
      EPL_Pack
  from EPL_01_EPG
 where Dk_Programme_Instance_Dim not in (select distinct
                                              Dk_Programme_Instance_Dim
                                           from EPL_02_Viewing_Summary
                                          where Service_Key > 0)
 order by Broadcast_Start_Time_UTC;


  -- Get list of programmes with no data
select
      a.Channel,
      a.Programme,
      a.EPL_Pack,
      a.Broadcast_Start_Time_UTC,
      a.Broadcast_End_Time_UTC,
      a.Broadcast_Duration,
      count(distinct Account_Number) as Accounts_Volume
  from EPL_01_EPG a
          left join EPL_02_Viewing_Summary b on a.Dk_Programme_Instance_Dim = b.Dk_Programme_Instance_Dim
                                            and b.Service_Key > 0
 where a.Broadcast_Start_Time_UTC <= '2013-10-31 23:59:59'
 group by
      a.Channel,
      a.Programme,
      a.EPL_Pack,
      a.Broadcast_Start_Time_UTC,
      a.Broadcast_End_Time_UTC,
      a.Broadcast_Duration
  order by a.Broadcast_Start_Time_UTC;



  -- ##############################################################################################################
  -- ##############################################################################################################












