/*###############################################################################
# Created on:   26/05/2014
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
# 26/05/2014  SBE   Initial version
#
###############################################################################*/


  -- ##############################################################################################################
  -- ##### Get a list of daily accounts available within the period                                           #####
  -- ##############################################################################################################
if object_id('EPL_52_CL_Viewing_Summary') is not null then drop table EPL_52_CL_Viewing_Summary end if;
create table EPL_52_CL_Viewing_Summary (
    Pk_Identifier                           bigint            identity,

      -- Account
    Account_Number                          varchar(20)       null      default null,
    Period                                  tinyint           null      default 0,
    First_BT_Viewing                        date              null      default null,

      -- Viewing
    Viewing_Day                             date              null      default null,
    Dk_Programme_Instance_Dim               bigint            null      default 0,      -- -1: other viewing
    Service_Key                             smallint          null      default null,
    Sports_Genre_Flag                       bit               null      default 0,
    Sports_Channel_Type                     tinyint           null      default 0,      -- 1: Sky Sports, 2: BT Sports, 3: ESPN, 4: All other
    Pay_TV_Type                             tinyint           null      default 0,      -- 0: FTA, 1: Pay package, 2: Pay Sports, 3: Pay Movies,
                                                                                        -- 4: 3rd party, 5: A'La Carte, 6: PPV/other
    Sky_Exclusive_Channel_Type              tinyint           null      default 0,      -- 0: All other, 1: Sky Atlantic (all), 2: Other Sky Exclusive channels
    Sky_Virgin_Exclusive_Channel_Type       tinyint           null      default 0,      -- 0: All other, 1: Sky/Virgin exclusive
    Viewing_Type                            tinyint           null      default 0,      -- 1: Live, 2: Live pause (up to 15 mins), 3: Playback
    Viewing_Duration                        bigint            null      default 0,

      -- EPG
    Broadcast_Start_Time_UTC                datetime          null      default null,
    Broadcast_End_Time_UTC                  datetime          null      default null,
    Broadcast_Duration                      bigint            null      default 0,
    Channel                                 varchar(50)       null      default null,
    Programme                               varchar(100)      null      default null,
    Programme_Instance_Name                 varchar(80)       null      default null,
    Live_Game_Flag                          bit               null      default null,
    EPL_Pack                                varchar(30)       null      default null,

    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name()
);

create        hg   index idx01 on EPL_52_CL_Viewing_Summary(Account_Number);
create        date index idx02 on EPL_52_CL_Viewing_Summary(First_BT_Viewing);
create        date index idx03 on EPL_52_CL_Viewing_Summary(Viewing_Day);
create        hg   index idx04 on EPL_52_CL_Viewing_Summary(Dk_Programme_Instance_Dim);
create        hg   index idx05 on EPL_52_CL_Viewing_Summary(Service_Key);
create        hg   index idx06 on EPL_52_CL_Viewing_Summary(Programme_Instance_Name);


if object_id('EPL_50_Extract_CL_Viewing') is not null then drop procedure EPL_50_Extract_CL_Viewing end if;
create procedure EPL_50_Extract_CL_Viewing
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
                             (Account_Number, Period, First_BT_Viewing, Viewing_Day, Dk_Programme_Instance_Dim, Service_Key,
                              Sports_Genre_Flag, Sports_Channel_Type, Pay_TV_Type, Sky_Exclusive_Channel_Type, Sky_Virgin_Exclusive_Channel_Type,
                              Viewing_Type, Viewing_Duration, Broadcast_Start_Time_UTC, Broadcast_End_Time_UTC, Broadcast_Duration,
                              Channel, Programme, Programme_Instance_Name, Live_Game_Flag, EPL_Pack)
                      select
                            un.Account_Number,
                            ' || @parPeriod || ',
                            max(un.First_BT_Viewing),
                            un.Data_Day,                                            -- Viewing_Day
                            case
                              when epg.Dk_Programme_Instance_Dim is null then -1
                                else epg.Dk_Programme_Instance_Dim
                            end as xProg_Instance_Dim,                              -- Dk_Programme_Instance_Dim
                            case
                              when epg.Dk_Programme_Instance_Dim is null then -1
                                else epg.Service_Key
                            end as xService_Key,                                    -- Service_Key
                            case
                              when vw.Genre_Description = ''Sports'' then 1
                                else 0
                            end as xSports_Genre,                                   -- Sports_Genre_Flag

                              -- 1: Sky Sports, 2: BT Sports, 3: ESPN, 4: All other
                            case
                              when vw.Service_Key in (1301, 1701, 4002, 1302, 4081, 1306, 3835,
                                                      1333, 4022, 1322, 4026, 1302, 4081) then 1              -- Sky Sports 1/2/F1/3/4/Ashes
                              when vw.Service_Key in (3625, 3661, 3627, 3663) then 2                          -- BT Sports 1/2
                              when vw.Service_Key in (3141, 4040) then 3                                      -- ESPN
                                else 4
                            end as xSports_Channel_Type,                            -- Sports_Channel_Type

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
                            end as xPay_TV_Type,                                    -- Pay_TV_Type


                              -- 0: All other, 1: Sky Atlantic (all), 2: Other Sky Exclusive channels
                            case
                              when vw.Service_Key in (
                                                      1412,       -- Sky Atlantic (SD)
                                                      4053,       -- Sky Atlantic (HD)
                                                      1414,       -- Sky Atlantic (HD)
                                                      1413        -- Sky Atlantic (HD)
                                                     ) then 1

                              when vw.Service_Key in (
                                                      4088,       -- Crime/CI (HD)
                                                      4071,       -- Disney Channel (HD)
                                                      4073,       -- Disney Junior (HD)
                                                      4070,       -- Disney XD (HD)
                                                      4028,       -- E! HD (HD)
                                                      4086,       -- History (HD)
                                                      3920,       -- Lifetime HD (HD)
                                                      4043,       -- More4 (HD)
                                                      4050,       -- Sky News (HD)
                                                      4049,       -- Sky Sports News (HD)
                                                      5300,       -- Star Plus (HD)
                                                      4080        -- Universal (HD)
                                                     ) then 2

                                else 0
                            end as xSky_Exclusive_Channel_Type,                     -- Sky_Exclusive_Channel_Type


                              -- 0: All other, 1: Sky/Virgin exclusive
                            case
                              when vw.Service_Key in (
                                                      1314,       -- Sky Sports News
                                                      4049,       -- Sky Sports News
                                                      1752,       -- Sky Arts 1 (SD)
                                                      4063,       -- Sky Arts 1 (HD)
                                                      1753,       -- Sky Arts 2 (SD)
                                                      4064,       -- Sky Arts 2 (HD)
                                                      2201,       -- Sky Living (SD)
                                                      4066,       -- Sky Living (HD)
                                                      2205,       -- Sky Living+1 (SD)
                                                      1404,       -- Sky News (SD)
                                                      4050,       -- Sky News (HD)
                                                      1402,       -- Sky1 (SD)
                                                      4061,       -- Sky1 (HD)
                                                      1403,       -- Sky1+1 (SD)
                                                      1833        -- Sky2 (SD)
                                                     ) then 1
                                else 0
                            end as xSky_Virgin_Exclusive_Channel_Type,              -- Sky_Virgin_Exclusive_Channel_Type

                              -- 1: Live, 2: Live pause (up to 15 mins), 3: Playback
                            case
                              when vw.live_recorded = ''LIVE'' then 1
                              when vw.time_in_seconds_since_recording <= 15 * 60 then 2
                                else 3
                            end as xViewing_Type,                                   -- Viewing_Type

                            sum(case
                                  when vw.capped_partial_flag = 1 then datediff(second, vw.instance_start_date_time_utc, vw.capping_end_date_time_utc)
                                    else datediff(second, vw.instance_start_date_time_utc, vw.instance_end_date_time_utc)
                                end) as xViewing_Duration,                          -- Viewing_Duration
                            max(epg.Broadcast_Start_Time_UTC),
                            max(epg.Broadcast_End_Time_UTC),
                            max(epg.Broadcast_Duration),
                            max(epg.Channel),
                            max(epg.Programme),
                            max(case
                                  when epg.Dk_Programme_Instance_Dim is null then null
                                    else epg.Programme || '' ['' || dateformat(epg.Broadcast_Start_Time_UTC, ''yyyy-mm-dd @hh:mm'') || '']''
                                end),
                            max(epg.Live_Game_Flag),
                            max(epg.EPL_Pack)

                        from EPL_01_Universe un,
                             sk_prod.sk_prod.vespa_dp_prog_viewed_' || @varTableSuffix || ' vw
                                left join EPL_50_CL_EPG epg                                                  on vw.Dk_Programme_Instance_Dim = epg.Dk_Programme_Instance_Dim
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
                         and vw.instance_start_date_time_utc < vw.instance_end_date_time_utc        -- Remove 0sec instances
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
                         --and vw.dk_event_start_datehour_dim between 2013080100 and 2013080123

                       group by
                            un.Account_Number,
                            un.Data_Day,
                            xProg_Instance_Dim,
                            xService_Key,
                            xSports_Genre,
                            xSports_Channel_Type,
                            xPay_TV_Type,
                            xSky_Exclusive_Channel_Type,
                            xSky_Virgin_Exclusive_Channel_Type,
                            xViewing_Type
                      commit

                      execute logger_add_event 0, 0, ''Data has been processed'', @@rowcount
                    '

      execute(@varSQL)


end;


if object_id('EPL_50_Extract_CL_Viewing_Assemble') is not null then drop procedure EPL_50_Extract_CL_Viewing_Assemble end if;
create procedure EPL_50_Extract_CL_Viewing_Assemble
      @parSrcTableId            varchar(50)
as
begin
      declare @varSQL                         varchar(25000)

      execute logger_add_event 0, 0, '##### Assembling viewing data #####', null
      execute logger_add_event 0, 0, 'Source table used: EPL_52_CL_Viewing_Summary_' || @parSrcTableId, null
      execute logger_add_event 0, 0, 'Destination table used: EPL_52_CL_Viewing_Summary', null


      set @varSQL = '
                      insert into EPL_52_CL_Viewing_Summary
                            (Account_Number, Period, First_BT_Viewing, Viewing_Day, Dk_Programme_Instance_Dim, Service_Key, Sports_Genre_Flag, Sports_Channel_Type,
                             Pay_TV_Type, Sky_Exclusive_Channel_Type, Sky_Virgin_Exclusive_Channel_Type, Viewing_Type, Viewing_Duration, Broadcast_Start_Time_UTC,
                             Broadcast_End_Time_UTC, Broadcast_Duration, Channel, Programme, Programme_Instance_Name, Live_Game_Flag, EPL_Pack)
                        select
                              Account_Number, Period, First_BT_Viewing, Viewing_Day, Dk_Programme_Instance_Dim, Service_Key, Sports_Genre_Flag, Sports_Channel_Type,
                              Pay_TV_Type, Sky_Exclusive_Channel_Type, Sky_Virgin_Exclusive_Channel_Type, Viewing_Type, Viewing_Duration, Broadcast_Start_Time_UTC,
                              Broadcast_End_Time_UTC, Broadcast_Duration, Channel, Programme, Programme_Instance_Name, Live_Game_Flag, EPL_Pack
                         from EPL_52_CL_Viewing_Summary_' || @parSrcTableId || '

                      commit
                      execute logger_add_event 0, 0, ''Data has been uploaded to the main table'', @@rowcount
                    '

      execute(@varSQL)


end;



  -- ##############################################################################################################
  -- Run extract - period 1 (Aug '13 - Feb '14)
execute EPL_50_Extract_CL_Viewing '2013-08-01', '2013-08-15', 1, 'EPL_52_CL_Viewing_Summary_1';
execute EPL_50_Extract_CL_Viewing '2013-08-16', '2013-08-31', 1, 'EPL_52_CL_Viewing_Summary_1';

execute EPL_50_Extract_CL_Viewing '2013-09-01', '2013-09-15', 1, 'EPL_52_CL_Viewing_Summary_1';
execute EPL_50_Extract_CL_Viewing '2013-09-16', '2013-09-30', 1, 'EPL_52_CL_Viewing_Summary_1';

execute EPL_50_Extract_CL_Viewing '2013-10-01', '2013-10-15', 1, 'EPL_52_CL_Viewing_Summary_2';
execute EPL_50_Extract_CL_Viewing '2013-10-16', '2013-10-31', 1, 'EPL_52_CL_Viewing_Summary_2';

execute EPL_50_Extract_CL_Viewing '2013-11-01', '2013-11-15', 1, 'EPL_52_CL_Viewing_Summary_2';
execute EPL_50_Extract_CL_Viewing '2013-11-16', '2013-11-30', 1, 'EPL_52_CL_Viewing_Summary_2';

execute EPL_50_Extract_CL_Viewing '2013-12-01', '2013-12-15', 1, 'EPL_52_CL_Viewing_Summary_3';
execute EPL_50_Extract_CL_Viewing '2013-12-16', '2013-12-31', 1, 'EPL_52_CL_Viewing_Summary_3';

execute EPL_50_Extract_CL_Viewing '2014-01-01', '2014-01-15', 1, 'EPL_52_CL_Viewing_Summary_3';
execute EPL_50_Extract_CL_Viewing '2014-01-16', '2014-01-31', 1, 'EPL_52_CL_Viewing_Summary_4';

execute EPL_50_Extract_CL_Viewing '2014-02-01', '2014-02-14', 1, 'EPL_52_CL_Viewing_Summary_4';
execute EPL_50_Extract_CL_Viewing '2014-02-15', '2014-02-28', 1, 'EPL_52_CL_Viewing_Summary_4';

  -- Assemble data
execute EPL_50_Extract_CL_Viewing_Assemble '1';
execute EPL_50_Extract_CL_Viewing_Assemble '2';
execute EPL_50_Extract_CL_Viewing_Assemble '3';
execute EPL_50_Extract_CL_Viewing_Assemble '4';


  -- Reprocessing certain days since EPG data has been updated
-- execute EPL_50_Extract_CL_Viewing '2013-10-23', '2013-10-23', 1, 'EPL_52_CL_Viewing_Summary';
-- execute EPL_50_Extract_CL_Viewing '2014-02-19', '2014-02-19', 1, 'EPL_52_CL_Viewing_Summary';
-- execute EPL_50_Extract_CL_Viewing '2014-02-26', '2014-02-26', 1, 'EPL_52_CL_Viewing_Summary';

execute EPL_50_Extract_CL_Viewing '2014-03-01', '2014-03-15', 4, 'EPL_52_CL_Viewing_Summary_1';
execute EPL_50_Extract_CL_Viewing '2014-03-16', '2014-03-31', 4, 'EPL_52_CL_Viewing_Summary_1';

execute EPL_50_Extract_CL_Viewing '2014-04-01', '2014-04-15', 4, 'EPL_52_CL_Viewing_Summary_2';
execute EPL_50_Extract_CL_Viewing '2014-04-16', '2014-04-30', 4, 'EPL_52_CL_Viewing_Summary_2';

execute EPL_50_Extract_CL_Viewing '2014-05-01', '2014-05-15', 4, 'EPL_52_CL_Viewing_Summary_3';
execute EPL_50_Extract_CL_Viewing '2014-05-16', '2014-05-31', 4, 'EPL_52_CL_Viewing_Summary_3';


  -- Assemble data
execute EPL_50_Extract_CL_Viewing_Assemble '1';
execute EPL_50_Extract_CL_Viewing_Assemble '2';
execute EPL_50_Extract_CL_Viewing_Assemble '3';
-- truncate table EPL_52_CL_Viewing_Summary_1;
-- truncate table EPL_52_CL_Viewing_Summary_2;
-- truncate table EPL_52_CL_Viewing_Summary_3;
-- truncate table EPL_52_CL_Viewing_Summary_4;


select Viewing_Day, count(*) from EPL_52_CL_Viewing_Summary group by Viewing_Day order by Viewing_Day;



  -- ##############################################################################################################
  -- ##### Check data                                                                                         #####
  -- ##############################################################################################################
  -- Basic counts
select Sports_Genre_Flag   , count(*) as Cnr  from EPL_52_CL_Viewing_Summary group by Sports_Genre_Flag   order by 1;
select Sports_Channel_Type , count(*) as Cnr  from EPL_52_CL_Viewing_Summary group by Sports_Channel_Type order by 1;
select Pay_TV_Type         , count(*) as Cnr  from EPL_52_CL_Viewing_Summary group by Pay_TV_Type         order by 1;
select Viewing_Type        , count(*) as Cnr  from EPL_52_CL_Viewing_Summary group by Viewing_Type        order by 1;
select Sky_Exclusive_Channel_Type         , count(*) as Cnr  from EPL_52_CL_Viewing_Summary group by Sky_Exclusive_Channel_Type          order by 1;
select Sky_Virgin_Exclusive_Channel_Type  , count(*) as Cnr  from EPL_52_CL_Viewing_Summary group by Sky_Virgin_Exclusive_Channel_Type   order by 1;
select Channel             , count(*) as Cnr  from EPL_52_CL_Viewing_Summary group by Channel             order by 1;
select Programme           , count(*) as Cnr  from EPL_52_CL_Viewing_Summary group by Programme           order by 1;
select EPL_Pack            , count(*) as Cnr  from EPL_52_CL_Viewing_Summary group by EPL_Pack            order by 1;


  -- Get list of programmes with no data
select
      a.Channel,
      a.Programme,
      a.EPL_Pack,
      a.Broadcast_Start_Time_UTC,
      a.Broadcast_End_Time_UTC,
      a.Broadcast_Duration,
      count(distinct Account_Number) as Accounts_Volume,
      sum(Viewing_Duration) as Total_Viewing,
      cast(1.0 * Total_Viewing / Accounts_Volume / 60 as decimal(10, 1)) as Avg_Viewing,
      case when a.Channel = 'Sky Sports interactive' then 'YES' else '' end as Red_Buttom
  from EPL_50_CL_EPG a
          left join EPL_52_CL_Viewing_Summary b on a.Dk_Programme_Instance_Dim = b.Dk_Programme_Instance_Dim
                                            and b.Service_Key > 0
 group by
      a.Channel,
      a.Programme,
      a.EPL_Pack,
      a.Broadcast_Start_Time_UTC,
      a.Broadcast_End_Time_UTC,
      a.Broadcast_Duration
  order by a.Broadcast_Start_Time_UTC;


select
      a.Dk_Programme_Instance_Dim,
      a.Channel,
      a.Programme,
      a.EPL_Pack,
      a.Broadcast_Start_Time_UTC,
      a.Broadcast_End_Time_UTC,
      a.Broadcast_Duration,
      count(distinct Account_Number) as Accounts_Volume,
      sum(Viewing_Duration) as Total_Viewing,
      cast(1.0 * Total_Viewing / Accounts_Volume / 60 as decimal(10, 1)) as Avg_Viewing,
      case when a.Channel = 'Sky Sports interactive' then 'YES' else '' end as Red_Buttom
  from EPL_50_CL_EPG a
          left join EPL_52_CL_Viewing_Summary b on a.Dk_Programme_Instance_Dim = b.Dk_Programme_Instance_Dim
                                            and b.Service_Key > 0
 group by
      a.Dk_Programme_Instance_Dim,
      a.Channel,
      a.Programme,
      a.EPL_Pack,
      a.Broadcast_Start_Time_UTC,
      a.Broadcast_End_Time_UTC,
      a.Broadcast_Duration
  order by a.Broadcast_Start_Time_UTC;



  -- ##############################################################################################################
  -- ##############################################################################################################














