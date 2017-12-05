/*###############################################################################
# Created on:   29/05/2014
# Created by:   Sebastian Bednaszynski(SBE)
# Description:  EPL rights project - Metric calculations (SOC, SOV etc.) - Sky Sports only
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 10/03/2014  SBE   Initial version
#
###############################################################################*/


  -- ##############################################################################################################
  -- ##### Create structures                                                                                  #####
  -- ##############################################################################################################
if object_id('EPL_03_SOCs_Sky_Sports_Only') is not null then drop table EPL_03_SOCs_Sky_Sports_Only end if;
create table EPL_03_SOCs_Sky_Sports_Only (
    Pk_Identifier                           bigint            identity,

      -- Account
    Account_Number                          varchar(20)       null      default null,
    Period                                  tinyint           null      default 0,

      -- Channel/Programme
    Broadcast_Date                          date              null      default null,
    Channel                                 varchar(20)       null      default null,
    Programme                               varchar(100)      null      default null,
    Programme_Instance_Name                 varchar(80)       null      default null,
    Kick_Off_Time                           time              null      default null,
    Day_Of_Week                             varchar(10)       null      default null,
    Live_Game_Flag                          bit               null      default 0,
    EPL_Pack                                varchar(30)       null      default null,

      -- Viewing
    Content_Available                       tinyint           null default 0,
    Content_Watched                         bit               null default 0,
    Calculated_SOC                          decimal(15, 6)    null default 0,

    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name()
);
create        hg   index idx01 on EPL_03_SOCs_Sky_Sports_Only(Account_Number);
create        lf   index idx02 on EPL_03_SOCs_Sky_Sports_Only(Period);
create        date index idx03 on EPL_03_SOCs_Sky_Sports_Only(Broadcast_Date);
create        hg   index idx04 on EPL_03_SOCs_Sky_Sports_Only(Programme_Instance_Name);
grant select on EPL_03_SOCs_Sky_Sports_Only to vespa_group_low_security;


if object_id('EPL_03_SOCs_Sky_Sports_Only_Summaries') is not null then drop table EPL_03_SOCs_Sky_Sports_Only_Summaries end if;
create table EPL_03_SOCs_Sky_Sports_Only_Summaries (
    Pk_Identifier                           bigint            identity,

      -- Account
    Account_Number                          varchar(20)       null      default null,
    Period                                  tinyint           null      default 0,

    Metric                                  varchar(100)      null      default null,
    Category                                varchar(100)      null      default null,

    Content_Available                       bigint            null default 0,
    Content_Watched                         bigint            null default 0,
    Calculated_SOC                          decimal(15, 6)    null default 0,

    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name()
);
create        hg   index idx01 on EPL_03_SOCs_Sky_Sports_Only_Summaries(Account_Number);
create        lf   index idx02 on EPL_03_SOCs_Sky_Sports_Only_Summaries(Period);
create        lf   index idx03 on EPL_03_SOCs_Sky_Sports_Only_Summaries(Metric);
grant select on EPL_03_SOCs_Sky_Sports_Only_Summaries to vespa_group_low_security;


if object_id('EPL_03_SOVs_Sky_Sports_Only') is not null then drop table EPL_03_SOVs_Sky_Sports_Only end if;
create table EPL_03_SOVs_Sky_Sports_Only (
    Pk_Identifier                           bigint            identity,

      -- Account
    Account_Number                          varchar(20)       null      default null,
    Period                                  tinyint           null      default 0,

    Metric                                  varchar(100)      null      default null,
    Category                                varchar(100)      null      default null,

    Category_Consumption                    decimal(15, 6)    null default 0,
    Total_Consumption                       decimal(15, 6)    null default 0,
    Calculated_SOV                          decimal(15, 6)    null default 0,

    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name()
);
create        hg   index idx01 on EPL_03_SOVs_Sky_Sports_Only(Account_Number);
create        lf   index idx02 on EPL_03_SOVs_Sky_Sports_Only(Period);
create        lf   index idx03 on EPL_03_SOVs_Sky_Sports_Only(Metric);
grant select on EPL_03_SOVs_Sky_Sports_Only to vespa_group_low_security;



  -- ##############################################################################################################
  -- ##### Share of Content - preparation                                                                     #####
  -- ##############################################################################################################
  -- Get content programmes available to each account
-- truncate table EPL_03_SOCs_Sky_Sports_Only;
insert into EPL_03_SOCs_Sky_Sports_Only
      (Account_Number, Period, Broadcast_Date, Channel, Programme, Programme_Instance_Name, Kick_Off_Time,
       Day_Of_Week, Live_Game_Flag, EPL_Pack, Content_Available)
  select
        a.Account_Number,
        a.Period,
        b.Broadcast_Date,
        b.Channel,
        b.Programme,
        b.Programme_Instance_Name,
        b.Kick_Off_Time,
        b.Day_Of_Week,
        b.Live_Game_Flag,
        b.EPL_Pack,
        1
    from EPL_01_Universe a,
         EPL_01_EPG b
   where a.Data_Day = b.Broadcast_Date
     and a.Valid_Account_Flag = 1
     and a.DTV_Flag = 1
     and (
            ( a.Sky_Sports_Flag = 1 and b.Channel in ('Sky Sports 1', 'Sky Sports 2', 'Sky Sports 3', 'Sky Sports 4', '3D channel') )   -- Sky Sports channels programmes only, if user eligible
         )
   group by a.Account_Number, a.Period, b.Broadcast_Date, b.Channel, b.Programme, b.Programme_Instance_Name,
            b.Kick_Off_Time, b.Day_Of_Week, b.Live_Game_Flag, b.EPL_Pack;
commit;


  -- Attribute viewing
update EPL_03_SOCs_Sky_Sports_Only base
   set base.Content_Watched = 1
  from (select
              Account_Number,
              Period,
              Programme_Instance_Name
          from EPL_02_Viewing_Summary
         where Dk_Programme_Instance_Dim > -1                                     -- EPL live/non-live content only
           and Viewing_Type in (1, 2)                                             -- Live & Live pause of up to 15 minutes
         group by Account_Number, Period, Programme_Instance_Name
        having sum(Viewing_Duration) >= (15 * 60) ) det                           -- Aggregated viewing of at least 15 minutes
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and base.Programme_Instance_Name = det.Programme_Instance_Name;
commit;


  -- ##############################################################################################################
  -- ##### Generate SOCs                                                                                      #####
  -- ##############################################################################################################
  -- OVERALL - live
delete from EPL_03_SOCs_Sky_Sports_Only_Summaries
 where Metric in ('Live games - overall', 'Non-live programmes - overall');
commit;

insert into EPL_03_SOCs_Sky_Sports_Only_Summaries
      (Account_Number, Period, Metric, Category, Content_Available, Content_Watched, Calculated_SOC)
  select
        Account_Number,
        Period,
        case
          when Live_Game_Flag = 1 then 'Live games - overall'
            else 'Non-live programmes - overall'
        end                                           as xMetric,
        '(all)'                                       as xCategory,
        sum(Content_Available)                        as xContent_Available,
        sum(Content_Watched)                          as xContent_Watched,
        case
          when xContent_Available = 0 then null
            else 1.0 * xContent_Watched / xContent_Available
        end                                           as xCalculated_SOC
    from EPL_03_SOCs_Sky_Sports_Only
   where Live_Game_Flag = 1
   group by Account_Number, Period, xMetric, xCategory;
commit;



  -- LIVE - BY PACK
delete from EPL_03_SOCs_Sky_Sports_Only_Summaries
 where Metric = 'Live games - by EPL pack';
commit;

insert into EPL_03_SOCs_Sky_Sports_Only_Summaries
      (Account_Number, Period, Metric, Category, Content_Available, Content_Watched, Calculated_SOC)
  select
        Account_Number,
        Period,
        'Live games - by EPL pack'                    as xMetric,
        EPL_Pack                                      as xCategory,
        sum(Content_Available)                        as xContent_Available,
        sum(Content_Watched)                          as xContent_Watched,
        case
          when xContent_Available = 0 then null
            else 1.0 * xContent_Watched / xContent_Available
        end                                           as xCalculated_SOC
    from EPL_03_SOCs_Sky_Sports_Only
   where Live_Game_Flag = 1                                                   -- Live EPL games only
   group by Account_Number, Period, xMetric, xCategory;
commit;


  -- Get results
select
      case
        when Period = 1 then '2) Aug ''13 - Feb ''14'
        when Period = 2 then '1) Feb ''13 - Jul ''13'
          else '???'
      end as xPeriod,
      Metric,
      Category,
      case
        when Calculated_SOC = 0 then '01) DNW'
        when ceil(Calculated_SOC * 100) <=   5 then '02) 1-5%'
        when ceil(Calculated_SOC * 100) <=  10 then '03) 6-10%'
        when ceil(Calculated_SOC * 100) <=  15 then '04) 11-15%'
        when ceil(Calculated_SOC * 100) <=  20 then '05) 16-20%'
        when ceil(Calculated_SOC * 100) <=  25 then '06) 21-25%'
        when ceil(Calculated_SOC * 100) <=  30 then '07) 26-30%'
        when ceil(Calculated_SOC * 100) <=  35 then '08) 31-35%'
        when ceil(Calculated_SOC * 100) <=  40 then '09) 36-40%'
        when ceil(Calculated_SOC * 100) <=  45 then '10) 41-45%'
        when ceil(Calculated_SOC * 100) <=  50 then '11) 46-50%'
        when ceil(Calculated_SOC * 100) <=  55 then '12) 51-55%'
        when ceil(Calculated_SOC * 100) <=  60 then '13) 56-60%'
        when ceil(Calculated_SOC * 100) <=  65 then '14) 61-65%'
        when ceil(Calculated_SOC * 100) <=  70 then '15) 66-70%'
        when ceil(Calculated_SOC * 100) <=  75 then '16) 71-75%'
        when ceil(Calculated_SOC * 100) <=  80 then '17) 76-80%'
        when ceil(Calculated_SOC * 100) <=  85 then '18) 81-85%'
        when ceil(Calculated_SOC * 100) <=  90 then '19) 86-90%'
        when ceil(Calculated_SOC * 100) <=  95 then '20) 91-95%'
        when ceil(Calculated_SOC * 100) <= 100 then '21) 96-100%'
          else '99) ???'
      end as SOC_1,
      case
        when Calculated_SOC = 0 then '01) DNW'
        when ceil(Calculated_SOC * 100) <=  10 then '02) 1-10%'
        when ceil(Calculated_SOC * 100) <=  20 then '03) 11-20%'
        when ceil(Calculated_SOC * 100) <=  30 then '04) 21-30%'
        when ceil(Calculated_SOC * 100) <=  40 then '05) 31-40%'
        when ceil(Calculated_SOC * 100) <=  50 then '06) 41-50%'
        when ceil(Calculated_SOC * 100) <=  60 then '07) 51-60%'
        when ceil(Calculated_SOC * 100) <=  70 then '08) 61-70%'
        when ceil(Calculated_SOC * 100) <=  80 then '09) 71-80%'
        when ceil(Calculated_SOC * 100) <=  90 then '10) 81-90%'
        when ceil(Calculated_SOC * 100) <= 100 then '11) 91-100%'
          else '99) ???'
      end as SOC_2,
      count(*) as Accts
  from EPL_03_SOCs_Sky_Sports_Only_Summaries
 group by
      xPeriod,
      Metric,
      Category,
      SOC_1,
      SOC_2;



  -- ##############################################################################################################
  -- ##### Share of Viewing                                                                                   #####
  -- ##############################################################################################################
  -- EPL SoSV
  --    ‘All live EPL matches on SS only (including live paused matches <= 15 mins
  --    from orig. broadcast time but excluding any other playback)’ / ‘All Sports genre viewing on SS
  --    (including live paused matches <= 15 mins from orig. broadcast time but excluding any other playback)’.
delete from EPL_03_SOVs_Sky_Sports_Only
 where Metric = 'EPL SoSV';
commit;

insert into EPL_03_SOVs_Sky_Sports_Only
      (Account_Number, Period, Metric, Category, Category_Consumption, Total_Consumption, Calculated_SOV)
  select
        Account_Number,
        Period,
        'EPL SoSV' as Metric,
        '(all)' as Category,
        sum(case
              when Live_Game_Flag = 1 then Viewing_Duration
                else 0
            end) as xCategory_Consumption,
        sum(case
              when Sports_Genre_Flag = 1 then Viewing_Duration
                else 0
            end) as xTotal_Consumption,
        case
          when xTotal_Consumption > 0 then 1.0 * xCategory_Consumption / xTotal_Consumption
            else 0
        end as xCalculated_SOV
    from (select
                Account_Number,
                Period,
                Live_Game_Flag,
                Sports_Genre_Flag,
                Viewing_Duration
            from EPL_02_Viewing_Summary
           where Sports_Channel_Type in (1)                               -- 1: Sky Sports, 2: BT Sports, 3: ESPN, 4: All other
             and Viewing_Type in (1, 2)) det                              -- 1: Live, 2: Live pause (up to 15 mins), 3: Playback
   group by Account_Number, Period;
commit;


  --  Sports SoV
  --    ‘All Sports genre viewing on SS only (including live paused matches <= 15 mins from orig.
  --    broadcast time but excluding any other playback)’ / ‘All Pay TV viewing (including all playback)’
delete from EPL_03_SOVs_Sky_Sports_Only
 where Metric = 'Sports SoV';
commit;

insert into EPL_03_SOVs_Sky_Sports_Only
      (Account_Number, Period, Metric, Category, Category_Consumption, Total_Consumption, Calculated_SOV)
  select
        Account_Number,
        Period,
        'Sports SoV' as Metric,
        '(all)' as Category,
        sum(case
              when Sports_Channel_Type in (1) and Viewing_Type in (1, 2)           -- Sports_Channel_Type - 1: Sky Sports, 2: BT Sports, 3: ESPN, 4: All other
                                                                                   -- Viewing_Type  -       1: Live, 2: Live pause (up to 15 mins), 3: Playback
                   and Sports_Genre_Flag = 1 then Viewing_Duration
                else 0
            end) as xCategory_Consumption,
        sum(case
              when Pay_TV_Type in (1, 2, 3, 4, 5) then Viewing_Duration           -- Pay_TV_Type    - 0: FTA, 1: Pay package, 2: Pay Sports, 3: Pay Movies,
                                                                                  --                  4: 3rd party, 5: A'La Carte, 6: PPV/other
                else 0
            end) as xTotal_Consumption,
        case
          when xTotal_Consumption > 0 then 1.0 * xCategory_Consumption / xTotal_Consumption
            else 0
        end as xCalculated_SOV
    from EPL_02_Viewing_Summary det
   group by Account_Number, Period;
commit;



  -- Get results
select
      xPeriod,
      Metric,
      Category,
      case
        when Calculated_SOV = 0 then '01) DNW'
        when ceil(Calculated_SOV * 100) <=   5 then '02) 1-5%'
        when ceil(Calculated_SOV * 100) <=  10 then '03) 6-10%'
        when ceil(Calculated_SOV * 100) <=  15 then '04) 11-15%'
        when ceil(Calculated_SOV * 100) <=  20 then '05) 16-20%'
        when ceil(Calculated_SOV * 100) <=  25 then '06) 21-25%'
        when ceil(Calculated_SOV * 100) <=  30 then '07) 26-30%'
        when ceil(Calculated_SOV * 100) <=  35 then '08) 31-35%'
        when ceil(Calculated_SOV * 100) <=  40 then '09) 36-40%'
        when ceil(Calculated_SOV * 100) <=  45 then '10) 41-45%'
        when ceil(Calculated_SOV * 100) <=  50 then '11) 46-50%'
        when ceil(Calculated_SOV * 100) <=  55 then '12) 51-55%'
        when ceil(Calculated_SOV * 100) <=  60 then '13) 56-60%'
        when ceil(Calculated_SOV * 100) <=  65 then '14) 61-65%'
        when ceil(Calculated_SOV * 100) <=  70 then '15) 66-70%'
        when ceil(Calculated_SOV * 100) <=  75 then '16) 71-75%'
        when ceil(Calculated_SOV * 100) <=  80 then '17) 76-80%'
        when ceil(Calculated_SOV * 100) <=  85 then '18) 81-85%'
        when ceil(Calculated_SOV * 100) <=  90 then '19) 86-90%'
        when ceil(Calculated_SOV * 100) <=  95 then '20) 91-95%'
        when ceil(Calculated_SOV * 100) <= 100 then '21) 96-100%'
          else '99) ???'
      end as SOV_1,
      case
        when Calculated_SOV = 0 then '01) DNW'
        when ceil(Calculated_SOV * 100) <=  10 then '02) 1-10%'
        when ceil(Calculated_SOV * 100) <=  20 then '03) 11-20%'
        when ceil(Calculated_SOV * 100) <=  30 then '04) 21-30%'
        when ceil(Calculated_SOV * 100) <=  40 then '05) 31-40%'
        when ceil(Calculated_SOV * 100) <=  50 then '06) 41-50%'
        when ceil(Calculated_SOV * 100) <=  60 then '07) 51-60%'
        when ceil(Calculated_SOV * 100) <=  70 then '08) 61-70%'
        when ceil(Calculated_SOV * 100) <=  80 then '09) 71-80%'
        when ceil(Calculated_SOV * 100) <=  90 then '10) 81-90%'
        when ceil(Calculated_SOV * 100) <= 100 then '11) 91-100%'
          else '99) ???'
      end as SOV_2,
      count(*) as Accts,
      sum(xTotal_Category_Consumption) as Total_Category_Consumption,                           -- Average of account daily average
      sum(xTotal_Consumption) as Total_Consumption                                              -- Average of account daily average
  from (select
              a.Period,
              a.Account_Number,
              case
                when a.Period = 1 then '2) Aug ''13 - Feb ''14'
                when a.Period = 2 then '1) Feb ''13 - Jul ''13'
                when a.Period = 3 then '3) Feb ''13 - May ''13'
                  else '???'
              end as xPeriod,
              Metric,
              Category,
              Calculated_SOV,
              1.0 * Category_Consumption / Valid_Days as xTotal_Category_Consumption,
              1.0 * Total_Consumption / Valid_Days as xTotal_Consumption
          from EPL_03_SOVs_Sky_Sports_Only a,
              (select
                     a.Period,
                     a.Account_Number,
                     sum(a.valid_account_flag) as Valid_Days
                 from EPL_01_Universe a,
                      EPL_04_Profiling_Variables b
                where a.Account_Number = b.Account_Number
                  and a.Period = b.Period
                  and a.Period = 1
                  and b.Prem_Sports > 0
                group by a.Period, a.Account_Number) b
         where a.Account_Number = b.Account_Number
           and a.Period = b.Period) xx
 group by
      xPeriod,
      Metric,
      Category,
      SOV_1,
      SOV_2;



  -- ##############################################################################################################
  -- ##### Get single account engagement view                                                                 #####
  -- ##############################################################################################################
if object_id('EPL_04_Eng_Matrix_Sky_Sports_Only') is not null then drop table EPL_04_Eng_Matrix_Sky_Sports_Only end if;
create table EPL_04_Eng_Matrix_Sky_Sports_Only (
    Pk_Identifier                           bigint            identity,
    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name(),

      -- Account
    Account_Number                          varchar(20)       null      default null,
    Period                                  tinyint           null      default 0,
    Low_Content_Flag                        bit               null      default 0,

    Metric                                  varchar(50)       null      default null,
    Category                                varchar(50)       null      default null,

    EPL_SOC                                 varchar(30)       null      default 'Not calculated',
    EPL_SOC__Deciles                        varchar(15)       null      default 'Not calculated',
    EPL_SOC__Percentiles                    varchar(15)       null      default 'Not calculated',
    EPL_SOC__010_Groups                     varchar(15)       null      default 'Not calculated',
    EPL_SOC__100_Groups                     varchar(15)       null      default 'Not calculated',

    EPL_SoNLC                               varchar(30)       null      default 'Not calculated',
    EPL_SoNLC__Deciles                      varchar(15)       null      default 'Not calculated',
    EPL_SoNLC__Percentiles                  varchar(15)       null      default 'Not calculated',
    EPL_SoSV                                varchar(30)       null      default 'Not calculated',
    EPL_SoSV__Deciles                       varchar(15)       null      default 'Not calculated',
    EPL_SoSV__Percentiles                   varchar(15)       null      default 'Not calculated',
    Sport_SoV                               varchar(30)       null      default 'Not calculated',
    Sport_SoV__Deciles                      varchar(15)       null      default 'Not calculated',
    Sport_SoV__Percentiles                  varchar(15)       null      default 'Not calculated',
    Movies_SoV                              varchar(30)       null      default 'Not calculated',
    Movies_SoV__Deciles                     varchar(15)       null      default 'Not calculated',
    Movies_SoV__Percentiles                 varchar(15)       null      default 'Not calculated',

    EPL_SOC__Pack_Monday_Evening            varchar(30)       null      default 'Not calculated',
    EPL_SOC__Pack_Midweek_Evening           varchar(30)       null      default 'Not calculated',
    EPL_SOC__Pack_Saturday_Lunch            varchar(30)       null      default 'Not calculated',
    EPL_SOC__Pack_Saturday_Afternoon        varchar(30)       null      default 'Not calculated',
    EPL_SOC__Pack_Sunday_Early_Afternoon    varchar(30)       null      default 'Not calculated',
    EPL_SOC__Pack_Sunday_Late_Afternoon     varchar(30)       null      default 'Not calculated',

    Sky_Atlantic_SoV                        varchar(30)       null      default 'Not calculated',
    Sky_Atlantic_SoV__Deciles               varchar(15)       null      default 'Not calculated',
    Sky_Atlantic_SoV__Percentiles           varchar(15)       null      default 'Not calculated',
    Entertainment_Pack_SoV                  varchar(30)       null      default 'Not calculated',
    Entertainment_Pack_SoV__Deciles         varchar(15)       null      default 'Not calculated',
    Entertainment_Pack_SoV__Percentiles     varchar(15)       null      default 'Not calculated',
    Sky_Excl_Channels_SoV                   varchar(30)       null      default 'Not calculated',
    Sky_Excl_Channels_SoV__Deciles          varchar(15)       null      default 'Not calculated',
    Sky_Excl_Channels_SoV__Percentiles      varchar(15)       null      default 'Not calculated',
    Sky_Virgin_Excl_Channels_SoV            varchar(30)       null      default 'Not calculated',
    Sky_Virgin_Excl_Channels_SoV__Deciles   varchar(15)       null      default 'Not calculated',
    Sky_Virgin_Excl_Channels_SoV__Percentiles varchar(15)     null      default 'Not calculated',

    EPL_Games_Watched__Pack_Mon_Ev          smallint          null      default 0,
    EPL_Games_Watched__Pack_Mid_Ev          smallint          null      default 0,
    EPL_Games_Watched__Pack_Sat_Lunch       smallint          null      default 0,
    EPL_Games_Watched__Pack_Sat_Aft         smallint          null      default 0,
    EPL_Games_Watched__Pack_Sun_Early_Aft   smallint          null      default 0,
    EPL_Games_Watched__Pack_Sun_Late_Aft    smallint          null      default 0,
    EPL_Games_Watched__Overall              smallint          null      default 0,

    EPL_Pack_SOV__Monday_Evening            varchar(15)       null      default 'Not calculated',
    EPL_Pack_SOV__Midweek_Evening           varchar(15)       null      default 'Not calculated',
    EPL_Pack_SOV__Saturday_Lunch            varchar(15)       null      default 'Not calculated',
    EPL_Pack_SOV__Saturday_Afternoon        varchar(15)       null      default 'Not calculated',
    EPL_Pack_SOV__Sunday_Early_Afternoon    varchar(15)       null      default 'Not calculated',
    EPL_Pack_SOV__Sunday_Late_Afternoon     varchar(15)       null      default 'Not calculated'
);
create        hg   index idx01 on EPL_04_Eng_Matrix_Sky_Sports_Only(Account_Number);
create        lf   index idx02 on EPL_04_Eng_Matrix_Sky_Sports_Only(Period);
create        lf   index idx03 on EPL_04_Eng_Matrix_Sky_Sports_Only(Metric);
grant select on EPL_04_Eng_Matrix_Sky_Sports_Only to vespa_group_low_security;


  -- Overall level - base
insert into EPL_04_Eng_Matrix_Sky_Sports_Only
      (Account_Number, Period, Metric, Category)
  select
        a.Account_Number,
        a.Period,
        'Overall',
        '(all)'
    from EPL_01_Universe a
   where a.Period = 1
   group by a.Account_Number, a.Period
  having max(a.Valid_Account_Flag) = 1;
commit;


  -- Pack level - base
insert into EPL_04_Eng_Matrix_Sky_Sports_Only
      (Account_Number, Period, Metric, Category)
  select
        a.Account_Number,
        a.Period,
        'By EPL pack',
         b.EPL_Pack
    from (select
                Account_Number,
                Period
            from EPL_01_Universe
           where Period = 1
           group by Account_Number, Period
          having max(Valid_Account_Flag) = 1) a
         cross join
         (select
                EPL_Pack
            from EPL_01_EPG
           where Broadcast_Date >= '2013-08-01'
             and Live_Game_Flag = 1
           group by EPL_Pack) b;
commit;

-- select Period, Metric, Category, count(*) as Cnt, count(distinct Account_Number) as Cnt_Accts from EPL_04_Eng_Matrix_Sky_Sports_Only group by Period, Metric, Category order by Period, Metric, Category;



  -- ##############################################################################################################
  -- ##############################################################################################################
  -- Measures - OVERALL
  -- • The engagement thresholds for:
  --    o Share of Content (what proportion of all EPL matches does the household watch): H = >50%, M = 20-50% and L = 0.1-19.9%
  --    o Share of Sports Viewing (what proportion of their paid Sports viewing is to EPL): H = >50%, M = 20-50% and L = 0.1-19.9%
  --    o Share of Viewing (what proportion of their total paid Sky viewing is to paid Sports): HH = >20%, H = 10-20%, M = 5-9.9%, L = 0.1-4.9%


  --    o Share of Content (what proportion of all EPL matches does the household watch): H = >50%, M = 20-50% and L = 0.1-19.9%
update EPL_04_Eng_Matrix_Sky_Sports_Only base
   set base.Low_Content_Flag    = case
                                    when det.Content_Available <= 3 then 1                        -- Low content flag for EPL games available
                                      else 0
                                  end,
       base.EPL_SOC             = case
                                    when det.Calculated_SOC =  0    then 'Did not watch'
                                    when det.Calculated_SOC <  0.20 then 'Low'
                                    when det.Calculated_SOC <= 0.50 then 'Medium'
                                      else 'High'
                                  end
  from EPL_03_SOCs_Sky_Sports_Only_Summaries det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and (
        ( base.Metric = 'Overall' and det.Metric = 'Live games - overall' )
        or
        ( base.Metric = 'By EPL pack' and det.Metric = 'Live games - by EPL pack' )
       )
   and base.Category = det.Category;
commit;


  -- Add deciles
if object_id('EPL_tmp_Deciles_Percentiles') is not null then drop table EPL_tmp_Deciles_Percentiles end if;
select
      a.Account_Number,
      a.Period,
      a.Metric,
      a.Category,
      b.Calculated_SOC,
      ntile(10) over (partition by a.Metric, a.Category order by b.Calculated_SOC) as Group_Id
  into EPL_tmp_Deciles_Percentiles
  from EPL_04_Eng_Matrix_Sky_Sports_Only a,
       (select
              Account_Number,
              Period,
              case
                when Metric = 'Live games - overall' then 'Overall'
                  else 'By EPL pack'
              end as Metric,
              Category,
              Calculated_SOC
          from EPL_03_SOCs_Sky_Sports_Only_Summaries a
         where a.Metric in ('Live games - overall', 'Live games - by EPL pack')) b
 where a.Account_Number = b.Account_Number
   and a.Period = b.Period
   and a.Metric = b.Metric
   and a.Category = b.Category
   and a.EPL_SOC not in ('Did not watch', 'Not calculated');
commit;

update EPL_04_Eng_Matrix_Sky_Sports_Only
   set EPL_SOC__Deciles       = case
                                  when base.EPL_SOC in ('Did not watch', 'Not calculated') then base.EPL_SOC
                                  when det.Group_Id between 1 and 9 then '0' || cast(det.Group_Id as varchar(1))
                                    else cast(det.Group_Id as varchar(2))
                                end
  from EPL_04_Eng_Matrix_Sky_Sports_Only base
          left join EPL_tmp_Deciles_Percentiles det
          on base.Account_Number = det.Account_Number
         and base.Period = det.Period
         and base.Metric = det.Metric
         and base.Category = det.Category;
commit;

  -- QA
select Period, Metric, Category, EPL_SOC, EPL_SOC__Deciles, count(*) as Cnt
  from EPL_04_Eng_Matrix_Sky_Sports_Only
 group by Period, Metric, Category, EPL_SOC, EPL_SOC__Deciles
 order by Period, Metric, Category, EPL_SOC__Deciles, EPL_SOC;


  -- Add percentiles
if object_id('EPL_tmp_Deciles_Percentiles') is not null then drop table EPL_tmp_Deciles_Percentiles end if;
select
      a.Account_Number,
      a.Period,
      a.Metric,
      a.Category,
      b.Calculated_SOC,
      ntile(100) over (partition by a.Metric, a.Category order by b.Calculated_SOC) as Group_Id
  into EPL_tmp_Deciles_Percentiles
  from EPL_04_Eng_Matrix_Sky_Sports_Only a,
       (select
              Account_Number,
              Period,
              case
                when Metric = 'Live games - overall' then 'Overall'
                  else 'By EPL pack'
              end as Metric,
              Category,
              Calculated_SOC
          from EPL_03_SOCs_Sky_Sports_Only_Summaries a
         where a.Metric in ('Live games - overall', 'Live games - by EPL pack')) b
 where a.Account_Number = b.Account_Number
   and a.Period = b.Period
   and a.Metric = b.Metric
   and a.Category = b.Category
   and a.EPL_SOC not in ('Did not watch', 'Not calculated');
commit;

update EPL_04_Eng_Matrix_Sky_Sports_Only
   set EPL_SOC__Percentiles   = case
                                  when base.EPL_SOC in ('Did not watch', 'Not calculated') then base.EPL_SOC
                                  when det.Group_Id between 1 and 9 then '00' || cast(det.Group_Id as varchar(1))
                                  when det.Group_Id between 10 and 99 then '0' || cast(det.Group_Id as varchar(2))
                                    else cast(det.Group_Id as varchar(3))
                                end
  from EPL_04_Eng_Matrix_Sky_Sports_Only base
          left join EPL_tmp_Deciles_Percentiles det
          on base.Account_Number = det.Account_Number
         and base.Period = det.Period
         and base.Metric = det.Metric
         and base.Category = det.Category;
commit;

  -- QA
select Period, Metric, Category, EPL_SOC, EPL_SOC__Percentiles, count(*) as Cnt
  from EPL_04_Eng_Matrix_Sky_Sports_Only
 group by Period, Metric, Category, EPL_SOC, EPL_SOC__Percentiles
 order by Period, Metric, Category, EPL_SOC__Percentiles, EPL_SOC;


  -- Add groups
update EPL_04_Eng_Matrix_Sky_Sports_Only base
   set base.EPL_SOC__010_Groups = case
                                    when det.Calculated_SOC =  0    then 'Did not watch'
                                    when det.Calculated_SOC <= 0.10 then '01) 0-10%'
                                    when det.Calculated_SOC <= 0.20 then '02) 10-20%'
                                    when det.Calculated_SOC <= 0.30 then '03) 20-30%'
                                    when det.Calculated_SOC <= 0.40 then '04) 30-40%'
                                    when det.Calculated_SOC <= 0.50 then '05) 40-50%'
                                    when det.Calculated_SOC <= 0.60 then '06) 50-60%'
                                    when det.Calculated_SOC <= 0.70 then '07) 60-70%'
                                    when det.Calculated_SOC <= 0.80 then '08) 70-80%'
                                    when det.Calculated_SOC <= 0.90 then '09) 80-90%'
                                    when det.Calculated_SOC <= 1.00 then '10) 90-100%'
                                      else '???'
                                  end
  from EPL_03_SOCs_Sky_Sports_Only_Summaries det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and (
        ( base.Metric = 'Overall' and det.Metric = 'Live games - overall' )
        or
        ( base.Metric = 'By EPL pack' and det.Metric = 'Live games - by EPL pack' )
       )
   and base.Category = det.Category;
commit;


update EPL_04_Eng_Matrix_Sky_Sports_Only base
   set base.EPL_SOC__100_Groups = case
                                    when det.Calculated_SOC =  0    then 'Did not watch'
                                    when det.Calculated_SOC <= 0.01 then '001) 0-1%'
                                    when det.Calculated_SOC <= 0.02 then '002) 1-2%'
                                    when det.Calculated_SOC <= 0.03 then '003) 2-3%'
                                    when det.Calculated_SOC <= 0.04 then '004) 3-4%'
                                    when det.Calculated_SOC <= 0.05 then '005) 4-5%'
                                    when det.Calculated_SOC <= 0.06 then '006) 5-6%'
                                    when det.Calculated_SOC <= 0.07 then '007) 6-7%'
                                    when det.Calculated_SOC <= 0.08 then '008) 7-8%'
                                    when det.Calculated_SOC <= 0.09 then '009) 8-9%'
                                    when det.Calculated_SOC <= 0.10 then '010) 9-10%'
                                    when det.Calculated_SOC <= 0.11 then '011) 10-11%'
                                    when det.Calculated_SOC <= 0.12 then '012) 11-12%'
                                    when det.Calculated_SOC <= 0.13 then '013) 12-13%'
                                    when det.Calculated_SOC <= 0.14 then '014) 13-14%'
                                    when det.Calculated_SOC <= 0.15 then '015) 14-15%'
                                    when det.Calculated_SOC <= 0.16 then '016) 15-16%'
                                    when det.Calculated_SOC <= 0.17 then '017) 16-17%'
                                    when det.Calculated_SOC <= 0.18 then '018) 17-18%'
                                    when det.Calculated_SOC <= 0.19 then '019) 18-19%'
                                    when det.Calculated_SOC <= 0.20 then '020) 19-20%'
                                    when det.Calculated_SOC <= 0.21 then '021) 20-21%'
                                    when det.Calculated_SOC <= 0.22 then '022) 21-22%'
                                    when det.Calculated_SOC <= 0.23 then '023) 22-23%'
                                    when det.Calculated_SOC <= 0.24 then '024) 23-24%'
                                    when det.Calculated_SOC <= 0.25 then '025) 24-25%'
                                    when det.Calculated_SOC <= 0.26 then '026) 25-26%'
                                    when det.Calculated_SOC <= 0.27 then '027) 26-27%'
                                    when det.Calculated_SOC <= 0.28 then '028) 27-28%'
                                    when det.Calculated_SOC <= 0.29 then '029) 28-29%'
                                    when det.Calculated_SOC <= 0.30 then '030) 29-30%'
                                    when det.Calculated_SOC <= 0.31 then '031) 30-31%'
                                    when det.Calculated_SOC <= 0.32 then '032) 31-32%'
                                    when det.Calculated_SOC <= 0.33 then '033) 32-33%'
                                    when det.Calculated_SOC <= 0.34 then '034) 33-34%'
                                    when det.Calculated_SOC <= 0.35 then '035) 34-35%'
                                    when det.Calculated_SOC <= 0.36 then '036) 35-36%'
                                    when det.Calculated_SOC <= 0.37 then '037) 36-37%'
                                    when det.Calculated_SOC <= 0.38 then '038) 37-38%'
                                    when det.Calculated_SOC <= 0.39 then '039) 38-39%'
                                    when det.Calculated_SOC <= 0.40 then '040) 39-40%'
                                    when det.Calculated_SOC <= 0.41 then '041) 40-41%'
                                    when det.Calculated_SOC <= 0.42 then '042) 41-42%'
                                    when det.Calculated_SOC <= 0.43 then '043) 42-43%'
                                    when det.Calculated_SOC <= 0.44 then '044) 43-44%'
                                    when det.Calculated_SOC <= 0.45 then '045) 44-45%'
                                    when det.Calculated_SOC <= 0.46 then '046) 45-46%'
                                    when det.Calculated_SOC <= 0.47 then '047) 46-47%'
                                    when det.Calculated_SOC <= 0.48 then '048) 47-48%'
                                    when det.Calculated_SOC <= 0.49 then '049) 48-49%'
                                    when det.Calculated_SOC <= 0.50 then '050) 49-50%'
                                    when det.Calculated_SOC <= 0.51 then '051) 50-51%'
                                    when det.Calculated_SOC <= 0.52 then '052) 51-52%'
                                    when det.Calculated_SOC <= 0.53 then '053) 52-53%'
                                    when det.Calculated_SOC <= 0.54 then '054) 53-54%'
                                    when det.Calculated_SOC <= 0.55 then '055) 54-55%'
                                    when det.Calculated_SOC <= 0.56 then '056) 55-56%'
                                    when det.Calculated_SOC <= 0.57 then '057) 56-57%'
                                    when det.Calculated_SOC <= 0.58 then '058) 57-58%'
                                    when det.Calculated_SOC <= 0.59 then '059) 58-59%'
                                    when det.Calculated_SOC <= 0.60 then '060) 59-60%'
                                    when det.Calculated_SOC <= 0.61 then '061) 60-61%'
                                    when det.Calculated_SOC <= 0.62 then '062) 61-62%'
                                    when det.Calculated_SOC <= 0.63 then '063) 62-63%'
                                    when det.Calculated_SOC <= 0.64 then '064) 63-64%'
                                    when det.Calculated_SOC <= 0.65 then '065) 64-65%'
                                    when det.Calculated_SOC <= 0.66 then '066) 65-66%'
                                    when det.Calculated_SOC <= 0.67 then '067) 66-67%'
                                    when det.Calculated_SOC <= 0.68 then '068) 67-68%'
                                    when det.Calculated_SOC <= 0.69 then '069) 68-69%'
                                    when det.Calculated_SOC <= 0.70 then '070) 69-70%'
                                    when det.Calculated_SOC <= 0.71 then '071) 70-71%'
                                    when det.Calculated_SOC <= 0.72 then '072) 71-72%'
                                    when det.Calculated_SOC <= 0.73 then '073) 72-73%'
                                    when det.Calculated_SOC <= 0.74 then '074) 73-74%'
                                    when det.Calculated_SOC <= 0.75 then '075) 74-75%'
                                    when det.Calculated_SOC <= 0.76 then '076) 75-76%'
                                    when det.Calculated_SOC <= 0.77 then '077) 76-77%'
                                    when det.Calculated_SOC <= 0.78 then '078) 77-78%'
                                    when det.Calculated_SOC <= 0.79 then '079) 78-79%'
                                    when det.Calculated_SOC <= 0.80 then '080) 79-80%'
                                    when det.Calculated_SOC <= 0.81 then '081) 80-81%'
                                    when det.Calculated_SOC <= 0.82 then '082) 81-82%'
                                    when det.Calculated_SOC <= 0.83 then '083) 82-83%'
                                    when det.Calculated_SOC <= 0.84 then '084) 83-84%'
                                    when det.Calculated_SOC <= 0.85 then '085) 84-85%'
                                    when det.Calculated_SOC <= 0.86 then '086) 85-86%'
                                    when det.Calculated_SOC <= 0.87 then '087) 86-87%'
                                    when det.Calculated_SOC <= 0.88 then '088) 87-88%'
                                    when det.Calculated_SOC <= 0.89 then '089) 88-89%'
                                    when det.Calculated_SOC <= 0.90 then '090) 89-90%'
                                    when det.Calculated_SOC <= 0.91 then '091) 90-91%'
                                    when det.Calculated_SOC <= 0.92 then '092) 91-92%'
                                    when det.Calculated_SOC <= 0.93 then '093) 92-93%'
                                    when det.Calculated_SOC <= 0.94 then '094) 93-94%'
                                    when det.Calculated_SOC <= 0.95 then '095) 94-95%'
                                    when det.Calculated_SOC <= 0.96 then '096) 95-96%'
                                    when det.Calculated_SOC <= 0.97 then '097) 96-97%'
                                    when det.Calculated_SOC <= 0.98 then '098) 97-98%'
                                    when det.Calculated_SOC <= 0.99 then '099) 98-99%'
                                    when det.Calculated_SOC <= 1.00 then '100) 99-100%'
                                      else '???'
                                  end
  from EPL_03_SOCs_Sky_Sports_Only_Summaries det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and (
        ( base.Metric = 'Overall' and det.Metric = 'Live games - overall' )
        or
        ( base.Metric = 'By EPL pack' and det.Metric = 'Live games - by EPL pack' )
       )
   and base.Category = det.Category;
commit;



  -- ##############################################################################################################
  -- ##############################################################################################################
  --    o Share of Sports Viewing (what proportion of their paid Sports viewing is to EPL): H = >50%, M = 20-50% and L = 0.1-19.9%
update EPL_04_Eng_Matrix_Sky_Sports_Only base
   set base.EPL_SoSV            = case
                                    when det.Calculated_SOV =  0    then 'Did not watch'
                                    when det.Calculated_SOV <  0.20 then 'Low'
                                    when det.Calculated_SOV <= 0.50 then 'Medium'
                                      else 'High'
                                  end
  from EPL_03_SOVs_Sky_Sports_Only det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and det.Metric = 'EPL SoSV';
commit;

  -- Add deciles
if object_id('EPL_tmp_Deciles_Percentiles') is not null then drop table EPL_tmp_Deciles_Percentiles end if;
select
      a.Account_Number,
      a.Period,
      a.Metric,
      a.Category,
      b.Calculated_SOV,
      ntile(10) over (partition by a.Metric, a.Category order by b.Calculated_SOV) as Group_Id
  into EPL_tmp_Deciles_Percentiles
  from EPL_04_Eng_Matrix_Sky_Sports_Only a,
       EPL_03_SOVs_Sky_Sports_Only b
 where a.Account_Number = b.Account_Number
   and a.Period = b.Period
   and a.EPL_SoSV not in ('Did not watch', 'Not calculated')
   and a.Metric = 'Overall'                                       -- All non-live EPL metrics are common for all categories
   and b.Metric in ('EPL SoSV');
commit;

update EPL_04_Eng_Matrix_Sky_Sports_Only
   set EPL_SoSV__Deciles      = case
                                  when base.EPL_SoSV in ('Did not watch', 'Not calculated') then base.EPL_SoSV
                                  when det.Group_Id between 1 and 9 then '0' || cast(det.Group_Id as varchar(1))
                                    else cast(det.Group_Id as varchar(2))
                                end
  from EPL_04_Eng_Matrix_Sky_Sports_Only base
          left join EPL_tmp_Deciles_Percentiles det
          on base.Account_Number = det.Account_Number
         and base.Period = det.Period;
commit;

  -- QA
select Period, Metric, Category, EPL_SoSV, EPL_SoSV__Deciles, count(*) as Cnt
  from EPL_04_Eng_Matrix_Sky_Sports_Only
 group by Period, Metric, Category, EPL_SoSV, EPL_SoSV__Deciles
 order by Period, Metric, Category, EPL_SoSV__Deciles, EPL_SoSV;


  -- Add percentiles
if object_id('EPL_tmp_Deciles_Percentiles') is not null then drop table EPL_tmp_Deciles_Percentiles end if;
select
      a.Account_Number,
      a.Period,
      a.Metric,
      a.Category,
      b.Calculated_SOV,
      ntile(100) over (partition by a.Metric, a.Category order by b.Calculated_SOV) as Group_Id
  into EPL_tmp_Deciles_Percentiles
  from EPL_04_Eng_Matrix_Sky_Sports_Only a,
       EPL_03_SOVs_Sky_Sports_Only b
 where a.Account_Number = b.Account_Number
   and a.Period = b.Period
   and a.EPL_SoSV not in ('Did not watch', 'Not calculated')
   and a.Metric = 'Overall'                                       -- All non-live EPL metrics are common for all categories
   and b.Metric in ('EPL SoSV');
commit;

update EPL_04_Eng_Matrix_Sky_Sports_Only
   set EPL_SoSV__Percentiles  = case
                                  when base.EPL_SoSV in ('Did not watch', 'Not calculated') then base.EPL_SoSV
                                  when det.Group_Id between 1 and 9 then '00' || cast(det.Group_Id as varchar(1))
                                  when det.Group_Id between 10 and 99 then '0' || cast(det.Group_Id as varchar(2))
                                    else cast(det.Group_Id as varchar(3))
                                end
  from EPL_04_Eng_Matrix_Sky_Sports_Only base
          left join EPL_tmp_Deciles_Percentiles det
          on base.Account_Number = det.Account_Number
         and base.Period = det.Period;
commit;

  -- QA
select Period, Metric, Category, EPL_SoSV, EPL_SoSV__Percentiles, count(*) as Cnt
  from EPL_04_Eng_Matrix_Sky_Sports_Only
 group by Period, Metric, Category, EPL_SoSV, EPL_SoSV__Percentiles
 order by Period, Metric, Category, EPL_SoSV__Percentiles, EPL_SoSV;


  -- ##############################################################################################################
  -- ##############################################################################################################
/*
  --    o Share of Viewing (what proportion of their total paid Sky viewing is to paid Sports): HH = >20%, H = 10-20%, M = 5-9.9%, L = 0.1-4.9%
  -- REVISED BANDINGS (21/03)
  --    Further to yesterday’s session where we discussed a higher top banding for SoV (in order to identify those who really only subscribe to
  --    Sky for the Sport), I’d like to propose the following new banding, based on chart 3.9 in the attached:
  --    L:  0.1-4.9% (39% of HHs)
  --    M: 5-24.9% (31%)
  --    H: 25-44.9% (19%)
  --    VH: >=45% (11%)
update EPL_04_Eng_Matrix_Sky_Sports_Only base
   set base.Sport_SoV           = case
                                    when det.Calculated_SOV =  0    then 'Did not watch'
                                    when det.Calculated_SOV <  0.05 then 'Low'
                                    when det.Calculated_SOV <  0.25 then 'Medium'
                                    when det.Calculated_SOV <  0.45 then 'High'
                                      else 'Very high'
                                  end
  from EPL_03_SOVs_Sky_Sports_Only det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and det.Metric = 'Sports SoV';
commit;

  -- Add deciles
if object_id('EPL_tmp_Deciles_Percentiles') is not null then drop table EPL_tmp_Deciles_Percentiles end if;
select
      a.Account_Number,
      a.Period,
      a.Metric,
      a.Category,
      b.Calculated_SOV,
      ntile(10) over (partition by a.Metric, a.Category order by b.Calculated_SOV) as Group_Id
  into EPL_tmp_Deciles_Percentiles
  from EPL_04_Eng_Matrix_Sky_Sports_Only a,
       EPL_03_SOVs_Sky_Sports_Only b
 where a.Account_Number = b.Account_Number
   and a.Period = b.Period
   and a.Sport_SoV not in ('Did not watch', 'Not calculated')
   and a.Metric = 'Overall'                                       -- All non-live EPL metrics are common for all categories
   and b.Metric in ('Sports SoV');
commit;

update EPL_04_Eng_Matrix_Sky_Sports_Only
   set Sport_SoV__Deciles     = case
                                  when base.Sport_SoV in ('Did not watch', 'Not calculated') then base.Sport_SoV
                                  when det.Group_Id between 1 and 9 then '0' || cast(det.Group_Id as varchar(1))
                                    else cast(det.Group_Id as varchar(2))
                                end
  from EPL_04_Eng_Matrix_Sky_Sports_Only base
          left join EPL_tmp_Deciles_Percentiles det
          on base.Account_Number = det.Account_Number
         and base.Period = det.Period;
commit;

  -- QA
select Period, Metric, Category, Sport_SoV, Sport_SoV__Deciles, count(*) as Cnt
  from EPL_04_Eng_Matrix_Sky_Sports_Only
 group by Period, Metric, Category, Sport_SoV, Sport_SoV__Deciles
 order by Period, Metric, Category, Sport_SoV__Deciles, Sport_SoV;


  -- Add percentiles
if object_id('EPL_tmp_Deciles_Percentiles') is not null then drop table EPL_tmp_Deciles_Percentiles end if;
select
      a.Account_Number,
      a.Period,
      a.Metric,
      a.Category,
      b.Calculated_SOV,
      ntile(100) over (partition by a.Metric, a.Category order by b.Calculated_SOV) as Group_Id
  into EPL_tmp_Deciles_Percentiles
  from EPL_04_Eng_Matrix_Sky_Sports_Only a,
       EPL_03_SOVs_Sky_Sports_Only b
 where a.Account_Number = b.Account_Number
   and a.Period = b.Period
   and a.Sport_SoV not in ('Did not watch', 'Not calculated')
   and a.Metric = 'Overall'                                       -- All non-live EPL metrics are common for all categories
   and b.Metric in ('Sports SoV');
commit;

update EPL_04_Eng_Matrix_Sky_Sports_Only
   set Sport_SoV__Percentiles = case
                                  when base.Sport_SoV in ('Did not watch', 'Not calculated') then base.Sport_SoV
                                  when det.Group_Id between 1 and 9 then '00' || cast(det.Group_Id as varchar(1))
                                  when det.Group_Id between 10 and 99 then '0' || cast(det.Group_Id as varchar(2))
                                    else cast(det.Group_Id as varchar(3))
                                end
  from EPL_04_Eng_Matrix_Sky_Sports_Only base
          left join EPL_tmp_Deciles_Percentiles det
          on base.Account_Number = det.Account_Number
         and base.Period = det.Period;
commit;

  -- QA
select Period, Metric, Category, Sport_SoV, Sport_SoV__Percentiles, count(*) as Cnt
  from EPL_04_Eng_Matrix_Sky_Sports_Only
 group by Period, Metric, Category, Sport_SoV, Sport_SoV__Percentiles
 order by Period, Metric, Category, Sport_SoV__Percentiles, Sport_SoV;
*/


  -- ##############################################################################################################
  -- ##############################################################################################################
  -- Update column view
update EPL_04_Eng_Matrix_Sky_Sports_Only base
   set base.EPL_SOC__Pack_Monday_Evening = det.EPL_SOC
  from EPL_04_Eng_Matrix_Sky_Sports_Only det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and det.Metric = 'By EPL pack'
   and det.Category = 'Monday Evening';
commit;

update EPL_04_Eng_Matrix_Sky_Sports_Only base
   set base.EPL_SOC__Pack_Midweek_Evening = det.EPL_SOC
  from EPL_04_Eng_Matrix_Sky_Sports_Only det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and det.Metric = 'By EPL pack'
   and det.Category = 'Midweek Evening';
commit;

update EPL_04_Eng_Matrix_Sky_Sports_Only base
   set base.EPL_SOC__Pack_Saturday_Lunch = det.EPL_SOC
  from EPL_04_Eng_Matrix_Sky_Sports_Only det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and det.Metric = 'By EPL pack'
   and det.Category = 'Saturday Lunchtime';
commit;

update EPL_04_Eng_Matrix_Sky_Sports_Only base
   set base.EPL_SOC__Pack_Saturday_Afternoon = det.EPL_SOC
  from EPL_04_Eng_Matrix_Sky_Sports_Only det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and det.Metric = 'By EPL pack'
   and det.Category = 'Saturday Afternoon';
commit;

update EPL_04_Eng_Matrix_Sky_Sports_Only base
   set base.EPL_SOC__Pack_Sunday_Early_Afternoon = det.EPL_SOC
  from EPL_04_Eng_Matrix_Sky_Sports_Only det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and det.Metric = 'By EPL pack'
   and det.Category = 'Sunday Early Afternoon';
commit;

update EPL_04_Eng_Matrix_Sky_Sports_Only base
   set base.EPL_SOC__Pack_Sunday_Late_Afternoon = det.EPL_SOC
  from EPL_04_Eng_Matrix_Sky_Sports_Only det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and det.Metric = 'By EPL pack'
   and det.Category = 'Sunday Late Afternoon';
commit;



  -- ##############################################################################################################
  -- ##############################################################################################################
  -- SUMMARIZE
select
      'Any content' as Source,
      case
        when a.Period = 1 then '2) Aug ''13 - Feb ''14'
        when a.Period = 2 then '1) Feb ''13 - Jul ''13'
        when a.Period = 3 then '3) Feb ''13 - May ''13'
          else '???'
      end as xPeriod,
      case
        when b.Prem_Sports = 0 then 'No'
          else 'Yes'
      end as Sports_Premium,
      case
        when Low_Content_Flag = 1 then 'Low'
          else 'Normal'
      end as xLow_Content_Flag,
      a.Metric,
      a.Category,
      a.EPL_SOC,
      a.EPL_SoNLC,
      a.EPL_SoSV,
      a.Sport_SoV,
      count(*) as Accts,
      count(distinct a.Account_Number) as Accts2
  from EPL_04_Eng_Matrix_Sky_Sports_Only a,
       EPL_04_Profiling_Variables b
  where a.Account_Number = b.Account_Number
   and a.Period = b.Period
   and a.Period = 1
 group by
      Source,
      xPeriod,
      Sports_Premium,
      xLow_Content_Flag,
      Metric,
      Category,
      EPL_SOC,
      EPL_SoNLC,
      EPL_SoSV,
      Sport_SoV

union all

select
      'Sky Sports only' as Source,
      case
        when a.Period = 1 then '2) Aug ''13 - Feb ''14'
        when a.Period = 2 then '1) Feb ''13 - Jul ''13'
        when a.Period = 3 then '3) Feb ''13 - May ''13'
          else '???'
      end as xPeriod,
      case
        when b.Prem_Sports = 0 then 'No'
          else 'Yes'
      end as Sports_Premium,
      case
        when Low_Content_Flag = 1 then 'Low'
          else 'Normal'
      end as xLow_Content_Flag,
      a.Metric,
      a.Category,
      a.EPL_SOC,
      a.EPL_SoNLC,
      a.EPL_SoSV,
      a.Sport_SoV,
      count(distinct a.Account_Number) as Accts
  from EPL_04_Eng_Matrix_Sky_Sports_Only a,
       EPL_04_Profiling_Variables b
  where a.Account_Number = b.Account_Number
   and a.Period = b.Period
   and a.Period = 1
 group by
      Source,
      xPeriod,
      Sports_Premium,
      xLow_Content_Flag,
      Metric,
      Category,
      EPL_SOC,
      EPL_SoNLC,
      EPL_SoSV,
      Sport_SoV;



  -- ##############################################################################################################
  -- ##############################################################################################################

















