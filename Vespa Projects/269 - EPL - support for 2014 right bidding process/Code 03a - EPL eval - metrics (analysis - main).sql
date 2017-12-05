/*###############################################################################
# Created on:   10/03/2014
# Created by:   Sebastian Bednaszynski(SBE)
# Description:  EPL rights project - Metric calculations (SOC, SOV etc.)
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
if object_id('EPL_03_SOCs') is not null then drop table EPL_03_SOCs end if;
create table EPL_03_SOCs (
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
create        hg   index idx01 on EPL_03_SOCs(Account_Number);
create        lf   index idx02 on EPL_03_SOCs(Period);
create        date index idx03 on EPL_03_SOCs(Broadcast_Date);
create        hg   index idx04 on EPL_03_SOCs(Programme_Instance_Name);
grant select on EPL_03_SOCs to vespa_group_low_security;


if object_id('EPL_03_SOCs_Summaries') is not null then drop table EPL_03_SOCs_Summaries end if;
create table EPL_03_SOCs_Summaries (
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
create        hg   index idx01 on EPL_03_SOCs_Summaries(Account_Number);
create        lf   index idx02 on EPL_03_SOCs_Summaries(Period);
create        lf   index idx03 on EPL_03_SOCs_Summaries(Metric);
grant select on EPL_03_SOCs_Summaries to vespa_group_low_security;


if object_id('EPL_03_SOVs') is not null then drop table EPL_03_SOVs end if;
create table EPL_03_SOVs (
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
create        hg   index idx01 on EPL_03_SOVs(Account_Number);
create        lf   index idx02 on EPL_03_SOVs(Period);
create        lf   index idx03 on EPL_03_SOVs(Metric);
grant select on EPL_03_SOVs to vespa_group_low_security;



  -- ##############################################################################################################
  -- ##### Share of Content - preparation                                                                     #####
  -- ##############################################################################################################
  -- Get content programmes available to each account
-- truncate table EPL_03_SOCs;
insert into EPL_03_SOCs
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
            b.Channel not in ('Sky Sports 1', 'Sky Sports 2', 'Sky Sports 3', 'Sky Sports 4', 'BT Sport 1', 'ESPN', '3D channel')       -- Terrestrial/non-premium channels
            or
            ( a.Sky_Sports_Flag = 1 and b.Channel in ('Sky Sports 1', 'Sky Sports 2', 'Sky Sports 3', 'Sky Sports 4', '3D channel') )   -- OR Sky Sports channels, if eligible
            or
            ( a.BT_Sport_Flag = 1 and b.Channel in ('BT Sport 1') )                                                                     -- OR BT Sport channel, if eligible
            or
            ( a.ESPN_Flag = 1 and b.Channel in ('ESPN') )                                                                               -- OR ESPN channel, if eligible
         )
   group by a.Account_Number, a.Period, b.Broadcast_Date, b.Channel, b.Programme, b.Programme_Instance_Name,
            b.Kick_Off_Time, b.Day_Of_Week, b.Live_Game_Flag, b.EPL_Pack;
commit;


  -- Attribute viewing
update EPL_03_SOCs base
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
  -- OVERALL - live & non-live
delete from EPL_03_SOCs_Summaries
 where Metric in ('Live games - overall', 'Non-live programmes - overall');
commit;

insert into EPL_03_SOCs_Summaries
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
    from EPL_03_SOCs
   where Live_Game_Flag = 1
      or (
          Live_Game_Flag = 0
          and
          Programme in ('Final Score', 'Football First', 'Football Focus', 'Match of the Day', 'Match of the Day 2')
         )
   group by Account_Number, Period, xMetric, xCategory;
commit;


  -- OVERALL - live & Sky channels only
delete from EPL_03_SOCs_Summaries
 where Metric in ('Live games - overall (Sky channels only)', 'Non-live programmes - overall (Sky channels only)');
commit;

insert into EPL_03_SOCs_Summaries
      (Account_Number, Period, Metric, Category, Content_Available, Content_Watched, Calculated_SOC)
  select
        Account_Number,
        Period,
        case
          when Live_Game_Flag = 1 then 'Live games - overall (Sky channels only)'
            else 'Non-live programmes - overall (Sky channels only)'
        end                                           as xMetric,
        '(all)'                                       as xCategory,
        sum(Content_Available)                        as xContent_Available,
        sum(Content_Watched)                          as xContent_Watched,
        case
          when xContent_Available = 0 then null
            else 1.0 * xContent_Watched / xContent_Available
        end                                           as xCalculated_SOC
    from EPL_03_SOCs
   where (
            Live_Game_Flag = 1
            or
            (
              Live_Game_Flag = 0
              and
              Programme in ('Final Score', 'Football First', 'Football Focus', 'Match of the Day', 'Match of the Day 2')
            )
          )
     and Channel in ('3D channel', 'Sky Sports 1', 'Sky Sports 2', 'Sky Sports 3', 'Sky Sports 4')
   group by Account_Number, Period, xMetric, xCategory;
commit;


  -- OVERALL - live & non-Sky channels only
delete from EPL_03_SOCs_Summaries
 where Metric in ('Live games - overall (non-Sky channels only)', 'Non-live programmes - overall (non-Sky channels only)');
commit;

insert into EPL_03_SOCs_Summaries
      (Account_Number, Period, Metric, Category, Content_Available, Content_Watched, Calculated_SOC)
  select
        Account_Number,
        Period,
        case
          when Live_Game_Flag = 1 then 'Live games - overall (non-Sky channels only)'
            else 'Non-live programmes - overall (non-Sky channels only)'
        end                                           as xMetric,
        '(all)'                                       as xCategory,
        sum(Content_Available)                        as xContent_Available,
        sum(Content_Watched)                          as xContent_Watched,
        case
          when xContent_Available = 0 then null
            else 1.0 * xContent_Watched / xContent_Available
        end                                           as xCalculated_SOC
    from EPL_03_SOCs
   where (
            Live_Game_Flag = 1
            or
            (
              Live_Game_Flag = 0
              and
              Programme in ('Final Score', 'Football First', 'Football Focus', 'Match of the Day', 'Match of the Day 2')
            )
          )
     and Channel not in ('3D channel', 'Sky Sports 1', 'Sky Sports 2', 'Sky Sports 3', 'Sky Sports 4')
   group by Account_Number, Period, xMetric, xCategory;
commit;


  -- LIVE - BY PACK
delete from EPL_03_SOCs_Summaries
 where Metric = 'Live games - by EPL pack';
commit;

insert into EPL_03_SOCs_Summaries
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
    from EPL_03_SOCs
   where Live_Game_Flag = 1                                                   -- Live EPL games only
   group by Account_Number, Period, xMetric, xCategory;
commit;


  -- LIVE - BY GAME TIME
delete from EPL_03_SOCs_Summaries
 where Metric = 'Live games - by game time';
commit;

insert into EPL_03_SOCs_Summaries
      (Account_Number, Period, Metric, Category, Content_Available, Content_Watched, Calculated_SOC)
  select
        Account_Number,
        Period,
        'Live games - by game time'                   as xMetric,
        case
          when Day_Of_Week not in ('Saturday', 'Sunday') then 'Weekday'
            else Day_Of_Week
        end || ' @' || dateformat(Kick_Off_Time, 'hh:mm')
                                                      as xCategory,
        sum(Content_Available)                        as xContent_Available,
        sum(Content_Watched)                          as xContent_Watched,
        case
          when xContent_Available = 0 then null
            else 1.0 * xContent_Watched / xContent_Available
        end                                           as xCalculated_SOC
    from EPL_03_SOCs
   where Live_Game_Flag = 1                                                   -- Live EPL games only
   group by Account_Number, Period, xMetric, xCategory;
commit;


  -- LIVE - BY CHANNEL
delete from EPL_03_SOCs_Summaries
 where Metric = 'Live games - by channel';
commit;

insert into EPL_03_SOCs_Summaries
      (Account_Number, Period, Metric, Category, Content_Available, Content_Watched, Calculated_SOC)
  select
        Account_Number,
        Period,
        'Live games - by channel'                     as xMetric,
        case
          when Channel like 'Sky%' then 'Sky channels'
          when Channel like '3D%' then 'Sky channels'
          when Channel like 'BT%' then 'BT channels'
          when Channel like 'ESPN%' then 'ESPN channels'
            else '???'
        end                                           as xCategory,
        sum(Content_Available)                        as xContent_Available,
        sum(Content_Watched)                          as xContent_Watched,
        case
          when xContent_Available = 0 then null
            else 1.0 * xContent_Watched / xContent_Available
        end                                           as xCalculated_SOC
    from EPL_03_SOCs
   where Live_Game_Flag = 1                                                   -- Live EPL games only
   group by Account_Number, Period, xMetric, xCategory;
commit;


  -- NON-LIVE - BY CHANNEL
delete from EPL_03_SOCs_Summaries
 where Metric = 'Non-live programmes - by channel';
commit;

insert into EPL_03_SOCs_Summaries
      (Account_Number, Period, Metric, Category, Content_Available, Content_Watched, Calculated_SOC)
  select
        Account_Number,
        Period,
        'Non-live programmes - by channel'            as xMetric,
        case
          when Channel like 'Sky%' then 'Sky channels'
          when Channel like '3D%' then 'Sky channels'
          when Channel like 'BT%' then 'BT channels'
          when Channel like 'ESPN%' then 'ESPN channels'
          when Channel like 'BBC%' then 'BBC channels'
            else '???'
        end                                           as xCategory,
        sum(Content_Available)                        as xContent_Available,
        sum(Content_Watched)                          as xContent_Watched,
        case
          when xContent_Available = 0 then null
            else 1.0 * xContent_Watched / xContent_Available
        end                                           as xCalculated_SOC
    from EPL_03_SOCs
   where Live_Game_Flag = 0                                                   -- Non-live programmes only
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
  from EPL_03_SOCs_Summaries
 group by
      xPeriod,
      Metric,
      Category,
      SOC_1,
      SOC_2;


  -- Lookup for number of games
select Season, '(overall)' as Dimension, '(overall)' as Category, count(distinct Programme_Instance_Name) as Games_Num from EPL_01_EPG where Live_Game_Flag = 1 group by Season, Category union all
select Season, 'EPL Pack' as Dimension, EPL_Pack as Category, count(distinct Programme_Instance_Name) as Games_Num from EPL_01_EPG where Live_Game_Flag = 1 group by Season, Category union all
select Season, 'Channel' as Dimension,
                  case
                    when Channel like 'Sky%' then 'Sky channels'
                    when Channel like '3D%' then 'Sky channels'
                    when Channel like 'BT%' then 'BT channels'
                    when Channel like 'ESPN%' then 'ESPN channels'
                      else '???'
                  end as Category, count(distinct Programme_Instance_Name) as Games_Num from EPL_01_EPG where Live_Game_Flag = 1 group by Season, Category union all
select Season, 'Kick-off time' as Dimension,
                  case
                    when Day_Of_Week not in ('Saturday', 'Sunday') then 'Weekday'
                      else Day_Of_Week
                  end || ' @' || dateformat(Kick_Off_Time, 'hh:mm')
                    as Category, count(distinct Programme_Instance_Name) as Games_Num from EPL_01_EPG where Live_Game_Flag = 1 group by Season, Category
order by 1, 2, 3;




  -- ##############################################################################################################
  -- ##### Share of Viewing                                                                                   #####
  -- ##############################################################################################################
  -- EPL SoSV
  --    ‘All live EPL matches on SS / BT / ESPN (including live paused matches <= 15 mins
  --    from orig. broadcast time but excluding any other playback)’ / ‘All Sports genre viewing on SS / BT / ESPN
  --    (including live paused matches <= 15 mins from orig. broadcast time but excluding any other playback)’.
delete from EPL_03_SOVs
 where Metric = 'EPL SoSV';
commit;

insert into EPL_03_SOVs
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
           where Sports_Channel_Type in (1, 2, 3)                         -- 1: Sky Sports, 2: BT Sports, 3: ESPN, 4: All other
             and Viewing_Type in (1, 2)) det                              -- 1: Live, 2: Live pause (up to 15 mins), 3: Playback
   group by Account_Number, Period;
commit;

  -- By end of May only
insert into EPL_03_SOVs
      (Account_Number, Period, Metric, Category, Category_Consumption, Total_Consumption, Calculated_SOV)
  select
        Account_Number,
        3,                                                                -- Manual hack to make it easier for Excel
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
           where Sports_Channel_Type in (1, 2, 3)                         -- 1: Sky Sports, 2: BT Sports, 3: ESPN, 4: All other
             and Viewing_Type in (1, 2)                                   -- 1: Live, 2: Live pause (up to 15 mins), 3: Playback
             and Viewing_Day <= '2013-05-31') det
   group by Account_Number, Period;
commit;


  --  Sports SoV
  --    ‘All Sports genre viewing on SS/BT/ESPN (including live paused matches <= 15 mins from orig.
  --    broadcast time but excluding any other playback)’ / ‘All Pay TV viewing (including all playback)’
delete from EPL_03_SOVs
 where Metric = 'Sports SoV';
commit;

insert into EPL_03_SOVs
      (Account_Number, Period, Metric, Category, Category_Consumption, Total_Consumption, Calculated_SOV)
  select
        Account_Number,
        Period,
        'Sports SoV' as Metric,
        '(all)' as Category,
        sum(case
              when Sports_Channel_Type in (1, 2, 3) and Viewing_Type in (1, 2)     -- Sports_Channel_Type - 1: Sky Sports, 2: BT Sports, 3: ESPN, 4: All other
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

  -- By end of May only
insert into EPL_03_SOVs
      (Account_Number, Period, Metric, Category, Category_Consumption, Total_Consumption, Calculated_SOV)
  select
        Account_Number,
        3,                                                                -- Manual hack to make it easier for Excel
        'Sports SoV' as Metric,
        '(all)' as Category,
        sum(case
              when Sports_Channel_Type in (1, 2, 3) and Viewing_Type in (1, 2)     -- Sports_Channel_Type - 1: Sky Sports, 2: BT Sports, 3: ESPN, 4: All other
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
   where Viewing_Day <= '2013-05-31'
        /*(select
                Account_Number,
                Period,
                Sports_Channel_Type,
                Viewing_Type,
                Sports_Genre_Flag,
                Pay_TV_Type,
                Viewing_Duration
            from EPL_02_Viewing_Summary) det*/
   group by Account_Number, Period;
commit;


  --  Sky Atlantic SoV
  --    Viewing to Sky Atlantic:  Can we create a None/L/M/H segmentation for this based on the distribution of our universe (total mins. viewed over the period)?
  --    SoV denominator: RCrouch - My recommendation would be All Base package TV (without prem movies/sports) – as we already have the pay overall  to pay  sport split
  --    1/04 - RCrouch - change from "Cat viewing"/"Pay+FTA" to "Cat viewing"/"Pay only"
delete from EPL_03_SOVs
 where Metric = 'Sky Atlantic SoV';
commit;

insert into EPL_03_SOVs
      (Account_Number, Period, Metric, Category, Category_Consumption, Total_Consumption, Calculated_SOV)
  select
        Account_Number,
        Period,
        'Sky Atlantic SoV' as Metric,
        '(all)' as Category,
        sum(case
              when Sky_Exclusive_Channel_Type = 1 then Viewing_Duration           -- 0: All other, 1: Sky Atlantic (all), 2: Other Sky Exclusive channels, 3: Sky Sports News
                else 0
            end) as xCategory_Consumption,
        sum(case
              when Pay_TV_Type = 1 then Viewing_Duration                          -- Pay_TV_Type    - 0: FTA, 1: Pay package, 2: Pay Sports, 3: Pay Movies,
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


-- [!!!]
  --  Sky Atlantic SoV
  --    Additional metric requested on 27/06: Viewing to Sky Atlantic - use no. of complete programmes viewed rather than SoV to define thresholds
delete from EPL_03_SOVs
 where Metric = 'Sky Atlantic - number of complete programmes';
commit;

insert into EPL_03_SOVs
      (Account_Number, Period, Metric, Category, Category_Consumption, Total_Consumption, Calculated_SOV)
  select
        a.Account_Number,
        a.Period,
        'Sky Atlantic - number of complete programmes' as xMetric,
        '(all)' as xCategory,
        count(distinct case when Proportion_Of_Programme_Watched >= 0.60 then Dk_Programme_Instance_Dim else null end) as xProgramme_Watched,
        max(b.Days_Data_Available) as xTotal_Consumption,
        1.0 * xProgramme_Watched / max(b.Days_Data_Available) as xCalculated_SOV
    from EPL_02_Viewing_Summary__Sky_Atlantic a,
         (select distinct
                Account_Number,
                Period,
                Days_Data_Available
            from EPL_01_Universe
           where Period = 1) b
   where a.Account_Number = b.Account_Number
     and a.Period = b.Period
   group by a.Account_Number,
            a.Period;
commit;


  --  Sky Sports News
delete from EPL_03_SOVs
 where Metric = 'Sky Sports News SoV';
commit;

insert into EPL_03_SOVs
      (Account_Number, Period, Metric, Category, Category_Consumption, Total_Consumption, Calculated_SOV)
  select
        Account_Number,
        Period,
        'Sky Sports News SoV' as Metric,
        '(all)' as Category,
        sum(case
              when Sky_Exclusive_Channel_Type in (3) then Viewing_Duration        -- 0: All other, 1: Sky Atlantic (all), 2: Other Sky Exclusive channels, 3: Sky Sports News
                else 0
            end) as xCategory_Consumption,
        sum(case
              when Pay_TV_Type = 1 then Viewing_Duration                          -- Pay_TV_Type    - 0: FTA, 1: Pay package, 2: Pay Sports, 3: Pay Movies,
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


  --  Entertainment pack SoV
  --    % of total viewing that is to Basic Ent Content:  Can we do the same as above to create four buckets?
  --    BASIC ENT CONTENT = FTA+Pay TV (non-premium)
delete from EPL_03_SOVs
 where Metric = 'Entertainment Pack SoV';
commit;

insert into EPL_03_SOVs
      (Account_Number, Period, Metric, Category, Category_Consumption, Total_Consumption, Calculated_SOV)
  select
        Account_Number,
        Period,
        'Entertainment Pack SoV' as Metric,
        '(all)' as Category,
        sum(case
              when Pay_TV_Type in (0, 1)  then Viewing_Duration                   -- Pay_TV_Type    - 0: FTA, 1: Pay package, 2: Pay Sports, 3: Pay Movies,
                                                                                  --                  4: 3rd party, 5: A'La Carte, 6: PPV/other
                else 0
            end) as xCategory_Consumption,
        sum(case
              when Pay_TV_Type in (0, 1, 2, 3, 4, 5) then Viewing_Duration        -- Pay_TV_Type    - 0: FTA, 1: Pay package, 2: Pay Sports, 3: Pay Movies,
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


  --  Sky exclusive channels SoV
  --    % of Basic Ent Content viewed that is to Sky Exclusive channels*:  Again as above
  --    SoV denominator: RCrouch - My recommendation would be All Base package TV (without prem movies/sports) – as we already have the pay overall  to pay  sport split
  --    1/04 - RCrouch - change from "Cat viewing"/"Pay+FTA" to "Cat viewing"/"Pay only"
delete from EPL_03_SOVs
 where Metric = 'Sky exclusive channels SoV';
commit;

insert into EPL_03_SOVs
      (Account_Number, Period, Metric, Category, Category_Consumption, Total_Consumption, Calculated_SOV)
  select
        Account_Number,
        Period,
        'Sky exclusive channels SoV' as Metric,
        '(all)' as Category,
        sum(case
              when Sky_Exclusive_Channel_Type in (1, 2) then Viewing_Duration     -- 0: All other, 1: Sky Atlantic (all), 2: Other Sky Exclusive channels, 3: Sky Sports News
                else 0
            end) as xCategory_Consumption,
        sum(case
              when Pay_TV_Type = 1 then Viewing_Duration                          -- Pay_TV_Type    - 0: FTA, 1: Pay package, 2: Pay Sports, 3: Pay Movies,
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

--[!!!]
  --  Sky branded Pay TV channels (excluding Sky Atlantic: Sky 1 & 2, Sky Living, Sky Arts 1 & 2, Sky LivingIt, Sky Poker, Sky 3D)
  --    Can we include an additional ‘level’ in the tree which refers to high (or not) viewing to (non-Atlantic) Sky Pay TV channels
  --    (suggest looking at avg mins / day, rather than an SoV metric, that we can then cut into HML bands)
  --    Since 30/10/2014 Sky Atlantic is included in Sky Branded category
delete from EPL_03_SOVs
 where Metric = 'Sky branded channels (incl. Sky Atlantic)';
commit;

insert into EPL_03_SOVs
      (Account_Number, Period, Metric, Category, Category_Consumption, Total_Consumption, Calculated_SOV)
  select
        b.Account_Number,
        b.Period,
        'Sky branded channels (incl. Sky Atlantic)' as Metric,
        '(all)' as Category,
        sum(Viewing_Duration) as xCategory_Consumption,
        max(b.Days_Data_Available) as xTotal_Consumption,
        case
          when xTotal_Consumption > 0 then (1.0 * xCategory_Consumption / xTotal_Consumption) / 60
            else 0
        end as xCalculated_SOV
    from EPL_02_Viewing_Summary__Sky_Branded_Chnls det,
         (select distinct
                Account_Number,
                Period,
                Days_Data_Available
            from EPL_01_Universe
           where Period = 1) b
   where det.Account_Number = b.Account_Number
     and det.Period = b.Period
   group by b.Account_Number, b.Period;
commit;



  --  Sky/Virgin exclusive channels SoV
  --    RCrouch's list of channels, SoV as above
  --    SoV denominator: RCrouch - My recommendation would be All Base package TV (without prem movies/sports) – as we already have the pay overall  to pay  sport split
  --    1/04 - RCrouch - change from "Cat viewing"/"Pay+FTA" to "Cat viewing"/"Pay only"
delete from EPL_03_SOVs
 where Metric = 'Sky/Virgin exclusive channels SoV';
commit;

insert into EPL_03_SOVs
      (Account_Number, Period, Metric, Category, Category_Consumption, Total_Consumption, Calculated_SOV)
  select
        Account_Number,
        Period,
        'Sky/Virgin exclusive channels SoV' as Metric,
        '(all)' as Category,
        sum(case
              when Sky_Virgin_Exclusive_Channel_Type = 1 then Viewing_Duration    -- 0: All other, 1: Sky/Virgin exclusive
                else 0
            end) as xCategory_Consumption,
        sum(case
              when Pay_TV_Type = 1 then Viewing_Duration                          -- Pay_TV_Type    - 0: FTA, 1: Pay package, 2: Pay Sports, 3: Pay Movies,
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


  --  Movies SoV
  --  % of total viewing that is to Prem Movies content
  --    SBE: Currently used definitions are:
  --          -	Ent pack SoV: (FTA+Pay TV) / (All TV)
  --          -	Sports SoV: (Live/Live pause sport genre viewing on SS/BT/ESPN) / (All Pay TV viewing)
  --          For Movies – would you like the denominator to be “All TV” or “All Pay TV”, assuming both live and all playback?
  --    KSargent: Suggest All Pay TV and including playback
  --    RCrounch: Needs to be pay TV excluding pay sport  as we already have the Sport as % of total pay
delete from EPL_03_SOVs
 where Metric = 'Movies SoV';
commit;

insert into EPL_03_SOVs
      (Account_Number, Period, Metric, Category, Category_Consumption, Total_Consumption, Calculated_SOV)
  select
        Account_Number,
        Period,
        'Movies SoV' as Metric,
        '(all)' as Category,
        sum(case
              when Pay_TV_Type = 3 then Viewing_Duration                          -- Pay_TV_Type    - 0: FTA, 1: Pay package, 2: Pay Sports, 3: Pay Movies,
                                                                                  --                  4: 3rd party, 5: A'La Carte, 6: PPV/other
                else 0
            end) as xCategory_Consumption,
        sum(case
              when Pay_TV_Type in (1, 3, 4, 5) then Viewing_Duration              -- Pay_TV_Type    - 0: FTA, 1: Pay package, 2: Pay Sports, 3: Pay Movies,
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
      case
        when a.Period = 1 then '2) Aug ''13 - Feb ''14'
        when a.Period = 2 then '1) Feb ''13 - Jul ''13'
        when a.Period = 3 then '3) Feb ''13 - May ''13'
          else '???'
      end as xPeriod,
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
  from EPL_03_SOVs xx
 group by
      xPeriod,
      Metric,
      Category,
      SOV_1,
      SOV_2;



/*
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
          from EPL_03_SOVs a,
              (select
                     Period,
                     Account_Number,
                     sum(valid_account_flag) as Valid_Days
                 from EPL_01_Universe
                group by Period, Account_Number) b
         where a.Account_Number = b.Account_Number
           and a.Period = b.Period) xx
 group by
      xPeriod,
      Metric,
      Category,
      SOV_1,
      SOV_2;
*/

  -- ##############################################################################################################
  -- ##### Get single account engagement view                                                                 #####
  -- ##############################################################################################################
if object_id('EPL_04_Eng_Matrix') is not null then drop table EPL_04_Eng_Matrix end if;
create table EPL_04_Eng_Matrix (
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
    Sky_Sports_News_SoV                     varchar(30)       null      default 'Not calculated',
    Sky_Sports_News_SoV__Deciles            varchar(15)       null      default 'Not calculated',
    Sky_Sports_News_SoV__Percentiles        varchar(15)       null      default 'Not calculated',
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
    EPL_Pack_SOV__Sunday_Late_Afternoon     varchar(15)       null      default 'Not calculated',

    Sky_Atlantic_Complete_Progs_Viewed      varchar(30)       null      default 'Not calculated',
    Sky_Atlantic_Complete_Progs_Viewed__Deciles  varchar(15)  null      default 'Not calculated',
    Sky_Atlantic_Complete_Progs_Viewed__Percentiles varchar(15) null      default 'Not calculated',

    Sky_Branded_Channels                    varchar(30)       null      default 'Not calculated',
    Sky_Branded_Channels__Deciles           varchar(15)       null      default 'Not calculated',
    Sky_Branded_Channels__Percentiles       varchar(15)       null      default 'Not calculated',

    Sport_SoV_20p                           varchar(30)       null      default 'Not calculated',
    Sport_SoV_20p__Deciles                  varchar(15)       null      default 'Not calculated',
    Sport_SoV_20p__Percentiles              varchar(15)       null      default 'Not calculated',
    Sport_SoV_30p                           varchar(30)       null      default 'Not calculated',
    Sport_SoV_30p__Deciles                  varchar(15)       null      default 'Not calculated',
    Sport_SoV_30p__Percentiles              varchar(15)       null      default 'Not calculated'

);
create        hg   index idx01 on EPL_04_Eng_Matrix(Account_Number);
create        lf   index idx02 on EPL_04_Eng_Matrix(Period);
create        lf   index idx03 on EPL_04_Eng_Matrix(Metric);
grant select on EPL_04_Eng_Matrix to vespa_group_low_security;


  -- Overall level - base
insert into EPL_04_Eng_Matrix
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
insert into EPL_04_Eng_Matrix
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

-- select Period, Metric, Category, count(*) as Cnt, count(distinct Account_Number) as Cnt_Accts from EPL_04_Eng_Matrix group by Period, Metric, Category order by Period, Metric, Category;



  -- ##############################################################################################################
  -- ##############################################################################################################
  -- Measures - OVERALL
  -- • The engagement thresholds discussed were:
  --    o Share of Content (what proportion of all EPL matches does the household watch): H = >50%, M = 20-50% and L = 0.1-19.9%
  --    o Share of Non-Live Content (what proportion of all EPL non-match programs does the household watch): H = >10%, M = 5-10% and L = 0.1-4.9%
  --    o Share of Sports Viewing (what proportion of their paid Sports viewing is to EPL): H = >50%, M = 20-50% and L = 0.1-19.9%
  --    o Share of Viewing (what proportion of their total paid Sky viewing is to paid Sports): HH = >20%, H = 10-20%, M = 5-9.9%, L = 0.1-4.9%


  --    o Share of Content (what proportion of all EPL matches does the household watch): H = >50%, M = 20-50% and L = 0.1-19.9%
update EPL_04_Eng_Matrix base
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
  from EPL_03_SOCs_Summaries det
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
  from EPL_04_Eng_Matrix a,
       (select
              Account_Number,
              Period,
              case
                when Metric = 'Live games - overall' then 'Overall'
                  else 'By EPL pack'
              end as Metric,
              Category,
              Calculated_SOC
          from EPL_03_SOCs_Summaries a
         where a.Metric in ('Live games - overall', 'Live games - by EPL pack')) b
 where a.Account_Number = b.Account_Number
   and a.Period = b.Period
   and a.Metric = b.Metric
   and a.Category = b.Category
   and a.EPL_SOC not in ('Did not watch', 'Not calculated');
commit;

update EPL_04_Eng_Matrix
   set EPL_SOC__Deciles       = case
                                  when base.EPL_SOC in ('Did not watch', 'Not calculated') then base.EPL_SOC
                                  when det.Group_Id between 1 and 9 then '0' || cast(det.Group_Id as varchar(1))
                                    else cast(det.Group_Id as varchar(2))
                                end
  from EPL_04_Eng_Matrix base
          left join EPL_tmp_Deciles_Percentiles det
          on base.Account_Number = det.Account_Number
         and base.Period = det.Period
         and base.Metric = det.Metric
         and base.Category = det.Category;
commit;

  -- QA
select Period, Metric, Category, EPL_SOC, EPL_SOC__Deciles, count(*) as Cnt
  from EPL_04_Eng_Matrix
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
  from EPL_04_Eng_Matrix a,
       (select
              Account_Number,
              Period,
              case
                when Metric = 'Live games - overall' then 'Overall'
                  else 'By EPL pack'
              end as Metric,
              Category,
              Calculated_SOC
          from EPL_03_SOCs_Summaries a
         where a.Metric in ('Live games - overall', 'Live games - by EPL pack')) b
 where a.Account_Number = b.Account_Number
   and a.Period = b.Period
   and a.Metric = b.Metric
   and a.Category = b.Category
   and a.EPL_SOC not in ('Did not watch', 'Not calculated');
commit;

update EPL_04_Eng_Matrix
   set EPL_SOC__Percentiles   = case
                                  when base.EPL_SOC in ('Did not watch', 'Not calculated') then base.EPL_SOC
                                  when det.Group_Id between 1 and 9 then '00' || cast(det.Group_Id as varchar(1))
                                  when det.Group_Id between 10 and 99 then '0' || cast(det.Group_Id as varchar(2))
                                    else cast(det.Group_Id as varchar(3))
                                end
  from EPL_04_Eng_Matrix base
          left join EPL_tmp_Deciles_Percentiles det
          on base.Account_Number = det.Account_Number
         and base.Period = det.Period
         and base.Metric = det.Metric
         and base.Category = det.Category;
commit;

  -- QA
select Period, Metric, Category, EPL_SOC, EPL_SOC__Percentiles, count(*) as Cnt
  from EPL_04_Eng_Matrix
 group by Period, Metric, Category, EPL_SOC, EPL_SOC__Percentiles
 order by Period, Metric, Category, EPL_SOC__Percentiles, EPL_SOC;




  -- ##############################################################################################################
  -- ##############################################################################################################
  --    o Share of Non-Live Content (what proportion of all EPL non-match programs does the household watch): H = >10%, M = 5-10% and L = 0.1-4.9%
update EPL_04_Eng_Matrix base
   set base.EPL_SoNLC           = case
                                    when det.Calculated_SOC =  0    then 'Did not watch'
                                    when det.Calculated_SOC <  0.05 then 'Low'
                                    when det.Calculated_SOC <= 0.10 then 'Medium'
                                      else 'High'
                                  end
  from EPL_03_SOCs_Summaries det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and det.Metric = 'Non-live programmes - overall';
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
  from EPL_04_Eng_Matrix a,
       EPL_03_SOCs_Summaries b
 where a.Account_Number = b.Account_Number
   and a.Period = b.Period
   and a.EPL_SoNLC not in ('Did not watch', 'Not calculated')
   and a.Metric = 'Overall'                                       -- All non-live EPL metrics are common for all categories
   and b.Metric in ('Non-live programmes - overall');
commit;

update EPL_04_Eng_Matrix
   set EPL_SoNLC__Deciles     = case
                                  when base.EPL_SoNLC in ('Did not watch', 'Not calculated') then base.EPL_SoNLC
                                  when det.Group_Id between 1 and 9 then '0' || cast(det.Group_Id as varchar(1))
                                    else cast(det.Group_Id as varchar(2))
                                end
  from EPL_04_Eng_Matrix base
          left join EPL_tmp_Deciles_Percentiles det
          on base.Account_Number = det.Account_Number
         and base.Period = det.Period;
commit;

  -- QA
select Period, Metric, Category, EPL_SoNLC, EPL_SoNLC__Deciles, count(*) as Cnt
  from EPL_04_Eng_Matrix
 group by Period, Metric, Category, EPL_SoNLC, EPL_SoNLC__Deciles
 order by Period, Metric, Category, EPL_SoNLC__Deciles, EPL_SoNLC;


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
  from EPL_04_Eng_Matrix a,
       EPL_03_SOCs_Summaries b
 where a.Account_Number = b.Account_Number
   and a.Period = b.Period
   and a.EPL_SoNLC not in ('Did not watch', 'Not calculated')
   and a.Metric = 'Overall'                                       -- All non-live EPL metrics are common for all categories
   and b.Metric in ('Non-live programmes - overall');
commit;

update EPL_04_Eng_Matrix
   set EPL_SoNLC__Percentiles = case
                                  when base.EPL_SoNLC in ('Did not watch', 'Not calculated') then base.EPL_SoNLC
                                  when det.Group_Id between 1 and 9 then '00' || cast(det.Group_Id as varchar(1))
                                  when det.Group_Id between 10 and 99 then '0' || cast(det.Group_Id as varchar(2))
                                    else cast(det.Group_Id as varchar(3))
                                end
  from EPL_04_Eng_Matrix base
          left join EPL_tmp_Deciles_Percentiles det
          on base.Account_Number = det.Account_Number
         and base.Period = det.Period;
commit;

  -- QA
select Period, Metric, Category, EPL_SoNLC, EPL_SoNLC__Percentiles, count(*) as Cnt
  from EPL_04_Eng_Matrix
 group by Period, Metric, Category, EPL_SoNLC, EPL_SoNLC__Percentiles
 order by Period, Metric, Category, EPL_SoNLC__Percentiles, EPL_SoNLC;



  -- ##############################################################################################################
  -- ##############################################################################################################
  --    o Share of Sports Viewing (what proportion of their paid Sports viewing is to EPL): H = >50%, M = 20-50% and L = 0.1-19.9%
update EPL_04_Eng_Matrix base
   set base.EPL_SoSV            = case
                                    when det.Calculated_SOV =  0    then 'Did not watch'
                                    when det.Calculated_SOV <  0.20 then 'Low'
                                    when det.Calculated_SOV <= 0.50 then 'Medium'
                                      else 'High'
                                  end
  from EPL_03_SOVs det
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
  from EPL_04_Eng_Matrix a,
       EPL_03_SOVs b
 where a.Account_Number = b.Account_Number
   and a.Period = b.Period
   and a.EPL_SoSV not in ('Did not watch', 'Not calculated')
   and a.Metric = 'Overall'                                       -- All non-live EPL metrics are common for all categories
   and b.Metric in ('EPL SoSV');
commit;

update EPL_04_Eng_Matrix
   set EPL_SoSV__Deciles      = case
                                  when base.EPL_SoSV in ('Did not watch', 'Not calculated') then base.EPL_SoSV
                                  when det.Group_Id between 1 and 9 then '0' || cast(det.Group_Id as varchar(1))
                                    else cast(det.Group_Id as varchar(2))
                                end
  from EPL_04_Eng_Matrix base
          left join EPL_tmp_Deciles_Percentiles det
          on base.Account_Number = det.Account_Number
         and base.Period = det.Period;
commit;

  -- QA
select Period, Metric, Category, EPL_SoSV, EPL_SoSV__Deciles, count(*) as Cnt
  from EPL_04_Eng_Matrix
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
  from EPL_04_Eng_Matrix a,
       EPL_03_SOVs b
 where a.Account_Number = b.Account_Number
   and a.Period = b.Period
   and a.EPL_SoSV not in ('Did not watch', 'Not calculated')
   and a.Metric = 'Overall'                                       -- All non-live EPL metrics are common for all categories
   and b.Metric in ('EPL SoSV');
commit;

update EPL_04_Eng_Matrix
   set EPL_SoSV__Percentiles  = case
                                  when base.EPL_SoSV in ('Did not watch', 'Not calculated') then base.EPL_SoSV
                                  when det.Group_Id between 1 and 9 then '00' || cast(det.Group_Id as varchar(1))
                                  when det.Group_Id between 10 and 99 then '0' || cast(det.Group_Id as varchar(2))
                                    else cast(det.Group_Id as varchar(3))
                                end
  from EPL_04_Eng_Matrix base
          left join EPL_tmp_Deciles_Percentiles det
          on base.Account_Number = det.Account_Number
         and base.Period = det.Period;
commit;

  -- QA
select Period, Metric, Category, EPL_SoSV, EPL_SoSV__Percentiles, count(*) as Cnt
  from EPL_04_Eng_Matrix
 group by Period, Metric, Category, EPL_SoSV, EPL_SoSV__Percentiles
 order by Period, Metric, Category, EPL_SoSV__Percentiles, EPL_SoSV;



  -- ##############################################################################################################
  -- ##############################################################################################################
  --    o Share of Viewing (what proportion of their total paid Sky viewing is to paid Sports): HH = >20%, H = 10-20%, M = 5-9.9%, L = 0.1-4.9%
  -- REVISED BANDINGS (21/03)
  --    Further to yesterday’s session where we discussed a higher top banding for SoV (in order to identify those who really only subscribe to
  --    Sky for the Sport), I’d like to propose the following new banding, based on chart 3.9 in the attached:
  --    L:  0.1-4.9% (39% of HHs)
  --    M: 5-24.9% (31%)
  --    H: 25-44.9% (19%)
  --    VH: >=45% (11%)
update EPL_04_Eng_Matrix base
   set base.Sport_SoV           = case
                                    when det.Calculated_SOV =  0    then 'Did not watch'
                                    when det.Calculated_SOV <  0.05 then 'Low'
                                    when det.Calculated_SOV <  0.25 then 'Medium'
                                    when det.Calculated_SOV <  0.45 then 'High'
                                      else 'Very high'
                                  end
  from EPL_03_SOVs det
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
  from EPL_04_Eng_Matrix a,
       EPL_03_SOVs b
 where a.Account_Number = b.Account_Number
   and a.Period = b.Period
   and a.Sport_SoV not in ('Did not watch', 'Not calculated')
   and a.Metric = 'Overall'                                       -- All non-live EPL metrics are common for all categories
   and b.Metric in ('Sports SoV');
commit;

update EPL_04_Eng_Matrix
   set Sport_SoV__Deciles     = case
                                  when base.Sport_SoV in ('Did not watch', 'Not calculated') then base.Sport_SoV
                                  when det.Group_Id between 1 and 9 then '0' || cast(det.Group_Id as varchar(1))
                                    else cast(det.Group_Id as varchar(2))
                                end
  from EPL_04_Eng_Matrix base
          left join EPL_tmp_Deciles_Percentiles det
          on base.Account_Number = det.Account_Number
         and base.Period = det.Period;
commit;

  -- QA
select Period, Metric, Category, Sport_SoV, Sport_SoV__Deciles, count(*) as Cnt
  from EPL_04_Eng_Matrix
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
  from EPL_04_Eng_Matrix a,
       EPL_03_SOVs b
 where a.Account_Number = b.Account_Number
   and a.Period = b.Period
   and a.Sport_SoV not in ('Did not watch', 'Not calculated')
   and a.Metric = 'Overall'                                       -- All non-live EPL metrics are common for all categories
   and b.Metric in ('Sports SoV');
commit;

update EPL_04_Eng_Matrix
   set Sport_SoV__Percentiles = case
                                  when base.Sport_SoV in ('Did not watch', 'Not calculated') then base.Sport_SoV
                                  when det.Group_Id between 1 and 9 then '00' || cast(det.Group_Id as varchar(1))
                                  when det.Group_Id between 10 and 99 then '0' || cast(det.Group_Id as varchar(2))
                                    else cast(det.Group_Id as varchar(3))
                                end
  from EPL_04_Eng_Matrix base
          left join EPL_tmp_Deciles_Percentiles det
          on base.Account_Number = det.Account_Number
         and base.Period = det.Period;
commit;

  -- QA
select Period, Metric, Category, Sport_SoV, Sport_SoV__Percentiles, count(*) as Cnt
  from EPL_04_Eng_Matrix
 group by Period, Metric, Category, Sport_SoV, Sport_SoV__Percentiles
 order by Period, Metric, Category, Sport_SoV__Percentiles, Sport_SoV;



  -- REVISED BANDINGS (15/07)
  --    "High" threshold @ 20%:
  --    L:  0.1-4.9%
  --    M: 5-19.9%
  --    H: 20-44.9%
  --    VH: >=45%
update EPL_04_Eng_Matrix base
   set base.Sport_SoV_20p       = case
                                    when det.Calculated_SOV =  0    then 'Did not watch'
                                    when det.Calculated_SOV <  0.05 then 'Low'
                                    when det.Calculated_SOV <  0.20 then 'Medium'
                                    when det.Calculated_SOV <  0.45 then 'High'
                                      else 'Very high'
                                  end
  from EPL_03_SOVs det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and det.Metric = 'Sports SoV';
commit;


  -- REVISED BANDINGS (15/07)
  --    "High" threshold @ 30%:
  --    L:  0.1-4.9%
  --    M: 5-29.9%
  --    H: 30-44.9%
  --    VH: >=45%
update EPL_04_Eng_Matrix base
   set base.Sport_SoV_30p       = case
                                    when det.Calculated_SOV =  0    then 'Did not watch'
                                    when det.Calculated_SOV <  0.05 then 'Low'
                                    when det.Calculated_SOV <  0.30 then 'Medium'
                                    when det.Calculated_SOV <  0.45 then 'High'
                                      else 'Very high'
                                  end
  from EPL_03_SOVs det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and det.Metric = 'Sports SoV';
commit;

  -- QA
select
      Sport_SoV,
      Sport_SoV_20p,
      Sport_SoV_30p,
      count(*) as Cnt
  from EPL_04_Eng_Matrix
 group by
        Sport_SoV,
        Sport_SoV_20p,
        Sport_SoV_30p
 order by 1, 2, 3;



  -- ##############################################################################################################
  -- ##############################################################################################################
  --  Sky Atlantic SoV
  --    Viewing to Sky Atlantic:  Can we create a None/L/M/H segmentation for this based on the distribution of our universe (total mins. viewed over the period)?
  --    L:	0.1-1.5%, M:	1.6-4%, H:	>4%
  --    Revised thresholds due to denominator change (Pay TV only): L:	0.1-2%, M:	2.1-5%, H:	>5%
update EPL_04_Eng_Matrix base
   set base.Sky_Atlantic_SoV    = case
                                    when det.Calculated_SOV  = 0     then 'Did not watch'
                                    when det.Calculated_SOV <= 0.020 then 'Low'
                                    when det.Calculated_SOV <= 0.050 then 'Medium'
                                      else 'High'
                                  end
  from EPL_03_SOVs det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and det.Metric = 'Sky Atlantic SoV';
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
  from EPL_04_Eng_Matrix a,
       EPL_03_SOVs b
 where a.Account_Number = b.Account_Number
   and a.Period = b.Period
   and a.Sky_Atlantic_SoV not in ('Did not watch', 'Not calculated')
   and a.Metric = 'Overall'                                       -- All non-live EPL metrics are common for all categories
   and b.Metric in ('Sky Atlantic SoV');
commit;

update EPL_04_Eng_Matrix
   set Sky_Atlantic_SoV__Deciles = case
                                  when base.Sky_Atlantic_SoV in ('Did not watch', 'Not calculated') then base.Sky_Atlantic_SoV
                                  when det.Group_Id between 1 and 9 then '0' || cast(det.Group_Id as varchar(1))
                                    else cast(det.Group_Id as varchar(2))
                                end
  from EPL_04_Eng_Matrix base
          left join EPL_tmp_Deciles_Percentiles det
          on base.Account_Number = det.Account_Number
         and base.Period = det.Period;
commit;

  -- QA
select Period, Metric, Category, Sky_Atlantic_SoV, Sky_Atlantic_SoV__Deciles, count(*) as Cnt
  from EPL_04_Eng_Matrix
 group by Period, Metric, Category, Sky_Atlantic_SoV, Sky_Atlantic_SoV__Deciles
 order by Period, Metric, Category, Sky_Atlantic_SoV__Deciles, Sky_Atlantic_SoV;


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
  from EPL_04_Eng_Matrix a,
       EPL_03_SOVs b
 where a.Account_Number = b.Account_Number
   and a.Period = b.Period
   and a.Sky_Atlantic_SoV not in ('Did not watch', 'Not calculated')
   and a.Metric = 'Overall'                                       -- All non-live EPL metrics are common for all categories
   and b.Metric in ('Sky Atlantic SoV');
commit;

update EPL_04_Eng_Matrix
   set Sky_Atlantic_SoV__Percentiles = case
                                  when base.Sky_Atlantic_SoV in ('Did not watch', 'Not calculated') then base.Sky_Atlantic_SoV
                                  when det.Group_Id between 1 and 9 then '00' || cast(det.Group_Id as varchar(1))
                                  when det.Group_Id between 10 and 99 then '0' || cast(det.Group_Id as varchar(2))
                                    else cast(det.Group_Id as varchar(3))
                                end
  from EPL_04_Eng_Matrix base
          left join EPL_tmp_Deciles_Percentiles det
          on base.Account_Number = det.Account_Number
         and base.Period = det.Period;
commit;

  -- QA
select Period, Metric, Category, Sky_Atlantic_SoV, Sky_Atlantic_SoV__Percentiles, count(*) as Cnt
  from EPL_04_Eng_Matrix
 group by Period, Metric, Category, Sky_Atlantic_SoV, Sky_Atlantic_SoV__Percentiles
 order by Period, Metric, Category, Sky_Atlantic_SoV__Percentiles, Sky_Atlantic_SoV;



  --  Sky Atlantic SoV
  --    Additional metric requested on 27/06: Viewing to Sky Atlantic - use no. of complete programmes viewed rather than SoV to define thresholds
  --    None: 0, Low: >0 and <=0.02, Med: >0.02 and <=0.1, High: >0.1
update EPL_04_Eng_Matrix base
   set base.Sky_Atlantic_Complete_Progs_Viewed
                                = case
                                    when det.Calculated_SOV  = 0     then 'Did not watch'
                                    when det.Calculated_SOV <= 0.020 then 'Low'
                                    when det.Calculated_SOV <= 0.100 then 'Medium'
                                      else 'High'
                                  end
  from EPL_03_SOVs det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and det.Metric = 'Sky Atlantic - number of complete programmes';
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
  from EPL_04_Eng_Matrix a,
       EPL_03_SOVs b
 where a.Account_Number = b.Account_Number
   and a.Period = b.Period
   and a.Sky_Atlantic_Complete_Progs_Viewed not in ('Did not watch', 'Not calculated')
   and a.Metric = 'Overall'                                       -- All non-live EPL metrics are common for all categories
   and b.Metric in ('Sky Atlantic - number of complete programmes');
commit;

update EPL_04_Eng_Matrix
   set Sky_Atlantic_Complete_Progs_Viewed__Deciles
                              = case
                                  when base.Sky_Atlantic_Complete_Progs_Viewed in ('Did not watch', 'Not calculated') then base.Sky_Atlantic_Complete_Progs_Viewed
                                  when det.Group_Id between 1 and 9 then '0' || cast(det.Group_Id as varchar(1))
                                    else cast(det.Group_Id as varchar(2))
                                end
  from EPL_04_Eng_Matrix base
          left join EPL_tmp_Deciles_Percentiles det
          on base.Account_Number = det.Account_Number
         and base.Period = det.Period;
commit;

  -- QA
select Period, Metric, Category, Sky_Atlantic_Complete_Progs_Viewed, Sky_Atlantic_Complete_Progs_Viewed__Deciles, count(*) as Cnt
  from EPL_04_Eng_Matrix
 group by Period, Metric, Category, Sky_Atlantic_Complete_Progs_Viewed, Sky_Atlantic_Complete_Progs_Viewed__Deciles
 order by Period, Metric, Category, Sky_Atlantic_Complete_Progs_Viewed__Deciles, Sky_Atlantic_Complete_Progs_Viewed;


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
  from EPL_04_Eng_Matrix a,
       EPL_03_SOVs b
 where a.Account_Number = b.Account_Number
   and a.Period = b.Period
   and a.Sky_Atlantic_Complete_Progs_Viewed not in ('Did not watch', 'Not calculated')
   and a.Metric = 'Overall'                                       -- All non-live EPL metrics are common for all categories
   and b.Metric in ('Sky Atlantic - number of complete programmes');
commit;

update EPL_04_Eng_Matrix
   set Sky_Atlantic_Complete_Progs_Viewed__Percentiles
                              = case
                                  when base.Sky_Atlantic_Complete_Progs_Viewed in ('Did not watch', 'Not calculated') then base.Sky_Atlantic_Complete_Progs_Viewed
                                  when det.Group_Id between 1 and 9 then '00' || cast(det.Group_Id as varchar(1))
                                  when det.Group_Id between 10 and 99 then '0' || cast(det.Group_Id as varchar(2))
                                    else cast(det.Group_Id as varchar(3))
                                end
  from EPL_04_Eng_Matrix base
          left join EPL_tmp_Deciles_Percentiles det
          on base.Account_Number = det.Account_Number
         and base.Period = det.Period;
commit;

  -- QA
select Period, Metric, Category, Sky_Atlantic_Complete_Progs_Viewed, Sky_Atlantic_Complete_Progs_Viewed__Percentiles, count(*) as Cnt
  from EPL_04_Eng_Matrix
 group by Period, Metric, Category, Sky_Atlantic_Complete_Progs_Viewed, Sky_Atlantic_Complete_Progs_Viewed__Percentiles
 order by Period, Metric, Category, Sky_Atlantic_Complete_Progs_Viewed__Percentiles, Sky_Atlantic_Complete_Progs_Viewed;




  -- ##############################################################################################################
  -- ##############################################################################################################
  --  Sky Sports News SoV
  --    Additional metric requested on 27/06: Viewing to Sky Atlantic - use no. of complete programmes viewed rather than SoV to define thresholds
  --    None: 0, Low: >0 and <=0.02, Med: >0.02 and <=0.1, High: >0.1
update EPL_04_Eng_Matrix base
   set base.Sky_Sports_News_SoV
                                = case
                                    when det.Calculated_SOV  = 0     then 'Did not watch'
                                    when det.Calculated_SOV <= 0.032 then 'Low'
                                    when det.Calculated_SOV <= 0.200 then 'Medium'
                                      else 'High'
                                  end
  from EPL_03_SOVs det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and det.Metric = 'Sky Sports News SoV';
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
  from EPL_04_Eng_Matrix a,
       EPL_03_SOVs b
 where a.Account_Number = b.Account_Number
   and a.Period = b.Period
   and a.Sky_Sports_News_SoV not in ('Did not watch', 'Not calculated')
   and a.Metric = 'Overall'                                       -- All non-live EPL metrics are common for all categories
   and b.Metric in ('Sky Sports News SoV');
commit;

update EPL_04_Eng_Matrix
   set Sky_Sports_News_SoV__Deciles
                              = case
                                  when base.Sky_Sports_News_SoV in ('Did not watch', 'Not calculated') then base.Sky_Sports_News_SoV
                                  when det.Group_Id between 1 and 9 then '0' || cast(det.Group_Id as varchar(1))
                                    else cast(det.Group_Id as varchar(2))
                                end
  from EPL_04_Eng_Matrix base
          left join EPL_tmp_Deciles_Percentiles det
          on base.Account_Number = det.Account_Number
         and base.Period = det.Period;
commit;

  -- QA
select Period, Metric, Category, Sky_Sports_News_SoV, Sky_Sports_News_SoV__Deciles, count(*) as Cnt
  from EPL_04_Eng_Matrix
 group by Period, Metric, Category, Sky_Sports_News_SoV, Sky_Sports_News_SoV__Deciles
 order by Period, Metric, Category, Sky_Sports_News_SoV__Deciles, Sky_Sports_News_SoV;


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
  from EPL_04_Eng_Matrix a,
       EPL_03_SOVs b
 where a.Account_Number = b.Account_Number
   and a.Period = b.Period
   and a.Sky_Sports_News_SoV not in ('Did not watch', 'Not calculated')
   and a.Metric = 'Overall'                                       -- All non-live EPL metrics are common for all categories
   and b.Metric in ('Sky Sports News SoV');
commit;

update EPL_04_Eng_Matrix
   set Sky_Sports_News_SoV__Percentiles
                              = case
                                  when base.Sky_Sports_News_SoV in ('Did not watch', 'Not calculated') then base.Sky_Sports_News_SoV
                                  when det.Group_Id between 1 and 9 then '00' || cast(det.Group_Id as varchar(1))
                                  when det.Group_Id between 10 and 99 then '0' || cast(det.Group_Id as varchar(2))
                                    else cast(det.Group_Id as varchar(3))
                                end
  from EPL_04_Eng_Matrix base
          left join EPL_tmp_Deciles_Percentiles det
          on base.Account_Number = det.Account_Number
         and base.Period = det.Period;
commit;

  -- QA
select Period, Metric, Category, Sky_Sports_News_SoV, Sky_Sports_News_SoV__Percentiles, count(*) as Cnt
  from EPL_04_Eng_Matrix
 group by Period, Metric, Category, Sky_Sports_News_SoV, Sky_Sports_News_SoV__Percentiles
 order by Period, Metric, Category, Sky_Sports_News_SoV__Percentiles, Sky_Sports_News_SoV;



  -- ##############################################################################################################
  -- ##############################################################################################################
  --  Entertainment pack SoV
  --    % of total viewing that is to Basic Ent Content:  Can we do the same as above to create four buckets?
  --    L:	0.1-90%, M:	90.1-98%, H:	>98%
  --    Revised thresholds due to denominator change (Pay TV only): L:	0.1-90%, M:	90.1-98%, H:	>98%
update EPL_04_Eng_Matrix base
   set base.Entertainment_Pack_SoV
                                = case
                                    when det.Calculated_SOV  = 0    then 'Did not watch'
                                    when det.Calculated_SOV <= 0.90 then 'Low'
                                    when det.Calculated_SOV <= 0.98 then 'Medium'
                                      else 'High'
                                  end
  from EPL_03_SOVs det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and det.Metric = 'Entertainment Pack SoV';
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
  from EPL_04_Eng_Matrix a,
       EPL_03_SOVs b
 where a.Account_Number = b.Account_Number
   and a.Period = b.Period
   and a.Entertainment_Pack_SoV not in ('Did not watch', 'Not calculated')
   and a.Metric = 'Overall'                                       -- All non-live EPL metrics are common for all categories
   and b.Metric in ('Entertainment Pack SoV');
commit;

update EPL_04_Eng_Matrix
   set Entertainment_Pack_SoV__Deciles = case
                                  when base.Entertainment_Pack_SoV in ('Did not watch', 'Not calculated') then base.Entertainment_Pack_SoV
                                  when det.Group_Id between 1 and 9 then '0' || cast(det.Group_Id as varchar(1))
                                    else cast(det.Group_Id as varchar(2))
                                end
  from EPL_04_Eng_Matrix base
          left join EPL_tmp_Deciles_Percentiles det
          on base.Account_Number = det.Account_Number
         and base.Period = det.Period;
commit;

  -- QA
select Period, Metric, Category, Entertainment_Pack_SoV, Entertainment_Pack_SoV__Deciles, count(*) as Cnt
  from EPL_04_Eng_Matrix
 group by Period, Metric, Category, Entertainment_Pack_SoV, Entertainment_Pack_SoV__Deciles
 order by Period, Metric, Category, Entertainment_Pack_SoV__Deciles, Entertainment_Pack_SoV;


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
  from EPL_04_Eng_Matrix a,
       EPL_03_SOVs b
 where a.Account_Number = b.Account_Number
   and a.Period = b.Period
   and a.Entertainment_Pack_SoV not in ('Did not watch', 'Not calculated')
   and a.Metric = 'Overall'                                       -- All non-live EPL metrics are common for all categories
   and b.Metric in ('Entertainment Pack SoV');
commit;

update EPL_04_Eng_Matrix
   set Entertainment_Pack_SoV__Percentiles = case
                                  when base.Entertainment_Pack_SoV in ('Did not watch', 'Not calculated') then base.Entertainment_Pack_SoV
                                  when det.Group_Id between 1 and 9 then '00' || cast(det.Group_Id as varchar(1))
                                  when det.Group_Id between 10 and 99 then '0' || cast(det.Group_Id as varchar(2))
                                    else cast(det.Group_Id as varchar(3))
                                end
  from EPL_04_Eng_Matrix base
          left join EPL_tmp_Deciles_Percentiles det
          on base.Account_Number = det.Account_Number
         and base.Period = det.Period;
commit;

  -- QA
select Period, Metric, Category, Entertainment_Pack_SoV, Entertainment_Pack_SoV__Percentiles, count(*) as Cnt
  from EPL_04_Eng_Matrix
 group by Period, Metric, Category, Entertainment_Pack_SoV, Entertainment_Pack_SoV__Percentiles
 order by Period, Metric, Category, Entertainment_Pack_SoV__Percentiles, Entertainment_Pack_SoV;



  -- ##############################################################################################################
  -- ##############################################################################################################
  --  Sky exclusive channels SoV
  --    % of Basic Ent Content viewed that is to Sky Exclusive channels*:  Again as above
  --    L:	0.1-3%, M:	3.1-10%, H:	>10%
  --    Revised thresholds due to denominator change (Pay TV only): L:	0.1-4%, M:	4.1-25%, H:	>25%
update EPL_04_Eng_Matrix base
   set base.Sky_Excl_Channels_SoV
                                = case
                                    when det.Calculated_SOV  = 0    then 'Did not watch'
                                    when det.Calculated_SOV <= 0.04 then 'Low'
                                    when det.Calculated_SOV <= 0.25 then 'Medium'
                                      else 'High'
                                  end
  from EPL_03_SOVs det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and det.Metric = 'Sky exclusive channels SoV';
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
  from EPL_04_Eng_Matrix a,
       EPL_03_SOVs b
 where a.Account_Number = b.Account_Number
   and a.Period = b.Period
   and a.Sky_Excl_Channels_SoV not in ('Did not watch', 'Not calculated')
   and a.Metric = 'Overall'                                       -- All non-live EPL metrics are common for all categories
   and b.Metric in ('Sky exclusive channels SoV');
commit;

update EPL_04_Eng_Matrix
   set Sky_Excl_Channels_SoV__Deciles = case
                                  when base.Sky_Excl_Channels_SoV in ('Did not watch', 'Not calculated') then base.Sky_Excl_Channels_SoV
                                  when det.Group_Id between 1 and 9 then '0' || cast(det.Group_Id as varchar(1))
                                    else cast(det.Group_Id as varchar(2))
                                end
  from EPL_04_Eng_Matrix base
          left join EPL_tmp_Deciles_Percentiles det
          on base.Account_Number = det.Account_Number
         and base.Period = det.Period;
commit;

  -- QA
select Period, Metric, Category, Sky_Excl_Channels_SoV, Sky_Excl_Channels_SoV__Deciles, count(*) as Cnt
  from EPL_04_Eng_Matrix
 group by Period, Metric, Category, Sky_Excl_Channels_SoV, Sky_Excl_Channels_SoV__Deciles
 order by Period, Metric, Category, Sky_Excl_Channels_SoV__Deciles, Sky_Excl_Channels_SoV;


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
  from EPL_04_Eng_Matrix a,
       EPL_03_SOVs b
 where a.Account_Number = b.Account_Number
   and a.Period = b.Period
   and a.Sky_Excl_Channels_SoV not in ('Did not watch', 'Not calculated')
   and a.Metric = 'Overall'                                       -- All non-live EPL metrics are common for all categories
   and b.Metric in ('Sky exclusive channels SoV');
commit;

update EPL_04_Eng_Matrix
   set Sky_Excl_Channels_SoV__Percentiles = case
                                  when base.Sky_Excl_Channels_SoV in ('Did not watch', 'Not calculated') then base.Sky_Excl_Channels_SoV
                                  when det.Group_Id between 1 and 9 then '00' || cast(det.Group_Id as varchar(1))
                                  when det.Group_Id between 10 and 99 then '0' || cast(det.Group_Id as varchar(2))
                                    else cast(det.Group_Id as varchar(3))
                                end
  from EPL_04_Eng_Matrix base
          left join EPL_tmp_Deciles_Percentiles det
          on base.Account_Number = det.Account_Number
         and base.Period = det.Period;
commit;

  -- QA
select Period, Metric, Category, Sky_Excl_Channels_SoV, Sky_Excl_Channels_SoV__Percentiles, count(*) as Cnt
  from EPL_04_Eng_Matrix
 group by Period, Metric, Category, Sky_Excl_Channels_SoV, Sky_Excl_Channels_SoV__Percentiles
 order by Period, Metric, Category, Sky_Excl_Channels_SoV__Percentiles, Sky_Excl_Channels_SoV;



  -- ##############################################################################################################
  -- ##############################################################################################################
  --  Sky/Virgin exclusive channels SoV
  --    RCrouch's list of channels, SoV as above
  --    L:	0.1-5%, M:	5.1-15%, H:	>15%
  --    Revised thresholds due to denominator change (Pay TV only): L:	0.1-17%, M:	17.1-40%, H:	>40%
update EPL_04_Eng_Matrix base
   set base.Sky_Virgin_Excl_Channels_SoV
                                = case
                                    when det.Calculated_SOV  = 0    then 'Did not watch'
                                    when det.Calculated_SOV <= 0.17 then 'Low'
                                    when det.Calculated_SOV <= 0.40 then 'Medium'
                                      else 'High'
                                  end
  from EPL_03_SOVs det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and det.Metric = 'Sky/Virgin exclusive channels SoV';
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
  from EPL_04_Eng_Matrix a,
       EPL_03_SOVs b
 where a.Account_Number = b.Account_Number
   and a.Period = b.Period
   and a.Sky_Virgin_Excl_Channels_SoV not in ('Did not watch', 'Not calculated')
   and a.Metric = 'Overall'                                       -- All non-live EPL metrics are common for all categories
   and b.Metric in ('Sky/Virgin exclusive channels SoV');
commit;

update EPL_04_Eng_Matrix
   set Sky_Virgin_Excl_Channels_SoV__Deciles = case
                                  when base.Sky_Virgin_Excl_Channels_SoV in ('Did not watch', 'Not calculated') then base.Sky_Virgin_Excl_Channels_SoV
                                  when det.Group_Id between 1 and 9 then '0' || cast(det.Group_Id as varchar(1))
                                    else cast(det.Group_Id as varchar(2))
                                end
  from EPL_04_Eng_Matrix base
          left join EPL_tmp_Deciles_Percentiles det
          on base.Account_Number = det.Account_Number
         and base.Period = det.Period;
commit;

  -- QA
select Period, Metric, Category, Sky_Virgin_Excl_Channels_SoV, Sky_Virgin_Excl_Channels_SoV__Deciles, count(*) as Cnt
  from EPL_04_Eng_Matrix
 group by Period, Metric, Category, Sky_Virgin_Excl_Channels_SoV, Sky_Virgin_Excl_Channels_SoV__Deciles
 order by Period, Metric, Category, Sky_Virgin_Excl_Channels_SoV__Deciles, Sky_Virgin_Excl_Channels_SoV;


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
  from EPL_04_Eng_Matrix a,
       EPL_03_SOVs b
 where a.Account_Number = b.Account_Number
   and a.Period = b.Period
   and a.Sky_Virgin_Excl_Channels_SoV not in ('Did not watch', 'Not calculated')
   and a.Metric = 'Overall'                                       -- All non-live EPL metrics are common for all categories
   and b.Metric in ('Sky/Virgin exclusive channels SoV');
commit;

update EPL_04_Eng_Matrix
   set Sky_Virgin_Excl_Channels_SoV__Percentiles = case
                                  when base.Sky_Virgin_Excl_Channels_SoV in ('Did not watch', 'Not calculated') then base.Sky_Virgin_Excl_Channels_SoV
                                  when det.Group_Id between 1 and 9 then '00' || cast(det.Group_Id as varchar(1))
                                  when det.Group_Id between 10 and 99 then '0' || cast(det.Group_Id as varchar(2))
                                    else cast(det.Group_Id as varchar(3))
                                end
  from EPL_04_Eng_Matrix base
          left join EPL_tmp_Deciles_Percentiles det
          on base.Account_Number = det.Account_Number
         and base.Period = det.Period;
commit;

  -- QA
select Period, Metric, Category, Sky_Virgin_Excl_Channels_SoV, Sky_Virgin_Excl_Channels_SoV__Percentiles, count(*) as Cnt
  from EPL_04_Eng_Matrix
 group by Period, Metric, Category, Sky_Virgin_Excl_Channels_SoV, Sky_Virgin_Excl_Channels_SoV__Percentiles
 order by Period, Metric, Category, Sky_Virgin_Excl_Channels_SoV__Percentiles, Sky_Virgin_Excl_Channels_SoV;



  -- ##############################################################################################################
  -- ##############################################################################################################
  --  Movies SoV
  --  % of total viewing that is to Prem Movies content
  --    L	0.1-2%, M	2.1%-11%, H	>11%
update EPL_04_Eng_Matrix base
   set base.Movies_SOV
                                = case
                                    when det.Calculated_SOV  = 0    then 'Did not watch'
                                    when det.Calculated_SOV <= 0.02 then 'Low'
                                    when det.Calculated_SOV <= 0.11 then 'Medium'
                                      else 'High'
                                  end
  from EPL_03_SOVs det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and det.Metric = 'Movies SoV';
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
  from EPL_04_Eng_Matrix a,
       EPL_03_SOVs b
 where a.Account_Number = b.Account_Number
   and a.Period = b.Period
   and a.Movies_SoV not in ('Did not watch', 'Not calculated')
   and a.Metric = 'Overall'                                       -- All non-live EPL metrics are common for all categories
   and b.Metric in ('Movies SoV');
commit;

update EPL_04_Eng_Matrix
   set Movies_SoV__Deciles    = case
                                  when base.Movies_SoV in ('Did not watch', 'Not calculated') then base.Movies_SoV
                                  when det.Group_Id between 1 and 9 then '0' || cast(det.Group_Id as varchar(1))
                                    else cast(det.Group_Id as varchar(2))
                                end
  from EPL_04_Eng_Matrix base
          left join EPL_tmp_Deciles_Percentiles det
          on base.Account_Number = det.Account_Number
         and base.Period = det.Period;
commit;

  -- QA
select Period, Metric, Category, Movies_SoV, Movies_SoV__Deciles, count(*) as Cnt
  from EPL_04_Eng_Matrix
 group by Period, Metric, Category, Movies_SoV, Movies_SoV__Deciles
 order by Period, Metric, Category, Movies_SoV__Deciles, Movies_SoV;


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
  from EPL_04_Eng_Matrix a,
       EPL_03_SOVs b
 where a.Account_Number = b.Account_Number
   and a.Period = b.Period
   and a.Movies_SoV not in ('Did not watch', 'Not calculated')
   and a.Metric = 'Overall'                                       -- All non-live EPL metrics are common for all categories
   and b.Metric in ('Movies SoV');
commit;

update EPL_04_Eng_Matrix
   set Movies_SoV__Percentiles = case
                                  when base.Movies_SoV in ('Did not watch', 'Not calculated') then base.Movies_SoV
                                  when det.Group_Id between 1 and 9 then '00' || cast(det.Group_Id as varchar(1))
                                  when det.Group_Id between 10 and 99 then '0' || cast(det.Group_Id as varchar(2))
                                    else cast(det.Group_Id as varchar(3))
                                end
  from EPL_04_Eng_Matrix base
          left join EPL_tmp_Deciles_Percentiles det
          on base.Account_Number = det.Account_Number
         and base.Period = det.Period;
commit;

  -- QA
select Period, Metric, Category, Movies_SoV, Movies_SoV__Percentiles, count(*) as Cnt
  from EPL_04_Eng_Matrix
 group by Period, Metric, Category, Movies_SoV, Movies_SoV__Percentiles
 order by Period, Metric, Category, Movies_SoV__Percentiles, Movies_SoV;



  -- ##############################################################################################################
  -- ##############################################################################################################
  --  Sky branded Pay TV channels (excluding Sky Atlantic: Sky 1 & 2, Sky Living, Sky Arts 1 & 2, Sky LivingIt, Sky Poker, Sky 3D)
  --    Suggest looking at avg mins / day, rather than an SoV metric, that we can then cut into HML bands:
  --    Low: 0-7 mins, Med: 8-20 mins, High: >20 mins
update EPL_04_Eng_Matrix base
   set base.Sky_Branded_Channels
                                = case
                                    when det.Calculated_SOV  =  0    then 'Did not watch'
                                    when det.Calculated_SOV <=  7.00 then 'Low'
                                    when det.Calculated_SOV <= 20.00 then 'Medium'
                                      else 'High'
                                  end
  from EPL_03_SOVs det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and det.Metric = 'Sky branded channels (incl. Sky Atlantic)';
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
  from EPL_04_Eng_Matrix a,
       EPL_03_SOVs b
 where a.Account_Number = b.Account_Number
   and a.Period = b.Period
   and a.Sky_Branded_Channels not in ('Did not watch', 'Not calculated')
   and a.Metric = 'Overall'                                       -- All non-live EPL metrics are common for all categories
   and b.Metric in ('Sky branded channels (incl. Sky Atlantic)');
commit;

update EPL_04_Eng_Matrix
   set Sky_Branded_Channels__Deciles    = case
                                  when base.Sky_Branded_Channels in ('Did not watch', 'Not calculated') then base.Sky_Branded_Channels
                                  when det.Group_Id between 1 and 9 then '0' || cast(det.Group_Id as varchar(1))
                                    else cast(det.Group_Id as varchar(2))
                                end
  from EPL_04_Eng_Matrix base
          left join EPL_tmp_Deciles_Percentiles det
          on base.Account_Number = det.Account_Number
         and base.Period = det.Period;
commit;

  -- QA
select Period, Metric, Category, Sky_Branded_Channels, Sky_Branded_Channels__Deciles, count(*) as Cnt
  from EPL_04_Eng_Matrix
 group by Period, Metric, Category, Sky_Branded_Channels, Sky_Branded_Channels__Deciles
 order by Period, Metric, Category, Sky_Branded_Channels__Deciles, Sky_Branded_Channels;


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
  from EPL_04_Eng_Matrix a,
       EPL_03_SOVs b
 where a.Account_Number = b.Account_Number
   and a.Period = b.Period
   and a.Sky_Branded_Channels not in ('Did not watch', 'Not calculated')
   and a.Metric = 'Overall'                                       -- All non-live EPL metrics are common for all categories
   and b.Metric in ('Sky branded channels (incl. Sky Atlantic)');
commit;

update EPL_04_Eng_Matrix
   set Sky_Branded_Channels__Percentiles = case
                                  when base.Sky_Branded_Channels in ('Did not watch', 'Not calculated') then base.Sky_Branded_Channels
                                  when det.Group_Id between 1 and 9 then '00' || cast(det.Group_Id as varchar(1))
                                  when det.Group_Id between 10 and 99 then '0' || cast(det.Group_Id as varchar(2))
                                    else cast(det.Group_Id as varchar(3))
                                end
  from EPL_04_Eng_Matrix base
          left join EPL_tmp_Deciles_Percentiles det
          on base.Account_Number = det.Account_Number
         and base.Period = det.Period;
commit;

  -- QA
select Period, Metric, Category, Sky_Branded_Channels, Sky_Branded_Channels__Percentiles, count(*) as Cnt
  from EPL_04_Eng_Matrix
 group by Period, Metric, Category, Sky_Branded_Channels, Sky_Branded_Channels__Percentiles
 order by Period, Metric, Category, Sky_Branded_Channels__Percentiles, Sky_Branded_Channels;



  -- ##############################################################################################################
  -- ##############################################################################################################
  -- Update column view
update EPL_04_Eng_Matrix base
   set base.EPL_SOC__Pack_Monday_Evening = det.EPL_SOC
  from EPL_04_Eng_Matrix det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and det.Metric = 'By EPL pack'
   and det.Category = 'Monday Evening';
commit;

update EPL_04_Eng_Matrix base
   set base.EPL_SOC__Pack_Midweek_Evening = det.EPL_SOC
  from EPL_04_Eng_Matrix det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and det.Metric = 'By EPL pack'
   and det.Category = 'Midweek Evening';
commit;

update EPL_04_Eng_Matrix base
   set base.EPL_SOC__Pack_Saturday_Lunch = det.EPL_SOC
  from EPL_04_Eng_Matrix det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and det.Metric = 'By EPL pack'
   and det.Category = 'Saturday Lunchtime';
commit;

update EPL_04_Eng_Matrix base
   set base.EPL_SOC__Pack_Saturday_Afternoon = det.EPL_SOC
  from EPL_04_Eng_Matrix det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and det.Metric = 'By EPL pack'
   and det.Category = 'Saturday Afternoon';
commit;

update EPL_04_Eng_Matrix base
   set base.EPL_SOC__Pack_Sunday_Early_Afternoon = det.EPL_SOC
  from EPL_04_Eng_Matrix det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and det.Metric = 'By EPL pack'
   and det.Category = 'Sunday Early Afternoon';
commit;

update EPL_04_Eng_Matrix base
   set base.EPL_SOC__Pack_Sunday_Late_Afternoon = det.EPL_SOC
  from EPL_04_Eng_Matrix det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and det.Metric = 'By EPL pack'
   and det.Category = 'Sunday Late Afternoon';
commit;



  -- ##############################################################################################################
  -- ##############################################################################################################
update EPL_04_Eng_Matrix a
  set a.EPL_Games_Watched__Pack_Mon_Ev        = det.Pack_Mon_Ev,
      a.EPL_Games_Watched__Pack_Mid_Ev        = det.Pack_Mid_Ev,
      a.EPL_Games_Watched__Pack_Sat_Lunch     = det.Pack_Sat_Lunch,
      a.EPL_Games_Watched__Pack_Sat_Aft       = det.Pack_Sat_Aft,
      a.EPL_Games_Watched__Pack_Sun_Early_Aft = det.Pack_Sun_Early_Aft,
      a.EPL_Games_Watched__Pack_Sun_Late_Aft  = det.Pack_Sun_Late_Aft,
      a.EPL_Games_Watched__Overall            = det.Overall
  from (select
              Account_Number,
              Period,
              max(case when Category = 'Monday Evening'         then Content_Watched else 0 end) as Pack_Mon_Ev,
              max(case when Category = 'Midweek Evening'        then Content_Watched else 0 end) as Pack_Mid_Ev,
              max(case when Category = 'Saturday Lunchtime'     then Content_Watched else 0 end) as Pack_Sat_Lunch,
              max(case when Category = 'Saturday Afternoon'     then Content_Watched else 0 end) as Pack_Sat_Aft,
              max(case when Category = 'Sunday Early Afternoon' then Content_Watched else 0 end) as Pack_Sun_Early_Aft,
              max(case when Category = 'Sunday Late Afternoon'  then Content_Watched else 0 end) as Pack_Sun_Late_Aft,
              sum(Content_Watched) as Overall
          from EPL_03_SOCs_Summaries
         where Metric = 'Live games - by EPL pack'
         group by Account_Number, Period) det
 where a.Account_Number = det.Account_Number
   and a.Period = det.Period;
commit;


update EPL_04_Eng_Matrix a
  set a.EPL_Pack_SOV__Monday_Evening          = case
                                                  when EPL_Games_Watched__Overall = 0                                            then 'Not calculated'
                                                  when EPL_Games_Watched__Pack_Mon_Ev = 0                                        then 'Did not watch'
                                                  when 1.0 * EPL_Games_Watched__Pack_Mon_Ev / EPL_Games_Watched__Overall <= 0.25 then 'Low'
                                                  when 1.0 * EPL_Games_Watched__Pack_Mon_Ev / EPL_Games_Watched__Overall <= 0.50 then 'Medium'
                                                    else 'High'
                                                end,
      a.EPL_Pack_SOV__Midweek_Evening         = case
                                                  when EPL_Games_Watched__Overall = 0                                            then 'Not calculated'
                                                  when EPL_Games_Watched__Pack_Mid_Ev = 0                                        then 'Did not watch'
                                                  when 1.0 * EPL_Games_Watched__Pack_Mid_Ev / EPL_Games_Watched__Overall <= 0.25 then 'Low'
                                                  when 1.0 * EPL_Games_Watched__Pack_Mid_Ev / EPL_Games_Watched__Overall <= 0.50 then 'Medium'
                                                    else 'High'
                                                end,
      a.EPL_Pack_SOV__Saturday_Lunch          = case
                                                  when EPL_Games_Watched__Overall = 0                                            then 'Not calculated'
                                                  when EPL_Games_Watched__Pack_Sat_Lunch = 0                                        then 'Did not watch'
                                                  when 1.0 * EPL_Games_Watched__Pack_Sat_Lunch / EPL_Games_Watched__Overall <= 0.25 then 'Low'
                                                  when 1.0 * EPL_Games_Watched__Pack_Sat_Lunch / EPL_Games_Watched__Overall <= 0.50 then 'Medium'
                                                    else 'High'
                                                end,
      a.EPL_Pack_SOV__Saturday_Afternoon      = case
                                                  when EPL_Games_Watched__Overall = 0                                            then 'Not calculated'
                                                  when EPL_Games_Watched__Pack_Sat_Aft = 0                                        then 'Did not watch'
                                                  when 1.0 * EPL_Games_Watched__Pack_Sat_Aft / EPL_Games_Watched__Overall <= 0.25 then 'Low'
                                                  when 1.0 * EPL_Games_Watched__Pack_Sat_Aft / EPL_Games_Watched__Overall <= 0.50 then 'Medium'
                                                    else 'High'
                                                end,
      a.EPL_Pack_SOV__Sunday_Early_Afternoon  = case
                                                  when EPL_Games_Watched__Overall = 0                                            then 'Not calculated'
                                                  when EPL_Games_Watched__Pack_Sun_Early_Aft = 0                                        then 'Did not watch'
                                                  when 1.0 * EPL_Games_Watched__Pack_Sun_Early_Aft / EPL_Games_Watched__Overall <= 0.25 then 'Low'
                                                  when 1.0 * EPL_Games_Watched__Pack_Sun_Early_Aft / EPL_Games_Watched__Overall <= 0.50 then 'Medium'
                                                    else 'High'
                                                end,
      a.EPL_Pack_SOV__Sunday_Late_Afternoon   = case
                                                  when EPL_Games_Watched__Overall = 0                                            then 'Not calculated'
                                                  when EPL_Games_Watched__Pack_Sun_Late_Aft = 0                                        then 'Did not watch'
                                                  when 1.0 * EPL_Games_Watched__Pack_Sun_Late_Aft / EPL_Games_Watched__Overall <= 0.25 then 'Low'
                                                  when 1.0 * EPL_Games_Watched__Pack_Sun_Late_Aft / EPL_Games_Watched__Overall <= 0.50 then 'Medium'
                                                    else 'High'
                                                end;
commit;



  -- ##############################################################################################################
  -- ##############################################################################################################
  -- Check (expect 0s)
select count(*) as Cnt from EPL_04_Eng_Matrix base where EPL_SOC__Pack_Monday_Evening <> EPL_SOC          and Category = 'Monday Evening';
select count(*) as Cnt from EPL_04_Eng_Matrix base where EPL_SOC__Pack_Midweek_Evening <> EPL_SOC         and Category = 'Midweek Evening';
select count(*) as Cnt from EPL_04_Eng_Matrix base where EPL_SOC__Pack_Saturday_Lunch <> EPL_SOC          and Category = 'Saturday Lunchtime';
select count(*) as Cnt from EPL_04_Eng_Matrix base where EPL_SOC__Pack_Saturday_Afternoon <> EPL_SOC      and Category = 'Saturday Afternoon';
select count(*) as Cnt from EPL_04_Eng_Matrix base where EPL_SOC__Pack_Sunday_Early_Afternoon <> EPL_SOC  and Category = 'Sunday Early Afternoon';
select count(*) as Cnt from EPL_04_Eng_Matrix base where EPL_SOC__Pack_Sunday_Late_Afternoon <> EPL_SOC   and Category = 'Sunday Late Afternoon';


  -- SUMMARIZE
select
      case
        when Period = 1 then '2) Aug ''13 - Feb ''14'
        when Period = 2 then '1) Feb ''13 - Jul ''13'
        when Period = 3 then '3) Feb ''13 - May ''13'
          else '???'
      end as xPeriod,
      case
        when Low_Content_Flag = 1 then 'Low'
          else 'Normal'
      end as xLow_Content_Flag,
      Metric,
      Category,
      EPL_SOC,
      EPL_SoNLC,
      EPL_SoSV,
      Sport_SoV,
      count(*) as Accts,
      count(distinct account_number) as Accts2
  from EPL_04_Eng_Matrix
 group by
      xPeriod,
      xLow_Content_Flag,
      Metric,
      Category,
      EPL_SOC,
      EPL_SoNLC,
      EPL_SoSV,
      Sport_SoV;



  -- ##############################################################################################################
  -- ##############################################################################################################

















