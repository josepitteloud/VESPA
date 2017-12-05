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
# 20/11/2014  SBE   Based on "Code 03a - EPL eval - metrics (analysis - main).sql" -
#                   retained only key metrics, used in the final tree.
#                   Metric definitions changed as per request
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
         where EPL_Content > 0                                                    -- EPL live/non-live content only
           and Viewing_Type in (1, 2)                                             -- Live & Live pause of up to 15 minutes
         group by Account_Number, Period, Programme_Instance_Name
        having sum(Instance_Duration) >= (15 * 60) ) det                          -- Aggregated viewing of at least 15 minutes
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and base.Programme_Instance_Name = det.Programme_Instance_Name;
commit;


  -- ##############################################################################################################
  -- ##### Generate SOCs                                                                                      #####
  -- ##############################################################################################################
  -- OVERALL - live
delete from EPL_03_SOCs_Summaries
 where Metric in ('Live games - overall');
commit;

insert into EPL_03_SOCs_Summaries
      (Account_Number, Period, Metric, Category, Content_Available, Content_Watched, Calculated_SOC)
  select
        Account_Number,
        Period,
        'Live games - overall'                        as xMetric,
        '(all)'                                       as xCategory,
        sum(Content_Available)                        as xContent_Available,
        sum(Content_Watched)                          as xContent_Watched,
        case
          when xContent_Available = 0 then null
            else 1.0 * xContent_Watched / xContent_Available
        end                                           as xCalculated_SOC
    from EPL_03_SOCs
   where Live_Game_Flag = 1
   group by Account_Number, Period, xMetric, xCategory;
commit;



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
              when Live_Game_Flag = 1 then Instance_Duration
                else 0
            end) as xCategory_Consumption,
        sum(case
              when Sports_Genre_Flag = 1 then Instance_Duration
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
                Instance_Duration
            from EPL_02_Viewing_Summary
           where Sports_Channel_Type in (1, 2, 3)                         -- 1: Sky Sports, 2: BT Sports, 3: ESPN, 4: All other
             and Viewing_Type in (1, 2)                                   -- 1: Live, 2: Live pause (up to 15 mins), 3: Playback
             and Period = 1) det
   group by Account_Number, Period;
commit;



  -- ##############################################################################################################
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
                   and Sports_Genre_Flag = 1 then Instance_Duration
                else 0
            end) as xCategory_Consumption,
        sum(case
              when Pay_TV_Type in (1, 2, 3, 4, 5) then Instance_Duration           -- Pay_TV_Type    - 0: FTA, 1: Pay package, 2: Pay Sports, 3: Pay Movies,
                                                                                  --                  4: 3rd party, 5: A'La Carte, 6: PPV/other
                else 0
            end) as xTotal_Consumption,
        case
          when xTotal_Consumption > 0 then 1.0 * xCategory_Consumption / xTotal_Consumption
            else 0
        end as xCalculated_SOV
    from EPL_02_Viewing_Summary det
   where Period = 1
   group by Account_Number, Period;
commit;



  -- ##############################################################################################################
  -- Key pay entertainment channels
  --  18/11/2014 Richard Ashton:
  --      We need to change this to high ‘key pay entertainment channel viewing’:
  --      a. Adding in the following channel providers:
  --        i.    Fox
  --        ii.   Universal
  --        iii.  Comedy Central
  --        iv.   Eurosport
  --        v.    Discovery
  --        vi.   Syfy
  --      b. Raising average (non-consecutive) daily viewing from 20 minutes to 45 minutes (please advise if you think this threshold is too high)
  --      c. Only include viewing that is over a 10 minute consecutive period
delete from EPL_03_SOVs
 where Metric = 'Key pay entertainment channels';
commit;

insert into EPL_03_SOVs
      (Account_Number, Period, Metric, Category, Category_Consumption, Total_Consumption, Calculated_SOV)
  select
        Account_Number,
        Period,
        'Key pay entertainment channels' as Metric,
        '(all)' as Category,
        sum(case
              when (
                    Sky_Branded_Channel in (1, 2)                                 -- 0: All other, 1: Sky Atlantic (all), 2: Other Sky Branded channels, 3: Sky Sports News
                    or
                    Third_Party_Channel in (1, 2, 3, 4, 5, 6)                     -- 0: All other, 1: FOX, 2: Universal, 3: Comedy Central, 4: Eurosport, 5: Discovery, 6: Syf
                   )
                   and Event_Duration > (10 * 60) then Instance_Duration          -- Events/sessions longer than 10 minutes
                else 0
            end) as xCategory_Consumption,
        max(Days_Data_Available) as xTotal_Consumption,
        case
          when xTotal_Consumption > 0 then (1.0 * xCategory_Consumption / xTotal_Consumption) / 60
            else 0
        end as xCalculated_SOV
    from EPL_02_Viewing_Summary
   where Period = 1
   group by Account_Number, Period;
commit;



  -- ##############################################################################################################
  --  Sky Sports News
  --  18/11/2014 Richard Ashton:
  --    a. Change viewing metric from 20% of pay viewing to average (non-consecutive) daily viewing of 30 minutes (please advise if you think this threshold is too high)
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
              when Sky_Branded_Channel in (3) then Instance_Duration
                else 0
            end) as xCategory_Consumption,
        max(Days_Data_Available) as xTotal_Consumption,
        case
          when xTotal_Consumption > 0 then (1.0 * xCategory_Consumption / xTotal_Consumption) / 60
            else 0
        end as xCalculated_SOV
    from EPL_02_Viewing_Summary
   where Period = 1
   group by Account_Number, Period;
commit;



  -- ##############################################################################################################
  --  Movies SoV
  --  % of total viewing that is to Prem Movies content
  --    SBE: Currently used definitions are:
  --          - Ent pack SoV: (FTA+Pay TV) / (All TV)
  --          - Sports SoV: (Live/Live pause sport genre viewing on SS/BT/ESPN) / (All Pay TV viewing)
  --          For Movies – would you like the denominator to be “All TV” or “All Pay TV”, assuming both live and all playback?
  --    KSargent: Suggest All Pay TV and including playback
  --    RCrounch: Needs to be pay TV excluding pay sport  as we already have the Sport as % of total pay
  --  18/11/2014 Richard Ashton:
  --    a) Change to an average of 45 minutes of consecutive viewing across linear and on demand per week (please advise if you think this threshold is too high)
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
              when Pay_TV_Type = 3 and                                            -- 0: FTA, 1: Pay package, 2: Pay Sports, 3: Pay Movies, 4: 3rd party, 5: A'La Carte, 6: PPV/other
                   Event_Duration > (45 * 60) then Instance_Duration              -- Events/sessions longer than 45 minutes
                else 0
            end) as xCategory_Consumption,
        max(Days_Data_Available) as xTotal_Consumption,
        case
          when xTotal_Consumption > 0 then (1.0 * xCategory_Consumption / xTotal_Consumption) / 60
            else 0
        end as xCalculated_SOV
    from EPL_02_Viewing_Summary
   where Period = 1
   group by Account_Number, Period;
commit;



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
    EPL_SoSV                                varchar(30)       null      default 'Not calculated',
    Sport_SoV                               varchar(30)       null      default 'Not calculated',

    Key_Pay_Entertainment_Avg_DV            varchar(30)       null      default 'Not calculated',
    Sky_Sports_News_Avg_DV                  varchar(30)       null      default 'Not calculated',
    Movies_Avg_DV                           varchar(30)       null      default 'Not calculated'

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


-- select Period, Metric, Category, count(*) as Cnt, count(distinct Account_Number) as Cnt_Accts from EPL_04_Eng_Matrix group by Period, Metric, Category order by Period, Metric, Category;



  -- ##############################################################################################################
  -- ##############################################################################################################
  -- Share of Content (what proportion of all EPL matches does the household watch): H = >50%, M = 20-50% and L = 0.1-19.9%
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
   and base.Metric = 'Overall'
   and det.Metric = 'Live games - overall'
   and base.Category = det.Category;
commit;



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



  -- ##############################################################################################################
  -- ##############################################################################################################
  -- Key pay entertainment channels
  --  18/11/2014 Richard Ashton:
  --      We need to change this to high ‘key pay entertainment channel viewing’:
  --      a. Adding in the following channel providers:
  --        i.    Fox
  --        ii.   Universal
  --        iii.  Comedy Central
  --        iv.   Eurosport
  --        v.    Discovery
  --        vi.   Syfy
  --      b. Raising average (non-consecutive) daily viewing from 20 minutes to 45 minutes (please advise if you think this threshold is too high)
  --      c. Only include viewing that is over a 10 minute consecutive period
update EPL_04_Eng_Matrix base
   set base.Key_Pay_Entertainment_Avg_DV
                                = case
                                    when det.Calculated_SOV  = 0     then 'Did not watch'
                                    when det.Calculated_SOV  < 45.00 then 'Low'
                                      else 'High'
                                  end
  from EPL_03_SOVs det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and det.Metric = 'Key pay entertainment channels';
commit;



  -- ##############################################################################################################
  -- ##############################################################################################################
  --  Sky Sports News SoV
  --    Additional metric requested on 27/06: Viewing to Sky Atlantic - use no. of complete programmes viewed rather than SoV to define thresholds
  --    None: 0, Low: >0 and <=0.02, Med: >0.02 and <=0.1, High: >0.1
  --  18/11/2014 Richard Ashton:
  --    a. Change viewing metric from 20% of pay viewing to average (non-consecutive) daily viewing of 30 minutes (please advise if you think this threshold is too high)
  --  21/11/2014 Richard Ashton:
  --    Agree with 15 minute recommendation
update EPL_04_Eng_Matrix base
   set base.Sky_Sports_News_Avg_DV
                                = case
                                    when det.Calculated_SOV  = 0     then 'Did not watch'
                                    when det.Calculated_SOV  < 15.00 then 'Low'
                                      else 'High'
                                  end
  from EPL_03_SOVs det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and det.Metric = 'Sky Sports News SoV';
commit;



  -- ##############################################################################################################
  -- ##############################################################################################################
  --  Movies SoV
  --  % of total viewing that is to Prem Movies content
  --    L 0.1-2%, M 2.1%-11%, H >11%
  --  18/11/2014 Richard Ashton:
  --    a) Change to an average of 45 minutes of consecutive viewing across linear and on demand per week (please advise if you think this threshold is too high)
update EPL_04_Eng_Matrix base
   set base.Movies_Avg_DV
                                = case
                                    when det.Calculated_SOV  = 0     then 'Did not watch'
                                    when det.Calculated_SOV  < 6.429 then 'Low'                 -- 45 mins/week = 6.429 mins/day
                                      else 'High'
                                  end
  from EPL_03_SOVs det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and det.Metric = 'Movies SoV';
commit;



  -- ##############################################################################################################
  -- ##############################################################################################################
/*
select
      Grp,
      sum(Scaling_Weight) as Accounts_Num
  from (select
              round(a.Calculated_SOV, 2) as Grp,
              Scaling_Weight
          from EPL_03_SOVs a,
               EPL_05_Scaling_Weights b,
               EPL_04_Profiling_Variables c
         where a.Account_Number = b.Account_Number
           and a.Account_Number = c.Account_Number
           and a.Period = b.Period
           and a.Period = c.Period
           and c.Prem_Sports > 0
           and a.Metric = 'EPL SoSV'
           and a.Calculated_SOV > 0) aa
 group by Grp;

select
      Grp,
      sum(Scaling_Weight) as Accounts_Num
  from (select
              round(a.Calculated_SOV, 2) as Grp,
              Scaling_Weight
          from EPL_03_SOVs a,
               EPL_05_Scaling_Weights b,
               EPL_04_Profiling_Variables c
         where a.Account_Number = b.Account_Number
           and a.Account_Number = c.Account_Number
           and a.Period = b.Period
           and a.Period = c.Period
           and c.Prem_Sports > 0
           and a.Metric = 'Sports SoV'
           and a.Calculated_SOV > 0) aa
 group by Grp;

select
      Grp,
      sum(Scaling_Weight) as Accounts_Num
  from (select
              round(a.Calculated_SOV, 0) as Grp,
              Scaling_Weight
          from EPL_03_SOVs a,
               EPL_05_Scaling_Weights b,
               EPL_04_Profiling_Variables c
         where a.Account_Number = b.Account_Number
           and a.Account_Number = c.Account_Number
           and a.Period = b.Period
           and a.Period = c.Period
           and c.Prem_Sports > 0
           and a.Metric = 'Key pay entertainment channels'
           and a.Calculated_SOV > 0) aa
 group by Grp;

select
      Grp,
      sum(Scaling_Weight) as Accounts_Num
  from (select
              round(a.Calculated_SOV, 0) as Grp,
              Scaling_Weight
          from EPL_03_SOVs a,
               EPL_05_Scaling_Weights b,
               EPL_04_Profiling_Variables c
         where a.Account_Number = b.Account_Number
           and a.Account_Number = c.Account_Number
           and a.Period = b.Period
           and a.Period = c.Period
           and c.Prem_Sports > 0
           and a.Metric = 'Sky Sports News SoV'
           and a.Calculated_SOV > 0) aa
 group by Grp;

select
      Grp,
      sum(Scaling_Weight) as Accounts_Num
  from (select
              round(a.Calculated_SOV, 0) as Grp,
              Scaling_Weight
          from EPL_03_SOVs a,
               EPL_05_Scaling_Weights b,
               EPL_04_Profiling_Variables c
         where a.Account_Number = b.Account_Number
           and a.Account_Number = c.Account_Number
           and a.Period = b.Period
           and a.Period = c.Period
           and c.Prem_Sports > 0
           and a.Metric = 'Movies SoV'
           and a.Calculated_SOV > 0) aa
 group by Grp;
*/


/*
select
      EPL_SOC as Grp,
      sum(Scaling_Weight) as Sum_Accounts
  from EPL_04_Eng_Matrix a,
       EPL_05_Scaling_Weights b,
       EPL_04_Profiling_Variables c
 where a.Account_Number = b.Account_Number
   and a.Account_Number = c.Account_Number
   and a.Period = b.Period
   and a.Period = c.Period
   and c.Prem_Sports > 0
 group by Grp;

select
      EPL_SoSV as Grp,
      sum(Scaling_Weight) as Sum_Accounts
  from EPL_04_Eng_Matrix a,
       EPL_05_Scaling_Weights b,
       EPL_04_Profiling_Variables c
 where a.Account_Number = b.Account_Number
   and a.Account_Number = c.Account_Number
   and a.Period = b.Period
   and a.Period = c.Period
   and c.Prem_Sports > 0
 group by Grp;

select
      Sport_SoV as Grp,
      sum(Scaling_Weight) as Sum_Accounts
  from EPL_04_Eng_Matrix a,
       EPL_05_Scaling_Weights b,
       EPL_04_Profiling_Variables c
 where a.Account_Number = b.Account_Number
   and a.Account_Number = c.Account_Number
   and a.Period = b.Period
   and a.Period = c.Period
   and c.Prem_Sports > 0
 group by Grp;

select
      Key_Pay_Entertainment_Avg_DV as Grp,
      sum(Scaling_Weight) as Sum_Accounts
  from EPL_04_Eng_Matrix a,
       EPL_05_Scaling_Weights b,
       EPL_04_Profiling_Variables c
 where a.Account_Number = b.Account_Number
   and a.Account_Number = c.Account_Number
   and a.Period = b.Period
   and a.Period = c.Period
   and c.Prem_Sports > 0
 group by Grp;

select
      Sky_Sports_News_Avg_DV as Grp,
      sum(Scaling_Weight) as Sum_Accounts
  from EPL_04_Eng_Matrix a,
       EPL_05_Scaling_Weights b,
       EPL_04_Profiling_Variables c
 where a.Account_Number = b.Account_Number
   and a.Account_Number = c.Account_Number
   and a.Period = b.Period
   and a.Period = c.Period
   and c.Prem_Sports > 0
 group by Grp;

select
      Movies_Avg_DV as Grp,
      sum(Scaling_Weight) as Sum_Accounts
  from EPL_04_Eng_Matrix a,
       EPL_05_Scaling_Weights b,
       EPL_04_Profiling_Variables c
 where a.Account_Number = b.Account_Number
   and a.Account_Number = c.Account_Number
   and a.Period = b.Period
   and a.Period = c.Period
   and c.Prem_Sports > 0
 group by Grp;
*/
















