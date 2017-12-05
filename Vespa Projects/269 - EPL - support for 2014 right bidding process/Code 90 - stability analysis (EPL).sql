/*###############################################################################
# Created on:   09/12/2014
# Created by:   Sebastian Bednaszynski(SBE)
# Description:  EPL rights project - stability analysis for EPL tree
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 09/12/2014  SBE   Initial version
#
###############################################################################*/



  -- ##############################################################################################################
  -- ##### Get the original segments first                                                                    #####
  -- ##############################################################################################################
if object_id('EPL_90_Stability_EPL__Variable_Period') is not null then drop table EPL_90_Stability_EPL__Variable_Period end if;
create table EPL_90_Stability_EPL__Variable_Period (
    Pk_Identifier                           bigint            identity,
    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name(),

      -- Account
    Account_Number                          varchar(20)       null      default null,
    Period                                  tinyint           null      default 0,
    Period_Start_Date                       date              null      default null,
    Period_End_Date                         date              null      default null,
    Viewing_Days                            smallint          null      default 0,

    Measure_Group                           varchar(50)       null      default null,

    Low_Content_Flag                        bit               null      default 0,
    Metric                                  varchar(50)       null      default null,
    Category                                varchar(50)       null      default null,

    EPL_SOC                                 varchar(30)       null      default 'Not calculated',
    EPL_SoSV                                varchar(30)       null      default 'Not calculated',
    Sport_SoV                               varchar(30)       null      default 'Not calculated',

    Key_Pay_Entertainment_Avg_DV            varchar(30)       null      default 'Not calculated',
    Sky_Sports_News_Avg_DV                  varchar(30)       null      default 'Not calculated',
    Movies_Avg_DV                           varchar(30)       null      default 'Not calculated',

    Risk_Segment                            varchar(50)       null      default 'Not calculated',

    Content_Available                       smallint          null      default 0,
    Content_Watched                         smallint          null      default 0
);
create        hg   index idx01 on EPL_90_Stability_EPL__Variable_Period(Account_Number);
create        lf   index idx02 on EPL_90_Stability_EPL__Variable_Period(Period);
create        lf   index idx03 on EPL_90_Stability_EPL__Variable_Period(Metric);
create        lf   index idx04 on EPL_90_Stability_EPL__Variable_Period(Measure_Group);
create unique hg   index idx05 on EPL_90_Stability_EPL__Variable_Period(Account_Number, Period);
grant select on EPL_90_Stability_EPL__Variable_Period to vespa_group_low_security;


delete from EPL_90_Stability_EPL__Variable_Period
 where Period = 0;
commit;

insert into EPL_90_Stability_EPL__Variable_Period
      (Account_Number, Period, Period_Start_Date, Period_End_Date, Measure_Group, Low_Content_Flag,
       Metric, Category, EPL_SOC, EPL_SoSV, Sport_SoV, Key_Pay_Entertainment_Avg_DV, Sky_Sports_News_Avg_DV,
       Movies_Avg_DV, Risk_Segment, Content_Available, Content_Watched)
  select
        a.Account_Number,
        0,
        '2013-01-08',
        '2014-02-28',
        'Actual',

        a.Low_Content_Flag,
        a.Metric,
        a.Category,
        a.EPL_SOC,
        a.EPL_SoSV,
        case
          when a.Sport_SoV = 'Very high' then 'High'
            else a.Sport_SoV
        end Sport_SoV,
        a.Key_Pay_Entertainment_Avg_DV,
        a.Sky_Sports_News_Avg_DV,
        a.Movies_Avg_DV,
        b.xRisk_Segment_3,
        c.Content_Available,
        c.Content_Watched
  from EPL_04_Eng_Matrix a,
       EPL_07_Risk_Groups_View b,
       EPL_03_SOCs_Summaries c,
       EPL_04_Profiling_Variables d
 where a.Account_Number = b.Account_Number
   and a.Period = b.Period
   and a.Period = 1
   and a.Account_Number = c.Account_Number
   and a.Period = c.Period
   and a.Metric = 'Overall'
   and a.Category = '(all)'
   and c.Metric = 'Live games - overall'
   and c.Category = '(all)'
   and a.Account_Number = d.Account_Number
   and a.Period = d.Period
   and d.Prem_Sports > 0;
commit;



  -- Create a temp table for quicker calculation of number of days available
select distinct
      Account_Number,
      Viewing_Day
  into EPL_90_tmp_Viewing_Days
  from EPL_02_Viewing_Summary
 where Period = 1;
commit;
create        hg   index idx01 on EPL_90_tmp_Viewing_Days(Account_Number);
create        lf   index idx02 on EPL_90_tmp_Viewing_Days(Viewing_Day);


update EPL_90_Stability_EPL__Variable_Period base
   set base.Viewing_Days = det.Viewing_Days
  from (select
              Account_Number,
              count(distinct Viewing_Day) as Viewing_Days
          from EPL_90_tmp_Viewing_Days
         group by Account_Number) det
 where base.Account_Number = det.Account_Number
   and base.Period = 0;
commit;



  -- Summary table for PCCs
if object_id('EPL_90_Stability_EPL__PCCs') is not null then drop table EPL_90_Stability_EPL__PCCs end if;
create table EPL_90_Stability_EPL__PCCs (
    Pk_Identifier                           bigint            identity,
    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name(),

    Measure_Group                           varchar(50)       null      default null,
    Period                                  tinyint           null      default 0,
    Period_Start_Date                       date              null      default null,
    Period_End_Date                         date              null      default null,

    No_Viewing_Data                         bigint            null      default 0,
    Correctly_Classified                    bigint            null      default 0,
    Total_Accounts                          bigint            null      default 0,
    PCC                                     decimal(10, 4)    null      default 0
);
create unique hg   index idx01 on EPL_90_Stability_EPL__PCCs(Measure_Group, Period);
grant select on EPL_90_Stability_EPL__PCCs to vespa_group_low_security;


if object_id('EPL_90_Stability_EPL__PCC_Details') is not null then drop table EPL_90_Stability_EPL__PCC_Details end if;
create table EPL_90_Stability_EPL__PCC_Details (
    Pk_Identifier                           bigint            identity,
    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name(),

    Measure_Group                           varchar(50)       null      default null,
    Period                                  tinyint           null      default 0,
    Period_Start_Date                       date              null      default null,
    Period_End_Date                         date              null      default null,

    Class_Details                           varchar(100)      null      default '???',
    Incorrect_Class_Flag                    varchar(20)       null      default '???',
    Total_Accounts                          bigint            null      default 0
);
grant select on EPL_90_Stability_EPL__PCC_Details to vespa_group_low_security;



  -- ##############################################################################################################
  -- ##### Flag accounts with <=70% days available                                                            #####
  -- ##############################################################################################################
set option query_temp_space_limit = 0;


if object_id('EPL_9_Stability_EPL_Run') is not null then drop procedure EPL_9_Stability_EPL_Run end if;
create procedure EPL_9_Stability_EPL_Run
      @parStartDate             date = null,
      @parEndDate               date = null,
      @parStartPeriod           tinyint = 0
as
begin

    declare @runStartTime datetime
    declare @execTime bigint
    declare @varSql varchar(25000)      -- SQL string for dynamic SQL execution

    set @runStartTime = now()
    message '[' || now() || '] ###### Starting period ' || @parStartPeriod || ' (end date: ' || @parEndDate || ') #####' type status to client


      -- ##############################################################################################################
      -- ##### Get number of days per account                                                                     #####
      -- ##############################################################################################################

    if object_id('EPL_90_tmp_Acc_Num_Days') is not null drop table EPL_90_tmp_Acc_Num_Days
    select
          Account_Number,
          count(distinct Viewing_Day) as Days_Data_Available
      into EPL_90_tmp_Acc_Num_Days
      from EPL_90_tmp_Viewing_Days
     where Viewing_Day between @parStartDate and @parEndDate
     group by Account_Number
    commit
    create unique hg   index idx01 on EPL_90_tmp_Acc_Num_Days(Account_Number)



      -- ##############################################################################################################
      -- ##### Calculate metrics                                                                                  #####
      -- ##############################################################################################################
    if object_id('EPL_90_tmp_SOCs') is not null drop table EPL_90_tmp_SOCs
    create table EPL_90_tmp_SOCs (
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
    )
    create        hg   index idx01 on EPL_90_tmp_SOCs(Account_Number)
    create        lf   index idx02 on EPL_90_tmp_SOCs(Period)
    create        date index idx03 on EPL_90_tmp_SOCs(Broadcast_Date)
    create        hg   index idx04 on EPL_90_tmp_SOCs(Programme_Instance_Name)
    grant select on EPL_90_tmp_SOCs to vespa_group_low_security


    if object_id('EPL_90_tmp_SOCs_Summaries') is not null drop table EPL_90_tmp_SOCs_Summaries
    create table EPL_90_tmp_SOCs_Summaries (
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
    )
    create        hg   index idx01 on EPL_90_tmp_SOCs_Summaries(Account_Number)
    create        lf   index idx02 on EPL_90_tmp_SOCs_Summaries(Period)
    create        lf   index idx03 on EPL_90_tmp_SOCs_Summaries(Metric)
    grant select on EPL_90_tmp_SOCs_Summaries to vespa_group_low_security


    if object_id('EPL_90_tmp_SOVs') is not null drop table EPL_90_tmp_SOVs
    create table EPL_90_tmp_SOVs (
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
    )
    create        hg   index idx01 on EPL_90_tmp_SOVs(Account_Number)
    create        lf   index idx02 on EPL_90_tmp_SOVs(Period)
    create        lf   index idx03 on EPL_90_tmp_SOVs(Metric)
    grant select on EPL_90_tmp_SOVs to vespa_group_low_security



      -- ##### Share of Content - preparation                                                                     #####
    insert into EPL_90_tmp_SOCs
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
         and b.Live_Game_Flag = 1
         and (
                b.Channel not in ('Sky Sports 1', 'Sky Sports 2', 'Sky Sports 3', 'Sky Sports 4', 'BT Sport 1', 'ESPN', '3D channel')       -- Terrestrial/non-premium channels
                or
                ( a.Sky_Sports_Flag = 1 and b.Channel in ('Sky Sports 1', 'Sky Sports 2', 'Sky Sports 3', 'Sky Sports 4', '3D channel') )   -- OR Sky Sports channels, if eligible
                or
                ( a.BT_Sport_Flag = 1 and b.Channel in ('BT Sport 1') )                                                                     -- OR BT Sport channel, if eligible
                or
                ( a.ESPN_Flag = 1 and b.Channel in ('ESPN') )                                                                               -- OR ESPN channel, if eligible
             )
         and a.Data_Day between @parStartDate and @parEndDate
       group by a.Account_Number, a.Period, b.Broadcast_Date, b.Channel, b.Programme, b.Programme_Instance_Name,
                b.Kick_Off_Time, b.Day_Of_Week, b.Live_Game_Flag, b.EPL_Pack
    commit


      -- Attribute viewing
    update EPL_90_tmp_SOCs base
       set base.Content_Watched = 1
      from (select
                  Account_Number,
                  Period,
                  Programme_Instance_Name
              from EPL_02_Viewing_Summary
             where EPL_Content > 0                                                    -- EPL live/non-live content only
               and Viewing_Type in (1, 2)                                             -- Live & Live pause of up to 15 minutes
               and Viewing_Day between @parStartDate and @parEndDate
             group by Account_Number, Period, Programme_Instance_Name
            having sum(Instance_Duration) >= (15 * 60) ) det                          -- Aggregated viewing of at least 15 minutes
     where base.Account_Number = det.Account_Number
       and base.Period = det.Period
       and base.Programme_Instance_Name = det.Programme_Instance_Name
    commit
    message '[' || now() || '] Done - SOCs (1/25)' type status to client


      -- ##### Generate SOCs                                                                                      #####
      -- OVERALL - live
    delete from EPL_90_tmp_SOCs_Summaries
     where Metric in ('Live games - overall')
    commit

    insert into EPL_90_tmp_SOCs_Summaries
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
        from EPL_90_tmp_SOCs
       where Live_Game_Flag = 1
       group by Account_Number, Period, xMetric, xCategory
    commit
    message '[' || now() || '] Done - SOC summaries (2/25)' type status to client



      -- ##### Share of Viewing                                                                                   #####
      -- EPL SoSV
    delete from EPL_90_tmp_SOVs
     where Metric = 'EPL SoSV'
    commit

    insert into EPL_90_tmp_SOVs
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
                 and Viewing_Day between @parStartDate and @parEndDate
                 and Period = 1) det
       group by Account_Number, Period
    commit
    message '[' || now() || '] Done - SOVs (3/25)' type status to client



      --  Sports SoV
    delete from EPL_90_tmp_SOVs
     where Metric = 'Sports SoV'
    commit

    insert into EPL_90_tmp_SOVs
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
         and Viewing_Day between @parStartDate and @parEndDate
       group by Account_Number, Period
    commit
    message '[' || now() || '] Done - metric "Sports SoV" (4/25)' type status to client



      -- Key pay entertainment channels
    delete from EPL_90_tmp_SOVs
     where Metric = 'Key pay entertainment channels'
    commit

    insert into EPL_90_tmp_SOVs
          (Account_Number, Period, Metric, Category, Category_Consumption, Total_Consumption, Calculated_SOV)
      select
            a.Account_Number,
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
            max(b.Days_Data_Available) as xTotal_Consumption,
            case
              when xTotal_Consumption > 0 then (1.0 * xCategory_Consumption / xTotal_Consumption) / 60
                else 0
            end as xCalculated_SOV
        from EPL_02_Viewing_Summary a,
             EPL_90_tmp_Acc_Num_Days b
       where a.Account_Number = b.Account_Number
         and Period = 1
         and Viewing_Day between @parStartDate and @parEndDate
       group by a.Account_Number, Period
    commit
    message '[' || now() || '] Done - metric "Key pay entertainment channels" (5/25)' type status to client


      --  Sky Sports News
    delete from EPL_90_tmp_SOVs
     where Metric = 'Sky Sports News SoV'
    commit

    insert into EPL_90_tmp_SOVs
          (Account_Number, Period, Metric, Category, Category_Consumption, Total_Consumption, Calculated_SOV)
      select
            a.Account_Number,
            Period,
            'Sky Sports News SoV' as Metric,
            '(all)' as Category,
            sum(case
                  when Sky_Branded_Channel in (3) then Instance_Duration
                    else 0
                end) as xCategory_Consumption,
            max(b.Days_Data_Available) as xTotal_Consumption,
            case
              when xTotal_Consumption > 0 then (1.0 * xCategory_Consumption / xTotal_Consumption) / 60
                else 0
            end as xCalculated_SOV
        from EPL_02_Viewing_Summary a,
             EPL_90_tmp_Acc_Num_Days b
       where a.Account_Number = b.Account_Number
         and Period = 1
         and Viewing_Day between @parStartDate and @parEndDate
       group by a.Account_Number, Period
    commit
    message '[' || now() || '] Done - metric "Sky Sports News SoV" (6/25)' type status to client



      --  Movies SoV
    delete from EPL_90_tmp_SOVs
     where Metric = 'Movies SoV'
    commit

    insert into EPL_90_tmp_SOVs
          (Account_Number, Period, Metric, Category, Category_Consumption, Total_Consumption, Calculated_SOV)
      select
            a.Account_Number,
            Period,
            'Movies SoV' as Metric,
            '(all)' as Category,
            sum(case
                  when Pay_TV_Type = 3 and                                            -- 0: FTA, 1: Pay package, 2: Pay Sports, 3: Pay Movies, 4: 3rd party, 5: A'La Carte, 6: PPV/other
                       Event_Duration > (45 * 60) then Instance_Duration              -- Events/sessions longer than 45 minutes
                    else 0
                end) as xCategory_Consumption,
            max(b.Days_Data_Available) as xTotal_Consumption,
            case
              when xTotal_Consumption > 0 then (1.0 * xCategory_Consumption / xTotal_Consumption) / 60
                else 0
            end as xCalculated_SOV
        from EPL_02_Viewing_Summary a,
             EPL_90_tmp_Acc_Num_Days b
       where a.Account_Number = b.Account_Number
         and Period = 1
         and Viewing_Day between @parStartDate and @parEndDate
       group by a.Account_Number, Period
    commit
    message '[' || now() || '] Done - metric "Movies SoV" (7/25)' type status to client



      -- ##### Get single account engagement view                                                                 #####
    if object_id('EPL_90_tmp_Eng_Matrix') is not null drop table EPL_90_tmp_Eng_Matrix
    create table EPL_90_tmp_Eng_Matrix (
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

    )
    create        hg   index idx01 on EPL_90_tmp_Eng_Matrix(Account_Number)
    create        lf   index idx02 on EPL_90_tmp_Eng_Matrix(Period)
    create        lf   index idx03 on EPL_90_tmp_Eng_Matrix(Metric)
    grant select on EPL_90_tmp_Eng_Matrix to vespa_group_low_security


      -- Overall level - base
    insert into EPL_90_tmp_Eng_Matrix
          (Account_Number, Period, Metric, Category)
      select
            a.Account_Number,
            a.Period,
            'Overall',
            '(all)'
        from EPL_01_Universe a
       where a.Period = 1
       group by a.Account_Number, a.Period
      having max(a.Valid_Account_Flag) = 1
    commit
    message '[' || now() || '] Done - engagement matrix base (8/25)' type status to client



      -- Share of Content (what proportion of all EPL matches does the household watch): H = >50%, M = 20-50% and L = 0.1-19.9%
    update EPL_90_tmp_Eng_Matrix base
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
      from EPL_90_tmp_SOCs_Summaries det
     where base.Account_Number = det.Account_Number
       and base.Period = det.Period
       and base.Metric = 'Overall'
       and det.Metric = 'Live games - overall'
       and base.Category = det.Category
    commit
    message '[' || now() || '] Done - metric categories for "EPL SOC" (9/25)' type status to client



      --    o Share of Sports Viewing (what proportion of their paid Sports viewing is to EPL): H = >50%, M = 20-50% and L = 0.1-19.9%
    update EPL_90_tmp_Eng_Matrix base
       set base.EPL_SoSV            = case
                                        when det.Calculated_SOV =  0    then 'Did not watch'
                                        when det.Calculated_SOV <  0.20 then 'Low'
                                        when det.Calculated_SOV <= 0.50 then 'Medium'
                                          else 'High'
                                      end
      from EPL_90_tmp_SOVs det
     where base.Account_Number = det.Account_Number
       and base.Period = det.Period
       and det.Metric = 'EPL SoSV'
    commit
    message '[' || now() || '] Done - metric categories for "EPL SoSV" (10/25)' type status to client



      --    o Share of Viewing (what proportion of their total paid Sky viewing is to paid Sports): HH = >20%, H = 10-20%, M = 5-9.9%, L = 0.1-4.9%
    update EPL_90_tmp_Eng_Matrix base
       set base.Sport_SoV           = case
                                        when det.Calculated_SOV =  0    then 'Did not watch'
                                        when det.Calculated_SOV <  0.05 then 'Low'
                                        when det.Calculated_SOV <  0.25 then 'Medium'
                                          else 'High'
                                      end
      from EPL_90_tmp_SOVs det
     where base.Account_Number = det.Account_Number
       and base.Period = det.Period
       and det.Metric = 'Sports SoV'
    commit
    message '[' || now() || '] Done - metric categories for "Sports SoV" (11/25)' type status to client



      -- Key pay entertainment channels
    update EPL_90_tmp_Eng_Matrix base
       set base.Key_Pay_Entertainment_Avg_DV
                                    = case
                                        when det.Calculated_SOV  = 0     then 'Did not watch'
                                        when det.Calculated_SOV  < 45.00 then 'Low'
                                          else 'High'
                                      end
      from EPL_90_tmp_SOVs det
     where base.Account_Number = det.Account_Number
       and base.Period = det.Period
       and det.Metric = 'Key pay entertainment channels'
    commit
    message '[' || now() || '] Done - metric categories for "Key pay entertainment channels" (12/25)' type status to client



      --  Sky Sports News SoV
    update EPL_90_tmp_Eng_Matrix base
       set base.Sky_Sports_News_Avg_DV
                                    = case
                                        when det.Calculated_SOV  = 0     then 'Did not watch'
                                        when det.Calculated_SOV  < 15.00 then 'Low'
                                          else 'High'
                                      end
      from EPL_90_tmp_SOVs det
     where base.Account_Number = det.Account_Number
       and base.Period = det.Period
       and det.Metric = 'Sky Sports News SoV'
    commit
    message '[' || now() || '] Done - metric categories for "Sky Sports News SoV" (13/25)' type status to client



      --  Movies SoV
    update EPL_90_tmp_Eng_Matrix base
       set base.Movies_Avg_DV
                                    = case
                                        when det.Calculated_SOV  = 0     then 'Did not watch'
                                        when det.Calculated_SOV  < 6.429 then 'Low'                 -- 45 mins/week = 6.429 mins/day
                                          else 'High'
                                      end
      from EPL_90_tmp_SOVs det
     where base.Account_Number = det.Account_Number
       and base.Period = det.Period
       and det.Metric = 'Movies SoV'
    commit
    message '[' || now() || '] Done - metric categories for "Movies SoV" (14/25)' type status to client



      -- ##############################################################################################################
      -- ##### Calculate risk groups                                                                              #####
      -- ##############################################################################################################
    if object_id('EPL_90_tmp_Risk_Groups') is not null drop table EPL_90_tmp_Risk_Groups
    create table EPL_90_tmp_Risk_Groups (
        Pk_Identifier                           bigint            identity,
        Updated_On                              datetime          not null  default timestamp,
        Updated_By                              varchar(30)       not null  default user_name(),

          -- Account
        Account_Number                          varchar(20)       null      default null,
        Period                                  tinyint           null      default 0,
        Sports_Package                          varchar(20)       null      default 'No Sky Sports',
        Risk_Segment_1                          smallint          null      default -1,               -- Basic risk group - Sky loses EPL in full
        Risk_Segment_2                          smallint          null      default -1,               -- Basic risk group - Sky loses majority of EPL
        Risk_Segment_3                          smallint          null      default -1,               -- EPL risk group - Sky loses EPL in full
        Risk_Segment_4                          smallint          null      default -1,               -- EPL risk group - Sky loses majority of EPL
        Risk_Segment_5                          smallint          null      default -1,
        Risk_Segment_6                          smallint          null      default -1,
        Risk_Segment_7                          smallint          null      default -1,
        Risk_Segment_8                          smallint          null      default -1,
        Risk_Segment_9                          smallint          null      default -1,
    )
    create        hg   index idx01 on EPL_90_tmp_Risk_Groups(Account_Number)
    create        lf   index idx02 on EPL_90_tmp_Risk_Groups(Period)
    create unique hg   index idx03 on EPL_90_tmp_Risk_Groups(Account_Number, Period)
    create        lf   index idx04 on EPL_90_tmp_Risk_Groups(Sports_Package)
    grant select on EPL_90_tmp_Risk_Groups to vespa_group_low_security


    insert into EPL_90_tmp_Risk_Groups
          (Account_Number, Period, Sports_Package)
    select
        Account_Number,
        Period,
        case
          when Prem_Sports > 0 then 'Sky Sports'
            else 'No Sky Sports'
        end
      from EPL_04_Profiling_Variables
     where Period = 1
    commit
    message '[' || now() || '] Done - risk groups base (15/25)' type status to client


      -- ##### Create table and pull existing information from the profiling analysis                             #####
      -- Basic risk groups
    update EPL_90_tmp_Risk_Groups base
       set base.Risk_Segment_1  =                                               -- Basic risk group - Sky loses EPL In full
          case
            when det.EPL_SoSV in ('High')                                                           and prof.FSS in ('12) Platinum Pensions', '06) Accumulated Wealth', '13) Sunset Security')            then 11
            when det.EPL_SoSV in ('High')                                                                                                                                                                 then 12

            when det.EPL_SoSV in ('Medium')     and prof.Sports_Segment_SIG_v3 = 'Low risk SIGs'                                                                                                          then 7
            when det.EPL_SoSV in ('Medium')     and prof.Sports_Segment_SIG_v3 = 'High risk SIGs'   and prof.FSS in ('12) Platinum Pensions', '06) Accumulated Wealth', '13) Sunset Security')            then 13
            when det.EPL_SoSV in ('Medium')     and prof.Sports_Segment_SIG_v3 = 'High risk SIGs'                                                                                                         then 14

            when det.EPL_SoSV in ('Low')        and det.EPL_SOC in ('Medium', 'High')               and prof.Sports_Segment_SIG_v4 = 'Low risk SIGs'                                                      then 9
            when det.EPL_SoSV in ('Low')        and det.EPL_SOC in ('Medium', 'High')               and prof.Sports_Segment_SIG_v4 = 'High risk SIGs'
                                                                                                            and prof.FSS in ('12) Platinum Pensions', '06) Accumulated Wealth', '13) Sunset Security')    then 15
            when det.EPL_SoSV in ('Low')        and det.EPL_SOC in ('Medium', 'High')               and prof.Sports_Segment_SIG_v4 = 'High risk SIGs'                                                     then 16
            when det.EPL_SoSV in ('Low')        and det.EPL_SOC in ('Low')                                                                                                                                then 6

            when det.EPL_SoSV in ('Did not watch')                                                                                                                                                        then 17
              else 0
          end
      from EPL_90_tmp_Eng_Matrix det,
           EPL_04_Profiling_Variables prof
     where base.Account_Number = det.Account_Number
       and base.Period = det.Period
       and base.Sports_Package = 'Sky Sports'
       and det.Metric = 'Overall'
       and det.Account_Number = prof.Account_Number
       and det.Period = prof.Period
    commit
    message '[' || now() || '] Done - risk groups branches (16/25)' type status to client



    -- This view take information from fields updated above AND below, needs to be updated only once though.
    create or replace view EPL_90_tmp_Risk_Groups_View as
      select
            a.Account_Number,
            a.Period,
            a.Sports_Package,

            a.Risk_Segment_1 as Risk_Segment_1_Raw,
            case
              when a.Risk_Segment_1 in (12)             then 'Branch 1'
              when a.Risk_Segment_1 in (11)             then 'Branch 2'
              when a.Risk_Segment_1 in (14)             then 'Branch 3'
              when a.Risk_Segment_1 in (7)              then 'Branch 4'
              when a.Risk_Segment_1 in (16)             then 'Branch 5'
              when a.Risk_Segment_1 in (13)             then 'Branch 6'
              when a.Risk_Segment_1 in (15)             then 'Branch 7'
              when a.Risk_Segment_1 in (9)              then 'Branch 8'
              when a.Risk_Segment_1 in (6)              then 'Branch 9'
              when a.Risk_Segment_1 in (17)             then 'Branch 10'
              when a.Risk_Segment_1 = 0                 then 'Excluded'
                else 'No Sky Sports'
            end as xRisk_Segment_1,

            case
              when xRisk_Segment_1 = 'Excluded'                                                                 then 'Excluded'


              when xRisk_Segment_1 = 'Branch 1' and a.Risk_Segment_3 in (102, 104, 106, 108)                    then 'Downgrade risk'
              when xRisk_Segment_1 = 'Branch 1' and a.Risk_Segment_3 in (109)                                   then 'Churn risk (with low Sports SoV)'
              when xRisk_Segment_1 = 'Branch 1' and a.Risk_Segment_3 in (110)                                   then 'Churn risk (with high Sports SoV)'
              when xRisk_Segment_1 = 'Branch 1'                                                                 then 'Excluded'

              when xRisk_Segment_1 = 'Branch 2' and a.Risk_Segment_3 in (216)                                   then 'No change'
              when xRisk_Segment_1 = 'Branch 2'                                                                 then 'Excluded'

              when xRisk_Segment_1 = 'Branch 3' and a.Risk_Segment_3 in (302, 304, 306, 308)                    then 'Downgrade risk'
              when xRisk_Segment_1 = 'Branch 3' and a.Risk_Segment_3 in (309)                                   then 'Churn risk (with low Sports SoV)'
              when xRisk_Segment_1 = 'Branch 3' and a.Risk_Segment_3 in (310)                                   then 'Churn risk (with high Sports SoV)'
              when xRisk_Segment_1 = 'Branch 3'                                                                 then 'Excluded'

              when xRisk_Segment_1 = 'Branch 4' and a.Risk_Segment_3 in (416)                                   then 'No change'
              when xRisk_Segment_1 = 'Branch 4'                                                                 then 'Excluded'

              when xRisk_Segment_1 = 'Branch 5' and a.Risk_Segment_3 in (502, 504, 506, 508)                    then 'Downgrade risk'
              when xRisk_Segment_1 = 'Branch 5' and a.Risk_Segment_3 in (509)                                   then 'Churn risk (with low Sports SoV)'
              when xRisk_Segment_1 = 'Branch 5' and a.Risk_Segment_3 in (510)                                   then 'Churn risk (with high Sports SoV)'
              when xRisk_Segment_1 = 'Branch 5'                                                                 then 'Excluded'

              when xRisk_Segment_1 = 'Branch 6' and a.Risk_Segment_3 in (616)                                   then 'No change'
              when xRisk_Segment_1 = 'Branch 6'                                                                 then 'Excluded'

              when xRisk_Segment_1 = 'Branch 7' and a.Risk_Segment_3 in (716)                                   then 'No change'
              when xRisk_Segment_1 = 'Branch 7'                                                                 then 'Excluded'

              when xRisk_Segment_1 = 'Branch 8' and a.Risk_Segment_3 in (816)                                   then 'No change'
              when xRisk_Segment_1 = 'Branch 8'                                                                 then 'Excluded'

              when xRisk_Segment_1 = 'Branch 9' and a.Risk_Segment_3 in (916)                                   then 'No change'
              when xRisk_Segment_1 = 'Branch 9'                                                                 then 'Excluded'

              when xRisk_Segment_1 = 'Branch 10' and a.Risk_Segment_3 in (1016)                                 then 'No change'
              when xRisk_Segment_1 = 'Branch 10'                                                                then 'Excluded'

                else 'No Sky Sports'
            end as xRisk_Segment_3

        from EPL_90_tmp_Risk_Groups a
    commit


      -- EPL risk groups
    update EPL_90_tmp_Risk_Groups a
       set a.Risk_Segment_3  =                                                  -- EPL risk group - Sky loses EPL In full
          case

              -- #######################################################################################################
              -- ##### Branch 1 #####
            when base.xRisk_Segment_1 in ('Branch 1') then
                case
                    when eng.Key_Pay_Entertainment_Avg_DV in ('High')                       then 102
                    when eng.Sky_Sports_News_Avg_DV in ('High')                             then 104
                    when eng.Movies_Avg_DV in ('High')                                      then 106
                    when prof.Number_Of_Sky_Products_No_DTV >= 4                            then 108
                    when eng.Sport_SoV in ('High')                                          then 110
                      else                                                                       109
                end

              -- #######################################################################################################
              -- ##### Branch 2 #####
            when base.xRisk_Segment_1 in ('Branch 2')                                       then 216

              -- #######################################################################################################
              -- ##### Branch 3 #####
            when base.xRisk_Segment_1 in ('Branch 3') then
                case
                    when eng.Key_Pay_Entertainment_Avg_DV in ('High')                       then 302
                    when eng.Sky_Sports_News_Avg_DV in ('High')                             then 304
                    when eng.Movies_Avg_DV in ('High')                                      then 306
                    when prof.Number_Of_Sky_Products_No_DTV >= 4                            then 308
                    when eng.Sport_SoV in ('High')                                          then 310
                      else                                                                       309
                end

              -- #######################################################################################################
              -- ##### Branch 4 #####
            when base.xRisk_Segment_1 in ('Branch 4')                                       then 416

              -- #######################################################################################################
              -- ##### Branch 5 #####
            when base.xRisk_Segment_1 in ('Branch 5') then
                case
                    when eng.Key_Pay_Entertainment_Avg_DV in ('High')                       then 502
                    when eng.Sky_Sports_News_Avg_DV in ('High')                             then 504
                    when eng.Movies_Avg_DV in ('High')                                      then 506
                    when prof.Number_Of_Sky_Products_No_DTV >= 4                            then 508
                    when eng.Sport_SoV in ('High')                                          then 510
                      else                                                                       509
                end

              -- #######################################################################################################
              -- ##### Branch 6 #####
            when base.xRisk_Segment_1 in ('Branch 6')                                       then 616

              -- #######################################################################################################
              -- ##### Branch 7 #####
            when base.xRisk_Segment_1 in ('Branch 7')                                       then 716

              -- #######################################################################################################
              -- ##### Branch 8 #####
            when base.xRisk_Segment_1 in ('Branch 8')                                       then 816

              -- #######################################################################################################
              -- ##### Branch 9 #####
            when base.xRisk_Segment_1 in ('Branch 9')                                       then 916

              -- #######################################################################################################
              -- ##### Branch 10 #####
            when base.xRisk_Segment_1 in ('Branch 10')                                      then 1016

              -- #######################################################################################################
              -- ##### Excluded #####
              else 0

          end

      from EPL_90_tmp_Risk_Groups_View base,
           EPL_90_tmp_Eng_Matrix eng,
           EPL_04_Profiling_Variables prof

     where a.Account_Number = base.Account_Number
       and a.Period = base.Period
       and a.Sports_Package = 'Sky Sports'

       and a.Account_Number = eng.Account_Number
       and a.Period = eng.Period
       and eng.Metric = 'Overall'

       and a.Account_Number = prof.Account_Number
       and a.Period = prof.Period

    commit
    message '[' || now() || '] Done - risk groups segments (17/25)' type status to client



      -- ##############################################################################################################
      -- ##### Append calculated data the main summary table                                                      #####
      -- ##############################################################################################################
    set @varSql = '
                    delete from EPL_90_Stability_EPL__Variable_Period
                     where Period = ' || @parStartPeriod || '
                    commit

                    insert into EPL_90_Stability_EPL__Variable_Period
                          (Account_Number, Period, Period_Start_Date, Period_End_Date, Measure_Group, Low_Content_Flag,
                           Metric, Category, EPL_SOC, EPL_SoSV, Sport_SoV, Key_Pay_Entertainment_Avg_DV, Sky_Sports_News_Avg_DV,
                           Movies_Avg_DV, Risk_Segment, Content_Available, Content_Watched)
                      select
                            a.Account_Number,
                            ' || @parStartPeriod || ',
                            ''' || @parStartDate || ''',
                            ''' || @parEndDate || ''',
                            ''Period ' || @parStartPeriod || ''',

                            a.Low_Content_Flag,
                            a.Metric,
                            a.Category,
                            a.EPL_SOC,
                            a.EPL_SoSV,
                            a.Sport_SoV,
                            a.Key_Pay_Entertainment_Avg_DV,
                            a.Sky_Sports_News_Avg_DV,
                            a.Movies_Avg_DV,
                            b.xRisk_Segment_3,
                            c.Content_Available,
                            c.Content_Watched
                      from EPL_90_tmp_Eng_Matrix a,
                           EPL_90_tmp_Risk_Groups_View b,
                           EPL_90_tmp_SOCs_Summaries c,
                           EPL_04_Profiling_Variables d
                     where a.Account_Number = b.Account_Number
                       and a.Period = b.Period
                       and a.Account_Number = c.Account_Number
                       and a.Period = c.Period
                       and a.Metric = ''Overall''
                       and a.Category = ''(all)''
                       and c.Metric = ''Live games - overall''
                       and c.Category = ''(all)''
                       and a.Account_Number = d.Account_Number
                       and a.Period = d.Period
                       and d.Prem_Sports > 0
                    commit

                    update EPL_90_Stability_EPL__Variable_Period base
                       set base.Viewing_Days = det.Days_Data_Available
                      from EPL_90_tmp_Acc_Num_Days det
                     where base.Account_Number = det.Account_Number
                       and base.Period = ' || @parStartPeriod || '
                    commit
                  '
    execute(@varSql)
    commit
    message '[' || now() || '] Done - period data stored (18/25)' type status to client



      -- ##############################################################################################################
      -- ##### Calculate PCCs                                                                                     #####
      -- ##############################################################################################################
    set @varSql = '
          delete from EPL_90_Stability_EPL__PCCs
           where Period = ' || @parStartPeriod || '
             and Measure_Group = ''##^Var^##''
          commit

          insert into EPL_90_Stability_EPL__PCCs
                (Measure_Group, Period, Period_Start_Date, Period_End_Date, No_Viewing_Data, Correctly_Classified, Total_Accounts, PCC)
            select
                  ''##^Var^##''                                                                                       as Measure_Group,
                  ' || @parStartPeriod || ', ''' || @parStartDate || ''', ''' || @parEndDate || ''',  -- Period, Period_Start_Date, Period_End_Date

                  sum(case when b.Account_Number is null then 1 else 0 end)                                           as No_Viewing_Data,
                  sum(case when a.##^Var^## = b.##^Var^## then 1 else 0 end)                                          as Correctly_Classified,
                  count(*)                                                                                            as Total_Accounts,
                  cast(1.0 * Correctly_Classified / Total_Accounts as decimal(10, 4))                                 as PCC

              from EPL_90_Stability_EPL__Variable_Period a left join EPL_90_Stability_EPL__Variable_Period b
                      on a.Account_Number = b.Account_Number
                     and b.Period = ' || @parStartPeriod || '
             where a.Period = 0
          commit


          delete from EPL_90_Stability_EPL__PCC_Details
           where Period = ' || @parStartPeriod || '
           and Measure_Group = ''##^Var^##''
          commit

          insert into EPL_90_Stability_EPL__PCC_Details
                (Measure_Group, Period, Period_Start_Date, Period_End_Date, Class_Details, Incorrect_Class_Flag, Total_Accounts)
            select
                  ''##^Var^##''                                                                                       as Measure_Group,
                  ' || @parStartPeriod || ', ''' || @parStartDate || ''', ''' || @parEndDate || ''',  -- Period, Period_Start_Date, Period_End_Date
                  a.##^Var^##                                                                                         as Class_Details,
                  case
                    when a.##^Var^## = b.##^Var^## then ''OK''
                      else ''Misclassified''
                  end                                                                                                 as Incorrect_Class_Flag,
                  count(*)                                                                                            as Total_Accounts

              from EPL_90_Stability_EPL__Variable_Period a left join EPL_90_Stability_EPL__Variable_Period b
                      on a.Account_Number = b.Account_Number
                     and b.Period = ' || @parStartPeriod || '
             where a.Period = 0
             group by Class_Details, Incorrect_Class_Flag
          commit


          insert into EPL_90_Stability_EPL__PCC_Details
                (Measure_Group, Period, Period_Start_Date, Period_End_Date, Class_Details, Incorrect_Class_Flag, Total_Accounts)
            select
                  ''##^Var^##''                                                                                       as Measure_Group,
                  ' || @parStartPeriod || ', ''' || @parStartDate || ''', ''' || @parEndDate || ''',  -- Period, Period_Start_Date, Period_End_Date
                  b.##^Var^##                                                                                         as Class_Details,
                  case
                    when a.##^Var^## = b.##^Var^## then ''OK''
                      else ''Incorrectly assigned''
                  end                                                                                                 as Incorrect_Class_Flag,
                  count(*)                                                                                            as Total_Accounts

              from EPL_90_Stability_EPL__Variable_Period a left join EPL_90_Stability_EPL__Variable_Period b
                      on a.Account_Number = b.Account_Number
                     and b.Period = ' || @parStartPeriod || '
             where a.Period = 0
               and b.Account_Number is not null
               and Incorrect_Class_Flag = ''Incorrectly assigned''
             group by Class_Details, Incorrect_Class_Flag
          commit

                  '
    execute( replace(@varSql, '##^Var^##', 'EPL_SOC') )
    message '[' || now() || '] Done - PPCs for "EPL_SOC" (19/25)' type status to client

    execute( replace(@varSql, '##^Var^##', 'EPL_SoSV') )
    message '[' || now() || '] Done - PPCs for "EPL_SoSV" (20/25)' type status to client

    execute( replace(@varSql, '##^Var^##', 'Sport_SoV') )
    message '[' || now() || '] Done - PPCs for "Sport_SoV" (21/25)' type status to client

    execute( replace(@varSql, '##^Var^##', 'Key_Pay_Entertainment_Avg_DV') )
    message '[' || now() || '] Done - PPCs for "Key_Pay_Entertainment_Avg_DV" (22/25)' type status to client

    execute( replace(@varSql, '##^Var^##', 'Sky_Sports_News_Avg_DV') )
    message '[' || now() || '] Done - PPCs for "Sky_Sports_News_Avg_DV" (23/25)' type status to client

    execute( replace(@varSql, '##^Var^##', 'Movies_Avg_DV') )
    message '[' || now() || '] Done - PPCs for "Movies_Avg_DV" (24/25)' type status to client

    execute( replace(@varSql, '##^Var^##', 'Risk_Segment') )
    message '[' || now() || '] Done - PPCs for "Risk_Segment" (25/25)' type status to client

    commit


    set @execTime = datediff(second, @runStartTime, now())
    message '[' || now() || '] ###### Completed. Run time: ' ||
                cast(floor(@execTime / 3600) as tinyint) || 'h ' ||
                cast(floor(@execTime / 60) % 60 as tinyint) || 'm ' ||
                cast(@execTime % 60 as tinyint) || 's ######' type status to client

end;


execute EPL_9_Stability_EPL_Run '2013-08-12', '2013-08-18', 1;
execute EPL_9_Stability_EPL_Run '2013-08-12', '2013-08-25', 2;
execute EPL_9_Stability_EPL_Run '2013-08-12', '2013-09-01', 3;
execute EPL_9_Stability_EPL_Run '2013-08-12', '2013-09-08', 4;
execute EPL_9_Stability_EPL_Run '2013-08-12', '2013-09-15', 5;
execute EPL_9_Stability_EPL_Run '2013-08-12', '2013-09-22', 6;
execute EPL_9_Stability_EPL_Run '2013-08-12', '2013-09-29', 7;
execute EPL_9_Stability_EPL_Run '2013-08-12', '2013-10-06', 8;
execute EPL_9_Stability_EPL_Run '2013-08-12', '2013-10-13', 9;
execute EPL_9_Stability_EPL_Run '2013-08-12', '2013-10-20', 10;
execute EPL_9_Stability_EPL_Run '2013-08-12', '2013-10-27', 11;
execute EPL_9_Stability_EPL_Run '2013-08-12', '2013-11-03', 12;
execute EPL_9_Stability_EPL_Run '2013-08-12', '2013-11-10', 13;
execute EPL_9_Stability_EPL_Run '2013-08-12', '2013-11-17', 14;
execute EPL_9_Stability_EPL_Run '2013-08-12', '2013-11-24', 15;
execute EPL_9_Stability_EPL_Run '2013-08-12', '2013-12-01', 16;
execute EPL_9_Stability_EPL_Run '2013-08-12', '2013-12-08', 17;
execute EPL_9_Stability_EPL_Run '2013-08-12', '2013-12-15', 18;
execute EPL_9_Stability_EPL_Run '2013-08-12', '2013-12-22', 19;
execute EPL_9_Stability_EPL_Run '2013-08-12', '2013-12-29', 20;
execute EPL_9_Stability_EPL_Run '2013-08-12', '2014-01-05', 21;
execute EPL_9_Stability_EPL_Run '2013-08-12', '2014-01-12', 22;
execute EPL_9_Stability_EPL_Run '2013-08-12', '2014-01-19', 23;
execute EPL_9_Stability_EPL_Run '2013-08-12', '2014-01-26', 24;
execute EPL_9_Stability_EPL_Run '2013-08-12', '2014-02-02', 25;
execute EPL_9_Stability_EPL_Run '2013-08-12', '2014-02-09', 26;
execute EPL_9_Stability_EPL_Run '2013-08-12', '2014-02-16', 27;
execute EPL_9_Stability_EPL_Run '2013-08-12', '2014-02-23', 28;




  -- ##############################################################################################################
  -- ##############################################################################################################

















