/*###############################################################################
# Created on:   15/12/2014
# Created by:   Sebastian Bednaszynski(SBE)
# Description:  EPL rights project - stability analysis for Champions League
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 15/12/2014  SBE   Initial version
#
###############################################################################*/



  -- ##############################################################################################################
  -- ##### Get the original segments first                                                                    #####
  -- ##############################################################################################################
if object_id('EPL_91_Stability_CL__Variable_Period') is not null then drop table EPL_91_Stability_CL__Variable_Period end if;
create table EPL_91_Stability_CL__Variable_Period (
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

    Metric                                  varchar(50)       null      default null,
    Category                                varchar(50)       null      default null,

    CL_SOC                                  varchar(30)       null      default 'Not calculated',

    Content_Available                       smallint          null      default 0,
    Content_Watched                         smallint          null      default 0
);
create        hg   index idx01 on EPL_91_Stability_CL__Variable_Period(Account_Number);
create        lf   index idx02 on EPL_91_Stability_CL__Variable_Period(Period);
create        lf   index idx03 on EPL_91_Stability_CL__Variable_Period(Metric);
create        lf   index idx04 on EPL_91_Stability_CL__Variable_Period(Measure_Group);
create unique hg   index idx05 on EPL_91_Stability_CL__Variable_Period(Account_Number, Period);
grant select on EPL_91_Stability_CL__Variable_Period to vespa_group_low_security;


delete from EPL_91_Stability_CL__Variable_Period
 where Period = 0;
commit;

insert into EPL_91_Stability_CL__Variable_Period
      (Account_Number, Period, Period_Start_Date, Period_End_Date, Measure_Group, Metric, Category,
       CL_SOC, Content_Available, Content_Watched)
  select
        a.Account_Number,
        0,
        '2013-01-08',
        '2014-05-31',
        'Actual',

        a.Metric,
        a.Category,
        a.CL_SOC,
        c.Content_Available,
        c.Content_Watched
  from EPL_54_CL_Eng_Matrix a,
       EPL_91_tmp_SOCs_Summaries c,
       EPL_04_Profiling_Variables d
 where a.Period = 1
   and a.Account_Number = c.Account_Number
   and a.Period = c.Period
   and a.Metric = 'Overall'
   and a.Category = '(all)'
   and c.Metric = 'Live CL games - overall'
   and c.Category = '(all)'
   and a.Account_Number = d.Account_Number
   and a.Period = d.Period
   and d.Prem_Sports > 0;
commit;


/*
update EPL_91_Stability_CL__Variable_Period base
   set base.Viewing_Days = det.Viewing_Days
  from (select
              Account_Number,
              count(distinct Viewing_Day) as Viewing_Days
          from EPL_91_tmp_Viewing_Days
         group by Account_Number) det
 where base.Account_Number = det.Account_Number
   and base.Period = 0;
commit;
*/



  -- Summary table for PCCs
if object_id('EPL_91_Stability_CL__PCCs') is not null then drop table EPL_91_Stability_CL__PCCs end if;
create table EPL_91_Stability_CL__PCCs (
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
create unique hg   index idx01 on EPL_91_Stability_CL__PCCs(Measure_Group, Period);
grant select on EPL_91_Stability_CL__PCCs to vespa_group_low_security;


if object_id('EPL_91_Stability_CL_PCC_Details') is not null then drop table EPL_91_Stability_CL_PCC_Details end if;
create table EPL_91_Stability_CL_PCC_Details (
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
grant select on EPL_91_Stability_CL_PCC_Details to vespa_group_low_security;



  -- ##############################################################################################################
  -- ##### Flag accounts with <=70% days available                                                            #####
  -- ##############################################################################################################
set option query_temp_space_limit = 0;


if object_id('EPL_9_Stability_CL_Run') is not null then drop procedure EPL_9_Stability_CL_Run end if;
create procedure EPL_9_Stability_CL_Run
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

/*
      -- ##############################################################################################################
      -- ##### Calculate metrics                                                                                  #####
      -- ##############################################################################################################
    if object_id('EPL_91_tmp_SOCs') is not null drop table EPL_91_tmp_SOCs
    create table EPL_91_tmp_SOCs (
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
    create        hg   index idx01 on EPL_91_tmp_SOCs(Account_Number)
    create        lf   index idx02 on EPL_91_tmp_SOCs(Period)
    create        date index idx03 on EPL_91_tmp_SOCs(Broadcast_Date)
    create        hg   index idx04 on EPL_91_tmp_SOCs(Programme_Instance_Name)
    grant select on EPL_91_tmp_SOCs to vespa_group_low_security


    if object_id('EPL_91_tmp_SOCs_Summaries') is not null drop table EPL_91_tmp_SOCs_Summaries
    create table EPL_91_tmp_SOCs_Summaries (
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
    create        hg   index idx01 on EPL_91_tmp_SOCs_Summaries(Account_Number)
    create        lf   index idx02 on EPL_91_tmp_SOCs_Summaries(Period)
    create        lf   index idx03 on EPL_91_tmp_SOCs_Summaries(Metric)
    grant select on EPL_91_tmp_SOCs_Summaries to vespa_group_low_security



      -- ##### Share of Content - preparation                                                                     #####
    insert into EPL_91_tmp_SOCs
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
             EPL_50_CL_EPG b
       where a.Data_Day = b.Broadcast_Date
         and a.Valid_Account_Flag = 1
         and a.DTV_Flag = 1
         and b.Live_Game_Flag = 1
         and b.EPL_Pack = 'Linear broadcast'
         and (
                b.Channel not in ('Sky Sports 1', 'Sky Sports 2', 'Sky Sports 3', 'Sky Sports 4', '3D channel')                             -- Terrestrial/non-premium channels
                or
                ( a.Sky_Sports_Flag = 1 and b.Channel in ('Sky Sports 1', 'Sky Sports 2', 'Sky Sports 3', 'Sky Sports 4', '3D channel') )   -- OR Sky Sports channels, if eligible
             )
         and a.Data_Day between @parStartDate and @parEndDate
       group by a.Account_Number, a.Period, b.Broadcast_Date, b.Channel, b.Programme, b.Programme_Instance_Name,
                b.Kick_Off_Time, b.Day_Of_Week, b.Live_Game_Flag, b.EPL_Pack
    commit


      -- Attribute viewing
    update EPL_91_tmp_SOCs base
       set base.Content_Watched = 1
      from (select
                  Account_Number,
                  Period,
                  Programme_Instance_Name
              from EPL_52_CL_Viewing_Summary
             where Dk_Programme_Instance_Dim > -1                                     -- EPL live/non-live content only
               and Viewing_Type in (1, 2)                                             -- Live & Live pause of up to 15 minutes
             group by Account_Number, Period, Programme_Instance_Name
            having sum(Viewing_Duration) >= (15 * 60) ) det                           -- Aggregated viewing of at least 15 minutes
     where base.Account_Number = det.Account_Number
       and base.Period = det.Period
       and base.Programme_Instance_Name = det.Programme_Instance_Name
    commit
    message '[' || now() || '] Done - SOCs (1/6)' type status to client


      -- ##############################################################################################################
      -- ##### Generate SOCs                                                                                      #####
      -- ##############################################################################################################
      -- OVERALL - live
    delete from EPL_91_tmp_SOCs_Summaries
     where Metric in ('Live CL games - overall')
    commit

    insert into EPL_91_tmp_SOCs_Summaries
          (Account_Number, Period, Metric, Category, Content_Available, Content_Watched, Calculated_SOC)
      select
            Account_Number,
            case
              when Period in (1, 4) then 1                                                -- Period 4 is an extension of period 1
                else Period
            end as xPeriod,
            'Live CL games - overall'                     as xMetric,
            '(all)'                                       as xCategory,
            sum(Content_Available)                        as xContent_Available,
            sum(Content_Watched)                          as xContent_Watched,
            case
              when xContent_Available = 0 then null
                else 1.0 * xContent_Watched / xContent_Available
            end                                           as xCalculated_SOC
        from EPL_91_tmp_SOCs
       where Live_Game_Flag = 1                                                           -- This is to exclude non-live games which at this stage would be ITV +1 channels
       group by Account_Number, xPeriod, xMetric, xCategory
    commit
    message '[' || now() || '] Done - SOC summaries (2/6)' type status to client



      -- ##### Get single account engagement view                                                                 #####
    if object_id('EPL_91_tmp_Eng_Matrix') is not null drop table EPL_91_tmp_Eng_Matrix
    create table EPL_91_tmp_Eng_Matrix (
        Pk_Identifier                           bigint            identity,
        Updated_On                              datetime          not null  default timestamp,
        Updated_By                              varchar(30)       not null  default user_name(),

          -- Account
        Account_Number                          varchar(20)       null      default null,
        Period                                  tinyint           null      default 0,
        Low_Content_Flag                        bit               null      default 0,

        Metric                                  varchar(50)       null      default null,
        Category                                varchar(50)       null      default null,

        CL_SOC                                  varchar(30)       null      default 'Not calculated'
    )
    create        hg   index idx01 on EPL_91_tmp_Eng_Matrix(Account_Number)
    create        lf   index idx02 on EPL_91_tmp_Eng_Matrix(Period)
    create        lf   index idx03 on EPL_91_tmp_Eng_Matrix(Metric)
    grant select on EPL_91_tmp_Eng_Matrix to vespa_group_low_security


      -- Overall level - base
    insert into EPL_91_tmp_Eng_Matrix
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
    message '[' || now() || '] Done - engagement matrix base (3/6)' type status to client



      -- Share of Content (what proportion of all EPL matches does the household watch): H = >50%, M = 20-50% and L = 0.1-19.9%
    update EPL_91_tmp_Eng_Matrix base
       set base.CL_SOC              = case
                                        when det.Calculated_SOC =  0    then 'Did not watch'
                                        when det.Calculated_SOC <= 0.15 then 'Low'
                                        when det.Calculated_SOC <= 0.35 then 'Medium'
                                          else 'High'
                                      end
      from EPL_91_tmp_SOCs_Summaries det
     where base.Account_Number = det.Account_Number
       and base.Period = det.Period
       and base.Metric = 'Overall'
       and det.Metric = 'Live CL games - overall'
       and base.Category = det.Category
    commit
    message '[' || now() || '] Done - metric categories for "CL SOC" (4/6)' type status to client



      -- ##############################################################################################################
      -- ##### Append calculated data the main summary table                                                      #####
      -- ##############################################################################################################
    set @varSql = '
                    delete from EPL_91_Stability_CL__Variable_Period
                     where Period = ' || @parStartPeriod || '
                    commit

                    insert into EPL_91_Stability_CL__Variable_Period
                          (Account_Number, Period, Period_Start_Date, Period_End_Date, Measure_Group, Metric, Category,
                           CL_SOC, Content_Available, Content_Watched)
                      select
                            a.Account_Number,
                            ' || @parStartPeriod || ',
                            ''' || @parStartDate || ''',
                            ''' || @parEndDate || ''',
                            ''Period ' || @parStartPeriod || ''',

                            a.Metric,
                            a.Category,
                            a.CL_SOC,
                            c.Content_Available,
                            c.Content_Watched
                      from EPL_91_tmp_Eng_Matrix a,
                           EPL_91_tmp_SOCs_Summaries c,
                           EPL_04_Profiling_Variables d
                     where a.Period = 1
                       and a.Account_Number = c.Account_Number
                       and a.Period = c.Period
                       and a.Metric = ''Overall''
                       and a.Category = ''(all)''
                       and c.Metric = ''Live CL games - overall''
                       and c.Category = ''(all)''
                       and a.Account_Number = d.Account_Number
                       and a.Period = d.Period
                       and d.Prem_Sports > 0
                    commit
                  '
    execute(@varSql)
    commit
    message '[' || now() || '] Done - period data stored (5/6)' type status to client
*/


      -- ##############################################################################################################
      -- ##### Calculate PCCs                                                                                     #####
      -- ##############################################################################################################
    set @varSql = '
--          delete from EPL_91_Stability_CL__PCCs
--           where Period = ' || @parStartPeriod || '
--             and Measure_Group = ''##^Var^##''
--          commit
--
--          insert into EPL_91_Stability_CL__PCCs
--                (Measure_Group, Period, Period_Start_Date, Period_End_Date, No_Viewing_Data, Correctly_Classified, Total_Accounts, PCC)
--            select
--                  ''##^Var^##''                                                                                       as Measure_Group,
--                  ' || @parStartPeriod || ', ''' || @parStartDate || ''', ''' || @parEndDate || ''',  -- Period, Period_Start_Date, Period_End_Date
--
--                  sum(case when b.Account_Number is null then 1 else 0 end)                                           as No_Viewing_Data,
--                  sum(case when a.##^Var^## = b.##^Var^## then 1 else 0 end)                                          as Correctly_Classified,
--                  count(*)                                                                                            as Total_Accounts,
--                  cast(1.0 * Correctly_Classified / Total_Accounts as decimal(10, 4))                                 as PCC
--
--              from EPL_91_Stability_CL__Variable_Period a left join EPL_91_Stability_CL__Variable_Period b
--                      on a.Account_Number = b.Account_Number
--                     and b.Period = ' || @parStartPeriod - 15 || '
--             where a.Period = 15
--          commit
--
--
--          delete from EPL_91_Stability_CL_PCC_Details
--           where Period = ' || @parStartPeriod || '
--           and Measure_Group = ''##^Var^##''
--          commit
--
--          insert into EPL_91_Stability_CL_PCC_Details
--                (Measure_Group, Period, Period_Start_Date, Period_End_Date, Class_Details, Incorrect_Class_Flag, Total_Accounts)
--            select
--                  ''##^Var^##''                                                                                       as Measure_Group,
--                  ' || @parStartPeriod || ', ''' || @parStartDate || ''', ''' || @parEndDate || ''',  -- Period, Period_Start_Date, Period_End_Date
--                  a.##^Var^##                                                                                         as Class_Details,
--                  case
--                    when a.##^Var^## = b.##^Var^## then ''OK''
--                      else ''Misclassified''
--                  end                                                                                                 as Incorrect_Class_Flag,
--                  count(*)                                                                                            as Total_Accounts
--
--              from EPL_91_Stability_CL__Variable_Period a left join EPL_91_Stability_CL__Variable_Period b
--                      on a.Account_Number = b.Account_Number
--                     and b.Period = ' || @parStartPeriod - 15 || '
--             where a.Period = 0
--             group by Class_Details, Incorrect_Class_Flag
--          commit


          insert into EPL_91_Stability_CL_PCC_Details
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

              from EPL_91_Stability_CL__Variable_Period a left join EPL_91_Stability_CL__Variable_Period b
                      on a.Account_Number = b.Account_Number
                     and b.Period = ' || @parStartPeriod || '
             where a.Period = 0
               and b.Account_Number is not null
               and Incorrect_Class_Flag = ''Incorrectly assigned''
             group by Class_Details, Incorrect_Class_Flag
          commit

                  '
    execute( replace(@varSql, '##^Var^##', 'CL_SOC') )
    message '[' || now() || '] Done - PPCs for "CL_SOC" (6/6)' type status to client

    commit


    set @execTime = datediff(second, @runStartTime, now())
    message '[' || now() || '] ###### Completed. Run time: ' ||
                cast(floor(@execTime / 3600) as tinyint) || 'h ' ||
                cast(floor(@execTime / 60) % 60 as tinyint) || 'm ' ||
                cast(@execTime % 60 as tinyint) || 's ######' type status to client

end;


execute EPL_9_Stability_CL_Run '2014-02-17', '2014-02-23', 1;
execute EPL_9_Stability_CL_Run '2014-02-17', '2014-03-02', 2;
execute EPL_9_Stability_CL_Run '2014-02-17', '2014-03-09', 3;
execute EPL_9_Stability_CL_Run '2014-02-17', '2014-03-16', 4;
execute EPL_9_Stability_CL_Run '2014-02-17', '2014-03-23', 5;
execute EPL_9_Stability_CL_Run '2014-02-17', '2014-03-30', 6;
execute EPL_9_Stability_CL_Run '2014-02-17', '2014-04-06', 7;
execute EPL_9_Stability_CL_Run '2014-02-17', '2014-04-13', 8;
execute EPL_9_Stability_CL_Run '2014-02-17', '2014-04-20', 9;
execute EPL_9_Stability_CL_Run '2014-02-17', '2014-04-27', 10;
execute EPL_9_Stability_CL_Run '2014-02-17', '2014-05-04', 11;
execute EPL_9_Stability_CL_Run '2014-02-17', '2014-05-11', 12;
execute EPL_9_Stability_CL_Run '2014-02-17', '2014-05-18', 13;
execute EPL_9_Stability_CL_Run '2014-02-17', '2014-05-25', 14;
execute EPL_9_Stability_CL_Run '2014-02-17', '2014-06-01', 15;

execute EPL_9_Stability_CL_Run '2014-02-17', '2014-02-23', 16;
execute EPL_9_Stability_CL_Run '2014-02-17', '2014-03-02', 17;
execute EPL_9_Stability_CL_Run '2014-02-17', '2014-03-09', 18;
execute EPL_9_Stability_CL_Run '2014-02-17', '2014-03-16', 19;
execute EPL_9_Stability_CL_Run '2014-02-17', '2014-03-23', 20;
execute EPL_9_Stability_CL_Run '2014-02-17', '2014-03-30', 21;
execute EPL_9_Stability_CL_Run '2014-02-17', '2014-04-06', 22;
execute EPL_9_Stability_CL_Run '2014-02-17', '2014-04-13', 23;
execute EPL_9_Stability_CL_Run '2014-02-17', '2014-04-20', 24;
execute EPL_9_Stability_CL_Run '2014-02-17', '2014-04-27', 25;
execute EPL_9_Stability_CL_Run '2014-02-17', '2014-05-04', 26;
execute EPL_9_Stability_CL_Run '2014-02-17', '2014-05-11', 27;
execute EPL_9_Stability_CL_Run '2014-02-17', '2014-05-18', 28;
execute EPL_9_Stability_CL_Run '2014-02-17', '2014-05-25', 29;
execute EPL_9_Stability_CL_Run '2014-02-17', '2014-06-01', 30;





  -- ##############################################################################################################
  -- ##############################################################################################################

















