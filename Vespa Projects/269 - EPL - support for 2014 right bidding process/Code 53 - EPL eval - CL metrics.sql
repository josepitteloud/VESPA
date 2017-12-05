/*###############################################################################
# Created on:   27/05/2014
# Created by:   Sebastian Bednaszynski(SBE)
# Description:  EPL rights project - Metric calculations (SOC, SOV etc.) - CL analysis
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 27/05/2014  SBE   Initial version
#
###############################################################################*/


  -- ##############################################################################################################
  -- ##### Create structures                                                                                  #####
  -- ##############################################################################################################
if object_id('EPL_53_CL_SOCs') is not null then drop table EPL_53_CL_SOCs end if;
create table EPL_53_CL_SOCs (
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
create        hg   index idx01 on EPL_53_CL_SOCs(Account_Number);
create        lf   index idx02 on EPL_53_CL_SOCs(Period);
create        date index idx03 on EPL_53_CL_SOCs(Broadcast_Date);
create        hg   index idx04 on EPL_53_CL_SOCs(Programme_Instance_Name);
grant select on EPL_53_CL_SOCs to vespa_group_low_security;


if object_id('EPL_53_CL_SOCs_Summaries') is not null then drop table EPL_53_CL_SOCs_Summaries end if;
create table EPL_53_CL_SOCs_Summaries (
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
create        hg   index idx01 on EPL_53_CL_SOCs_Summaries(Account_Number);
create        lf   index idx02 on EPL_53_CL_SOCs_Summaries(Period);
create        lf   index idx03 on EPL_53_CL_SOCs_Summaries(Metric);
grant select on EPL_53_CL_SOCs_Summaries to vespa_group_low_security;



  -- ##############################################################################################################
  -- ##### Share of Content - preparation                                                                     #####
  -- ##############################################################################################################
  -- Get content programmes available to each account
-- truncate table EPL_53_CL_SOCs;
insert into EPL_53_CL_SOCs
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
     and b.EPL_Pack = 'Linear broadcast'
     and (
            b.Channel not in ('Sky Sports 1', 'Sky Sports 2', 'Sky Sports 3', 'Sky Sports 4', '3D channel')                             -- Terrestrial/non-premium channels
            or
            ( a.Sky_Sports_Flag = 1 and b.Channel in ('Sky Sports 1', 'Sky Sports 2', 'Sky Sports 3', 'Sky Sports 4', '3D channel') )   -- OR Sky Sports channels, if eligible
         )
   group by a.Account_Number, a.Period, b.Broadcast_Date, b.Channel, b.Programme, b.Programme_Instance_Name,
            b.Kick_Off_Time, b.Day_Of_Week, b.Live_Game_Flag, b.EPL_Pack;
commit;


  -- Attribute viewing
update EPL_53_CL_SOCs base
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
   and base.Programme_Instance_Name = det.Programme_Instance_Name;
commit;


  -- ##############################################################################################################
  -- ##### Generate SOCs                                                                                      #####
  -- ##############################################################################################################
  -- OVERALL - live
delete from EPL_53_CL_SOCs_Summaries
 where Metric in ('Live CL games - overall');
commit;

insert into EPL_53_CL_SOCs_Summaries
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
    from EPL_53_CL_SOCs
   where Live_Game_Flag = 1                                                           -- This is to exclude non-live games which at this stage would be ITV +1 channels
   group by Account_Number, xPeriod, xMetric, xCategory;
commit;


  -- OVERALL - Assume no ability to watch simulcast matches (content available counted as "1" for multicast games)
  --           Deduped at day @ kick off time level
delete from EPL_53_CL_SOCs_Summaries
 where Metric in ('Live CL games - overall (deduped multicast)');
commit;

insert into EPL_53_CL_SOCs_Summaries
      (Account_Number, Period, Metric, Category, Content_Available, Content_Watched, Calculated_SOC)
  select
        Account_Number,
        case
          when Period in (1, 4) then 1                                                -- Period 4 is an extension of period 1
            else Period
        end as xPeriod,
        'Live CL games - overall (deduped multicast)' as xMetric,
        '(all)'                                       as xCategory,
        count(distinct Broadcast_Date || Kick_Off_Time) as xContent_Available,
        sum(Content_Watched)                          as xContent_Watched,
        case
          when xContent_Available = 0 then null
            else 1.0 * xContent_Watched / xContent_Available
        end                                           as xCalculated_SOC
    from EPL_53_CL_SOCs
   where Live_Game_Flag = 1                                                           -- This is to exclude non-live games which at this stage would be ITV +1 channels
   group by Account_Number, xPeriod, xMetric, xCategory;
commit;


  -- OVERALL - Split out matches with British clubs vs those without
delete from EPL_53_CL_SOCs_Summaries
 where Metric in ('Live CL games - British clubs only');
commit;

insert into EPL_53_CL_SOCs_Summaries
      (Account_Number, Period, Metric, Category, Content_Available, Content_Watched, Calculated_SOC)
  select
        Account_Number,
        case
          when Period in (1, 4) then 1                                                -- Period 4 is an extension of period 1
            else Period
        end as xPeriod,
        'Live CL games - British clubs only'          as xMetric,
        '(all)'                                       as xCategory,
        sum(Content_Available)                        as xContent_Available,
        sum(Content_Watched)                          as xContent_Watched,
        case
          when xContent_Available = 0 then null
            else 1.0 * xContent_Watched / xContent_Available
        end                                           as xCalculated_SOC
    from EPL_53_CL_SOCs
   where Live_Game_Flag = 1                                                           -- This is to exclude non-live games which at this stage would be ITV +1 channels
     and (
          Programme like '%Arsenal%' or
          Programme like '%Celtic%' or
          Programme like '%Chelsea%' or
          Programme like '%Man City%' or
          Programme like '%Man Utd%'
         )
   group by Account_Number, xPeriod, xMetric, xCategory;
commit;


  -- OVERALL - Split out ITV v SS matches (Sky SPorts matches only)
delete from EPL_53_CL_SOCs_Summaries
 where Metric in ('Live CL games - Sky Sports only');
commit;

insert into EPL_53_CL_SOCs_Summaries
      (Account_Number, Period, Metric, Category, Content_Available, Content_Watched, Calculated_SOC)
  select
        Account_Number,
        case
          when Period in (1, 4) then 1                                                -- Period 4 is an extension of period 1
            else Period
        end as xPeriod,
        'Live CL games - Sky Sports only'             as xMetric,
        '(all)'                                       as xCategory,
        sum(Content_Available)                        as xContent_Available,
        sum(Content_Watched)                          as xContent_Watched,
        case
          when xContent_Available = 0 then null
            else 1.0 * xContent_Watched / xContent_Available
        end                                           as xCalculated_SOC
    from EPL_53_CL_SOCs
   where Live_Game_Flag = 1                                                           -- This is to exclude non-live games which at this stage would be ITV +1 channels
     and Channel in ('Sky Sports 1', 'Sky Sports 2', 'Sky Sports 3', 'Sky Sports 4')
   group by Account_Number, xPeriod, xMetric, xCategory;
commit;



  -- ##############################################################################################################
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
  from EPL_53_CL_SOCs_Summaries
 group by
      xPeriod,
      Metric,
      Category,
      SOC_1,
      SOC_2;



  -- ##############################################################################################################
  -- ##### Get single account engagement view                                                                 #####
  -- ##############################################################################################################
if object_id('EPL_54_CL_Eng_Matrix') is not null then drop table EPL_54_CL_Eng_Matrix end if;
create table EPL_54_CL_Eng_Matrix (
    Pk_Identifier                           bigint            identity,
    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name(),

      -- Account
    Account_Number                          varchar(20)       null      default null,
    Period                                  tinyint           null      default 0,
    Low_Content_Flag                        bit               null      default 0,

    Metric                                  varchar(50)       null      default null,
    Category                                varchar(50)       null      default null,

    CL_SOC                                  varchar(30)       null      default 'Not calculated',
    CL_SOC__Deciles                         varchar(15)       null      default 'Not calculated',
    CL_SOC__Percentiles                     varchar(15)       null      default 'Not calculated',
    CL_SOC__10_Groups                       varchar(15)       null      default 'Not calculated',
    CL_SOC__25_Groups                       varchar(15)       null      default 'Not calculated',

    CL_SOC_Multicast_Deduped                varchar(30)       null      default 'Not calculated',
    CL_SOC_Multicast_Deduped__Deciles       varchar(15)       null      default 'Not calculated',
    CL_SOC_Multicast_Deduped__Percentiles   varchar(15)       null      default 'Not calculated',
    CL_SOC_Multicast_Deduped__10_Groups     varchar(15)       null      default 'Not calculated',
    CL_SOC_Multicast_Deduped__25_Groups     varchar(15)       null      default 'Not calculated',

    CL_SOC_British_Clubs                    varchar(30)       null      default 'Not calculated',
    CL_SOC_British_Clubs__Deciles           varchar(15)       null      default 'Not calculated',
    CL_SOC_British_Clubs__Percentiles       varchar(15)       null      default 'Not calculated',
    CL_SOC_British_Clubs__10_Groups         varchar(15)       null      default 'Not calculated',
    CL_SOC_British_Clubs__25_Groups         varchar(15)       null      default 'Not calculated',

    CL_SOC_Sky_Sports_Only                  varchar(30)       null      default 'Not calculated',
    CL_SOC_Sky_Sports_Only__Deciles         varchar(15)       null      default 'Not calculated',
    CL_SOC_Sky_Sports_Only__Percentiles     varchar(15)       null      default 'Not calculated',
    CL_SOC_Sky_Sports_Only__10_Groups       varchar(15)       null      default 'Not calculated',
    CL_SOC_Sky_Sports_Only__25_Groups       varchar(15)       null      default 'Not calculated'

);
create        hg   index idx01 on EPL_54_CL_Eng_Matrix(Account_Number);
create        lf   index idx02 on EPL_54_CL_Eng_Matrix(Period);
create        lf   index idx03 on EPL_54_CL_Eng_Matrix(Metric);
grant select on EPL_54_CL_Eng_Matrix to vespa_group_low_security;


  -- Overall level - base
insert into EPL_54_CL_Eng_Matrix
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



  -- ##############################################################################################################
  -- ##############################################################################################################
  -- Measures - OVERALL
  -- • The engagement thresholds discussed were:
  --    o Share of Content (what proportion of all EPL matches does the household watch): L = 1-15%, M = 16-35%, H = 36%+
update EPL_54_CL_Eng_Matrix base
   set base.Low_Content_Flag    = case
                                    when det.Content_Available <= 3 then 1                        -- Low content flag for CL games available
                                      else 0
                                  end,
       base.CL_SOC              = case
                                    when det.Calculated_SOC =  0    then 'Did not watch'
                                    when det.Calculated_SOC <= 0.15 then 'Low'
                                    when det.Calculated_SOC <= 0.35 then 'Medium'
                                      else 'High'
                                  end
  from EPL_53_CL_SOCs_Summaries det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and base.Metric = 'Overall'
   and det.Metric = 'Live CL games - overall'
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
  from EPL_54_CL_Eng_Matrix a,
       (select
              Account_Number,
              Period,
              case
                when Metric = 'Live CL games - overall' then 'Overall'
                  else 'Other ???'
              end as Metric,
              Category,
              Calculated_SOC
          from EPL_53_CL_SOCs_Summaries a
         where a.Metric = 'Live CL games - overall') b
 where a.Account_Number = b.Account_Number
   and a.Period = b.Period
   and a.Metric = b.Metric
   and a.Category = b.Category
   and a.CL_SOC not in ('Did not watch', 'Not calculated');
commit;

update EPL_54_CL_Eng_Matrix
   set CL_SOC__Deciles        = case
                                  when base.CL_SOC in ('Did not watch', 'Not calculated') then base.CL_SOC
                                  when det.Group_Id between 1 and 9 then '0' || cast(det.Group_Id as varchar(1))
                                    else cast(det.Group_Id as varchar(2))
                                end
  from EPL_54_CL_Eng_Matrix base
          left join EPL_tmp_Deciles_Percentiles det
          on base.Account_Number = det.Account_Number
         and base.Period = det.Period
         and base.Metric = det.Metric
         and base.Category = det.Category;
commit;

  -- QA
select Period, Metric, Category, CL_SOC, CL_SOC__Deciles, count(*) as Cnt
  from EPL_54_CL_Eng_Matrix
 group by Period, Metric, Category, CL_SOC, CL_SOC__Deciles
 order by Period, Metric, Category, CL_SOC__Deciles, CL_SOC;


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
  from EPL_54_CL_Eng_Matrix a,
       (select
              Account_Number,
              Period,
              case
                when Metric = 'Live CL games - overall' then 'Overall'
                  else 'Other ???'
              end as Metric,
              Category,
              Calculated_SOC
          from EPL_53_CL_SOCs_Summaries a
         where a.Metric = 'Live CL games - overall') b
 where a.Account_Number = b.Account_Number
   and a.Period = b.Period
   and a.Metric = b.Metric
   and a.Category = b.Category
   and a.CL_SOC not in ('Did not watch', 'Not calculated');
commit;

update EPL_54_CL_Eng_Matrix
   set CL_SOC__Percentiles    = case
                                  when base.CL_SOC in ('Did not watch', 'Not calculated') then base.CL_SOC
                                  when det.Group_Id between 1 and 9 then '00' || cast(det.Group_Id as varchar(1))
                                  when det.Group_Id between 10 and 99 then '0' || cast(det.Group_Id as varchar(2))
                                    else cast(det.Group_Id as varchar(3))
                                end
  from EPL_54_CL_Eng_Matrix base
          left join EPL_tmp_Deciles_Percentiles det
          on base.Account_Number = det.Account_Number
         and base.Period = det.Period
         and base.Metric = det.Metric
         and base.Category = det.Category;
commit;

  -- QA
select Period, Metric, Category, CL_SOC, CL_SOC__Percentiles, count(*) as Cnt
  from EPL_54_CL_Eng_Matrix
 group by Period, Metric, Category, CL_SOC, CL_SOC__Percentiles
 order by Period, Metric, Category, CL_SOC__Percentiles, CL_SOC;


  -- 10 & 25 Groups
update EPL_54_CL_Eng_Matrix base
  set base.CL_SOC__10_Groups  = case
                                  when base.CL_SOC in ('Did not watch', 'Not calculated') then base.CL_SOC
                                  when det.Calculated_SOC = 0 then '01) DNW'
                                  when ceil(det.Calculated_SOC * 100) <=  10 then '01) 1-10%'
                                  when ceil(det.Calculated_SOC * 100) <=  20 then '02) 11-20%'
                                  when ceil(det.Calculated_SOC * 100) <=  30 then '03) 21-30%'
                                  when ceil(det.Calculated_SOC * 100) <=  40 then '04) 31-40%'
                                  when ceil(det.Calculated_SOC * 100) <=  50 then '05) 41-50%'
                                  when ceil(det.Calculated_SOC * 100) <=  60 then '06) 51-60%'
                                  when ceil(det.Calculated_SOC * 100) <=  70 then '07) 61-70%'
                                  when ceil(det.Calculated_SOC * 100) <=  80 then '08) 71-80%'
                                  when ceil(det.Calculated_SOC * 100) <=  90 then '09) 81-90%'
                                  when ceil(det.Calculated_SOC * 100) <= 100 then '10) 91-100%'
                                    else '99) ???'
                                end,
      base.CL_SOC__25_Groups  = case
                                  when base.CL_SOC in ('Did not watch', 'Not calculated') then base.CL_SOC
                                  when det.Calculated_SOC = 0 then '01) DNW'
                                  when ceil(det.Calculated_SOC * 100) <=   5 then '01) 1-5%'
                                  when ceil(det.Calculated_SOC * 100) <=  10 then '02) 6-10%'
                                  when ceil(det.Calculated_SOC * 100) <=  15 then '03) 11-15%'
                                  when ceil(det.Calculated_SOC * 100) <=  20 then '04) 16-20%'
                                  when ceil(det.Calculated_SOC * 100) <=  25 then '05) 21-25%'
                                  when ceil(det.Calculated_SOC * 100) <=  30 then '06) 26-30%'
                                  when ceil(det.Calculated_SOC * 100) <=  35 then '07) 31-35%'
                                  when ceil(det.Calculated_SOC * 100) <=  40 then '08) 36-40%'
                                  when ceil(det.Calculated_SOC * 100) <=  45 then '09) 41-45%'
                                  when ceil(det.Calculated_SOC * 100) <=  50 then '10) 46-50%'
                                  when ceil(det.Calculated_SOC * 100) <=  55 then '11) 51-55%'
                                  when ceil(det.Calculated_SOC * 100) <=  60 then '12) 56-60%'
                                  when ceil(det.Calculated_SOC * 100) <=  65 then '13) 61-65%'
                                  when ceil(det.Calculated_SOC * 100) <=  70 then '14) 66-70%'
                                  when ceil(det.Calculated_SOC * 100) <=  75 then '15) 71-75%'
                                  when ceil(det.Calculated_SOC * 100) <=  80 then '16) 76-80%'
                                  when ceil(det.Calculated_SOC * 100) <=  85 then '17) 81-85%'
                                  when ceil(det.Calculated_SOC * 100) <=  90 then '18) 86-90%'
                                  when ceil(det.Calculated_SOC * 100) <=  95 then '19) 91-95%'
                                  when ceil(det.Calculated_SOC * 100) <= 100 then '20) 96-100%'
                                    else '99) ???'
                                end
  from EPL_53_CL_SOCs_Summaries det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and base.Metric = 'Overall'
   and det.Metric = 'Live CL games - overall';
commit;

  -- QA
select CL_SOC__10_Groups, count(*) from EPL_54_CL_Eng_Matrix group by CL_SOC__10_Groups order by 1;
select CL_SOC__25_Groups, count(*) from EPL_54_CL_Eng_Matrix group by CL_SOC__25_Groups order by 1;


  -- ##############################################################################################################
  -- Measures - OVERALL
  -- • Assume no ability to watch simulcast matches (content available counted as "1" for multicast games)
  --   Deduped at day @ kick off time level

  -- ### THRESHOLDS NOT DEFINED ###
update EPL_54_CL_Eng_Matrix base
   set base.CL_SOC_Multicast_Deduped
                                = case
                                    when det.Calculated_SOC =  0    then 'Did not watch'
                                    -- when det.Calculated_SOC <= ???? then 'Low'
                                    -- when det.Calculated_SOC <= ???? then 'Medium'
                                      else '???'
                                  end
  from EPL_53_CL_SOCs_Summaries det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and base.Metric = 'Overall'
   and det.Metric = 'Live CL games - overall (deduped multicast)'
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
  from EPL_54_CL_Eng_Matrix a,
       (select
              Account_Number,
              Period,
              case
                when Metric = 'Live CL games - overall (deduped multicast)' then 'Overall'
                  else 'Other ???'
              end as Metric,
              Category,
              Calculated_SOC
          from EPL_53_CL_SOCs_Summaries a
         where a.Metric = 'Live CL games - overall (deduped multicast)') b
 where a.Account_Number = b.Account_Number
   and a.Period = b.Period
   and a.Metric = b.Metric
   and a.Category = b.Category
   and a.CL_SOC_Multicast_Deduped not in ('Did not watch', 'Not calculated');
commit;

update EPL_54_CL_Eng_Matrix
   set CL_SOC_Multicast_Deduped__Deciles
                              = case
                                  when base.CL_SOC_Multicast_Deduped in ('Did not watch', 'Not calculated') then base.CL_SOC_Multicast_Deduped
                                  when det.Group_Id between 1 and 9 then '0' || cast(det.Group_Id as varchar(1))
                                    else cast(det.Group_Id as varchar(2))
                                end
  from EPL_54_CL_Eng_Matrix base
          left join EPL_tmp_Deciles_Percentiles det
          on base.Account_Number = det.Account_Number
         and base.Period = det.Period
         and base.Metric = det.Metric
         and base.Category = det.Category;
commit;

  -- QA
select Period, Metric, Category, CL_SOC_Multicast_Deduped, CL_SOC_Multicast_Deduped__Deciles, count(*) as Cnt
  from EPL_54_CL_Eng_Matrix
 group by Period, Metric, Category, CL_SOC_Multicast_Deduped, CL_SOC_Multicast_Deduped__Deciles
 order by Period, Metric, Category, CL_SOC_Multicast_Deduped__Deciles, CL_SOC_Multicast_Deduped;


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
  from EPL_54_CL_Eng_Matrix a,
       (select
              Account_Number,
              Period,
              case
                when Metric = 'Live CL games - overall (deduped multicast)' then 'Overall'
                  else 'Other ???'
              end as Metric,
              Category,
              Calculated_SOC
          from EPL_53_CL_SOCs_Summaries a
         where a.Metric = 'Live CL games - overall (deduped multicast)') b
 where a.Account_Number = b.Account_Number
   and a.Period = b.Period
   and a.Metric = b.Metric
   and a.Category = b.Category
   and a.CL_SOC_Multicast_Deduped not in ('Did not watch', 'Not calculated');
commit;

update EPL_54_CL_Eng_Matrix
   set CL_SOC_Multicast_Deduped__Percentiles
                              = case
                                  when base.CL_SOC_Multicast_Deduped in ('Did not watch', 'Not calculated') then base.CL_SOC_Multicast_Deduped
                                  when det.Group_Id between 1 and 9 then '00' || cast(det.Group_Id as varchar(1))
                                  when det.Group_Id between 10 and 99 then '0' || cast(det.Group_Id as varchar(2))
                                    else cast(det.Group_Id as varchar(3))
                                end
  from EPL_54_CL_Eng_Matrix base
          left join EPL_tmp_Deciles_Percentiles det
          on base.Account_Number = det.Account_Number
         and base.Period = det.Period
         and base.Metric = det.Metric
         and base.Category = det.Category;
commit;

  -- QA
select Period, Metric, Category, CL_SOC_Multicast_Deduped, CL_SOC_Multicast_Deduped__Percentiles, count(*) as Cnt
  from EPL_54_CL_Eng_Matrix
 group by Period, Metric, Category, CL_SOC_Multicast_Deduped, CL_SOC_Multicast_Deduped__Percentiles
 order by Period, Metric, Category, CL_SOC_Multicast_Deduped__Percentiles, CL_SOC_Multicast_Deduped;


  -- 10 & 25 Groups
update EPL_54_CL_Eng_Matrix base
  set base.CL_SOC_Multicast_Deduped__10_Groups
                              = case
                                  when base.CL_SOC_Multicast_Deduped in ('Did not watch', 'Not calculated') then base.CL_SOC_Multicast_Deduped
                                  when ceil(det.Calculated_SOC * 100) <=  10 then '01) 1-10%'
                                  when ceil(det.Calculated_SOC * 100) <=  20 then '02) 11-20%'
                                  when ceil(det.Calculated_SOC * 100) <=  30 then '03) 21-30%'
                                  when ceil(det.Calculated_SOC * 100) <=  40 then '04) 31-40%'
                                  when ceil(det.Calculated_SOC * 100) <=  50 then '05) 41-50%'
                                  when ceil(det.Calculated_SOC * 100) <=  60 then '06) 51-60%'
                                  when ceil(det.Calculated_SOC * 100) <=  70 then '07) 61-70%'
                                  when ceil(det.Calculated_SOC * 100) <=  80 then '08) 71-80%'
                                  when ceil(det.Calculated_SOC * 100) <=  90 then '09) 81-90%'
                                  when ceil(det.Calculated_SOC * 100) <= 100 then '10) 91-100%'
                                    else '11) 100%+'
                                end,
      base.CL_SOC_Multicast_Deduped__25_Groups
                              = case
                                  when base.CL_SOC_Multicast_Deduped in ('Did not watch', 'Not calculated') then base.CL_SOC_Multicast_Deduped
                                  when ceil(det.Calculated_SOC * 100) <=   5 then '01) 1-5%'
                                  when ceil(det.Calculated_SOC * 100) <=  10 then '02) 6-10%'
                                  when ceil(det.Calculated_SOC * 100) <=  15 then '03) 11-15%'
                                  when ceil(det.Calculated_SOC * 100) <=  20 then '04) 16-20%'
                                  when ceil(det.Calculated_SOC * 100) <=  25 then '05) 21-25%'
                                  when ceil(det.Calculated_SOC * 100) <=  30 then '06) 26-30%'
                                  when ceil(det.Calculated_SOC * 100) <=  35 then '07) 31-35%'
                                  when ceil(det.Calculated_SOC * 100) <=  40 then '08) 36-40%'
                                  when ceil(det.Calculated_SOC * 100) <=  45 then '09) 41-45%'
                                  when ceil(det.Calculated_SOC * 100) <=  50 then '10) 46-50%'
                                  when ceil(det.Calculated_SOC * 100) <=  55 then '11) 51-55%'
                                  when ceil(det.Calculated_SOC * 100) <=  60 then '12) 56-60%'
                                  when ceil(det.Calculated_SOC * 100) <=  65 then '13) 61-65%'
                                  when ceil(det.Calculated_SOC * 100) <=  70 then '14) 66-70%'
                                  when ceil(det.Calculated_SOC * 100) <=  75 then '15) 71-75%'
                                  when ceil(det.Calculated_SOC * 100) <=  80 then '16) 76-80%'
                                  when ceil(det.Calculated_SOC * 100) <=  85 then '17) 81-85%'
                                  when ceil(det.Calculated_SOC * 100) <=  90 then '18) 86-90%'
                                  when ceil(det.Calculated_SOC * 100) <=  95 then '19) 91-95%'
                                  when ceil(det.Calculated_SOC * 100) <= 100 then '20) 96-100%'
                                    else '21) 100%+'
                                end
  from EPL_53_CL_SOCs_Summaries det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and base.Metric = 'Overall'
   and det.Metric = 'Live CL games - overall (deduped multicast)';
commit;

  -- QA
select CL_SOC_Multicast_Deduped__10_Groups, count(*) from EPL_54_CL_Eng_Matrix group by CL_SOC_Multicast_Deduped__10_Groups order by 1;
select CL_SOC_Multicast_Deduped__25_Groups, count(*) from EPL_54_CL_Eng_Matrix group by CL_SOC_Multicast_Deduped__25_Groups order by 1;


  -- ##############################################################################################################
  -- Measures - OVERALL
  -- • Split out matches with British clubs vs those without

update EPL_54_CL_Eng_Matrix base
   set base.CL_SOC_British_Clubs
                                = case
                                    when det.Calculated_SOC =  0    then 'Did not watch'
                                    when det.Calculated_SOC <= 0.20 then 'Low'
                                    when det.Calculated_SOC <= 0.45 then 'Medium'
                                      else '???'
                                  end
  from EPL_53_CL_SOCs_Summaries det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and base.Metric = 'Overall'
   and det.Metric = 'Live CL games - British clubs only'
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
  from EPL_54_CL_Eng_Matrix a,
       (select
              Account_Number,
              Period,
              case
                when Metric = 'Live CL games - British clubs only' then 'Overall'
                  else 'Other ???'
              end as Metric,
              Category,
              Calculated_SOC
          from EPL_53_CL_SOCs_Summaries a
         where a.Metric = 'Live CL games - British clubs only') b
 where a.Account_Number = b.Account_Number
   and a.Period = b.Period
   and a.Metric = b.Metric
   and a.Category = b.Category
   and a.CL_SOC_British_Clubs not in ('Did not watch', 'Not calculated');
commit;

update EPL_54_CL_Eng_Matrix
   set CL_SOC_British_Clubs__Deciles        = case
                                  when base.CL_SOC_British_Clubs in ('Did not watch', 'Not calculated') then base.CL_SOC_British_Clubs
                                  when det.Group_Id between 1 and 9 then '0' || cast(det.Group_Id as varchar(1))
                                    else cast(det.Group_Id as varchar(2))
                                end
  from EPL_54_CL_Eng_Matrix base
          left join EPL_tmp_Deciles_Percentiles det
          on base.Account_Number = det.Account_Number
         and base.Period = det.Period
         and base.Metric = det.Metric
         and base.Category = det.Category;
commit;

  -- QA
select Period, Metric, Category, CL_SOC_British_Clubs, CL_SOC_British_Clubs__Deciles, count(*) as Cnt
  from EPL_54_CL_Eng_Matrix
 group by Period, Metric, Category, CL_SOC_British_Clubs, CL_SOC_British_Clubs__Deciles
 order by Period, Metric, Category, CL_SOC_British_Clubs__Deciles, CL_SOC_British_Clubs;


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
  from EPL_54_CL_Eng_Matrix a,
       (select
              Account_Number,
              Period,
              case
                when Metric = 'Live CL games - British clubs only' then 'Overall'
                  else 'Other ???'
              end as Metric,
              Category,
              Calculated_SOC
          from EPL_53_CL_SOCs_Summaries a
         where a.Metric = 'Live CL games - British clubs only') b
 where a.Account_Number = b.Account_Number
   and a.Period = b.Period
   and a.Metric = b.Metric
   and a.Category = b.Category
   and a.CL_SOC_British_Clubs not in ('Did not watch', 'Not calculated');
commit;

update EPL_54_CL_Eng_Matrix
   set CL_SOC_British_Clubs__Percentiles
                              = case
                                  when base.CL_SOC_British_Clubs in ('Did not watch', 'Not calculated') then base.CL_SOC_British_Clubs
                                  when det.Group_Id between 1 and 9 then '00' || cast(det.Group_Id as varchar(1))
                                  when det.Group_Id between 10 and 99 then '0' || cast(det.Group_Id as varchar(2))
                                    else cast(det.Group_Id as varchar(3))
                                end
  from EPL_54_CL_Eng_Matrix base
          left join EPL_tmp_Deciles_Percentiles det
          on base.Account_Number = det.Account_Number
         and base.Period = det.Period
         and base.Metric = det.Metric
         and base.Category = det.Category;
commit;

  -- QA
select Period, Metric, Category, CL_SOC_British_Clubs, CL_SOC_British_Clubs__Percentiles, count(*) as Cnt
  from EPL_54_CL_Eng_Matrix
 group by Period, Metric, Category, CL_SOC_British_Clubs, CL_SOC_British_Clubs__Percentiles
 order by Period, Metric, Category, CL_SOC_British_Clubs__Percentiles, CL_SOC_British_Clubs;


  -- 10 & 25 Groups
update EPL_54_CL_Eng_Matrix base
  set base.CL_SOC_British_Clubs__10_Groups
                              = case
                                  when base.CL_SOC_British_Clubs in ('Did not watch', 'Not calculated') then base.CL_SOC_British_Clubs
                                  when ceil(det.Calculated_SOC * 100) <=  10 then '01) 1-10%'
                                  when ceil(det.Calculated_SOC * 100) <=  20 then '02) 11-20%'
                                  when ceil(det.Calculated_SOC * 100) <=  30 then '03) 21-30%'
                                  when ceil(det.Calculated_SOC * 100) <=  40 then '04) 31-40%'
                                  when ceil(det.Calculated_SOC * 100) <=  50 then '05) 41-50%'
                                  when ceil(det.Calculated_SOC * 100) <=  60 then '06) 51-60%'
                                  when ceil(det.Calculated_SOC * 100) <=  70 then '07) 61-70%'
                                  when ceil(det.Calculated_SOC * 100) <=  80 then '08) 71-80%'
                                  when ceil(det.Calculated_SOC * 100) <=  90 then '09) 81-90%'
                                  when ceil(det.Calculated_SOC * 100) <= 100 then '10) 91-100%'
                                    else '99) ???'
                                end,
      base.CL_SOC_British_Clubs__25_Groups
                              = case
                                  when base.CL_SOC_British_Clubs in ('Did not watch', 'Not calculated') then base.CL_SOC_British_Clubs
                                  when ceil(det.Calculated_SOC * 100) <=   5 then '01) 1-5%'
                                  when ceil(det.Calculated_SOC * 100) <=  10 then '02) 6-10%'
                                  when ceil(det.Calculated_SOC * 100) <=  15 then '03) 11-15%'
                                  when ceil(det.Calculated_SOC * 100) <=  20 then '04) 16-20%'
                                  when ceil(det.Calculated_SOC * 100) <=  25 then '05) 21-25%'
                                  when ceil(det.Calculated_SOC * 100) <=  30 then '06) 26-30%'
                                  when ceil(det.Calculated_SOC * 100) <=  35 then '07) 31-35%'
                                  when ceil(det.Calculated_SOC * 100) <=  40 then '08) 36-40%'
                                  when ceil(det.Calculated_SOC * 100) <=  45 then '09) 41-45%'
                                  when ceil(det.Calculated_SOC * 100) <=  50 then '10) 46-50%'
                                  when ceil(det.Calculated_SOC * 100) <=  55 then '11) 51-55%'
                                  when ceil(det.Calculated_SOC * 100) <=  60 then '12) 56-60%'
                                  when ceil(det.Calculated_SOC * 100) <=  65 then '13) 61-65%'
                                  when ceil(det.Calculated_SOC * 100) <=  70 then '14) 66-70%'
                                  when ceil(det.Calculated_SOC * 100) <=  75 then '15) 71-75%'
                                  when ceil(det.Calculated_SOC * 100) <=  80 then '16) 76-80%'
                                  when ceil(det.Calculated_SOC * 100) <=  85 then '17) 81-85%'
                                  when ceil(det.Calculated_SOC * 100) <=  90 then '18) 86-90%'
                                  when ceil(det.Calculated_SOC * 100) <=  95 then '19) 91-95%'
                                  when ceil(det.Calculated_SOC * 100) <= 100 then '20) 96-100%'
                                    else '99) ???'
                                end
  from EPL_53_CL_SOCs_Summaries det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and base.Metric = 'Overall'
   and det.Metric = 'Live CL games - British clubs only';
commit;

  -- QA
select CL_SOC_British_Clubs__10_Groups, count(*) from EPL_54_CL_Eng_Matrix group by CL_SOC_British_Clubs__10_Groups order by 1;
select CL_SOC_British_Clubs__25_Groups, count(*) from EPL_54_CL_Eng_Matrix group by CL_SOC_British_Clubs__25_Groups order by 1;




  -- ##############################################################################################################
  -- Measures - OVERALL
  -- • Split out ITV v SS matches (Sky SPorts matches only)

  -- ### THRESHOLDS NOT DEFINED ###
update EPL_54_CL_Eng_Matrix base
   set base.CL_SOC_Sky_Sports_Only
                                = case
                                    when det.Calculated_SOC =  0    then 'Did not watch'
                                    -- when det.Calculated_SOC <= ???? then 'Low'
                                    -- when det.Calculated_SOC <= ???? then 'Medium'
                                      else '???'
                                  end
  from EPL_53_CL_SOCs_Summaries det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and base.Metric = 'Overall'
   and det.Metric = 'Live CL games - Sky Sports only'
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
  from EPL_54_CL_Eng_Matrix a,
       (select
              Account_Number,
              Period,
              case
                when Metric = 'Live CL games - Sky Sports only' then 'Overall'
                  else 'Other ???'
              end as Metric,
              Category,
              Calculated_SOC
          from EPL_53_CL_SOCs_Summaries a
         where a.Metric = 'Live CL games - Sky Sports only') b
 where a.Account_Number = b.Account_Number
   and a.Period = b.Period
   and a.Metric = b.Metric
   and a.Category = b.Category
   and a.CL_SOC_Sky_Sports_Only not in ('Did not watch', 'Not calculated');
commit;

update EPL_54_CL_Eng_Matrix
   set CL_SOC_Sky_Sports_Only__Deciles        = case
                                  when base.CL_SOC_Sky_Sports_Only in ('Did not watch', 'Not calculated') then base.CL_SOC_Sky_Sports_Only
                                  when det.Group_Id between 1 and 9 then '0' || cast(det.Group_Id as varchar(1))
                                    else cast(det.Group_Id as varchar(2))
                                end
  from EPL_54_CL_Eng_Matrix base
          left join EPL_tmp_Deciles_Percentiles det
          on base.Account_Number = det.Account_Number
         and base.Period = det.Period
         and base.Metric = det.Metric
         and base.Category = det.Category;
commit;

  -- QA
select Period, Metric, Category, CL_SOC_Sky_Sports_Only, CL_SOC_Sky_Sports_Only__Deciles, count(*) as Cnt
  from EPL_54_CL_Eng_Matrix
 group by Period, Metric, Category, CL_SOC_Sky_Sports_Only, CL_SOC_Sky_Sports_Only__Deciles
 order by Period, Metric, Category, CL_SOC_Sky_Sports_Only__Deciles, CL_SOC_Sky_Sports_Only;


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
  from EPL_54_CL_Eng_Matrix a,
       (select
              Account_Number,
              Period,
              case
                when Metric = 'Live CL games - Sky Sports only' then 'Overall'
                  else 'Other ???'
              end as Metric,
              Category,
              Calculated_SOC
          from EPL_53_CL_SOCs_Summaries a
         where a.Metric = 'Live CL games - Sky Sports only') b
 where a.Account_Number = b.Account_Number
   and a.Period = b.Period
   and a.Metric = b.Metric
   and a.Category = b.Category
   and a.CL_SOC_Sky_Sports_Only not in ('Did not watch', 'Not calculated');
commit;

update EPL_54_CL_Eng_Matrix
   set CL_SOC_Sky_Sports_Only__Percentiles
                              = case
                                  when base.CL_SOC_Sky_Sports_Only in ('Did not watch', 'Not calculated') then base.CL_SOC_Sky_Sports_Only
                                  when det.Group_Id between 1 and 9 then '00' || cast(det.Group_Id as varchar(1))
                                  when det.Group_Id between 10 and 99 then '0' || cast(det.Group_Id as varchar(2))
                                    else cast(det.Group_Id as varchar(3))
                                end
  from EPL_54_CL_Eng_Matrix base
          left join EPL_tmp_Deciles_Percentiles det
          on base.Account_Number = det.Account_Number
         and base.Period = det.Period
         and base.Metric = det.Metric
         and base.Category = det.Category;
commit;

  -- QA
select Period, Metric, Category, CL_SOC_Sky_Sports_Only, CL_SOC_Sky_Sports_Only__Percentiles, count(*) as Cnt
  from EPL_54_CL_Eng_Matrix
 group by Period, Metric, Category, CL_SOC_Sky_Sports_Only, CL_SOC_Sky_Sports_Only__Percentiles
 order by Period, Metric, Category, CL_SOC_Sky_Sports_Only__Percentiles, CL_SOC_Sky_Sports_Only;


  -- 10 & 25 Groups
update EPL_54_CL_Eng_Matrix base
  set base.CL_SOC_Sky_Sports_Only__10_Groups
                              = case
                                  when base.CL_SOC_Sky_Sports_Only in ('Did not watch', 'Not calculated') then base.CL_SOC_Sky_Sports_Only
                                  when ceil(det.Calculated_SOC * 100) <=  10 then '01) 1-10%'
                                  when ceil(det.Calculated_SOC * 100) <=  20 then '02) 11-20%'
                                  when ceil(det.Calculated_SOC * 100) <=  30 then '03) 21-30%'
                                  when ceil(det.Calculated_SOC * 100) <=  40 then '04) 31-40%'
                                  when ceil(det.Calculated_SOC * 100) <=  50 then '05) 41-50%'
                                  when ceil(det.Calculated_SOC * 100) <=  60 then '06) 51-60%'
                                  when ceil(det.Calculated_SOC * 100) <=  70 then '07) 61-70%'
                                  when ceil(det.Calculated_SOC * 100) <=  80 then '08) 71-80%'
                                  when ceil(det.Calculated_SOC * 100) <=  90 then '09) 81-90%'
                                  when ceil(det.Calculated_SOC * 100) <= 100 then '10) 91-100%'
                                    else '99) ???'
                                end,
      base.CL_SOC_Sky_Sports_Only__25_Groups
                              = case
                                  when base.CL_SOC_Sky_Sports_Only in ('Did not watch', 'Not calculated') then base.CL_SOC_Sky_Sports_Only
                                  when ceil(det.Calculated_SOC * 100) <=   5 then '01) 1-5%'
                                  when ceil(det.Calculated_SOC * 100) <=  10 then '02) 6-10%'
                                  when ceil(det.Calculated_SOC * 100) <=  15 then '03) 11-15%'
                                  when ceil(det.Calculated_SOC * 100) <=  20 then '04) 16-20%'
                                  when ceil(det.Calculated_SOC * 100) <=  25 then '05) 21-25%'
                                  when ceil(det.Calculated_SOC * 100) <=  30 then '06) 26-30%'
                                  when ceil(det.Calculated_SOC * 100) <=  35 then '07) 31-35%'
                                  when ceil(det.Calculated_SOC * 100) <=  40 then '08) 36-40%'
                                  when ceil(det.Calculated_SOC * 100) <=  45 then '09) 41-45%'
                                  when ceil(det.Calculated_SOC * 100) <=  50 then '10) 46-50%'
                                  when ceil(det.Calculated_SOC * 100) <=  55 then '11) 51-55%'
                                  when ceil(det.Calculated_SOC * 100) <=  60 then '12) 56-60%'
                                  when ceil(det.Calculated_SOC * 100) <=  65 then '13) 61-65%'
                                  when ceil(det.Calculated_SOC * 100) <=  70 then '14) 66-70%'
                                  when ceil(det.Calculated_SOC * 100) <=  75 then '15) 71-75%'
                                  when ceil(det.Calculated_SOC * 100) <=  80 then '16) 76-80%'
                                  when ceil(det.Calculated_SOC * 100) <=  85 then '17) 81-85%'
                                  when ceil(det.Calculated_SOC * 100) <=  90 then '18) 86-90%'
                                  when ceil(det.Calculated_SOC * 100) <=  95 then '19) 91-95%'
                                  when ceil(det.Calculated_SOC * 100) <= 100 then '20) 96-100%'
                                    else '99) ???'
                                end
  from EPL_53_CL_SOCs_Summaries det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and base.Metric = 'Overall'
   and det.Metric = 'Live CL games - Sky Sports only';
commit;

  -- QA
select CL_SOC_Sky_Sports_Only__10_Groups, count(*) from EPL_54_CL_Eng_Matrix group by CL_SOC_Sky_Sports_Only__10_Groups order by 1;
select CL_SOC_Sky_Sports_Only__25_Groups, count(*) from EPL_54_CL_Eng_Matrix group by CL_SOC_Sky_Sports_Only__25_Groups order by 1;


  -- ##############################################################################################################
  -- ##############################################################################################################
  -- SUMMARIZE
if object_id('EPL_55_CL_Eng_Matrix_Summary') is not null then drop table EPL_55_CL_Eng_Matrix_Summary end if;
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
      CL_SOC,
      CL_SOC__10_Groups,
      CL_SOC__25_Groups,
      CL_SOC_Multicast_Deduped,
      CL_SOC_Multicast_Deduped__10_Groups,
      CL_SOC_Multicast_Deduped__25_Groups,
      CL_SOC_British_Clubs,
      CL_SOC_British_Clubs__10_Groups,
      CL_SOC_British_Clubs__25_Groups,
      CL_SOC_Sky_Sports_Only,
      CL_SOC_Sky_Sports_Only__10_Groups,
      CL_SOC_Sky_Sports_Only__25_Groups,
      count(*) as Accounts_Unscaled
  into EPL_55_CL_Eng_Matrix_Summary
  from EPL_54_CL_Eng_Matrix a
 where Account_Number in (select Account_Number
                            from EPL_04_Profiling_Variables
                           where Period = 1
                             and Prem_Sports > 0)
 group by
      xPeriod,
      xLow_Content_Flag,
      Metric,
      Category,
      CL_SOC,
      CL_SOC__10_Groups,
      CL_SOC__25_Groups,
      CL_SOC_Multicast_Deduped,
      CL_SOC_Multicast_Deduped__10_Groups,
      CL_SOC_Multicast_Deduped__25_Groups,
      CL_SOC_British_Clubs,
      CL_SOC_British_Clubs__10_Groups,
      CL_SOC_British_Clubs__25_Groups,
      CL_SOC_Sky_Sports_Only,
      CL_SOC_Sky_Sports_Only__10_Groups,
      CL_SOC_Sky_Sports_Only__25_Groups;



  -- ##############################################################################################################
  -- ##############################################################################################################


















