/*###############################################################################
# Created on:   26/11/2013
# Created by:   Sebastian Bednaszynski
# Description:  Opinion formers - viewing data summary
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 26/11/2013  SBE   Initial version
#
###############################################################################*/



  -- ##############################################################################################################
  -- ##### STEP 1 - Get summary by account & day                                                              #####
  -- ##############################################################################################################
if object_id('OpForm_03_Account_Daily_Summary') is not null then drop table OpForm_03_Account_Daily_Summary end if;
create table OpForm_03_Account_Daily_Summary (
      Account_Number                      varchar(20)                default null,
      Viewing_Day                         date                       default null,
      Weekend_Flag                        varchar(3)        null     default 'No',

      Dur_Daytime                         bigint            null     default 0,
      Dur_Primetime                       bigint            null     default 0,

      Dur_Live                            bigint            null     default 0,
      Dur_Live_Daytime                    bigint            null     default 0,
      Dur_Live_Primetime                  bigint            null     default 0,

      Dur_Playback                        bigint            null     default 0,
      Dur_Playback_Daytime                bigint            null     default 0,
      Dur_Playback_Daytime_RecDaytime     bigint            null     default 0,
      Dur_Playback_Daytime_RecPrimetime   bigint            null     default 0,
      Dur_Playback_Primetime              bigint            null     default 0,
      Dur_Playback_Primetime_RecDaytime   bigint            null     default 0,
      Dur_Playback_Primetime_RecPrimetime bigint            null     default 0,

      Cnt_VOD_Daytime                     bigint            null     default 0,
      Cnt_VOD_Primetime                   bigint            null     default 0,

      Daytime_Consumption                 varchar(10)       null     default 'No viewing',
      Primetime_Consumption               varchar(10)       null     default 'No viewing'
);

create        hg index idx01 on OpForm_03_Account_Daily_Summary(Account_Number);
create      date index idx02 on OpForm_03_Account_Daily_Summary(Viewing_Day);


insert into OpForm_03_Account_Daily_Summary
  select
        Account_Number,
        Viewing_Day,
        max(Weekend_Flag)                                 as Weekend_Flag,

        sum(case
              when Event_Type = 'Linear' then Instance_Duration_Daytime
                else 0
            end)                                          as Dur_Daytime,
        sum(case
              when Event_Type = 'Linear' then Instance_Duration_Primetime
                else 0
            end)                                          as Dur_Primetime,

        sum(case
              when Event_Type = 'Linear' and Playback_Flag = 0 then Instance_Duration_DayPrimetime
                else 0
            end)                                          as Dur_Live,
        sum(case
              when Event_Type = 'Linear' and Playback_Flag = 0 then Instance_Duration_Daytime
                else 0
            end)                                          as Dur_Live_Daytime,
        sum(case
              when Event_Type = 'Linear' and Playback_Flag = 0 then Instance_Duration_Primetime
                else 0
            end)                                          as Dur_Live_Primetime,

        sum(case
              when Event_Type = 'Linear' and Playback_Flag = 1 then Instance_Duration_DayPrimetime
                else 0
            end)                                          as Dur_Playback,
        sum(case
              when Event_Type = 'Linear' and Playback_Flag = 1 then Instance_Duration_Daytime
                else 0
            end)                                          as Dur_Playback_Daytime,
        sum(case
              when Event_Type = 'Linear' and Playback_Flag = 1 and Daytime_Broadcast = 1 then Instance_Duration_Daytime
                else 0
            end)                                          as Dur_Playback_Daytime_RecDaytime,
        sum(case
              when Event_Type = 'Linear' and Playback_Flag = 1 and Primetime_Broadcast = 1 then Instance_Duration_Daytime
                else 0
            end)                                          as Dur_Playback_Daytime_RecPrimetime,
        sum(case
              when Event_Type = 'Linear' and Playback_Flag = 1 then Instance_Duration_Primetime
                else 0
            end)                                          as Dur_Playback_Primetime,
        sum(case
              when Event_Type = 'Linear' and Playback_Flag = 1 and Daytime_Broadcast = 1 then Instance_Duration_Primetime
                else 0
            end)                                          as Dur_Playback_Primetime_RecDaytime,
        sum(case
              when Event_Type = 'Linear' and Playback_Flag = 1 and Primetime_Broadcast = 1 then Instance_Duration_Primetime
                else 0
            end)                                          as Dur_Playback_Primetime_RecPrimetime,

        sum(case
              when Event_Type in ('PPV', 'Anytime+ DL') and Instance_Duration_Daytime > 5 then 1          -- At least 5% downloaded
                else 0
            end)                                          as Cnt_VOD_Daytime,
        sum(case
              when Event_Type in ('PPV', 'Anytime+ DL') and Instance_Duration_Primetime > 5 then 1        -- At least 5% downloaded
                else 0
            end)                                          as Cnt_VOD_Primetime,
        'No viewing',
        'No viewing'
    from OpForm_02_Raw_Viewing_Events
   group by Account_Number, Viewing_Day;
commit;


  --Workout Low/High usage to apply back to the results above
if object_id('OpForm_03_Account_Daily_Summary_Daytime_Ntile') is not null then drop table OpForm_03_Account_Daily_Summary_Daytime_Ntile end if;
select
      Account_Number,
      Viewing_Day,
      Weekend_Flag,
      Dur_Daytime,
      ntile(9) over (partition by Weekend_Flag order by Dur_Daytime desc) Daytime_Ntile
  into OpForm_03_Account_Daily_Summary_Daytime_Ntile
  from OpForm_03_Account_Daily_Summary
 where Dur_Daytime > 0;
commit;

if object_id('OpForm_03_Account_Daily_Summary_Primetime_Ntile') is not null then drop table OpForm_03_Account_Daily_Summary_Primetime_Ntile end if;
select
      Account_Number,
      Viewing_Day,
      Weekend_Flag,
      Dur_Primetime,
      ntile(9) over (partition by Weekend_Flag order by Dur_Primetime desc) Primetime_Ntile
  into OpForm_03_Account_Daily_Summary_Primetime_Ntile
  from OpForm_03_Account_Daily_Summary
 where Dur_Primetime > 0;
commit;

/*
select
      Weekend_Flag,
      Daytime_Ntile,
      min(Dur_Daytime) as Min_Dur_Daytime,
      max(Dur_Daytime) as Max_Dur_Daytime
  from OpForm_03_Account_Daily_Summary_Daytime_Ntile
 group by Weekend_Flag, Daytime_Ntile
 order by 1, 2;

select
      Weekend_Flag,
      Primetime_Ntile,
      min(Dur_Primetime) as Min_Dur_Primetime,
      max(Dur_Primetime) as Max_Dur_Primetime
  from OpForm_03_Account_Daily_Summary_Primetime_Ntile
 group by Weekend_Flag, Primetime_Ntile
 order by 1, 2;
*/

update OpForm_03_Account_Daily_Summary base
   set Daytime_Consumption  = case
                                when Daytime_Ntile between 1 and 3 then 'High'
                                when Daytime_Ntile between 4 and 6 then 'Medium'
                                when Daytime_Ntile between 7 and 9 then 'Low'
                                  else '???'
                              end
  from (select
              Weekend_Flag,
              Daytime_Ntile,
              min(Dur_Daytime) as Min_Dur_Daytime,
              max(Dur_Daytime) as Max_Dur_Daytime
          from OpForm_03_Account_Daily_Summary_Daytime_Ntile
         group by Weekend_Flag, Daytime_Ntile) det
 where base.Weekend_Flag = det.Weekend_Flag
   and base.Dur_Daytime between det.Min_Dur_Daytime and det.Max_Dur_Daytime;
commit;

update OpForm_03_Account_Daily_Summary base
   set Primetime_Consumption  = case
                                  when Primetime_Ntile between 1 and 3 then 'High'
                                  when Primetime_Ntile between 4 and 6 then 'Medium'
                                  when Primetime_Ntile between 7 and 9 then 'Low'
                                    else '???'
                                end
  from (select
              Weekend_Flag,
              Primetime_Ntile,
              min(Dur_Primetime) as Min_Dur_Primetime,
              max(Dur_Primetime) as Max_Dur_Primetime
          from OpForm_03_Account_Daily_Summary_Primetime_Ntile
         group by Weekend_Flag, Primetime_Ntile) det
 where base.Weekend_Flag = det.Weekend_Flag
   and base.Dur_Primetime between det.Min_Dur_Primetime and det.Max_Dur_Primetime;
commit;

/*
select
      Daytime_Consumption,
      min(Dur_Daytime) as Min_Dur_Daytime,
      max(Dur_Daytime) as Max_Dur_Daytime
  from OpForm_03_Account_Daily_Summary
 group by Daytime_Consumption
 order by 1;

select
      Primetime_Consumption,
      min(Dur_Primetime) as Min_Dur_Primetime,
      max(Dur_Primetime) as Max_Dur_Primetime
  from OpForm_03_Account_Daily_Summary
 group by Primetime_Consumption
 order by 1;
*/

grant select on OpForm_03_Account_Daily_Summary to vespa_group_low_security;



  -- ##############################################################################################################
  -- ##### STEP 2 - Aggregate                                                                                 #####
  -- ##############################################################################################################
if object_id('OpForm_04_Account_Daily_Summary_Aggr') is not null then drop table OpForm_04_Account_Daily_Summary_Aggr end if;
select
      HH_Composition,
      HH_Lifestage,
      Mirror_ABC1,
      Kids_Age_le4,
      Kids_Age_4to9,
      Kids_Age_10to15,
      Mosaic,
      H_Affluence,
      Region,

      Weekend_Flag,

      sum(Dur_Daytime)                          as Tot_Dur_Daytime,
      sum(Dur_Primetime)                        as Tot_Dur_Primetime,

      sum(Dur_Live)                             as Tot_Dur_Live,
      sum(Dur_Live_Daytime)                     as Tot_Dur_Live_Daytime,
      sum(Dur_Live_Primetime)                   as Tot_Dur_Live_Primetime,

      sum(Dur_Playback)                         as Tot_Dur_Playback,
      sum(Dur_Playback_Daytime)                 as Tot_Dur_Playback_Daytime,
      sum(Dur_Playback_Daytime_RecDaytime)      as Tot_Dur_Playback_Daytime_RecDaytime,
      sum(Dur_Playback_Daytime_RecPrimetime)    as Tot_Dur_Playback_Daytime_RecPrimetime,
      sum(Dur_Playback_Primetime)               as Tot_Dur_Playback_Primetime,
      sum(Dur_Playback_Primetime_RecDaytime)    as Tot_Dur_Playback_Primetime_RecDaytime,
      sum(Dur_Playback_Primetime_RecPrimetime)  as Tot_Dur_Playback_Primetime_RecPrimetime,

      sum(Cnt_VOD_Daytime)                      as Tot_Cnt_VOD_Daytime,
      sum(Cnt_VOD_Primetime)                    as Tot_Cnt_VOD_Primetime,

      count(*)                                                                                            as Accs_Total,
      count( (case when Dur_Daytime > 300 then a.Account_Number else null end))                           as Accs_Daytime,
      count( (case when Dur_Primetime > 300 then a.Account_Number else null end))                         as Accs_Primetime,

      count( (case when Dur_Live > 300 then a.Account_Number else null end))                              as Accs_Live,
      count( (case when Dur_Live_Daytime > 300  then a.Account_Number else null end))                     as Accs_Live_Daytime,
      count( (case when Dur_Live_Primetime > 300  then a.Account_Number else null end))                   as Accs_Live_Primetime,
      count( (case when Dur_Playback > 300  then a.Account_Number else null end))                         as Accs_Playback,
      count( (case when Dur_Playback_Daytime > 300  then a.Account_Number else null end))                 as Accs_Playback_Daytime,
      count( (case when Dur_Playback_Daytime_RecDaytime > 300  then a.Account_Number else null end))      as Accs_Playback_Daytime_RecDaytime,
      count( (case when Dur_Playback_Daytime_RecPrimetime > 300  then a.Account_Number else null end))    as Accs_Playback_Daytime_RecPrimetime,
      count( (case when Dur_Playback_Primetime > 300  then a.Account_Number else null end))               as Accs_Playback_Primetime,
      count( (case when Dur_Playback_Primetime_RecDaytime > 300  then a.Account_Number else null end))    as Accs_Playback_Primetime_RecDaytime,
      count( (case when Dur_Playback_Primetime_RecPrimetime > 300  then a.Account_Number else null end))  as Accs_Playback_Primetime_RecPrimetime,

      count( (case when Cnt_VOD_Daytime > 1 then a.Account_Number else null end))                         as Accs_VOD_Daytime,
      count( (case when Cnt_VOD_Primetime > 1  then a.Account_Number else null end))                      as Accs_VOD_Primetime

  into OpForm_04_Account_Daily_Summary_Aggr
  from OpForm_01_Account_Attributes a,
       OpForm_03_Account_Daily_Summary b
 where a.Account_Number = b.Account_Number
   and a.Ent_DTV_Sub = 1
   and a.Movmt_DTV_Sub = 0
 group by HH_Composition, HH_Lifestage, Mirror_ABC1, Kids_Age_le4, Kids_Age_4to9, Kids_Age_10to15, Mosaic,
          H_Affluence, Region, Weekend_Flag;
commit;

grant select on OpForm_04_Account_Daily_Summary_Aggr to vespa_group_low_security;



  -- ##############################################################################################################
  -- ##### STEP 3 - Genres summary                                                                            #####
  -- ##############################################################################################################
if object_id('OpForm_03_Account_Daily_Genres_Summary') is not null then drop table OpForm_03_Account_Daily_Genres_Summary end if;
create table OpForm_03_Account_Daily_Genres_Summary (
      Account_Number                      varchar(20)                default null,
      Viewing_Day                         date                       default null,
      Genre                               varchar(200)      null     default 'Unknown',

      Weekend_Flag                        varchar(3)        null     default 'No',

      Dur_Daytime                         bigint            null     default 0,
      Dur_Primetime                       bigint            null     default 0,

      Dur_Live                            bigint            null     default 0,
      Dur_Live_Daytime                    bigint            null     default 0,
      Dur_Live_Primetime                  bigint            null     default 0,

      Dur_Playback                        bigint            null     default 0,
      Dur_Playback_Daytime                bigint            null     default 0,
      Dur_Playback_Daytime_RecDaytime     bigint            null     default 0,
      Dur_Playback_Daytime_RecPrimetime   bigint            null     default 0,
      Dur_Playback_Primetime              bigint            null     default 0,
      Dur_Playback_Primetime_RecDaytime   bigint            null     default 0,
      Dur_Playback_Primetime_RecPrimetime bigint            null     default 0,

      Cnt_VOD_Daytime                     bigint            null     default 0,
      Cnt_VOD_Primetime                   bigint            null     default 0,

      Daytime_Consumption                 varchar(10)       null     default 'No viewing',
      Primetime_Consumption               varchar(10)       null     default 'No viewing'
);

create        hg index idx01 on OpForm_03_Account_Daily_Genres_Summary(Account_Number);
create      date index idx02 on OpForm_03_Account_Daily_Genres_Summary(Viewing_Day);


insert into OpForm_03_Account_Daily_Genres_Summary
  select
        Account_Number,
        Viewing_Day,
        case
          when Genre_Description = 'Unknown' or Sub_Genre_Description = 'Unknown' then 'Unknown'
            else Genre_Description || ' (' || Sub_Genre_Description || ')'
        end as Genre,

        max(Weekend_Flag)                                 as Weekend_Flag,

        sum(case
              when Event_Type = 'Linear' then Instance_Duration_Daytime
                else 0
            end)                                          as Dur_Daytime,
        sum(case
              when Event_Type = 'Linear' then Instance_Duration_Primetime
                else 0
            end)                                          as Dur_Primetime,

        sum(case
              when Event_Type = 'Linear' and Playback_Flag = 0 then Instance_Duration_DayPrimetime
                else 0
            end)                                          as Dur_Live,
        sum(case
              when Event_Type = 'Linear' and Playback_Flag = 0 then Instance_Duration_Daytime
                else 0
            end)                                          as Dur_Live_Daytime,
        sum(case
              when Event_Type = 'Linear' and Playback_Flag = 0 then Instance_Duration_Primetime
                else 0
            end)                                          as Dur_Live_Primetime,

        sum(case
              when Event_Type = 'Linear' and Playback_Flag = 1 then Instance_Duration_DayPrimetime
                else 0
            end)                                          as Dur_Playback,
        sum(case
              when Event_Type = 'Linear' and Playback_Flag = 1 then Instance_Duration_Daytime
                else 0
            end)                                          as Dur_Playback_Daytime,
        sum(case
              when Event_Type = 'Linear' and Playback_Flag = 1 and Daytime_Broadcast = 1 then Instance_Duration_Daytime
                else 0
            end)                                          as Dur_Playback_Daytime_RecDaytime,
        sum(case
              when Event_Type = 'Linear' and Playback_Flag = 1 and Primetime_Broadcast = 1 then Instance_Duration_Daytime
                else 0
            end)                                          as Dur_Playback_Daytime_RecPrimetime,
        sum(case
              when Event_Type = 'Linear' and Playback_Flag = 1 then Instance_Duration_Primetime
                else 0
            end)                                          as Dur_Playback_Primetime,
        sum(case
              when Event_Type = 'Linear' and Playback_Flag = 1 and Daytime_Broadcast = 1 then Instance_Duration_Primetime
                else 0
            end)                                          as Dur_Playback_Primetime_RecDaytime,
        sum(case
              when Event_Type = 'Linear' and Playback_Flag = 1 and Primetime_Broadcast = 1 then Instance_Duration_Primetime
                else 0
            end)                                          as Dur_Playback_Primetime_RecPrimetime,

        sum(case
              when Event_Type in ('PPV', 'Anytime+ DL') and Instance_Duration_Daytime > 5 then 1          -- At least 5% downloaded
                else 0
            end)                                          as Cnt_VOD_Daytime,
        sum(case
              when Event_Type in ('PPV', 'Anytime+ DL') and Instance_Duration_Primetime > 5 then 1        -- At least 5% downloaded
                else 0
            end)                                          as Cnt_VOD_Primetime,
        'No viewing',
        'No viewing'
    from OpForm_02_Raw_Viewing_Events
   group by Account_Number, Viewing_Day, Genre;
commit;


if object_id('OpForm_03_Account_Daily_Genres_Summary_Aggr') is not null then drop table OpForm_03_Account_Daily_Genres_Summary_Aggr end if;
select
      Weekend_Flag,
      Genre

      count(*)                                                                                            as Accs_Total,
      count( (case when Dur_Daytime > 300 then a.Account_Number else null end))                           as Accs_Daytime,
      count( (case when Dur_Primetime > 300 then a.Account_Number else null end))                         as Accs_Primetime,

      count( (case when Dur_Live > 300 then a.Account_Number else null end))                              as Accs_Live,
      count( (case when Dur_Live_Daytime > 300  then a.Account_Number else null end))                     as Accs_Live_Daytime,
      count( (case when Dur_Live_Primetime > 300  then a.Account_Number else null end))                   as Accs_Live_Primetime,
      count( (case when Dur_Playback > 300  then a.Account_Number else null end))                         as Accs_Playback,
      count( (case when Dur_Playback_Daytime > 300  then a.Account_Number else null end))                 as Accs_Playback_Daytime,
      count( (case when Dur_Playback_Daytime_RecDaytime > 300  then a.Account_Number else null end))      as Accs_Playback_Daytime_RecDaytime,
      count( (case when Dur_Playback_Daytime_RecPrimetime > 300  then a.Account_Number else null end))    as Accs_Playback_Daytime_RecPrimetime,
      count( (case when Dur_Playback_Primetime > 300  then a.Account_Number else null end))               as Accs_Playback_Primetime,
      count( (case when Dur_Playback_Primetime_RecDaytime > 300  then a.Account_Number else null end))    as Accs_Playback_Primetime_RecDaytime,
      count( (case when Dur_Playback_Primetime_RecPrimetime > 300  then a.Account_Number else null end))  as Accs_Playback_Primetime_RecPrimetime,

      count( (case when Cnt_VOD_Daytime > 1 then a.Account_Number else null end))                         as Accs_VOD_Daytime,
      count( (case when Cnt_VOD_Primetime > 1  then a.Account_Number else null end))                      as Accs_VOD_Primetime

  into OpForm_03_Account_Daily_Genres_Summary_Aggr
  from OpForm_01_Account_Attributes a,
       OpForm_03_Account_Daily_Genres_Summary b
 where a.Account_Number = b.Account_Number
   and a.Ent_DTV_Sub = 1
   and a.Movmt_DTV_Sub = 0
 group by Weekend_Flag, Genre;
commit;

grant select on OpForm_03_Account_Daily_Genres_Summary_Aggr to vespa_group_low_security;



  -- ##############################################################################################################
  -- ##### STEP 4 - Programme summary                                                                         #####
  -- ##############################################################################################################
if object_id('OpForm_03_Programme_Summary') is not null then drop table OpForm_03_Programme_Summary end if;
select
      case
        when Genre_Description = 'Unknown' or Sub_Genre_Description = 'Unknown' then 'Unknown'
          else Genre_Description || ' (' || Sub_Genre_Description || ')'
      end as Genre,
      Programme_Name,
      count(distinct Account_Number) as Account_Volume
  into OpForm_03_Programme_Summary
  from OpForm_02_Raw_Viewing_Events
 where Event_Type = 'Linear'
 group by Genre, Programme_Name;
commit;




  -- ##############################################################################################################
  -- ##### STEP 5 - Modelling dataset                                                                         #####
  -- ##############################################################################################################
if object_id('OpForm_04_Account_Daily_Summary_All') is not null then drop table OpForm_04_Account_Daily_Summary_All end if;
select
      a.Account_Number,
      Viewing_Day,
      HH_Composition,
      HH_Lifestage,
      Mirror_ABC1,
      Kids_Age_le4,
      Kids_Age_4to9,
      Kids_Age_10to15,
      Mosaic,
      H_Affluence,
      Region,

      Daytime_Consumption,
      Primetime_Consumption,

      Weekend_Flag,

      Dur_Daytime,
      Dur_Primetime,

      Dur_Live,
      Dur_Live_Daytime,
      Dur_Live_Primetime,

      Dur_Playback,
      Dur_Playback_Daytime,
      Dur_Playback_Daytime_RecDaytime,
      Dur_Playback_Daytime_RecPrimetime,
      Dur_Playback_Primetime,
      Dur_Playback_Primetime_RecDaytime,
      Dur_Playback_Primetime_RecPrimetime,

      Cnt_VOD_Daytime,
      Cnt_VOD_Primetime,

      cast(null as decimal(12, 8)) as Random_Value

  into OpForm_04_Account_Daily_Summary_All
  from OpForm_01_Account_Attributes a,
       OpForm_03_Account_Daily_Summary b
 where a.Account_Number = b.Account_Number
   and a.Ent_DTV_Sub = 1
   and a.Movmt_DTV_Sub = 0;
commit;

create        hg index idx01 on OpForm_04_Account_Daily_Summary_All(Account_Number);
create      date index idx02 on OpForm_04_Account_Daily_Summary_All(Viewing_Day);
create unique hg index idx03 on OpForm_04_Account_Daily_Summary_All(Account_Number, Viewing_Day);
create        hg index idx04 on OpForm_04_Account_Daily_Summary_All(Random_Value);

create variable @multiplier bigint;
set @multiplier = datepart(millisecond, now()) + 1;

update OpForm_04_Account_Daily_Summary_All
   set Random_Value = rand(number(*) * @multiplier);
commit;

grant select on OpForm_04_Account_Daily_Summary_All to vespa_group_low_security;



-- Profiles
begin
  declare @varSQL varchar(25000)

  if object_id('OpForm_05_Profiles') is not null drop table OpForm_05_Profiles
  create table OpForm_05_Profiles (
        Variable_Name                       varchar(100)      null     default null,
        Category                            varchar(100)      null     default null,
        Accounts_All_1                      bigint            null     default 0,
        Accounts_All_2                      bigint            null     default 0,
        Accounts_DayPrimetime               bigint            null     default 0,
        Accounts_Daytime                    bigint            null     default 0,
        Accounts_Primetime                  bigint            null     default 0
  )

  set @varSQL = '
                  insert into OpForm_05_Profiles
                  select
                        ''##^1^##'' as Variable_Name,
                        ##^1^## as Category,
                        count(case when Daytime_Consumption in (''High'', ''Medium'', ''Low'', ''No viewing'') and Primetime_Consumption in (''High'', ''Medium'', ''Low'', ''No viewing'') then Account_Number else null end) as Accounts_All_1,
                        count(case
                                when ( Daytime_Consumption in (''High'', ''Medium'') and Primetime_Consumption in (''High'', ''Medium'') ) or
                                     ( Daytime_Consumption in (''High'', ''Medium'') and Primetime_Consumption in (''Low'', ''No viewing'') ) or
                                     ( Daytime_Consumption in (''Low'', ''No viewing'') and Primetime_Consumption in (''High'', ''Medium'') ) then Account_Number
                                  else null
                              end) as Accounts_All_2,
                        count(case when Daytime_Consumption in (''High'', ''Medium'') and Primetime_Consumption in (''High'', ''Medium'') then Account_Number else null end) as Accounts_DayPrimetime,
                        count(case when Daytime_Consumption in (''High'', ''Medium'') and Primetime_Consumption in (''Low'', ''No viewing'') then Account_Number else null end) as Accounts_Daytime,
                        count(case when Daytime_Consumption in (''Low'', ''No viewing'') and Primetime_Consumption in (''High'', ''Medium'') then Account_Number else null end) as Accounts_Primetime
                    from OpForm_04_Account_Daily_Summary_All
                   group by ##^1^##
                   order by ##^1^##
                  commit
                '

  execute(replace(@varSQL, '##^1^##', 'HH_Composition'))
  execute(replace(@varSQL, '##^1^##', 'HH_Lifestage'))
  execute(replace(@varSQL, '##^1^##', 'Mirror_ABC1'))
  execute(replace(@varSQL, '##^1^##', 'Kids_Age_le4'))
  execute(replace(@varSQL, '##^1^##', 'Kids_Age_4to9'))
  execute(replace(@varSQL, '##^1^##', 'Kids_Age_10to15'))
  execute(replace(@varSQL, '##^1^##', 'Mosaic'))
  execute(replace(@varSQL, '##^1^##', 'H_Affluence'))
  execute(replace(@varSQL, '##^1^##', 'Region'))

end;

select * from OpForm_05_Profiles;



  -- ##############################################################################################################
  -- ##############################################################################################################










