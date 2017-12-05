/*###############################################################################
# Created on:   10/12/2013
# Created by:   Sebastian Bednaszynski
# Description:  Opinion formers - time series
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 10/12/2013  SBE   Initial version
#
###############################################################################*/



  -- ##############################################################################################################
  -- ##### STEP 1 - Get summary by account & day                                                              #####
  -- ##############################################################################################################
if object_id('OpForm_05_Account_Time_Series') is not null then drop table OpForm_05_Account_Time_Series end if;
create table OpForm_05_Account_Time_Series (
      Account_Number                      varchar(20)                default null,
      Viewing_Day                         date                       default null,
      Viewing_Interval_Start              time              null     default null,
      Viewing_Interval_End                time              null     default null,
      Weekend_Flag                        varchar(3)        null     default 'No',
      Channel_Name                        varchar(50)       null     default null,
      Genre_Description                   varchar(50)       null     default null,
      Sub_Genre_Description               varchar(50)       null     default null,

      Viewing_Live_Flag                   tinyint           null     default 0,
      Viewing_Playback_Flag               tinyint           null     default 0,
      Viewing_Flag                        tinyint           null     default 0
);

create        hg index idx01 on OpForm_05_Account_Time_Series(Account_Number);
create      date index idx02 on OpForm_05_Account_Time_Series(Viewing_Day);
create      time index idx03 on OpForm_05_Account_Time_Series(Viewing_Interval_Start);
create      time index idx04 on OpForm_05_Account_Time_Series(Viewing_Interval_End);
grant select on OpForm_05_Account_Time_Series to vespa_group_low_security;


begin
    declare @varIntervalStart     time
    declare @varIntervalEnd       time
    declare @varLastIntervalEnd   time

    set @varIntervalStart       = '07:30:00'
    set @varLastIntervalEnd     = '23:00:00'

    while @varIntervalStart < @varLastIntervalEnd
      begin

            set @varIntervalEnd = dateadd(minute, 10, @varIntervalStart)

            insert into OpForm_05_Account_Time_Series
              select
                    Account_Number,
                    Viewing_Day,
                    @varIntervalStart,
                    @varIntervalEnd,
                    Weekend_Flag,
                    Channel_Name,
                    Genre_Description,
                    Sub_Genre_Description,

                    max(case
                          when Playback_Flag in (0, 2) and                                                                -- Live & live pause
                               cast(Instance_Start_Date_Time as time) <= dateadd(minute, -3, @varIntervalEnd) and
                               cast(Instance_End_Date_Time_Capped as time) >= dateadd(minute, 3, @varIntervalStart) and
                               datediff(second, Instance_Start_Date_Time,Instance_End_Date_Time_Capped) >= 180 then 1
                            else 0
                        end) as Viewing_Live_Flag,

                    max(case
                          when Playback_Flag = 1 and                                                                      -- Timeshifted viewing only
                               cast(Instance_Start_Date_Time as time) <= dateadd(minute, -3, @varIntervalEnd) and
                               cast(Instance_End_Date_Time_Capped as time) >= dateadd(minute, 3, @varIntervalStart) and
                               datediff(second, Instance_Start_Date_Time,Instance_End_Date_Time_Capped) >= 180 then 1
                            else 0
                        end) as Viewing_Playback_Flag,

                    max(case
                          when cast(Instance_Start_Date_Time as time) <= dateadd(minute, -3, @varIntervalEnd) and
                               cast(Instance_End_Date_Time_Capped as time) >= dateadd(minute, 3, @varIntervalStart) and
                               datediff(second, Instance_Start_Date_Time,Instance_End_Date_Time_Capped) >= 180 then 1
                            else 0
                        end) as Viewing_Flag
                from OpForm_02_Raw_Viewing_Events
               where Event_Type = 'Linear'
                 and Viewing_Day between '2013-09-16' and '2013-09-22'
               group by Account_Number, Viewing_Day, Weekend_Flag, Channel_Name, Genre_Description, Sub_Genre_Description
              having Viewing_Flag > 0
            commit

            execute logger_add_event -1, 3, 'Interval processed: ' || @varIntervalStart || ' - ' || @varIntervalEnd, @@rowcount

            set @varIntervalStart = @varIntervalEnd

      end

end;



  -- ##############################################################################################################
  -- ##### STEP 2 - Aggregate for News                                                                        #####
  -- ##############################################################################################################
if object_id('OpForm_06_Account_Time_Series__News_Top_Chn') is not null then drop table OpForm_06_Account_Time_Series__News_Top_Chn end if;
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
      Day_Of_Week,
      Viewing_Interval_Start,
      Viewing_Interval_End,
      Weekend_Flag,
      Channel_Name2 as Channel_Name,
      sum(x_Viewing_Live_Flag) as Audience_Live,
      sum(x_Viewing_Playback_Flag) as Audience_Playback,
      sum(x_Viewing_Flag) as Audience_All,
      cast(null as decimal(12, 8)) as Random_Value

  into OpForm_06_Account_Time_Series__News_Top_Chn
  from (select
              a.Account_Number,
              a.HH_Composition,
              a.HH_Lifestage,
              a.Mirror_ABC1,
              a.Kids_Age_le4,
              a.Kids_Age_4to9,
              a.Kids_Age_10to15,
              a.Mosaic,
              a.H_Affluence,
              a.Region,
              b.Viewing_Day,
              case
                when b.Viewing_Day = '2013-09-21' then 'Saturday'
                when b.Viewing_Day = '2013-09-22' then 'Sunday'
                  else 'Weekday'
              end as Day_Of_Week,
              b.Viewing_Interval_Start,
              b.Viewing_Interval_End,
              case
                when Channel_Name = 'ITV1 +1' then 'ITV1'
                when Channel_Name = 'Channel 4+1' then 'Channel 4'
                when Channel_Name = 'Disney Channel+1' then 'Disney Channel'
                when Channel_Name in ('BBC 1' ,'ITV1' ,'Sky Sports 1' ,'Channel 4' ,'BBC 2' ,'Channel 5' ,'Sky Sports News' ,'Sky News' ,'ITV1 HD','Sky Sports 2',
                                      'Sky Sports F1' ,'Sky1' ,'Disney Junior' ,'Comedy Central' ,'Disney Channel' ,'Gold' ,'Sky Living' ,'ITV 2' ,'Sky Sports 3',
                                      'ITV2' ,'ITV 3' ,'Disney Junior+' ,'E4' ,'ITV1 +1' ,'Alibi' ,'Cartoon Network' ,'Video on demand' ,'Disney Channel+1' ,'Channel 4+1',
                                      'ITV3' ,'Watch' ,'Discovery' ,'Boomerang' ,'Universal' ,'Sky Atlantic' ,'Dave' ,'E!' ,'TCM' ,'FX' ,'ITV 4', 'FOX News', 'CNN') then Channel_Name
                  else 'Other'
              end as Channel_Name2,
              b.Weekend_Flag,
              max(b.Viewing_Live_Flag) as x_Viewing_Live_Flag,
              max(b.Viewing_Playback_Flag) as x_Viewing_Playback_Flag,
              max(b.Viewing_Flag) as x_Viewing_Flag
          from OpForm_01_Account_Attributes a,
               OpForm_05_Account_Time_Series b
         where a.Account_Number = b.Account_Number
           and a.Ent_DTV_Sub = 1
           and a.Movmt_DTV_Sub = 0
           and b.Genre_Description = 'News & Documentaries'
           and b.Sub_Genre_Description = 'News'
           and b.Viewing_Flag > 0
         group by a.Account_Number, a.HH_Composition, a.HH_Lifestage, a.Mirror_ABC1, a.Kids_Age_le4, a.Kids_Age_4to9, a.Kids_Age_10to15, a.Mosaic, a.H_Affluence,
                  a.Region, b.Viewing_Day, b.Viewing_Interval_Start, b.Viewing_Interval_End, Channel_Name2, b.Weekend_Flag) det
 group by HH_Composition, HH_Lifestage, Mirror_ABC1, Kids_Age_le4, Kids_Age_4to9, Kids_Age_10to15, Mosaic, H_Affluence,
          Region, Day_Of_Week, Viewing_Interval_Start, Viewing_Interval_End, Weekend_Flag, Channel_Name;
commit;
grant select on OpForm_06_Account_Time_Series__News_Top_Chn to vespa_group_low_security;

create variable @multiplier bigint;
set @multiplier = datepart(millisecond, now()) + 1;

update OpForm_06_Account_Time_Series__News_Top_Chn
   set Random_Value = rand(number(*) * @multiplier);
commit;



  -- ##############################################################################################################
  -- ##### STEP 3 - Generic aggregate                                                                         #####
  -- ##############################################################################################################
if object_id('OpForm_06_Account_Time_Series__Tops') is not null then drop table OpForm_06_Account_Time_Series__Tops end if;
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
      Day_Of_Week,
      Viewing_Interval_Start,
      Viewing_Interval_End,
      Weekend_Flag,
      Channel_Name2 as Channel_Name,
      Genre_Description2 as Genre_Description,
      Sub_Genre_Description2 as Sub_Genre_Description,
      sum(x_Viewing_Live_Flag) as Audience_Live,
      sum(x_Viewing_Playback_Flag) as Audience_Playback,
      sum(x_Viewing_Flag) as Audience_All,
      cast(null as decimal(12, 8)) as Random_Value

  into OpForm_06_Account_Time_Series__Tops
  from (select
              a.Account_Number,
              a.HH_Composition,
              a.HH_Lifestage,
              a.Mirror_ABC1,
              a.Kids_Age_le4,
              a.Kids_Age_4to9,
              a.Kids_Age_10to15,
              a.Mosaic,
              a.H_Affluence,
              a.Region,
              b.Viewing_Day,
              case
                when b.Viewing_Day = '2013-09-21' then 'Saturday'
                when b.Viewing_Day = '2013-09-22' then 'Sunday'
                  else 'Weekday'
              end as Day_Of_Week,
              b.Viewing_Interval_Start,
              b.Viewing_Interval_End,
              case
                when Channel_Name = 'ITV1 +1' then 'ITV1'
                when Channel_Name = 'Channel 4+1' then 'Channel 4'
                when Channel_Name = 'Disney Channel+1' then 'Disney Channel'
                when Channel_Name in ('BBC 1' ,'ITV1' ,'Sky Sports 1' ,'Channel 4' ,'BBC 2' ,'Channel 5' ,'Sky Sports News' ,'Sky News' ,'ITV1 HD','Sky Sports 2',
                                      'Sky Sports F1' ,'Sky1' ,'Disney Junior' ,'Comedy Central' ,'Disney Channel' ,'Gold' ,'Sky Living' ,'ITV 2' ,'Sky Sports 3',
                                      'ITV2' ,'ITV 3' ,'Disney Junior+' ,'E4' ,'ITV1 +1' ,'Alibi' ,'Cartoon Network' ,'Video on demand' ,'Disney Channel+1' ,'Channel 4+1',
                                      'ITV3' ,'Watch' ,'Discovery' ,'Boomerang' ,'Universal' ,'Sky Atlantic' ,'Dave' ,'E!' ,'TCM' ,'FX' ,'ITV 4', 'FOX News', 'CNN') then Channel_Name
                  else 'Other'
              end as Channel_Name2,
              b.Weekend_Flag,

              case
                when b.Genre_Description is null then 'Unknown'
                when b.Genre_Description = 'Undefined' then 'Unknown'
                  else b.Genre_Description
              end as Genre_Description2,
              case
                when b.Sub_Genre_Description is null then 'Unknown'
                when b.Sub_Genre_Description = 'Undefined' then 'Unknown'
                when b.Sub_Genre_Description = 'Channel 4+1' then 'Channel 4'
                when b.Sub_Genre_Description = 'Disney Channel+1' then 'Disney Channel'
                when b.Sub_Genre_Description in ('Drama', 'News', 'Comedy', 'Factual', 'Football', 'Game Shows', 'Features', 'Soaps', 'Cooking', 'Detective', 'Rock & Pop',
                                                 'Chat Show', 'Cartoons', 'Action', 'Shopping', 'Under 5', 'Nature', 'Motor Sport', 'Antiques', 'Thriller', 'Sci-Fi', 'Lifestyle',
                                                 'Animation', 'Rugby', 'Motors', 'Golf', 'Adventure', 'Family', 'Historical', 'Home', 'Showbiz', 'Wrestling', 'Horror', 'Racing',
                                                 'Cricket', 'Science', 'American Football', 'Western', 'Politics', 'Darts') then b.Sub_Genre_Description
                  else 'Other'
              end as Sub_Genre_Description2,

              max(b.Viewing_Live_Flag) as x_Viewing_Live_Flag,
              max(b.Viewing_Playback_Flag) as x_Viewing_Playback_Flag,
              max(b.Viewing_Flag) as x_Viewing_Flag
          from OpForm_01_Account_Attributes a,
               OpForm_05_Account_Time_Series b
         where a.Account_Number = b.Account_Number
           and a.Ent_DTV_Sub = 1
           and a.Movmt_DTV_Sub = 0
           and b.Viewing_Flag > 0
         group by a.Account_Number, a.HH_Composition, a.HH_Lifestage, a.Mirror_ABC1, a.Kids_Age_le4, a.Kids_Age_4to9, a.Kids_Age_10to15, a.Mosaic, a.H_Affluence,
                  a.Region, b.Viewing_Day, b.Viewing_Interval_Start, b.Viewing_Interval_End, Channel_Name2, b.Weekend_Flag, Genre_Description2, Sub_Genre_Description2) det

 group by HH_Composition, HH_Lifestage, Mirror_ABC1, Kids_Age_le4, Kids_Age_4to9, Kids_Age_10to15, Mosaic, H_Affluence,
          Region, Day_Of_Week, Viewing_Interval_Start, Viewing_Interval_End, Weekend_Flag, Channel_Name, Genre_Description,
          Sub_Genre_Description;
commit;
grant select on OpForm_06_Account_Time_Series__Tops to vespa_group_low_security;

create variable @multiplier bigint;
set @multiplier = datepart(millisecond, now()) + 1;

update OpForm_06_Account_Time_Series__Tops
   set Random_Value = rand(number(*) * @multiplier);
commit;



  -- ##############################################################################################################
  -- ##############################################################################################################










