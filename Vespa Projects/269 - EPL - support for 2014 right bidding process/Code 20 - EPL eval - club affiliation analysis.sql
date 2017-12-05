/*###############################################################################
# Created on:   18/04/2014
# Created by:   Sebastian Bednaszynski(SBE)
# Description:  EPL rights project - club affiliation analysis
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 18/04/2014  SBE   Initial version
#
###############################################################################*/


  -- ##############################################################################################################
  -- ##### Team games by pack                                                                                 #####
  -- ##############################################################################################################
select
      Period,
      Channel,
      EPL_Pack,
      Club,
      count(*) as Cnt
  from (select distinct
              case
                when Match_Date <= '2013-07-31' then '1) Feb ''13 - Jul ''13'
                  else '2) Aug ''13 - Feb ''14'
              end as Period,
              Channel,
              EPL_Pack,
              case
                when substr(Teams, 1, charindex(' v ', Teams ) - 1 ) = 'Manchester City' then 'Man City'
                  else substr(Teams, 1, charindex(' v ', Teams ) - 1 )
              end as Club
          from bednaszs.EPL_01_EPG
         where Live_Game_Flag = 1

        union all

        select distinct
              case
                when Match_Date <= '2013-07-31' then '1) Feb ''13 - Jul ''13'
                  else '2) Aug ''13 - Feb ''14'
              end as Period,
              Channel,
              EPL_Pack,
              case
                when substr(Teams, charindex(' v ', teams ) + 3 ) = 'Manchester City' then 'Man City'
                  else substr(Teams, charindex(' v ', teams ) + 3 )
              end as Club
          from bednaszs.EPL_01_EPG
         where Live_Game_Flag = 1) det
 group by
      Period,
      Channel,
      EPL_Pack,
      Club;


  -- ##############################################################################################################
  -- ##### Account engagement with each club                                                                  #####
  -- ##############################################################################################################
if object_id('EPL_20_ClubAff_All_Content') is not null then drop table EPL_20_ClubAff_All_Content end if;
select
      Account_Number,
      Period,
      Broadcast_Date,
      Programme,
      max(Content_Available) as Content_Available,
      max(Content_Watched) as Content_Watched
  into EPL_20_ClubAff_All_Content
  from EPL_03_SOCs
 where Live_Game_Flag = 1
   and Period = 1
   --and Account_Number = '620017172771'
 group by Account_Number, Period, Broadcast_Date, Programme;
commit;


  -- Get watched/not watched for each individual team
if object_id('EPL_21_ClubAff_Teams') is not null then drop table EPL_21_ClubAff_Teams end if;

  -- Add home team records
select
      Account_Number,
      Period,
      Broadcast_Date,
      Programme,
      cast(null as varchar(50)) as Teams,
      'Home' as Match_Type,
      cast(null as varchar(30)) as Club,
      Content_Available,
      Content_Watched
  into EPL_21_ClubAff_Teams
  from EPL_20_ClubAff_All_Content;
commit;

  -- Add away team records
insert into EPL_21_ClubAff_Teams
  select
        Account_Number,
        Period,
        Broadcast_Date,
        Programme,
        cast(null as varchar(50)) as Teams,
        'Away' as Match_Type,
        cast(null as varchar(30)) as Club,
        Content_Available,
        Content_Watched
    from EPL_20_ClubAff_All_Content;
commit;


update EPL_21_ClubAff_Teams
   set Programme = replace( replace(Programme, 'Man utd', 'Man Utd') , 'Manchester City', 'Man City'),
       Teams = replace( replace( replace(replace(Programme, 'Man utd', 'Man Utd'), 'Manchester City', 'Man City') , ')', '') , 'Live game (', '');
commit;

update EPL_21_ClubAff_Teams
   set Club = trim(case
                     when Match_Type = 'Home' then substr(Teams, 1, charindex(' v ', Teams ) - 1 )
                       else substr(Teams, charindex(' v ', teams ) + 3 )
                   end);
commit;


  -- Calculate SOCs
if object_id('EPL_22_ClubAff_Total_SOCs') is not null then drop table EPL_22_ClubAff_Total_SOCs end if;
select
      Account_Number,
      Period,
      sum(Content_Available) as Games_Available,
      sum(Content_Watched) as Games_Watched,
      cast(1.0 * sum(Content_Watched) / sum(Content_Available) as decimal(15, 6)) as Calculated_SOC
  into EPL_22_ClubAff_Total_SOCs
  from EPL_20_ClubAff_All_Content
 group by Account_Number, Period;
commit;


  -- Club SOCs
if object_id('EPL_23_ClubAff_Club_SOCs') is not null then drop table EPL_23_ClubAff_Club_SOCs end if;
select
      Account_Number,
      Period,
      Club,
      sum(Content_Available) as Games_Available,
      sum(Content_Watched) as Games_Watched,
      cast(1.0 * sum(Content_Watched) / sum(Content_Available) as decimal(15, 6)) as Calculated_SOC,
      rank () over (partition by Account_Number, Period order by Calculated_SOC desc, Club) as Club_Rank_SOC,
      rank () over (partition by Account_Number, Period order by Club, Calculated_SOC desc) as Club_Rank_Alph
  into EPL_23_ClubAff_Club_SOCs
  from EPL_21_ClubAff_Teams
 group by Account_Number, Period, Club;
commit;



  -- ##############################################################################################################
  -- ##### Summarize                                                                                          #####
  -- ##############################################################################################################
if object_id('EPL_30_ClubAff_Summ_Club_Nums') is not null then drop table EPL_30_ClubAff_Summ_Club_Nums end if;
create table EPL_30_ClubAff_Summ_Club_Nums (
    Pk_Identifier                           bigint            identity,
    Period                                  tinyint           null      default 0,

    EPL_SOC                                 varchar(20)       null      default '???',
    Club_SOC_Threshold                      varchar(10)       null      default '???',
    Clubs_Watched                           tinyint           null      default 0,
    Accounts_Unscaled                       bigint            null      default 0,
    Accounts_Scaled                         decimal(15, 6)    null      default 0,

    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name()
  );


if object_id('EPL_20_ClubAff_Summary') is not null then drop procedure EPL_20_ClubAff_Summary end if;
create procedure EPL_20_ClubAff_Summary
      @parSOCThreshold          varchar(5)
as
begin

      declare @varSQL                         varchar(25000)

      execute logger_add_event 0, 0, '##### Processing threshold: ' || @parSOCThreshold || ' #####', null

      set @varSQL = '
                      insert into EPL_30_ClubAff_Summ_Club_Nums
                            (Period, EPL_SOC, Club_SOC_Threshold, Clubs_Watched, Accounts_Unscaled, Accounts_Scaled)
                        select
                              a.Period,
                              case
                                when EPL_SOC <= 1 then ''0'' || EPL_SOC * 5 || '' - '' || 1.0 * (EPL_SOC + 1) * 5 - 0.1
                                when EPL_SOC >= 19 then ''95 - 100''
                                  else EPL_SOC * 5 || '' - '' || 1.0 * (EPL_SOC + 1) * 5 - 0.1
                              end xEPL_SOC,
                              ''>=' || @parSOCThreshold || ''' as Club_SOC_Threshold,
                              Clubs_Watched,
                              count(*) as Accounts_Unscaled,
                              sum(case
                                    when b.Account_Number is null then 0
                                      else b.Scaling_Weight
                                  end) as Accounts_Scaled
                          from (select
                                      a.Account_Number,
                                      a.Period,
                                      cast( (a.Calculated_SOC * 100) / 5 as tinyint) as EPL_SOC,
                                      count(distinct case
                                                       when b.Games_Watched > 0 then Club
                                                         else null
                                                     end) as Clubs_Watched
                                  from EPL_22_ClubAff_Total_SOCs a,
                                       EPL_23_ClubAff_Club_SOCs b
                                 where a.Account_Number = b.Account_Number
                                   and a.Period = b.Period
                                   and b.Calculated_SOC >= ' || @parSOCThreshold || '
                                 group by a.Account_Number, a.Period, EPL_SOC) a

                                left join EPL_05_Scaling_Weights b
                                  on a.Account_Number = b.Account_Number
                                 and a.Period = b.Period
                         group by
                              a.Period,
                              xEPL_SOC,
                              Club_SOC_Threshold,
                              Clubs_Watched;
                        commit;

                      execute logger_add_event 0, 0, ''Threshold "' || @parSOCThreshold || '" processed'', @@rowcount
                    '
      execute(@varSQL)

end;

truncate table EPL_30_ClubAff_Summ_Club_Nums;
execute EPL_20_ClubAff_Summary '0.00';
execute EPL_20_ClubAff_Summary '0.10';
execute EPL_20_ClubAff_Summary '0.20';
execute EPL_20_ClubAff_Summary '0.30';
execute EPL_20_ClubAff_Summary '0.40';
execute EPL_20_ClubAff_Summary '0.50';
execute EPL_20_ClubAff_Summary '0.60';
execute EPL_20_ClubAff_Summary '0.70';
execute EPL_20_ClubAff_Summary '0.80';
execute EPL_20_ClubAff_Summary '0.90';

select
      Period,
      EPL_SOC,
      Club_SOC_Threshold,
      Clubs_Watched,
      Accounts_Volume
  from EPL_30_ClubAff_Summ_Club_Nums;



  -- ##############################################################################################################
  -- ##### Breakdown by club                                                                                  #####
  -- ##############################################################################################################
if object_id('EPL_31_ClubAff_Summ_Club_Details') is not null then drop table EPL_31_ClubAff_Summ_Club_Details end if;
create table EPL_31_ClubAff_Summ_Club_Details (
    Pk_Identifier                           bigint            identity,
    Period                                  tinyint           null      default 0,

    Min_Club_Games_Available                tinyint           null      default 0,
    Clubs_Watched                           tinyint           null      default 0,
    EPL_SOC                                 varchar(20)       null      default '???',
    Club_SOC_Threshold                      varchar(10)       null      default '???',
    Teams                                   varchar(100)      null      default '???',
    Team_1                                  varchar(30)       null      default '???',
    Team_2                                  varchar(30)       null      default '???',
    Team_3                                  varchar(30)       null      default '???',
    Team_4                                  varchar(30)       null      default '???',
    Accounts_Unscaled                       bigint            null      default 0,
    Accounts_Scaled                         decimal(15, 6)    null      default 0,

    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name()
  );


if object_id('EPL_20_ClubAff_Club_Details') is not null then drop procedure EPL_20_ClubAff_Club_Details end if;
create procedure EPL_20_ClubAff_Club_Details
      @parEPLSOCMinThreshold          varchar(5),
      @parEPLSOCMaxThreshold          varchar(5),
      @parMaxNumberOfClubsFollowed    varchar(5),
      @parMinGamesAvailableThreshold  varchar(5),
      @parSOCThreshold                varchar(5)
as
begin

      declare @varSQL                         varchar(25000)

      execute logger_add_event 0, 0, '##### Processing threshold: ' || @parSOCThreshold || ' #####', null

      set @varSQL = '
                      insert into EPL_31_ClubAff_Summ_Club_Details
                            (Period, Min_Club_Games_Available, Clubs_Watched, EPL_SOC, Club_SOC_Threshold,
                             Teams, Team_1, Team_2, Team_3, Team_4, Accounts_Unscaled, Accounts_Scaled)
                        select
                              socs.Period,
                              ' || @parMinGamesAvailableThreshold || ' as Min_Club_Games_Available,
                              socs.Clubs_Watched,
                              ''' || @parEPLSOCMinThreshold || ' - ' || @parEPLSOCMaxThreshold || ''' as xEPL_SOC,
                              ''>=' || @parSOCThreshold || ''' as Club_SOC_Threshold,
                              case when socs.Team_1 <> ''-'' then socs.Team_1 else '''' end ||
                              case when socs.Team_2 <> ''-'' then '' / '' || socs.Team_2 else '''' end ||
                              case when socs.Team_3 <> ''-'' then '' / '' || socs.Team_3 else '''' end ||
                              case when socs.Team_4 <> ''-'' then '' / '' || socs.Team_4 else '''' end as Teams,
                              socs.Team_1,
                              socs.Team_2,
                              socs.Team_3,
                              socs.Team_4,
                              count(distinct socs.Account_Number) as Accounts_Unscaled,
                              sum(case
                                    when scal.Account_Number is null then 0
                                      else scal.Scaling_Weight
                                  end) as Accounts_Scaled

                          from (select
                                      a.Account_Number,
                                      a.Period,
                                      a.Clubs_Watched,
                                      max(case when b.Club_Rank_Alph = 1 then b.Club else ''-'' end) as Team_1,
                                      max(case when b.Club_Rank_Alph = 2 then b.Club else ''-'' end) as Team_2,
                                      max(case when b.Club_Rank_Alph = 3 then b.Club else ''-'' end) as Team_3,
                                      max(case when b.Club_Rank_Alph = 4 then b.Club else ''-'' end) as Team_4
                                  from (select
                                              a.Account_Number,
                                              a.Period,
                                              cast( (a.Calculated_SOC * 100) / 5 as tinyint) as EPL_SOC,
                                              count(distinct case
                                                               when b.Games_Watched > 0 then Club
                                                                 else null
                                                             end) as Clubs_Watched
                                          from EPL_22_ClubAff_Total_SOCs a,
                                               EPL_23_ClubAff_Club_SOCs b
                                         where a.Account_Number = b.Account_Number
                                           and a.Period = b.Period
                                           and b.Calculated_SOC >= ' || @parSOCThreshold || '
                                           and b.Games_Available >= ' || @parMinGamesAvailableThreshold || '        -- Minimal games available threshold
                                           and EPL_SOC * 5 >= ' || @parEPLSOCMinThreshold || '                      -- EPL SOC retriction
                                           and 1.0 * (EPL_SOC + 1) * 5 - 0.1 <= ' || @parEPLSOCMaxThreshold || '    -- EPL SOC retriction
                                         group by a.Account_Number, a.Period, EPL_SOC
                                        having Clubs_Watched <= ' || @parMaxNumberOfClubsFollowed || '              -- Number of clubs watched
                                        ) a,
                                       (select
                                              Account_Number,
                                              Period,
                                              Club,
                                              rank () over (partition by Account_Number, Period order by Club, Calculated_SOC desc) as Club_Rank_Alph
                                          from EPL_23_ClubAff_Club_SOCs
                                         where Calculated_SOC >= ' || @parSOCThreshold || '
                                           and Games_Available >= ' || @parMinGamesAvailableThreshold || ')b       -- Minimal games available threshold
                                 where a.Account_Number = b.Account_Number
                                   and a.Period = b.Period
                                 group by a.Account_Number, a.Period, a.Clubs_Watched) socs

                                left join EPL_05_Scaling_Weights scal
                                  on socs.Account_Number = scal.Account_Number
                                 and socs.Period = scal.Period

                         group by socs.Period, Min_Club_Games_Available, socs.Clubs_Watched, xEPL_SOC, Club_SOC_Threshold, Teams,
                                  socs.Team_1, socs.Team_2, socs.Team_3, socs.Team_4
                        commit

                      execute logger_add_event 0, 0, ''Threshold "' || @parSOCThreshold || '" processed'', @@rowcount
                    '
      execute(@varSQL)

end;

truncate table EPL_31_ClubAff_Summ_Club_Details;
execute EPL_20_ClubAff_Club_Details '00', '19.9', '4', '1', '0.50';   --  [@parEPLSOCMinThreshold]
                                                                      --  [@parEPLSOCMaxThreshold]
                                                                      --  [@parMaxNumberOfClubsFollowed]
                                                                      --  [@parMinGamesAvailableThreshold]
                                                                      --  [@parSOCThreshold]
execute EPL_20_ClubAff_Club_Details '00', '19.9', '4', '1', '0.60';
execute EPL_20_ClubAff_Club_Details '00', '19.9', '4', '1', '0.70';
execute EPL_20_ClubAff_Club_Details '00', '19.9', '4', '1', '0.80';
execute EPL_20_ClubAff_Club_Details '00', '19.9', '4', '1', '0.90';

execute EPL_20_ClubAff_Club_Details '00', '19.9', '4', '2', '0.50';
execute EPL_20_ClubAff_Club_Details '00', '19.9', '4', '2', '0.60';
execute EPL_20_ClubAff_Club_Details '00', '19.9', '4', '2', '0.70';
execute EPL_20_ClubAff_Club_Details '00', '19.9', '4', '2', '0.80';
execute EPL_20_ClubAff_Club_Details '00', '19.9', '4', '2', '0.90';

execute EPL_20_ClubAff_Club_Details '00', '19.9', '4', '3', '0.50';
execute EPL_20_ClubAff_Club_Details '00', '19.9', '4', '3', '0.60';
execute EPL_20_ClubAff_Club_Details '00', '19.9', '4', '3', '0.70';
execute EPL_20_ClubAff_Club_Details '00', '19.9', '4', '3', '0.80';
execute EPL_20_ClubAff_Club_Details '00', '19.9', '4', '3', '0.90';

execute EPL_20_ClubAff_Club_Details '00', '19.9', '4', '4', '0.50';
execute EPL_20_ClubAff_Club_Details '00', '19.9', '4', '4', '0.60';
execute EPL_20_ClubAff_Club_Details '00', '19.9', '4', '4', '0.70';
execute EPL_20_ClubAff_Club_Details '00', '19.9', '4', '4', '0.80';
execute EPL_20_ClubAff_Club_Details '00', '19.9', '4', '4', '0.90';

execute EPL_20_ClubAff_Club_Details '00', '19.9', '4', '5', '0.50';
execute EPL_20_ClubAff_Club_Details '00', '19.9', '4', '5', '0.60';
execute EPL_20_ClubAff_Club_Details '00', '19.9', '4', '5', '0.70';
execute EPL_20_ClubAff_Club_Details '00', '19.9', '4', '5', '0.80';
execute EPL_20_ClubAff_Club_Details '00', '19.9', '4', '5', '0.90';

execute EPL_20_ClubAff_Club_Details '00', '19.9', '4', '6', '0.50';
execute EPL_20_ClubAff_Club_Details '00', '19.9', '4', '6', '0.60';
execute EPL_20_ClubAff_Club_Details '00', '19.9', '4', '6', '0.70';
execute EPL_20_ClubAff_Club_Details '00', '19.9', '4', '6', '0.80';
execute EPL_20_ClubAff_Club_Details '00', '19.9', '4', '6', '0.90';

select
      Period,
      Min_Club_Games_Available,
      Clubs_Watched,
      EPL_SOC,
      Club_SOC_Threshold,
      Teams,
      Team_1,
      Team_2,
      Team_3,
      Team_4,
      Accounts_Unscaled,
      Accounts_Scaled
 from EPL_31_ClubAff_Summ_Club_Details;



  -- ##############################################################################################################
  -- ##############################################################################################################

















