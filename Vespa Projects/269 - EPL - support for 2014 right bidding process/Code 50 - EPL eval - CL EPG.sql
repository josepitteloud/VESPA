/*###############################################################################
# Created on:   26/05/2014
# Created by:   Sebastian Bednaszynski(SBE)
# Description:  EPL rights project - EPG lookup (Champions League games)
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
  -- ##### Get an EPG lookup                                                                                  #####
  -- ##############################################################################################################
  -- Clean-up, table created by someone else (James McKane)
if object_id('EPL_50_CL_EPG') is not null then drop table EPL_50_CL_EPG end if;
create table EPL_50_CL_EPG (
    Pk_Identifier                           bigint            identity,
    Dk_Programme_Instance_Dim               bigint            not null  default 0,
    Service_Key                             smallint          null      default null,
    Broadcast_Date                          date              null      default null,
    Broadcast_Start_Time_UTC                datetime          null      default null,
    Broadcast_End_Time_UTC                  datetime          null      default null,
    Broadcast_Duration                      bigint            null      default 0,
    Channel                                 varchar(50)       null      default null,
    Programme                               varchar(100)      null      default null,
    Programme_Instance_Name                 varchar(80)       null      default null,

    Teams                                   varchar(50)       null      default null,
    Game                                    tinyint           null      default null,
    Season                                  varchar(10)       null      default null,
    Match_Date                              date              null      default null,
    Kick_Off_Time                           time              null      default null,
    Day_Of_Week                             varchar(10)       null      default null,
    Live_Game_Flag                          bit               null      default 0,
    EPL_Pack                                varchar(30)       null      default null,

    Data_Source                             varchar(50)       null      default null,
    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name()
);
create unique hg   index idx01 on EPL_50_CL_EPG(Dk_Programme_Instance_Dim);
create        lf   index idx02 on EPL_50_CL_EPG(Service_Key);
create        hg   index idx03 on EPL_50_CL_EPG(Programme_Instance_Name);
create        date index idx04 on EPL_50_CL_EPG(Broadcast_Date);
create        dttm index idx05 on EPL_50_CL_EPG(Broadcast_Start_Time_UTC);
create        lf   index idx06 on EPL_50_CL_EPG(Channel);
grant select on EPL_50_CL_EPG to vespa_group_low_security;


/*
v269_EPL_Live_Matches2              for live matches
v269_EPL_Non_Live                   for non live shows

v269_EPL_Non_Live_Feb_Jan           for non live
and v269_EPL_Live_Matches_Feb_Jan2  for live
*/


insert into EPL_50_CL_EPG
      (Dk_Programme_Instance_Dim, Service_Key, Broadcast_Start_Time_UTC, Broadcast_End_Time_UTC, Broadcast_Duration,
       Channel, Programme, Teams, Game, Season, Match_Date, Kick_Off_Time, Day_Of_Week, Live_Game_Flag, EPL_Pack, Data_Source)
select
      Dk_Programme_Instance_Dim,
      Service_Key,
      Broadcast_Start_Date_Time_UTC,
      Broadcast_End_Date_Time_UTC,
      datediff(second, Broadcast_Start_Date_Time_UTC, Broadcast_End_Date_Time_UTC),
      Channel_Name,
      Programme_Instance_Name,
      replace(Teams, ' vs ', ' v '),
      Game,
      '13/14',
      Match_Date,
      Kick_Off,
      Day,
      case                                                                                                            -- Live_Game_Flag
        when Redbutton = 'XXX' then 0                                                                                 -- Timeshifted games played much earlier in
                                                                                                                      -- time zone +2
        when Service_Key in (6012, 6065, 6125, 6126, 6127, 6128, 6145, 6155, 6274, 6325, 6355, 6365) then 0           -- ITV +1 channels
          else 1
      end,
      case                                                                                                            -- EPL_Pack
        when trim(Redbutton) = '' then 'Linear broadcast'
          else 'Red button'
      end,
      'lusholi.v269_CL_Live_Matches2'
  from lusholi.v269_CL_Live_Matches2
 where Game is not null;                -- Excluded games (3D repeats + 1 cancelled game)
commit;


  -- Manual hacks & updates
update EPL_50_CL_EPG base
   set base.Channel = trim(replace( replace(det.Vespa_Name, '+ 1', ''), '+1', '' ))
  from VESPA_Analysts.Channel_Map_Prod_Service_Key_Attributes det
 where base.Service_Key = det.Service_Key
   and date(det.Effective_To) = '2999-12-31';
commit;

update EPL_50_CL_EPG base
   set base.Broadcast_Date = date(Broadcast_Start_Time_UTC),
       base.Channel       = case
                              when base.Channel = 'Interactive applications' then 'Sky Sports interactive'
                              when base.Channel in ('ITV1 HD', 'ITV1 UTV') then 'ITV1'
                              when base.Channel in ('ITV 4') then 'ITV4'
                              when base.Service_Key in (4052, 4054) then '3D channel'
                              when base.Service_Key in (4005, 1341, 1361, 1311) then 'Sky Sports 1'
                              when base.Service_Key in (1362) then 'Sky Sports 2'
                              when base.Service_Key in (1320, 1405, 1702, 2051, 4085) then 'Pub channel/Other'
                                else base.Channel
                            end,
       base.Programme     = 'CL game (' || Teams || ')';
commit;

update EPL_50_CL_EPG base
   set base.Programme_Instance_Name =
                            case
                              when base.Dk_Programme_Instance_Dim is null then null
                                    else base.Programme || ' [' || dateformat(base.Broadcast_Start_Time_UTC, 'yyyy-mm-dd @hh:mm') || ']'
                            end;
commit;

  -- Negligible viewing, a few programme only - causes issues with overlapping programmes
delete from EPL_50_CL_EPG
 where Channel = 'Pub channel/Other';
commit;

  -- Manual hack to correct linear/red button association
update EPL_50_CL_EPG
   set EPL_Pack = case
                    when Service_Key in (1471, 1472, 1473) then 'Red button'
                      else EPL_Pack
                  end;
commit;



  -- ##############################################################################################################
  -- ##### Audit                                                                                              #####
  -- ##############################################################################################################

select Data_Source, Broadcast_Duration , count(*) as Cnt from EPL_50_CL_EPG group by Data_Source, Broadcast_Duration order by 1, 2;
select Data_Source, Programme          , count(*) as Cnt from EPL_50_CL_EPG group by Data_Source, Programme          order by 1, 2;
select Data_Source, Channel            , count(*) as Cnt from EPL_50_CL_EPG group by Data_Source, Channel            order by 1, 2;
select Data_Source, Game               , count(*) as Cnt from EPL_50_CL_EPG group by Data_Source, Game               order by 1, 2;
select Data_Source, Season             , count(*) as Cnt from EPL_50_CL_EPG group by Data_Source, Season             order by 1, 2;
select Data_Source, Match_Date         , count(*) as Cnt from EPL_50_CL_EPG group by Data_Source, Match_Date         order by 1, 2;
select Data_Source, Kick_Off_Time      , count(*) as Cnt from EPL_50_CL_EPG group by Data_Source, Kick_Off_Time      order by 1, 2;
select Data_Source, Day_Of_Week        , count(*) as Cnt from EPL_50_CL_EPG group by Data_Source, Day_Of_Week        order by 1, 2;
select Data_Source, Live_Game_Flag     , count(*) as Cnt from EPL_50_CL_EPG group by Data_Source, Live_Game_Flag     order by 1, 2;
select Data_Source, EPL_Pack           , count(*) as Cnt from EPL_50_CL_EPG group by Data_Source, EPL_Pack           order by 1, 2;
select Channel, EPL_Pack               , count(*) as Cnt from EPL_50_CL_EPG group by Channel, EPL_Pack           order by 1, 2;


  -- ##############################################################################################################
  -- ##############################################################################################################


























