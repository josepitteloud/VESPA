/*###############################################################################
# Created on:   04/11/2013
# Created by:   Mandy Ng (MNG)
# Description:  CIA Phase 2 (genres) - viewing instance flagging
#
# List of steps:
#               STEP 1 - Table creation
#               STEP 2 - Viewing table processing
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Based on a pre-processed copy of DP_PROG_VIEWED_201305 stored in "bednaszs"
#    schema (VAggr_02_Viewing_Events_201305)
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 04/11/2013  MNG   Initial version
# 08/11/2013  SBE   Corrections, modifications and adjustments to meet project
#                   requirements + optimisation
#
###############################################################################*/


  -- ##############################################################################################################
  -- ##### STEP 1 - Table creation                                                                            #####
  -- ##############################################################################################################
if object_id('VAggr_02_Viewing_Events_PH2_GENRES') is not null then drop table VAggr_02_Viewing_Events_PH2_GENRES end if;
create table VAggr_02_Viewing_Events_PH2_GENRES (
      pk_viewing_prog_instance_fact       bigint                     default null,
      Account_Number                      varchar(20)                default null,
      Subscriber_Id                       numeric(8, 0)              default null,
      Service_Key                         bigint                     default null,

      Event_Start_Date                    date                       default null,
      Instance_Start_Date                 date                       default null,
      Instance_Start_Date_Time            timestamp                  default null,
      Instance_End_Date_Time              timestamp                  default null,
      Instance_Duration                   int                        default null,

      Prog_Instance_Id                    bigint            null     default null,
      Prog_Instance_Broadcast_Duration    bigint            null     default null,
      Prog_Instance_Viewed_Duration       bigint            null     default null,

      F_Playback                          bit               null     default 0,
      F_Format_HD                         bit               null     default 0,
      F_Format_SD                         bit               null     default 0,
      F_Format_3D                         bit               null     default 0,

      F_CType_Ent_Pay                     bit               null     default 0,
      F_CType_Ent_Extra_Pay               bit               null     default 0,
      F_CType_Ent_Extra_Plus_Pay          bit               null     default 0,
      F_CType_E_EE_EEP_FTA                bit               null     default 0,

      F_CType_Retail_Movies               bit               null     default 0,
      F_CType_Retail_ALC_Movies_Pack      bit               null     default 0,
      F_CType_Retail_Sports               bit               null     default 0,
      F_CType_Retail_ALa_Carte            bit               null     default 0,
      F_CType_3rd_Party                   bit               null     default 0,
      F_CType_Pay                         bit               null     default 0,

       -- Individual genres (non-premium)
      F_Genre_Children                    bit               null     default 0,
      F_Genre_Movies                      bit               null     default 0,
      F_Genre_News_Documentaries          bit               null     default 0,
      F_Genre_Sports                      bit               null     default 0,
      F_Genre_Action_SciFi                bit               null     default 0,
      F_Genre_Arts_Lifestyle              bit               null     default 0,
      F_Genre_Comedy_GameShows            bit               null     default 0,
      F_Genre_Drama_Crime                 bit               null     default 0,

       -- Individual genres (Movies premium)
      F_Genre_Action_Adventure            bit               null     default 0,
      F_Genre_Comedy                      bit               null     default 0,
      F_Genre_Drama_Romance               bit               null     default 0,
      F_Genre_Family                      bit               null     default 0,
      F_Genre_Horror_Thriller             bit               null     default 0,
      F_Genre_SciFi_Fantasy               bit               null     default 0,

         -- Individual genres (Sports premium)
      F_Genre_American                    bit               null     default 0,
      F_Genre_Boxing_Wrestling            bit               null     default 0,
      F_Genre_Cricket                     bit               null     default 0,
      F_Genre_Football                    bit               null     default 0,
      F_Genre_Golf                        bit               null     default 0,
      F_Genre_Motor_Extreme               bit               null     default 0,
      F_Genre_Rugby                       bit               null     default 0,
      F_Genre_Tennis                      bit               null     default 0,
      F_Genre_Niche_Sport                 bit               null     default 0

);

create   hg index idx01 on VAggr_02_Viewing_Events_PH2_GENRES(pk_viewing_prog_instance_fact);
create   hg index idx02 on VAggr_02_Viewing_Events_PH2_GENRES(Account_Number);
create   hg index idx03 on VAggr_02_Viewing_Events_PH2_GENRES(Service_Key);
create date index idx04 on VAggr_02_Viewing_Events_PH2_GENRES(Event_Start_Date);
create date index idx05 on VAggr_02_Viewing_Events_PH2_GENRES(Instance_Start_Date);
create dttm index idx06 on VAggr_02_Viewing_Events_PH2_GENRES(Instance_Start_Date_Time);
create   hg index idx07 on VAggr_02_Viewing_Events_PH2_GENRES(Prog_Instance_Id);

grant select on VAggr_02_Viewing_Events_PH2_GENRES to vespa_group_low_security;
grant update, insert, delete on VAggr_02_Viewing_Events_PH2_GENRES to ngm;

truncate bednaszs.VAggr_02_Viewing_Events_PH2_GENRES;



  -- ##############################################################################################################
  -- ##### STEP 2 - Viewing table processing                                                                  #####
  -- ##############################################################################################################
begin

      declare @varBuildId                     bigint              -- Logger ID (so all builds end up in same queue)
      declare @varStartDate                   date
      declare @varEndDate                     date
      declare @varEventStartHour              int
      declare @varEventEndHour                int

      set @varStartDate = '2013-05-01'
      set @varEndDate   = '2013-05-31'

      execute logger_create_run 'VAggr_2_Vw_Data_v01', 'SBE run', @varBuildId output

      execute logger_add_event @varBuildId, 3, 'Process start', null

      while @varStartDate <= @varEndDate
          begin

              set @varEventStartHour    = (dateformat(@varStartDate, 'yyyymmdd00'))
              set @varEventEndHour      = (dateformat(@varStartDate, 'yyyymmdd23'))

              delete from bednaszs.VAggr_02_Viewing_Events_PH2_GENRES
               where Event_Start_Date = @varStartDate
              execute logger_add_event @varBuildId, 3, 'Viewing data rows for current period removed', @@rowcount


              insert into bednaszs.VAggr_02_Viewing_Events_PH2_GENRES
                select
                        -- Keys
                      vw.pk_viewing_prog_instance_fact,
                      base.Account_Number,
                      vw.Subscriber_Id,
                      vw.Service_Key,

                        -- Dates/durations
                      vw.Event_Start_Date,
                      vw.Instance_Start_Date,
                      vw.Instance_Start_Date_Time,
                      vw.Instance_End_Date_Time,
                      vw.Instance_Duration,

                        -- Programme instance viewing/broadcast
                      vw.Dk_Programme_Instance_Dim                            as Prog_Instance_Id,
                      vw.Prog_Instance_Broadcast_Duration,
                      0                                                       as Prog_Instance_Viewed_Duration,


                       -- Flags
                      vw.Playback_Flag as F_Playback,
                      case when (vw.Format = 'HD') then 1 else 0 end          as F_Format_HD,
                      case when (vw.Format = 'SD') then 1 else 0 end          as F_Format_SD,
                      case when (vw.Format = '3D') then 1 else 0 end          as F_Format_3D,

                      case
                        when vw.Channel_Type in ('Retail - Entertainment', 'Retail - Entertainment (NI only)') then 1
                          else 0
                      end                                                     as F_CType_Ent_Pay,

                      case
                        when vw.Channel_Type in ('Retail - Entertainment Extra', 'Retail - Entertainment', 'Retail - Entertainment (NI only)') then 1
                          else 0
                      end                                                     as F_CType_Ent_Extra_Pay,

                      case
                        when vw.Channel_Type in ('Retail - Entertainment+', 'Retail - Entertainment Extra', 'Retail - Entertainment',
                                                 'Retail - Entertainment (NI only)', 'Retail - 3D') then 1
                          else 0
                      end                                                     as F_CType_Ent_Extra_Plus_Pay,

                      case
                        when vw.Channel_Type in ('NR - FTA', 'NR - Automatically Entitled', 'NR - FTA (Sky)', 'NR - Regional Variant') then 1
                          else 0
                      end                                                     as F_CType_E_EE_EEP_FTA,

                      case
                        when vw.Channel_Type in ('Retail - Movies', 'Retail - Movies + HD Pack') then 1
                          else 0
                      end                                                     as F_CType_Retail_Movies,

                      case
                        when vw.Channel_Type in ('Retail - ALC / Movies Pack + Ent Extra+') then 1
                          else 0
                      end                                                     as F_CType_Retail_ALC_Movies_Pack,

                      case
                        when vw.Channel_Type in ('Retail - Sports', 'Retail - Sports + HD Pack') then 1
                          else 0
                      end                                                     as F_CType_Retail_Sports,

                      case
                        when vw.Channel_Type in ('Retail - ALC', 'Retail - ALC + Ent Extra+') then 1
                          else 0
                      end                                                     as F_CType_Retail_ALa_Carte,

                      case
                        when vw.Channel_Type in ('NR - Conditional Access', 'NR - Pay-per-view') then 1
                          else 0
                      end                                                     as F_CType_3rd_Party,

                      case
                        when vw.Channel_Type in ('Retail - 3D', 'Retail - Adult Nightly', 'Retail – ALC', 'Retail - ALC / Movies Pack + Ent Extra+',
                                                 'Retail - ALC + Ent Extra+', 'Retail – Entertainment', 'Retail - Entertainment (NI only)',
                                                 'Retail - Entertainment Extra', 'Retail - Entertainment+', 'Retail – Movies', 'Retail - Movies + HD Pack',
                                                 'Retail - Pay-per-night', 'Retail - Pay-per-view', 'Retail - PPV HD', 'Retail - ROI Bonus',
                                                 'Retail – Sports', 'Retail - Sports + HD Pack') then 1
                          else 0
                      end                                                     as F_CType_Pay,


                        ------ ######################################################################################
                        -- Genre Flags
                        -- Package #1
                      case
                          when vw.Genre_Description = 'Children' then 1
                            else 0
                      end                                                     as F_Genre_Children,

                       ---- Package #2
                      case
                        when vw.Genre_Description = 'Movies' then 1
                          else 0
                      end                                                     as F_Genre_Movies,

                        ------ Package #3
                      case
                        when (vw.Genre_Description = 'News & Documentaries' or
                             (vw.Genre_Description = 'Entertainment' and vw.Sub_Genre_Description in
                               ('Chat Show',
                                'Factual',
                                'Medical',
                                'Motors',
                                'Reviews',
                                'Technology'))) then 1
                          else 0
                      end                                                     as F_Genre_News_Documentaries,

                       ----Package #4
                      case
                        when vw.Genre_Description = 'Sports' then 1
                          else 0
                      end                                                     as F_Genre_Sports,

                        ------ Package #5
                      case
                        when (vw.Genre_Description = 'Entertainment' and
                             vw.Sub_Genre_Description in ('Action', 'Sci-Fi')) then 1
                          else 0
                      end                                                     as F_Genre_Action_SciFi,

                        ------ Package #6
                      case
                        when ((vw.Genre_Description = 'Entertainment' and vw.Sub_Genre_Description in
                                ('Antiques',
                                'Art&Lit',
                                'Arts',
                                'Ballet',
                                'Cooking',
                                'Fashion',
                                'Gardening',
                                'Home',
                                'Lifestyle',
                                'Magazine',
                                'Opera',
                                'Travel'))
                                or (vw.Genre_Description = 'Specialist' and vw.Sub_Genre_Description in ('Events', 'Shopping'))
                                or vw.Genre_Description = 'Music & Radio') then 1
                          else 0
                      end                                                     as F_Genre_Arts_Lifestyle,

                        ------ Package #7
                      case
                        when (vw.Genre_Description = 'Entertainment' and vw.Sub_Genre_Description in
                                ('Comedy',
                                'Game Shows',
                                'Animation')
                                or (vw.Genre_Description = 'Specialist' and vw.Sub_Genre_Description = 'Gaming')) then 1
                          else 0
                      end                                                     as F_Genre_Comedy_GameShows,

                        ------ Package #8
                      case
                        when (vw.Genre_Description = 'Entertainment' and vw.Sub_Genre_Description in
                                ('Detective',
                                'Drama',
                                'Soaps')) then 1
                          else 0
                      end                                                     as F_Genre_Drama_Crime,


                        ------ ######################################################################################
                        ------ Movies #9a
                     case
                       when (vw.Genre_Description = 'Movies' and vw.Sub_Genre_Description in
                                ('Action',
                                'Adventure',
                                'War',
                                'Western')) then 1
                          else 0
                      end                                                     as F_Genre_Action_Adventure,

                        ------ Movies #10a
                      case
                        when (vw.Genre_Description = 'Movies' and vw.Sub_Genre_Description in
                                ('Comedy')) then 1
                          else 0
                      end                                                     as F_Genre_Comedy,

                        ------ Movies #11a
                      case
                        when (vw.Genre_Description = 'Movies' and vw.Sub_Genre_Description in
                                ('Drama',
                                'Romance',
                                'Musical',
                                'Factual')) then 1
                          else 0
                      end                                                     as F_Genre_Drama_Romance,

                        ------ Movies #12a
                      case
                        when (vw.Genre_Description = 'Movies' and vw.Sub_Genre_Description in
                                ('Family',
                                'Animation')) then 1
                          else 0
                      end                                                     as F_Genre_Family,

                        ------ Movies #13a
                      case
                        when (vw.Genre_Description = 'Movies' and vw.Sub_Genre_Description in
                                ('Horror',
                                'Thriller')) then 1
                          else 0
                      end                                                     as F_Genre_Horror_Thriller,

                        ------ Movies #14a
                      case
                        when (vw.Genre_Description = 'Movies' and vw.Sub_Genre_Description in
                                ('Fantasy',
                                'Sci-Fi')) then 1
                          else 0
                      end                                                     as F_Genre_SciFi_Fantasy,

                        ------ ######################################################################################
                        ------ Sports #15
                      case
                        when (vw.Genre_Description = 'Sports' and vw.Sub_Genre_Description in
                                ('American Football',
                                'Baseball',
                                'Basketball',
                                'Ice Hockey')) then 1
                          else 0
                      end                                                     as F_Genre_American,


                        ------ Sports #16
                      case
                        when (vw.Genre_Description = 'Sports' and vw.Sub_Genre_Description in
                                ('Boxing',
                                'Wrestling')) then 1
                          else 0
                      end                                                     as F_Genre_Boxing_Wrestling,

                        ------ Sports #17
                      case
                        when (vw.Genre_Description = 'Sports' and vw.Sub_Genre_Description in
                                ('Cricket')) then 1
                          else 0
                      end                                                     as F_Genre_Cricket,

                        ------ Sports #18
                      case
                        when (vw.Genre_Description = 'Sports' and vw.Sub_Genre_Description in
                                ('Football')) then 1
                          else 0
                      end                                                     as F_Genre_Football,

                        ------ Sports #19
                      case
                        when (vw.Genre_Description = 'Sports' and vw.Sub_Genre_Description in
                                ('Golf')) then 1
                          else 0
                      end                                                     as F_Genre_Golf,

                        ------ Sports #20
                      case
                        when (vw.Genre_Description = 'Sports' and vw.Sub_Genre_Description in
                                ('Motor Sport', 'Extreme')) then 1
                          else 0
                      end                                                     as F_Genre_Motor_Extreme,

                        ------ Sports #21
                      case
                        when (vw.Genre_Description = 'Sports' and vw.Sub_Genre_Description in
                                ('Rugby')) then 1
                          else 0
                      end                                                     as F_Genre_Rugby,

                        ------ Sports #22
                      case
                        when (vw.Genre_Description = 'Sports' and vw.Sub_Genre_Description ='Tennis') then 1
                          else 0
                      end                                                     as F_Genre_Tennis,

                        ------ Sports #23
                      case
                        when (vw.Genre_Description = 'Sports' and vw.Sub_Genre_Description in
                                ('Athletics',
                                'Watersports',
                                'Wintersports',
                                'Equestrian',
                                'Fishing',
                                'Racing',
                                'Darts',
                                'Snooker/Pool')) then 1
                          else 0
                      end                                                     as F_Genre_Niche_Sports

                  from vespa_shared.Aggr_Account_Attributes base
                          inner join bednaszs.VAggr_02_Viewing_Events_201305 vw       on base.Account_Number = vw.Account_Number
                                                                                      and base.Period_Key = 5
                                                                                      and vw.dk_event_start_datehour_dim between @varEventStartHour and @varEventEndHour
                                                                                      and vw.event_duration_capped > 6
              commit

              execute logger_add_event @varBuildId, 3, 'Day processed: ' || dateformat(@varStartDate, 'dd/mm/yyyy'), @@rowcount

              set @varStartDate = @varStartDate + 1
          end

      execute logger_add_event @varBuildId, 3, 'Viewing data created', null

end



  -- ##############################################################################################################
  -- ##############################################################################################################






