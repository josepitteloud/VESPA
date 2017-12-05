/*###############################################################################
# Created on:   04/11/2013
# Created by:   Mandy Ng (MNG)
# Description:  CIA Phase 2 (genres) - metric creation for aggregations
#
# List of steps:
#               STEP 1 - Table creation
#               STEP 2 - Metric calculation
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables required:
#     - VESPA_Shared.Aggr_Account_Attributes
#     - VAggr_02_Viewing_Events_PH2_GENRES
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
if object_id('VAggr_03_Account_Metrics_PH2_GENRES') is not null then drop table VAggr_03_Account_Metrics_PH2_GENRES end if;
create table VAggr_03_Account_Metrics_PH2_GENRES (
      Id                                  bigint            identity,
      Aggregation_Id                      tinyint           default 0,
      Account_Number                      varchar(20)       default null,
      Median_Weight                       decimal(15, 6)    default 0,
      Days_Data_Returned                  smallint          default 0,
      Days_Period                         smallint          default 0,
      Account_Status                      smallint          default 0,

      Total_Viewing_Dur                   bigint            default 0,
      Total_Aggr_Viewing_Dur              bigint            default 0,
      Avg_Daily_Viewing_Dur               decimal(15, 6)    default 0,
      Avg_Daily_Aggr_Viewing_Dur          decimal(15, 6)    default 0,
      Share_Of_Viewing                    decimal(15, 6)    default 0,

      QA_Result                           varchar(30)       default null,

      Updated_On                          datetime          default timestamp,
      Updated_By                          varchar(30)       default user_name()
);

create        hg index idx01 on VAggr_03_Account_Metrics_PH2_GENRES(Account_Number);
create        lf index idx02 on VAggr_03_Account_Metrics_PH2_GENRES(Aggregation_Id);
grant select on VAggr_03_Account_Metrics_PH2_GENRES to vespa_group_low_security;
grant update, insert, delete on VAggr_03_Account_Metrics_PH2_GENRES to ngm;



  -- ##############################################################################################################
  -- ##### STEP 1 - Metric calculation                                                                        #####
  -- ##############################################################################################################

-- ####################################
-- ##### TOTALS                   #####
-- ####################################
create variable @varSql               varchar(15000);
create variable @varAccStatus         varchar(15000);
create variable @varTotalAggrVwDur    varchar(15000);
create variable @varSOV               varchar(15000);

set @varSql = '
                message ''##### Processing aggregation ###^2^## #####'' to client

                delete from VAggr_03_Account_Metrics_PH2_GENRES
                 where Aggregation_Id = ##^2^##
                commit
                message ''['' || dateformat( now(), ''yyyy-mm-dd HH:mm:ss'' ) || ''] Existing records deleted ('' || @@rowcount || '' rows deleted)'' to client

                insert into VAggr_03_Account_Metrics_PH2_GENRES
                       (Aggregation_Id,
                        Account_Number,
                        Median_Weight,
                        Days_Data_Returned,
                        Days_Period,
                        Account_Status,

                        Total_Viewing_Dur,
                        Total_Aggr_Viewing_Dur,
                        Avg_Daily_Viewing_Dur,
                        Avg_Daily_Aggr_Viewing_Dur,
                        Share_Of_Viewing)

                  select
                        ##^2^## as Aggregation_Id,                                 -- Aggregation Id
                        acc.Account_Number,
                        max(acc.Median_Scaling_Weight),
                        max(acc.Days_Data_Returned),
                        max(acc.Days_Period),
                        min(##^3^##)                                                as Account_Status,
                        sum(case when vw.Instance_Duration is null then 0 else vw.Instance_Duration end)
                                                                                    as Total_Viewing_Dur,
                        sum(##^1^##)                                                as Total_Aggr_Viewing_Dur,

                        1.0 * Total_Viewing_Dur / max(acc.Days_Data_Returned)       as Avg_Daily_Viewing_Dur,
                        1.0 * Total_Aggr_Viewing_Dur / max(acc.Days_Data_Returned)  as Avg_Daily_Aggr_Viewing_Dur,

                        1.0 * Avg_Daily_Aggr_Viewing_Dur / ##^0^##                  as Share_Of_Viewing

                    from VESPA_Shared.Aggr_Account_Attributes acc
                            left join VAggr_02_Viewing_Events_PH2_GENRES vw          on acc.Account_Number = vw.Account_Number

                   where acc.Ent_DTV_Sub                        = 1                 -- Always must have active DTV sub
                     and acc.Movmt_DTV_Sub                      = 0                 -- No movement for DTV sub
                     and acc.period_key = 5
                   group by acc.Account_Number
                commit
                message ''['' || dateformat( now(), ''yyyy-mm-dd HH:mm:ss'' ) || ''] Metrics created ('' || @@rowcount || '' rows inserted)'' to client

                  -- QA
                update VAggr_03_Account_Metrics_PH2_GENRES
                   set QA_Result  = case
                                      when Total_Viewing_Dur < Total_Aggr_Viewing_Dur then ''All view < Segm View''
                                        else ''OK''
                                    end
                 where QA_Result is null
                commit
                message ''['' || dateformat( now(), ''yyyy-mm-dd HH:mm:ss'' ) || ''] QA results updated('' || @@rowcount || '' rows updated)'' to client

              ';
commit;


-- ##################################################################
-- ##### E/EE/EE+/MOVIES/SPORTS total aggregations              #####
-- ##################################################################
-- ##### Aggregation #02 - Entertainment ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Pack_Ent = 0 or acc.Movmt_DTV_Pack_Ent = 1 then -3                               -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when vw.F_CType_Ent_Pay = 1 or vw.F_CType_E_EE_EEP_FTA = 1 then Instance_Duration
                            else 0
                        end
                      ';

set @varSOV =  '
                        case when Avg_Daily_Viewing_Dur = 0 then null else Avg_Daily_Viewing_Dur end
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '2'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;


-- ##### Aggregation #03 - Entertainment Extra ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Pack_Ent_Extra = 0 or acc.Movmt_DTV_Pack_Ent_Extra = 1 then -3                   -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when vw.F_CType_Ent_Extra_Pay = 1 or vw.F_CType_E_EE_EEP_FTA = 1 then Instance_Duration
                            else 0
                        end
                      ';

set @varSOV =  '
                        case when Avg_Daily_Viewing_Dur = 0 then null else Avg_Daily_Viewing_Dur end
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '3'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;


-- ##### Aggregation #04 - Entertainment Extra+ ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Pack_Ent_Extra_Plus = 0 or acc.Movmt_DTV_Pack_Ent_Extra_Plus = 1 then -3         -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when vw.F_CType_Ent_Extra_Plus_Pay = 1 or vw.F_CType_E_EE_EEP_FTA = 1 then Instance_Duration
                            else 0
                        end
                      ';
set @varSOV =  '
                        case when Avg_Daily_Viewing_Dur = 0 then null else Avg_Daily_Viewing_Dur end
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '4'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;


-- ##### Aggregation #05 - Movies ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Prem_Movies = 0 or acc.Movmt_DTV_Prem_Movies = 1 then -3                         -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when vw.F_CType_Retail_Movies = 1 then Instance_Duration

                              --Additionally for customers with Movies package who have the (Entertainment Extra+ OR HD package) include = "Retail - ALC / Movies Pack + Ent Extra+"
                          when (acc.Ent_DTV_Prem_Movies = 1 and (acc.Ent_DTV_Pack_Ent_Extra_Plus = 1 or acc.Ent_HD_Sub = 1)) and
                               vw.F_CType_Retail_ALC_Movies_Pack = 1 then Instance_Duration

                            else 0
                        end
                      ';
set @varSOV =  '
                        case when Avg_Daily_Viewing_Dur = 0 then null else Avg_Daily_Viewing_Dur end
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '5'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;


-- ##### Aggregation #06 - Sports ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Prem_Sports = 0 or acc.Movmt_DTV_Prem_Sports = 1 then -3                         -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when vw.F_CType_Retail_Sports = 1 then Instance_Duration
                            else 0
                        end
                      ';
set @varSOV =  '
                        case when Avg_Daily_Viewing_Dur = 0 then null else Avg_Daily_Viewing_Dur end
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '6'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;



-- ##################################################################
-- ##### ENTERTAINMENT                                          #####
-- ##################################################################
-- ##### Aggregation #10 - Entertainment - Children ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Pack_Ent = 0 or acc.Movmt_DTV_Pack_Ent = 1 then -3                               -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when (vw.F_CType_Ent_Pay = 1 or vw.F_CType_E_EE_EEP_FTA = 1) and vw.F_Genre_Children = 1 then Instance_Duration
                            else 0
                        end
                      ';
set @varSOV =  '
                        (select case when Avg_Daily_Aggr_Viewing_Dur = 0 then null else Avg_Daily_Aggr_Viewing_Dur end
                           from VAggr_03_Account_Metrics_PH2_GENRES mettwo
                          where Aggregation_Id = 2
                            and acc.Account_Number = mettwo.Account_Number)
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '10'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;


-- ##### Aggregation #11 - Entertainment - Movies ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Pack_Ent = 0 or acc.Movmt_DTV_Pack_Ent = 1 then -3                               -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when (vw.F_CType_Ent_Pay = 1 or vw.F_CType_E_EE_EEP_FTA = 1) and vw.F_Genre_Movies = 1 then Instance_Duration
                            else 0
                        end
                      ';
set @varSOV =  '
                        (select case when Avg_Daily_Aggr_Viewing_Dur = 0 then null else Avg_Daily_Aggr_Viewing_Dur end
                           from VAggr_03_Account_Metrics_PH2_GENRES mettwo
                          where Aggregation_Id = 2
                            and acc.Account_Number = mettwo.Account_Number)
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '11'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;


-- ##### Aggregation #12 - Entertainment - News & Documentaries ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Pack_Ent = 0 or acc.Movmt_DTV_Pack_Ent = 1 then -3                               -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when (vw.F_CType_Ent_Pay = 1 or vw.F_CType_E_EE_EEP_FTA = 1) and vw.F_Genre_News_Documentaries = 1 then Instance_Duration
                            else 0
                        end
                      ';
set @varSOV =  '
                        (select case when Avg_Daily_Aggr_Viewing_Dur = 0 then null else Avg_Daily_Aggr_Viewing_Dur end
                           from VAggr_03_Account_Metrics_PH2_GENRES mettwo
                          where Aggregation_Id = 2
                            and acc.Account_Number = mettwo.Account_Number)
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '12'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;


-- ##### Aggregation #13 - Entertainment - Sports ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Pack_Ent = 0 or acc.Movmt_DTV_Pack_Ent = 1 then -3                               -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when (vw.F_CType_Ent_Pay = 1 or vw.F_CType_E_EE_EEP_FTA = 1) and vw.F_Genre_Sports = 1 then Instance_Duration
                            else 0
                        end
                      ';
set @varSOV =  '
                        (select case when Avg_Daily_Aggr_Viewing_Dur = 0 then null else Avg_Daily_Aggr_Viewing_Dur end
                           from VAggr_03_Account_Metrics_PH2_GENRES mettwo
                          where Aggregation_Id = 2
                            and acc.Account_Number = mettwo.Account_Number)
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '13'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;


-- ##### Aggregation #14 - Entertainment - Action SciFi ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Pack_Ent = 0 or acc.Movmt_DTV_Pack_Ent = 1 then -3                               -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when (vw.F_CType_Ent_Pay = 1 or vw.F_CType_E_EE_EEP_FTA = 1) and vw.F_Genre_Action_SciFi = 1 then Instance_Duration
                            else 0
                        end
                      ';
set @varSOV =  '
                        (select case when Avg_Daily_Aggr_Viewing_Dur = 0 then null else Avg_Daily_Aggr_Viewing_Dur end
                           from VAggr_03_Account_Metrics_PH2_GENRES mettwo
                          where Aggregation_Id = 2
                            and acc.Account_Number = mettwo.Account_Number)
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '14'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;


-- ##### Aggregation #15 - Entertainment - Arts Lifestyle ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Pack_Ent = 0 or acc.Movmt_DTV_Pack_Ent = 1 then -3                               -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when (vw.F_CType_Ent_Pay = 1 or vw.F_CType_E_EE_EEP_FTA = 1) and vw.F_Genre_Arts_Lifestyle = 1 then Instance_Duration
                            else 0
                        end
                      ';
set @varSOV =  '
                        (select case when Avg_Daily_Aggr_Viewing_Dur = 0 then null else Avg_Daily_Aggr_Viewing_Dur end
                           from VAggr_03_Account_Metrics_PH2_GENRES mettwo
                          where Aggregation_Id = 2
                            and acc.Account_Number = mettwo.Account_Number)
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '15'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;


-- ##### Aggregation #16 - Entertainment - Comedy Gameshow ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Pack_Ent = 0 or acc.Movmt_DTV_Pack_Ent = 1 then -3                               -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when (vw.F_CType_Ent_Pay = 1 or vw.F_CType_E_EE_EEP_FTA = 1) and vw.F_Genre_Comedy_GameShows = 1 then Instance_Duration
                            else 0
                        end
                      ';
set @varSOV =  '
                        (select case when Avg_Daily_Aggr_Viewing_Dur = 0 then null else Avg_Daily_Aggr_Viewing_Dur end
                           from VAggr_03_Account_Metrics_PH2_GENRES mettwo
                          where Aggregation_Id = 2
                            and acc.Account_Number = mettwo.Account_Number)
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '16'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;


-- ##### Aggregation #17 - Entertainment - Drama Crime ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Pack_Ent = 0 or acc.Movmt_DTV_Pack_Ent = 1 then -3                               -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when (vw.F_CType_Ent_Pay = 1 or vw.F_CType_E_EE_EEP_FTA = 1) and vw.F_Genre_Drama_Crime = 1 then Instance_Duration
                            else 0
                        end
                      ';
set @varSOV =  '
                        (select case when Avg_Daily_Aggr_Viewing_Dur = 0 then null else Avg_Daily_Aggr_Viewing_Dur end
                           from VAggr_03_Account_Metrics_PH2_GENRES mettwo
                          where Aggregation_Id = 2
                            and acc.Account_Number = mettwo.Account_Number)
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '17'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;



-- ##################################################################
-- ##### ENTERTAINMENT EXTRA                                    #####
-- ##################################################################
-- ##### Aggregation #20 - Entertainment Extra - Children ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Pack_Ent_Extra = 0 or acc.Movmt_DTV_Pack_Ent_Extra = 1 then -3                   -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when (vw.F_CType_Ent_Extra_Pay = 1 or vw.F_CType_E_EE_EEP_FTA = 1) and vw.F_Genre_Children = 1 then Instance_Duration
                            else 0
                        end
                      ';
set @varSOV =  '
                        (select case when Avg_Daily_Aggr_Viewing_Dur = 0 then null else Avg_Daily_Aggr_Viewing_Dur end
                           from VAggr_03_Account_Metrics_PH2_GENRES mettwo
                          where Aggregation_Id = 3
                            and acc.Account_Number = mettwo.Account_Number)
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '20'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;


-- ##### Aggregation #21 - Entertainment Extra - Movies ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Pack_Ent_Extra = 0 or acc.Movmt_DTV_Pack_Ent_Extra = 1 then -3                   -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when (vw.F_CType_Ent_Extra_Pay = 1 or vw.F_CType_E_EE_EEP_FTA = 1) and vw.F_Genre_Movies = 1 then Instance_Duration
                            else 0
                        end
                      ';
set @varSOV =  '
                        (select case when Avg_Daily_Aggr_Viewing_Dur = 0 then null else Avg_Daily_Aggr_Viewing_Dur end
                           from VAggr_03_Account_Metrics_PH2_GENRES mettwo
                          where Aggregation_Id = 3
                            and acc.Account_Number = mettwo.Account_Number)
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '21'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;


-- ##### Aggregation #22 - Entertainment Extra - News & Documentaries ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Pack_Ent_Extra = 0 or acc.Movmt_DTV_Pack_Ent_Extra = 1 then -3                   -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when (vw.F_CType_Ent_Extra_Pay = 1 or vw.F_CType_E_EE_EEP_FTA = 1) and vw.F_Genre_News_Documentaries = 1 then Instance_Duration
                            else 0
                        end
                      ';
set @varSOV =  '
                        (select case when Avg_Daily_Aggr_Viewing_Dur = 0 then null else Avg_Daily_Aggr_Viewing_Dur end
                           from VAggr_03_Account_Metrics_PH2_GENRES mettwo
                          where Aggregation_Id = 3
                            and acc.Account_Number = mettwo.Account_Number)
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '22'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;


-- ##### Aggregation #23 - Entertainment Extra - Sports ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Pack_Ent_Extra = 0 or acc.Movmt_DTV_Pack_Ent_Extra = 1 then -3                   -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when (vw.F_CType_Ent_Extra_Pay = 1 or vw.F_CType_E_EE_EEP_FTA = 1) and vw.F_Genre_Sports = 1 then Instance_Duration
                            else 0
                        end
                      ';
set @varSOV =  '
                        (select case when Avg_Daily_Aggr_Viewing_Dur = 0 then null else Avg_Daily_Aggr_Viewing_Dur end
                           from VAggr_03_Account_Metrics_PH2_GENRES mettwo
                          where Aggregation_Id = 3
                            and acc.Account_Number = mettwo.Account_Number)
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '23'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;


-- ##### Aggregation #24 - Entertainment Extra - Action SciFi ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Pack_Ent_Extra = 0 or acc.Movmt_DTV_Pack_Ent_Extra = 1 then -3                   -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when (vw.F_CType_Ent_Extra_Pay = 1 or vw.F_CType_E_EE_EEP_FTA = 1) and vw.F_Genre_Action_SciFi = 1 then Instance_Duration
                            else 0
                        end
                      ';
set @varSOV =  '
                        (select case when Avg_Daily_Aggr_Viewing_Dur = 0 then null else Avg_Daily_Aggr_Viewing_Dur end
                           from VAggr_03_Account_Metrics_PH2_GENRES mettwo
                          where Aggregation_Id = 3
                            and acc.Account_Number = mettwo.Account_Number)
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '24'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;


-- ##### Aggregation #25 - Entertainment Extra - Arts Lifestyle ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Pack_Ent_Extra = 0 or acc.Movmt_DTV_Pack_Ent_Extra = 1 then -3                   -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when (vw.F_CType_Ent_Extra_Pay = 1 or vw.F_CType_E_EE_EEP_FTA = 1) and vw.F_Genre_Arts_Lifestyle = 1 then Instance_Duration
                            else 0
                        end
                      ';
set @varSOV =  '
                        (select case when Avg_Daily_Aggr_Viewing_Dur = 0 then null else Avg_Daily_Aggr_Viewing_Dur end
                           from VAggr_03_Account_Metrics_PH2_GENRES mettwo
                          where Aggregation_Id = 3
                            and acc.Account_Number = mettwo.Account_Number)
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '25'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;


-- ##### Aggregation #26 - Entertainment Extra - Comedy Gameshow ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Pack_Ent_Extra = 0 or acc.Movmt_DTV_Pack_Ent_Extra = 1 then -3                   -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when (vw.F_CType_Ent_Extra_Pay = 1 or vw.F_CType_E_EE_EEP_FTA = 1) and vw.F_Genre_Comedy_GameShows = 1 then Instance_Duration
                            else 0
                        end
                      ';
set @varSOV =  '
                        (select case when Avg_Daily_Aggr_Viewing_Dur = 0 then null else Avg_Daily_Aggr_Viewing_Dur end
                           from VAggr_03_Account_Metrics_PH2_GENRES mettwo
                          where Aggregation_Id = 3
                            and acc.Account_Number = mettwo.Account_Number)
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '26'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;


-- ##### Aggregation #27 - Entertainment Extra - Drama Crime ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Pack_Ent_Extra = 0 or acc.Movmt_DTV_Pack_Ent_Extra = 1 then -3                   -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when (vw.F_CType_Ent_Extra_Pay = 1 or vw.F_CType_E_EE_EEP_FTA = 1) and vw.F_Genre_Drama_Crime = 1 then Instance_Duration
                            else 0
                        end
                      ';
set @varSOV =  '
                        (select case when Avg_Daily_Aggr_Viewing_Dur = 0 then null else Avg_Daily_Aggr_Viewing_Dur end
                           from VAggr_03_Account_Metrics_PH2_GENRES mettwo
                          where Aggregation_Id = 3
                            and acc.Account_Number = mettwo.Account_Number)
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '27'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;



-- ##################################################################
-- ##### ENTERTAINMENT EXTRA PLUS                               #####
-- ##################################################################
-- ##### Aggregation #30 - Entertainment Extra Plus - Children ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Pack_Ent_Extra_Plus = 0 or acc.Movmt_DTV_Pack_Ent_Extra_Plus = 1 then -3         -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when (vw.F_CType_Ent_Extra_Plus_Pay = 1 or vw.F_CType_E_EE_EEP_FTA = 1) and vw.F_Genre_Children = 1 then Instance_Duration
                            else 0
                        end
                      ';
set @varSOV =  '
                        (select case when Avg_Daily_Aggr_Viewing_Dur = 0 then null else Avg_Daily_Aggr_Viewing_Dur end
                           from VAggr_03_Account_Metrics_PH2_GENRES mettwo
                          where Aggregation_Id = 4
                            and acc.Account_Number = mettwo.Account_Number)
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '30'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;


-- ##### Aggregation #31 - Entertainment Extra Plus - Movies ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Pack_Ent_Extra_Plus = 0 or acc.Movmt_DTV_Pack_Ent_Extra_Plus = 1 then -3         -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when (vw.F_CType_Ent_Extra_Plus_Pay = 1 or vw.F_CType_E_EE_EEP_FTA = 1) and F_Genre_Movies = 1 then Instance_Duration
                            else 0
                        end
                      ';
set @varSOV =  '
                        (select case when Avg_Daily_Aggr_Viewing_Dur = 0 then null else Avg_Daily_Aggr_Viewing_Dur end
                           from VAggr_03_Account_Metrics_PH2_GENRES mettwo
                          where Aggregation_Id = 4
                            and acc.Account_Number = mettwo.Account_Number)
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '31'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;


-- ##### Aggregation #32 - Entertainment Extra Plus - News & Documentaries ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Pack_Ent_Extra_Plus = 0 or acc.Movmt_DTV_Pack_Ent_Extra_Plus = 1 then -3         -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when (vw.F_CType_Ent_Extra_Plus_Pay = 1 or vw.F_CType_E_EE_EEP_FTA = 1) and F_Genre_News_Documentaries = 1 then Instance_Duration
                            else 0
                        end
                      ';
set @varSOV =  '
                        (select case when Avg_Daily_Aggr_Viewing_Dur = 0 then null else Avg_Daily_Aggr_Viewing_Dur end
                           from VAggr_03_Account_Metrics_PH2_GENRES mettwo
                          where Aggregation_Id = 4
                            and acc.Account_Number = mettwo.Account_Number)
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '32'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;


-- ##### Aggregation #33 - Entertainment Extra Plus - Sports ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Pack_Ent_Extra_Plus = 0 or acc.Movmt_DTV_Pack_Ent_Extra_Plus = 1 then -3         -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when (vw.F_CType_Ent_Extra_Plus_Pay = 1 or vw.F_CType_E_EE_EEP_FTA = 1) and F_Genre_Sports = 1 then Instance_Duration
                            else 0
                        end
                      ';
set @varSOV =  '
                        (select case when Avg_Daily_Aggr_Viewing_Dur = 0 then null else Avg_Daily_Aggr_Viewing_Dur end
                           from VAggr_03_Account_Metrics_PH2_GENRES mettwo
                          where Aggregation_Id = 4
                            and acc.Account_Number = mettwo.Account_Number)
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '33'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;


-- ##### Aggregation #34 - Entertainment Extra Plus - Action SciFi ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Pack_Ent_Extra_Plus = 0 or acc.Movmt_DTV_Pack_Ent_Extra_Plus = 1 then -3         -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when (vw.F_CType_Ent_Extra_Plus_Pay = 1 or vw.F_CType_E_EE_EEP_FTA = 1) and F_Genre_Action_SciFi = 1 then Instance_Duration
                            else 0
                        end
                      ';
set @varSOV =  '
                        (select case when Avg_Daily_Aggr_Viewing_Dur = 0 then null else Avg_Daily_Aggr_Viewing_Dur end
                           from VAggr_03_Account_Metrics_PH2_GENRES mettwo
                          where Aggregation_Id = 4
                            and acc.Account_Number = mettwo.Account_Number)
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '34'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;


-- ##### Aggregation #35 - Entertainment Extra Plus - Arts Lifestyle ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Pack_Ent_Extra_Plus = 0 or acc.Movmt_DTV_Pack_Ent_Extra_Plus = 1 then -3         -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when (vw.F_CType_Ent_Extra_Plus_Pay = 1 or vw.F_CType_E_EE_EEP_FTA = 1) and F_Genre_Arts_Lifestyle = 1 then Instance_Duration
                            else 0
                        end
                      ';
set @varSOV =  '
                        (select case when Avg_Daily_Aggr_Viewing_Dur = 0 then null else Avg_Daily_Aggr_Viewing_Dur end
                           from VAggr_03_Account_Metrics_PH2_GENRES mettwo
                          where Aggregation_Id = 4
                            and acc.Account_Number = mettwo.Account_Number)
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '35'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;


-- ##### Aggregation #36 - Entertainment Extra Plus - Comedy Gameshow ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Pack_Ent_Extra_Plus = 0 or acc.Movmt_DTV_Pack_Ent_Extra_Plus = 1 then -3         -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when (vw.F_CType_Ent_Extra_Plus_Pay = 1 or vw.F_CType_E_EE_EEP_FTA = 1) and F_Genre_Comedy_GameShows = 1 then Instance_Duration
                            else 0
                        end
                      ';
set @varSOV =  '
                        (select case when Avg_Daily_Aggr_Viewing_Dur = 0 then null else Avg_Daily_Aggr_Viewing_Dur end
                           from VAggr_03_Account_Metrics_PH2_GENRES mettwo
                          where Aggregation_Id = 4
                            and acc.Account_Number = mettwo.Account_Number)
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '36'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;


-- ##### Aggregation #37 - Entertainment Extra Plus - Drama Crime ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Pack_Ent_Extra_Plus = 0 or acc.Movmt_DTV_Pack_Ent_Extra_Plus = 1 then -3         -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when (vw.F_CType_Ent_Extra_Plus_Pay = 1 or vw.F_CType_E_EE_EEP_FTA = 1) and F_Genre_Drama_Crime = 1 then Instance_Duration
                            else 0
                        end
                      ';
set @varSOV =  '
                        (select case when Avg_Daily_Aggr_Viewing_Dur = 0 then null else Avg_Daily_Aggr_Viewing_Dur end
                           from VAggr_03_Account_Metrics_PH2_GENRES mettwo
                          where Aggregation_Id = 4
                            and acc.Account_Number = mettwo.Account_Number)
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '37'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;




-- ##################################################################
-- ##### PREMIUM MOVIES                                         #####
-- ##################################################################
-- ##### Aggregation #40 - Premium Movies - Action & Adventure ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Prem_Movies = 0 or acc.Movmt_DTV_Prem_Movies = 1 then -3                         -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when vw.F_CType_Retail_Movies = 1 and vw.F_Genre_Action_Adventure  = 1 then Instance_Duration

                              --Additionally for customers with Movies package who have the (Entertainment Extra+ OR HD package) include = "Retail - ALC / Movies Pack + Ent Extra+"
                          when (acc.Ent_DTV_Prem_Movies = 1 and (acc.Ent_DTV_Pack_Ent_Extra_Plus = 1 or acc.Ent_HD_Sub = 1)) and
                               vw.F_CType_Retail_ALC_Movies_Pack = 1 and vw.F_Genre_Action_Adventure  = 1 then Instance_Duration

                            else 0
                        end
                      ';
set @varSOV =  '
                        (select case when Avg_Daily_Aggr_Viewing_Dur = 0 then null else Avg_Daily_Aggr_Viewing_Dur end
                           from VAggr_03_Account_Metrics_PH2_GENRES mettwo
                          where Aggregation_Id = 5
                            and acc.Account_Number = mettwo.Account_Number)
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '40'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;


-- ##### Aggregation #41 - Premium Movies - Comedy ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Prem_Movies = 0 or acc.Movmt_DTV_Prem_Movies = 1 then -3                         -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when vw.F_CType_Retail_Movies = 1 and vw.F_Genre_Comedy  = 1 then Instance_Duration

                              --Additionally for customers with Movies package who have the (Entertainment Extra+ OR HD package) include = "Retail - ALC / Movies Pack + Ent Extra+"
                          when (acc.Ent_DTV_Prem_Movies = 1 and (acc.Ent_DTV_Pack_Ent_Extra_Plus = 1 or acc.Ent_HD_Sub = 1)) and
                               vw.F_CType_Retail_ALC_Movies_Pack = 1 and vw.F_Genre_Comedy  = 1 then Instance_Duration

                            else 0
                        end
                      ';
set @varSOV =  '
                        (select case when Avg_Daily_Aggr_Viewing_Dur = 0 then null else Avg_Daily_Aggr_Viewing_Dur end
                           from VAggr_03_Account_Metrics_PH2_GENRES mettwo
                          where Aggregation_Id = 5
                            and acc.Account_Number = mettwo.Account_Number)
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '41'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;


-- ##### Aggregation #42 - Premium Movies - Drama & Romance ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Prem_Movies = 0 or acc.Movmt_DTV_Prem_Movies = 1 then -3                         -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when vw.F_CType_Retail_Movies = 1 and vw.F_Genre_Drama_Romance  = 1 then Instance_Duration

                              --Additionally for customers with Movies package who have the (Entertainment Extra+ OR HD package) include = "Retail - ALC / Movies Pack + Ent Extra+"
                          when (acc.Ent_DTV_Prem_Movies = 1 and (acc.Ent_DTV_Pack_Ent_Extra_Plus = 1 or acc.Ent_HD_Sub = 1)) and
                               vw.F_CType_Retail_ALC_Movies_Pack = 1 and vw.F_Genre_Drama_Romance  = 1 then Instance_Duration

                            else 0
                        end
                      ';
set @varSOV =  '
                        (select case when Avg_Daily_Aggr_Viewing_Dur = 0 then null else Avg_Daily_Aggr_Viewing_Dur end
                           from VAggr_03_Account_Metrics_PH2_GENRES mettwo
                          where Aggregation_Id = 5
                            and acc.Account_Number = mettwo.Account_Number)
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '42'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;


-- ##### Aggregation #43 - Premium Movies - Family ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Prem_Movies = 0 or acc.Movmt_DTV_Prem_Movies = 1 then -3                         -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when vw.F_CType_Retail_Movies = 1 and vw.F_Genre_Family  = 1 then Instance_Duration

                              --Additionally for customers with Movies package who have the (Entertainment Extra+ OR HD package) include = "Retail - ALC / Movies Pack + Ent Extra+"
                          when (acc.Ent_DTV_Prem_Movies = 1 and (acc.Ent_DTV_Pack_Ent_Extra_Plus = 1 or acc.Ent_HD_Sub = 1)) and
                               vw.F_CType_Retail_ALC_Movies_Pack = 1 and vw.F_Genre_Family  = 1 then Instance_Duration

                            else 0
                        end
                      ';
set @varSOV =  '
                        (select case when Avg_Daily_Aggr_Viewing_Dur = 0 then null else Avg_Daily_Aggr_Viewing_Dur end
                           from VAggr_03_Account_Metrics_PH2_GENRES mettwo
                          where Aggregation_Id = 5
                            and acc.Account_Number = mettwo.Account_Number)
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '43'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;


-- ##### Aggregation #44 - Premium Movies - Horror & Thriller ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Prem_Movies = 0 or acc.Movmt_DTV_Prem_Movies = 1 then -3                         -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when vw.F_CType_Retail_Movies = 1 and vw.F_Genre_Horror_Thriller  = 1 then Instance_Duration

                              --Additionally for customers with Movies package who have the (Entertainment Extra+ OR HD package) include = "Retail - ALC / Movies Pack + Ent Extra+"
                          when (acc.Ent_DTV_Prem_Movies = 1 and (acc.Ent_DTV_Pack_Ent_Extra_Plus = 1 or acc.Ent_HD_Sub = 1)) and
                               vw.F_CType_Retail_ALC_Movies_Pack = 1 and vw.F_Genre_Horror_Thriller  = 1 then Instance_Duration

                            else 0
                        end
                      ';
set @varSOV =  '
                        (select case when Avg_Daily_Aggr_Viewing_Dur = 0 then null else Avg_Daily_Aggr_Viewing_Dur end
                           from VAggr_03_Account_Metrics_PH2_GENRES mettwo
                          where Aggregation_Id = 5
                            and acc.Account_Number = mettwo.Account_Number)
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '44'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;


-- ##### Aggregation #45 - Premium Movies - SciFi & Fantasy ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Prem_Movies = 0 or acc.Movmt_DTV_Prem_Movies = 1 then -3                         -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when vw.F_CType_Retail_Movies = 1 and vw.F_Genre_SciFi_Fantasy  = 1 then Instance_Duration

                              --Additionally for customers with Movies package who have the (Entertainment Extra+ OR HD package) include = "Retail - ALC / Movies Pack + Ent Extra+"
                          when (acc.Ent_DTV_Prem_Movies = 1 and (acc.Ent_DTV_Pack_Ent_Extra_Plus = 1 or acc.Ent_HD_Sub = 1)) and
                               vw.F_CType_Retail_ALC_Movies_Pack = 1 and vw.F_Genre_SciFi_Fantasy  = 1 then Instance_Duration

                            else 0
                        end
                      ';
set @varSOV =  '
                        (select case when Avg_Daily_Aggr_Viewing_Dur = 0 then null else Avg_Daily_Aggr_Viewing_Dur end
                           from VAggr_03_Account_Metrics_PH2_GENRES mettwo
                          where Aggregation_Id = 5
                            and acc.Account_Number = mettwo.Account_Number)
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '45'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;



-- ##################################################################
-- ##### PREMIUM SPORTS                                         #####
-- ##################################################################
-- ##### Aggregation #50 - Premium Sports - American ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Prem_Sports = 0 or acc.Movmt_DTV_Prem_Sports = 1 then -3                         -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when vw.F_CType_Retail_Sports = 1 and vw.F_Genre_American = 1 then Instance_Duration
                            else 0
                        end
                      ';
set @varSOV =  '
                        (select case when Avg_Daily_Aggr_Viewing_Dur = 0 then null else Avg_Daily_Aggr_Viewing_Dur end
                           from VAggr_03_Account_Metrics_PH2_GENRES mettwo
                          where Aggregation_Id = 6
                            and acc.Account_Number = mettwo.Account_Number)
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '50'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;


-- ##### Aggregation #51 - Premium Sports - Boxing & Wrestling ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Prem_Sports = 0 or acc.Movmt_DTV_Prem_Sports = 1 then -3                         -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when vw.F_CType_Retail_Sports = 1 and vw.F_Genre_Boxing_Wrestling = 1 then Instance_Duration
                            else 0
                        end
                      ';
set @varSOV =  '
                        (select case when Avg_Daily_Aggr_Viewing_Dur = 0 then null else Avg_Daily_Aggr_Viewing_Dur end
                           from VAggr_03_Account_Metrics_PH2_GENRES mettwo
                          where Aggregation_Id = 6
                            and acc.Account_Number = mettwo.Account_Number)
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '51'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;


-- ##### Aggregation #52 - Premium Sports - Cricket ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Prem_Sports = 0 or acc.Movmt_DTV_Prem_Sports = 1 then -3                         -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when vw.F_CType_Retail_Sports = 1 and vw.F_Genre_Cricket = 1 then Instance_Duration
                            else 0
                        end
                      ';
set @varSOV =  '
                        (select case when Avg_Daily_Aggr_Viewing_Dur = 0 then null else Avg_Daily_Aggr_Viewing_Dur end
                           from VAggr_03_Account_Metrics_PH2_GENRES mettwo
                          where Aggregation_Id = 6
                            and acc.Account_Number = mettwo.Account_Number)
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '52'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;


-- ##### Aggregation #53 - Premium Sports - Football ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Prem_Sports = 0 or acc.Movmt_DTV_Prem_Sports = 1 then -3                         -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when vw.F_CType_Retail_Sports = 1 and vw.F_Genre_Football = 1 then Instance_Duration
                            else 0
                        end
                      ';
set @varSOV =  '
                        (select case when Avg_Daily_Aggr_Viewing_Dur = 0 then null else Avg_Daily_Aggr_Viewing_Dur end
                           from VAggr_03_Account_Metrics_PH2_GENRES mettwo
                          where Aggregation_Id = 6
                            and acc.Account_Number = mettwo.Account_Number)
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '53'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;


-- ##### Aggregation #54 - Premium Sports - Golf ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Prem_Sports = 0 or acc.Movmt_DTV_Prem_Sports = 1 then -3                         -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when vw.F_CType_Retail_Sports = 1 and vw.F_Genre_Golf = 1 then Instance_Duration
                            else 0
                        end
                      ';
set @varSOV =  '
                        (select case when Avg_Daily_Aggr_Viewing_Dur = 0 then null else Avg_Daily_Aggr_Viewing_Dur end
                           from VAggr_03_Account_Metrics_PH2_GENRES mettwo
                          where Aggregation_Id = 6
                            and acc.Account_Number = mettwo.Account_Number)
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '54'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;


-- ##### Aggregation #55 - Premium Sports - Motor & Extreme ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Prem_Sports = 0 or acc.Movmt_DTV_Prem_Sports = 1 then -3                         -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when vw.F_CType_Retail_Sports = 1 and vw.F_Genre_Motor_Extreme = 1 then Instance_Duration
                            else 0
                        end
                      ';
set @varSOV =  '
                        (select case when Avg_Daily_Aggr_Viewing_Dur = 0 then null else Avg_Daily_Aggr_Viewing_Dur end
                           from VAggr_03_Account_Metrics_PH2_GENRES mettwo
                          where Aggregation_Id = 6
                            and acc.Account_Number = mettwo.Account_Number)
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '55'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;


-- ##### Aggregation #56 - Premium Sports - Rugby ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Prem_Sports = 0 or acc.Movmt_DTV_Prem_Sports = 1 then -3                         -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when vw.F_CType_Retail_Sports = 1 and vw.F_Genre_Rugby = 1 then Instance_Duration
                            else 0
                        end
                      ';
set @varSOV =  '
                        (select case when Avg_Daily_Aggr_Viewing_Dur = 0 then null else Avg_Daily_Aggr_Viewing_Dur end
                           from VAggr_03_Account_Metrics_PH2_GENRES mettwo
                          where Aggregation_Id = 6
                            and acc.Account_Number = mettwo.Account_Number)
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '56'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;


-- ##### Aggregation #57 - Premium Sports - Tennis ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Prem_Sports = 0 or acc.Movmt_DTV_Prem_Sports = 1 then -3                         -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when vw.F_CType_Retail_Sports = 1 and vw.F_Genre_Tennis = 1 then Instance_Duration
                            else 0
                        end
                      ';
set @varSOV =  '
                        (select case when Avg_Daily_Aggr_Viewing_Dur = 0 then null else Avg_Daily_Aggr_Viewing_Dur end
                           from VAggr_03_Account_Metrics_PH2_GENRES mettwo
                          where Aggregation_Id = 6
                            and acc.Account_Number = mettwo.Account_Number)
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '57'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;


-- ##### Aggregation #58 - Premium Sports - Niche Sport ####
set @varAccStatus = '
                        case
                          when acc.Ent_DTV_Prem_Sports = 0 or acc.Movmt_DTV_Prem_Sports = 1 then -3                         -- Not eligible
                            else 1
                        end
                      ';

set @varTotalAggrVwDur = '
                        case
                          when vw.F_CType_Retail_Sports = 1 and vw.F_Genre_Niche_Sport = 1 then Instance_Duration
                            else 0
                        end
                      ';
set @varSOV =  '
                        (select case when Avg_Daily_Aggr_Viewing_Dur = 0 then null else Avg_Daily_Aggr_Viewing_Dur end
                           from VAggr_03_Account_Metrics_PH2_GENRES mettwo
                          where Aggregation_Id = 6
                            and acc.Account_Number = mettwo.Account_Number)
                  ';
execute(replace(replace(replace(replace(@varSql, '##^3^##', @varAccStatus), '##^2^##', '58'), '##^1^##', @varTotalAggrVwDur),'##^0^##', @varSOV));
commit;



  -- ##############################################################################################################
  -- ##############################################################################################################













