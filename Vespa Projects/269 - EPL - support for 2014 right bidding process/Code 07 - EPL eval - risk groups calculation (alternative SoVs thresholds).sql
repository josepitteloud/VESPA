/*###############################################################################
# Created on:   29/04/2014
# Created by:   Sebastian Bednaszynski(SBE)
# Description:  EPL rights project - risk groups calculation based on provided rules
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 15/07/2014  SBE   Initial version - based on
#                   "Code 07 - EPL eval - risk groups calculation.sql" on 15/07/2014
#
###############################################################################*/


  -- ##############################################################################################################
  -- ##############################################################################################################
if object_id('EPL_07_Risk_Groups__Sports_SoV_30p') is not null then drop table EPL_07_Risk_Groups__Sports_SoV_30p end if;
create table EPL_07_Risk_Groups__Sports_SoV_30p (
    Pk_Identifier                           bigint            identity,
    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name(),

      -- Account
    Account_Number                          varchar(20)       null      default null,
    Period                                  tinyint           null      default 0,
    Sports_Package                          varchar(20)       null      default 'No Sky Sports',
    Risk_Segment_1                          varchar(50)       null      default 'No Sky Sports',    -- Basic risk group - Sky loses EPL in full
    Risk_Segment_2                          varchar(50)       null      default 'No Sky Sports',    -- Basic risk group - Sky loses majority of EPL
    Risk_Segment_3                          varchar(50)       null      default 'No Sky Sports',    -- EPL risk group - Sky loses EPL in full
    Risk_Segment_4                          varchar(50)       null      default 'No Sky Sports',    -- EPL risk group - Sky loses majority of EPL
    Risk_Segment_5                          varchar(50)       null      default 'No Sky Sports',
    Risk_Segment_6                          varchar(50)       null      default 'No Sky Sports',
    Risk_Segment_7                          varchar(50)       null      default 'No Sky Sports',
    Risk_Segment_8                          varchar(50)       null      default 'No Sky Sports',
    Risk_Segment_9                          varchar(50)       null      default 'No Sky Sports',
);
create        hg   index idx01 on EPL_07_Risk_Groups__Sports_SoV_30p(Account_Number);
create        lf   index idx02 on EPL_07_Risk_Groups__Sports_SoV_30p(Period);
create unique hg   index idx03 on EPL_07_Risk_Groups__Sports_SoV_30p(Account_Number, Period);
create        lf   index idx04 on EPL_07_Risk_Groups__Sports_SoV_30p(Sports_Package);
grant select on EPL_07_Risk_Groups__Sports_SoV_30p to vespa_group_low_security;


insert into EPL_07_Risk_Groups__Sports_SoV_30p
      (Account_Number, Period, Sports_Package)
select
    Account_Number,
    Period,
    case
      when Prem_Sports > 0 then 'Sky Sports'
        else 'No Sky Sports'
    end
  from EPL_04_Profiling_Variables
 where Period = 1;
commit;



  -- ##############################################################################################################
  -- ##### Create table and pull existing information from the profiling analysis                             #####
  -- ##############################################################################################################
  -- Basic risk groups
update EPL_07_Risk_Groups__Sports_SoV_30p base
   set base.Risk_Segment_1  =                                               -- Basic risk group - Sky loses EPL In full
      case
        when det.EPL_SoSV in ('High')             and det.Sport_SoV_30p in ('High', 'Very high')                                          then 'Risk group 1'
        when det.EPL_SoSV in ('Low', 'Medium')    and det.Sport_SoV_30p in ('High', 'Very high')    and det.EPL_SOC in ('Medium', 'High') then 'Risk group 2'
        when det.EPL_SoSV in ('Medium', 'High')   and det.Sport_SoV_30p in ('Low', 'Medium')                                              then 'Risk group 3'
        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV_30p in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 'Risk group 4'
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV_30p in ('Low', 'Medium')                                              then 'Risk group 5'
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV_30p in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 'Risk group 5'
          else 'Excluded'
      end,

        base.Risk_Segment_2  =                                               -- Basic risk group - Sky loses majority of EPL
      case
        when det.EPL_SoSV in ('High')             and det.Sport_SoV_30p in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 'Risk group 1'

        when det.EPL_SoSV in ('High')             and det.Sport_SoV_30p in ('Low', 'Medium')        and det.EPL_SOC in ('Low')            then 'Risk group 2'
        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV_30p in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 'Risk group 2'

        when det.EPL_SoSV in ('Medium', 'High')                                                 and det.EPL_SOC in ('Medium', 'High') then 'Risk group 3'

        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV_30p in ('Low', 'Medium')        and det.EPL_SOC in ('Low')            then 'Risk group 4'
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV_30p in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 'Risk group 4'

        when det.EPL_SoSV in ('Low')              and det.Sport_SoV_30p in ('Low', 'Medium')                                              then 'Risk group 5'
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV_30p in ('High', 'Very high')    and det.EPL_SOC in ('Medium', 'High') then 'Risk group 5'
      end
  from EPL_04_Eng_Matrix det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and base.Sports_Package = 'Sky Sports'
   and det.Metric = 'Overall';
commit;


  -- EPL risk groups
update EPL_07_Risk_Groups__Sports_SoV_30p base
   set base.Risk_Segment_3  =                                               -- EPL risk group - Sky loses EPL In full
      case

          -- #######################################################################################################
          -- ##### Risk group 1 #####
        when base.Risk_Segment_1 in ('Risk group 1') then
            case
            -- ===== BT Sport =====
                when prof.BT_Sport_Viewier = 'Yes' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 'No change'
                when prof.BT_Sport_Viewier = 'Yes' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                     then 'No change'
                when prof.BT_Sport_Viewier = 'Yes' and eng.Movies_SOV in ('High')                                                             then 'No change'
                when prof.BT_Sport_Viewier = 'Yes' and prof.Number_Of_Sky_Products_GO_OD >= 6                                                 then 'No change'
                when prof.BT_Sport_Viewier = 'Yes' and cl_eng.CL_SOC in ('High')                                                              then 'No change'
                when prof.BT_Sport_Viewier = 'Yes' and prof.Value_Segment = 'F) Unstable'                                                     then 'Churn risk'
                when prof.BT_Sport_Viewier = 'Yes' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 'Churn risk'
                when prof.BT_Sport_Viewier = 'Yes'                                                                                            then 'Churn risk'

            -- ===== Non-BT Sport =====
                when prof.BT_Sport_Viewier = 'No' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 'No change'
                when prof.BT_Sport_Viewier = 'No' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                      then 'Downgrade risk'
                when prof.BT_Sport_Viewier = 'No' and eng.Movies_SOV in ('High')                                                              then 'Downgrade risk'
                when prof.BT_Sport_Viewier = 'No' and prof.Number_Of_Sky_Products_GO_OD >= 6                                                  then 'Downgrade risk'
                when prof.BT_Sport_Viewier = 'No' and cl_eng.CL_SOC in ('High')                                                               then 'Churn risk'
                when prof.BT_Sport_Viewier = 'No' and prof.Value_Segment = 'F) Unstable'                                                      then 'Churn risk'
                when prof.BT_Sport_Viewier = 'No' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 'Churn risk'
                when prof.BT_Sport_Viewier = 'No'                                                                                             then 'Churn risk'
            end

          -- #######################################################################################################
          -- ##### Risk group 2 #####
        when base.Risk_Segment_1 in ('Risk group 2') then
            case
            -- ===== BT Sport =====
                when prof.BT_Sport_Viewier = 'Yes' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 'No change'
                when prof.BT_Sport_Viewier = 'Yes' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                     then 'No change'
                when prof.BT_Sport_Viewier = 'Yes' and eng.Sky_Branded_Channels in ('High')                                                   then 'No change'
                when prof.BT_Sport_Viewier = 'Yes' and eng.Movies_SOV in ('High')                                                             then 'No change'
                when prof.BT_Sport_Viewier = 'Yes' and prof.Number_Of_Sky_Products_GO_OD >= 6                                                 then 'No change'
                when prof.BT_Sport_Viewier = 'Yes' and cl_eng.CL_SOC in ('High')                                                              then 'No change'
                when prof.BT_Sport_Viewier = 'Yes' and prof.Value_Segment = 'F) Unstable'                                                     then 'Churn risk'
                when prof.BT_Sport_Viewier = 'Yes' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 'Churn risk'
                when prof.BT_Sport_Viewier = 'Yes'                                                                                            then 'Churn risk'

            -- ===== Non-BT Sport =====
                when prof.BT_Sport_Viewier = 'No' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 'No change'
                when prof.BT_Sport_Viewier = 'No' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                      then 'Downgrade risk'
                when prof.BT_Sport_Viewier = 'No' and eng.Sky_Branded_Channels in ('High')                                                    then 'Downgrade risk'
                when prof.BT_Sport_Viewier = 'No' and eng.Movies_SOV in ('High')                                                              then 'Downgrade risk'
                when prof.BT_Sport_Viewier = 'No' and prof.Number_Of_Sky_Products_GO_OD >= 6                                                  then 'Downgrade risk'
                when prof.BT_Sport_Viewier = 'No' and cl_eng.CL_SOC in ('High')                                                               then 'Churn risk'
                when prof.BT_Sport_Viewier = 'No' and prof.Value_Segment = 'F) Unstable'                                                      then 'Churn risk'
                when prof.BT_Sport_Viewier = 'No' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 'Churn risk'
                when prof.BT_Sport_Viewier = 'No'                                                                                             then 'Churn risk'
            end

          -- #######################################################################################################
          -- ##### Risk group 3 #####
        when base.Risk_Segment_1 in ('Risk group 3') then
            case
            -- ===== BT Sport =====
                when prof.BT_Sport_Viewier = 'Yes' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 'No change'
                when prof.BT_Sport_Viewier = 'Yes' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                     then 'No change'
                when prof.BT_Sport_Viewier = 'Yes' and eng.Sky_Branded_Channels in ('High')                                                   then 'No change'
                when prof.BT_Sport_Viewier = 'Yes' and eng.Movies_SOV in ('High')                                                             then 'No change'
                when prof.BT_Sport_Viewier = 'Yes' and prof.Number_Of_Sky_Products_GO_OD >= 6                                                 then 'No change'
                when prof.BT_Sport_Viewier = 'Yes' and cl_eng.CL_SOC in ('High')                                                              then 'No change'
                when prof.BT_Sport_Viewier = 'Yes' and prof.Value_Segment = 'F) Unstable'                                                     then 'Churn risk'
                when prof.BT_Sport_Viewier = 'Yes' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 'Churn risk'
                when prof.BT_Sport_Viewier = 'Yes'                                                                                            then 'Churn risk'

            -- ===== Non-BT Sport =====
                when prof.BT_Sport_Viewier = 'No' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 'No change'
                when prof.BT_Sport_Viewier = 'No' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                      then 'Downgrade risk'
                when prof.BT_Sport_Viewier = 'No' and eng.Sky_Branded_Channels in ('High')                                                    then 'Downgrade risk'
                when prof.BT_Sport_Viewier = 'No' and eng.Movies_SOV in ('High')                                                              then 'Downgrade risk'
                when prof.BT_Sport_Viewier = 'No' and prof.Number_Of_Sky_Products_GO_OD >= 6                                                  then 'Downgrade risk'
                when prof.BT_Sport_Viewier = 'No' and cl_eng.CL_SOC in ('High')                                                               then 'Downgrade risk'
                when prof.BT_Sport_Viewier = 'No' and prof.Value_Segment = 'F) Unstable'                                                      then 'Churn risk'
                when prof.BT_Sport_Viewier = 'No' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 'Churn risk'
                when prof.BT_Sport_Viewier = 'No'                                                                                             then 'Churn risk'
            end

          -- #######################################################################################################
          -- ##### Risk group 4 #####
        when base.Risk_Segment_1 in ('Risk group 4') then
            case
            -- ===== BT Sport =====
                when prof.BT_Sport_Viewier = 'Yes' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 'No change'
                when prof.BT_Sport_Viewier = 'Yes' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                     then 'No change'
                when prof.BT_Sport_Viewier = 'Yes' and eng.Movies_SOV in ('High')                                                             then 'No change'
                when prof.BT_Sport_Viewier = 'Yes' and prof.Number_Of_Sky_Products_GO_OD >= 6                                                 then 'No change'
                when prof.BT_Sport_Viewier = 'Yes' and cl_eng.CL_SOC in ('High')                                                              then 'No change'
                when prof.BT_Sport_Viewier = 'Yes' and prof.Value_Segment = 'F) Unstable'                                                     then 'Downgrade risk'
                when prof.BT_Sport_Viewier = 'Yes' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 'Downgrade risk'
                when prof.BT_Sport_Viewier = 'Yes'                                                                                            then 'No change'

            -- ===== Non-BT Sport =====
                when prof.BT_Sport_Viewier = 'No' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 'No change'
                when prof.BT_Sport_Viewier = 'No' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                      then 'No change'
                when prof.BT_Sport_Viewier = 'No' and eng.Movies_SOV in ('High')                                                              then 'No change'
                when prof.BT_Sport_Viewier = 'No' and prof.Number_Of_Sky_Products_GO_OD >= 6                                                  then 'No change'
                when prof.BT_Sport_Viewier = 'No' and cl_eng.CL_SOC in ('High')                                                               then 'Downgrade risk'
                when prof.BT_Sport_Viewier = 'No' and prof.Value_Segment = 'F) Unstable'                                                      then 'Downgrade risk'
                when prof.BT_Sport_Viewier = 'No' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 'Downgrade risk'
                when prof.BT_Sport_Viewier = 'No'                                                                                             then 'No change'
            end

          -- #######################################################################################################
          -- ##### Risk group 5 #####
        when base.Risk_Segment_1 in ('Risk group 5') then
            case

                when                                  eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                      then 'No change'
                when                                  eng.Movies_SOV in ('High')                                                              then 'No change'
                when                                  prof.Number_Of_Sky_Products_GO_OD >= 6                                                  then 'No change'
                when                                  cl_eng.CL_SOC in ('High')                                                               then 'No change'
                when                                  prof.Value_Segment = 'F) Unstable'                                                      then 'Downgrade risk'
                when                                  prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 'Downgrade risk'
                  else                                                                                                                             'No change'
            end

          -- #######################################################################################################
          -- ##### Excluded #####
          else 'Excluded'

      end,


       base.Risk_Segment_4  =                                               -- EPL risk group - Sky loses majority of EPL
      case

          -- #######################################################################################################
          -- ##### Risk group 1 #####
        when base.Risk_Segment_2 in ('Risk group 1') then
            case
            -- ===== BT Sport =====
                when prof.BT_Sport_Viewier = 'Yes' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 'No change'
                when prof.BT_Sport_Viewier = 'Yes' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                     then 'No change'
                when prof.BT_Sport_Viewier = 'Yes' and eng.Movies_SOV in ('High')                                                             then 'No change'
                when prof.BT_Sport_Viewier = 'Yes' and prof.Number_Of_Sky_Products_GO_OD >= 6                                                 then 'No change'
                when prof.BT_Sport_Viewier = 'Yes' and cl_eng.CL_SOC in ('High')                                                              then 'No change'
                when prof.BT_Sport_Viewier = 'Yes' and prof.Value_Segment = 'F) Unstable'                                                     then 'Downgrade risk'
                when prof.BT_Sport_Viewier = 'Yes' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 'Downgrade risk'
                when prof.BT_Sport_Viewier = 'Yes'                                                                                            then 'Downgrade risk'

            -- ===== Non-BT Sport =====
                when prof.BT_Sport_Viewier = 'No' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 'Downgrade risk'
                when prof.BT_Sport_Viewier = 'No' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                      then 'Downgrade risk'
                when prof.BT_Sport_Viewier = 'No' and eng.Movies_SOV in ('High')                                                              then 'Downgrade risk'
                when prof.BT_Sport_Viewier = 'No' and prof.Number_Of_Sky_Products_GO_OD >= 6                                                  then 'Downgrade risk'
                when prof.BT_Sport_Viewier = 'No' and cl_eng.CL_SOC in ('High')                                                               then 'Churn risk'
                when prof.BT_Sport_Viewier = 'No' and prof.Value_Segment = 'F) Unstable'                                                      then 'Churn risk'
                when prof.BT_Sport_Viewier = 'No' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 'Churn risk'
                when prof.BT_Sport_Viewier = 'No'                                                                                             then 'Downgrade risk'
            end

          -- #######################################################################################################
          -- ##### Risk group 2 #####
        when base.Risk_Segment_2 in ('Risk group 2') then
            case
            -- ===== BT Sport =====
                when prof.BT_Sport_Viewier = 'Yes' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 'No change'
                when prof.BT_Sport_Viewier = 'Yes' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                     then 'No change'
                when prof.BT_Sport_Viewier = 'Yes' and eng.Movies_SOV in ('High')                                                             then 'No change'
                when prof.BT_Sport_Viewier = 'Yes' and prof.Number_Of_Sky_Products_GO_OD >= 6                                                 then 'No change'
                when prof.BT_Sport_Viewier = 'Yes' and cl_eng.CL_SOC in ('High')                                                              then 'No change'
                when prof.BT_Sport_Viewier = 'Yes' and prof.Value_Segment = 'F) Unstable'                                                     then 'Churn risk'
                when prof.BT_Sport_Viewier = 'Yes' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 'Churn risk'
                when prof.BT_Sport_Viewier = 'Yes'                                                                                            then 'No change'

            -- ===== Non-BT Sport =====
                when prof.BT_Sport_Viewier = 'No' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 'Downgrade risk'
                when prof.BT_Sport_Viewier = 'No' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                      then 'Downgrade risk'
                when prof.BT_Sport_Viewier = 'No' and eng.Movies_SOV in ('High')                                                              then 'Downgrade risk'
                when prof.BT_Sport_Viewier = 'No' and prof.Number_Of_Sky_Products_GO_OD >= 6                                                  then 'Downgrade risk'
                when prof.BT_Sport_Viewier = 'No' and cl_eng.CL_SOC in ('High')                                                               then 'Churn risk'
                when prof.BT_Sport_Viewier = 'No' and prof.Value_Segment = 'F) Unstable'                                                      then 'Churn risk'
                when prof.BT_Sport_Viewier = 'No' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 'Churn risk'
                when prof.BT_Sport_Viewier = 'No'                                                                                             then 'No change'
            end

          -- #######################################################################################################
          -- ##### Risk group 3 #####
        when base.Risk_Segment_2 in ('Risk group 3') then
            case
            -- ===== BT Sport =====
                when prof.BT_Sport_Viewier = 'Yes' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 'No change'
                when prof.BT_Sport_Viewier = 'Yes' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                     then 'No change'
                when prof.BT_Sport_Viewier = 'Yes' and eng.Movies_SOV in ('High')                                                             then 'No change'
                when prof.BT_Sport_Viewier = 'Yes' and prof.Number_Of_Sky_Products_GO_OD >= 6                                                 then 'No change'
                when prof.BT_Sport_Viewier = 'Yes' and cl_eng.CL_SOC in ('High')                                                              then 'No change'
                when prof.BT_Sport_Viewier = 'Yes' and prof.Value_Segment = 'F) Unstable'                                                     then 'Churn risk'
                when prof.BT_Sport_Viewier = 'Yes' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 'Downgrade risk'
                when prof.BT_Sport_Viewier = 'Yes'                                                                                            then 'Churn risk'

            -- ===== Non-BT Sport =====
                when prof.BT_Sport_Viewier = 'No' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 'No change'
                when prof.BT_Sport_Viewier = 'No' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                      then 'No change'
                when prof.BT_Sport_Viewier = 'No' and eng.Movies_SOV in ('High')                                                              then 'Downgrade risk'
                when prof.BT_Sport_Viewier = 'No' and prof.Number_Of_Sky_Products_GO_OD >= 6                                                  then 'Downgrade risk'
                when prof.BT_Sport_Viewier = 'No' and cl_eng.CL_SOC in ('High')                                                               then 'Downgrade risk'
                when prof.BT_Sport_Viewier = 'No' and prof.Value_Segment = 'F) Unstable'                                                      then 'Downgrade risk'
                when prof.BT_Sport_Viewier = 'No' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 'Downgrade risk'
                when prof.BT_Sport_Viewier = 'No'                                                                                             then 'Churn risk'
            end

          -- #######################################################################################################
          -- ##### Risk group 4 #####
        when base.Risk_Segment_2 in ('Risk group 4') then
            case
            -- ===== BT Sport =====
                when prof.BT_Sport_Viewier = 'Yes' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 'No change'
                when prof.BT_Sport_Viewier = 'Yes' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                     then 'No change'
                when prof.BT_Sport_Viewier = 'Yes' and eng.Movies_SOV in ('High')                                                             then 'No change'
                when prof.BT_Sport_Viewier = 'Yes' and prof.Number_Of_Sky_Products_GO_OD >= 6                                                 then 'No change'
                when prof.BT_Sport_Viewier = 'Yes' and cl_eng.CL_SOC in ('High')                                                              then 'No change'
                when prof.BT_Sport_Viewier = 'Yes' and prof.Value_Segment = 'F) Unstable'                                                     then 'Downgrade risk'
                when prof.BT_Sport_Viewier = 'Yes' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 'Downgrade risk'
                when prof.BT_Sport_Viewier = 'Yes'                                                                                            then 'No change'

            -- ===== Non-BT Sport =====
                when prof.BT_Sport_Viewier = 'No' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 'No change'
                when prof.BT_Sport_Viewier = 'No' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                      then 'No change'
                when prof.BT_Sport_Viewier = 'No' and eng.Movies_SOV in ('High')                                                              then 'No change'
                when prof.BT_Sport_Viewier = 'No' and prof.Number_Of_Sky_Products_GO_OD >= 6                                                  then 'No change'
                when prof.BT_Sport_Viewier = 'No' and cl_eng.CL_SOC in ('High')                                                               then 'Downgrade risk'
                when prof.BT_Sport_Viewier = 'No' and prof.Value_Segment = 'F) Unstable'                                                      then 'Downgrade risk'
                when prof.BT_Sport_Viewier = 'No' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 'Downgrade risk'
                when prof.BT_Sport_Viewier = 'No'                                                                                             then 'No change'
            end

          -- #######################################################################################################
          -- ##### Risk group 5 #####
        when base.Risk_Segment_2 in ('Risk group 5') then
            case

                when                                  eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                      then 'No change'
                when                                  eng.Movies_SOV in ('High')                                                              then 'No change'
                when                                  prof.Number_Of_Sky_Products_GO_OD >= 6                                                  then 'No change'
                when                                  cl_eng.CL_SOC in ('High')                                                               then 'No change'
                when                                  prof.Value_Segment = 'F) Unstable'                                                      then 'Downgrade risk'
                when                                  prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 'Downgrade risk'
                  else                                                                                                                             'No change'
            end

          -- #######################################################################################################
          -- ##### Excluded #####
          else 'Excluded'

      end

  from EPL_04_Eng_Matrix eng,
       EPL_04_Profiling_Variables prof,
       EPL_54_CL_Eng_Matrix cl_eng
 where base.Account_Number = eng.Account_Number
   and base.Period = eng.Period
   and base.Sports_Package = 'Sky Sports'
   and eng.Metric = 'Overall'
   and base.Account_Number = prof.Account_Number
   and base.Period = prof.Period
   and base.Account_Number = cl_eng.Account_Number
   and base.Period = cl_eng.Period
   and cl_eng.Metric = 'Overall';
commit;


  -- ##############################################################################################################
  -- Getting counts - scaled & unscaled

  -- All EPL lost
select
        Risk_Segment_3,
        count(*) as Unscaled_Volume,
        sum(Scaling_Weight) as Scaled_Volume
  from EPL_07_Risk_Groups__Sports_SoV_30p a left join EPL_05_Scaling_Weights b  on a.Account_Number = b.Account_Number
                                                               and a.Period = b.Period
 group by Risk_Segment_3
 order by Risk_Segment_3;

  -- Majority EPL lost
select
        Risk_Segment_4,
        count(*) as Unscaled_Volume,
        sum(Scaling_Weight) as Scaled_Volume
  from EPL_07_Risk_Groups__Sports_SoV_30p a left join EPL_05_Scaling_Weights b  on a.Account_Number = b.Account_Number
                                                               and a.Period = b.Period
 group by Risk_Segment_4
 order by Risk_Segment_4;



  -- ##############################################################################################################
  -- ##############################################################################################################







