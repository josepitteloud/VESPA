/*################################################################################
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
# 29/04/2014  SBE   Initial version
# 03/07/2014  SBE   Revised rules implemented (v2)
#                     Added: Sky Movies, Sky Atlantic - num complete programmes,
#                            extended number of Sky products
# 09/07/2014  SBE   Revised rules implemented (v3)
#                     Added: CL engagement
# 10/07/2014  SBE   Revised rules implemented (v4)
#                     Latent groups replaced with actual outcomes
# 15/07/2014  SBE   Revised rules implemented (v5)
#                     Additional dimension added for Risk Groups 2 & 3 (All EPL lost)
# 21/07/2014  SBE   Revised rules implemented (v6)
#                     SIG level removed from Risk Group 1 (all EPL lost only)
#                     Risk Group 5 - all outcomes are "No Change" now
# 14/08/2014  SBE   Number of products EXCLUDE broadband (v7)
# 18/08/2014  SBE   Changed outcomes for High CL engagement (v8)
# 04/11/2014  SBE   Tree simplified, SIG layer removed, BT viewer/non-viewer removed (v9)
# 11/11/2014  SBE   SIGs added back in and list revised (v10)
# 13/11/2014  SBE   Tree revised, risk groups removed, top three variables changed (v11)
# 26/11/2014  SBE   FSS layer added and definitions & thresholds changed for some variables (v12)
#
###############################################################################*/


  -- ##############################################################################################################
  -- ##############################################################################################################
if object_id('EPL_07_Risk_Groups') is not null then drop table EPL_07_Risk_Groups end if;
create table EPL_07_Risk_Groups (
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
);
create        hg   index idx01 on EPL_07_Risk_Groups(Account_Number);
create        lf   index idx02 on EPL_07_Risk_Groups(Period);
create unique hg   index idx03 on EPL_07_Risk_Groups(Account_Number, Period);
create        lf   index idx04 on EPL_07_Risk_Groups(Sports_Package);
grant select on EPL_07_Risk_Groups to vespa_group_low_security;


insert into EPL_07_Risk_Groups
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
update EPL_07_Risk_Groups base
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
  from EPL_04_Eng_Matrix det,
       EPL_04_Profiling_Variables prof
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and base.Sports_Package = 'Sky Sports'
   and det.Metric = 'Overall'
   and det.Account_Number = prof.Account_Number
   and det.Period = prof.Period;
commit;



-- This view take information from fields updated above AND below, needs to be updated only once though.
create or replace view EPL_07_Risk_Groups_View as
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

    from EPL_07_Risk_Groups a;
commit;


  -- EPL risk groups
update EPL_07_Risk_Groups a
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
                when eng.Sport_SoV in ('High', 'Very high')                             then 110
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
                when eng.Sport_SoV in ('High', 'Very high')                             then 310
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
                when eng.Sport_SoV in ('High', 'Very high')                             then 510
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

  from EPL_07_Risk_Groups_View base,
       EPL_04_Eng_Matrix eng,
       EPL_04_Profiling_Variables prof,
       EPL_54_CL_Eng_Matrix cl_eng

 where a.Account_Number = base.Account_Number
   and a.Period = base.Period
   and a.Sports_Package = 'Sky Sports'

   and a.Account_Number = eng.Account_Number
   and a.Period = eng.Period
   and eng.Metric = 'Overall'

   and a.Account_Number = prof.Account_Number
   and a.Period = prof.Period

   and a.Account_Number = cl_eng.Account_Number
   and a.Period = cl_eng.Period
   and cl_eng.Metric = 'Overall';
commit;





-- Store version (change ? to the current tree version)
drop table EPL_07_Risk_Groups_v?;
select *
  into EPL_07_Risk_Groups_v?
  from EPL_07_Risk_Groups_View;
commit;

alter table EPL_07_Risk_Groups_v? rename xRisk_Segment_1 to Risk_Segment_1;
-- alter table EPL_07_Risk_Groups_v? rename xRisk_Segment_2 to Risk_Segment_2;
alter table EPL_07_Risk_Groups_v? rename xRisk_Segment_3 to Risk_Segment_3;
-- alter table EPL_07_Risk_Groups_v? rename xRisk_Segment_4 to Risk_Segment_4;





  -- ##############################################################################################################
  -- Getting counts - scaled & unscaled
  -- DATA FOR SPREADSHEET LOOKUP (TREES)
  -- All
select Risk_Segment_1 as Risk_Segment, count(*) as Unscaled_Volume, sum(Scaling_Weight) as Scaled_Volume
  from EPL_07_Risk_Groups a left join EPL_05_Scaling_Weights b  on a.Account_Number = b.Account_Number
                                                               and a.Period = b.Period
group by Risk_Segment_1
union
select Risk_Segment_3 as Risk_Segment, count(*) as Unscaled_Volume, sum(Scaling_Weight) as Scaled_Volume
  from EPL_07_Risk_Groups a left join EPL_05_Scaling_Weights b  on a.Account_Number = b.Account_Number
                                                               and a.Period = b.Period
group by Risk_Segment_3
order by 1;




-- ##### TT customers only #####
select Risk_Segment_1 as Risk_Segment, count(*) as Unscaled_Volume, sum(Scaling_Weight) as Scaled_Volume
  from EPL_07_Risk_Groups a left join EPL_05_Scaling_Weights b  on a.Account_Number = b.Account_Number
                                                               and a.Period = b.Period,
       EPL_04_Profiling_Variables c
 where a.Account_Number = c.Account_Number
   and a.Period = c.Period
   and c.Prem_Movies > 0              -- "=0" for DS
   and c.Prem_Sports > 0              -- ">0" for DS
group by Risk_Segment_1
union
select Risk_Segment_3 as Risk_Segment, count(*) as Unscaled_Volume, sum(Scaling_Weight) as Scaled_Volume
  from EPL_07_Risk_Groups a left join EPL_05_Scaling_Weights b  on a.Account_Number = b.Account_Number
                                                               and a.Period = b.Period,
       EPL_04_Profiling_Variables c
 where a.Account_Number = c.Account_Number
   and a.Period = c.Period
   and c.Prem_Movies > 0              -- "=0" for DS
   and c.Prem_Sports > 0              -- ">0" for DS
group by Risk_Segment_3
order by 1;




-- ##### DS customers only #####
select Risk_Segment_1 as Risk_Segment, count(*) as Unscaled_Volume, sum(Scaling_Weight) as Scaled_Volume
  from EPL_07_Risk_Groups a left join EPL_05_Scaling_Weights b  on a.Account_Number = b.Account_Number
                                                               and a.Period = b.Period,
       EPL_04_Profiling_Variables c
 where a.Account_Number = c.Account_Number
   and a.Period = c.Period
   and c.Prem_Movies = 0              -- "=0" for DS
   and c.Prem_Sports > 0              -- ">0" for DS
group by Risk_Segment_1
union
select Risk_Segment_3 as Risk_Segment, count(*) as Unscaled_Volume, sum(Scaling_Weight) as Scaled_Volume
  from EPL_07_Risk_Groups a left join EPL_05_Scaling_Weights b  on a.Account_Number = b.Account_Number
                                                               and a.Period = b.Period,
       EPL_04_Profiling_Variables c
 where a.Account_Number = c.Account_Number
   and a.Period = c.Period
   and c.Prem_Movies = 0              -- "=0" for DS
   and c.Prem_Sports > 0              -- ">0" for DS
group by Risk_Segment_3
order by 1;




  -- ##### Interactive tree ######
if object_id('EPL_12_Results_Tree') is not null then drop table EPL_12_Results_Tree end if;
select
      trim(case
             when Prem_Movies > 0 and Prem_Sports > 0 then 'Top Tier'
               else 'Dual Sports'
           end) as TV_Package,
      trim(case
             when EPL_SoSV in ('High', 'Low', 'Medium', 'Did not watch') then EPL_SoSV
               else 'Other'
           end) as EPL_SoSV,
      trim(case
            when EPL_SOC in ('Medium', 'High') then 'High'
            when EPL_SOC in ('Low') then 'Low'
              else 'Other'
           end) as EPL_SOC,
      trim(case
             when EPL_SoSV = 'High' then Sports_Segment_SIG
             when EPL_SoSV = 'Medium' then Sports_Segment_SIG_v1
             when EPL_SoSV = 'Low' then Sports_Segment_SIG_v2
               else Sports_Segment_SIG
           end) as Sports_Segment_SIG,
      FSS,
      trim(case
             when Key_Pay_Entertainment_Avg_DV = 'High' then 'High'
               else 'Other'
           end) Key_Pay_Entertainment_Avg_DV,
      trim(case
             when Sky_Sports_News_Avg_DV = 'High' then 'High'
               else 'Other'
           end) Sky_Sports_News_Avg_DV,
      trim(case
             when Movies_Avg_DV = 'High' then 'High'
               else 'Other'
           end) Movies_Avg_DV,
      trim(case
             when Number_Of_Sky_Products_No_DTV >= 4 then 'High'
               else 'Other'
           end) Number_Of_Sky_Products,
      trim(case
             when Sport_SoV in ('High', 'Very high') then 'High'
               else 'Other'
           end) Sport_SoV,
      count(*) as Accounts_Unscaled,
      sum(Scaling_Weight) as Accounts_Scaled
  into EPL_12_Results_Tree
  from EPL_04_Eng_Matrix eng,
       EPL_04_Profiling_Variables prof,
       EPL_05_Scaling_Weights b
 where eng.Account_Number = prof.Account_Number
   and eng.Period = prof.Period
   and eng.Account_Number = b.Account_Number
   and eng.Period = b.Period
   and eng.Metric = 'Overall'
   and prof.Prem_Sports > 0
 group by
        TV_Package,
        EPL_SoSV,
        EPL_SOC,
        Sports_Segment_SIG,
        FSS,
        Key_Pay_Entertainment_Avg_DV,
        Sky_Sports_News_Avg_DV,
        Movies_Avg_DV,
        Number_Of_Sky_Products,
        Sport_SoV;
commit;






  -- COUNTS - SPREADSHEET (sheet HL volumes)
  -- All EPL lost
select
        xRisk_Segment_3,
        count(*) as Unscaled_Volume,
        sum(Scaling_Weight) as Scaled_Volume
  from EPL_07_Risk_Groups_View a left join EPL_05_Scaling_Weights b  on a.Account_Number = b.Account_Number
                                                                    and a.Period = b.Period
 group by xRisk_Segment_3
 order by xRisk_Segment_3;



  -- COUNTS - SPREADSHEET (Leaf sizes)
  -- All EPL lost
select
        'All EPL lost' as Scenario,
        xRisk_Segment_1 as Risk_Group,
        xRisk_Segment_3 as Segment,
        count(*) as Unscaled_Volume,
        sum(Scaling_Weight) as Scaled_Volume
  from EPL_07_Risk_Groups_View a left join EPL_05_Scaling_Weights b  on a.Account_Number = b.Account_Number
                                                                    and a.Period = b.Period
 group by xRisk_Segment_1, xRisk_Segment_3
 order by 1, 2, 3;



 -- ##### EXTRAS #####
 -- Breakdown by premiums
select
      'All EPL lost' as Scenario,
      case
        when c.Prem_Movies > 0 and c.Prem_Sports > 0 then 'Top Tier'
        when c.Prem_Movies = 0 and c.Prem_Sports > 0 then 'Dual Sports'
        else 'Basic'
      end as Premiums,
      case
        when eng.Movies_Avg_DV in ('High') then 'High'
          else 'Non-high'
      end as Movies_Engagement,
      xRisk_Segment_3 as Risk_Segment,
      sum(Scaling_Weight) as Scaled_Volume
  from EPL_07_Risk_Groups_View a left join EPL_05_Scaling_Weights b  on a.Account_Number = b.Account_Number
                                                                    and a.Period = b.Period,
       EPL_04_Profiling_Variables c,
       EPL_04_Eng_Matrix eng,
 where a.Account_Number = c.Account_Number
   and a.Period = c.Period
   and a.Account_Number = eng.Account_Number
   and a.Period = eng.Period
   and eng.Metric = 'Overall'
group by Premiums, Movies_Engagement, xRisk_Segment_3
  union all
select
      'Majority of EPL lost' as Scenario,
      case
        when c.Prem_Movies > 0 and c.Prem_Sports > 0 then 'Top Tier'
        when c.Prem_Movies = 0 and c.Prem_Sports > 0 then 'Dual Sports'
        else 'Basic'
      end as Premiums,
      case
        when eng.Movies_Avg_DV in ('High') then 'High'
          else 'Non-high'
      end as Movies_Engagement,
      xRisk_Segment_4 as Risk_Segment,
      sum(Scaling_Weight) as Scaled_Volume
  from EPL_07_Risk_Groups_View a left join EPL_05_Scaling_Weights b  on a.Account_Number = b.Account_Number
                                                                    and a.Period = b.Period,
       EPL_04_Profiling_Variables c,
       EPL_04_Eng_Matrix eng,
 where a.Account_Number = c.Account_Number
   and a.Period = c.Period
   and a.Account_Number = eng.Account_Number
   and a.Period = eng.Period
   and eng.Metric = 'Overall'
group by Premiums, Movies_Engagement, xRisk_Segment_4
order by 1, 2, 3;



  -- ##############################################################################################################
  -- ###### Ad-hoc/supporting queries                                                                        ######
  -- ##############################################################################################################
  -- COUNTS - adhoc -> risk groups by a variable
select
      prof.Broadband as Factor,
      xRisk_Segment_1 as Risk_Group,
      xRisk_Segment_3 as Segment,
      count(*) as Unscaled_Volume,
      sum(Scaling_Weight) as Scaled_Volume
  from EPL_07_Risk_Groups_View a left join EPL_05_Scaling_Weights b  on a.Account_Number = b.Account_Number
                                                                    and a.Period = b.Period,
       EPL_04_Profiling_Variables prof
 where a.Account_Number = prof.Account_Number
   and a.Period = prof.Period
 group by Factor, xRisk_Segment_1, xRisk_Segment_3
 order by 1, 2, 3;


  -- COUNTS - number of accounts (Downgrade risk only) meeting "high" criteria for 4 variables
 select
        xRisk_Segment_3 as Segment,
        case when eng.Sky_Branded_Channels in ('High') then 1 else 0 end +
        case when eng.Sky_Sports_News_Avg_DV in ('High') then 1 else 0 end +
        case when eng.Movies_Avg_DV in ('High') then 1 else 0 end +
        case when prof.Number_Of_Sky_Products_No_DTV >= 5 then 1 else 0 end as Num_Criteria_High,
        count(*) as Unscaled_Volume,
        sum(Scaling_Weight) as Scaled_Volume
  from EPL_07_Risk_Groups_View a left join EPL_05_Scaling_Weights b  on a.Account_Number = b.Account_Number
                                                                    and a.Period = b.Period,
       EPL_04_Profiling_Variables prof,
       EPL_04_Eng_Matrix eng

 where a.xRisk_Segment_3 = 'Downgrade risk'
   and a.Account_Number = prof.Account_Number
   and a.Period = prof.Period
   and prof.Prem_Movies > 0              -- "=0" for DS
   and prof.Prem_Sports > 0              -- ">0" for DS

   and a.Account_Number = eng.Account_Number
   and a.Period = eng.Period
   and eng.Metric = 'Overall'

 group by xRisk_Segment_3, Num_Criteria_High
 order by 1, 2, 3;





  -- ##############################################################################################################
  -- ###### Key driver on risk group assessment                                                              ######
  -- ##############################################################################################################
/*
  -- Approach 1
if object_id('EPL_07_Risk_Groups_Profiles') is not null then drop table EPL_07_Risk_Groups_Profiles end if;
create table EPL_07_Risk_Groups_Profiles (
    Pk_Identifier                           bigint            identity,
    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name(),

    Scaled_Universe_Flag                    varchar(3)        null      default '???',
    Scenario                                varchar(50)       null      default '???',
    Risk_Outcome                            varchar(50)       null      default '???',
    Profile_Variable                        varchar(100)      null      default '???',
    Variable_Category                       varchar(100)      null      default '???',
    Accounts_Volume                         bigint            null      default 0
);
create        lf   index idx01 on EPL_07_Risk_Groups_Profiles(Scaled_Universe_Flag);
create        lf   index idx03 on EPL_07_Risk_Groups_Profiles(Profile_Variable);
create        lf   index idx04 on EPL_07_Risk_Groups_Profiles(Variable_Category);
grant select on EPL_07_Risk_Groups_Profiles to vespa_group_low_security;


if object_id('EPL_07_Risk_Group_Profiles') is not null then drop procedure EPL_07_Risk_Group_Profiles end if;
create procedure EPL_07_Risk_Group_Profiles
      @parScenario              varchar(100) = '',
      @parRiskVariable          varchar(100) = '',
      @parVariable              varchar(100) = '',
      @parCategoryRules         varchar(1000) = ''
as

begin
      declare @varSQL                         varchar(25000)

      set @varSQL = '
                      insert into EPL_07_Risk_Groups_Profiles
                            (Scaled_Universe_Flag, Scenario, Risk_Outcome, Profile_Variable, Variable_Category, Accounts_Volume)
                        select
                              ''No''                                  as Scaled_Universe_Flag,
                              ''' || @parScenario || '''              as xScenario,
                              ' || @parRiskVariable || '              as xRisk_Outcome,
                              replace(''' || @parVariable || ''', ''_'', '' '') as xProfile_Variable,
                              case
                                when ' || @parCategoryRules || ' then ''Yes''
                                  else ''No''
                              end                                     as xVariable_Category,
                              count(*) as Account_Unscaled
                          from EPL_07_Risk_Groups base,
                               EPL_04_Eng_Matrix eng,
                               EPL_04_Profiling_Variables prof
                         where base.Account_Number = eng.Account_Number
                           and base.Period = eng.Period
                           and base.Sports_Package = ''Sky Sports''
                           and eng.Metric = ''Overall''
                           and base.Account_Number = prof.Account_Number
                           and base.Period = prof.Period
                           and ' || @parRiskVariable || ' <> ''Excluded''

                         group by Scaled_Universe_Flag, xRisk_Outcome, xScenario, xProfile_Variable, xVariable_Category
                      commit
                    '

      execute (@varSQL)

end;

truncate table EPL_07_Risk_Groups_Profiles;
execute EPL_07_Risk_Group_Profiles 'All EPL lost', 'Risk_Segment_3', 'BT_Sport_Viewier' , ' prof.BT_Sport_Viewier = ''Yes'' ';
execute EPL_07_Risk_Group_Profiles 'All EPL lost', 'Risk_Segment_3', 'Sports_Segment_SIG' , ' prof.Sports_Segment_SIG in (''SIG 02 - Flower of Scotland'', ''SIG 05 - Cricket Enthusiasts'', ''SIG 11 - Cricket Fanatics'', ''SIG 20 - Cricket Fans'') ';
execute EPL_07_Risk_Group_Profiles 'All EPL lost', 'Risk_Segment_3', 'Sky_Atlantic_Complete_Progs_Viewed', ' eng.Sky_Atlantic_Complete_Progs_Viewed in (''High'') ';
execute EPL_07_Risk_Group_Profiles 'All EPL lost', 'Risk_Segment_3', 'Movies_SOV', ' eng.Movies_SOV in (''High'') ';
execute EPL_07_Risk_Group_Profiles 'All EPL lost', 'Risk_Segment_3', 'Number_Of_Sky_Products_GO_OD', ' prof.Number_Of_Sky_Products_GO_OD >= 6 ';
execute EPL_07_Risk_Group_Profiles 'All EPL lost', 'Risk_Segment_3', 'Value_Segment', ' prof.Value_Segment = ''F) Unstable'' ';
execute EPL_07_Risk_Group_Profiles 'All EPL lost', 'Risk_Segment_3', 'FSS', ' prof.FSS in (''07) Consolidating Assets'', ''08) Balancing Budgets'', ''09) Stretched Finances'', ''14) Traditional Thrift'') ';

execute EPL_07_Risk_Group_Profiles 'All EPL lost', ' ''(Overall)'' ', 'BT_Sport_Viewier' , ' prof.BT_Sport_Viewier = ''Yes'' ';
execute EPL_07_Risk_Group_Profiles 'All EPL lost', ' ''(Overall)'' ', 'Sports_Segment_SIG' , ' prof.Sports_Segment_SIG in (''SIG 02 - Flower of Scotland'', ''SIG 05 - Cricket Enthusiasts'', ''SIG 11 - Cricket Fanatics'', ''SIG 20 - Cricket Fans'') ';
execute EPL_07_Risk_Group_Profiles 'All EPL lost', ' ''(Overall)'' ', 'Sky_Atlantic_Complete_Progs_Viewed', ' eng.Sky_Atlantic_Complete_Progs_Viewed in (''High'') ';
execute EPL_07_Risk_Group_Profiles 'All EPL lost', ' ''(Overall)'' ', 'Movies_SOV', ' eng.Movies_SOV in (''High'') ';
execute EPL_07_Risk_Group_Profiles 'All EPL lost', ' ''(Overall)'' ', 'Number_Of_Sky_Products_GO_OD', ' prof.Number_Of_Sky_Products_GO_OD >= 6 ';
execute EPL_07_Risk_Group_Profiles 'All EPL lost', ' ''(Overall)'' ', 'Value_Segment', ' prof.Value_Segment = ''F) Unstable'' ';
execute EPL_07_Risk_Group_Profiles 'All EPL lost', ' ''(Overall)'' ', 'FSS', ' prof.FSS in (''07) Consolidating Assets'', ''08) Balancing Budgets'', ''09) Stretched Finances'', ''14) Traditional Thrift'') ';


execute EPL_07_Risk_Group_Profiles 'Majority of EPL lost', 'Risk_Segment_4', 'BT_Sport_Viewier' , ' prof.BT_Sport_Viewier = ''Yes'' ';
execute EPL_07_Risk_Group_Profiles 'Majority of EPL lost', 'Risk_Segment_4', 'Sports_Segment_SIG' , ' prof.Sports_Segment_SIG in (''SIG 02 - Flower of Scotland'', ''SIG 05 - Cricket Enthusiasts'', ''SIG 11 - Cricket Fanatics'', ''SIG 20 - Cricket Fans'') ';
execute EPL_07_Risk_Group_Profiles 'Majority of EPL lost', 'Risk_Segment_4', 'Sky_Atlantic_Complete_Progs_Viewed', ' eng.Sky_Atlantic_Complete_Progs_Viewed in (''High'') ';
execute EPL_07_Risk_Group_Profiles 'Majority of EPL lost', 'Risk_Segment_4', 'Movies_SOV', ' eng.Movies_SOV in (''High'') ';
execute EPL_07_Risk_Group_Profiles 'Majority of EPL lost', 'Risk_Segment_4', 'Number_Of_Sky_Products_GO_OD', ' prof.Number_Of_Sky_Products_GO_OD >= 6 ';
execute EPL_07_Risk_Group_Profiles 'Majority of EPL lost', 'Risk_Segment_4', 'Value_Segment', ' prof.Value_Segment = ''F) Unstable'' ';
execute EPL_07_Risk_Group_Profiles 'Majority of EPL lost', 'Risk_Segment_4', 'FSS', ' prof.FSS in (''07) Consolidating Assets'', ''08) Balancing Budgets'', ''09) Stretched Finances'', ''14) Traditional Thrift'') ';

execute EPL_07_Risk_Group_Profiles 'Majority of EPL lost', ' ''(Overall)'' ', 'BT_Sport_Viewier' , ' prof.BT_Sport_Viewier = ''Yes'' ';
execute EPL_07_Risk_Group_Profiles 'Majority of EPL lost', ' ''(Overall)'' ', 'Sports_Segment_SIG' , ' prof.Sports_Segment_SIG in (''SIG 02 - Flower of Scotland'', ''SIG 05 - Cricket Enthusiasts'', ''SIG 11 - Cricket Fanatics'', ''SIG 20 - Cricket Fans'') ';
execute EPL_07_Risk_Group_Profiles 'Majority of EPL lost', ' ''(Overall)'' ', 'Sky_Atlantic_Complete_Progs_Viewed', ' eng.Sky_Atlantic_Complete_Progs_Viewed in (''High'') ';
execute EPL_07_Risk_Group_Profiles 'Majority of EPL lost', ' ''(Overall)'' ', 'Movies_SOV', ' eng.Movies_SOV in (''High'') ';
execute EPL_07_Risk_Group_Profiles 'Majority of EPL lost', ' ''(Overall)'' ', 'Number_Of_Sky_Products_GO_OD', ' prof.Number_Of_Sky_Products_GO_OD >= 6 ';
execute EPL_07_Risk_Group_Profiles 'Majority of EPL lost', ' ''(Overall)'' ', 'Value_Segment', ' prof.Value_Segment = ''F) Unstable'' ';
execute EPL_07_Risk_Group_Profiles 'Majority of EPL lost', ' ''(Overall)'' ', 'FSS', ' prof.FSS in (''07) Consolidating Assets'', ''08) Balancing Budgets'', ''09) Stretched Finances'', ''14) Traditional Thrift'') ';

select * from EPL_07_Risk_Groups_Profiles;
*/


  -- Approach 2
if object_id('EPL_07_Risk_Groups_Acc_Movements') is not null then drop table EPL_07_Risk_Groups_Acc_Movements end if;
create table EPL_07_Risk_Groups_Acc_Movements (
    Pk_Identifier                           bigint            identity,
    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name(),

    Account_Number                          varchar(20)       null      default null,
    Period                                  tinyint           null      default 0,
    Risk_Group_All_Lost_v1                  varchar(50)       null      default 'No Sky Sports',
    Risk_Group_All_Lost_v2                  varchar(50)       null      default 'No Sky Sports',
    Risk_Group_All_Lost_v3                  varchar(50)       null      default 'No Sky Sports',
    Risk_Group_All_Lost_v4                  varchar(50)       null      default 'No Sky Sports',
    Risk_Group_All_Lost_v5                  varchar(50)       null      default 'No Sky Sports',
    Risk_Group_All_Lost_v6                  varchar(50)       null      default 'No Sky Sports',
    Risk_Group_All_Lost_v7                  varchar(50)       null      default 'No Sky Sports',
    Risk_Group_All_Lost_v8                  varchar(50)       null      default 'No Sky Sports',
    Risk_Group_All_Lost_v9                  varchar(50)       null      default 'No Sky Sports',
    Risk_Segment_All_Lost_v1                varchar(50)       null      default 'No Sky Sports',
    Risk_Segment_All_Lost_v2                varchar(50)       null      default 'No Sky Sports',
    Risk_Segment_All_Lost_v3                varchar(50)       null      default 'No Sky Sports',
    Risk_Segment_All_Lost_v4                varchar(50)       null      default 'No Sky Sports',
    Risk_Segment_All_Lost_v5                varchar(50)       null      default 'No Sky Sports',
    Risk_Segment_All_Lost_v6                varchar(50)       null      default 'No Sky Sports',
    Risk_Segment_All_Lost_v7                varchar(50)       null      default 'No Sky Sports',
    Risk_Segment_All_Lost_v8                varchar(50)       null      default 'No Sky Sports',
    Risk_Segment_All_Lost_v9                varchar(50)       null      default 'No Sky Sports',

    Risk_Group_Majority_Lost_v1             varchar(50)       null      default 'No Sky Sports',
    Risk_Group_Majority_Lost_v2             varchar(50)       null      default 'No Sky Sports',
    Risk_Group_Majority_Lost_v3             varchar(50)       null      default 'No Sky Sports',
    Risk_Group_Majority_Lost_v4             varchar(50)       null      default 'No Sky Sports',
    Risk_Group_Majority_Lost_v5             varchar(50)       null      default 'No Sky Sports',
    Risk_Group_Majority_Lost_v6             varchar(50)       null      default 'No Sky Sports',
    Risk_Group_Majority_Lost_v7             varchar(50)       null      default 'No Sky Sports',
    Risk_Group_Majority_Lost_v8             varchar(50)       null      default 'No Sky Sports',
    Risk_Group_Majority_Lost_v9             varchar(50)       null      default 'No Sky Sports',
    Risk_Segment_Majority_Lost_v1           varchar(50)       null      default 'No Sky Sports',
    Risk_Segment_Majority_Lost_v2           varchar(50)       null      default 'No Sky Sports',
    Risk_Segment_Majority_Lost_v3           varchar(50)       null      default 'No Sky Sports',
    Risk_Segment_Majority_Lost_v4           varchar(50)       null      default 'No Sky Sports',
    Risk_Segment_Majority_Lost_v5           varchar(50)       null      default 'No Sky Sports',
    Risk_Segment_Majority_Lost_v6           varchar(50)       null      default 'No Sky Sports',
    Risk_Segment_Majority_Lost_v7           varchar(50)       null      default 'No Sky Sports',
    Risk_Segment_Majority_Lost_v8           varchar(50)       null      default 'No Sky Sports',
    Risk_Segment_Majority_Lost_v9           varchar(50)       null      default 'No Sky Sports'

);
create        hg   index idx01 on EPL_07_Risk_Groups_Acc_Movements(Account_Number);
create        lf   index idx02 on EPL_07_Risk_Groups_Acc_Movements(Period);
create unique hg   index idx03 on EPL_07_Risk_Groups_Acc_Movements(Account_Number, Period);
grant select on EPL_07_Risk_Groups_Acc_Movements to vespa_group_low_security;


  -- V1
insert into EPL_07_Risk_Groups_Acc_Movements
      (Account_Number, Period, Risk_Group_All_Lost_v1, Risk_Group_Majority_Lost_v1, Risk_Segment_All_Lost_v1, Risk_Segment_Majority_Lost_v1)
  select
        Account_Number,
        Period,
        Risk_Segment_1,
        Risk_Segment_2,
        Risk_Segment_3,
        Risk_Segment_4
    from EPL_07_Risk_Groups_v1
   where Period = 1;
commit;


  -- V2 (Added: Sky Movies, Sky Atlantic - num complete programmes, extended number of Sky products)
update EPL_07_Risk_Groups_Acc_Movements base
   set base.Risk_Group_All_Lost_v2          = det.Risk_Segment_1,
       base.Risk_Group_Majority_Lost_v2     = det.Risk_Segment_2,
       base.Risk_Segment_All_Lost_v2        = det.Risk_Segment_3,
       base.Risk_Segment_Majority_Lost_v2   = det.Risk_Segment_4
  from EPL_07_Risk_Groups_v2 det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period;
commit;


  -- V3 (Added: CL engagement)
update EPL_07_Risk_Groups_Acc_Movements base
   set base.Risk_Group_All_Lost_v3          = det.Risk_Segment_1,
       base.Risk_Group_Majority_Lost_v3     = det.Risk_Segment_2,
       base.Risk_Segment_All_Lost_v3        = det.Risk_Segment_3,
       base.Risk_Segment_Majority_Lost_v3   = det.Risk_Segment_4
  from EPL_07_Risk_Groups_v3 det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period;
commit;


  -- V4 (Added: Latent groups replaced with actual outcomes)
update EPL_07_Risk_Groups_Acc_Movements base
   set base.Risk_Group_All_Lost_v4          = det.Risk_Segment_1,
       base.Risk_Group_Majority_Lost_v4     = det.Risk_Segment_2,
       base.Risk_Segment_All_Lost_v4        = det.Risk_Segment_3,
       base.Risk_Segment_Majority_Lost_v4   = det.Risk_Segment_4
  from EPL_07_Risk_Groups_v4 det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period;
commit;


  -- V5 (Added: Additional dimension added for Risk Groups 2 & 3 (All EPL lost))
update EPL_07_Risk_Groups_Acc_Movements base
   set base.Risk_Group_All_Lost_v5          = det.Risk_Segment_1,
       base.Risk_Group_Majority_Lost_v5     = det.Risk_Segment_2,
       base.Risk_Segment_All_Lost_v5        = det.Risk_Segment_3,
       base.Risk_Segment_Majority_Lost_v5   = det.Risk_Segment_4
  from EPL_07_Risk_Groups_v5 det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period;
commit;


  -- V6 (Added: SIG level removed from Risk Group 1 (all EPL lost only) &  Risk Group 5 - all outcomes are "No Change" now
update EPL_07_Risk_Groups_Acc_Movements base
   set base.Risk_Group_All_Lost_v6          = det.Risk_Segment_1,
       base.Risk_Group_Majority_Lost_v6     = det.Risk_Segment_2,
       base.Risk_Segment_All_Lost_v6        = det.Risk_Segment_3,
       base.Risk_Segment_Majority_Lost_v6   = det.Risk_Segment_4
  from EPL_07_Risk_Groups_v6 det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period;
commit;


  -- Summarise movements
select
      base.Risk_Group_All_Lost_v1,
      base.Risk_Group_All_Lost_v2,
      base.Risk_Group_All_Lost_v3,
      base.Risk_Group_All_Lost_v4,
      base.Risk_Group_All_Lost_v5,
      base.Risk_Group_All_Lost_v6,
      base.Risk_Group_All_Lost_v7,
      base.Risk_Group_All_Lost_v8,
      base.Risk_Group_All_Lost_v9,
      base.Risk_Segment_All_Lost_v1,
      base.Risk_Segment_All_Lost_v2,
      base.Risk_Segment_All_Lost_v3,
      base.Risk_Segment_All_Lost_v4,
      base.Risk_Segment_All_Lost_v5,
      base.Risk_Segment_All_Lost_v6,
      base.Risk_Segment_All_Lost_v7,
      base.Risk_Segment_All_Lost_v8,
      base.Risk_Segment_All_Lost_v9,
      case when prof.BT_Sport_Viewier = 'Yes' then 'Yes' else 'No' end as xBT_Sport_Viewer,
      case when prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans') then 'Yes' else 'No' end as xSport_Segment_SIG,
      case when eng.Sky_Atlantic_Complete_Progs_Viewed in ('High') then 'Yes' else 'No' end as xHigh_Sky_Atlantic,
      case when eng.Movies_SOV in ('High') then 'Yes' else 'No' end as xHigh_Sky_Movies,
      case when prof.Number_Of_Sky_Products_GO_OD >= 6 then 'Yes' else 'No' end as xHigh_Sky_Products,
      case when prof.Value_Segment = 'F) Unstable' then 'Yes' else 'No' end as xUnstable_Value_Segment,
      case when prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift') then 'Yes' else 'No' end as xSelected_FSS,
      count(*) as Accounts_Unscaled,
      sum(Scaling_Weight) as Accounts_Scaled
  from EPL_07_Risk_Groups_Acc_Movements base left join EPL_05_Scaling_Weights b  on base.Account_Number = b.Account_Number
                                                                                and base.Period = b.Period,
       EPL_04_Eng_Matrix eng,
       EPL_04_Profiling_Variables prof
 where base.Risk_Segment_All_Lost_v1 not in ('Excluded', 'No Sky Sports')
   and base.Account_Number = eng.Account_Number
   and base.Period = eng.Period
   and eng.Metric = 'Overall'
   and base.Account_Number = prof.Account_Number
   and base.Period = prof.Period
 group by
        base.Risk_Group_All_Lost_v1,
        base.Risk_Group_All_Lost_v2,
        base.Risk_Group_All_Lost_v3,
        base.Risk_Group_All_Lost_v4,
        base.Risk_Group_All_Lost_v5,
        base.Risk_Group_All_Lost_v6,
        base.Risk_Group_All_Lost_v7,
        base.Risk_Group_All_Lost_v8,
        base.Risk_Group_All_Lost_v9,
        base.Risk_Segment_All_Lost_v1,
        base.Risk_Segment_All_Lost_v2,
        base.Risk_Segment_All_Lost_v3,
        base.Risk_Segment_All_Lost_v4,
        base.Risk_Segment_All_Lost_v5,
        base.Risk_Segment_All_Lost_v6,
        base.Risk_Segment_All_Lost_v7,
        base.Risk_Segment_All_Lost_v8,
        base.Risk_Segment_All_Lost_v9,
        xBT_Sport_Viewer,
        xSport_Segment_SIG,
        xHigh_Sky_Atlantic,
        xHigh_Sky_Movies,
        xHigh_Sky_Products,
        xUnstable_Value_Segment,
        xSelected_FSS
 order by base.Risk_Group_All_Lost_v1, base.Risk_Group_All_Lost_v2, base.Risk_Segment_All_Lost_v1, base.Risk_Segment_All_Lost_v2;





  -- ##############################################################################################################
  -- ##############################################################################################################
  -- ###### HISTORICAL RULES #####
/*
  -- ##############################################################################################################
  -- ##### V1                                                                                                 #####
  -- ##############################################################################################################
update EPL_07_Risk_Groups base
   set base.Risk_Segment_1  =                                               -- Basic risk group - Sky loses EPL In full
      case
        when det.EPL_SoSV in ('High')             and det.Sport_SoV in ('High', 'Very high')                                          then 'Risk group 1'
        when det.EPL_SoSV in ('Low', 'Medium')    and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Medium', 'High') then 'Risk group 2'
        when det.EPL_SoSV in ('Medium', 'High')   and det.Sport_SoV in ('Low', 'Medium')                                              then 'Risk group 3'
        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 'Risk group 4'
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('Low', 'Medium')                                              then 'Risk group 5'
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 'Risk group 5'
          else 'Excluded'
      end,

        base.Risk_Segment_2  =                                               -- Basic risk group - Sky loses majority of EPL
      case
        when det.EPL_SoSV in ('High')             and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 'Risk group 1'
        when det.EPL_SoSV in ('Medium', 'High')   and det.Sport_SoV in ('Low', 'Medium')        and det.EPL_SOC in ('Low')            then 'Risk group 2'
        when det.EPL_SoSV in ('Medium', 'High')                                                 and det.EPL_SOC in ('Medium', 'High') then 'Risk group 3'
        when det.EPL_SoSV in ('Low', 'Medium')    and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 'Risk group 4'
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('Low', 'Medium')                                              then 'Risk group 5'
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Medium', 'High') then 'Risk group 5'
          else 'Excluded'
      end
  from EPL_04_Eng_Matrix det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and base.Sports_Package = 'Sky Sports'
   and det.Metric = 'Overall';
commit;


  -- EPL risk groups
update EPL_07_Risk_Groups base
   set base.Risk_Segment_3  =                                               -- EPL risk group - Sky loses EPL In full
      case
        when base.Risk_Segment_1 in ('Risk group 1', 'Risk group 2', 'Risk group 3', 'Risk group 4') and BT_Sport_Viewier = 'Yes'             then 'Latent risk'

        when base.Risk_Segment_1 in ('Risk group 2', 'Risk group 3', 'Risk group 4') and
             prof.Sports_Segment_SIG in ('SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')                then 'Will stay'

        when base.Risk_Segment_1 in ('Risk group 1', 'Risk group 2', 'Risk group 3', 'Risk group 4') and eng.Sky_Atlantic_SoV = 'High'        then 'Downgrade risk'

        when base.Risk_Segment_1 in ('Risk group 2', 'Risk group 3') and prof.Value_Segment = 'F) Unstable'                                   then 'Churn risk'
        when base.Risk_Segment_1 in ('Risk group 4', 'Risk group 5') and prof.Value_Segment = 'F) Unstable'                                   then 'Downgrade risk'

        when base.Risk_Segment_1 in ('Risk group 2', 'Risk group 3') and
             prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')            then 'Churn risk'
        when base.Risk_Segment_1 in ('Risk group 4', 'Risk group 5') and
             prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')            then 'Downgrade risk'

        when base.Risk_Segment_1 in ('Risk group 1')                                                                                          then 'Churn risk'
        when base.Risk_Segment_1 in ('Risk group 2', 'Risk group 3')                                                                          then 'Downgrade risk'
        when base.Risk_Segment_1 in ('Risk group 4', 'Risk group 5')                                                                          then 'Will stay'

          else 'Excluded'
      end,

       base.Risk_Segment_4  =                                               -- EPL risk group - Sky loses majority of EPL
      case
        when base.Risk_Segment_2 in ('Risk group 1', 'Risk group 2', 'Risk group 3', 'Risk group 4') and BT_Sport_Viewier = 'Yes'             then 'Latent risk'

        when base.Risk_Segment_2 in ('Risk group 2', 'Risk group 3', 'Risk group 4') and
             prof.Sports_Segment_SIG in ('SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')                then 'Will stay'

        when base.Risk_Segment_2 in ('Risk group 1', 'Risk group 2', 'Risk group 3', 'Risk group 4') and eng.Sky_Atlantic_SoV = 'High'        then 'Downgrade risk'

        when base.Risk_Segment_2 in ('Risk group 2') and prof.Value_Segment = 'F) Unstable'                                                   then 'Churn risk'
        when base.Risk_Segment_2 in ('Risk group 3') and prof.Value_Segment = 'F) Unstable'                                                   then 'Downgrade risk'
        when base.Risk_Segment_2 in ('Risk group 4', 'Risk group 5') and prof.Value_Segment = 'F) Unstable'                                   then 'Will stay'

        when base.Risk_Segment_2 in ('Risk group 2') and
             prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')            then 'Churn risk'
        when base.Risk_Segment_2 in ('Risk group 3') and
             prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')            then 'Downgrade risk'
        when base.Risk_Segment_2 in ('Risk group 4', 'Risk group 5') and
             prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')            then 'Will stay'

        when base.Risk_Segment_2 in ('Risk group 1')                                                                                          then 'Churn risk'
        when base.Risk_Segment_2 in ('Risk group 2')                                                                                          then 'Downgrade risk'
        when base.Risk_Segment_2 in ('Risk group 3', 'Risk group 4')                                                                          then 'Latent risk'
        when base.Risk_Segment_2 in ('Risk group 5')                                                                                          then 'Will stay'

          else 'Excluded'
      end

  from EPL_04_Eng_Matrix eng,
       EPL_04_Profiling_Variables prof
 where base.Account_Number = eng.Account_Number
   and base.Period = eng.Period
   and base.Sports_Package = 'Sky Sports'
   and eng.Metric = 'Overall'
   and base.Account_Number = prof.Account_Number
   and base.Period = prof.Period;



  -- ##############################################################################################################
  -- ##### V2                                                                                                 #####
  -- ##############################################################################################################
update EPL_07_Risk_Groups base
   set base.Risk_Segment_1  =                                               -- Basic risk group - Sky loses EPL In full
      case
        when det.EPL_SoSV in ('High')             and det.Sport_SoV in ('High', 'Very high')                                          then 'Risk group 1'
        when det.EPL_SoSV in ('Low', 'Medium')    and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Medium', 'High') then 'Risk group 2'
        when det.EPL_SoSV in ('Medium', 'High')   and det.Sport_SoV in ('Low', 'Medium')                                              then 'Risk group 3'
        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 'Risk group 4'
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('Low', 'Medium')                                              then 'Risk group 5'
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 'Risk group 5'
          else 'Excluded'
      end,

        base.Risk_Segment_2  =                                               -- Basic risk group - Sky loses majority of EPL
      case
        when det.EPL_SoSV in ('High')             and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 'Risk group 1'
        when det.EPL_SoSV in ('Medium', 'High')   and det.Sport_SoV in ('Low', 'Medium')        and det.EPL_SOC in ('Low')            then 'Risk group 2'
        when det.EPL_SoSV in ('Medium', 'High')                                                 and det.EPL_SOC in ('Medium', 'High') then 'Risk group 3'
        when det.EPL_SoSV in ('Low', 'Medium')    and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 'Risk group 4'
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('Low', 'Medium')                                              then 'Risk group 5'
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Medium', 'High') then 'Risk group 5'
          else 'Excluded'
      end
  from EPL_04_Eng_Matrix det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and base.Sports_Package = 'Sky Sports'
   and det.Metric = 'Overall';
commit;


  -- EPL risk groups
update EPL_07_Risk_Groups base
   set base.Risk_Segment_3  =                                               -- EPL risk group - Sky loses EPL In full
      case

          -- ##### Risk group 1 #####
        when base.Risk_Segment_1 in ('Risk group 1') and prof.BT_Sport_Viewier = 'Yes'                                                        then 'Latent risk'
        when base.Risk_Segment_1 in ('Risk group 1') and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 'Downgrade risk'
        when base.Risk_Segment_1 in ('Risk group 1') and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                   then 'Downgrade risk'
        when base.Risk_Segment_1 in ('Risk group 1') and eng.Movies_SOV in ('High')                                                           then 'Downgrade risk'
        when base.Risk_Segment_1 in ('Risk group 1') and prof.Number_Of_Sky_Products_GO_OD >= 6                                               then 'Downgrade risk'
        when base.Risk_Segment_1 in ('Risk group 1') and prof.Value_Segment = 'F) Unstable'                                                   then 'Churn risk'
        when base.Risk_Segment_1 in ('Risk group 1') and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 'Churn risk'
        when base.Risk_Segment_1 in ('Risk group 1')                                                                                          then 'Downgrade risk'

          -- ##### Risk group 2 #####
        when base.Risk_Segment_1 in ('Risk group 2') and prof.BT_Sport_Viewier = 'Yes'                                                        then 'Latent risk'
        when base.Risk_Segment_1 in ('Risk group 2') and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 'Will stay'
        when base.Risk_Segment_1 in ('Risk group 2') and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                   then 'Will stay'
        when base.Risk_Segment_1 in ('Risk group 2') and eng.Movies_SOV in ('High')                                                           then 'Downgrade risk'
        when base.Risk_Segment_1 in ('Risk group 2') and prof.Number_Of_Sky_Products_GO_OD >= 6                                               then 'Downgrade risk'
        when base.Risk_Segment_1 in ('Risk group 2') and prof.Value_Segment = 'F) Unstable'                                                   then 'Churn risk'
        when base.Risk_Segment_1 in ('Risk group 2') and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 'Churn risk'
        when base.Risk_Segment_1 in ('Risk group 2')                                                                                          then 'Downgrade risk'

          -- ##### Risk group 3 #####
        when base.Risk_Segment_1 in ('Risk group 3') and prof.BT_Sport_Viewier = 'Yes'                                                        then 'Latent risk'
        when base.Risk_Segment_1 in ('Risk group 3') and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 'Will stay'
        when base.Risk_Segment_1 in ('Risk group 3') and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                   then 'Will stay'
        when base.Risk_Segment_1 in ('Risk group 3') and eng.Movies_SOV in ('High')                                                           then 'Downgrade risk'
        when base.Risk_Segment_1 in ('Risk group 3') and prof.Number_Of_Sky_Products_GO_OD >= 6                                               then 'Downgrade risk'
        when base.Risk_Segment_1 in ('Risk group 3') and prof.Value_Segment = 'F) Unstable'                                                   then 'Churn risk'
        when base.Risk_Segment_1 in ('Risk group 3') and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 'Churn risk'
        when base.Risk_Segment_1 in ('Risk group 3')                                                                                          then 'Downgrade risk'

          -- ##### Risk group 4 #####
        when base.Risk_Segment_1 in ('Risk group 4') and prof.BT_Sport_Viewier = 'Yes'                                                        then 'Latent risk'
        when base.Risk_Segment_1 in ('Risk group 4') and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 'Will stay'
        when base.Risk_Segment_1 in ('Risk group 4') and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                   then 'Will stay'
        when base.Risk_Segment_1 in ('Risk group 4') and eng.Movies_SOV in ('High')                                                           then 'Will stay'
        when base.Risk_Segment_1 in ('Risk group 4') and prof.Number_Of_Sky_Products_GO_OD >= 6                                               then 'Will stay'
        when base.Risk_Segment_1 in ('Risk group 4') and prof.Value_Segment = 'F) Unstable'                                                   then 'Downgrade risk'
        when base.Risk_Segment_1 in ('Risk group 4') and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 'Downgrade risk'
        when base.Risk_Segment_1 in ('Risk group 4')                                                                                          then 'Will stay'

          -- ##### Risk group 5 #####
        when base.Risk_Segment_1 in ('Risk group 5') and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                   then 'Will stay'
        when base.Risk_Segment_1 in ('Risk group 5') and eng.Movies_SOV in ('High')                                                           then 'Will stay'
        when base.Risk_Segment_1 in ('Risk group 5') and prof.Number_Of_Sky_Products_GO_OD >= 6                                               then 'Will stay'
        when base.Risk_Segment_1 in ('Risk group 5') and prof.Value_Segment = 'F) Unstable'                                                   then 'Downgrade risk'
        when base.Risk_Segment_1 in ('Risk group 5') and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 'Downgrade risk'
        when base.Risk_Segment_1 in ('Risk group 5')                                                                                          then 'Will stay'

          else 'Excluded'
      end,

       base.Risk_Segment_4  =                                               -- EPL risk group - Sky loses majority of EPL
      case

          -- ##### Risk group 1 #####
        when base.Risk_Segment_2 in ('Risk group 1') and prof.BT_Sport_Viewier = 'Yes'                                                        then 'Latent risk'
        when base.Risk_Segment_2 in ('Risk group 1') and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 'Downgrade risk'
        when base.Risk_Segment_2 in ('Risk group 1') and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                   then 'Downgrade risk'
        when base.Risk_Segment_2 in ('Risk group 1') and eng.Movies_SOV in ('High')                                                           then 'Downgrade risk'
        when base.Risk_Segment_2 in ('Risk group 1') and prof.Number_Of_Sky_Products_GO_OD >= 6                                               then 'Downgrade risk'
        when base.Risk_Segment_2 in ('Risk group 1') and prof.Value_Segment = 'F) Unstable'                                                   then 'Churn risk'
        when base.Risk_Segment_2 in ('Risk group 1') and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 'Churn risk'
        when base.Risk_Segment_2 in ('Risk group 1')                                                                                          then 'Downgrade risk'

          -- ##### Risk group 2 #####
        when base.Risk_Segment_2 in ('Risk group 2') and prof.BT_Sport_Viewier = 'Yes'                                                        then 'Latent risk'
        when base.Risk_Segment_2 in ('Risk group 2') and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 'Downgrade risk'
        when base.Risk_Segment_2 in ('Risk group 2') and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                   then 'Downgrade risk'
        when base.Risk_Segment_2 in ('Risk group 2') and eng.Movies_SOV in ('High')                                                           then 'Downgrade risk'
        when base.Risk_Segment_2 in ('Risk group 2') and prof.Number_Of_Sky_Products_GO_OD >= 6                                               then 'Downgrade risk'
        when base.Risk_Segment_2 in ('Risk group 2') and prof.Value_Segment = 'F) Unstable'                                                   then 'Churn risk'
        when base.Risk_Segment_2 in ('Risk group 2') and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 'Churn risk'
        when base.Risk_Segment_2 in ('Risk group 2')                                                                                          then 'Downgrade risk'

          -- ##### Risk group 3 #####
        when base.Risk_Segment_2 in ('Risk group 3') and prof.BT_Sport_Viewier = 'Yes'                                                        then 'Latent risk'
        when base.Risk_Segment_2 in ('Risk group 3') and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 'Will stay'
        when base.Risk_Segment_2 in ('Risk group 3') and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                   then 'Will stay'
        when base.Risk_Segment_2 in ('Risk group 3') and eng.Movies_SOV in ('High')                                                           then 'Downgrade risk'
        when base.Risk_Segment_2 in ('Risk group 3') and prof.Number_Of_Sky_Products_GO_OD >= 6                                               then 'Downgrade risk'
        when base.Risk_Segment_2 in ('Risk group 3') and prof.Value_Segment = 'F) Unstable'                                                   then 'Downgrade risk'
        when base.Risk_Segment_2 in ('Risk group 3') and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 'Downgrade risk'
        when base.Risk_Segment_2 in ('Risk group 3')                                                                                          then 'Latent risk'

          -- ##### Risk group 4 #####
        when base.Risk_Segment_2 in ('Risk group 4') and prof.BT_Sport_Viewier = 'Yes'                                                        then 'Latent risk'
        when base.Risk_Segment_2 in ('Risk group 4') and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 'Will stay'
        when base.Risk_Segment_2 in ('Risk group 4') and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                   then 'Will stay'
        when base.Risk_Segment_2 in ('Risk group 4') and eng.Movies_SOV in ('High')                                                           then 'Will stay'
        when base.Risk_Segment_2 in ('Risk group 4') and prof.Number_Of_Sky_Products_GO_OD >= 6                                               then 'Will stay'
        when base.Risk_Segment_2 in ('Risk group 4') and prof.Value_Segment = 'F) Unstable'                                                   then 'Downgrade risk'
        when base.Risk_Segment_2 in ('Risk group 4') and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 'Downgrade risk'
        when base.Risk_Segment_2 in ('Risk group 4')                                                                                          then 'Latent risk'

          -- ##### Risk group 5 #####
        when base.Risk_Segment_2 in ('Risk group 5') and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                   then 'Will stay'
        when base.Risk_Segment_2 in ('Risk group 5') and eng.Movies_SOV in ('High')                                                           then 'Will stay'
        when base.Risk_Segment_2 in ('Risk group 5') and prof.Number_Of_Sky_Products_GO_OD >= 6                                               then 'Will stay'
        when base.Risk_Segment_2 in ('Risk group 5') and prof.Value_Segment = 'F) Unstable'                                                   then 'Downgrade risk'
        when base.Risk_Segment_2 in ('Risk group 5') and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 'Downgrade risk'
        when base.Risk_Segment_2 in ('Risk group 5')                                                                                          then 'Will stay'

          else 'Excluded'
      end

  from EPL_04_Eng_Matrix eng,
       EPL_04_Profiling_Variables prof
 where base.Account_Number = eng.Account_Number
   and base.Period = eng.Period
   and base.Sports_Package = 'Sky Sports'
   and eng.Metric = 'Overall'
   and base.Account_Number = prof.Account_Number
   and base.Period = prof.Period;
commit;


  -- ##############################################################################################################
  -- ##### V3                                                                                                 #####
  -- ##############################################################################################################
  -- Basic risk groups
update EPL_07_Risk_Groups base
   set base.Risk_Segment_1  =                                               -- Basic risk group - Sky loses EPL In full
      case
        when det.EPL_SoSV in ('High')             and det.Sport_SoV in ('High', 'Very high')                                          then 'Risk group 1'
        when det.EPL_SoSV in ('Low', 'Medium')    and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Medium', 'High') then 'Risk group 2'
        when det.EPL_SoSV in ('Medium', 'High')   and det.Sport_SoV in ('Low', 'Medium')                                              then 'Risk group 3'
        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 'Risk group 4'
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('Low', 'Medium')                                              then 'Risk group 5'
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 'Risk group 5'
          else 'Excluded'
      end,

        base.Risk_Segment_2  =                                               -- Basic risk group - Sky loses majority of EPL
      case
        when det.EPL_SoSV in ('High')             and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 'Risk group 1'
        when det.EPL_SoSV in ('Medium', 'High')   and det.Sport_SoV in ('Low', 'Medium')        and det.EPL_SOC in ('Low')            then 'Risk group 2'
        when det.EPL_SoSV in ('Medium', 'High')                                                 and det.EPL_SOC in ('Medium', 'High') then 'Risk group 3'
        when det.EPL_SoSV in ('Low', 'Medium')    and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 'Risk group 4'
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('Low', 'Medium')                                              then 'Risk group 5'
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Medium', 'High') then 'Risk group 5'
          else 'Excluded'
      end
  from EPL_04_Eng_Matrix det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and base.Sports_Package = 'Sky Sports'
   and det.Metric = 'Overall';
commit;


  -- EPL risk groups
update EPL_07_Risk_Groups base
   set base.Risk_Segment_3  =                                               -- EPL risk group - Sky loses EPL In full
      case

          -- ##### Risk group 1 #####
        when base.Risk_Segment_1 in ('Risk group 1') and prof.BT_Sport_Viewier = 'Yes'                                                        then 'Latent risk'
        when base.Risk_Segment_1 in ('Risk group 1') and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 'No change'
        when base.Risk_Segment_1 in ('Risk group 1') and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                   then 'Downgrade risk'
        when base.Risk_Segment_1 in ('Risk group 1') and eng.Movies_SOV in ('High')                                                           then 'Downgrade risk'
        when base.Risk_Segment_1 in ('Risk group 1') and prof.Number_Of_Sky_Products_GO_OD >= 6                                               then 'Downgrade risk'
        when base.Risk_Segment_1 in ('Risk group 1') and cl_eng.CL_SOC in ('High')                                                            then 'Churn risk'
        when base.Risk_Segment_1 in ('Risk group 1') and prof.Value_Segment = 'F) Unstable'                                                   then 'Churn risk'
        when base.Risk_Segment_1 in ('Risk group 1') and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 'Churn risk'
        when base.Risk_Segment_1 in ('Risk group 1')                                                                                          then 'Churn risk'

          -- ##### Risk group 2 #####
        when base.Risk_Segment_1 in ('Risk group 2') and prof.BT_Sport_Viewier = 'Yes'                                                        then 'Latent risk'
        when base.Risk_Segment_1 in ('Risk group 2') and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 'No change'
        when base.Risk_Segment_1 in ('Risk group 2') and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                   then 'Downgrade risk'
        when base.Risk_Segment_1 in ('Risk group 2') and eng.Movies_SOV in ('High')                                                           then 'Downgrade risk'
        when base.Risk_Segment_1 in ('Risk group 2') and prof.Number_Of_Sky_Products_GO_OD >= 6                                               then 'Downgrade risk'
        when base.Risk_Segment_1 in ('Risk group 2') and cl_eng.CL_SOC in ('High')                                                            then 'Churn risk'
        when base.Risk_Segment_1 in ('Risk group 2') and prof.Value_Segment = 'F) Unstable'                                                   then 'Churn risk'
        when base.Risk_Segment_1 in ('Risk group 2') and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 'Churn risk'
        when base.Risk_Segment_1 in ('Risk group 2')                                                                                          then 'Churn risk'

          -- ##### Risk group 3 #####
        when base.Risk_Segment_1 in ('Risk group 3') and prof.BT_Sport_Viewier = 'Yes'                                                        then 'Latent risk'
        when base.Risk_Segment_1 in ('Risk group 3') and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 'No change'
        when base.Risk_Segment_1 in ('Risk group 3') and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                   then 'Downgrade risk'
        when base.Risk_Segment_1 in ('Risk group 3') and eng.Movies_SOV in ('High')                                                           then 'Downgrade risk'
        when base.Risk_Segment_1 in ('Risk group 3') and prof.Number_Of_Sky_Products_GO_OD >= 6                                               then 'Downgrade risk'
        when base.Risk_Segment_1 in ('Risk group 3') and cl_eng.CL_SOC in ('High')                                                            then 'Downgrade risk'
        when base.Risk_Segment_1 in ('Risk group 3') and prof.Value_Segment = 'F) Unstable'                                                   then 'Churn risk'
        when base.Risk_Segment_1 in ('Risk group 3') and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 'Churn risk'
        when base.Risk_Segment_1 in ('Risk group 3')                                                                                          then 'Churn risk'

          -- ##### Risk group 4 #####
        when base.Risk_Segment_1 in ('Risk group 4') and prof.BT_Sport_Viewier = 'Yes'                                                        then 'Latent risk'
        when base.Risk_Segment_1 in ('Risk group 4') and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 'No change'
        when base.Risk_Segment_1 in ('Risk group 4') and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                   then 'No change'
        when base.Risk_Segment_1 in ('Risk group 4') and eng.Movies_SOV in ('High')                                                           then 'No change'
        when base.Risk_Segment_1 in ('Risk group 4') and prof.Number_Of_Sky_Products_GO_OD >= 6                                               then 'No change'
        when base.Risk_Segment_1 in ('Risk group 4') and cl_eng.CL_SOC in ('High')                                                            then 'Downgrade risk'
        when base.Risk_Segment_1 in ('Risk group 4') and prof.Value_Segment = 'F) Unstable'                                                   then 'Downgrade risk'
        when base.Risk_Segment_1 in ('Risk group 4') and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 'Downgrade risk'
        when base.Risk_Segment_1 in ('Risk group 4')                                                                                          then 'No change'

          -- ##### Risk group 5 #####
        when base.Risk_Segment_1 in ('Risk group 5') and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                   then 'No change'
        when base.Risk_Segment_1 in ('Risk group 5') and eng.Movies_SOV in ('High')                                                           then 'No change'
        when base.Risk_Segment_1 in ('Risk group 5') and prof.Number_Of_Sky_Products_GO_OD >= 6                                               then 'No change'
        when base.Risk_Segment_1 in ('Risk group 5') and cl_eng.CL_SOC in ('High')                                                            then 'No change'
        when base.Risk_Segment_1 in ('Risk group 5') and prof.Value_Segment = 'F) Unstable'                                                   then 'Downgrade risk'
        when base.Risk_Segment_1 in ('Risk group 5') and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 'Downgrade risk'
        when base.Risk_Segment_1 in ('Risk group 5')                                                                                          then 'No change'

          else 'Excluded'
      end,

       base.Risk_Segment_4  =                                               -- EPL risk group - Sky loses majority of EPL
      case

          -- ##### Risk group 1 #####
        when base.Risk_Segment_2 in ('Risk group 1') and prof.BT_Sport_Viewier = 'Yes'                                                        then 'Latent risk'
        when base.Risk_Segment_2 in ('Risk group 1') and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 'Downgrade risk'
        when base.Risk_Segment_2 in ('Risk group 1') and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                   then 'Downgrade risk'
        when base.Risk_Segment_2 in ('Risk group 1') and eng.Movies_SOV in ('High')                                                           then 'Downgrade risk'
        when base.Risk_Segment_2 in ('Risk group 1') and prof.Number_Of_Sky_Products_GO_OD >= 6                                               then 'Downgrade risk'
        when base.Risk_Segment_2 in ('Risk group 1') and cl_eng.CL_SOC in ('High')                                                            then 'Churn risk'
        when base.Risk_Segment_2 in ('Risk group 1') and prof.Value_Segment = 'F) Unstable'                                                   then 'Churn risk'
        when base.Risk_Segment_2 in ('Risk group 1') and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 'Churn risk'
        when base.Risk_Segment_2 in ('Risk group 1')                                                                                          then 'Downgrade risk'

          -- ##### Risk group 2 #####
        when base.Risk_Segment_2 in ('Risk group 2') and prof.BT_Sport_Viewier = 'Yes'                                                        then 'Latent risk'
        when base.Risk_Segment_2 in ('Risk group 2') and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 'Downgrade risk'
        when base.Risk_Segment_2 in ('Risk group 2') and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                   then 'Downgrade risk'
        when base.Risk_Segment_2 in ('Risk group 2') and eng.Movies_SOV in ('High')                                                           then 'Downgrade risk'
        when base.Risk_Segment_2 in ('Risk group 2') and prof.Number_Of_Sky_Products_GO_OD >= 6                                               then 'Downgrade risk'
        when base.Risk_Segment_2 in ('Risk group 2') and cl_eng.CL_SOC in ('High')                                                            then 'Churn risk'
        when base.Risk_Segment_2 in ('Risk group 2') and prof.Value_Segment = 'F) Unstable'                                                   then 'Churn risk'
        when base.Risk_Segment_2 in ('Risk group 2') and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 'Churn risk'
        when base.Risk_Segment_2 in ('Risk group 2')                                                                                          then 'No change'

          -- ##### Risk group 3 #####
        when base.Risk_Segment_2 in ('Risk group 3') and prof.BT_Sport_Viewier = 'Yes'                                                        then 'Latent risk'
        when base.Risk_Segment_2 in ('Risk group 3') and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 'No change'
        when base.Risk_Segment_2 in ('Risk group 3') and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                   then 'No change'
        when base.Risk_Segment_2 in ('Risk group 3') and eng.Movies_SOV in ('High')                                                           then 'Downgrade risk'
        when base.Risk_Segment_2 in ('Risk group 3') and prof.Number_Of_Sky_Products_GO_OD >= 6                                               then 'Downgrade risk'
        when base.Risk_Segment_2 in ('Risk group 3') and cl_eng.CL_SOC in ('High')                                                            then 'Downgrade risk'
        when base.Risk_Segment_2 in ('Risk group 3') and prof.Value_Segment = 'F) Unstable'                                                   then 'Downgrade risk'
        when base.Risk_Segment_2 in ('Risk group 3') and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 'Downgrade risk'
        when base.Risk_Segment_2 in ('Risk group 3')                                                                                          then 'Churn risk'

          -- ##### Risk group 4 #####
        when base.Risk_Segment_2 in ('Risk group 4') and prof.BT_Sport_Viewier = 'Yes'                                                        then 'Latent risk'
        when base.Risk_Segment_2 in ('Risk group 4') and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 'No change'
        when base.Risk_Segment_2 in ('Risk group 4') and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                   then 'No change'
        when base.Risk_Segment_2 in ('Risk group 4') and eng.Movies_SOV in ('High')                                                           then 'No change'
        when base.Risk_Segment_2 in ('Risk group 4') and prof.Number_Of_Sky_Products_GO_OD >= 6                                               then 'No change'
        when base.Risk_Segment_2 in ('Risk group 4') and cl_eng.CL_SOC in ('High')                                                            then 'Downgrade risk'
        when base.Risk_Segment_2 in ('Risk group 4') and prof.Value_Segment = 'F) Unstable'                                                   then 'Downgrade risk'
        when base.Risk_Segment_2 in ('Risk group 4') and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 'Downgrade risk'
        when base.Risk_Segment_2 in ('Risk group 4')                                                                                          then 'No change'

          -- ##### Risk group 5 #####
        when base.Risk_Segment_2 in ('Risk group 5') and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                   then 'No change'
        when base.Risk_Segment_2 in ('Risk group 5') and eng.Movies_SOV in ('High')                                                           then 'No change'
        when base.Risk_Segment_2 in ('Risk group 5') and prof.Number_Of_Sky_Products_GO_OD >= 6                                               then 'No change'
        when base.Risk_Segment_2 in ('Risk group 5') and cl_eng.CL_SOC in ('High')                                                            then 'No change'
        when base.Risk_Segment_2 in ('Risk group 5') and prof.Value_Segment = 'F) Unstable'                                                   then 'Downgrade risk'
        when base.Risk_Segment_2 in ('Risk group 5') and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 'Downgrade risk'
        when base.Risk_Segment_2 in ('Risk group 5')                                                                                          then 'No change'

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
  -- ##### V4                                                                                                 #####
  -- ##############################################################################################################
  -- Basic risk groups
update EPL_07_Risk_Groups base
   set base.Risk_Segment_1  =                                               -- Basic risk group - Sky loses EPL In full
      case
        when det.EPL_SoSV in ('High')             and det.Sport_SoV in ('High', 'Very high')                                          then 'Risk group 1'
        when det.EPL_SoSV in ('Low', 'Medium')    and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Medium', 'High') then 'Risk group 2'
        when det.EPL_SoSV in ('Medium', 'High')   and det.Sport_SoV in ('Low', 'Medium')                                              then 'Risk group 3'
        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 'Risk group 4'
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('Low', 'Medium')                                              then 'Risk group 5'
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 'Risk group 5'
          else 'Excluded'
      end,

        base.Risk_Segment_2  =                                               -- Basic risk group - Sky loses majority of EPL
      case
        when det.EPL_SoSV in ('High')             and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 'Risk group 1'

        when det.EPL_SoSV in ('High')             and det.Sport_SoV in ('Low', 'Medium')        and det.EPL_SOC in ('Low')            then 'Risk group 2'
        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 'Risk group 2'

        when det.EPL_SoSV in ('Medium', 'High')                                                 and det.EPL_SOC in ('Medium', 'High') then 'Risk group 3'

        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('Low', 'Medium')        and det.EPL_SOC in ('Low')            then 'Risk group 4'
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 'Risk group 4'

        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('Low', 'Medium')                                              then 'Risk group 5'
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Medium', 'High') then 'Risk group 5'
          else 'Excluded'
      end
  from EPL_04_Eng_Matrix det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and base.Sports_Package = 'Sky Sports'
   and det.Metric = 'Overall';
commit;


  -- EPL risk groups
update EPL_07_Risk_Groups base
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
          -- ##### Risk group 3 #####
        when base.Risk_Segment_1 in ('Risk group 3') then
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
  -- ##### V5 (strings)                                                                                       #####
  -- ##############################################################################################################
update EPL_07_Risk_Groups base
   set base.Risk_Segment_1  =                                               -- Basic risk group - Sky loses EPL In full
      case
        when det.EPL_SoSV in ('High')             and det.Sport_SoV in ('High', 'Very high')                                          then 'Risk group 1'
        when det.EPL_SoSV in ('Low', 'Medium')    and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Medium', 'High') then 'Risk group 2'
        when det.EPL_SoSV in ('Medium', 'High')   and det.Sport_SoV in ('Low', 'Medium')                                              then 'Risk group 3'
        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 'Risk group 4'
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('Low', 'Medium')                                              then 'Risk group 5'
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 'Risk group 5'
          else 'Excluded'
      end,

        base.Risk_Segment_2  =                                               -- Basic risk group - Sky loses majority of EPL
      case
        when det.EPL_SoSV in ('High')             and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 'Risk group 1'

        when det.EPL_SoSV in ('High')             and det.Sport_SoV in ('Low', 'Medium')        and det.EPL_SOC in ('Low')            then 'Risk group 2'
        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 'Risk group 2'

        when det.EPL_SoSV in ('Medium', 'High')                                                 and det.EPL_SOC in ('Medium', 'High') then 'Risk group 3'

        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('Low', 'Medium')        and det.EPL_SOC in ('Low')            then 'Risk group 4'
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 'Risk group 4'

        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('Low', 'Medium')                                              then 'Risk group 5'
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Medium', 'High') then 'Risk group 5'
          else 'Excluded'
      end
  from EPL_04_Eng_Matrix det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and base.Sports_Package = 'Sky Sports'
   and det.Metric = 'Overall';
commit;


update EPL_07_Risk_Groups base
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
  -- ##### V6                                                                                                 #####
  -- #####   - SIG level removed from Risk Group 1 (all EPL lost only)                                        #####
  -- #####   - Risk Group 5 - all outcomes are "No Change" now                                                #####
  -- ##############################################################################################################
update EPL_07_Risk_Groups base
   set base.Risk_Segment_1  =                                               -- Basic risk group - Sky loses EPL In full
      case
        when det.EPL_SoSV in ('High')             and det.Sport_SoV in ('High', 'Very high')                                          then 5
        when det.EPL_SoSV in ('High')             and det.Sport_SoV in ('Low', 'Medium')                                              then 6

        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('Low', 'Medium')                                              then 7
        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 11
        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Medium', 'High') then 12

        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Medium', 'High') then 13
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 14
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('Low', 'Medium')                                              then 10
          else 0
      end,

        base.Risk_Segment_2  =                                               -- Basic risk group - Sky loses majority of EPL
      case
        when det.EPL_SoSV in ('High')             and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 11
        when det.EPL_SoSV in ('High')             and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Medium', 'High') then 12
        when det.EPL_SoSV in ('High')             and det.Sport_SoV in ('Low', 'Medium')        and det.EPL_SOC in ('Medium', 'High') then 13
        when det.EPL_SoSV in ('High')             and det.Sport_SoV in ('Low', 'Medium')        and det.EPL_SOC in ('Low')            then 14

        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('Low', 'Medium')        and det.EPL_SOC in ('Medium', 'High') then 15
        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('Low', 'Medium')        and det.EPL_SOC in ('Low')            then 16
        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Medium', 'High') then 17
        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 18

        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 19
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Medium', 'High') then 20
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('Low', 'Medium')                                              then 10
          else 0
      end
  from EPL_04_Eng_Matrix det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and base.Sports_Package = 'Sky Sports'
   and det.Metric = 'Overall';
commit;


-- This view take information from fields updated above AND below, eeds to be updated only once though.
create or replace view EPL_07_Risk_Groups_View as
  select
        a.Account_Number,
        a.Period,
        a.Sports_Package,

        -- EPL risk group - Sky loses EPL In full
        a.Risk_Segment_1 as Risk_Segment_1_Raw,
        case
          when a.Risk_Segment_1 in (5)          then 'Risk group 1'
          when a.Risk_Segment_1 in (12, 13)     then 'Risk group 2'
          when a.Risk_Segment_1 in (6, 7)       then 'Risk group 3'
          when a.Risk_Segment_1 in (11)         then 'Risk group 4'
          when a.Risk_Segment_1 in (10, 14)     then 'Risk group 5'
          when a.Risk_Segment_1 in (0)          then 'Excluded'
            else 'No Sky Sports'
        end as xRisk_Segment_1,

        case
          when xRisk_Segment_1 = 'Risk group 1' and a.Risk_Segment_3 in (105, 107, 109, 111)                                        then 'No change'
          when xRisk_Segment_1 = 'Risk group 1' and a.Risk_Segment_3 in (113, 114, 115, 161, 163, 164, 165)                         then 'Churn risk'
          when xRisk_Segment_1 = 'Risk group 1' and a.Risk_Segment_3 in (155, 157, 159)                                             then 'Downgrade risk'
          when xRisk_Segment_1 = 'Risk group 1'                                                                                     then 'Excluded'

          when xRisk_Segment_1 = 'Risk group 2' and a.Risk_Segment_3 in (203, 205, 207, 209, 211, 213, 253)                         then 'No change'
          when xRisk_Segment_1 = 'Risk group 2' and a.Risk_Segment_3 in (215, 216, 217, 263, 265, 266, 267)                         then 'Churn risk'
          when xRisk_Segment_1 = 'Risk group 2' and a.Risk_Segment_3 in (255, 257, 259, 261)                                        then 'Downgrade risk'
          when xRisk_Segment_1 = 'Risk group 2'                                                                                     then 'Excluded'

          when xRisk_Segment_1 = 'Risk group 3' and a.Risk_Segment_3 in (303, 305, 307, 309, 311, 313, 353)                         then 'No change'
          when xRisk_Segment_1 = 'Risk group 3' and a.Risk_Segment_3 in (315, 316, 317, 365, 366, 367)                              then 'Churn risk'
          when xRisk_Segment_1 = 'Risk group 3' and a.Risk_Segment_3 in (355, 357, 359, 361, 363)                                   then 'Downgrade risk'
          when xRisk_Segment_1 = 'Risk group 3'                                                                                     then 'Excluded'

          when xRisk_Segment_1 = 'Risk group 4' and a.Risk_Segment_3 in (403, 405, 407, 409, 411, 414, 453, 455, 457, 459, 464)     then 'No change'
          when xRisk_Segment_1 = 'Risk group 4' and a.Risk_Segment_3 in (413, 415, 461, 463, 465)                                   then 'Downgrade risk'
          when xRisk_Segment_1 = 'Risk group 4'                                                                                     then 'Excluded'

          when xRisk_Segment_1 = 'Risk group 5' and a.Risk_Segment_3 in (502, 504, 506, 508, 510, 511, 512)                         then 'No risk'
          when xRisk_Segment_1 = 'Risk group 5'                                                                                     then 'Excluded'

            else 'No Sky Sports'
        end as xRisk_Segment_3,


        -- EPL risk group - Sky loses majority of EPL
        a.Risk_Segment_2 as Risk_Segment_2_Raw,
        case
          when a.Risk_Segment_2 in (11)             then 'Risk group 1'
          when a.Risk_Segment_2 in (14, 18)         then 'Risk group 2'
          when a.Risk_Segment_2 in (12, 13, 15, 17) then 'Risk group 3'
          when a.Risk_Segment_2 in (16, 19)         then 'Risk group 4'
          when a.Risk_Segment_2 in (10, 20)         then 'Risk group 5'
          when a.Risk_Segment_2 in (0)              then 'Excluded'
            else 'No Sky Sports'
        end as xRisk_Segment_2,

        case
          when xRisk_Segment_2 = 'Risk group 1' and a.Risk_Segment_4 in (103, 105, 107, 109, 111)                                   then 'No change'
          when xRisk_Segment_2 = 'Risk group 1' and a.Risk_Segment_4 in (161, 163, 165)                                             then 'Churn risk'
          when xRisk_Segment_2 = 'Risk group 1' and a.Risk_Segment_4 in (113, 114, 115, 153, 155, 157, 159, 164)                    then 'Downgrade risk'
          when xRisk_Segment_2 = 'Risk group 1'                                                                                     then 'Excluded'

          when xRisk_Segment_2 = 'Risk group 2' and a.Risk_Segment_4 in (203, 205, 207, 209, 211, 214, 264)                         then 'No change'
          when xRisk_Segment_2 = 'Risk group 2' and a.Risk_Segment_4 in (213, 215, 261, 263, 265)                                   then 'Churn risk'
          when xRisk_Segment_2 = 'Risk group 2' and a.Risk_Segment_4 in (253, 255, 257, 259)                                        then 'Downgrade risk'
          when xRisk_Segment_2 = 'Risk group 2'                                                                                     then 'Excluded'

          when xRisk_Segment_2 = 'Risk group 3' and a.Risk_Segment_4 in (303, 305, 307, 309, 311, 353, 355)                         then 'No change'
          when xRisk_Segment_2 = 'Risk group 3' and a.Risk_Segment_4 in (313, 314, 364)                                             then 'Churn risk'
          when xRisk_Segment_2 = 'Risk group 3' and a.Risk_Segment_4 in (315, 357, 359, 361, 363, 365)                              then 'Downgrade risk'
          when xRisk_Segment_2 = 'Risk group 3'                                                                                     then 'Excluded'

          when xRisk_Segment_2 = 'Risk group 4' and a.Risk_Segment_4 in (403, 405, 407, 409, 411, 414, 453, 455, 457, 459, 464)     then 'No change'
          when xRisk_Segment_2 = 'Risk group 4' and a.Risk_Segment_4 in (413, 415, 461, 463, 465)                                   then 'Downgrade risk'
          when xRisk_Segment_2 = 'Risk group 4'                                                                                     then 'Excluded'

          when xRisk_Segment_1 = 'Risk group 5' and a.Risk_Segment_3 in (502, 504, 506, 508, 510, 511, 512)                         then 'No risk'
          when xRisk_Segment_2 = 'Risk group 5'                                                                                     then 'Excluded'

            else 'No Sky Sports'
        end as xRisk_Segment_4

    from EPL_07_Risk_Groups a;
commit;


  -- EPL risk groups
update EPL_07_Risk_Groups a
   set a.Risk_Segment_3  =                                                  -- EPL risk group - Sky loses EPL In full
      case

          -- #######################################################################################################
          -- ##### Risk group 1 #####
        when base.xRisk_Segment_1 in ('Risk group 1') then
            case
            -- ===== BT Sport =====
                when prof.BT_Sport_Viewier = 'Yes' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                     then 105
                when prof.BT_Sport_Viewier = 'Yes' and eng.Movies_SOV in ('High')                                                             then 107
                when prof.BT_Sport_Viewier = 'Yes' and prof.Number_Of_Sky_Products_GO_OD >= 6                                                 then 109
                when prof.BT_Sport_Viewier = 'Yes' and cl_eng.CL_SOC in ('High')                                                              then 111
                when prof.BT_Sport_Viewier = 'Yes' and prof.Value_Segment = 'F) Unstable'                                                     then 113
                when prof.BT_Sport_Viewier = 'Yes' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 115
                when prof.BT_Sport_Viewier = 'Yes'                                                                                            then 114

            -- ===== Non-BT Sport =====
                when prof.BT_Sport_Viewier = 'No' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                      then 155
                when prof.BT_Sport_Viewier = 'No' and eng.Movies_SOV in ('High')                                                              then 157
                when prof.BT_Sport_Viewier = 'No' and prof.Number_Of_Sky_Products_GO_OD >= 6                                                  then 159
                when prof.BT_Sport_Viewier = 'No' and cl_eng.CL_SOC in ('High')                                                               then 161
                when prof.BT_Sport_Viewier = 'No' and prof.Value_Segment = 'F) Unstable'                                                      then 163
                when prof.BT_Sport_Viewier = 'No' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 165
                when prof.BT_Sport_Viewier = 'No'                                                                                             then 164
            end

          -- #######################################################################################################
          -- ##### Risk group 2 #####
        when base.xRisk_Segment_1 in ('Risk group 2') then
            case
            -- ===== BT Sport =====
                when prof.BT_Sport_Viewier = 'Yes' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 203
                when prof.BT_Sport_Viewier = 'Yes' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                     then 205
                when prof.BT_Sport_Viewier = 'Yes' and eng.Sky_Branded_Channels in ('High')                                                   then 207
                when prof.BT_Sport_Viewier = 'Yes' and eng.Movies_SOV in ('High')                                                             then 209
                when prof.BT_Sport_Viewier = 'Yes' and prof.Number_Of_Sky_Products_GO_OD >= 6                                                 then 211
                when prof.BT_Sport_Viewier = 'Yes' and cl_eng.CL_SOC in ('High')                                                              then 213
                when prof.BT_Sport_Viewier = 'Yes' and prof.Value_Segment = 'F) Unstable'                                                     then 215
                when prof.BT_Sport_Viewier = 'Yes' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 217
                when prof.BT_Sport_Viewier = 'Yes'                                                                                            then 216

            -- ===== Non-BT Sport =====
                when prof.BT_Sport_Viewier = 'No' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 253
                when prof.BT_Sport_Viewier = 'No' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                      then 255
                when prof.BT_Sport_Viewier = 'No' and eng.Sky_Branded_Channels in ('High')                                                    then 257
                when prof.BT_Sport_Viewier = 'No' and eng.Movies_SOV in ('High')                                                              then 259
                when prof.BT_Sport_Viewier = 'No' and prof.Number_Of_Sky_Products_GO_OD >= 6                                                  then 261
                when prof.BT_Sport_Viewier = 'No' and cl_eng.CL_SOC in ('High')                                                               then 263
                when prof.BT_Sport_Viewier = 'No' and prof.Value_Segment = 'F) Unstable'                                                      then 265
                when prof.BT_Sport_Viewier = 'No' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 267
                when prof.BT_Sport_Viewier = 'No'                                                                                             then 266
            end

          -- #######################################################################################################
          -- ##### Risk group 3 #####
        when base.xRisk_Segment_1 in ('Risk group 3') then
            case
            -- ===== BT Sport =====
                when prof.BT_Sport_Viewier = 'Yes' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 303
                when prof.BT_Sport_Viewier = 'Yes' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                     then 305
                when prof.BT_Sport_Viewier = 'Yes' and eng.Sky_Branded_Channels in ('High')                                                   then 307
                when prof.BT_Sport_Viewier = 'Yes' and eng.Movies_SOV in ('High')                                                             then 309
                when prof.BT_Sport_Viewier = 'Yes' and prof.Number_Of_Sky_Products_GO_OD >= 6                                                 then 311
                when prof.BT_Sport_Viewier = 'Yes' and cl_eng.CL_SOC in ('High')                                                              then 313
                when prof.BT_Sport_Viewier = 'Yes' and prof.Value_Segment = 'F) Unstable'                                                     then 315
                when prof.BT_Sport_Viewier = 'Yes' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 317
                when prof.BT_Sport_Viewier = 'Yes'                                                                                            then 316

            -- ===== Non-BT Sport =====
                when prof.BT_Sport_Viewier = 'No' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 353
                when prof.BT_Sport_Viewier = 'No' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                      then 355
                when prof.BT_Sport_Viewier = 'No' and eng.Sky_Branded_Channels in ('High')                                                    then 357
                when prof.BT_Sport_Viewier = 'No' and eng.Movies_SOV in ('High')                                                              then 359
                when prof.BT_Sport_Viewier = 'No' and prof.Number_Of_Sky_Products_GO_OD >= 6                                                  then 361
                when prof.BT_Sport_Viewier = 'No' and cl_eng.CL_SOC in ('High')                                                               then 363
                when prof.BT_Sport_Viewier = 'No' and prof.Value_Segment = 'F) Unstable'                                                      then 365
                when prof.BT_Sport_Viewier = 'No' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 367
                when prof.BT_Sport_Viewier = 'No'                                                                                             then 366
            end

          -- #######################################################################################################
          -- ##### Risk group 4 #####
        when base.xRisk_Segment_1 in ('Risk group 4') then
            case
            -- ===== BT Sport =====
                when prof.BT_Sport_Viewier = 'Yes' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 403
                when prof.BT_Sport_Viewier = 'Yes' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                     then 405
                when prof.BT_Sport_Viewier = 'Yes' and eng.Movies_SOV in ('High')                                                             then 407
                when prof.BT_Sport_Viewier = 'Yes' and prof.Number_Of_Sky_Products_GO_OD >= 6                                                 then 409
                when prof.BT_Sport_Viewier = 'Yes' and cl_eng.CL_SOC in ('High')                                                              then 411
                when prof.BT_Sport_Viewier = 'Yes' and prof.Value_Segment = 'F) Unstable'                                                     then 413
                when prof.BT_Sport_Viewier = 'Yes' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 415
                when prof.BT_Sport_Viewier = 'Yes'                                                                                            then 414

            -- ===== Non-BT Sport =====
                when prof.BT_Sport_Viewier = 'No' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 453
                when prof.BT_Sport_Viewier = 'No' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                      then 455
                when prof.BT_Sport_Viewier = 'No' and eng.Movies_SOV in ('High')                                                              then 457
                when prof.BT_Sport_Viewier = 'No' and prof.Number_Of_Sky_Products_GO_OD >= 6                                                  then 459
                when prof.BT_Sport_Viewier = 'No' and cl_eng.CL_SOC in ('High')                                                               then 461
                when prof.BT_Sport_Viewier = 'No' and prof.Value_Segment = 'F) Unstable'                                                      then 463
                when prof.BT_Sport_Viewier = 'No' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 465
                when prof.BT_Sport_Viewier = 'No'                                                                                             then 464
            end

          -- #######################################################################################################
          -- ##### Risk group 5 #####
        when base.xRisk_Segment_1 in ('Risk group 5') then
            case

                when                                  eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                      then 502
                when                                  eng.Movies_SOV in ('High')                                                              then 504
                when                                  prof.Number_Of_Sky_Products_GO_OD >= 6                                                  then 506
                when                                  cl_eng.CL_SOC in ('High')                                                               then 508
                when                                  prof.Value_Segment = 'F) Unstable'                                                      then 510
                when                                  prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 512
                  else                                                                                                                             511
            end

          -- #######################################################################################################
          -- ##### Excluded #####
          else 0

      end,


       a.Risk_Segment_4  =                                                  -- EPL risk group - Sky loses majority of EPL
      case

          -- #######################################################################################################
          -- ##### Risk group 1 #####
        when base.xRisk_Segment_2 in ('Risk group 1') then
            case
            -- ===== BT Sport =====
                when prof.BT_Sport_Viewier = 'Yes' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 103
                when prof.BT_Sport_Viewier = 'Yes' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                     then 105
                when prof.BT_Sport_Viewier = 'Yes' and eng.Movies_SOV in ('High')                                                             then 107
                when prof.BT_Sport_Viewier = 'Yes' and prof.Number_Of_Sky_Products_GO_OD >= 6                                                 then 109
                when prof.BT_Sport_Viewier = 'Yes' and cl_eng.CL_SOC in ('High')                                                              then 111
                when prof.BT_Sport_Viewier = 'Yes' and prof.Value_Segment = 'F) Unstable'                                                     then 113
                when prof.BT_Sport_Viewier = 'Yes' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 115
                when prof.BT_Sport_Viewier = 'Yes'                                                                                            then 114

            -- ===== Non-BT Sport =====
                when prof.BT_Sport_Viewier = 'No' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 153
                when prof.BT_Sport_Viewier = 'No' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                      then 155
                when prof.BT_Sport_Viewier = 'No' and eng.Movies_SOV in ('High')                                                              then 157
                when prof.BT_Sport_Viewier = 'No' and prof.Number_Of_Sky_Products_GO_OD >= 6                                                  then 159
                when prof.BT_Sport_Viewier = 'No' and cl_eng.CL_SOC in ('High')                                                               then 161
                when prof.BT_Sport_Viewier = 'No' and prof.Value_Segment = 'F) Unstable'                                                      then 163
                when prof.BT_Sport_Viewier = 'No' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 165
                when prof.BT_Sport_Viewier = 'No'                                                                                             then 164
            end

          -- #######################################################################################################
          -- ##### Risk group 2 #####
        when base.xRisk_Segment_2 in ('Risk group 2') then
            case
            -- ===== BT Sport =====
                when prof.BT_Sport_Viewier = 'Yes' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 203
                when prof.BT_Sport_Viewier = 'Yes' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                     then 205
                when prof.BT_Sport_Viewier = 'Yes' and eng.Movies_SOV in ('High')                                                             then 207
                when prof.BT_Sport_Viewier = 'Yes' and prof.Number_Of_Sky_Products_GO_OD >= 6                                                 then 209
                when prof.BT_Sport_Viewier = 'Yes' and cl_eng.CL_SOC in ('High')                                                              then 211
                when prof.BT_Sport_Viewier = 'Yes' and prof.Value_Segment = 'F) Unstable'                                                     then 213
                when prof.BT_Sport_Viewier = 'Yes' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 215
                when prof.BT_Sport_Viewier = 'Yes'                                                                                            then 214

            -- ===== Non-BT Sport =====
                when prof.BT_Sport_Viewier = 'No' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 253
                when prof.BT_Sport_Viewier = 'No' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                      then 255
                when prof.BT_Sport_Viewier = 'No' and eng.Movies_SOV in ('High')                                                              then 257
                when prof.BT_Sport_Viewier = 'No' and prof.Number_Of_Sky_Products_GO_OD >= 6                                                  then 259
                when prof.BT_Sport_Viewier = 'No' and cl_eng.CL_SOC in ('High')                                                               then 261
                when prof.BT_Sport_Viewier = 'No' and prof.Value_Segment = 'F) Unstable'                                                      then 263
                when prof.BT_Sport_Viewier = 'No' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 265
                when prof.BT_Sport_Viewier = 'No'                                                                                             then 264
            end

          -- #######################################################################################################
          -- ##### Risk group 3 #####
        when base.xRisk_Segment_2 in ('Risk group 3') then
            case
            -- ===== BT Sport =====
                when prof.BT_Sport_Viewier = 'Yes' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 303
                when prof.BT_Sport_Viewier = 'Yes' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                     then 305
                when prof.BT_Sport_Viewier = 'Yes' and eng.Movies_SOV in ('High')                                                             then 307
                when prof.BT_Sport_Viewier = 'Yes' and prof.Number_Of_Sky_Products_GO_OD >= 6                                                 then 309
                when prof.BT_Sport_Viewier = 'Yes' and cl_eng.CL_SOC in ('High')                                                              then 311
                when prof.BT_Sport_Viewier = 'Yes' and prof.Value_Segment = 'F) Unstable'                                                     then 313
                when prof.BT_Sport_Viewier = 'Yes' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 315
                when prof.BT_Sport_Viewier = 'Yes'                                                                                            then 314

            -- ===== Non-BT Sport =====
                when prof.BT_Sport_Viewier = 'No' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 353
                when prof.BT_Sport_Viewier = 'No' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                      then 355
                when prof.BT_Sport_Viewier = 'No' and eng.Movies_SOV in ('High')                                                              then 357
                when prof.BT_Sport_Viewier = 'No' and prof.Number_Of_Sky_Products_GO_OD >= 6                                                  then 359
                when prof.BT_Sport_Viewier = 'No' and cl_eng.CL_SOC in ('High')                                                               then 361
                when prof.BT_Sport_Viewier = 'No' and prof.Value_Segment = 'F) Unstable'                                                      then 363
                when prof.BT_Sport_Viewier = 'No' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 365
                when prof.BT_Sport_Viewier = 'No'                                                                                             then 364
            end

          -- #######################################################################################################
          -- ##### Risk group 4 #####
        when base.xRisk_Segment_2 in ('Risk group 4') then
            case
            -- ===== BT Sport =====
                when prof.BT_Sport_Viewier = 'Yes' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 403
                when prof.BT_Sport_Viewier = 'Yes' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                     then 405
                when prof.BT_Sport_Viewier = 'Yes' and eng.Movies_SOV in ('High')                                                             then 407
                when prof.BT_Sport_Viewier = 'Yes' and prof.Number_Of_Sky_Products_GO_OD >= 6                                                 then 409
                when prof.BT_Sport_Viewier = 'Yes' and cl_eng.CL_SOC in ('High')                                                              then 411
                when prof.BT_Sport_Viewier = 'Yes' and prof.Value_Segment = 'F) Unstable'                                                     then 413
                when prof.BT_Sport_Viewier = 'Yes' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 415
                when prof.BT_Sport_Viewier = 'Yes'                                                                                            then 414

            -- ===== Non-BT Sport =====
                when prof.BT_Sport_Viewier = 'No' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 453
                when prof.BT_Sport_Viewier = 'No' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                      then 455
                when prof.BT_Sport_Viewier = 'No' and eng.Movies_SOV in ('High')                                                              then 457
                when prof.BT_Sport_Viewier = 'No' and prof.Number_Of_Sky_Products_GO_OD >= 6                                                  then 459
                when prof.BT_Sport_Viewier = 'No' and cl_eng.CL_SOC in ('High')                                                               then 461
                when prof.BT_Sport_Viewier = 'No' and prof.Value_Segment = 'F) Unstable'                                                      then 463
                when prof.BT_Sport_Viewier = 'No' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 465
                when prof.BT_Sport_Viewier = 'No'                                                                                             then 464
            end

          -- #######################################################################################################
          -- ##### Risk group 5 #####
        when base.xRisk_Segment_2 in ('Risk group 5') then
            case

                when                                  eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                      then 502
                when                                  eng.Movies_SOV in ('High')                                                              then 504
                when                                  prof.Number_Of_Sky_Products_GO_OD >= 6                                                  then 506
                when                                  cl_eng.CL_SOC in ('High')                                                               then 508
                when                                  prof.Value_Segment = 'F) Unstable'                                                      then 510
                when                                  prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 512
                  else                                                                                                                             511
            end

          -- #######################################################################################################
          -- ##### Excluded #####
          else 0

      end

  from EPL_07_Risk_Groups_View base,
       EPL_04_Eng_Matrix eng,
       EPL_04_Profiling_Variables prof,
       EPL_54_CL_Eng_Matrix cl_eng

 where a.Account_Number = base.Account_Number
   and a.Period = base.Period
   and a.Sports_Package = 'Sky Sports'

   and a.Account_Number = eng.Account_Number
   and a.Period = eng.Period
   and eng.Metric = 'Overall'

   and a.Account_Number = prof.Account_Number
   and a.Period = prof.Period

   and a.Account_Number = cl_eng.Account_Number
   and a.Period = cl_eng.Period
   and cl_eng.Metric = 'Overall';
commit;



  -- ##############################################################################################################
  -- ##### V7                                                                                                 #####
  -- #####   - Number of products EXCLUDE broadband                                                           #####
  -- ##############################################################################################################
update EPL_07_Risk_Groups base
   set base.Risk_Segment_1  =                                               -- Basic risk group - Sky loses EPL In full
      case
        when det.EPL_SoSV in ('High')             and det.Sport_SoV in ('High', 'Very high')                                          then 5
        when det.EPL_SoSV in ('High')             and det.Sport_SoV in ('Low', 'Medium')                                              then 6

        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('Low', 'Medium')                                              then 7
        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 11
        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Medium', 'High') then 12

        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Medium', 'High') then 13
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 14
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('Low', 'Medium')                                              then 10
          else 0
      end,

        base.Risk_Segment_2  =                                               -- Basic risk group - Sky loses majority of EPL
      case
        when det.EPL_SoSV in ('High')             and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 11
        when det.EPL_SoSV in ('High')             and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Medium', 'High') then 12
        when det.EPL_SoSV in ('High')             and det.Sport_SoV in ('Low', 'Medium')        and det.EPL_SOC in ('Medium', 'High') then 13
        when det.EPL_SoSV in ('High')             and det.Sport_SoV in ('Low', 'Medium')        and det.EPL_SOC in ('Low')            then 14

        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('Low', 'Medium')        and det.EPL_SOC in ('Medium', 'High') then 15
        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('Low', 'Medium')        and det.EPL_SOC in ('Low')            then 16
        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Medium', 'High') then 17
        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 18

        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 19
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Medium', 'High') then 20
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('Low', 'Medium')                                              then 10
          else 0
      end
  from EPL_04_Eng_Matrix det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and base.Sports_Package = 'Sky Sports'
   and det.Metric = 'Overall';
commit;


-- This view take information from fields updated above AND below, eeds to be updated only once though.
create or replace view EPL_07_Risk_Groups_View as
  select
        a.Account_Number,
        a.Period,
        a.Sports_Package,

        -- EPL risk group - Sky loses EPL In full
        a.Risk_Segment_1 as Risk_Segment_1_Raw,
        case
          when a.Risk_Segment_1 in (5)          then 'Risk group 1'
          when a.Risk_Segment_1 in (12, 13)     then 'Risk group 2'
          when a.Risk_Segment_1 in (6, 7)       then 'Risk group 3'
          when a.Risk_Segment_1 in (11)         then 'Risk group 4'
          when a.Risk_Segment_1 in (10, 14)     then 'Risk group 5'
          when a.Risk_Segment_1 in (0)          then 'Excluded'
            else 'No Sky Sports'
        end as xRisk_Segment_1,

        case
          when xRisk_Segment_1 = 'Excluded'                                                                                         then 'Excluded'

          when xRisk_Segment_1 = 'Risk group 1' and a.Risk_Segment_3 in (105, 107, 109, 111)                                        then 'No change'
          when xRisk_Segment_1 = 'Risk group 1' and a.Risk_Segment_3 in (113, 114, 115, 161, 163, 164, 165)                         then 'Churn risk'
          when xRisk_Segment_1 = 'Risk group 1' and a.Risk_Segment_3 in (155, 157, 159)                                             then 'Downgrade risk'
          when xRisk_Segment_1 = 'Risk group 1'                                                                                     then 'Excluded'

          when xRisk_Segment_1 = 'Risk group 2' and a.Risk_Segment_3 in (203, 205, 207, 209, 211, 213, 253)                         then 'No change'
          when xRisk_Segment_1 = 'Risk group 2' and a.Risk_Segment_3 in (215, 216, 217, 263, 265, 266, 267)                         then 'Churn risk'
          when xRisk_Segment_1 = 'Risk group 2' and a.Risk_Segment_3 in (255, 257, 259, 261)                                        then 'Downgrade risk'
          when xRisk_Segment_1 = 'Risk group 2'                                                                                     then 'Excluded'

          when xRisk_Segment_1 = 'Risk group 3' and a.Risk_Segment_3 in (303, 305, 307, 309, 311, 313, 353)                         then 'No change'
          when xRisk_Segment_1 = 'Risk group 3' and a.Risk_Segment_3 in (315, 316, 317, 365, 366, 367)                              then 'Churn risk'
          when xRisk_Segment_1 = 'Risk group 3' and a.Risk_Segment_3 in (355, 357, 359, 361, 363)                                   then 'Downgrade risk'
          when xRisk_Segment_1 = 'Risk group 3'                                                                                     then 'Excluded'

          when xRisk_Segment_1 = 'Risk group 4' and a.Risk_Segment_3 in (403, 405, 407, 409, 411, 414, 453, 455, 457, 459, 464)     then 'No change'
          when xRisk_Segment_1 = 'Risk group 4' and a.Risk_Segment_3 in (413, 415, 461, 463, 465)                                   then 'Downgrade risk'
          when xRisk_Segment_1 = 'Risk group 4'                                                                                     then 'Excluded'

          when xRisk_Segment_1 = 'Risk group 5' and a.Risk_Segment_3 in (502, 504, 506, 508, 510, 511, 512)                         then 'No change'
          when xRisk_Segment_1 = 'Risk group 5'                                                                                     then 'Excluded'

            else 'No Sky Sports'
        end as xRisk_Segment_3,


        -- EPL risk group - Sky loses majority of EPL
        a.Risk_Segment_2 as Risk_Segment_2_Raw,
        case
          when a.Risk_Segment_2 in (11)             then 'Risk group 1'
          when a.Risk_Segment_2 in (14, 18)         then 'Risk group 2'
          when a.Risk_Segment_2 in (12, 13, 15, 17) then 'Risk group 3'
          when a.Risk_Segment_2 in (16, 19)         then 'Risk group 4'
          when a.Risk_Segment_2 in (10, 20)         then 'Risk group 5'
          when a.Risk_Segment_2 in (0)              then 'Excluded'
            else 'No Sky Sports'
        end as xRisk_Segment_2,

        case
          when xRisk_Segment_2 = 'Excluded'                                                                                         then 'Excluded'

          when xRisk_Segment_2 = 'Risk group 1' and a.Risk_Segment_4 in (103, 105, 107, 109, 111)                                   then 'No change'
          when xRisk_Segment_2 = 'Risk group 1' and a.Risk_Segment_4 in (161, 163, 165)                                             then 'Churn risk'
          when xRisk_Segment_2 = 'Risk group 1' and a.Risk_Segment_4 in (113, 114, 115, 153, 155, 157, 159, 164)                    then 'Downgrade risk'
          when xRisk_Segment_2 = 'Risk group 1'                                                                                     then 'Excluded'

          when xRisk_Segment_2 = 'Risk group 2' and a.Risk_Segment_4 in (203, 205, 207, 209, 211, 214, 264)                         then 'No change'
          when xRisk_Segment_2 = 'Risk group 2' and a.Risk_Segment_4 in (213, 215, 261, 263, 265)                                   then 'Churn risk'
          when xRisk_Segment_2 = 'Risk group 2' and a.Risk_Segment_4 in (253, 255, 257, 259)                                        then 'Downgrade risk'
          when xRisk_Segment_2 = 'Risk group 2'                                                                                     then 'Excluded'

          when xRisk_Segment_2 = 'Risk group 3' and a.Risk_Segment_4 in (303, 305, 307, 309, 311, 353, 355)                         then 'No change'
          when xRisk_Segment_2 = 'Risk group 3' and a.Risk_Segment_4 in (313, 314, 364)                                             then 'Churn risk'
          when xRisk_Segment_2 = 'Risk group 3' and a.Risk_Segment_4 in (315, 357, 359, 361, 363, 365)                              then 'Downgrade risk'
          when xRisk_Segment_2 = 'Risk group 3'                                                                                     then 'Excluded'

          when xRisk_Segment_2 = 'Risk group 4' and a.Risk_Segment_4 in (403, 405, 407, 409, 411, 414, 453, 455, 457, 459, 464)     then 'No change'
          when xRisk_Segment_2 = 'Risk group 4' and a.Risk_Segment_4 in (413, 415, 461, 463, 465)                                   then 'Downgrade risk'
          when xRisk_Segment_2 = 'Risk group 4'                                                                                     then 'Excluded'

          when xRisk_Segment_2 = 'Risk group 5' and a.Risk_Segment_4 in (502, 504, 506, 508, 510, 511, 512)                         then 'No change'
          when xRisk_Segment_2 = 'Risk group 5'                                                                                     then 'Excluded'

            else 'No Sky Sports'
        end as xRisk_Segment_4

    from EPL_07_Risk_Groups a;
commit;


  -- EPL risk groups
update EPL_07_Risk_Groups a
   set a.Risk_Segment_3  =                                                  -- EPL risk group - Sky loses EPL In full
      case

          -- #######################################################################################################
          -- ##### Risk group 1 #####
        when base.xRisk_Segment_1 in ('Risk group 1') then
            case
            -- ===== BT Sport =====
                when prof.BT_Sport_Viewier = 'Yes' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                     then 105
                when prof.BT_Sport_Viewier = 'Yes' and eng.Movies_SOV in ('High')                                                             then 107
                when prof.BT_Sport_Viewier = 'Yes' and prof.Number_Of_Sky_Products_No_BB >= 5                                                 then 109
                when prof.BT_Sport_Viewier = 'Yes' and cl_eng.CL_SOC in ('High')                                                              then 111
                when prof.BT_Sport_Viewier = 'Yes' and prof.Value_Segment = 'F) Unstable'                                                     then 113
                when prof.BT_Sport_Viewier = 'Yes' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 115
                when prof.BT_Sport_Viewier = 'Yes'                                                                                            then 114

            -- ===== Non-BT Sport =====
                when prof.BT_Sport_Viewier = 'No' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                      then 155
                when prof.BT_Sport_Viewier = 'No' and eng.Movies_SOV in ('High')                                                              then 157
                when prof.BT_Sport_Viewier = 'No' and prof.Number_Of_Sky_Products_No_BB >= 5                                                  then 159
                when prof.BT_Sport_Viewier = 'No' and cl_eng.CL_SOC in ('High')                                                               then 161
                when prof.BT_Sport_Viewier = 'No' and prof.Value_Segment = 'F) Unstable'                                                      then 163
                when prof.BT_Sport_Viewier = 'No' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 165
                when prof.BT_Sport_Viewier = 'No'                                                                                             then 164
            end

          -- #######################################################################################################
          -- ##### Risk group 2 #####
        when base.xRisk_Segment_1 in ('Risk group 2') then
            case
            -- ===== BT Sport =====
                when prof.BT_Sport_Viewier = 'Yes' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 203
                when prof.BT_Sport_Viewier = 'Yes' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                     then 205
                when prof.BT_Sport_Viewier = 'Yes' and eng.Sky_Branded_Channels in ('High')                                                   then 207
                when prof.BT_Sport_Viewier = 'Yes' and eng.Movies_SOV in ('High')                                                             then 209
                when prof.BT_Sport_Viewier = 'Yes' and prof.Number_Of_Sky_Products_No_BB >= 5                                                 then 211
                when prof.BT_Sport_Viewier = 'Yes' and cl_eng.CL_SOC in ('High')                                                              then 213
                when prof.BT_Sport_Viewier = 'Yes' and prof.Value_Segment = 'F) Unstable'                                                     then 215
                when prof.BT_Sport_Viewier = 'Yes' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 217
                when prof.BT_Sport_Viewier = 'Yes'                                                                                            then 216

            -- ===== Non-BT Sport =====
                when prof.BT_Sport_Viewier = 'No' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 253
                when prof.BT_Sport_Viewier = 'No' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                      then 255
                when prof.BT_Sport_Viewier = 'No' and eng.Sky_Branded_Channels in ('High')                                                    then 257
                when prof.BT_Sport_Viewier = 'No' and eng.Movies_SOV in ('High')                                                              then 259
                when prof.BT_Sport_Viewier = 'No' and prof.Number_Of_Sky_Products_No_BB >= 5                                                  then 261
                when prof.BT_Sport_Viewier = 'No' and cl_eng.CL_SOC in ('High')                                                               then 263
                when prof.BT_Sport_Viewier = 'No' and prof.Value_Segment = 'F) Unstable'                                                      then 265
                when prof.BT_Sport_Viewier = 'No' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 267
                when prof.BT_Sport_Viewier = 'No'                                                                                             then 266
            end

          -- #######################################################################################################
          -- ##### Risk group 3 #####
        when base.xRisk_Segment_1 in ('Risk group 3') then
            case
            -- ===== BT Sport =====
                when prof.BT_Sport_Viewier = 'Yes' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 303
                when prof.BT_Sport_Viewier = 'Yes' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                     then 305
                when prof.BT_Sport_Viewier = 'Yes' and eng.Sky_Branded_Channels in ('High')                                                   then 307
                when prof.BT_Sport_Viewier = 'Yes' and eng.Movies_SOV in ('High')                                                             then 309
                when prof.BT_Sport_Viewier = 'Yes' and prof.Number_Of_Sky_Products_No_BB >= 5                                                 then 311
                when prof.BT_Sport_Viewier = 'Yes' and cl_eng.CL_SOC in ('High')                                                              then 313
                when prof.BT_Sport_Viewier = 'Yes' and prof.Value_Segment = 'F) Unstable'                                                     then 315
                when prof.BT_Sport_Viewier = 'Yes' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 317
                when prof.BT_Sport_Viewier = 'Yes'                                                                                            then 316

            -- ===== Non-BT Sport =====
                when prof.BT_Sport_Viewier = 'No' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 353
                when prof.BT_Sport_Viewier = 'No' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                      then 355
                when prof.BT_Sport_Viewier = 'No' and eng.Sky_Branded_Channels in ('High')                                                    then 357
                when prof.BT_Sport_Viewier = 'No' and eng.Movies_SOV in ('High')                                                              then 359
                when prof.BT_Sport_Viewier = 'No' and prof.Number_Of_Sky_Products_No_BB >= 5                                                  then 361
                when prof.BT_Sport_Viewier = 'No' and cl_eng.CL_SOC in ('High')                                                               then 363
                when prof.BT_Sport_Viewier = 'No' and prof.Value_Segment = 'F) Unstable'                                                      then 365
                when prof.BT_Sport_Viewier = 'No' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 367
                when prof.BT_Sport_Viewier = 'No'                                                                                             then 366
            end

          -- #######################################################################################################
          -- ##### Risk group 4 #####
        when base.xRisk_Segment_1 in ('Risk group 4') then
            case
            -- ===== BT Sport =====
                when prof.BT_Sport_Viewier = 'Yes' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 403
                when prof.BT_Sport_Viewier = 'Yes' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                     then 405
                when prof.BT_Sport_Viewier = 'Yes' and eng.Movies_SOV in ('High')                                                             then 407
                when prof.BT_Sport_Viewier = 'Yes' and prof.Number_Of_Sky_Products_No_BB >= 5                                                 then 409
                when prof.BT_Sport_Viewier = 'Yes' and cl_eng.CL_SOC in ('High')                                                              then 411
                when prof.BT_Sport_Viewier = 'Yes' and prof.Value_Segment = 'F) Unstable'                                                     then 413
                when prof.BT_Sport_Viewier = 'Yes' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 415
                when prof.BT_Sport_Viewier = 'Yes'                                                                                            then 414

            -- ===== Non-BT Sport =====
                when prof.BT_Sport_Viewier = 'No' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 453
                when prof.BT_Sport_Viewier = 'No' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                      then 455
                when prof.BT_Sport_Viewier = 'No' and eng.Movies_SOV in ('High')                                                              then 457
                when prof.BT_Sport_Viewier = 'No' and prof.Number_Of_Sky_Products_No_BB >= 5                                                  then 459
                when prof.BT_Sport_Viewier = 'No' and cl_eng.CL_SOC in ('High')                                                               then 461
                when prof.BT_Sport_Viewier = 'No' and prof.Value_Segment = 'F) Unstable'                                                      then 463
                when prof.BT_Sport_Viewier = 'No' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 465
                when prof.BT_Sport_Viewier = 'No'                                                                                             then 464
            end

          -- #######################################################################################################
          -- ##### Risk group 5 #####
        when base.xRisk_Segment_1 in ('Risk group 5') then
            case

                when                                  eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                      then 502
                when                                  eng.Movies_SOV in ('High')                                                              then 504
                when                                  prof.Number_Of_Sky_Products_No_BB >= 5                                                  then 506
                when                                  cl_eng.CL_SOC in ('High')                                                               then 508
                when                                  prof.Value_Segment = 'F) Unstable'                                                      then 510
                when                                  prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 512
                  else                                                                                                                             511
            end

          -- #######################################################################################################
          -- ##### Excluded #####
          else 0

      end,


       a.Risk_Segment_4  =                                                  -- EPL risk group - Sky loses majority of EPL
      case

          -- #######################################################################################################
          -- ##### Risk group 1 #####
        when base.xRisk_Segment_2 in ('Risk group 1') then
            case
            -- ===== BT Sport =====
                when prof.BT_Sport_Viewier = 'Yes' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 103
                when prof.BT_Sport_Viewier = 'Yes' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                     then 105
                when prof.BT_Sport_Viewier = 'Yes' and eng.Movies_SOV in ('High')                                                             then 107
                when prof.BT_Sport_Viewier = 'Yes' and prof.Number_Of_Sky_Products_No_BB >= 5                                                 then 109
                when prof.BT_Sport_Viewier = 'Yes' and cl_eng.CL_SOC in ('High')                                                              then 111
                when prof.BT_Sport_Viewier = 'Yes' and prof.Value_Segment = 'F) Unstable'                                                     then 113
                when prof.BT_Sport_Viewier = 'Yes' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 115
                when prof.BT_Sport_Viewier = 'Yes'                                                                                            then 114

            -- ===== Non-BT Sport =====
                when prof.BT_Sport_Viewier = 'No' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 153
                when prof.BT_Sport_Viewier = 'No' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                      then 155
                when prof.BT_Sport_Viewier = 'No' and eng.Movies_SOV in ('High')                                                              then 157
                when prof.BT_Sport_Viewier = 'No' and prof.Number_Of_Sky_Products_No_BB >= 5                                                  then 159
                when prof.BT_Sport_Viewier = 'No' and cl_eng.CL_SOC in ('High')                                                               then 161
                when prof.BT_Sport_Viewier = 'No' and prof.Value_Segment = 'F) Unstable'                                                      then 163
                when prof.BT_Sport_Viewier = 'No' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 165
                when prof.BT_Sport_Viewier = 'No'                                                                                             then 164
            end

          -- #######################################################################################################
          -- ##### Risk group 2 #####
        when base.xRisk_Segment_2 in ('Risk group 2') then
            case
            -- ===== BT Sport =====
                when prof.BT_Sport_Viewier = 'Yes' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 203
                when prof.BT_Sport_Viewier = 'Yes' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                     then 205
                when prof.BT_Sport_Viewier = 'Yes' and eng.Movies_SOV in ('High')                                                             then 207
                when prof.BT_Sport_Viewier = 'Yes' and prof.Number_Of_Sky_Products_No_BB >= 5                                                 then 209
                when prof.BT_Sport_Viewier = 'Yes' and cl_eng.CL_SOC in ('High')                                                              then 211
                when prof.BT_Sport_Viewier = 'Yes' and prof.Value_Segment = 'F) Unstable'                                                     then 213
                when prof.BT_Sport_Viewier = 'Yes' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 215
                when prof.BT_Sport_Viewier = 'Yes'                                                                                            then 214

            -- ===== Non-BT Sport =====
                when prof.BT_Sport_Viewier = 'No' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 253
                when prof.BT_Sport_Viewier = 'No' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                      then 255
                when prof.BT_Sport_Viewier = 'No' and eng.Movies_SOV in ('High')                                                              then 257
                when prof.BT_Sport_Viewier = 'No' and prof.Number_Of_Sky_Products_No_BB >= 5                                                  then 259
                when prof.BT_Sport_Viewier = 'No' and cl_eng.CL_SOC in ('High')                                                               then 261
                when prof.BT_Sport_Viewier = 'No' and prof.Value_Segment = 'F) Unstable'                                                      then 263
                when prof.BT_Sport_Viewier = 'No' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 265
                when prof.BT_Sport_Viewier = 'No'                                                                                             then 264
            end

          -- #######################################################################################################
          -- ##### Risk group 3 #####
        when base.xRisk_Segment_2 in ('Risk group 3') then
            case
            -- ===== BT Sport =====
                when prof.BT_Sport_Viewier = 'Yes' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 303
                when prof.BT_Sport_Viewier = 'Yes' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                     then 305
                when prof.BT_Sport_Viewier = 'Yes' and eng.Movies_SOV in ('High')                                                             then 307
                when prof.BT_Sport_Viewier = 'Yes' and prof.Number_Of_Sky_Products_No_BB >= 5                                                 then 309
                when prof.BT_Sport_Viewier = 'Yes' and cl_eng.CL_SOC in ('High')                                                              then 311
                when prof.BT_Sport_Viewier = 'Yes' and prof.Value_Segment = 'F) Unstable'                                                     then 313
                when prof.BT_Sport_Viewier = 'Yes' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 315
                when prof.BT_Sport_Viewier = 'Yes'                                                                                            then 314

            -- ===== Non-BT Sport =====
                when prof.BT_Sport_Viewier = 'No' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 353
                when prof.BT_Sport_Viewier = 'No' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                      then 355
                when prof.BT_Sport_Viewier = 'No' and eng.Movies_SOV in ('High')                                                              then 357
                when prof.BT_Sport_Viewier = 'No' and prof.Number_Of_Sky_Products_No_BB >= 5                                                  then 359
                when prof.BT_Sport_Viewier = 'No' and cl_eng.CL_SOC in ('High')                                                               then 361
                when prof.BT_Sport_Viewier = 'No' and prof.Value_Segment = 'F) Unstable'                                                      then 363
                when prof.BT_Sport_Viewier = 'No' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 365
                when prof.BT_Sport_Viewier = 'No'                                                                                             then 364
            end

          -- #######################################################################################################
          -- ##### Risk group 4 #####
        when base.xRisk_Segment_2 in ('Risk group 4') then
            case
            -- ===== BT Sport =====
                when prof.BT_Sport_Viewier = 'Yes' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 403
                when prof.BT_Sport_Viewier = 'Yes' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                     then 405
                when prof.BT_Sport_Viewier = 'Yes' and eng.Movies_SOV in ('High')                                                             then 407
                when prof.BT_Sport_Viewier = 'Yes' and prof.Number_Of_Sky_Products_No_BB >= 5                                                 then 409
                when prof.BT_Sport_Viewier = 'Yes' and cl_eng.CL_SOC in ('High')                                                              then 411
                when prof.BT_Sport_Viewier = 'Yes' and prof.Value_Segment = 'F) Unstable'                                                     then 413
                when prof.BT_Sport_Viewier = 'Yes' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 415
                when prof.BT_Sport_Viewier = 'Yes'                                                                                            then 414

            -- ===== Non-BT Sport =====
                when prof.BT_Sport_Viewier = 'No' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 453
                when prof.BT_Sport_Viewier = 'No' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                      then 455
                when prof.BT_Sport_Viewier = 'No' and eng.Movies_SOV in ('High')                                                              then 457
                when prof.BT_Sport_Viewier = 'No' and prof.Number_Of_Sky_Products_No_BB >= 5                                                  then 459
                when prof.BT_Sport_Viewier = 'No' and cl_eng.CL_SOC in ('High')                                                               then 461
                when prof.BT_Sport_Viewier = 'No' and prof.Value_Segment = 'F) Unstable'                                                      then 463
                when prof.BT_Sport_Viewier = 'No' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 465
                when prof.BT_Sport_Viewier = 'No'                                                                                             then 464
            end

          -- #######################################################################################################
          -- ##### Risk group 5 #####
        when base.xRisk_Segment_2 in ('Risk group 5') then
            case

                when                                  eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                      then 502
                when                                  eng.Movies_SOV in ('High')                                                              then 504
                when                                  prof.Number_Of_Sky_Products_No_BB >= 5                                                  then 506
                when                                  cl_eng.CL_SOC in ('High')                                                               then 508
                when                                  prof.Value_Segment = 'F) Unstable'                                                      then 510
                when                                  prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 512
                  else                                                                                                                             511
            end

          -- #######################################################################################################
          -- ##### Excluded #####
          else 0

      end

  from EPL_07_Risk_Groups_View base,
       EPL_04_Eng_Matrix eng,
       EPL_04_Profiling_Variables prof,
       EPL_54_CL_Eng_Matrix cl_eng

 where a.Account_Number = base.Account_Number
   and a.Period = base.Period
   and a.Sports_Package = 'Sky Sports'

   and a.Account_Number = eng.Account_Number
   and a.Period = eng.Period
   and eng.Metric = 'Overall'

   and a.Account_Number = prof.Account_Number
   and a.Period = prof.Period

   and a.Account_Number = cl_eng.Account_Number
   and a.Period = cl_eng.Period
   and cl_eng.Metric = 'Overall';
commit;



  -- ##############################################################################################################
  -- ##### V8                                                                                                 #####
  -- #####   - Changed outcomes for High CL engagement                                                        #####
  -- ##############################################################################################################
if object_id('EPL_07_Risk_Groups') is not null then drop table EPL_07_Risk_Groups end if;
create table EPL_07_Risk_Groups (
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
);
create        hg   index idx01 on EPL_07_Risk_Groups(Account_Number);
create        lf   index idx02 on EPL_07_Risk_Groups(Period);
create unique hg   index idx03 on EPL_07_Risk_Groups(Account_Number, Period);
create        lf   index idx04 on EPL_07_Risk_Groups(Sports_Package);
grant select on EPL_07_Risk_Groups to vespa_group_low_security;


insert into EPL_07_Risk_Groups
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
update EPL_07_Risk_Groups base
   set base.Risk_Segment_1  =                                               -- Basic risk group - Sky loses EPL In full
      case
        when det.EPL_SoSV in ('High')             and det.Sport_SoV in ('High', 'Very high')                                          then 5
        when det.EPL_SoSV in ('High')             and det.Sport_SoV in ('Low', 'Medium')                                              then 6

        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('Low', 'Medium')                                              then 7
        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 11
        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Medium', 'High') then 12

        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Medium', 'High') then 13
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 14
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('Low', 'Medium')                                              then 10
          else 0
      end,

        base.Risk_Segment_2  =                                               -- Basic risk group - Sky loses majority of EPL
      case
        when det.EPL_SoSV in ('High')             and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 11
        when det.EPL_SoSV in ('High')             and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Medium', 'High') then 12
        when det.EPL_SoSV in ('High')             and det.Sport_SoV in ('Low', 'Medium')        and det.EPL_SOC in ('Medium', 'High') then 13
        when det.EPL_SoSV in ('High')             and det.Sport_SoV in ('Low', 'Medium')        and det.EPL_SOC in ('Low')            then 14

        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('Low', 'Medium')        and det.EPL_SOC in ('Medium', 'High') then 15
        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('Low', 'Medium')        and det.EPL_SOC in ('Low')            then 16
        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Medium', 'High') then 17
        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 18

        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 19
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Medium', 'High') then 20
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('Low', 'Medium')                                              then 10
          else 0
      end
  from EPL_04_Eng_Matrix det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and base.Sports_Package = 'Sky Sports'
   and det.Metric = 'Overall';
commit;


-- This view take information from fields updated above AND below, eeds to be updated only once though.
create or replace view EPL_07_Risk_Groups_View as
  select
        a.Account_Number,
        a.Period,
        a.Sports_Package,

        -- EPL risk group - Sky loses EPL In full
        a.Risk_Segment_1 as Risk_Segment_1_Raw,
        case
          when a.Risk_Segment_1 in (5)          then 'Risk group 1'
          when a.Risk_Segment_1 in (12, 13)     then 'Risk group 2'
          when a.Risk_Segment_1 in (6, 7)       then 'Risk group 3'
          when a.Risk_Segment_1 in (11)         then 'Risk group 4'
          when a.Risk_Segment_1 in (10, 14)     then 'Risk group 5'
          when a.Risk_Segment_1 in (0)          then 'Excluded'
            else 'No Sky Sports'
        end as xRisk_Segment_1,

        case
          when xRisk_Segment_1 = 'Excluded'                                                                                         then 'Excluded'

          when xRisk_Segment_1 = 'Risk group 1' and a.Risk_Segment_3 in (105, 107, 109)                                             then 'No change'
          when xRisk_Segment_1 = 'Risk group 1' and a.Risk_Segment_3 in (111, 113, 114, 115, 161, 163, 164, 165)                    then 'Churn risk'
          when xRisk_Segment_1 = 'Risk group 1' and a.Risk_Segment_3 in (155, 157, 159)                                             then 'Downgrade risk'
          when xRisk_Segment_1 = 'Risk group 1'                                                                                     then 'Excluded'

          when xRisk_Segment_1 = 'Risk group 2' and a.Risk_Segment_3 in (203, 205, 207, 209, 211, 253)                              then 'No change'
          when xRisk_Segment_1 = 'Risk group 2' and a.Risk_Segment_3 in (213, 215, 216, 217, 263, 265, 266, 267)                    then 'Churn risk'
          when xRisk_Segment_1 = 'Risk group 2' and a.Risk_Segment_3 in (255, 257, 259, 261)                                        then 'Downgrade risk'
          when xRisk_Segment_1 = 'Risk group 2'                                                                                     then 'Excluded'

          when xRisk_Segment_1 = 'Risk group 3' and a.Risk_Segment_3 in (303, 305, 307, 309, 311, 353)                              then 'No change'
          when xRisk_Segment_1 = 'Risk group 3' and a.Risk_Segment_3 in (313, 315, 316, 317, 365, 366, 367)                         then 'Churn risk'
          when xRisk_Segment_1 = 'Risk group 3' and a.Risk_Segment_3 in (355, 357, 359, 361, 363)                                   then 'Downgrade risk'
          when xRisk_Segment_1 = 'Risk group 3'                                                                                     then 'Excluded'

          when xRisk_Segment_1 = 'Risk group 4' and a.Risk_Segment_3 in (403, 405, 407, 409, 414, 453, 455, 457, 459, 464)          then 'No change'
          when xRisk_Segment_1 = 'Risk group 4' and a.Risk_Segment_3 in (/*411,* / 413, 415, 461, 463, 465)                          then 'Downgrade risk'
          when xRisk_Segment_1 = 'Risk group 4'                                                                                     then 'Excluded'

          when xRisk_Segment_1 = 'Risk group 5' and a.Risk_Segment_3 in (502, 504, 506, 508, 510, 511, 512)                         then 'No change'
          when xRisk_Segment_1 = 'Risk group 5'                                                                                     then 'Excluded'

            else 'No Sky Sports'
        end as xRisk_Segment_3,


        -- EPL risk group - Sky loses majority of EPL
        a.Risk_Segment_2 as Risk_Segment_2_Raw,
        case
          when a.Risk_Segment_2 in (11)             then 'Risk group 1'
          when a.Risk_Segment_2 in (14, 18)         then 'Risk group 2'
          when a.Risk_Segment_2 in (12, 13, 15, 17) then 'Risk group 3'
          when a.Risk_Segment_2 in (16, 19)         then 'Risk group 4'
          when a.Risk_Segment_2 in (10, 20)         then 'Risk group 5'
          when a.Risk_Segment_2 in (0)              then 'Excluded'
            else 'No Sky Sports'
        end as xRisk_Segment_2,

        case
          when xRisk_Segment_2 = 'Excluded'                                                                                         then 'Excluded'

          when xRisk_Segment_2 = 'Risk group 1' and a.Risk_Segment_4 in (103, 105, 107, 109)                                        then 'No change'
          when xRisk_Segment_2 = 'Risk group 1' and a.Risk_Segment_4 in (161, 163, 165)                                             then 'Churn risk'
          when xRisk_Segment_2 = 'Risk group 1' and a.Risk_Segment_4 in (111, 113, 114, 115, 153, 155, 157, 159, 164)               then 'Downgrade risk'
          when xRisk_Segment_2 = 'Risk group 1'                                                                                     then 'Excluded'

          when xRisk_Segment_2 = 'Risk group 2' and a.Risk_Segment_4 in (203, 205, 207, 209, 214, 264)                              then 'No change'
          when xRisk_Segment_2 = 'Risk group 2' and a.Risk_Segment_4 in (/*211,* / 213, 215, 261, 263, 265)                          then 'Churn risk'
          when xRisk_Segment_2 = 'Risk group 2' and a.Risk_Segment_4 in (253, 255, 257, 259)                                        then 'Downgrade risk'
          when xRisk_Segment_2 = 'Risk group 2'                                                                                     then 'Excluded'

          when xRisk_Segment_2 = 'Risk group 3' and a.Risk_Segment_4 in (303, 305, 307, 309, 353, 355)                              then 'No change'
          when xRisk_Segment_2 = 'Risk group 3' and a.Risk_Segment_4 in (/*311,* / 313, 314, 364)                                    then 'Churn risk'
          when xRisk_Segment_2 = 'Risk group 3' and a.Risk_Segment_4 in (315, 357, 359, 361, 363, 365)                              then 'Downgrade risk'
          when xRisk_Segment_2 = 'Risk group 3'                                                                                     then 'Excluded'

          when xRisk_Segment_2 = 'Risk group 4' and a.Risk_Segment_4 in (403, 405, 407, 409, 414, 453, 455, 457, 459, 464)          then 'No change'
          when xRisk_Segment_2 = 'Risk group 4' and a.Risk_Segment_4 in (/*411,* / 413, 415, 461, 463, 465)                          then 'Downgrade risk'
          when xRisk_Segment_2 = 'Risk group 4'                                                                                     then 'Excluded'

          when xRisk_Segment_2 = 'Risk group 5' and a.Risk_Segment_4 in (502, 504, 506, 508, 510, 511, 512)                         then 'No change'
          when xRisk_Segment_2 = 'Risk group 5'                                                                                     then 'Excluded'

            else 'No Sky Sports'
        end as xRisk_Segment_4

    from EPL_07_Risk_Groups a;
commit;


  -- EPL risk groups
update EPL_07_Risk_Groups a
   set a.Risk_Segment_3  =                                                  -- EPL risk group - Sky loses EPL In full
      case

          -- #######################################################################################################
          -- ##### Risk group 1 #####
        when base.xRisk_Segment_1 in ('Risk group 1') then
            case
            -- ===== BT Sport =====
                when prof.BT_Sport_Viewier = 'Yes' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                     then 105
                when prof.BT_Sport_Viewier = 'Yes' and eng.Movies_SOV in ('High')                                                             then 107
                when prof.BT_Sport_Viewier = 'Yes' and prof.Number_Of_Sky_Products_No_BB >= 5                                                 then 109
                when prof.BT_Sport_Viewier = 'Yes' and cl_eng.CL_SOC in ('High')                                                              then 111
                when prof.BT_Sport_Viewier = 'Yes' and prof.Value_Segment = 'F) Unstable'                                                     then 113
                when prof.BT_Sport_Viewier = 'Yes' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 115
                when prof.BT_Sport_Viewier = 'Yes'                                                                                            then 114

            -- ===== Non-BT Sport =====
                when prof.BT_Sport_Viewier = 'No' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                      then 155
                when prof.BT_Sport_Viewier = 'No' and eng.Movies_SOV in ('High')                                                              then 157
                when prof.BT_Sport_Viewier = 'No' and prof.Number_Of_Sky_Products_No_BB >= 5                                                  then 159
                when prof.BT_Sport_Viewier = 'No' and cl_eng.CL_SOC in ('High')                                                               then 161
                when prof.BT_Sport_Viewier = 'No' and prof.Value_Segment = 'F) Unstable'                                                      then 163
                when prof.BT_Sport_Viewier = 'No' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 165
                when prof.BT_Sport_Viewier = 'No'                                                                                             then 164
            end

          -- #######################################################################################################
          -- ##### Risk group 2 #####
        when base.xRisk_Segment_1 in ('Risk group 2') then
            case
            -- ===== BT Sport =====
                when prof.BT_Sport_Viewier = 'Yes' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 203
                when prof.BT_Sport_Viewier = 'Yes' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                     then 205
                when prof.BT_Sport_Viewier = 'Yes' and eng.Sky_Branded_Channels in ('High')                                                   then 207
                when prof.BT_Sport_Viewier = 'Yes' and eng.Movies_SOV in ('High')                                                             then 209
                when prof.BT_Sport_Viewier = 'Yes' and prof.Number_Of_Sky_Products_No_BB >= 5                                                 then 211
                when prof.BT_Sport_Viewier = 'Yes' and cl_eng.CL_SOC in ('High')                                                              then 213
                when prof.BT_Sport_Viewier = 'Yes' and prof.Value_Segment = 'F) Unstable'                                                     then 215
                when prof.BT_Sport_Viewier = 'Yes' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 217
                when prof.BT_Sport_Viewier = 'Yes'                                                                                            then 216

            -- ===== Non-BT Sport =====
                when prof.BT_Sport_Viewier = 'No' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 253
                when prof.BT_Sport_Viewier = 'No' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                      then 255
                when prof.BT_Sport_Viewier = 'No' and eng.Sky_Branded_Channels in ('High')                                                    then 257
                when prof.BT_Sport_Viewier = 'No' and eng.Movies_SOV in ('High')                                                              then 259
                when prof.BT_Sport_Viewier = 'No' and prof.Number_Of_Sky_Products_No_BB >= 5                                                  then 261
                when prof.BT_Sport_Viewier = 'No' and cl_eng.CL_SOC in ('High')                                                               then 263
                when prof.BT_Sport_Viewier = 'No' and prof.Value_Segment = 'F) Unstable'                                                      then 265
                when prof.BT_Sport_Viewier = 'No' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 267
                when prof.BT_Sport_Viewier = 'No'                                                                                             then 266
            end

          -- #######################################################################################################
          -- ##### Risk group 3 #####
        when base.xRisk_Segment_1 in ('Risk group 3') then
            case
            -- ===== BT Sport =====
                when prof.BT_Sport_Viewier = 'Yes' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 303
                when prof.BT_Sport_Viewier = 'Yes' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                     then 305
                when prof.BT_Sport_Viewier = 'Yes' and eng.Sky_Branded_Channels in ('High')                                                   then 307
                when prof.BT_Sport_Viewier = 'Yes' and eng.Movies_SOV in ('High')                                                             then 309
                when prof.BT_Sport_Viewier = 'Yes' and prof.Number_Of_Sky_Products_No_BB >= 5                                                 then 311
                when prof.BT_Sport_Viewier = 'Yes' and cl_eng.CL_SOC in ('High')                                                              then 313
                when prof.BT_Sport_Viewier = 'Yes' and prof.Value_Segment = 'F) Unstable'                                                     then 315
                when prof.BT_Sport_Viewier = 'Yes' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 317
                when prof.BT_Sport_Viewier = 'Yes'                                                                                            then 316

            -- ===== Non-BT Sport =====
                when prof.BT_Sport_Viewier = 'No' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 353
                when prof.BT_Sport_Viewier = 'No' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                      then 355
                when prof.BT_Sport_Viewier = 'No' and eng.Sky_Branded_Channels in ('High')                                                    then 357
                when prof.BT_Sport_Viewier = 'No' and eng.Movies_SOV in ('High')                                                              then 359
                when prof.BT_Sport_Viewier = 'No' and prof.Number_Of_Sky_Products_No_BB >= 5                                                  then 361
                when prof.BT_Sport_Viewier = 'No' and cl_eng.CL_SOC in ('High')                                                               then 363
                when prof.BT_Sport_Viewier = 'No' and prof.Value_Segment = 'F) Unstable'                                                      then 365
                when prof.BT_Sport_Viewier = 'No' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 367
                when prof.BT_Sport_Viewier = 'No'                                                                                             then 366
            end

          -- #######################################################################################################
          -- ##### Risk group 4 #####
        when base.xRisk_Segment_1 in ('Risk group 4') then
            case
            -- ===== BT Sport =====
                when prof.BT_Sport_Viewier = 'Yes' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 403
                when prof.BT_Sport_Viewier = 'Yes' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                     then 405
                when prof.BT_Sport_Viewier = 'Yes' and eng.Movies_SOV in ('High')                                                             then 407
                when prof.BT_Sport_Viewier = 'Yes' and prof.Number_Of_Sky_Products_No_BB >= 5                                                 then 409
                -- when prof.BT_Sport_Viewier = 'Yes' and cl_eng.CL_SOC in ('High')                                                              then 411
                when prof.BT_Sport_Viewier = 'Yes' and prof.Value_Segment = 'F) Unstable'                                                     then 413
                when prof.BT_Sport_Viewier = 'Yes' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 415
                when prof.BT_Sport_Viewier = 'Yes'                                                                                            then 414

            -- ===== Non-BT Sport =====
                when prof.BT_Sport_Viewier = 'No' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 453
                when prof.BT_Sport_Viewier = 'No' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                      then 455
                when prof.BT_Sport_Viewier = 'No' and eng.Movies_SOV in ('High')                                                              then 457
                when prof.BT_Sport_Viewier = 'No' and prof.Number_Of_Sky_Products_No_BB >= 5                                                  then 459
                when prof.BT_Sport_Viewier = 'No' and cl_eng.CL_SOC in ('High')                                                               then 461
                when prof.BT_Sport_Viewier = 'No' and prof.Value_Segment = 'F) Unstable'                                                      then 463
                when prof.BT_Sport_Viewier = 'No' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 465
                when prof.BT_Sport_Viewier = 'No'                                                                                             then 464
            end

          -- #######################################################################################################
          -- ##### Risk group 5 #####
        when base.xRisk_Segment_1 in ('Risk group 5') then
            case

                when                                  eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                      then 502
                when                                  eng.Movies_SOV in ('High')                                                              then 504
                when                                  prof.Number_Of_Sky_Products_No_BB >= 5                                                  then 506
                when                                  cl_eng.CL_SOC in ('High')                                                               then 508
                when                                  prof.Value_Segment = 'F) Unstable'                                                      then 510
                when                                  prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 512
                  else                                                                                                                             511
            end

          -- #######################################################################################################
          -- ##### Excluded #####
          else 0

      end,


       a.Risk_Segment_4  =                                                  -- EPL risk group - Sky loses majority of EPL
      case

          -- #######################################################################################################
          -- ##### Risk group 1 #####
        when base.xRisk_Segment_2 in ('Risk group 1') then
            case
            -- ===== BT Sport =====
                when prof.BT_Sport_Viewier = 'Yes' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 103
                when prof.BT_Sport_Viewier = 'Yes' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                     then 105
                when prof.BT_Sport_Viewier = 'Yes' and eng.Movies_SOV in ('High')                                                             then 107
                when prof.BT_Sport_Viewier = 'Yes' and prof.Number_Of_Sky_Products_No_BB >= 5                                                 then 109
                when prof.BT_Sport_Viewier = 'Yes' and cl_eng.CL_SOC in ('High')                                                              then 111
                when prof.BT_Sport_Viewier = 'Yes' and prof.Value_Segment = 'F) Unstable'                                                     then 113
                when prof.BT_Sport_Viewier = 'Yes' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 115
                when prof.BT_Sport_Viewier = 'Yes'                                                                                            then 114

            -- ===== Non-BT Sport =====
                when prof.BT_Sport_Viewier = 'No' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 153
                when prof.BT_Sport_Viewier = 'No' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                      then 155
                when prof.BT_Sport_Viewier = 'No' and eng.Movies_SOV in ('High')                                                              then 157
                when prof.BT_Sport_Viewier = 'No' and prof.Number_Of_Sky_Products_No_BB >= 5                                                  then 159
                when prof.BT_Sport_Viewier = 'No' and cl_eng.CL_SOC in ('High')                                                               then 161
                when prof.BT_Sport_Viewier = 'No' and prof.Value_Segment = 'F) Unstable'                                                      then 163
                when prof.BT_Sport_Viewier = 'No' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 165
                when prof.BT_Sport_Viewier = 'No'                                                                                             then 164
            end

          -- #######################################################################################################
          -- ##### Risk group 2 #####
        when base.xRisk_Segment_2 in ('Risk group 2') then
            case
            -- ===== BT Sport =====
                when prof.BT_Sport_Viewier = 'Yes' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 203
                when prof.BT_Sport_Viewier = 'Yes' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                     then 205
                when prof.BT_Sport_Viewier = 'Yes' and eng.Movies_SOV in ('High')                                                             then 207
                when prof.BT_Sport_Viewier = 'Yes' and prof.Number_Of_Sky_Products_No_BB >= 5                                                 then 209
                -- when prof.BT_Sport_Viewier = 'Yes' and cl_eng.CL_SOC in ('High')                                                              then 211
                when prof.BT_Sport_Viewier = 'Yes' and prof.Value_Segment = 'F) Unstable'                                                     then 213
                when prof.BT_Sport_Viewier = 'Yes' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 215
                when prof.BT_Sport_Viewier = 'Yes'                                                                                            then 214

            -- ===== Non-BT Sport =====
                when prof.BT_Sport_Viewier = 'No' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 253
                when prof.BT_Sport_Viewier = 'No' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                      then 255
                when prof.BT_Sport_Viewier = 'No' and eng.Movies_SOV in ('High')                                                              then 257
                when prof.BT_Sport_Viewier = 'No' and prof.Number_Of_Sky_Products_No_BB >= 5                                                  then 259
                when prof.BT_Sport_Viewier = 'No' and cl_eng.CL_SOC in ('High')                                                               then 261
                when prof.BT_Sport_Viewier = 'No' and prof.Value_Segment = 'F) Unstable'                                                      then 263
                when prof.BT_Sport_Viewier = 'No' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 265
                when prof.BT_Sport_Viewier = 'No'                                                                                             then 264
            end

          -- #######################################################################################################
          -- ##### Risk group 3 #####
        when base.xRisk_Segment_2 in ('Risk group 3') then
            case
            -- ===== BT Sport =====
                when prof.BT_Sport_Viewier = 'Yes' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 303
                when prof.BT_Sport_Viewier = 'Yes' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                     then 305
                when prof.BT_Sport_Viewier = 'Yes' and eng.Movies_SOV in ('High')                                                             then 307
                when prof.BT_Sport_Viewier = 'Yes' and prof.Number_Of_Sky_Products_No_BB >= 5                                                 then 309
                -- when prof.BT_Sport_Viewier = 'Yes' and cl_eng.CL_SOC in ('High')                                                              then 311
                when prof.BT_Sport_Viewier = 'Yes' and prof.Value_Segment = 'F) Unstable'                                                     then 313
                when prof.BT_Sport_Viewier = 'Yes' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 315
                when prof.BT_Sport_Viewier = 'Yes'                                                                                            then 314

            -- ===== Non-BT Sport =====
                when prof.BT_Sport_Viewier = 'No' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 353
                when prof.BT_Sport_Viewier = 'No' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                      then 355
                when prof.BT_Sport_Viewier = 'No' and eng.Movies_SOV in ('High')                                                              then 357
                when prof.BT_Sport_Viewier = 'No' and prof.Number_Of_Sky_Products_No_BB >= 5                                                  then 359
                when prof.BT_Sport_Viewier = 'No' and cl_eng.CL_SOC in ('High')                                                               then 361
                when prof.BT_Sport_Viewier = 'No' and prof.Value_Segment = 'F) Unstable'                                                      then 363
                when prof.BT_Sport_Viewier = 'No' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 365
                when prof.BT_Sport_Viewier = 'No'                                                                                             then 364
            end

          -- #######################################################################################################
          -- ##### Risk group 4 #####
        when base.xRisk_Segment_2 in ('Risk group 4') then
            case
            -- ===== BT Sport =====
                when prof.BT_Sport_Viewier = 'Yes' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 403
                when prof.BT_Sport_Viewier = 'Yes' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                     then 405
                when prof.BT_Sport_Viewier = 'Yes' and eng.Movies_SOV in ('High')                                                             then 407
                when prof.BT_Sport_Viewier = 'Yes' and prof.Number_Of_Sky_Products_No_BB >= 5                                                 then 409
                -- when prof.BT_Sport_Viewier = 'Yes' and cl_eng.CL_SOC in ('High')                                                              then 411
                when prof.BT_Sport_Viewier = 'Yes' and prof.Value_Segment = 'F) Unstable'                                                     then 413
                when prof.BT_Sport_Viewier = 'Yes' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 415
                when prof.BT_Sport_Viewier = 'Yes'                                                                                            then 414

            -- ===== Non-BT Sport =====
                when prof.BT_Sport_Viewier = 'No' and prof.Sports_Segment_SIG in ('SIG 02 - Flower of Scotland', 'SIG 05 - Cricket Enthusiasts', 'SIG 11 - Cricket Fanatics', 'SIG 20 - Cricket Fans')
                                                                                                                                              then 453
                when prof.BT_Sport_Viewier = 'No' and eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                      then 455
                when prof.BT_Sport_Viewier = 'No' and eng.Movies_SOV in ('High')                                                              then 457
                when prof.BT_Sport_Viewier = 'No' and prof.Number_Of_Sky_Products_No_BB >= 5                                                  then 459
                when prof.BT_Sport_Viewier = 'No' and cl_eng.CL_SOC in ('High')                                                               then 461
                when prof.BT_Sport_Viewier = 'No' and prof.Value_Segment = 'F) Unstable'                                                      then 463
                when prof.BT_Sport_Viewier = 'No' and prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 465
                when prof.BT_Sport_Viewier = 'No'                                                                                             then 464
            end

          -- #######################################################################################################
          -- ##### Risk group 5 #####
        when base.xRisk_Segment_2 in ('Risk group 5') then
            case

                when                                  eng.Sky_Atlantic_Complete_Progs_Viewed in ('High')                                      then 502
                when                                  eng.Movies_SOV in ('High')                                                              then 504
                when                                  prof.Number_Of_Sky_Products_No_BB >= 5                                                  then 506
                when                                  cl_eng.CL_SOC in ('High')                                                               then 508
                when                                  prof.Value_Segment = 'F) Unstable'                                                      then 510
                when                                  prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                                                                              then 512
                  else                                                                                                                             511
            end

          -- #######################################################################################################
          -- ##### Excluded #####
          else 0

      end

  from EPL_07_Risk_Groups_View base,
       EPL_04_Eng_Matrix eng,
       EPL_04_Profiling_Variables prof,
       EPL_54_CL_Eng_Matrix cl_eng

 where a.Account_Number = base.Account_Number
   and a.Period = base.Period
   and a.Sports_Package = 'Sky Sports'

   and a.Account_Number = eng.Account_Number
   and a.Period = eng.Period
   and eng.Metric = 'Overall'

   and a.Account_Number = prof.Account_Number
   and a.Period = prof.Period

   and a.Account_Number = cl_eng.Account_Number
   and a.Period = cl_eng.Period
   and cl_eng.Metric = 'Overall';
commit;



  -- ##############################################################################################################
  -- ##### V9                                                                                                 #####
  -- #####   - Tree simplified, SIG layer removed, BT viewer/non-viewer removed                               #####
  -- ##############################################################################################################
  -- Basic risk groups
update EPL_07_Risk_Groups base
   set base.Risk_Segment_1  =                                               -- Basic risk group - Sky loses EPL In full
      case
        when det.EPL_SoSV in ('High')             and det.Sport_SoV in ('High', 'Very high')                                          then 5
        when det.EPL_SoSV in ('High')             and det.Sport_SoV in ('Low', 'Medium')                                              then 6

        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('Low', 'Medium')                                              then 7
        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 11
        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Medium', 'High') then 12

        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Medium', 'High') then 13
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 14
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('Low', 'Medium')                                              then 10
          else 0
      end,

        base.Risk_Segment_2  =                                               -- Basic risk group - Sky loses majority of EPL
      case
        when det.EPL_SoSV in ('High')             and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 11
        when det.EPL_SoSV in ('High')             and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Medium', 'High') then 12
        when det.EPL_SoSV in ('High')             and det.Sport_SoV in ('Low', 'Medium')        and det.EPL_SOC in ('Medium', 'High') then 13
        when det.EPL_SoSV in ('High')             and det.Sport_SoV in ('Low', 'Medium')        and det.EPL_SOC in ('Low')            then 14

        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('Low', 'Medium')        and det.EPL_SOC in ('Medium', 'High') then 15
        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('Low', 'Medium')        and det.EPL_SOC in ('Low')            then 16
        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Medium', 'High') then 17
        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 18

        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 19
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Medium', 'High') then 20
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('Low', 'Medium')                                              then 10
          else 0
      end
  from EPL_04_Eng_Matrix det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and base.Sports_Package = 'Sky Sports'
   and det.Metric = 'Overall';
commit;


-- This view take information from fields updated above AND below, needs to be updated only once though.
create or replace view EPL_07_Risk_Groups_View as
  select
        a.Account_Number,
        a.Period,
        a.Sports_Package,

        -- EPL risk group - Sky loses EPL In full
        a.Risk_Segment_1 as Risk_Segment_1_Raw,
        case
          when a.Risk_Segment_1 in (5)          then 'Risk group 1'
          when a.Risk_Segment_1 in (12, 13)     then 'Risk group 2'
          when a.Risk_Segment_1 in (6, 7)       then 'Risk group 3'
          when a.Risk_Segment_1 in (11)         then 'Risk group 4'
          when a.Risk_Segment_1 in (10, 14)     then 'Risk group 5'
          when a.Risk_Segment_1 in (0)          then 'Excluded'
            else 'No Sky Sports'
        end as xRisk_Segment_1,

        case
          when xRisk_Segment_1 = 'Excluded'                                                                                         then 'Excluded'

          when xRisk_Segment_1 = 'Risk group 1' and a.Risk_Segment_3 in (152, 154, 156, 158)                                        then 'Downgrade risk'
          when xRisk_Segment_1 = 'Risk group 1' and a.Risk_Segment_3 in (157)                                                       then 'Churn risk'
          when xRisk_Segment_1 = 'Risk group 1'                                                                                     then 'Excluded'

          when xRisk_Segment_1 = 'Risk group 2' and a.Risk_Segment_3 in (254, 256, 258, 260)                                        then 'Downgrade risk'
          when xRisk_Segment_1 = 'Risk group 2' and a.Risk_Segment_3 in (259)                                                       then 'Churn risk'
          when xRisk_Segment_1 = 'Risk group 2'                                                                                     then 'Excluded'

          when xRisk_Segment_1 = 'Risk group 3' and a.Risk_Segment_3 in (354, 356, 358, 360)                                        then 'Downgrade risk'
          when xRisk_Segment_1 = 'Risk group 3' and a.Risk_Segment_3 in (359)                                                       then 'Churn risk'
          when xRisk_Segment_1 = 'Risk group 3'                                                                                     then 'Excluded'

          when xRisk_Segment_1 = 'Risk group 4' and a.Risk_Segment_3 in (454, 456, 458, 460)                                        then 'No change'
          when xRisk_Segment_1 = 'Risk group 4' and a.Risk_Segment_3 in (462, 464, 466)                                             then 'Downgrade risk'
          when xRisk_Segment_1 = 'Risk group 4' and a.Risk_Segment_3 in (465)                                                       then 'No change'
          when xRisk_Segment_1 = 'Risk group 4'                                                                                     then 'Excluded'

          when xRisk_Segment_1 = 'Risk group 5'                                                                                     then 'No change'

            else 'No Sky Sports'
        end as xRisk_Segment_3,


        -- EPL risk group - Sky loses majority of EPL
        a.Risk_Segment_2 as Risk_Segment_2_Raw,
        case
          when a.Risk_Segment_2 in (11)             then 'Risk group 1'
          when a.Risk_Segment_2 in (14, 18)         then 'Risk group 2'
          when a.Risk_Segment_2 in (12, 13, 15, 17) then 'Risk group 3'
          when a.Risk_Segment_2 in (16, 19)         then 'Risk group 4'
          when a.Risk_Segment_2 in (10, 20)         then 'Risk group 5'
          when a.Risk_Segment_2 in (0)              then 'Excluded'
            else 'No Sky Sports'
        end as xRisk_Segment_2,

        case
          when xRisk_Segment_2 = 'Excluded'                                                                                         then 'Excluded'

          when xRisk_Segment_2 = 'Risk group 1' and a.Risk_Segment_4 in (154, 156, 158, 160)                                        then 'Downgrade risk'
          when xRisk_Segment_2 = 'Risk group 1' and a.Risk_Segment_4 in (159)                                                       then 'Churn risk'
          when xRisk_Segment_2 = 'Risk group 1'                                                                                     then 'Excluded'

          when xRisk_Segment_2 = 'Risk group 2' and a.Risk_Segment_4 in (254, 256, 258, 260)                                        then 'Downgrade risk'
          when xRisk_Segment_2 = 'Risk group 2' and a.Risk_Segment_4 in (262, 264, 266)                                             then 'Churn risk'
          when xRisk_Segment_2 = 'Risk group 2' and a.Risk_Segment_4 in (265)                                                       then 'No change'
          when xRisk_Segment_2 = 'Risk group 2'                                                                                     then 'Excluded'

          when xRisk_Segment_2 = 'Risk group 3' and a.Risk_Segment_4 in (354, 356)                                                  then 'No change'
          when xRisk_Segment_2 = 'Risk group 3' and a.Risk_Segment_4 in (358, 360, 362, 364, 366)                                   then 'Downgrade risk'
          when xRisk_Segment_2 = 'Risk group 3' and a.Risk_Segment_4 in (365)                                                       then 'Churn risk'
          when xRisk_Segment_2 = 'Risk group 3'                                                                                     then 'Excluded'

          when xRisk_Segment_2 = 'Risk group 4' and a.Risk_Segment_4 in (454, 456, 458, 460)                                        then 'No change'
          when xRisk_Segment_2 = 'Risk group 4' and a.Risk_Segment_4 in (462, 464, 466)                                             then 'Downgrade risk'
          when xRisk_Segment_2 = 'Risk group 4' and a.Risk_Segment_4 in (465)                                                       then 'No change'
          when xRisk_Segment_2 = 'Risk group 4'                                                                                     then 'Excluded'

          when xRisk_Segment_2 = 'Risk group 5'                                                                                     then 'No change'

            else 'No Sky Sports'
        end as xRisk_Segment_4

    from EPL_07_Risk_Groups a;
commit;


  -- EPL risk groups
update EPL_07_Risk_Groups a
   set a.Risk_Segment_3  =                                                  -- EPL risk group - Sky loses EPL In full
      case

          -- #######################################################################################################
          -- ##### Risk group 1 #####
        when base.xRisk_Segment_1 in ('Risk group 1') then
            case
                when eng.Sky_Branded_Channels in ('High')                               then 152
                when eng.Sky_Sports_News_SoV in ('High')                                then 154
                when eng.Movies_SOV in ('High')                                         then 156
                when prof.Number_Of_Sky_Products_No_BB >= 5                             then 158
                  else                                                                       157
            end

          -- #######################################################################################################
          -- ##### Risk group 2 #####
        when base.xRisk_Segment_1 in ('Risk group 2') then
            case
                when eng.Sky_Branded_Channels in ('High')                               then 254
                when eng.Sky_Sports_News_SoV in ('High')                                then 256
                when eng.Movies_SOV in ('High')                                         then 258
                when prof.Number_Of_Sky_Products_No_BB >= 5                             then 260
                  else                                                                       259
            end

          -- #######################################################################################################
          -- ##### Risk group 3 #####
        when base.xRisk_Segment_1 in ('Risk group 3') then
            case
                when eng.Sky_Branded_Channels in ('High')                               then 354
                when eng.Sky_Sports_News_SoV in ('High')                                then 356
                when eng.Movies_SOV in ('High')                                         then 358
                when prof.Number_Of_Sky_Products_No_BB >= 5                             then 360
                  else                                                                       359
            end

          -- #######################################################################################################
          -- ##### Risk group 4 #####
        when base.xRisk_Segment_1 in ('Risk group 4') then
            case
                when eng.Sky_Branded_Channels in ('High')                               then 454
                when eng.Sky_Sports_News_SoV in ('High')                                then 456
                when eng.Movies_SOV in ('High')                                         then 458
                when prof.Number_Of_Sky_Products_No_BB >= 5                             then 460
                when cl_eng.CL_SOC in ('High')                                          then 462
                when prof.Value_Segment = 'F) Unstable'                                 then 464
                when prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                        then 466
                  else                                                                       465
            end

          -- #######################################################################################################
          -- ##### Risk group 5 #####
        when base.xRisk_Segment_1 in ('Risk group 5') then                                   593

          -- #######################################################################################################
          -- ##### Excluded #####
          else 0

      end,


       a.Risk_Segment_4  =                                                  -- EPL risk group - Sky loses majority of EPL
      case

          -- #######################################################################################################
          -- ##### Risk group 1 #####
        when base.xRisk_Segment_1 in ('Risk group 1') then
            case
                when eng.Sky_Branded_Channels in ('High')                               then 154
                when eng.Sky_Sports_News_SoV in ('High')                                then 156
                when eng.Movies_SOV in ('High')                                         then 158
                when prof.Number_Of_Sky_Products_No_BB >= 5                             then 160
                  else                                                                       159
            end

          -- #######################################################################################################
          -- ##### Risk group 2 #####
        when base.xRisk_Segment_1 in ('Risk group 2') then
            case
                when eng.Sky_Branded_Channels in ('High')                               then 254
                when eng.Sky_Sports_News_SoV in ('High')                                then 256
                when eng.Movies_SOV in ('High')                                         then 258
                when prof.Number_Of_Sky_Products_No_BB >= 5                             then 260
                when cl_eng.CL_SOC in ('High')                                          then 262
                when prof.Value_Segment = 'F) Unstable'                                 then 264
                when prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                        then 266
                  else                                                                       265
            end

          -- #######################################################################################################
          -- ##### Risk group 3 #####
        when base.xRisk_Segment_1 in ('Risk group 3') then
            case
                when eng.Sky_Branded_Channels in ('High')                               then 354
                when eng.Sky_Sports_News_SoV in ('High')                                then 356
                when eng.Movies_SOV in ('High')                                         then 358
                when prof.Number_Of_Sky_Products_No_BB >= 5                             then 360
                when cl_eng.CL_SOC in ('High')                                          then 362
                when prof.Value_Segment = 'F) Unstable'                                 then 364
                when prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                        then 366
                  else                                                                       365
            end

          -- #######################################################################################################
          -- ##### Risk group 4 #####
        when base.xRisk_Segment_1 in ('Risk group 4') then
            case
                when eng.Sky_Branded_Channels in ('High')                               then 454
                when eng.Sky_Sports_News_SoV in ('High')                                then 456
                when eng.Movies_SOV in ('High')                                         then 458
                when prof.Number_Of_Sky_Products_No_BB >= 5                             then 460
                when cl_eng.CL_SOC in ('High')                                          then 462
                when prof.Value_Segment = 'F) Unstable'                                 then 464
                when prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                        then 466
                  else                                                                       465
            end

          -- #######################################################################################################
          -- ##### Risk group 5 #####
        when base.xRisk_Segment_1 in ('Risk group 5') then                                   593

          -- #######################################################################################################
          -- ##### Excluded #####
          else 0

      end

  from EPL_07_Risk_Groups_View base,
       EPL_04_Eng_Matrix eng,
       EPL_04_Profiling_Variables prof,
       EPL_54_CL_Eng_Matrix cl_eng

 where a.Account_Number = base.Account_Number
   and a.Period = base.Period
   and a.Sports_Package = 'Sky Sports'

   and a.Account_Number = eng.Account_Number
   and a.Period = eng.Period
   and eng.Metric = 'Overall'

   and a.Account_Number = prof.Account_Number
   and a.Period = prof.Period

   and a.Account_Number = cl_eng.Account_Number
   and a.Period = cl_eng.Period
   and cl_eng.Metric = 'Overall';
commit;





  -- ##############################################################################################################
  -- ##### V10                                                                                                #####
  -- #####   - SIGs added back in and list revised                                                            #####
  -- ##############################################################################################################
  -- Basic risk groups
update EPL_07_Risk_Groups base
   set base.Risk_Segment_1  =                                               -- Basic risk group - Sky loses EPL In full
      case
        when det.EPL_SoSV in ('High')             and det.Sport_SoV in ('High', 'Very high')                                          then 5
        when det.EPL_SoSV in ('High')             and det.Sport_SoV in ('Low', 'Medium')                                              then 6

        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('Low', 'Medium')                                              then 7
        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 11
        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Medium', 'High') then 12

        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Medium', 'High') then 13
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 14
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('Low', 'Medium')                                              then 10
          else 0
      end,

        base.Risk_Segment_2  =                                               -- Basic risk group - Sky loses majority of EPL
      case
        when det.EPL_SoSV in ('High')             and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 11
        when det.EPL_SoSV in ('High')             and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Medium', 'High') then 12
        when det.EPL_SoSV in ('High')             and det.Sport_SoV in ('Low', 'Medium')        and det.EPL_SOC in ('Medium', 'High') then 13
        when det.EPL_SoSV in ('High')             and det.Sport_SoV in ('Low', 'Medium')        and det.EPL_SOC in ('Low')            then 14

        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('Low', 'Medium')        and det.EPL_SOC in ('Medium', 'High') then 15
        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('Low', 'Medium')        and det.EPL_SOC in ('Low')            then 16
        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Medium', 'High') then 17
        when det.EPL_SoSV in ('Medium')           and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 18

        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Low')            then 19
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('High', 'Very high')    and det.EPL_SOC in ('Medium', 'High') then 20
        when det.EPL_SoSV in ('Low')              and det.Sport_SoV in ('Low', 'Medium')                                              then 10
          else 0
      end
  from EPL_04_Eng_Matrix det
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and base.Sports_Package = 'Sky Sports'
   and det.Metric = 'Overall';
commit;


-- This view take information from fields updated above AND below, needs to be updated only once though.
create or replace view EPL_07_Risk_Groups_View as
  select
        a.Account_Number,
        a.Period,
        a.Sports_Package,

        -- EPL risk group - Sky loses EPL In full
        a.Risk_Segment_1 as Risk_Segment_1_Raw,
        case
          when a.Risk_Segment_1 in (5)          then 'Risk group 1'
          when a.Risk_Segment_1 in (12, 13)     then 'Risk group 2'
          when a.Risk_Segment_1 in (6, 7)       then 'Risk group 3'
          when a.Risk_Segment_1 in (11)         then 'Risk group 4'
          when a.Risk_Segment_1 in (10, 14)     then 'Risk group 5'
          when a.Risk_Segment_1 in (0)          then 'Excluded'
            else 'No Sky Sports'
        end as xRisk_Segment_1,

        case
          when xRisk_Segment_1 = 'Excluded'                                                                                         then 'Excluded'

          when xRisk_Segment_1 = 'Risk group 1' and a.Risk_Segment_3 in (154, 156, 158, 160)                                        then 'Downgrade risk'
          when xRisk_Segment_1 = 'Risk group 1' and a.Risk_Segment_3 in (159)                                                       then 'Churn risk'
          when xRisk_Segment_1 = 'Risk group 1'                                                                                     then 'Excluded'

          when xRisk_Segment_1 = 'Risk group 2' and a.Risk_Segment_3 in (252)                                                       then 'No change'
          when xRisk_Segment_1 = 'Risk group 2' and a.Risk_Segment_3 in (254, 256, 258, 260)                                        then 'Downgrade risk'
          when xRisk_Segment_1 = 'Risk group 2' and a.Risk_Segment_3 in (259)                                                       then 'Churn risk'
          when xRisk_Segment_1 = 'Risk group 2'                                                                                     then 'Excluded'

          when xRisk_Segment_1 = 'Risk group 3' and a.Risk_Segment_3 in (352)                                                       then 'No change'
          when xRisk_Segment_1 = 'Risk group 3' and a.Risk_Segment_3 in (354, 356, 358, 360)                                        then 'Downgrade risk'
          when xRisk_Segment_1 = 'Risk group 3' and a.Risk_Segment_3 in (359)                                                       then 'Churn risk'
          when xRisk_Segment_1 = 'Risk group 3'                                                                                     then 'Excluded'

          when xRisk_Segment_1 = 'Risk group 4' and a.Risk_Segment_3 in (452, 454, 456, 458, 460)                                   then 'No change'
          when xRisk_Segment_1 = 'Risk group 4' and a.Risk_Segment_3 in (462, 464, 466)                                             then 'Downgrade risk'
          when xRisk_Segment_1 = 'Risk group 4' and a.Risk_Segment_3 in (465)                                                       then 'No change'
          when xRisk_Segment_1 = 'Risk group 4'                                                                                     then 'Excluded'

          when xRisk_Segment_1 = 'Risk group 5'                                                                                     then 'No change'

            else 'No Sky Sports'
        end as xRisk_Segment_3,


        -- EPL risk group - Sky loses majority of EPL
        a.Risk_Segment_2 as Risk_Segment_2_Raw,
        case
          when a.Risk_Segment_2 in (11)             then 'Risk group 1'
          when a.Risk_Segment_2 in (14, 18)         then 'Risk group 2'
          when a.Risk_Segment_2 in (12, 13, 15, 17) then 'Risk group 3'
          when a.Risk_Segment_2 in (16, 19)         then 'Risk group 4'
          when a.Risk_Segment_2 in (10, 20)         then 'Risk group 5'
          when a.Risk_Segment_2 in (0)              then 'Excluded'
            else 'No Sky Sports'
        end as xRisk_Segment_2,

        case
          when xRisk_Segment_2 = 'Excluded'                                                                                         then 'Excluded'

          when xRisk_Segment_2 = 'Risk group 1' and a.Risk_Segment_4 in (152)                                                       then 'No change'
          when xRisk_Segment_2 = 'Risk group 1' and a.Risk_Segment_4 in (154, 156, 158, 160)                                        then 'Downgrade risk'
          when xRisk_Segment_2 = 'Risk group 1' and a.Risk_Segment_4 in (159)                                                       then 'Churn risk'
          when xRisk_Segment_2 = 'Risk group 1'                                                                                     then 'Excluded'

          when xRisk_Segment_2 = 'Risk group 2' and a.Risk_Segment_4 in (252)                                                       then 'No change'
          when xRisk_Segment_2 = 'Risk group 2' and a.Risk_Segment_4 in (254, 256, 258, 260)                                        then 'Downgrade risk'
          when xRisk_Segment_2 = 'Risk group 2' and a.Risk_Segment_4 in (262, 264, 266)                                             then 'Churn risk'
          when xRisk_Segment_2 = 'Risk group 2' and a.Risk_Segment_4 in (265)                                                       then 'No change'
          when xRisk_Segment_2 = 'Risk group 2'                                                                                     then 'Excluded'

          when xRisk_Segment_2 = 'Risk group 3' and a.Risk_Segment_4 in (352, 354, 356)                                             then 'No change'
          when xRisk_Segment_2 = 'Risk group 3' and a.Risk_Segment_4 in (358, 360, 362, 364, 366)                                   then 'Downgrade risk'
          when xRisk_Segment_2 = 'Risk group 3' and a.Risk_Segment_4 in (365)                                                       then 'Churn risk'
          when xRisk_Segment_2 = 'Risk group 3'                                                                                     then 'Excluded'

          when xRisk_Segment_2 = 'Risk group 4' and a.Risk_Segment_4 in (452, 454, 456, 458, 460)                                   then 'No change'
          when xRisk_Segment_2 = 'Risk group 4' and a.Risk_Segment_4 in (462, 464, 466)                                             then 'Downgrade risk'
          when xRisk_Segment_2 = 'Risk group 4' and a.Risk_Segment_4 in (465)                                                       then 'No change'
          when xRisk_Segment_2 = 'Risk group 4'                                                                                     then 'Excluded'

          when xRisk_Segment_2 = 'Risk group 5'                                                                                     then 'No change'

            else 'No Sky Sports'
        end as xRisk_Segment_4

    from EPL_07_Risk_Groups a;
commit;


  -- EPL risk groups
update EPL_07_Risk_Groups a
   set a.Risk_Segment_3  =                                                  -- EPL risk group - Sky loses EPL In full
      case

          -- #######################################################################################################
          -- ##### Risk group 1 #####
        when base.xRisk_Segment_1 in ('Risk group 1') then
            case
                when eng.Sky_Branded_Channels in ('High')                               then 154
                when eng.Sky_Sports_News_SoV in ('High')                                then 156
                when eng.Movies_SOV in ('High')                                         then 158
                when prof.Number_Of_Sky_Products_No_BB >= 5                             then 160
                  else                                                                       159
            end

          -- #######################################################################################################
          -- ##### Risk group 2 #####
        when base.xRisk_Segment_1 in ('Risk group 2') then
            case
                when prof.Sports_Segment_SIG in ('SIG 01 - International Rugby Fans', 'SIG 06 - Motor Sport Fans', 'SIG 08 - F1 Super Fans', 'SIG 09 - Super Sports Fans', 'SIG 10 - Sports Disengaged',
                                                 'SIG 11 - Cricket Fanatics', 'SIG 14 - Tennis Fans', 'SIG 15 - Club Rugby Fans', 'SIG 20 - Cricket Fans')
                                                                                        then 252
                when eng.Sky_Branded_Channels in ('High')                               then 254
                when eng.Sky_Sports_News_SoV in ('High')                                then 256
                when eng.Movies_SOV in ('High')                                         then 258
                when prof.Number_Of_Sky_Products_No_BB >= 5                             then 260
                  else                                                                       259
            end

          -- #######################################################################################################
          -- ##### Risk group 3 #####
        when base.xRisk_Segment_1 in ('Risk group 3') then
            case
                when prof.Sports_Segment_SIG in ('SIG 01 - International Rugby Fans', 'SIG 06 - Motor Sport Fans', 'SIG 08 - F1 Super Fans', 'SIG 09 - Super Sports Fans', 'SIG 10 - Sports Disengaged',
                                                 'SIG 11 - Cricket Fanatics', 'SIG 14 - Tennis Fans', 'SIG 15 - Club Rugby Fans', 'SIG 20 - Cricket Fans')
                                                                                        then 352
                when eng.Sky_Branded_Channels in ('High')                               then 354
                when eng.Sky_Sports_News_SoV in ('High')                                then 356
                when eng.Movies_SOV in ('High')                                         then 358
                when prof.Number_Of_Sky_Products_No_BB >= 5                             then 360
                  else                                                                       359
            end

          -- #######################################################################################################
          -- ##### Risk group 4 #####
        when base.xRisk_Segment_1 in ('Risk group 4') then
            case
                when prof.Sports_Segment_SIG in ('SIG 01 - International Rugby Fans', 'SIG 06 - Motor Sport Fans', 'SIG 08 - F1 Super Fans', 'SIG 09 - Super Sports Fans', 'SIG 10 - Sports Disengaged',
                                                 'SIG 11 - Cricket Fanatics', 'SIG 14 - Tennis Fans', 'SIG 15 - Club Rugby Fans', 'SIG 20 - Cricket Fans')
                                                                                        then 452
                when eng.Sky_Branded_Channels in ('High')                               then 454
                when eng.Sky_Sports_News_SoV in ('High')                                then 456
                when eng.Movies_SOV in ('High')                                         then 458
                when prof.Number_Of_Sky_Products_No_BB >= 5                             then 460
                when cl_eng.CL_SOC in ('High')                                          then 462
                when prof.Value_Segment = 'F) Unstable'                                 then 464
                when prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                        then 466
                  else                                                                       465
            end

          -- #######################################################################################################
          -- ##### Risk group 5 #####
        when base.xRisk_Segment_1 in ('Risk group 5') then                                   593

          -- #######################################################################################################
          -- ##### Excluded #####
          else 0

      end,


       a.Risk_Segment_4  =                                                  -- EPL risk group - Sky loses majority of EPL
      case

          -- #######################################################################################################
          -- ##### Risk group 1 #####
        when base.xRisk_Segment_2 in ('Risk group 1') then
            case
                when prof.Sports_Segment_SIG in ('SIG 01 - International Rugby Fans', 'SIG 06 - Motor Sport Fans', 'SIG 08 - F1 Super Fans', 'SIG 09 - Super Sports Fans', 'SIG 10 - Sports Disengaged',
                                                 'SIG 11 - Cricket Fanatics', 'SIG 14 - Tennis Fans', 'SIG 15 - Club Rugby Fans', 'SIG 20 - Cricket Fans')
                                                                                        then 152
                when eng.Sky_Branded_Channels in ('High')                               then 154
                when eng.Sky_Sports_News_SoV in ('High')                                then 156
                when eng.Movies_SOV in ('High')                                         then 158
                when prof.Number_Of_Sky_Products_No_BB >= 5                             then 160
                  else                                                                       159
            end

          -- #######################################################################################################
          -- ##### Risk group 2 #####
        when base.xRisk_Segment_2 in ('Risk group 2') then
            case
                when prof.Sports_Segment_SIG in ('SIG 01 - International Rugby Fans', 'SIG 06 - Motor Sport Fans', 'SIG 08 - F1 Super Fans', 'SIG 09 - Super Sports Fans', 'SIG 10 - Sports Disengaged',
                                                 'SIG 11 - Cricket Fanatics', 'SIG 14 - Tennis Fans', 'SIG 15 - Club Rugby Fans', 'SIG 20 - Cricket Fans')
                                                                                        then 252
                when eng.Sky_Branded_Channels in ('High')                               then 254
                when eng.Sky_Sports_News_SoV in ('High')                                then 256
                when eng.Movies_SOV in ('High')                                         then 258
                when prof.Number_Of_Sky_Products_No_BB >= 5                             then 260
                when cl_eng.CL_SOC in ('High')                                          then 262
                when prof.Value_Segment = 'F) Unstable'                                 then 264
                when prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                        then 266
                  else                                                                       265
            end

          -- #######################################################################################################
          -- ##### Risk group 3 #####
        when base.xRisk_Segment_2 in ('Risk group 3') then
            case
                when prof.Sports_Segment_SIG in ('SIG 01 - International Rugby Fans', 'SIG 06 - Motor Sport Fans', 'SIG 08 - F1 Super Fans', 'SIG 09 - Super Sports Fans', 'SIG 10 - Sports Disengaged',
                                                 'SIG 11 - Cricket Fanatics', 'SIG 14 - Tennis Fans', 'SIG 15 - Club Rugby Fans', 'SIG 20 - Cricket Fans')
                                                                                        then 352
                when eng.Sky_Branded_Channels in ('High')                               then 354
                when eng.Sky_Sports_News_SoV in ('High')                                then 356
                when eng.Movies_SOV in ('High')                                         then 358
                when prof.Number_Of_Sky_Products_No_BB >= 5                             then 360
                when cl_eng.CL_SOC in ('High')                                          then 362
                when prof.Value_Segment = 'F) Unstable'                                 then 364
                when prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                        then 366
                  else                                                                       365
            end

          -- #######################################################################################################
          -- ##### Risk group 4 #####
        when base.xRisk_Segment_2 in ('Risk group 4') then
            case
                when prof.Sports_Segment_SIG in ('SIG 01 - International Rugby Fans', 'SIG 06 - Motor Sport Fans', 'SIG 08 - F1 Super Fans', 'SIG 09 - Super Sports Fans', 'SIG 10 - Sports Disengaged',
                                                 'SIG 11 - Cricket Fanatics', 'SIG 14 - Tennis Fans', 'SIG 15 - Club Rugby Fans', 'SIG 20 - Cricket Fans')
                                                                                        then 452
                when eng.Sky_Branded_Channels in ('High')                               then 454
                when eng.Sky_Sports_News_SoV in ('High')                                then 456
                when eng.Movies_SOV in ('High')                                         then 458
                when prof.Number_Of_Sky_Products_No_BB >= 5                             then 460
                when cl_eng.CL_SOC in ('High')                                          then 462
                when prof.Value_Segment = 'F) Unstable'                                 then 464
                when prof.FSS in ('07) Consolidating Assets', '08) Balancing Budgets', '09) Stretched Finances', '14) Traditional Thrift')
                                                                                        then 466
                  else                                                                       465
            end

          -- #######################################################################################################
          -- ##### Risk group 5 #####
        when base.xRisk_Segment_2 in ('Risk group 5') then                                   593

          -- #######################################################################################################
          -- ##### Excluded #####
          else 0

      end

  from EPL_07_Risk_Groups_View base,
       EPL_04_Eng_Matrix eng,
       EPL_04_Profiling_Variables prof,
       EPL_54_CL_Eng_Matrix cl_eng

 where a.Account_Number = base.Account_Number
   and a.Period = base.Period
   and a.Sports_Package = 'Sky Sports'

   and a.Account_Number = eng.Account_Number
   and a.Period = eng.Period
   and eng.Metric = 'Overall'

   and a.Account_Number = prof.Account_Number
   and a.Period = prof.Period

   and a.Account_Number = cl_eng.Account_Number
   and a.Period = cl_eng.Period
   and cl_eng.Metric = 'Overall';
commit;







  -- ##############################################################################################################
  -- ##### V11                                                                                                #####
  -- #####   - Tree revised, risk groups removed, top three variables changed                                 #####
  -- ##############################################################################################################
  -- Basic risk groups
update EPL_07_Risk_Groups base
   set base.Risk_Segment_1  =                                               -- Basic risk group - Sky loses EPL In full
      case
        when det.EPL_SoSV in ('High')                                                                                                               then 2

        when det.EPL_SoSV in ('Medium')           and prof.Sports_Segment_SIG_v3 = 'Key SIGs'                                                       then 7
        when det.EPL_SoSV in ('Medium')           and prof.Sports_Segment_SIG_v3 = 'Non-key SIGs'                                                   then 8

        when det.EPL_SoSV in ('Low')              and det.EPL_SOC in ('Medium', 'High')             and prof.Sports_Segment_SIG_v4 = 'Key SIGs'     then 9
        when det.EPL_SoSV in ('Low')              and det.EPL_SOC in ('Medium', 'High')             and prof.Sports_Segment_SIG_v4 = 'Non-key SIGs' then 10
        when det.EPL_SoSV in ('Low')              and det.EPL_SOC in ('Low')                                                                        then 6
          else 0
      end
  from EPL_04_Eng_Matrix det,
       EPL_04_Profiling_Variables prof
 where base.Account_Number = det.Account_Number
   and base.Period = det.Period
   and base.Sports_Package = 'Sky Sports'
   and det.Metric = 'Overall'
   and det.Account_Number = prof.Account_Number
   and det.Period = prof.Period;
commit;



-- This view take information from fields updated above AND below, needs to be updated only once though.
create or replace view EPL_07_Risk_Groups_View as
  select
        a.Account_Number,
        a.Period,
        a.Sports_Package,

        a.Risk_Segment_1 as Risk_Segment_1_Raw,
        case
          when a.Risk_Segment_1 in (2)              then 'Branch 1'
          when a.Risk_Segment_1 in (7)              then 'Branch 2'
          when a.Risk_Segment_1 in (8)              then 'Branch 3'
          when a.Risk_Segment_1 in (9)              then 'Branch 4'
          when a.Risk_Segment_1 in (10)             then 'Branch 5'
          when a.Risk_Segment_1 in (6)              then 'Branch 6'
          when a.Risk_Segment_1 = 0             then 'Excluded'
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

            else 'No Sky Sports'
        end as xRisk_Segment_3

    from EPL_07_Risk_Groups a;
commit;


  -- EPL risk groups
update EPL_07_Risk_Groups a
   set a.Risk_Segment_3  =                                                  -- EPL risk group - Sky loses EPL In full
      case

          -- #######################################################################################################
          -- ##### Branch 1 #####
        when base.xRisk_Segment_1 in ('Branch 1') then
            case
                when eng.Sky_Branded_Channels in ('High')                               then 102
                when eng.Sky_Sports_News_SoV in ('High')                                then 104
                when eng.Movies_SOV in ('High')                                         then 106
                when prof.Number_Of_Sky_Products_No_BB >= 5                             then 108
                when eng.Sport_SoV in ('High', 'Very high')                             then 110
                  else                                                                       109
            end

          -- #######################################################################################################
          -- ##### Branch 2 #####
        when base.xRisk_Segment_1 in ('Branch 2')                                       then 216

          -- #######################################################################################################
          -- ##### Branch 3 #####
        when base.xRisk_Segment_1 in ('Branch 3') then
            case
                when eng.Sky_Branded_Channels in ('High')                               then 302
                when eng.Sky_Sports_News_SoV in ('High')                                then 304
                when eng.Movies_SOV in ('High')                                         then 306
                when prof.Number_Of_Sky_Products_No_BB >= 5                             then 308
                when eng.Sport_SoV in ('High', 'Very high')                             then 310
                  else                                                                       309
            end

          -- #######################################################################################################
          -- ##### Branch 4 #####
        when base.xRisk_Segment_1 in ('Branch 4')                                       then 416

          -- #######################################################################################################
          -- ##### Branch 5 #####
        when base.xRisk_Segment_1 in ('Branch 5') then
            case
                when eng.Sky_Branded_Channels in ('High')                               then 502
                when eng.Sky_Sports_News_SoV in ('High')                                then 504
                when eng.Movies_SOV in ('High')                                         then 506
                when prof.Number_Of_Sky_Products_No_BB >= 5                             then 508
                when eng.Sport_SoV in ('High', 'Very high')                             then 510
                  else                                                                       509
            end

          -- #######################################################################################################
          -- ##### Branch 6 #####
        when base.xRisk_Segment_1 in ('Branch 6')                                       then 616

          -- #######################################################################################################
          -- ##### Excluded #####
          else 0

      end

  from EPL_07_Risk_Groups_View base,
       EPL_04_Eng_Matrix eng,
       EPL_04_Profiling_Variables prof,
       EPL_54_CL_Eng_Matrix cl_eng

 where a.Account_Number = base.Account_Number
   and a.Period = base.Period
   and a.Sports_Package = 'Sky Sports'

   and a.Account_Number = eng.Account_Number
   and a.Period = eng.Period
   and eng.Metric = 'Overall'

   and a.Account_Number = prof.Account_Number
   and a.Period = prof.Period

   and a.Account_Number = cl_eng.Account_Number
   and a.Period = cl_eng.Period
   and cl_eng.Metric = 'Overall';
commit;







  -- ##############################################################################################################
  -- ##### V12                                                                                                #####
  -- #####   - FSS layer added and definitions & thresholds changed for some variables                        #####
  -- ##############################################################################################################


*/




