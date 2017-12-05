/*###############################################################################
# Created on:   17/03/2014
# Created by:   Sebastian Bednaszynski(SBE)
# Description:  EPL rights project - publishing results
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 17/03/2014  SBE   Initial version
#
###############################################################################*/


  -- ##############################################################################################################
  -- ##### Publish results                                                                                    #####
  -- ##############################################################################################################
select count(*) as Cnt, count(distinct Account_Number) as Unq_Cnt from EPL_04_Eng_Matrix where Period = 1;
select count(*) as Cnt, count(distinct Account_Number) as Unq_Cnt from EPL_04_Profiling_Variables where Period = 1;
select count(*) as Cnt, count(distinct Account_Number) as Unq_Cnt from EPL_05_Scaling_Weights where Period = 1;

if object_id('EPL_10_Results') is not null then drop table EPL_10_Results end if;
select
      a.Account_Number                              as x00_Account_Number,
      case
        when a.Period = 1 then '2) Aug ''13 - Feb ''14'
        when a.Period = 2 then '1) Feb ''13 - Jul ''13'
        when a.Period = 3 then '3) Feb ''13 - May ''13'
          else '???'
      end                                           as x01_Period,
      case
        when a.Low_Content_Flag = 1 then 'Low'
          else 'Normal'
      end                                           as x02_Low_Content_Flag,
      a.Metric                                      as x03_Metric,
      a.Category                                    as x04_Category,
      a.EPL_SOC                                     as x05a_EPL_SOC__HML,
      a.EPL_SoSV                                    as x05b_EPL_SoSV__HML,
      a.Sport_SoV                                   as x05c_Sport_SoV__HML,
      a.Key_Pay_Entertainment_Avg_DV                as x05d_Key_Pay_Entertainment_Avg_DV,
      a.Sky_Sports_News_Avg_DV                      as x05e_Sky_Sports_News_Avg_DV,
      a.Movies_Avg_DV                               as x05f_Movies_Avg_DV,

      f.CL_SOC                                      as x22a_CL_SOC__HML,

      g.EPL_SOC                                     as x23a_Sky_Only_EPL_SOC__HML,
      g.EPL_SOC__Deciles                            as x23b_Sky_Only_EPL_SOC__Deciles,
      g.EPL_SOC__Percentiles                        as x23c_Sky_Only_EPL_SOC__Percentiles,
      g.EPL_SOC__010_Groups                         as x23d_Sky_Only_EPL_SOC__010_Groups,
      g.EPL_SOC__100_Groups                         as x23e_Sky_Only_EPL_SOC__100_Groups,
      g.EPL_SoSV                                    as x24a_Sky_Only_EPL_SoSV__HML,
      g.EPL_SoSV__Deciles                           as x24b_Sky_Only_EPL_SoSV__Deciles,
      g.EPL_SoSV__Percentiles                       as x24c_Sky_Only_EPL_SoSV__Percentiles,

      b.Value_Segment                               as x34_Value_Segment,
      b.Sports_Segment_SIG                          as x35_Sports_Segment_SIG,
      b.Survey__EPL_Main_Reason                     as x36_Survey__EPL_Main_Reason,
      b.Survey__Num_Of_Sports_Claimed               as x37_Survey__Num_Of_Sports_Claimed,

      b.Base_Package                                as x38_Base_Package,
      b.Prem_Movies                                 as x39_Prem_Movies,
      b.Prem_Sports                                 as x40_Prem_Sports,
      b.TV_Package                                  as x41_TV_Package,
      b.Broadband                                   as x42_Broadband,
      b.HD                                          as x43_HD,
      b.Multiscreen                                 as x44_Multiscreen,
      b.Sky_Product                                 as x45_Sky_Product,

      b.Sports_Tenure_Continuous                    as x46_Sports_Tenure_Continuous,
      b.Sports_Downgrade_Event                      as x47_Sports_Downgrade_Event,
      b.Sports_Upgrade_Event                        as x48_Sports_Upgrade_Event,
      b.BT_Sport_Viewier                            as x49_BT_Sport_Viewier,
      b.Pay_TV_Consumption_Segment                  as x50_Pay_TV_Consumption_Segment,
      b.On_Demand_Usage_Segment                     as x51_On_Demand_Usage_Segment,
      b.Sky_Go_EPL_Usage_Segment                    as x52_Sky_Go_EPL_Usage_Segment,
      b.Sky_Go_Any_Usage_Segment                    as x53_Sky_Go_Any_Usage_Segment,
      b.HH_Composition                              as x54_HH_Composition,
      b.Region                                      as x55_Region,
      b.Affluence_Band                              as x56_Affluence_Band,
      b.FSS                                         as x57_FSS,
      b.CQM_Score                                   as x58_CQM_Score,
      b.Cable_Area                                  as x59_Cable_Area,
      b.Postcode_District                           as x60_Postcode_District,
      b.Mosaic_Segment                              as x61_Mosaic_Segment,
      b.Lifestage                                   as x62_Lifestage,
      b.Simple_Segment                              as x63_Simple_Segment,
      b.Simple_Sub_Segment                          as x64_Simple_Sub_Segment,

      e.xRisk_Segment_1                             as x81_Risk__All_EPL_Lost__Branch,
      e.xRisk_Segment_3                             as x83_Risk__All_EPL_Lost__Segment,

      count(distinct a.Account_Number)              as x90_Accounts_Unscaled,
      sum(c.Scaling_Weight)                         as x91_Accounts_Scaled,
      sum(d.No_PPV_Contribution)                    as x92_Total_Contribution_Unscaled,
      sum(d.No_PPV_Contribution * c.Scaling_Weight) as x93_Total_Contribution_Scaled,
      count(distinct case
                       when b.CQM_Score is null then null
                       when b.CQM_Score = 'Unknown' then null
                         else a.Account_Number
                     end)                           as x94_Accounts_With_CQM_Scores,
      sum(case
            when b.CQM_Score is null then 0
            when b.CQM_Score = 'Unknown' then 0
              else cast(b.CQM_Score as smallint)
          end)                                      as x95_Total_CQM_Scores,
      sum(b.Number_Of_Sky_Products)                 as x96a_Number_Of_Sky_Products,
      sum(b.Number_Of_Sky_Products_GO_OD)           as x96b_Number_Of_Sky_Products_Go_OD,
      sum(b.Number_Of_Sky_Products_No_BB)           as x96c_Number_Of_Sky_Products_No_BB,
      sum(b.Number_Of_Sky_Products_No_DTV)          as x96d_Number_Of_Sky_Products_No_DTV,

      sum(b.Bill_Payment_L12m)                         as x97a_Bill_Payment_L12m,
      sum(b.Bill_Payment_Avg_Monthly_L12m)             as x97b_Bill_Payment_Avg_Monthly_L12m,
      sum(b.Bill_Payment_Annuallised_Monthly_L12m)     as x97c_Bill_Payment_Annuallised_Monthly_L12m,
      sum(b.Bill_Balance_Due_L12m)                     as x97d_Bill_Balance_Due_L12m,
      sum(b.Bill_Balance_Due_Avg_Monthly_L12m)         as x97e_Bill_Balance_Due_Avg_Monthly_L12m,
      sum(b.Bill_Balance_Due_Annuallised_Monthly_L12m) as x97f_Bill_Balance_Due_Annuallised_Monthly_L12m

  into EPL_10_Results
  from EPL_04_Eng_Matrix a,
       EPL_04_Profiling_Variables b,
       EPL_05_Scaling_Weights c,
       EPL_05_Contribution d,
       EPL_07_Risk_Groups_View e,
       EPL_54_CL_Eng_Matrix f,
       EPL_04_Eng_Matrix_Sky_Sports_Only g
 where a.Account_Number = b.Account_Number
   and a.Account_Number = c.Account_Number
   and a.Account_Number = d.Account_Number
   and a.Account_Number = e.Account_Number
   and a.Account_Number = f.Account_Number
   and a.Account_Number = g.Account_Number
   and a.Period = b.Period
   and a.Period = c.Period
   and a.Period = d.Period
   and a.Period = e.Period
   and a.Period = f.Period
   and a.Period = g.Period
   and a.Period = 1
   and a.Metric = g.Metric
   and a.Category = g.Category
 group by
          x00_Account_Number,
          x01_Period,

          x02_Low_Content_Flag,
          x03_Metric,
          x04_Category,
          x05a_EPL_SOC__HML,
          x05b_EPL_SoSV__HML,
          x05c_Sport_SoV__HML,
          x05d_Key_Pay_Entertainment_Avg_DV,
          x05e_Sky_Sports_News_Avg_DV,
          x05f_Movies_Avg_DV,

          x22a_CL_SOC__HML,

          x23a_Sky_Only_EPL_SOC__HML,
          x23b_Sky_Only_EPL_SOC__Deciles,
          x23c_Sky_Only_EPL_SOC__Percentiles,
          x23d_Sky_Only_EPL_SOC__010_Groups,
          x23e_Sky_Only_EPL_SOC__100_Groups,
          x24a_Sky_Only_EPL_SoSV__HML,
          x24b_Sky_Only_EPL_SoSV__Deciles,
          x24c_Sky_Only_EPL_SoSV__Percentiles,

          x34_Value_Segment,
          x35_Sports_Segment_SIG,
          x36_Survey__EPL_Main_Reason,
          x37_Survey__Num_Of_Sports_Claimed,

          x38_Base_Package,
          x39_Prem_Movies,
          x40_Prem_Sports,
          x41_TV_Package,
          x42_Broadband,
          x43_HD,
          x44_Multiscreen,
          x45_Sky_Product,

          x46_Sports_Tenure_Continuous,
          x47_Sports_Downgrade_Event,
          x48_Sports_Upgrade_Event,
          x49_BT_Sport_Viewier,
          x50_Pay_TV_Consumption_Segment,
          x51_On_Demand_Usage_Segment,
          x52_Sky_Go_EPL_Usage_Segment,
          x53_Sky_Go_Any_Usage_Segment,
          x54_HH_Composition,
          x55_Region,
          x56_Affluence_Band,
          x57_FSS,
          x58_CQM_Score,
          x59_Cable_Area,
          x60_Postcode_District,
          x61_Mosaic_Segment,
          x62_Lifestage,
          x63_Simple_Segment,
          x64_Simple_Sub_Segment,

          x81_Risk__All_EPL_Lost__Branch,
          x83_Risk__All_EPL_Lost__Segment;
commit;
create        lf   index idx01 on EPL_10_Results(x03_Metric);
grant select on EPL_10_Results to vespa_group_low_security;
grant select on EPL_10_Results to vespa_crouchr;

select * from EPL_10_Results;
select x03_Metric, x04_Category, count(*) as Cnt, sum(x90_Accounts_Unscaled) as Accounts_Unscaled, sum(x91_Accounts_Scaled) as Accounts_Scaled from EPL_10_Results  group by x03_Metric, x04_Category order by 1, 2;


/*
-- CUT DOWN VERSION

if object_id('EPL_10_Results_TMP') is not null then drop table EPL_10_Results_TMP end if;
select
      case
        when a.Period = 1 then '2) Aug ''13 - Feb ''14'
        when a.Period = 2 then '1) Feb ''13 - Jul ''13'
        when a.Period = 3 then '3) Feb ''13 - May ''13'
          else '???'
      end                                           as x01_Period,
      case
        when a.Low_Content_Flag = 1 then 'Low'
          else 'Normal'
      end                                           as x02_Low_Content_Flag,
      a.Metric                                      as x03_Metric,
      a.Category                                    as x04_Category,

      a.Movies_SoV                                  as x09a_Movies_SoV__HML,
      b.Value_Segment                               as x34_Value_Segment,
      b.Sports_Segment_SIG                          as x35_Sports_Segment_SIG,
      b.Prem_Movies                                 as x39_Prem_Movies,
      b.Prem_Sports                                 as x40_Prem_Sports,
      b.Broadband                                   as x42_Broadband,
      b.HD                                          as x43_HD,
      b.Multiscreen                                 as x44_Multiscreen,
      b.HH_Composition                              as x54_HH_Composition,
      b.FSS                                         as x57_FSS,
      b.CQM_Score                                   as x58_CQM_Score,
      b.Cable_Area                                  as x59_Cable_Area,
      e.Risk_Segment_3                              as x83_Risk__All_EPL_Lost__Segment,
      e.Risk_Segment_4                              as x84_Risk__Most_EPL_Lost__Segment,

      count(distinct a.Account_Number)              as x90_Accounts_Unscaled,
      sum(c.Scaling_Weight)                         as x91_Accounts_Scaled,
      sum(b.Number_Of_Sky_Products)                 as x96_Number_Of_Sky_Products

  into EPL_10_Results_TMP
  from EPL_04_Eng_Matrix a,
       EPL_04_Profiling_Variables b,
       EPL_05_Scaling_Weights c,
       EPL_05_Contribution d,
       EPL_07_Risk_Groups e
 where a.Account_Number = b.Account_Number
   and a.Account_Number = c.Account_Number
   and a.Account_Number = d.Account_Number
   and a.Account_Number = e.Account_Number
   and a.Period = b.Period
   and a.Period = c.Period
   and a.Period = d.Period
   and a.Period = e.Period
   and a.Period = 1
 group by
          x01_Period,
          x02_Low_Content_Flag,
          x03_Metric,
          x04_Category,
          x09a_Movies_SoV__HML,

          x34_Value_Segment,
          x35_Sports_Segment_SIG,
          x39_Prem_Movies,
          x40_Prem_Sports,
          x42_Broadband,
          x43_HD,
          x44_Multiscreen,
          x54_HH_Composition,
          x57_FSS,
          x58_CQM_Score,
          x59_Cable_Area,

          x83_Risk__All_EPL_Lost__Segment,
          x84_Risk__Most_EPL_Lost__Segment
        ;
commit;


*/



  -- ##############################################################################################################
  -- ##############################################################################################################
  -- Cut-down version for research
  -- ~~~~~~ Research data 1 (sports customers) ~~~~~~
if object_id('EPL_11_Results_Research') is not null then drop table EPL_11_Results_Research end if;
select
      case
        when a.Period = 1 then '2) Aug ''13 - Feb ''14'
        when a.Period = 2 then '1) Feb ''13 - Jul ''13'
        when a.Period = 3 then '3) Feb ''13 - May ''13'
          else '???'
      end                                           as x01_Period,
      a.EPL_SOC                                     as x05_EPL_SOC,
      b.Sports_Segment_SIG                          as x35_Sports_Segment_SIG,
      b.Base_Package                                as x38_Base_Package,
      b.Prem_Movies                                 as x39_Prem_Movies,
      b.Prem_Sports                                 as x40_Prem_Sports,
      b.TV_Package                                  as x41_TV_Package,
      b.Broadband                                   as x42_Broadband,
      b.HD                                          as x43_HD,
      b.BT_Sport_Viewier                            as x49_BT_Sport_Viewier,
      count(distinct a.Account_Number)              as x90_Accounts_Unscaled

  into EPL_11_Results_Research
  from EPL_04_Eng_Matrix a,
       EPL_04_Profiling_Variables b,
       EPL_05_Scaling_Weights c,
       EPL_05_Contribution d
 where a.Account_Number = b.Account_Number
   and a.Account_Number = c.Account_Number
   and a.Account_Number = d.Account_Number
   and a.Period = b.Period
   and a.Period = c.Period
   and a.Period = d.Period
   and a.Period = 1
   and a.Metric = 'Overall'
 group by
      x01_Period,
      x05_EPL_SOC,
      x35_Sports_Segment_SIG,
      x38_Base_Package,
      x39_Prem_Movies,
      x40_Prem_Sports,
      x41_TV_Package,
      x42_Broadband,
      x43_HD,
      x49_BT_Sport_Viewier
      ;
commit;
grant select on EPL_11_Results_Research to vespa_group_low_security;
grant select on EPL_11_Results_Research to vespa_crouchr;



  -- Export accounts for selection
if object_id('EPL_11_Results_Research_Sample') is not null then drop table EPL_11_Results_Research_Sample end if;
select
      a.Account_Number,
      cast(case
             when b.Latest_Active_Date = '2014-02-28' then 1
               else 0
           end as bit) as DTV_Active,
      cast(null as decimal(15, 10)) as Rand_Num,
      cast(0 as tinyint) as Selected
  into EPL_11_Results_Research_Sample
  from EPL_04_Eng_Matrix a,
       EPL_04_Profiling_Variables b
 where a.Account_Number = b.Account_Number
   and a.Period = b.Period
   and a.Period = 1
   and a.Metric = 'Overall'
   and b.Prem_Sports in (1, 2)
      ;
commit;

create variable @multiplier bigint;
set @multiplier = datepart(millisecond, now()) + 1;

update EPL_11_Results_Research_Sample
   set Rand_Num = rand(number(*) * @multiplier);
commit;

/*
  -- With threshold
update EPL_11_Results_Research_Sample base
   set base.Selected = 1
  from (select 1.0 * 100000 / count(*) as Threshold
          from EPL_11_Results_Research_Sample
         where DTV_Active = 1) det
 where base.Rand_Num <= det.Threshold
   and base.DTV_Active = 1;
commit;
*/

update EPL_11_Results_Research_Sample base
   set base.Selected = 2
 where base.Selected = 0
   and base.DTV_Active = 1;
commit;


  -- Exclude a few not correctly captured cases
update EPL_11_Results_Research_Sample base
   set base.Selected = 0
  from (select
               a.Account_Number,
               det.Status_Code,
               cel.Prem_Sports
          from EPL_11_Results_Research_Sample a,
               sk_prod.cust_subs_hist det
                  left join sk_prod.cust_entitlement_lookup as cel  on det.current_short_description = cel.short_description
         where a.Account_Number = det.Account_Number
           and Effective_From_Dt <= '2014-02-28'
           and Effective_To_Dt > '2014-02-28'
           and Subscription_Sub_Type = 'DTV Primary Viewing'
           and Effective_From_Dt < Effective_To_Dt
           and (
                Status_Code in ('PO', 'SC')                 -- Inactive
                or
                Prem_Sports = 0                             -- Non SS subscriber
               )
           and a.Selected > 0) det
 where base.Account_Number = det.Account_Number;
commit;


  -- QA
select count(*) as Cnt, count(distinct Account_number) as Accts, 1.0 * 100000 / count(*) as Threshold, sum(case when Selected > 0 then 1 else 0 end) as Selection_Vol, sum(DTV_Active) as DTV_Active_Vol
  from EPL_11_Results_Research_Sample;

select DTV_Active, Selected, count(*) as Cnt
  from EPL_11_Results_Research_Sample
 group by DTV_Active, Selected
 order by DTV_Active, Selected;

select
       Status_Code,
       Prem_Sports,
       count(*) as cnt
  from sk_prod.cust_subs_hist det
          left join sk_prod.cust_entitlement_lookup as cel  on det.current_short_description = cel.short_description
 where Effective_From_Dt <= '2014-02-28'
   and Effective_To_Dt > '2014-02-28'
   and Subscription_Sub_Type = 'DTV Primary Viewing'
   and Effective_From_Dt < Effective_To_Dt
   and Account_Number in (select Account_Number from EPL_11_Results_Research_Sample where Selected = 1)
 group by
       Status_Code,
       Prem_Sports
 order by
       Status_Code,
       Prem_Sports;


  -- Export
select Account_Number
  from EPL_11_Results_Research_Sample
 where Selected > 0;
output to 'C:\_Playpen_\KNOWLEDGE DEVELOPMENT\Projects\05) EPL work\EPL_11_Results_Research_Sample v2.csv';



  -- ~~~~~~ Research data 2 (non-sports customers) ~~~~~~
if object_id('EPL_11_Results_Research_Sample_2') is not null then drop table EPL_11_Results_Research_Sample_2 end if;
select
      a.Account_Number,
      cast(case
             when b.Latest_Active_Date = '2014-02-28' then 1
               else 0
           end as bit) as DTV_Active,
      cast(null as decimal(15, 10)) as Rand_Num,
      cast(0 as tinyint) as Selected
  into EPL_11_Results_Research_Sample_2
  from EPL_04_Eng_Matrix a,
       EPL_04_Profiling_Variables b
 where a.Account_Number = b.Account_Number
   and a.Period = b.Period
   and a.Period = 1
   and a.Metric = 'Overall'
   and b.Prem_Sports not in (1, 2)
      ;
commit;


create variable @multiplier bigint;
set @multiplier = datepart(millisecond, now()) + 1;

update EPL_11_Results_Research_Sample_2
   set Rand_Num = rand(number(*) * @multiplier);
commit;

/*
  -- With threshold
update EPL_11_Results_Research_Sample_2 base
   set base.Selected = 1
  from (select 1.0 * 100000 / count(*) as Threshold
          from EPL_11_Results_Research_Sample_2
         where DTV_Active = 1) det
 where base.Rand_Num <= det.Threshold
   and base.DTV_Active = 1;
commit;
*/

update EPL_11_Results_Research_Sample_2 base
   set base.Selected = 2
 where base.Selected = 0
   and base.DTV_Active = 1;
commit;


  -- Exclude a few not correctly captured cases
update EPL_11_Results_Research_Sample_2 base
   set base.Selected = 0
  from (select
               a.Account_Number,
               det.Status_Code,
               cel.Prem_Sports
          from EPL_11_Results_Research_Sample_2 a,
               sk_prod.cust_subs_hist det
                  left join sk_prod.cust_entitlement_lookup as cel  on det.current_short_description = cel.short_description
         where a.Account_Number = det.Account_Number
           and Effective_From_Dt <= '2014-06-12'            -- As of June, the 12th when the second extract is run
           and Effective_To_Dt > '2014-06-12'
           and Subscription_Sub_Type = 'DTV Primary Viewing'
           and Effective_From_Dt < Effective_To_Dt
           and (
                Status_Code in ('PO', 'SC')                 -- Inactive
                or
                Prem_Sports > 0                             -- Had sports
               )
           and a.Selected > 0) det
 where base.Account_Number = det.Account_Number;
commit;


  -- QA
select count(*) as Cnt, count(distinct Account_number) as Accts, 1.0 * 100000 / count(*) as Threshold, sum(case when Selected > 0 then 1 else 0 end) as Selection_Vol, sum(DTV_Active) as DTV_Active_Vol
  from EPL_11_Results_Research_Sample_2;

select DTV_Active, Selected, count(*) as Cnt
  from EPL_11_Results_Research_Sample_2
 group by DTV_Active, Selected
 order by DTV_Active, Selected;

select
       Status_Code,
       Prem_Sports,
       count(*) as cnt
  from sk_prod.cust_subs_hist det
          left join sk_prod.cust_entitlement_lookup as cel  on det.current_short_description = cel.short_description
 where Effective_From_Dt <= '2014-06-12'                    -- As of June, the 12th when the second extract is run
   and Effective_To_Dt > '2014-06-12'
   and Subscription_Sub_Type = 'DTV Primary Viewing'
   and Effective_From_Dt < Effective_To_Dt
   and Account_Number in (select Account_Number from EPL_11_Results_Research_Sample_2 where Selected = 1)
 group by
       Status_Code,
       Prem_Sports
 order by
       Status_Code,
       Prem_Sports;


  -- Export
select Account_Number
  from EPL_11_Results_Research_Sample_2
 where Selected > 0;
output to 'C:\_Playpen_\KNOWLEDGE DEVELOPMENT\Projects\05) EPL work\EPL_11_Results_Research_Sample (non-sports) v1.csv';



  -- ##############################################################################################################
  -- ##############################################################################################################















