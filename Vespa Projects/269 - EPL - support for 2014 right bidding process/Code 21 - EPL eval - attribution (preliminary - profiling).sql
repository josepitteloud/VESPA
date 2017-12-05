/*###############################################################################
# Created on:   28/04/2014
# Created by:   Sebastian Bednaszynski(SBE)
# Description:  EPL rights project - attribution
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 28/04/2014  SBE   Initial version
#
###############################################################################*/


  -- ##############################################################################################################
  -- ##### Create table and pull existing information from the profiling analysis                             #####
  -- ##############################################################################################################
if object_id('EPL_35_Attribution') is not null then drop table EPL_35_Attribution end if;
create table EPL_35_Attribution (
    Pk_Identifier                           bigint            identity,
    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name(),

      -- Account
    Account_Number                          varchar(20)       null      default null,
    Period                                  tinyint           null      default 0,
    Latest_Active_Date                      date              null      default null,
    Scaling_Weight                          decimal(15, 6)    null      default 0,

    Risk__All_EPL_Lost__Risk_Group          varchar(50)       null      default '???',
    Risk__All_EPL_Lost__Segment             varchar(50)       null      default '???',
    Risk__All_EPL_Lost__Segment_Grouped_1   varchar(50)       null      default '???',
    Risk__All_EPL_Lost__Segment_Grouped_2   varchar(50)       null      default '???',

    Risk__Most_EPL_Lost__Risk_Group         varchar(50)       null      default '???',
    Risk__Most_EPL_Lost__Segment            varchar(50)       null      default '???',
    Risk__Most_EPL_Lost__Segment_Grouped_1  varchar(50)       null      default '???',
    Risk__Most_EPL_Lost__Segment_Grouped_2  varchar(50)       null      default '???',

      -- Variables
    Value_Segment                           varchar(25)       null      default 'Z) Unknown',

    Sports_Segment_SIG                      varchar(50)       null      default 'SIG 99 - Unknown',
    Survey__EPL_Main_Reason                 varchar(25)       null      default 'Unknown',
    Survey__Num_Of_Sports_Claimed           varchar(25)       null      default 'Unknown',

    Base_Package                            varchar(25)       null      default 'Unknown',
    Prem_Movies                             smallint          null      default 0,
    Prem_Sports                             smallint          null      default 0,
    TV_Package                              varchar(50)       null      default 'Unknown',

    SkyTalk                                 varchar(25)       null      default 'No SkyTalk',
    Broadband                               varchar(25)       null      default 'No Broadband',
    HD                                      varchar(25)       null      default 'No HD',
    Multiscreen                             varchar(25)       null      default 'No Multiscreen',
    Sky_Product                             varchar(50)       null      default 'Unknown',
    Number_Of_Sky_Products                  tinyint           null      default 0,

    Sports_Tenure_Continuous                varchar(25)       null      default 'Unknown',

    Sports_Downgrade_Event                  varchar(25)       null      default 'Unknown',
    Sports_Upgrade_Event                    varchar(25)       null      default 'Unknown',

    BT_Sport_Viewier                        varchar(25)       null      default 'Unknown',
    Pay_TV_Consumption_Segment              varchar(25)       null      default 'Unknown',

    On_Demand_Usage_Segment                 varchar(25)       null      default '1) Non-OnDemand user',
    Sky_Go_EPL_Usage_Segment                varchar(25)       null      default '1) Non-Sky Go user',
    Sky_Go_Any_Usage_Segment                varchar(25)       null      default '1) Non-Sky Go user',

    HH_Composition                          varchar(50)       null      default 'Unknown',
    Region                                  varchar(100)      null      default 'Unknown',
    Affluence_Band                          varchar(50)       null      default 'Unknown',
    FSS                                     varchar(50)       null      default '99) Unknown',
    CQM_Score                               varchar(10)       null      default 'Unknown',
    Cable_Area                              varchar(10)       null      default 'Unknown'
);
create        hg   index idx01 on EPL_35_Attribution(Account_Number);
create        lf   index idx02 on EPL_35_Attribution(Period);
create        date index idx03 on EPL_35_Attribution(Latest_Active_Date);
create unique hg   index idx04 on EPL_35_Attribution(Account_Number, Period);
grant select on EPL_35_Attribution to vespa_group_low_security;
grant select on EPL_35_Attribution to vespa_crouchr;


insert into EPL_35_Attribution
      (Account_Number, Period, Latest_Active_Date, Scaling_Weight,
       Risk__All_EPL_Lost__Risk_Group, Risk__All_EPL_Lost__Segment, Risk__Most_EPL_Lost__Risk_Group, Risk__Most_EPL_Lost__Segment,
       Value_Segment,
       Sports_Segment_SIG, Survey__EPL_Main_Reason, Survey__Num_Of_Sports_Claimed, Base_Package, Prem_Movies, Prem_Sports,
       TV_Package, SkyTalk, Broadband, HD, Multiscreen, Sky_Product, Number_Of_Sky_Products, Sports_Tenure_Continuous,
       Sports_Downgrade_Event, Sports_Upgrade_Event, BT_Sport_Viewier, Pay_TV_Consumption_Segment, On_Demand_Usage_Segment,
       Sky_Go_EPL_Usage_Segment, Sky_Go_Any_Usage_Segment, HH_Composition, Region, Affluence_Band, FSS, CQM_Score, Cable_Area)
select
    a.Account_Number,
    a.Period,
    a.Latest_Active_Date,
    c.Scaling_Weight,

    b.Risk_Segment_1,
    b.Risk_Segment_3,
    b.Risk_Segment_2,
    b.Risk_Segment_4,

    a.Value_Segment,
    a.Sports_Segment_SIG,
    a.Survey__EPL_Main_Reason,
    a.Survey__Num_Of_Sports_Claimed,
    a.Base_Package,
    a.Prem_Movies,
    a.Prem_Sports,
    a.TV_Package,
    a.SkyTalk,
    a.Broadband,
    a.HD,
    a.Multiscreen,
    a.Sky_Product,
    a.Number_Of_Sky_Products,
    a.Sports_Tenure_Continuous,
    a.Sports_Downgrade_Event,
    a.Sports_Upgrade_Event,
    a.BT_Sport_Viewier,
    a.Pay_TV_Consumption_Segment,
    a.On_Demand_Usage_Segment,
    a.Sky_Go_EPL_Usage_Segment,
    a.Sky_Go_Any_Usage_Segment,
    a.HH_Composition,
    a.Region,
    a.Affluence_Band,
    a.FSS,
    a.CQM_Score,
    a.Cable_Area
  from EPL_04_Profiling_Variables a,
       EPL_07_Risk_Groups b,
       EPL_05_Scaling_Weights c
 where a.Account_Number = b.Account_Number
   and a.Account_Number = c.Account_Number
   and a.Period = b.Period
   and a.Period = c.Period
   and a.Period = 1
   and a.Prem_Sports > 0;
commit;



  -- ##############################################################################################################
  -- ##############################################################################################################
/*
  -- Add risk groups
update EPL_35_Attribution base
   set base.Risk__All_EPL_Lost__Segment_Grouped_1       =
       base.Risk__All_EPL_Lost__Segment_Grouped_2       =
       base.Risk__Most_EPL_Lost__Segment_Grouped_1      =
       base.Risk__Most_EPL_Lost__Segment_Grouped_2      =
commit;

... to be compelted when defined!
*/


  -- ##############################################################################################################
  -- ##############################################################################################################
  -- Add demographics
alter table EPL_35_Attribution add (
      abc1_adults_in_hh varchar(10) null default 'Unknown',
      abc1_females_in_hh varchar(10) null default 'Unknown',
      abc1_males_in_hh varchar(10) null default 'Unknown',
      acc_gender varchar(10) null default 'Unknown',
      child_0_to_4 varchar(10) null default 'Unknown',
      child_12_to_17 varchar(10) null default 'Unknown',
      child_5_to_11 varchar(10) null default 'Unknown',
      children_in_hh varchar(10) null default 'Unknown',
      cl_owner_renter varchar(15) null default 'Unknown',
      council_tax_banding varchar(75) null default 'Unknown',
      credit_risk varchar(14) null default 'Unknown',
      credit_risk_model_decile varchar(10) null default 'Unknown',
      cust_gender varchar(10) null default 'Unknown',
      cust_marital_status varchar(13) null default 'Unknown',
      financial_outlook varchar(20) null default 'Unknown',
      financial_outlook_sub_segment varchar(21) null default 'Unknown',
      government_region varchar(24) null default 'Unknown',
      h_lifestage varchar(22) null default 'Unknown',
      home_owner_status varchar(29) null default 'Unknown',
      homeowner varchar(10) null default 'Unknown',
      household_oldest_adult_age varchar(10) null default 'Unknown',
      household_youngest_adult_age varchar(10) null default 'Unknown',
      ilu_adults_N varchar(10) null default 'Unknown',
      ilu_affluence varchar(10) null default 'Unknown',
      ilu_age_band varchar(10) null default 'Unknown',
      ilu_children_N varchar(10) null default 'Unknown',
      ilu_gender varchar(10) null default 'Unknown',
      ilu_income varchar(10) null default 'Unknown',
      ilu_len_of_residence varchar(10) null default 'Unknown',
      ilu_occupation varchar(10) null default 'Unknown',
      ilu_occupation_varchar varchar(10) null default 'Unknown',
      ilu_property_bedrooms varchar(10) null default 'Unknown',
      ilu_property_ownership varchar(10) null default 'Unknown',
      income_bands varchar(19) null default 'Unknown',
      isba_tv_region varchar(31) null default 'Unknown',
      kids_age_10to15 varchar(10) null default 'Unknown',
      kids_age_4to9 varchar(10) null default 'Unknown',
      kids_age_le4 varchar(10) null default 'Unknown',
      men_in_hh varchar(10) null default 'Unknown',
      men_in_hh_cd varchar(10) null default 'Unknown',
      mirror_abc1 varchar(10) null default 'Unknown',
      mirror_has_children varchar(10) null default 'Unknown',
      mirror_men varchar(10) null default 'Unknown',
      mirror_women varchar(10) null default 'Unknown',
      mosaic_segments varchar(20) null default 'Unknown',
      om_age_band varchar(10) null default 'Unknown',
      om_financial_stress varchar(10) null default 'Unknown',
      om_len_of_residence varchar(10) null default 'Unknown',
      om_lifestage varchar(100) null default 'Unknown',
      prof_freeview_area varchar(10) null default 'Unknown',
      prof_income varchar(10) null default 'Unknown',
      prof_no_in_household varchar(10) null default 'Unknown',
      prof_no_of_children varchar(10) null default 'Unknown',
      prop_count_of_televisions varchar(10) null default 'Unknown'
);


update EPL_35_Attribution base
   set
       base.abc1_adults_in_hh = case when trim(lower(cast(det.abc1_adults_in_hh as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.abc1_adults_in_hh is null then '{Unknown}' else cast(det.abc1_adults_in_hh as varchar(100)) end,
       base.abc1_females_in_hh = case when trim(lower(cast(det.abc1_females_in_hh as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.abc1_females_in_hh is null then '{Unknown}' else cast(det.abc1_females_in_hh as varchar(100)) end,
       base.abc1_males_in_hh = case when trim(lower(cast(det.abc1_males_in_hh as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.abc1_males_in_hh is null then '{Unknown}' else cast(det.abc1_males_in_hh as varchar(100)) end,
       base.acc_gender = case when trim(lower(cast(det.acc_gender as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.acc_gender is null then '{Unknown}' else cast(det.acc_gender as varchar(100)) end,
       base.child_0_to_4 = case when trim(lower(cast(det.child_0_to_4 as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.child_0_to_4 is null then '{Unknown}' else cast(det.child_0_to_4 as varchar(100)) end,
       base.child_12_to_17 = case when trim(lower(cast(det.child_12_to_17 as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.child_12_to_17 is null then '{Unknown}' else cast(det.child_12_to_17 as varchar(100)) end,
       base.child_5_to_11 = case when trim(lower(cast(det.child_5_to_11 as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.child_5_to_11 is null then '{Unknown}' else cast(det.child_5_to_11 as varchar(100)) end,
       base.children_in_hh = case when trim(lower(cast(det.children_in_hh as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.children_in_hh is null then '{Unknown}' else cast(det.children_in_hh as varchar(100)) end,
       base.cl_owner_renter = case when trim(lower(cast(det.cl_owner_renter as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.cl_owner_renter is null then '{Unknown}' else cast(det.cl_owner_renter as varchar(100)) end,
       base.council_tax_banding = case when trim(lower(cast(det.council_tax_banding as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.council_tax_banding is null then '{Unknown}' else cast(det.council_tax_banding as varchar(100)) end,
       base.credit_risk = case when trim(lower(cast(det.credit_risk as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.credit_risk is null then '{Unknown}' else cast(det.credit_risk as varchar(100)) end,
       base.credit_risk_model_decile = case when trim(lower(cast(det.credit_risk_model_decile as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.credit_risk_model_decile is null then '{Unknown}' else cast(det.credit_risk_model_decile as varchar(100)) end,
       base.cust_gender = case when trim(lower(cast(det.cust_gender as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.cust_gender is null then '{Unknown}' else cast(det.cust_gender as varchar(100)) end,
       base.cust_marital_status = case when trim(lower(cast(det.cust_marital_status as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.cust_marital_status is null then '{Unknown}' else cast(det.cust_marital_status as varchar(100)) end,
       base.financial_outlook = case when trim(lower(cast(det.financial_outlook as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.financial_outlook is null then '{Unknown}' else cast(det.financial_outlook as varchar(100)) end,
       base.financial_outlook_sub_segment = case when trim(lower(cast(det.financial_outlook_sub_segment as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.financial_outlook_sub_segment is null then '{Unknown}' else cast(det.financial_outlook_sub_segment as varchar(100)) end,
       base.government_region = case when trim(lower(cast(det.government_region as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.government_region is null then '{Unknown}' else cast(det.government_region as varchar(100)) end,
       base.h_lifestage = case when trim(lower(cast(det.h_lifestage as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.h_lifestage is null then '{Unknown}' else cast(det.h_lifestage as varchar(100)) end,
       base.home_owner_status = case when trim(lower(cast(det.home_owner_status as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.home_owner_status is null then '{Unknown}' else cast(det.home_owner_status as varchar(100)) end,
       base.homeowner = case when trim(lower(cast(det.homeowner as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.homeowner is null then '{Unknown}' else cast(det.homeowner as varchar(100)) end,
       base.household_oldest_adult_age = case when trim(lower(cast(det.household_oldest_adult_age as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.household_oldest_adult_age is null then '{Unknown}' else cast(det.household_oldest_adult_age as varchar(100)) end,
       base.household_youngest_adult_age = case when trim(lower(cast(det.household_youngest_adult_age as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.household_youngest_adult_age is null then '{Unknown}' else cast(det.household_youngest_adult_age as varchar(100)) end,
       base.ilu_adults_N = case when trim(lower(cast(det.ilu_adults_N as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.ilu_adults_N is null then '{Unknown}' else cast(det.ilu_adults_N as varchar(100)) end,
       base.ilu_affluence = case when trim(lower(cast(det.ilu_affluence as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.ilu_affluence is null then '{Unknown}' else cast(det.ilu_affluence as varchar(100)) end,
       base.ilu_age_band = case when trim(lower(cast(det.ilu_age_band as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.ilu_age_band is null then '{Unknown}' else cast(det.ilu_age_band as varchar(100)) end,
       base.ilu_children_N = case when trim(lower(cast(det.ilu_children_N as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.ilu_children_N is null then '{Unknown}' else cast(det.ilu_children_N as varchar(100)) end,
       base.ilu_gender = case when trim(lower(cast(det.ilu_gender as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.ilu_gender is null then '{Unknown}' else cast(det.ilu_gender as varchar(100)) end,
       base.ilu_income = case when trim(lower(cast(det.ilu_income as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.ilu_income is null then '{Unknown}' else cast(det.ilu_income as varchar(100)) end,
       base.ilu_len_of_residence = case when trim(lower(cast(det.ilu_len_of_residence as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.ilu_len_of_residence is null then '{Unknown}' else cast(det.ilu_len_of_residence as varchar(100)) end,
       base.ilu_occupation = case when trim(lower(cast(det.ilu_occupation as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.ilu_occupation is null then '{Unknown}' else cast(det.ilu_occupation as varchar(100)) end,
       base.ilu_occupation_varchar = case when trim(lower(cast(det.ilu_occupation_varchar as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.ilu_occupation_varchar is null then '{Unknown}' else cast(det.ilu_occupation_varchar as varchar(100)) end,
       base.ilu_property_bedrooms = case when trim(lower(cast(det.ilu_property_bedrooms as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.ilu_property_bedrooms is null then '{Unknown}' else cast(det.ilu_property_bedrooms as varchar(100)) end,
       base.ilu_property_ownership = case when trim(lower(cast(det.ilu_property_ownership as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.ilu_property_ownership is null then '{Unknown}' else cast(det.ilu_property_ownership as varchar(100)) end,
       base.income_bands = case when trim(lower(cast(det.income_bands as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.income_bands is null then '{Unknown}' else cast(det.income_bands as varchar(100)) end,
       base.isba_tv_region = case when trim(lower(cast(det.isba_tv_region as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.isba_tv_region is null then '{Unknown}' else cast(det.isba_tv_region as varchar(100)) end,
       base.kids_age_10to15 = case when trim(lower(cast(det.kids_age_10to15 as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.kids_age_10to15 is null then '{Unknown}' else cast(det.kids_age_10to15 as varchar(100)) end,
       base.kids_age_4to9 = case when trim(lower(cast(det.kids_age_4to9 as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.kids_age_4to9 is null then '{Unknown}' else cast(det.kids_age_4to9 as varchar(100)) end,
       base.kids_age_le4 = case when trim(lower(cast(det.kids_age_le4 as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.kids_age_le4 is null then '{Unknown}' else cast(det.kids_age_le4 as varchar(100)) end,
       base.men_in_hh = case when trim(lower(cast(det.men_in_hh as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.men_in_hh is null then '{Unknown}' else cast(det.men_in_hh as varchar(100)) end,
       base.men_in_hh_cd = case when trim(lower(cast(det.men_in_hh_cd as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.men_in_hh_cd is null then '{Unknown}' else cast(det.men_in_hh_cd as varchar(100)) end,
       base.mirror_abc1 = case when trim(lower(cast(det.mirror_abc1 as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.mirror_abc1 is null then '{Unknown}' else cast(det.mirror_abc1 as varchar(100)) end,
       base.mirror_has_children = case when trim(lower(cast(det.mirror_has_children as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.mirror_has_children is null then '{Unknown}' else cast(det.mirror_has_children as varchar(100)) end,
       base.mirror_men = case when trim(lower(cast(det.mirror_men as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.mirror_men is null then '{Unknown}' else cast(det.mirror_men as varchar(100)) end,
       base.mirror_women = case when trim(lower(cast(det.mirror_women as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.mirror_women is null then '{Unknown}' else cast(det.mirror_women as varchar(100)) end,
       base.mosaic_segments = case when trim(lower(cast(det.mosaic_segments as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.mosaic_segments is null then '{Unknown}' else cast(det.mosaic_segments as varchar(100)) end,
       base.om_age_band = case when trim(lower(cast(det.om_age_band as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.om_age_band is null then '{Unknown}' else cast(det.om_age_band as varchar(100)) end,
       base.om_financial_stress = case when trim(lower(cast(det.om_financial_stress as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.om_financial_stress is null then '{Unknown}' else cast(det.om_financial_stress as varchar(100)) end,
       base.om_len_of_residence = case when trim(lower(cast(det.om_len_of_residence as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.om_len_of_residence is null then '{Unknown}' else cast(det.om_len_of_residence as varchar(100)) end,
       base.om_lifestage = case when trim(lower(cast(det.om_lifestage as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.om_lifestage is null then '{Unknown}' else cast(det.om_lifestage as varchar(100)) end,
       base.prof_freeview_area = case when trim(lower(cast(det.prof_freeview_area as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.prof_freeview_area is null then '{Unknown}' else cast(det.prof_freeview_area as varchar(100)) end,
       base.prof_income = case when trim(lower(cast(det.prof_income as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.prof_income is null then '{Unknown}' else cast(det.prof_income as varchar(100)) end,
       base.prof_no_in_household = case when trim(lower(cast(det.prof_no_in_household as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.prof_no_in_household is null then '{Unknown}' else cast(det.prof_no_in_household as varchar(100)) end,
       base.prof_no_of_children = case when trim(lower(cast(det.prof_no_of_children as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.prof_no_of_children is null then '{Unknown}' else cast(det.prof_no_of_children as varchar(100)) end,
       base.prop_count_of_televisions = case when trim(lower(cast(det.prop_count_of_televisions as varchar(100)))) in ('u', '?', '', 'unknown', '5) unknown', 'unallocated', 'unclassified', 'missing') or det.prop_count_of_televisions is null then '{Unknown}' else cast(det.prop_count_of_televisions as varchar(100)) end
   from sk_prod.cust_single_account_view det
 where base.Account_Number = det.Account_Number;
commit;

/*
select 'abc1_adults_in_hh' as Field, abc1_adults_in_hh as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'abc1_females_in_hh' as Field, abc1_females_in_hh as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'abc1_males_in_hh' as Field, abc1_males_in_hh as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'acc_gender' as Field, acc_gender as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'child_0_to_4' as Field, child_0_to_4 as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'child_12_to_17' as Field, child_12_to_17 as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'child_5_to_11' as Field, child_5_to_11 as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'children_in_hh' as Field, children_in_hh as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'cl_owner_renter' as Field, cl_owner_renter as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'council_tax_banding' as Field, council_tax_banding as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'credit_risk' as Field, credit_risk as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'credit_risk_model_decile' as Field, credit_risk_model_decile as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'cust_gender' as Field, cust_gender as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'cust_marital_status' as Field, cust_marital_status as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'financial_outlook' as Field, financial_outlook as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'financial_outlook_sub_segment' as Field, financial_outlook_sub_segment as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'government_region' as Field, government_region as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'h_lifestage' as Field, h_lifestage as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'home_owner_status' as Field, home_owner_status as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'homeowner' as Field, homeowner as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'household_oldest_adult_age' as Field, household_oldest_adult_age as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'household_youngest_adult_age' as Field, household_youngest_adult_age as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'ilu_adults_N' as Field, ilu_adults_N as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'ilu_affluence' as Field, ilu_affluence as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'ilu_age_band' as Field, ilu_age_band as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'ilu_children_N' as Field, ilu_children_N as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'ilu_gender' as Field, ilu_gender as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'ilu_income' as Field, ilu_income as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'ilu_len_of_residence' as Field, ilu_len_of_residence as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'ilu_occupation' as Field, ilu_occupation as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'ilu_occupation_varchar' as Field, ilu_occupation_varchar as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'ilu_property_bedrooms' as Field, ilu_property_bedrooms as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'ilu_property_ownership' as Field, ilu_property_ownership as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'income_bands' as Field, income_bands as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'isba_tv_region' as Field, isba_tv_region as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'kids_age_10to15' as Field, kids_age_10to15 as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'kids_age_4to9' as Field, kids_age_4to9 as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'kids_age_le4' as Field, kids_age_le4 as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'men_in_hh' as Field, men_in_hh as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'men_in_hh_cd' as Field, men_in_hh_cd as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'mirror_abc1' as Field, mirror_abc1 as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'mirror_has_children' as Field, mirror_has_children as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'mirror_men' as Field, mirror_men as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'mirror_women' as Field, mirror_women as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'mosaic_segments' as Field, mosaic_segments as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'om_age_band' as Field, om_age_band as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'om_financial_stress' as Field, om_financial_stress as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'om_len_of_residence' as Field, om_len_of_residence as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'om_lifestage' as Field, om_lifestage as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'prof_freeview_area' as Field, prof_freeview_area as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'prof_income' as Field, prof_income as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'prof_no_in_household' as Field, prof_no_in_household as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'prof_no_of_children' as Field, prof_no_of_children as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value union all
select 'prop_count_of_televisions' as Field, prop_count_of_televisions as Value, count(*) as Cnt from EPL_35_Attribution group by Field, Value
order by 1, 2;
*/


  -- ##############################################################################################################
  -- ##### Create profiles                                                                                    #####
  -- ##############################################################################################################
if object_id('EPL_36_Attribution_Profiles') is not null then drop table EPL_36_Attribution_Profiles end if;
create table EPL_36_Attribution_Profiles (
    Pk_Identifier                           bigint            identity,
    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name(),

    Scaled_Universe_Flag                    varchar(3)        null      default '???',
    Scenario                                varchar(50)       null      default '???',
    Customer_Segment                        varchar(50)       null      default '???',
    Profile_Variable                        varchar(100)      null      default '???',
    Variable_Category                       varchar(100)      null      default '???',
    Accounts_Volume                         bigint            null      default 0
);
create        lf   index idx01 on EPL_36_Attribution_Profiles(Scaled_Universe_Flag);
create        lf   index idx02 on EPL_36_Attribution_Profiles(Customer_Segment);
create        lf   index idx03 on EPL_36_Attribution_Profiles(Profile_Variable);
create        lf   index idx04 on EPL_36_Attribution_Profiles(Variable_Category);
grant select on EPL_36_Attribution_Profiles to vespa_group_low_security;



if object_id('EPL_30_Attribution_Profiles') is not null then drop procedure EPL_30_Attribution_Profiles end if;
create procedure EPL_30_Attribution_Profiles
      @parVarOrder              varchar(3) = '',
      @parVariable              varchar(100) = ''
as
begin

      declare @varSQL                         varchar(25000)

      execute logger_add_event 0, 0, '##### Processing variable: ' || @parVariable || ' #####', null

        -- ##### Scenario 1 - All EPL lost #####
        -- === Non-scaled ===
      set @varSQL = '
                      insert into EPL_36_Attribution_Profiles
                            (Scaled_Universe_Flag, Scenario, Customer_Segment, Profile_Variable, Variable_Category, Accounts_Volume)
                        select
                              ''No''                                                  as xScaled_Universe_Flag,
                              ''Sky loses EPL in full''                               as xScenario,
                              ''Universe''                                            as xCustomer_Segment,
                              ''' || replace(@parVarOrder || ') ' || @parVariable, '_', ' ') || '''
                                                                                      as xProfile_Variable,
                              trim(cast(' || @parVariable || ' as varchar(100)))      as xVariable_Category,
                              count(*)                                                as xAccounts_Volume
                          from EPL_35_Attribution
                         group by
                                xScaled_Universe_Flag,
                                xScenario,
                                xCustomer_Segment,
                                xProfile_Variable,
                                xVariable_Category
                        union all
                        select
                              ''No''                                                  as xScaled_Universe_Flag,
                              ''Sky loses EPL in full''                               as xScenario,
                              Risk__All_EPL_Lost__Segment                             as xCustomer_Segment,
                              ''' || replace(@parVarOrder || ') ' || @parVariable, '_', ' ') || '''
                                                                                      as xProfile_Variable,
                              trim(cast(' || @parVariable || ' as varchar(100)))      as xVariable_Category,
                              count(*)                                                as xAccounts_Volume
                          from EPL_35_Attribution
                         group by
                                xScaled_Universe_Flag,
                                xScenario,
                                xCustomer_Segment,
                                xProfile_Variable,
                                xVariable_Category

                      commit

                      execute logger_add_event 0, 0, ''Scenario 1 (all rights lost), non-scaled processed'', @@rowcount
                    '
      execute(@varSQL)

        -- === Scaled ===
      set @varSQL = '
                      insert into EPL_36_Attribution_Profiles
                            (Scaled_Universe_Flag, Scenario, Customer_Segment, Profile_Variable, Variable_Category, Accounts_Volume)
                        select
                              ''Yes''                                                 as xScaled_Universe_Flag,
                              ''Sky loses EPL in full''                               as xScenario,
                              ''Universe''                                            as xCustomer_Segment,
                              ''' || replace(@parVarOrder || ') ' || @parVariable, '_', ' ') || '''
                                                                                      as xProfile_Variable,
                              trim(cast(' || @parVariable || ' as varchar(100)))      as xVariable_Category,
                              sum(Scaling_Weight)                                     as xAccounts_Volume
                          from EPL_35_Attribution
                         group by
                                xScaled_Universe_Flag,
                                xScenario,
                                xCustomer_Segment,
                                xProfile_Variable,
                                xVariable_Category
                        union all
                        select
                              ''Yes''                                                 as xScaled_Universe_Flag,
                              ''Sky loses EPL in full''                               as xScenario,
                              Risk__All_EPL_Lost__Segment                             as xCustomer_Segment,
                              ''' || replace(@parVarOrder || ') ' || @parVariable, '_', ' ') || '''
                                                                                      as xProfile_Variable,
                              trim(cast(' || @parVariable || ' as varchar(100)))      as xVariable_Category,
                              sum(Scaling_Weight)                                     as xAccounts_Volume
                          from EPL_35_Attribution
                         group by
                                xScaled_Universe_Flag,
                                xScenario,
                                xCustomer_Segment,
                                xProfile_Variable,
                                xVariable_Category

                      commit

                      execute logger_add_event 0, 0, ''Scenario 1 (all rights lost), scaled processed'', @@rowcount
                    '
      execute(@varSQL)


        -- ##### Scenario 2 - Sky loses majority of EPL #####
        -- === Non-scaled ===
      set @varSQL = '
                      insert into EPL_36_Attribution_Profiles
                            (Scaled_Universe_Flag, Scenario, Customer_Segment, Profile_Variable, Variable_Category, Accounts_Volume)
                        select
                              ''No''                                                  as xScaled_Universe_Flag,
                              ''Sky loses majority of EPL''                           as xScenario,
                              ''Universe''                                            as xCustomer_Segment,
                              ''' || replace(@parVarOrder || ') ' || @parVariable, '_', ' ') || '''
                                                                                      as xProfile_Variable,
                              trim(cast(' || @parVariable || ' as varchar(100)))      as xVariable_Category,
                              count(*)                                                as xAccounts_Volume
                          from EPL_35_Attribution
                         group by
                                xScaled_Universe_Flag,
                                xScenario,
                                xCustomer_Segment,
                                xProfile_Variable,
                                xVariable_Category
                        union all
                        select
                              ''No''                                                  as xScaled_Universe_Flag,
                              ''Sky loses majority of EPL''                           as xScenario,
                              Risk__Most_EPL_Lost__Segment                            as xCustomer_Segment,
                              ''' || replace(@parVarOrder || ') ' || @parVariable, '_', ' ') || '''
                                                                                      as xProfile_Variable,
                              trim(cast(' || @parVariable || ' as varchar(100)))      as xVariable_Category,
                              count(*)                                                as xAccounts_Volume
                          from EPL_35_Attribution
                         group by
                                xScaled_Universe_Flag,
                                xScenario,
                                xCustomer_Segment,
                                xProfile_Variable,
                                xVariable_Category

                      commit

                      execute logger_add_event 0, 0, ''Scenario 2 (majority rights lost), non-scaled processed'', @@rowcount
                    '
      execute(@varSQL)

        -- === Scaled ===
      set @varSQL = '
                      insert into EPL_36_Attribution_Profiles
                            (Scaled_Universe_Flag, Scenario, Customer_Segment, Profile_Variable, Variable_Category, Accounts_Volume)
                        select
                              ''Yes''                                                 as xScaled_Universe_Flag,
                              ''Sky loses majority of EPL''                           as xScenario,
                              ''Universe''                                            as xCustomer_Segment,
                              ''' || replace(@parVarOrder || ') ' || @parVariable, '_', ' ') || '''
                                                                                      as xProfile_Variable,
                              trim(cast(' || @parVariable || ' as varchar(100)))      as xVariable_Category,
                              sum(Scaling_Weight)                                     as xAccounts_Volume
                          from EPL_35_Attribution
                         group by
                                xScaled_Universe_Flag,
                                xScenario,
                                xCustomer_Segment,
                                xProfile_Variable,
                                xVariable_Category
                        union all
                        select
                              ''Yes''                                                 as xScaled_Universe_Flag,
                              ''Sky loses majority of EPL''                           as xScenario,
                              Risk__Most_EPL_Lost__Segment                            as xCustomer_Segment,
                              ''' || replace(@parVarOrder || ') ' || @parVariable, '_', ' ') || '''
                                                                                      as xProfile_Variable,
                              trim(cast(' || @parVariable || ' as varchar(100)))      as xVariable_Category,
                              sum(Scaling_Weight)                                     as xAccounts_Volume
                          from EPL_35_Attribution
                         group by
                                xScaled_Universe_Flag,
                                xScenario,
                                xCustomer_Segment,
                                xProfile_Variable,
                                xVariable_Category

                      commit

                      execute logger_add_event 0, 0, ''Scenario 2 (majority rights lost), scaled processed'', @@rowcount
                    '
      execute(@varSQL)

end;

truncate table EPL_36_Attribution_Profiles;
execute EPL_30_Attribution_Profiles '001', 'Value_Segment';
execute EPL_30_Attribution_Profiles '002', 'Sports_Segment_SIG';
execute EPL_30_Attribution_Profiles '003', 'Survey__EPL_Main_Reason';
execute EPL_30_Attribution_Profiles '004', 'Survey__Num_Of_Sports_Claimed';
execute EPL_30_Attribution_Profiles '005', 'Base_Package';
execute EPL_30_Attribution_Profiles '006', 'Prem_Movies';
execute EPL_30_Attribution_Profiles '007', 'Prem_Sports';
execute EPL_30_Attribution_Profiles '008', 'TV_Package';
execute EPL_30_Attribution_Profiles '009', 'SkyTalk';
execute EPL_30_Attribution_Profiles '010', 'Broadband';
execute EPL_30_Attribution_Profiles '011', 'HD';
execute EPL_30_Attribution_Profiles '012', 'Multiscreen';
execute EPL_30_Attribution_Profiles '013', 'Sky_Product';
execute EPL_30_Attribution_Profiles '014', 'Number_Of_Sky_Products';
execute EPL_30_Attribution_Profiles '015', 'Sports_Tenure_Continuous';
execute EPL_30_Attribution_Profiles '016', 'Sports_Downgrade_Event';
execute EPL_30_Attribution_Profiles '017', 'Sports_Upgrade_Event';
execute EPL_30_Attribution_Profiles '018', 'BT_Sport_Viewier';
execute EPL_30_Attribution_Profiles '019', 'Pay_TV_Consumption_Segment';
execute EPL_30_Attribution_Profiles '020', 'On_Demand_Usage_Segment';
execute EPL_30_Attribution_Profiles '021', 'Sky_Go_EPL_Usage_Segment';
execute EPL_30_Attribution_Profiles '022', 'Sky_Go_Any_Usage_Segment';
execute EPL_30_Attribution_Profiles '023', 'HH_Composition';
execute EPL_30_Attribution_Profiles '024', 'Region';
execute EPL_30_Attribution_Profiles '025', 'Affluence_Band';
execute EPL_30_Attribution_Profiles '026', 'FSS';
execute EPL_30_Attribution_Profiles '027', 'CQM_Score';
execute EPL_30_Attribution_Profiles '028', 'Cable_Area';
execute EPL_30_Attribution_Profiles '029', 'abc1_adults_in_hh';
execute EPL_30_Attribution_Profiles '030', 'abc1_females_in_hh';
execute EPL_30_Attribution_Profiles '031', 'abc1_males_in_hh';
execute EPL_30_Attribution_Profiles '032', 'acc_gender';
execute EPL_30_Attribution_Profiles '033', 'child_0_to_4';
execute EPL_30_Attribution_Profiles '034', 'child_12_to_17';
execute EPL_30_Attribution_Profiles '035', 'child_5_to_11';
execute EPL_30_Attribution_Profiles '036', 'children_in_hh';
execute EPL_30_Attribution_Profiles '037', 'cl_owner_renter';
execute EPL_30_Attribution_Profiles '038', 'council_tax_banding';
execute EPL_30_Attribution_Profiles '039', 'credit_risk';
execute EPL_30_Attribution_Profiles '040', 'credit_risk_model_decile';
execute EPL_30_Attribution_Profiles '041', 'cust_gender';
execute EPL_30_Attribution_Profiles '042', 'cust_marital_status';
execute EPL_30_Attribution_Profiles '043', 'financial_outlook';
execute EPL_30_Attribution_Profiles '044', 'financial_outlook_sub_segment';
execute EPL_30_Attribution_Profiles '045', 'government_region';
execute EPL_30_Attribution_Profiles '046', 'h_lifestage';
execute EPL_30_Attribution_Profiles '047', 'home_owner_status';
execute EPL_30_Attribution_Profiles '048', 'homeowner';
execute EPL_30_Attribution_Profiles '049', 'household_oldest_adult_age';
execute EPL_30_Attribution_Profiles '050', 'household_youngest_adult_age';
execute EPL_30_Attribution_Profiles '051', 'ilu_adults_N';
execute EPL_30_Attribution_Profiles '052', 'ilu_affluence';
execute EPL_30_Attribution_Profiles '053', 'ilu_age_band';
execute EPL_30_Attribution_Profiles '054', 'ilu_children_N';
execute EPL_30_Attribution_Profiles '055', 'ilu_gender';
execute EPL_30_Attribution_Profiles '056', 'ilu_income';
execute EPL_30_Attribution_Profiles '057', 'ilu_len_of_residence';
execute EPL_30_Attribution_Profiles '058', 'ilu_occupation';
execute EPL_30_Attribution_Profiles '059', 'ilu_occupation_varchar';
execute EPL_30_Attribution_Profiles '060', 'ilu_property_bedrooms';
execute EPL_30_Attribution_Profiles '061', 'ilu_property_ownership';
execute EPL_30_Attribution_Profiles '062', 'income_bands';
execute EPL_30_Attribution_Profiles '063', 'isba_tv_region';
execute EPL_30_Attribution_Profiles '064', 'kids_age_10to15';
execute EPL_30_Attribution_Profiles '065', 'kids_age_4to9';
execute EPL_30_Attribution_Profiles '066', 'kids_age_le4';
execute EPL_30_Attribution_Profiles '067', 'men_in_hh';
execute EPL_30_Attribution_Profiles '068', 'men_in_hh_cd';
execute EPL_30_Attribution_Profiles '069', 'mirror_abc1';
execute EPL_30_Attribution_Profiles '070', 'mirror_has_children';
execute EPL_30_Attribution_Profiles '071', 'mirror_men';
execute EPL_30_Attribution_Profiles '072', 'mirror_women';
execute EPL_30_Attribution_Profiles '073', 'mosaic_segments';
execute EPL_30_Attribution_Profiles '074', 'om_age_band';
execute EPL_30_Attribution_Profiles '075', 'om_financial_stress';
execute EPL_30_Attribution_Profiles '076', 'om_len_of_residence';
execute EPL_30_Attribution_Profiles '077', 'om_lifestage';
execute EPL_30_Attribution_Profiles '078', 'prof_freeview_area';
execute EPL_30_Attribution_Profiles '079', 'prof_income';
execute EPL_30_Attribution_Profiles '080', 'prof_no_in_household';
execute EPL_30_Attribution_Profiles '081', 'prof_no_of_children';
execute EPL_30_Attribution_Profiles '082', 'prop_count_of_televisions';



select
      Scaled_Universe_Flag,
      Scenario,
      Customer_Segment,
      Profile_Variable,
      Variable_Category,
      Accounts_Volume
  from EPL_36_Attribution_Profiles
 order by 1, 2, 3, 4, 5;












