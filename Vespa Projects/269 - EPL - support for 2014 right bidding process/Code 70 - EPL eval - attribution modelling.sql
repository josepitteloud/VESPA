/*###############################################################################
# Created on:   23/07/2014
# Created by:   Sebastian Bednaszynski(SBE)
# Description:  EPL rights project - attribution modelling
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 23/07/2014  SBE   Initial version
#
###############################################################################*/


  -- ##############################################################################################################
  -- ##### Create table and pull existing information from the profiling analysis                             #####
  -- ##############################################################################################################
if object_id('EPL_70_Attribution_Base_Table') is not null then drop table EPL_70_Attribution_Base_Table end if;
create table EPL_70_Attribution_Base_Table (
    Pk_Identifier                           bigint            identity,
    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name(),
    Rand_Num                                decimal(15, 10)   null      default null,

      -- Account
    Account_Number                          varchar(20)       null      default null,
    CB_Key_Household                        bigint            null      default null,
    CB_Key_Family                           bigint            null      default null,
    CB_Key_Individual                       bigint            null      default null,

    Latest_Active_Date                      date              null      default null,
    Scaling_Weight                          decimal(15, 6)    null      default 0,
    Prem_Sports                             tinyint           not null  default 0,
    Sample_Type_1                           varchar(30)       null      default '???',
    Sample_Type_2                           varchar(30)       null      default '???',
    Sample_Type_3                           varchar(30)       null      default '???',
    Sample_Type_4                           varchar(30)       null      default '???',

    Target_1                                varchar(30)       null      default '???',
    Target_2                                varchar(30)       null      default '???',
    Target_3                                varchar(30)       null      default '???',
    Target_4                                varchar(30)       null      default '???',

    Risk_Segment__All_EPL_Lost              varchar(50)       null      default '???',
    Risk_Segment__Majority_EPL_Lost         varchar(50)       null      default '???',
    Risk_Segment__All_EPL_Lost_ALT          varchar(50)       null      default '???',
    Risk_Segment__Majority_EPL_Lost_ALT     varchar(50)       null      default '???'

);
create unique hg   index idx01 on EPL_70_Attribution_Base_Table(Account_Number);
create        lf   index idx02 on EPL_70_Attribution_Base_Table(Sample_Type_1);
create        lf   index idx03 on EPL_70_Attribution_Base_Table(Sample_Type_2);
create        lf   index idx04 on EPL_70_Attribution_Base_Table(Sample_Type_3);
create        lf   index idx05 on EPL_70_Attribution_Base_Table(Sample_Type_4);
create        date index idx06 on EPL_70_Attribution_Base_Table(Latest_Active_Date);
grant select on EPL_70_Attribution_Base_Table to vespa_group_low_security;
grant select on EPL_70_Attribution_Base_Table to vespa_crouchr;


insert into EPL_70_Attribution_Base_Table
      (Account_Number, Latest_Active_Date, Scaling_Weight, Prem_Sports, Target_1, Risk_Segment__All_EPL_Lost, Target_2, Risk_Segment__Majority_EPL_Lost)
select
    a.Account_Number,
    a.Latest_Active_Date,
    c.Scaling_Weight,
    a.Prem_Sports,
    b.xRisk_Segment_3,  -- Target_1
    b.xRisk_Segment_3,  -- Risk_Segment__All_EPL_Lost
    b.xRisk_Segment_4,  -- Target_2
    b.xRisk_Segment_4   -- Risk_Segment__Majority_EPL_Lost
  from EPL_04_Profiling_Variables a,
       EPL_07_Risk_Groups_View b,
       EPL_05_Scaling_Weights c
 where a.Account_Number = b.Account_Number
   and a.Account_Number = c.Account_Number
   and a.Period = b.Period
   and a.Period = c.Period
   and a.Period = 1;
commit;

create variable @multiplier bigint;
set @multiplier = datepart(millisecond, now()) + 1;

update EPL_70_Attribution_Base_Table
   set Rand_Num = rand(number(*) * @multiplier);
commit;


  -- ##############################################################################################################
  -- ##### Create samples                                                                                     #####
  -- ##############################################################################################################
update EPL_70_Attribution_Base_Table base
   set base.Sample_Type_1   = case
                                when base.Risk_Segment__All_EPL_Lost in ('No change', 'Churn risk', 'Downgrade risk') and base.Rand_Num < 0.27 then 'Training'
                                when base.Risk_Segment__All_EPL_Lost in ('No change', 'Churn risk', 'Downgrade risk') and base.Rand_Num < 0.50 then 'Validation'
                                when base.Risk_Segment__All_EPL_Lost in ('No change', 'Churn risk', 'Downgrade risk') and base.Rand_Num < 0.80 then 'Test'
                                  else 'Other'
                              end,
       base.Sample_Type_2   = case
                                when base.Risk_Segment__Majority_EPL_Lost in ('No change', 'Churn risk', 'Downgrade risk') and base.Rand_Num < 0.27 then 'Training'
                                when base.Risk_Segment__Majority_EPL_Lost in ('No change', 'Churn risk', 'Downgrade risk') and base.Rand_Num < 0.50 then 'Validation'
                                when base.Risk_Segment__Majority_EPL_Lost in ('No change', 'Churn risk', 'Downgrade risk') and base.Rand_Num < 0.80 then 'Test'
                                  else 'Other'
                              end;
commit;

/*
select Sample_Type_1, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Sample_Type_1;
select
      case when Prem_Sports > 0 then 'Yes' else 'No' end as Sports,
      Risk_Segment__All_EPL_Lost,
      count(*) as Cnt
  from EPL_70_Attribution_Base_Table
 group by Sports, Risk_Segment__All_EPL_Lost
 order by 1, 2;

select Sample_Type_2, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Sample_Type_2;
select
      case when Prem_Sports > 0 then 'Yes' else 'No' end as Sports,
      Risk_Segment__Majority_EPL_Lost,
      count(*) as Cnt
  from EPL_70_Attribution_Base_Table
 group by Sports, Risk_Segment__Majority_EPL_Lost
 order by 1, 2;
*/


  -- ##############################################################################################################
  -- ##############################################################################################################
  -- Add demographics
alter table EPL_70_Attribution_Base_Table add (
      DEM_Abc1_adults_in_hh varchar(32) null default '{Unknown}',
      DEM_Abc1_females_in_hh varchar(32) null default '{Unknown}',
      DEM_Abc1_males_in_hh varchar(32) null default '{Unknown}',
      DEM_Acc_gender varchar(32) null default '{Unknown}',
      DEM_Child_0_to_4 varchar(32) null default '{Unknown}',
      DEM_Child_12_to_17 varchar(32) null default '{Unknown}',
      DEM_Child_5_to_11 varchar(32) null default '{Unknown}',
      DEM_Children_in_hh varchar(32) null default '{Unknown}',
      DEM_Cl_owner_occupier varchar(32) null default '{Unknown}',
      DEM_Cl_owner_renter varchar(32) null default '{Unknown}',
      DEM_Council_tax_banding varchar(150) null default '{Unknown}',
      DEM_Council_tax_banding_cd varchar(32) null default '{Unknown}',
      DEM_Credit_risk varchar(32) null default '{Unknown}',
      DEM_Credit_risk_model_decile varchar(32) null default '{Unknown}',
      DEM_Cust_gender varchar(32) null default '{Unknown}',
      DEM_Cust_marital_status varchar(32) null default '{Unknown}',
      DEM_Financial_outlook varchar(32) null default '{Unknown}',
      DEM_Financial_outlook_sub_segment varchar(32) null default '{Unknown}',
      DEM_Government_region varchar(32) null default '{Unknown}',
      DEM_H_affluence varchar(32) null default '{Unknown}',
      DEM_H_lifestage varchar(32) null default '{Unknown}',
      DEM_Home_owner_status varchar(32) null default '{Unknown}',
      DEM_Homeowner varchar(32) null default '{Unknown}',
      DEM_Household_composition varchar(32) null default '{Unknown}',
      DEM_Household_oldest_adult_age varchar(32) null default '{Unknown}',
      DEM_Household_youngest_adult_age varchar(32) null default '{Unknown}',
      DEM_Ilu_adults_N varchar(32) null default '{Unknown}',
      DEM_Ilu_affluence varchar(32) null default '{Unknown}',
      DEM_Ilu_age_band varchar(32) null default '{Unknown}',
      DEM_Ilu_children_N varchar(32) null default '{Unknown}',
      DEM_Ilu_FC varchar(32) null default '{Unknown}',
      DEM_Ilu_gender varchar(32) null default '{Unknown}',
      DEM_Ilu_income varchar(32) null default '{Unknown}',
      DEM_Ilu_len_of_residence varchar(32) null default '{Unknown}',
      DEM_Ilu_mailable_FC varchar(32) null default '{Unknown}',
      DEM_Ilu_occupation varchar(32) null default '{Unknown}',
      DEM_Ilu_occupation_varchar varchar(32) null default '{Unknown}',
      DEM_Ilu_property_bedrooms varchar(32) null default '{Unknown}',
      DEM_Ilu_property_ownership varchar(32) null default '{Unknown}',
      DEM_Income_bands varchar(32) null default '{Unknown}',
      DEM_Isba_tv_region varchar(32) null default '{Unknown}',
      DEM_Kids_age_10to15 varchar(32) null default '{Unknown}',
      DEM_Kids_age_4to9 varchar(32) null default '{Unknown}',
      DEM_Kids_age_le4 varchar(32) null default '{Unknown}',
      DEM_Men_in_hh varchar(32) null default '{Unknown}',
      DEM_Men_in_hh_cd varchar(32) null default '{Unknown}',
      DEM_Mirror_abc1 varchar(32) null default '{Unknown}',
      DEM_Mirror_has_children varchar(32) null default '{Unknown}',
      DEM_Mirror_men varchar(32) null default '{Unknown}',
      DEM_Mirror_women varchar(32) null default '{Unknown}',
      DEM_Mosaic_segments varchar(32) null default '{Unknown}',
      DEM_Om_age_band varchar(32) null default '{Unknown}',
      DEM_Om_FC varchar(32) null default '{Unknown}',
      DEM_Om_financial_stress varchar(32) null default '{Unknown}',
      DEM_Om_len_of_residence varchar(32) null default '{Unknown}',
      DEM_Om_lifestage varchar(32) null default '{Unknown}',
      DEM_Osm_segment varchar(32) null default '{Unknown}',
      DEM_Osm_segment_hh varchar(32) null default '{Unknown}',
      DEM_Osm_segment_ind varchar(32) null default '{Unknown}',
      DEM_Person_type varchar(32) null default '{Unknown}',
      DEM_Prof_freeview_area varchar(32) null default '{Unknown}',
      DEM_Prof_income varchar(32) null default '{Unknown}',
      DEM_Prof_no_in_household varchar(32) null default '{Unknown}',
      DEM_Prof_no_of_children varchar(32) null default '{Unknown}',
      DEM_Prop_count_of_televisions varchar(32) null default '{Unknown}'
);


update EPL_70_Attribution_Base_Table base
   set
       base.CB_Key_Household = det.CB_Key_Household,
       base.CB_Key_Family = det.CB_Key_Family,
       base.CB_Key_Individual = det.CB_Key_Individual,

       base.DEM_Abc1_adults_in_hh = case when trim(lower(cast(det.abc1_adults_in_hh as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.abc1_adults_in_hh is null then '{Unknown}' else cast(det.abc1_adults_in_hh as varchar(100)) end,
       base.DEM_Abc1_females_in_hh = case when trim(lower(cast(det.abc1_females_in_hh as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.abc1_females_in_hh is null then '{Unknown}' else cast(det.abc1_females_in_hh as varchar(100)) end,
       base.DEM_Abc1_males_in_hh = case when trim(lower(cast(det.abc1_males_in_hh as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.abc1_males_in_hh is null then '{Unknown}' else cast(det.abc1_males_in_hh as varchar(100)) end,
       base.DEM_Acc_gender = case when trim(lower(cast(det.acc_gender as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.acc_gender is null then '{Unknown}' else cast(det.acc_gender as varchar(100)) end,
       base.DEM_Child_0_to_4 = case when trim(lower(cast(det.child_0_to_4 as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.child_0_to_4 is null then '{Unknown}' else cast(det.child_0_to_4 as varchar(100)) end,
       base.DEM_Child_12_to_17 = case when trim(lower(cast(det.child_12_to_17 as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.child_12_to_17 is null then '{Unknown}' else cast(det.child_12_to_17 as varchar(100)) end,
       base.DEM_Child_5_to_11 = case when trim(lower(cast(det.child_5_to_11 as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.child_5_to_11 is null then '{Unknown}' else cast(det.child_5_to_11 as varchar(100)) end,
       base.DEM_Children_in_hh = case when trim(lower(cast(det.children_in_hh as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.children_in_hh is null then '{Unknown}' else cast(det.children_in_hh as varchar(100)) end,
       base.DEM_Cl_owner_occupier = case when trim(lower(cast(det.cl_owner_occupier as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.cl_owner_occupier is null then '{Unknown}' else cast(det.cl_owner_occupier as varchar(100)) end,
       base.DEM_Cl_owner_renter = case when trim(lower(cast(det.cl_owner_renter as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.cl_owner_renter is null then '{Unknown}' else cast(det.cl_owner_renter as varchar(100)) end,
       base.DEM_Council_tax_banding = case when trim(lower(cast(det.council_tax_banding as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.council_tax_banding is null then '{Unknown}' else cast(det.council_tax_banding as varchar(100)) end,
       base.DEM_Council_tax_banding_cd = case when trim(lower(cast(det.council_tax_banding_cd as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.council_tax_banding_cd is null then '{Unknown}' else cast(det.council_tax_banding_cd as varchar(100)) end,
       base.DEM_Credit_risk = case when trim(lower(cast(det.credit_risk as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.credit_risk is null then '{Unknown}' else cast(det.credit_risk as varchar(100)) end,
       base.DEM_Credit_risk_model_decile = case when trim(lower(cast(det.credit_risk_model_decile as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.credit_risk_model_decile is null then '{Unknown}' else cast(det.credit_risk_model_decile as varchar(100)) end,
       base.DEM_Cust_gender = case when trim(lower(cast(det.cust_gender as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.cust_gender is null then '{Unknown}' else cast(det.cust_gender as varchar(100)) end,
       base.DEM_Cust_marital_status = case when trim(lower(cast(det.cust_marital_status as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.cust_marital_status is null then '{Unknown}' else cast(det.cust_marital_status as varchar(100)) end,
       base.DEM_Financial_outlook = case when trim(lower(cast(det.financial_outlook as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.financial_outlook is null then '{Unknown}' else cast(det.financial_outlook as varchar(100)) end,
       base.DEM_Financial_outlook_sub_segment = case when trim(lower(cast(det.financial_outlook_sub_segment as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.financial_outlook_sub_segment is null then '{Unknown}' else cast(det.financial_outlook_sub_segment as varchar(100)) end,
       base.DEM_Government_region = case when trim(lower(cast(det.government_region as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.government_region is null then '{Unknown}' else cast(det.government_region as varchar(100)) end,
       base.DEM_H_affluence = case when trim(lower(cast(det.h_affluence as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.h_affluence is null then '{Unknown}' else cast(det.h_affluence as varchar(100)) end,
       base.DEM_H_lifestage = case when trim(lower(cast(det.h_lifestage as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.h_lifestage is null then '{Unknown}' else cast(det.h_lifestage as varchar(100)) end,
       base.DEM_Home_owner_status = case when trim(lower(cast(det.home_owner_status as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.home_owner_status is null then '{Unknown}' else cast(det.home_owner_status as varchar(100)) end,
       base.DEM_Homeowner = case when trim(lower(cast(det.homeowner as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.homeowner is null then '{Unknown}' else cast(det.homeowner as varchar(100)) end,
       base.DEM_Household_composition = case when trim(lower(cast(det.household_composition as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.household_composition is null then '{Unknown}' else cast(det.household_composition as varchar(100)) end,
       base.DEM_Household_oldest_adult_age = case when trim(lower(cast(det.household_oldest_adult_age as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.household_oldest_adult_age is null then '{Unknown}' else cast(det.household_oldest_adult_age as varchar(100)) end,
       base.DEM_Household_youngest_adult_age = case when trim(lower(cast(det.household_youngest_adult_age as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.household_youngest_adult_age is null then '{Unknown}' else cast(det.household_youngest_adult_age as varchar(100)) end,
       base.DEM_Ilu_adults_N = case when trim(lower(cast(det.ilu_adults_N as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.ilu_adults_N is null then '{Unknown}' else cast(det.ilu_adults_N as varchar(100)) end,
       base.DEM_Ilu_affluence = case when trim(lower(cast(det.ilu_affluence as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.ilu_affluence is null then '{Unknown}' else cast(det.ilu_affluence as varchar(100)) end,
       base.DEM_Ilu_age_band = case when trim(lower(cast(det.ilu_age_band as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.ilu_age_band is null then '{Unknown}' else cast(det.ilu_age_band as varchar(100)) end,
       base.DEM_Ilu_children_N = case when trim(lower(cast(det.ilu_children_N as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.ilu_children_N is null then '{Unknown}' else cast(det.ilu_children_N as varchar(100)) end,
       base.DEM_Ilu_FC = case when trim(lower(cast(det.ilu_FC as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.ilu_FC is null then '{Unknown}' else cast(det.ilu_FC as varchar(100)) end,
       base.DEM_Ilu_gender = case when trim(lower(cast(det.ilu_gender as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.ilu_gender is null then '{Unknown}' else cast(det.ilu_gender as varchar(100)) end,
       base.DEM_Ilu_income = case when trim(lower(cast(det.ilu_income as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.ilu_income is null then '{Unknown}' else cast(det.ilu_income as varchar(100)) end,
       base.DEM_Ilu_len_of_residence = case when trim(lower(cast(det.ilu_len_of_residence as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.ilu_len_of_residence is null then '{Unknown}' else cast(det.ilu_len_of_residence as varchar(100)) end,
       base.DEM_Ilu_mailable_FC = case when trim(lower(cast(det.ilu_mailable_FC as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.ilu_mailable_FC is null then '{Unknown}' else cast(det.ilu_mailable_FC as varchar(100)) end,
       base.DEM_Ilu_occupation = case when trim(lower(cast(det.ilu_occupation as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.ilu_occupation is null then '{Unknown}' else cast(det.ilu_occupation as varchar(100)) end,
       base.DEM_Ilu_occupation_varchar = case when trim(lower(cast(det.ilu_occupation_varchar as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.ilu_occupation_varchar is null then '{Unknown}' else cast(det.ilu_occupation_varchar as varchar(100)) end,
       base.DEM_Ilu_property_bedrooms = case when trim(lower(cast(det.ilu_property_bedrooms as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.ilu_property_bedrooms is null then '{Unknown}' else cast(det.ilu_property_bedrooms as varchar(100)) end,
       base.DEM_Ilu_property_ownership = case when trim(lower(cast(det.ilu_property_ownership as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.ilu_property_ownership is null then '{Unknown}' else cast(det.ilu_property_ownership as varchar(100)) end,
       base.DEM_Income_bands = case when trim(lower(cast(det.income_bands as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.income_bands is null then '{Unknown}' else cast(det.income_bands as varchar(100)) end,
       base.DEM_Isba_tv_region = case when trim(lower(cast(det.isba_tv_region as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.isba_tv_region is null then '{Unknown}' else cast(det.isba_tv_region as varchar(100)) end,
       base.DEM_Kids_age_10to15 = case when trim(lower(cast(det.kids_age_10to15 as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.kids_age_10to15 is null then '{Unknown}' else cast(det.kids_age_10to15 as varchar(100)) end,
       base.DEM_Kids_age_4to9 = case when trim(lower(cast(det.kids_age_4to9 as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.kids_age_4to9 is null then '{Unknown}' else cast(det.kids_age_4to9 as varchar(100)) end,
       base.DEM_Kids_age_le4 = case when trim(lower(cast(det.kids_age_le4 as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.kids_age_le4 is null then '{Unknown}' else cast(det.kids_age_le4 as varchar(100)) end,
       base.DEM_Men_in_hh = case when trim(lower(cast(det.men_in_hh as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.men_in_hh is null then '{Unknown}' else cast(det.men_in_hh as varchar(100)) end,
       base.DEM_Men_in_hh_cd = case when trim(lower(cast(det.men_in_hh_cd as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.men_in_hh_cd is null then '{Unknown}' else cast(det.men_in_hh_cd as varchar(100)) end,
       base.DEM_Mirror_abc1 = case when trim(lower(cast(det.mirror_abc1 as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.mirror_abc1 is null then '{Unknown}' else cast(det.mirror_abc1 as varchar(100)) end,
       base.DEM_Mirror_has_children = case when trim(lower(cast(det.mirror_has_children as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.mirror_has_children is null then '{Unknown}' else cast(det.mirror_has_children as varchar(100)) end,
       base.DEM_Mirror_men = case when trim(lower(cast(det.mirror_men as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.mirror_men is null then '{Unknown}' else cast(det.mirror_men as varchar(100)) end,
       base.DEM_Mirror_women = case when trim(lower(cast(det.mirror_women as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.mirror_women is null then '{Unknown}' else cast(det.mirror_women as varchar(100)) end,
       base.DEM_Mosaic_segments = case when trim(lower(cast(det.mosaic_segments as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.mosaic_segments is null then '{Unknown}' else cast(det.mosaic_segments as varchar(100)) end,
       base.DEM_Om_age_band = case when trim(lower(cast(det.om_age_band as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.om_age_band is null then '{Unknown}' else cast(det.om_age_band as varchar(100)) end,
       base.DEM_Om_FC = case when trim(lower(cast(det.om_FC as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.om_FC is null then '{Unknown}' else cast(det.om_FC as varchar(100)) end,
       base.DEM_Om_financial_stress = case when trim(lower(cast(det.om_financial_stress as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.om_financial_stress is null then '{Unknown}' else cast(det.om_financial_stress as varchar(100)) end,
       base.DEM_Om_len_of_residence = case when trim(lower(cast(det.om_len_of_residence as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.om_len_of_residence is null then '{Unknown}' else cast(det.om_len_of_residence as varchar(100)) end,
       base.DEM_Om_lifestage = case when trim(lower(cast(det.om_lifestage as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.om_lifestage is null then '{Unknown}' else cast(det.om_lifestage as varchar(100)) end,
       base.DEM_Osm_segment = case when trim(lower(cast(det.osm_segment as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.osm_segment is null then '{Unknown}' else cast(det.osm_segment as varchar(100)) end,
       base.DEM_Osm_segment_hh = case when trim(lower(cast(det.osm_segment_hh as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.osm_segment_hh is null then '{Unknown}' else cast(det.osm_segment_hh as varchar(100)) end,
       base.DEM_Osm_segment_ind = case when trim(lower(cast(det.osm_segment_ind as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.osm_segment_ind is null then '{Unknown}' else cast(det.osm_segment_ind as varchar(100)) end,
       base.DEM_Person_type = case when trim(lower(cast(det.person_type as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.person_type is null then '{Unknown}' else cast(det.person_type as varchar(100)) end,
       base.DEM_Prof_freeview_area = case when trim(lower(cast(det.prof_freeview_area as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.prof_freeview_area is null then '{Unknown}' else cast(det.prof_freeview_area as varchar(100)) end,
       base.DEM_Prof_income = case when trim(lower(cast(det.prof_income as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.prof_income is null then '{Unknown}' else cast(det.prof_income as varchar(100)) end,
       base.DEM_Prof_no_in_household = case when trim(lower(cast(det.prof_no_in_household as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.prof_no_in_household is null then '{Unknown}' else cast(det.prof_no_in_household as varchar(100)) end,
       base.DEM_Prof_no_of_children = case when trim(lower(cast(det.prof_no_of_children as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.prof_no_of_children is null then '{Unknown}' else cast(det.prof_no_of_children as varchar(100)) end,
       base.DEM_Prop_count_of_televisions = case when trim(lower(cast(det.prop_count_of_televisions as varchar(100)))) in ('?','5) unknown','not defined','u','unallocated','unclassified','unknown','','missing') or det.prop_count_of_televisions is null then '{Unknown}' else cast(det.prop_count_of_televisions as varchar(100)) end

   from sk_prod.cust_single_account_view det
 where base.Account_Number = det.Account_Number;
commit;

/*
select 'abc1_adults_in_hh' as Field, DEM_abc1_adults_in_hh as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'abc1_females_in_hh' as Field, DEM_abc1_females_in_hh as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'abc1_males_in_hh' as Field, DEM_abc1_males_in_hh as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'acc_gender' as Field, DEM_acc_gender as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'child_0_to_4' as Field, DEM_child_0_to_4 as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'child_12_to_17' as Field, DEM_child_12_to_17 as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'child_5_to_11' as Field, DEM_child_5_to_11 as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'children_in_hh' as Field, DEM_children_in_hh as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'cl_owner_occupier' as Field, DEM_cl_owner_occupier as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'cl_owner_renter' as Field, DEM_cl_owner_renter as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'council_tax_banding' as Field, DEM_council_tax_banding as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'council_tax_banding_cd' as Field, DEM_council_tax_banding_cd as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'credit_risk' as Field, DEM_credit_risk as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'credit_risk_model_decile' as Field, DEM_credit_risk_model_decile as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'cust_gender' as Field, DEM_cust_gender as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'cust_marital_status' as Field, DEM_cust_marital_status as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'financial_outlook' as Field, DEM_financial_outlook as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'financial_outlook_sub_segment' as Field, DEM_financial_outlook_sub_segment as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'government_region' as Field, DEM_government_region as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_affluence' as Field, DEM_h_affluence as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_lifestage' as Field, DEM_h_lifestage as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'home_owner_status' as Field, DEM_home_owner_status as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'homeowner' as Field, DEM_homeowner as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'household_composition' as Field, DEM_household_composition as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'household_oldest_adult_age' as Field, DEM_household_oldest_adult_age as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'household_youngest_adult_age' as Field, DEM_household_youngest_adult_age as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'ilu_adults_N' as Field, DEM_ilu_adults_N as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'ilu_affluence' as Field, DEM_ilu_affluence as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'ilu_age_band' as Field, DEM_ilu_age_band as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'ilu_children_N' as Field, DEM_ilu_children_N as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'ilu_FC' as Field, DEM_ilu_FC as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'ilu_gender' as Field, DEM_ilu_gender as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'ilu_income' as Field, DEM_ilu_income as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'ilu_len_of_residence' as Field, DEM_ilu_len_of_residence as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'ilu_mailable_FC' as Field, DEM_ilu_mailable_FC as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'ilu_occupation' as Field, DEM_ilu_occupation as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'ilu_occupation_varchar' as Field, DEM_ilu_occupation_varchar as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'ilu_property_bedrooms' as Field, DEM_ilu_property_bedrooms as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'ilu_property_ownership' as Field, DEM_ilu_property_ownership as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'income_bands' as Field, DEM_income_bands as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'isba_tv_region' as Field, DEM_isba_tv_region as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'kids_age_10to15' as Field, DEM_kids_age_10to15 as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'kids_age_4to9' as Field, DEM_kids_age_4to9 as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'kids_age_le4' as Field, DEM_kids_age_le4 as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'men_in_hh' as Field, DEM_men_in_hh as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'men_in_hh_cd' as Field, DEM_men_in_hh_cd as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'mirror_abc1' as Field, DEM_mirror_abc1 as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'mirror_has_children' as Field, DEM_mirror_has_children as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'mirror_men' as Field, DEM_mirror_men as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'mirror_women' as Field, DEM_mirror_women as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'mosaic_segments' as Field, DEM_mosaic_segments as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'om_age_band' as Field, DEM_om_age_band as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'om_FC' as Field, DEM_om_FC as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'om_financial_stress' as Field, DEM_om_financial_stress as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'om_len_of_residence' as Field, DEM_om_len_of_residence as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'om_lifestage' as Field, DEM_om_lifestage as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'osm_segment' as Field, DEM_osm_segment as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'osm_segment_hh' as Field, DEM_osm_segment_hh as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'osm_segment_ind' as Field, DEM_osm_segment_ind as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'person_type' as Field, DEM_person_type as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'prof_freeview_area' as Field, DEM_prof_freeview_area as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'prof_income' as Field, DEM_prof_income as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'prof_no_in_household' as Field, DEM_prof_no_in_household as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'prof_no_of_children' as Field, DEM_prof_no_of_children as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'prop_count_of_televisions' as Field, DEM_prop_count_of_televisions as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value
order by 1, 2;
*/


  -- ##############################################################################################################
  -- ##### Package & Product                                                                                  #####
  -- ##############################################################################################################
alter table EPL_70_Attribution_Base_Table add (
      PRD_Value_Segment_NOW                       varchar(25)       null      default 'Z) Unknown',
      PRD_Base_Package_NOW                        varchar(25)       null      default 'Unknown',
      PRD_Prem_Movies_NOW                         smallint          null      default 0,
      PRD_Prem_Sports_NOW                         smallint          null      default 0,
      PRD_TV_Package_NOW                          varchar(100)      null      default 'Unknown',
      PRD_SkyTalk_NOW                             varchar(25)       null      default 'No SkyTalk',
      PRD_Broadband_NOW                           varchar(25)       null      default 'No Broadband',
      PRD_HD_NOW                                  varchar(25)       null      default 'No HD',
      PRD_Multiscreen_NOW                         varchar(25)       null      default 'No Multiscreen',
      PRD_Sky_Product_NOW                         varchar(100)      null      default 'Unknown',

      PRD_Value_Segment_PREV                      varchar(25)       null      default 'Z) Unknown',
      PRD_Base_Package_PREV                       varchar(25)       null      default 'Unknown',
      PRD_Prem_Movies_PREV                        smallint          null      default 0,
      PRD_Prem_Sports_PREV                        smallint          null      default 0,
      PRD_TV_Package_PREV                         varchar(100)      null      default 'Unknown',
      PRD_SkyTalk_PREV                            varchar(25)       null      default 'No SkyTalk',
      PRD_Broadband_PREV                          varchar(25)       null      default 'No Broadband',
      PRD_HD_PREV                                 varchar(25)       null      default 'No HD',
      PRD_Multiscreen_PREV                        varchar(25)       null      default 'No Multiscreen',
      PRD_Sky_Product_PREV                        varchar(100)      null      default 'Unknown',

      PRD_Value_Segment_MVMNT                     varchar(25)       null      default 'Unknown',
      PRD_Base_Package_MVMNT                      varchar(25)       null      default 'Unknown',
      PRD_Prem_Movies_MVMNT                       varchar(25)       null      default 'Unknown',
      PRD_Prem_Sports_MVMNT                       varchar(25)       null      default 'Unknown',
      PRD_SkyTalk_MVMNT                           varchar(25)       null      default 'Unknown',
      PRD_Broadband_MVMNT                         varchar(25)       null      default 'Unknown',
      PRD_HD_MVMNT                                varchar(25)       null      default 'Unknown',
      PRD_Multiscreen_MVMNT                       varchar(25)       null      default 'Unknown',

      PRD_Number_Of_Sky_Products_NOW              tinyint           null      default 0,
      PRD_Number_Of_Sky_Products_PREV             tinyint           null      default 0,
      PRD_Number_Of_Sky_Products_MVMNT            varchar(30)       null      default 'Unknown',

      PRD_On_Demand_Streams_Num                   bigint            null      default 0,
      PRD_On_Demand_Programmes_Num                bigint            null      default 0,
      PRD_On_Demand_Usage_Segment                 varchar(25)       null      default '1) Non-OnDemand user',

      PRD_Sky_Go_Any_Usage_Segment                varchar(25)       null      default '1) Non-Sky Go user'
);



  -- ### Value segment ###
update EPL_70_Attribution_Base_Table base
   set base.PRD_Value_Segment_NOW
                              = case
                                  when det.Value_Segment like '%Platinum%' then 'A) Platinum'
                                  when det.Value_Segment like '%Gold%' then 'B) Gold'
                                  when det.Value_Segment like '%Silver%' then 'C) Silver'
                                  when det.Value_Segment like '%Bronze%' then 'D) Bronze'
                                  when det.Value_Segment like '%Copper%' then 'E) Copper'
                                  when det.Value_Segment like '%Unstable%' then 'F) Unstable'
                                  when det.Value_Segment like '%Bedding In%' then 'G) Bedding In'
                                    else 'Z) Unknown'
                                end
  from (select
              Account_Number,
              max(Value_Segment) as Value_Segment
          from sk_prod.value_segments_five_yrs
         where Value_Seg_Date = '2014-03-03'
         group by Account_Number) det
 where base.Account_Number = det.Account_Number;
commit;


update EPL_70_Attribution_Base_Table base
   set base.PRD_Value_Segment_PREV
                              = case
                                  when det.Value_Segment like '%Platinum%' then 'A) Platinum'
                                  when det.Value_Segment like '%Gold%' then 'B) Gold'
                                  when det.Value_Segment like '%Silver%' then 'C) Silver'
                                  when det.Value_Segment like '%Bronze%' then 'D) Bronze'
                                  when det.Value_Segment like '%Copper%' then 'E) Copper'
                                  when det.Value_Segment like '%Unstable%' then 'F) Unstable'
                                  when det.Value_Segment like '%Bedding In%' then 'G) Bedding In'
                                    else 'Z) Unknown'
                                end
  from (select
              Account_Number,
              max(Value_Segment) as Value_Segment
          from sk_prod.value_segments_five_yrs
         where Value_Seg_Date = '2013-09-02'
         group by Account_Number) det
 where base.Account_Number = det.Account_Number;
commit;


  -- ### Packages & Products ###
update EPL_70_Attribution_Base_Table base
   set base.PRD_Base_Package_NOW
                            = case
                                when hist.Base_Package  = 1             then '1) Ent'        -- Entertainment
                                when hist.Base_Package  = 2             then '2) Ent Extra'  -- Entertainment Extra
                                when hist.Base_Package  = 3             then '3) Ent Extra+' -- Entertainment Extra Plus
                                  else 'Unknown'
                              end,
       base.PRD_Prem_Movies_NOW
                             = hist.Prem_Movies,
       base.PRD_Prem_Sports_NOW
                             = hist.Prem_Sports,
       base.PRD_SkyTalk_NOW
                            = case
                                when hist.SkyTalk = 1 then 'SkyTalk'
                                  else 'No SkyTalk'
                              end,
       base.PRD_Broadband_NOW
                            = case
                                when hist.Broadband = 1 then 'Broadband'
                                  else 'No Broadband'
                              end,
       base.PRD_HD_NOW
                            = case
                                when hist.xHD = 1 then 'HD'
                                  else 'No HD'
                              end,
       base.PRD_Multiscreen_NOW
                            = case
                                when hist.Multiscreen = 1 then 'Multiscreen'
                                  else 'No Multiscreen'
                              end
  from (select
              a.Account_Number,
              max(case
                    when det.subscription_sub_type <> 'DTV Primary Viewing' then 0
                    when cel.mixes = 0                                                                then 1        -- Entertainment
                    when cel.mixes = 1 and (cel.style_culture = 1 or cel.variety = 1)                 then 1        -- Entertainment
                    when cel.mixes = 2 and (cel.style_culture + cel.variety = 2)                      then 1        -- Entertainment
                    when cel.product_sk in      ( 43672,43669,43670,43664,43667,43663,43668,43673,
                                                  43677,43674,43676,43666,43665,43662,43671,43675)    then 3        -- Entertainment Extra Plus
                    when cel.mixes > 0                                                                then 2        -- Entertainment Extra
                      else 0
                  end) as Base_Package,
              max(case
                    when det.subscription_sub_type = 'DTV Primary Viewing' then cel.prem_movies
                      else 0
                  end) as Prem_Movies,
              max(case
                    when det.subscription_sub_type = 'DTV Primary Viewing' then cel.Prem_Sports
                      else 0
                  end) as Prem_Sports,

              max(case
                    when (det.subscription_sub_type = 'SKY TALK SELECT') and
                         (
                            det.status_code = 'A' or
                            (det.status_code = 'FBP' and prev_status_code in ('PC','A')) or
                            (det.status_code = 'RI'  and prev_status_code in ('FBP','A')) or
                            (det.status_code = 'PC'  and prev_status_code = 'A')
                         ) then 1
                      else 0
                  end) as SkyTalk,

              max(case
                    when det.subscription_sub_type = 'Broadband DSL Line' and
                                     (       det.status_code in ('AC','AB')
                                         or (det.status_code='PC' AND det.prev_status_code not in ('?','RQ','AP','UB','BE','PA') )
                                         or (det.status_code='CF' AND det.prev_status_code='PC'                                  )
                                         or (det.status_code='AP' AND det.sale_type='SNS Bulk Migration'                         )
                                      ) then 1
                      else 0
                  end) as Broadband,
              max(case
                    when det.subscription_sub_type = 'DTV HD' and det.status_code in ('AC','AB','PC') then 1
                      else 0
                  end) as xHD,
              max(case
                    when det.subscription_sub_type = 'DTV Extra Subscription' and det.status_code in ('AC','AB','PC') then 1
                      else 0
                  end) as Multiscreen

          from EPL_70_Attribution_Base_Table a,
               sk_prod.cust_subs_hist det
                    left join sk_prod.cust_entitlement_lookup as cel  on det.current_short_description = cel.short_description
         where a.Account_Number = det.Account_Number
           and det.Effective_From_Dt <= a.Latest_Active_Date
           and det.Effective_To_Dt > a.Latest_Active_Date
           and det.Effective_From_Dt < det.Effective_To_Dt
         group by a.Account_Number) hist
 where base.Account_Number = hist.Account_Number;
commit;


  -- ### Packages & Products ###
update EPL_70_Attribution_Base_Table base
   set base.PRD_Base_Package_PREV
                            = case
                                when hist.Base_Package  = 1             then '1) Ent'        -- Entertainment
                                when hist.Base_Package  = 2             then '2) Ent Extra'  -- Entertainment Extra
                                when hist.Base_Package  = 3             then '3) Ent Extra+' -- Entertainment Extra Plus
                                  else 'Unknown'
                              end,
       base.PRD_Prem_Movies_PREV
                             = hist.Prem_Movies,
       base.PRD_Prem_Sports_PREV
                             = hist.Prem_Sports,
       base.PRD_SkyTalk_PREV
                            = case
                                when hist.SkyTalk = 1 then 'SkyTalk'
                                  else 'No SkyTalk'
                              end,
       base.PRD_Broadband_PREV
                            = case
                                when hist.Broadband = 1 then 'Broadband'
                                  else 'No Broadband'
                              end,
       base.PRD_HD_PREV
                            = case
                                when hist.xHD = 1 then 'HD'
                                  else 'No HD'
                              end,
       base.PRD_Multiscreen_PREV
                            = case
                                when hist.Multiscreen = 1 then 'Multiscreen'
                                  else 'No Multiscreen'
                              end
  from (select
              a.Account_Number,
              max(case
                    when det.subscription_sub_type <> 'DTV Primary Viewing' then 0
                    when cel.mixes = 0                                                                then 1        -- Entertainment
                    when cel.mixes = 1 and (cel.style_culture = 1 or cel.variety = 1)                 then 1        -- Entertainment
                    when cel.mixes = 2 and (cel.style_culture + cel.variety = 2)                      then 1        -- Entertainment
                    when cel.product_sk in      ( 43672,43669,43670,43664,43667,43663,43668,43673,
                                                  43677,43674,43676,43666,43665,43662,43671,43675)    then 3        -- Entertainment Extra Plus
                    when cel.mixes > 0                                                                then 2        -- Entertainment Extra
                      else 0
                  end) as Base_Package,
              max(case
                    when det.subscription_sub_type = 'DTV Primary Viewing' then cel.prem_movies
                      else 0
                  end) as Prem_Movies,
              max(case
                    when det.subscription_sub_type = 'DTV Primary Viewing' then cel.Prem_Sports
                      else 0
                  end) as Prem_Sports,

              max(case
                    when (det.subscription_sub_type = 'SKY TALK SELECT') and
                         (
                            det.status_code = 'A' or
                            (det.status_code = 'FBP' and prev_status_code in ('PC','A')) or
                            (det.status_code = 'RI'  and prev_status_code in ('FBP','A')) or
                            (det.status_code = 'PC'  and prev_status_code = 'A')
                         ) then 1
                      else 0
                  end) as SkyTalk,

              max(case
                    when det.subscription_sub_type = 'Broadband DSL Line' and
                                     (       det.status_code in ('AC','AB')
                                         or (det.status_code='PC' AND det.prev_status_code not in ('?','RQ','AP','UB','BE','PA') )
                                         or (det.status_code='CF' AND det.prev_status_code='PC'                                  )
                                         or (det.status_code='AP' AND det.sale_type='SNS Bulk Migration'                         )
                                      ) then 1
                      else 0
                  end) as Broadband,
              max(case
                    when det.subscription_sub_type = 'DTV HD' and det.status_code in ('AC','AB','PC') then 1
                      else 0
                  end) as xHD,
              max(case
                    when det.subscription_sub_type = 'DTV Extra Subscription' and det.status_code in ('AC','AB','PC') then 1
                      else 0
                  end) as Multiscreen

          from EPL_70_Attribution_Base_Table a,
               sk_prod.cust_subs_hist det
                    left join sk_prod.cust_entitlement_lookup as cel  on det.current_short_description = cel.short_description
         where a.Account_Number = det.Account_Number
           and det.Effective_From_Dt <= (a.Latest_Active_Date - 180)          -- Pack & Prod holding 6 months earlier
           and det.Effective_To_Dt > (a.Latest_Active_Date - 180)             -- Pack & Prod holding 6 months earlier
           and det.Effective_From_Dt < det.Effective_To_Dt
         group by a.Account_Number) hist
 where base.Account_Number = hist.Account_Number;
commit;


  -- ### Number of Sky products ###
update EPL_70_Attribution_Base_Table base
   set base.PRD_Number_Of_Sky_Products_NOW
                                        = case when base.PRD_Base_Package_NOW <> 'Unknown' then 1 else 0 end +
                                          case when base.PRD_Prem_Movies_NOW > 0 then 1 else 0 end +
                                          case when base.PRD_Broadband_NOW = 'Broadband' then 1 else 0 end +
                                          case when base.PRD_SkyTalk_NOW = 'SkyTalk' then 1 else 0 end +
                                          case when base.PRD_HD_NOW = 'HD' then 1 else 0 end +
                                          case when base.PRD_Multiscreen_NOW ='Multiscreen' then 1 else 0 end,
       base.PRD_Number_Of_Sky_Products_PREV
                                        = case when base.PRD_Base_Package_PREV <> 'Unknown' then 1 else 0 end +
                                          case when base.PRD_Prem_Movies_PREV > 0 then 1 else 0 end +
                                          case when base.PRD_Broadband_PREV = 'Broadband' then 1 else 0 end +
                                          case when base.PRD_SkyTalk_PREV = 'SkyTalk' then 1 else 0 end +
                                          case when base.PRD_HD_PREV = 'HD' then 1 else 0 end +
                                          case when base.PRD_Multiscreen_PREV ='Multiscreen' then 1 else 0 end;
commit;

update EPL_70_Attribution_Base_Table base
   set base.PRD_Number_Of_Sky_Products_MVMNT
                                        = case
                                            when base.PRD_Number_Of_Sky_Products_NOW = base.PRD_Number_Of_Sky_Products_PREV then 'No change'
                                            when base.PRD_Number_Of_Sky_Products_NOW > base.PRD_Number_Of_Sky_Products_PREV then 'Up'
                                              else 'Down'
                                          end;
commit;


  -- ### Pack & Product movements ###
update EPL_70_Attribution_Base_Table base
   set base.PRD_Value_Segment_MVMNT     = case
                                            when base.PRD_Value_Segment_NOW = 'Z) Unknown' or base.PRD_Value_Segment_PREV = 'Z) Unknown' then 'Unknown'
                                            when base.PRD_Value_Segment_NOW = base.PRD_Value_Segment_PREV then 'No change'
                                            when base.PRD_Value_Segment_NOW < base.PRD_Value_Segment_PREV then 'Up'     -- Leading sorting character
                                              else 'Down'
                                          end,
       base.PRD_Base_Package_MVMNT      = case
                                            when base.PRD_Base_Package_NOW = 'Unknown' or base.PRD_Base_Package_PREV = 'Unknown' then 'Unknown'
                                            when base.PRD_Base_Package_NOW = base.PRD_Base_Package_PREV then 'No change'
                                            when base.PRD_Base_Package_NOW > base.PRD_Base_Package_PREV then 'Up'       -- Leading number
                                              else 'Down'
                                          end,
       base.PRD_Prem_Movies_MVMNT       = case
                                            when base.PRD_Prem_Movies_NOW = base.PRD_Prem_Movies_PREV then 'No change'
                                            when base.PRD_Prem_Movies_NOW > base.PRD_Prem_Movies_PREV then 'Up'         -- Numeric
                                              else 'Down'
                                          end,
       base.PRD_Prem_Sports_MVMNT       = case
                                            when base.PRD_Prem_Sports_NOW = base.PRD_Prem_Sports_PREV then 'No change'
                                            when base.PRD_Prem_Sports_NOW > base.PRD_Prem_Sports_PREV then 'Up'         -- Numeric
                                              else 'Down'
                                          end,
       base.PRD_SkyTalk_MVMNT           = case
                                            when base.PRD_SkyTalk_NOW = base.PRD_SkyTalk_PREV then 'No change'
                                            when base.PRD_SkyTalk_NOW > base.PRD_SkyTalk_PREV then 'Up'                 -- SkyTalk vs No SkyTalk
                                              else 'Down'
                                          end,
       base.PRD_Broadband_MVMNT         = case
                                            when base.PRD_Broadband_NOW = base.PRD_Broadband_PREV then 'No change'
                                            when base.PRD_Broadband_NOW < base.PRD_Broadband_PREV then 'Up'             -- Broadband vs No Broadband
                                              else 'Down'
                                          end,
       base.PRD_HD_MVMNT                = case
                                            when base.PRD_HD_NOW = base.PRD_HD_PREV then 'No change'
                                            when base.PRD_HD_NOW < base.PRD_HD_PREV then 'Up'                           -- HD vs No HD
                                              else 'Down'
                                          end,
       base.PRD_Multiscreen_MVMNT       = case
                                            when base.PRD_Multiscreen_NOW = base.PRD_Multiscreen_PREV then 'No change'
                                            when base.PRD_Multiscreen_NOW < base.PRD_Multiscreen_PREV then 'Up'         -- Multiscreen vs No Multiscreen
                                              else 'Down'
                                          end;
commit;


update EPL_70_Attribution_Base_Table base
   set base.PRD_TV_Package_NOW      = substr(PRD_Base_Package_NOW, 4) ||
                                      case
                                        when PRD_Prem_Movies_NOW > 0 and PRD_Prem_Sports_NOW > 0 then ' & Top Tier'
                                        when PRD_Prem_Movies_NOW > 0 then ' & Dual Movies'
                                        when PRD_Prem_Sports_NOW > 0 then ' & Dual Sports'
                                          else ' & No Premium'
                                      end,
       base.PRD_TV_Package_PREV     = substr(PRD_Base_Package_PREV, 4) ||
                                      case
                                        when PRD_Prem_Movies_PREV > 0 and PRD_Prem_Sports_PREV > 0 then ' & Top Tier'
                                        when PRD_Prem_Movies_PREV > 0 then ' & Dual Movies'
                                        when PRD_Prem_Sports_PREV > 0 then ' & Dual Sports'
                                          else ' & No Premium'
                                      end,

       base.PRD_Sky_Product_NOW     = 'DTV' ||
                                      case
                                        when PRD_HD_NOW not like 'No%' then ', HD'
                                          else ''
                                      end ||
                                      case
                                        when PRD_Multiscreen_NOW not like 'No%' then ', Multiscreen'
                                          else ''
                                      end ||
                                      case
                                        when PRD_Broadband_NOW not like 'No%' then ', Broadband'
                                          else ''
                                      end ||
                                      case
                                        when PRD_SkyTalk_NOW not like 'No%' then ', SkyTalk'
                                          else ''
                                      end,
       base.PRD_Sky_Product_PREV    = 'DTV' ||
                                      case
                                        when PRD_HD_PREV not like 'No%' then ', HD'
                                          else ''
                                      end ||
                                      case
                                        when PRD_Multiscreen_PREV not like 'No%' then ', Multiscreen'
                                          else ''
                                      end ||
                                      case
                                        when PRD_Broadband_PREV not like 'No%' then ', Broadband'
                                          else ''
                                      end ||
                                      case
                                        when PRD_SkyTalk_PREV not like 'No%' then ', SkyTalk'
                                          else ''
                                      end;
commit;

/*
-- ### Movement calculation checks ###
select PRD_Value_Segment_PREV as prev, PRD_Value_Segment_NOW as now, PRD_Value_Segment_MVMNT as mvmnt, count(*) as Cnt from EPL_70_Attribution_Base_Table group by prev, now, mvmnt order by 1, 2, 3;
select PRD_Base_Package_PREV as prev, PRD_Base_Package_NOW as now, PRD_Base_Package_MVMNT as mvmnt, count(*) as Cnt from EPL_70_Attribution_Base_Table group by prev, now, mvmnt order by 1, 2, 3;
select PRD_Prem_Movies_PREV as prev, PRD_Prem_Movies_NOW as now, PRD_Prem_Movies_MVMNT as mvmnt, count(*) as Cnt from EPL_70_Attribution_Base_Table group by prev, now, mvmnt order by 1, 2, 3;
select PRD_Prem_Sports_PREV as prev, PRD_Prem_Sports_NOW as now, PRD_Prem_Sports_MVMNT as mvmnt, count(*) as Cnt from EPL_70_Attribution_Base_Table group by prev, now, mvmnt order by 1, 2, 3;
select PRD_SkyTalk_PREV as prev, PRD_SkyTalk_NOW as now, PRD_SkyTalk_MVMNT as mvmnt, count(*) as Cnt from EPL_70_Attribution_Base_Table group by prev, now, mvmnt order by 1, 2, 3;
select PRD_Broadband_PREV as prev, PRD_Broadband_NOW as now, PRD_Broadband_MVMNT as mvmnt, count(*) as Cnt from EPL_70_Attribution_Base_Table group by prev, now, mvmnt order by 1, 2, 3;
select PRD_HD_PREV as prev, PRD_HD_NOW as now, PRD_HD_MVMNT as mvmnt, count(*) as Cnt from EPL_70_Attribution_Base_Table group by prev, now, mvmnt order by 1, 2, 3;
select PRD_Multiscreen_PREV as prev, PRD_Multiscreen_NOW as now, PRD_Multiscreen_MVMNT as mvmnt, count(*) as Cnt from EPL_70_Attribution_Base_Table group by prev, now, mvmnt order by 1, 2, 3;

-- ### Pack & product calculation checks ###
select PRD_TV_Package_NOW, PRD_Base_Package_NOW, PRD_Prem_Movies_NOW, PRD_Prem_Sports_NOW, count(*) as Cnt from EPL_70_Attribution_Base_Table group by PRD_TV_Package_NOW, PRD_Base_Package_NOW, PRD_Prem_Movies_NOW, PRD_Prem_Sports_NOW order by 1, 2, 3, 4;
select PRD_TV_Package_PREV, PRD_Base_Package_PREV, PRD_Prem_Movies_PREV, PRD_Prem_Sports_PREV, count(*) as Cnt from EPL_70_Attribution_Base_Table group by PRD_TV_Package_PREV, PRD_Base_Package_PREV, PRD_Prem_Movies_PREV, PRD_Prem_Sports_PREV order by 1, 2, 3, 4;

select PRD_Sky_Product_NOW, PRD_HD_NOW, PRD_Multiscreen_NOW, PRD_Broadband_NOW, PRD_SkyTalk_NOW, count(*) as Cnt from EPL_70_Attribution_Base_Table group by PRD_Sky_Product_NOW, PRD_HD_NOW, PRD_Multiscreen_NOW, PRD_Broadband_NOW, PRD_SkyTalk_NOW order by 1, 2, 3, 4;
select PRD_Sky_Product_PREV, PRD_HD_PREV, PRD_Multiscreen_PREV, PRD_Broadband_PREV, PRD_SkyTalk_PREV, count(*) as Cnt from EPL_70_Attribution_Base_Table group by PRD_Sky_Product_PREV, PRD_HD_PREV, PRD_Multiscreen_PREV, PRD_Broadband_PREV, PRD_SkyTalk_PREV order by 1, 2, 3, 4;
*/


  -- ### Sky Go ###
update EPL_70_Attribution_Base_Table base
   set base.PRD_Sky_Go_Any_Usage_Segment
                                      = case
                                          when det.Total_Streams > 0 then '2) Sky Go user'
                                            else '1) Non-Sky Go user'
                                        end
  from (select
              a.Account_Number,
              count(*) as Total_Streams
          from sk_prod.sky_player_usage_detail a
         where a.Account_Number is not null
           and Activity_dt between '2013-08-01' and '2014-02-28'
         group by a.Account_Number) det
 where base.Account_Number = det.Account_Number;
commit;


  -- ### On Demand ###
update EPL_70_Attribution_Base_Table base
   set base.PRD_On_Demand_Streams_Num      = det.Total_Streams,
       base.PRD_On_Demand_Programmes_Num   = det.Total_Programmes,
       base.PRD_On_Demand_Usage_Segment    = case
                                               when det.Total_Programmes >= 74 then '4) Heavy OnDemand user'
                                               when det.Total_Programmes >= 15 then '3) Moderate OnDemand user'
                                               when det.Total_Programmes >= 1 then '2) Light OnDemand user'
                                                 else '1) Non-OnDemand user'
                                             end
  from (select
              a.Account_Number,
              count(distinct pdl_cdn_video_weblogs_sk) as Total_Programmes,
              count(*) as Total_Streams
          from sk_prod.Cust_anytime_plus_downloads a
         where a.x_content_type_desc = 'PROGRAMME'                                  -- to exclude trailers
           and a.x_actual_downloaded_size_mb > 1                                    -- to exclude any spurious header/trailer download records
           and a.last_modified_dt between '2013-09-01' and '2014-02-28'
         group by a.Account_Number) det
 where base.Account_Number = det.Account_Number;
commit;



  -- ##############################################################################################################
  -- ##### Behavioural (Sports)                                                                               #####
  -- ##############################################################################################################
alter table EPL_70_Attribution_Base_Table add (
      BEH_Sports_Downgrade_Events_Num         bigint            null      default 0,
      BEH_Sports_Downgrade_Event              varchar(25)       null      default 'Unknown',
      BEH_Sports_Upgrade_Events_Num           bigint            null      default 0,
      BEH_Sports_Upgrade_Event                varchar(25)       null      default 'Unknown',

      BEH_Sports_Downgrade_Events_Num_L6m     bigint            null      default 0,
      BEH_Sports_Downgrade_Event_L6m          varchar(25)       null      default 'Unknown',
      BEH_Sports_Upgrade_Events_Num_L6m       bigint            null      default 0,
      BEH_Sports_Upgrade_Event_L6m            varchar(25)       null      default 'Unknown',

      BEH_Sports_Tenure_Continuous            varchar(25)       null      default 'Unknown',
      BEH_Sports_Tenure_Overall               varchar(25)       null      default 'Unknown',

      BEH_Sports_Up_Down_Grade_Ratio          decimal(15, 6)    null      default -1,
      BEH_Sports_Up_Down_Grade_Ratio_L6m      decimal(15, 6)    null      default -1
);


  -- ### Sports upgrade / downgrade during period ###
if object_id('EPL_Attr_tmp_Sports_Ent_Hist') is not null then drop table EPL_Attr_tmp_Sports_Ent_Hist end if;
select
      base.Account_Number,
      base.Latest_Active_Date,
      det.Effective_From_Dt,
      det.Effective_To_Dt,

      lag(Status_Code) over (partition by base.Account_Number, base.Latest_Active_Date order by det.Effective_From_Dt, det.Effective_To_Dt) Prev_Status_Code,
      det.Status_Code,
      cast(case
             when Prev_Status_Code <> det.Status_Code then 1
               else 0
           end as bit) as Status_Changed,

      lag(cel.Prem_Sports) over (partition by base.Account_Number, base.Latest_Active_Date order by det.Effective_From_Dt, det.Effective_To_Dt) Prev_Prem_Sports,
      cel.Prem_Sports,
      cast(case
             when Prev_Prem_Sports <> cel.Prem_Sports then 1
               else 0
           end as bit) as Prem_Sports_Changed,

      case
        when cel.Prem_Sports > 0 and Prev_Prem_Sports = 0 then 1
        when Prev_Status_Code in ('PO', 'SC', 'IT', 'SU', 'EN') and det.Status_Code in ('AC', 'PC', 'AB') then 2
          else 0
      end as Sports_Upgrade_Event,

      case
        when cel.Prem_Sports = 0 and Prev_Prem_Sports > 0 then 1
        when det.Status_Code in ('PO', 'SC') and Prev_Status_Code in ('AC', 'PC', 'AB') then 2
          else 0
      end as Sports_Downgrade_Event

  into EPL_Attr_tmp_Sports_Ent_Hist
  from EPL_70_Attribution_Base_Table base,
       sk_prod.cust_subs_hist det
          left join sk_prod.cust_entitlement_lookup as cel  on det.current_short_description = cel.short_description
 where base.Account_Number = det.Account_Number
   and det.Subscription_Sub_Type = 'DTV Primary Viewing'
   and det.Effective_From_Dt < det.Effective_To_Dt
   and det.Effective_From_Dt <= base.Latest_Active_Date;
commit;
create        hg   index idx01 on EPL_Attr_tmp_Sports_Ent_Hist(Account_Number);
create        lf   index idx02 on EPL_Attr_tmp_Sports_Ent_Hist(Sports_Upgrade_Event);
create        lf   index idx03 on EPL_Attr_tmp_Sports_Ent_Hist(Sports_Downgrade_Event);


  -- Upgrade / downgrade during period
update EPL_70_Attribution_Base_Table base
   set base.BEH_Sports_Downgrade_Events_Num = det.Sports_Downgrade_Events,
       base.BEH_Sports_Downgrade_Event      = case
                                                when det.Sports_Downgrade_Events > 0 then 'Yes'
                                                  else 'No'
                                              end,
       base.BEH_Sports_Upgrade_Events_Num   = det.Sports_Upgrade_Events,
       base.BEH_Sports_Upgrade_Event        = case
                                                when det.Sports_Upgrade_Events > 0 then 'Yes'
                                                  else 'No'
                                              end,

       base.BEH_Sports_Downgrade_Events_Num_L6m
                                            = det.Sports_Downgrade_Events_L6m,
       base.BEH_Sports_Downgrade_Event_L6m  = case
                                                when det.Sports_Downgrade_Events_L6m > 0 then 'Yes'
                                                  else 'No'
                                              end,
       base.BEH_Sports_Upgrade_Events_Num_L6m
                                            = det.Sports_Upgrade_Events_L6m,
       base.BEH_Sports_Upgrade_Event_L6m    = case
                                                when det.Sports_Upgrade_Events_L6m > 0 then 'Yes'
                                                  else 'No'
                                              end

  from (select
              Account_Number,
              Latest_Active_Date,

              sum(case
                    when Sports_Upgrade_Event = 1 then 1
                      else 0
                  end) as Sports_Upgrade_Events,
              sum(case
                    when Sports_Downgrade_Event = 1 then 1
                      else 0
                  end) as Sports_Downgrade_Events,

              sum(case
                    when Sports_Upgrade_Event = 1 and Effective_From_Dt >= (Latest_Active_Date - 180) then 1
                      else 0
                  end) as Sports_Upgrade_Events_L6m,
              sum(case
                    when Sports_Downgrade_Event = 1 and Effective_From_Dt >= (Latest_Active_Date - 180) then 1
                      else 0
                  end) as Sports_Downgrade_Events_L6m

          from EPL_Attr_tmp_Sports_Ent_Hist
         group by Account_Number, Latest_Active_Date) det
 where base.Account_Number = det.Account_Number
   and base.Latest_Active_Date = det.Latest_Active_Date;
commit;


  -- Sports tenure (continuous)
update EPL_70_Attribution_Base_Table base
   set base.BEH_Sports_Tenure_Continuous
                                        = case
                                            when datediff(day, det.Sport_Since_Dt, base.Latest_Active_Date) <= 90        then '1) <=3 months'
                                            when datediff(day, det.Sport_Since_Dt, base.Latest_Active_Date) <= 180       then '2) 4-6 months'
                                            when datediff(day, det.Sport_Since_Dt, base.Latest_Active_Date) <= 365       then '3) 7-12 months'
                                            when datediff(day, det.Sport_Since_Dt, base.Latest_Active_Date) <= 2 * 365   then '4) 1-2 years'
                                            when datediff(day, det.Sport_Since_Dt, base.Latest_Active_Date) <= 5 * 365   then '5) 3-5 years'
                                              else '6) 5+ years'
                                          end
  from (select
              Account_Number,
              Latest_Active_Date,
              max(Effective_From_Dt) as Sport_Since_Dt
          from EPL_Attr_tmp_Sports_Ent_Hist
         where Sports_Upgrade_Event in (1, 2)                                                -- Upgrades from 0 to 1/2 Sports AND since recent (re)activation
         group by Account_Number, Latest_Active_Date) det
 where base.Account_Number = det.Account_Number
   and base.Latest_Active_Date = det.Latest_Active_Date
   and base.Prem_Sports > 0;
commit;


  -- Sports tenure (maximum)
update EPL_70_Attribution_Base_Table base
   set base.BEH_Sports_Tenure_Overall
                                        = case
                                            when datediff(day, det.Sport_Since_Dt, base.Latest_Active_Date) <= 90        then '1) <=3 months'
                                            when datediff(day, det.Sport_Since_Dt, base.Latest_Active_Date) <= 180       then '2) 4-6 months'
                                            when datediff(day, det.Sport_Since_Dt, base.Latest_Active_Date) <= 365       then '3) 7-12 months'
                                            when datediff(day, det.Sport_Since_Dt, base.Latest_Active_Date) <= 2 * 365   then '4) 1-2 years'
                                            when datediff(day, det.Sport_Since_Dt, base.Latest_Active_Date) <= 5 * 365   then '5) 3-5 years'
                                              else '6) 5+ years'
                                          end
  from (select
              Account_Number,
              Latest_Active_Date,
              min(Effective_From_Dt) as Sport_Since_Dt
          from EPL_Attr_tmp_Sports_Ent_Hist
         where Sports_Upgrade_Event in (1, 2)                                                -- Upgrades from 0 to 1/2 Sports AND since first (re)activation
         group by Account_Number, Latest_Active_Date) det
 where base.Account_Number = det.Account_Number
   and base.Latest_Active_Date = det.Latest_Active_Date
   and base.Prem_Sports > 0;
commit;


update EPL_70_Attribution_Base_Table base
   set base.BEH_Sports_Up_Down_Grade_Ratio
                                        = case
                                            when base.BEH_Sports_Downgrade_Events_Num = 0 then -1
                                              else 1.0 * base.BEH_Sports_Upgrade_Events_Num  / base.BEH_Sports_Downgrade_Events_Num
                                          end,
       base.BEH_Sports_Up_Down_Grade_Ratio_L6m
                                        = case
                                            when base.BEH_Sports_Downgrade_Events_Num_L6m = 0 then -1
                                              else 1.0 * base.BEH_Sports_Upgrade_Events_Num_L6m  / base.BEH_Sports_Downgrade_Events_Num_L6m
                                          end;
commit;




  -- ##############################################################################################################
  -- ##### Additional                                                                                         #####
  -- ##############################################################################################################
alter table EPL_70_Attribution_Base_Table add (
      EXT_CQM_Score                               varchar(10)       null      default 'Unknown',
      EXT_Cable_Area                              varchar(10)       null      default 'Unknown',

      EXT_acct_first_account_activation_dt        date              null      default null,
      EXT_prod_dtv_activation_dt                  date              null      default null,
      EXT_Account_Tenure                          varchar(25)       null      default 'Unknown',
      EXT_DTV_Tenure                              varchar(25)       null      default 'Unknown'
);

-- ### CQM score ###
update EPL_70_Attribution_Base_Table base
   set base.EXT_CQM_Score = case
                              when det.Model_Score between 1 and 9 then '0' || trim(cast(det.Model_Score as varchar(5)))
                              when det.Model_Score is not null then cast(det.Model_Score as varchar(5))
                                else 'Unknown'
                            end
  from (select
              a.Account_Number,
              max(b.Model_Score) as Model_Score
          from sk_prod.cust_single_account_view a,
               sk_prod.id_v_universe_all b
         where a.Cb_Key_Household = b.Cb_Key_Household
         group by a.Account_Number) det
 where base.Account_Number = det.Account_Number;
commit;


  -- ### Cable area ###
update EPL_70_Attribution_Base_Table base
   set base.EXT_Cable_Area  = case
                                when lower(det.Cable_Postcode) = 'n' then 'No'
                                when lower(det.Cable_Postcode) = 'y' then 'Yes'
                                  else 'Unknown'
                              end
  from (select
              a.Account_Number,
              max(b.Cable_Postcode) as Cable_Postcode
          from sk_prod.cust_single_account_view a left join sk_prod.broadband_postcode_exchange b
                  on replace(a.cb_address_postcode, ' ', '') = replace(b.cb_address_postcode,' ', '')
         group by a.Account_Number) det
 where base.Account_Number = det.Account_Number;
commit;



-- ### Tenure ###
update EPL_70_Attribution_Base_Table base
   set base.EXT_acct_first_account_activation_dt = sav.acct_first_account_activation_dt,
       base.EXT_prod_dtv_activation_dt = sav.prod_dtv_activation_dt,
       base.EXT_Account_Tenure
                          = case
                              when sav.acct_first_account_activation_dt is null or sav.acct_first_account_activation_dt > base.Latest_Active_Date then 'Unknown'
                              when datediff(day, sav.acct_first_account_activation_dt, base.Latest_Active_Date) <=    90 then '1) <=3 months'
                              when datediff(day, sav.acct_first_account_activation_dt, base.Latest_Active_Date) <=   180 then '2) 4-6 months'
                              when datediff(day, sav.acct_first_account_activation_dt, base.Latest_Active_Date) <=   360 then '3) 7-12 months'
                              when datediff(day, sav.acct_first_account_activation_dt, base.Latest_Active_Date) <=   730 then '4) 13-24 months'
                              when datediff(day, sav.acct_first_account_activation_dt, base.Latest_Active_Date) <=  1825 then '5) 3-5 years'
                              when datediff(day, sav.acct_first_account_activation_dt, base.Latest_Active_Date) <=  3650 then '6) 6-10 years'
                                else '7) 10+ years'
                            end,
       base.EXT_DTV_Tenure
                          = case
                              when sav.prod_dtv_activation_dt is null or sav.prod_dtv_activation_dt > base.Latest_Active_Date then 'Unknown'
                              when datediff(day, sav.prod_dtv_activation_dt, base.Latest_Active_Date) <=    90 then '1) <=3 months'
                              when datediff(day, sav.prod_dtv_activation_dt, base.Latest_Active_Date) <=   180 then '2) 4-6 months'
                              when datediff(day, sav.prod_dtv_activation_dt, base.Latest_Active_Date) <=   360 then '3) 7-12 months'
                              when datediff(day, sav.prod_dtv_activation_dt, base.Latest_Active_Date) <=   730 then '4) 13-24 months'
                              when datediff(day, sav.prod_dtv_activation_dt, base.Latest_Active_Date) <=  1825 then '5) 3-5 years'
                              when datediff(day, sav.prod_dtv_activation_dt, base.Latest_Active_Date) <=  3650 then '6) 6-10 years'
                                else '7) 10+ years'
                            end
  from sk_prod.cust_single_account_view sav
 where base.Account_Number = sav.Account_Number;
commit;


  -- ##############################################################################################################
  -- ##### Consumer propensities                                                                              #####
  -- ##############################################################################################################
alter table EPL_70_Attribution_Base_Table add (
      CP_h_affluence_v2 varchar(10) null default '{Unknown}',
      CP_h_age_coarse varchar(10) null default '{Unknown}',
      CP_h_age_fine varchar(10) null default '{Unknown}',
      CP_h_ccjs_number varchar(10) null default '{Unknown}',
      CP_h_ccjs_value varchar(10) null default '{Unknown}',
      CP_h_council_tax_band_wales_2003 varchar(10) null default '{Unknown}',
      CP_h_equivalised_income_band varchar(50) null default '{Unknown}',
      CP_h_equivalised_income_value varchar(10) null default '{Unknown}',
      CP_h_family_lifestage_2011 varchar(10) null default '{Unknown}',
      CP_h_fss_factor_a_decile varchar(10) null default '{Unknown}',
      CP_h_fss_factor_a_percentile varchar(10) null default '{Unknown}',
      CP_h_fss_factor_b_decile varchar(10) null default '{Unknown}',
      CP_h_fss_factor_b_percentile varchar(10) null default '{Unknown}',
      CP_h_fss_factor_c_decile varchar(10) null default '{Unknown}',
      CP_h_fss_factor_c_percentile varchar(10) null default '{Unknown}',
      CP_h_fss_factor_d_decile varchar(10) null default '{Unknown}',
      CP_h_fss_factor_d_percentile varchar(10) null default '{Unknown}',
      CP_h_fss_factor_e_decile varchar(10) null default '{Unknown}',
      CP_h_fss_factor_e_percentile varchar(10) null default '{Unknown}',
      CP_h_fss_factor_f_decile varchar(10) null default '{Unknown}',
      CP_h_fss_factor_f_percentile varchar(10) null default '{Unknown}',
      CP_h_fss_group varchar(10) null default '{Unknown}',
      CP_h_fss_v3_group varchar(10) null default '{Unknown}',
      CP_h_fss_v3_type varchar(10) null default '{Unknown}',
      CP_h_household_composition varchar(10) null default '{Unknown}',
      CP_h_income_band_v2 varchar(50) null default '{Unknown}',
      CP_h_income_value_v2 varchar(10) null default '{Unknown}',
      CP_h_length_of_residency varchar(10) null default '{Unknown}',
      CP_h_length_of_residency_coarse varchar(10) null default '{Unknown}',
      CP_h_lifestage varchar(10) null default '{Unknown}',
      CP_h_mosaic_ni_group varchar(10) null default '{Unknown}',
      CP_h_mosaic_ni_type varchar(10) null default '{Unknown}',
      CP_h_mosaic_scotland_group varchar(10) null default '{Unknown}',
      CP_h_mosaic_scotland_segment_alternative varchar(10) null default '{Unknown}',
      CP_h_mosaic_scotland_type varchar(10) null default '{Unknown}',
      CP_h_mosaic_uk_2003_group varchar(10) null default '{Unknown}',
      CP_h_mosaic_uk_2003_segment varchar(10) null default '{Unknown}',
      CP_h_mosaic_uk_2003_segment_alternative varchar(10) null default '{Unknown}',
      CP_h_mosaic_uk_2003_type varchar(10) null default '{Unknown}',
      CP_h_mosaic_uk_group varchar(10) null default '{Unknown}',
      CP_h_mosaic_uk_second_best_type varchar(10) null default '{Unknown}',
      CP_h_mosaic_uk_segment varchar(10) null default '{Unknown}',
      CP_h_mosaic_uk_segment_alternative varchar(10) null default '{Unknown}',
      CP_h_mosaic_uk_type varchar(10) null default '{Unknown}',
      CP_h_mosaic_uk_type_affinity_percentile varchar(10) null default '{Unknown}',
      CP_h_number_of_adults varchar(10) null default '{Unknown}',
      CP_h_number_of_bedrooms varchar(10) null default '{Unknown}',
      CP_h_number_of_children_in_household_2011 varchar(10) null default '{Unknown}',
      CP_h_outstanding_mortgage_v2 varchar(10) null default '{Unknown}',
      CP_h_presence_of_child_aged_0_4_2011 varchar(10) null default '{Unknown}',
      CP_h_presence_of_child_aged_12_17_2011 varchar(10) null default '{Unknown}',
      CP_h_presence_of_child_aged_5_11_2011 varchar(10) null default '{Unknown}',
      CP_h_presence_of_young_person_at_address varchar(10) null default '{Unknown}',
      CP_h_property_council_taxation varchar(10) null default '{Unknown}',
      CP_h_property_type varchar(10) null default '{Unknown}',
      CP_h_property_type_coarse varchar(10) null default '{Unknown}',
      CP_h_property_type_v2 varchar(10) null default '{Unknown}',
      CP_h_residence_type_v2 varchar(10) null default '{Unknown}',
      CP_h_shareholding_value varchar(10) null default '{Unknown}',
      CP_h_tenure varchar(10) null default '{Unknown}',
      CP_h_tenure_v2 varchar(10) null default '{Unknown}'
);


if object_id('EPL_Attr_Tmp_ConProp_Lookup') is not null then drop table EPL_Attr_Tmp_ConProp_Lookup end if;
SELECT
    cv.cb_key_household,
    cv.cb_key_family,
    cv.cb_key_individual,
    cv.cb_row_id,
    pp.p_head_of_household as p_head_of_household,
    rank() over(partition by cb_key_household  ORDER BY pp.p_head_of_household desc, cv.cb_row_id desc, pp.cb_row_id desc) as rank_hh,
    rank() over(partition by cb_key_family     ORDER BY pp.p_head_of_household desc, cv.cb_row_id desc, pp.cb_row_id desc) as rank_fam,
    rank() over(partition by cb_key_individual ORDER BY pp.p_head_of_household desc, cv.cb_row_id desc, pp.cb_row_id desc) as rank_ind
  INTO EPL_Attr_Tmp_ConProp_Lookup
  FROM sk_prod.experian_consumerview cv,
       sk_prod.playpen_consumerview_person_and_household pp
 WHERE cv.exp_cb_key_db_individual = pp.exp_cb_key_db_individual
   AND cv.cb_key_individual is not null;
COMMIT;

CREATE LF INDEX idx1 on EPL_Attr_Tmp_ConProp_Lookup(p_head_of_household);
CREATE HG INDEX idx2 on EPL_Attr_Tmp_ConProp_Lookup(rank_hh);
CREATE HG INDEX idx3 on EPL_Attr_Tmp_ConProp_Lookup(rank_fam);
CREATE HG INDEX idx4 on EPL_Attr_Tmp_ConProp_Lookup(rank_ind);
CREATE HG INDEX idx5 on EPL_Attr_Tmp_ConProp_Lookup(cb_key_household);
CREATE HG INDEX idx6 on EPL_Attr_Tmp_ConProp_Lookup(cb_row_id);


if object_id('EPL_Attr_Tmp_ConProp_Data') is not null then drop table EPL_Attr_Tmp_ConProp_Data end if;
select
      cp.cb_key_household,
      cp.cb_key_family,
      cp.cb_key_individual,
      cp.cb_row_id as cb_row_id,
      lk.p_head_of_household as p_head_of_household,
      lk.rank_hh,
      lk.rank_fam,
      lk.rank_ind,

      cp.h_affluence_v2,
      cp.h_age_coarse,
      cp.h_age_fine,
      cp.h_ccjs_number,
      cp.h_ccjs_value,
      cp.h_council_tax_band_wales_2003,
      cp.h_equivalised_income_band,
      cp.h_equivalised_income_value,
      cp.h_family_lifestage_2011,
      cp.h_fss_factor_a_decile,
      cp.h_fss_factor_a_percentile,
      cp.h_fss_factor_b_decile,
      cp.h_fss_factor_b_percentile,
      cp.h_fss_factor_c_decile,
      cp.h_fss_factor_c_percentile,
      cp.h_fss_factor_d_decile,
      cp.h_fss_factor_d_percentile,
      cp.h_fss_factor_e_decile,
      cp.h_fss_factor_e_percentile,
      cp.h_fss_factor_f_decile,
      cp.h_fss_factor_f_percentile,
      cp.h_fss_group,
      cp.h_fss_v3_group,
      cp.h_fss_v3_type,
      cp.h_household_composition,
      cp.h_income_band_v2,
      cp.h_income_value_v2,
      cp.h_length_of_residency,
      cp.h_length_of_residency_coarse,
      cp.h_lifestage,
      cp.h_mosaic_ni_group,
      cp.h_mosaic_ni_type,
      cp.h_mosaic_scotland_group,
      cp.h_mosaic_scotland_segment_alternative,
      cp.h_mosaic_scotland_type,
      cp.h_mosaic_uk_2003_group,
      cp.h_mosaic_uk_2003_segment,
      cp.h_mosaic_uk_2003_segment_alternative,
      cp.h_mosaic_uk_2003_type,
      cp.h_mosaic_uk_group,
      cp.h_mosaic_uk_second_best_type,
      cp.h_mosaic_uk_segment,
      cp.h_mosaic_uk_segment_alternative,
      cp.h_mosaic_uk_type,
      cp.h_mosaic_uk_type_affinity_percentile,
      cp.h_number_of_adults,
      cp.h_number_of_bedrooms,
      cp.h_number_of_children_in_household_2011,
      cp.h_outstanding_mortgage_v2,
      cp.h_presence_of_child_aged_0_4_2011,
      cp.h_presence_of_child_aged_12_17_2011,
      cp.h_presence_of_child_aged_5_11_2011,
      cp.h_presence_of_young_person_at_address,
      cp.h_property_council_taxation,
      cp.h_property_type,
      cp.h_property_type_coarse,
      cp.h_property_type_v2,
      cp.h_residence_type_v2,
      cp.h_shareholding_value,
      cp.h_tenure,
      cp.h_tenure_v2

  into EPL_Attr_Tmp_ConProp_Data
  from sk_prod.experian_consumerview cp,
       EPL_Attr_Tmp_ConProp_Lookup lk
 where cp.cb_row_id = lk.cb_row_id
   and lk.rank_hh = 1;
commit;

CREATE unique HG INDEX idx1 on EPL_Attr_Tmp_ConProp_Data(cb_key_household);



update EPL_70_Attribution_Base_Table base
   set
       base.CP_h_affluence_v2 = case when trim(lower(cast(det.h_affluence_v2 as varchar(50)))) in ('u', '', ' ') or det.h_affluence_v2 is null then '{Unknown}' else cast(det.h_affluence_v2 as varchar(50)) end,
       base.CP_h_age_coarse = case when trim(lower(cast(det.h_age_coarse as varchar(50)))) in ('u', '', ' ') or det.h_age_coarse is null then '{Unknown}' else cast(det.h_age_coarse as varchar(50)) end,
       base.CP_h_age_fine = case when trim(lower(cast(det.h_age_fine as varchar(50)))) in ('u', '', ' ') or det.h_age_fine is null then '{Unknown}' else cast(det.h_age_fine as varchar(50)) end,
       base.CP_h_ccjs_number = case when trim(lower(cast(det.h_ccjs_number as varchar(50)))) in ('u', '', ' ') or det.h_ccjs_number is null then '{Unknown}' else cast(det.h_ccjs_number as varchar(50)) end,
       base.CP_h_ccjs_value = case when trim(lower(cast(det.h_ccjs_value as varchar(50)))) in ('u', '', ' ') or det.h_ccjs_value is null then '{Unknown}' else cast(det.h_ccjs_value as varchar(50)) end,
       base.CP_h_council_tax_band_wales_2003 = case when trim(lower(cast(det.h_council_tax_band_wales_2003 as varchar(50)))) in ('u', '', ' ') or det.h_council_tax_band_wales_2003 is null then '{Unknown}' else cast(det.h_council_tax_band_wales_2003 as varchar(50)) end,
       base.CP_h_equivalised_income_band = case
                                             when trim(lower(cast(det.h_equivalised_income_band as varchar(50)))) in ('u', '', ' ') or det.h_equivalised_income_band is null then '{Unknown}'
                                             when det.h_equivalised_income_value <= 10000 then '01) <= £10000'
                                             when det.h_equivalised_income_value <= 15000 then '02) £10,001 - £15,000'
                                             when det.h_equivalised_income_value <= 20000 then '03) £15,001 - £20,000'
                                             when det.h_equivalised_income_value <= 25000 then '04) £20,001 - £25,000'
                                             when det.h_equivalised_income_value <= 30000 then '05) £25,001 - £30,000'
                                             when det.h_equivalised_income_value <= 35000 then '06) £30,001 - £35,000'
                                             when det.h_equivalised_income_value <= 40000 then '07) £35,001 - £40,000'
                                             when det.h_equivalised_income_value <= 50000 then '08) £40,001 - £50,000'
                                             when det.h_equivalised_income_value <= 65000 then '09) £50,001 - £65,000'
                                            else '10) $65,000+'
                                        end,
       base.CP_h_equivalised_income_value = case when trim(lower(cast(det.h_equivalised_income_value as varchar(50)))) in ('u', '', ' ') or det.h_equivalised_income_value is null then '{Unknown}' else cast(det.h_equivalised_income_value as varchar(50)) end,
       base.CP_h_family_lifestage_2011 = case when trim(lower(cast(det.h_family_lifestage_2011 as varchar(50)))) in ('u', '', ' ') or det.h_family_lifestage_2011 is null then '{Unknown}' else cast(det.h_family_lifestage_2011 as varchar(50)) end,
       base.CP_h_fss_factor_a_decile = case when trim(lower(cast(det.h_fss_factor_a_decile as varchar(50)))) in ('u', '', ' ') or det.h_fss_factor_a_decile is null then '{Unknown}' else cast(det.h_fss_factor_a_decile as varchar(50)) end,
       base.CP_h_fss_factor_a_percentile = case when trim(lower(cast(det.h_fss_factor_a_percentile as varchar(50)))) in ('u', '', ' ') or det.h_fss_factor_a_percentile is null then '{Unknown}' else cast(det.h_fss_factor_a_percentile as varchar(50)) end,
       base.CP_h_fss_factor_b_decile = case when trim(lower(cast(det.h_fss_factor_b_decile as varchar(50)))) in ('u', '', ' ') or det.h_fss_factor_b_decile is null then '{Unknown}' else cast(det.h_fss_factor_b_decile as varchar(50)) end,
       base.CP_h_fss_factor_b_percentile = case when trim(lower(cast(det.h_fss_factor_b_percentile as varchar(50)))) in ('u', '', ' ') or det.h_fss_factor_b_percentile is null then '{Unknown}' else cast(det.h_fss_factor_b_percentile as varchar(50)) end,
       base.CP_h_fss_factor_c_decile = case when trim(lower(cast(det.h_fss_factor_c_decile as varchar(50)))) in ('u', '', ' ') or det.h_fss_factor_c_decile is null then '{Unknown}' else cast(det.h_fss_factor_c_decile as varchar(50)) end,
       base.CP_h_fss_factor_c_percentile = case when trim(lower(cast(det.h_fss_factor_c_percentile as varchar(50)))) in ('u', '', ' ') or det.h_fss_factor_c_percentile is null then '{Unknown}' else cast(det.h_fss_factor_c_percentile as varchar(50)) end,
       base.CP_h_fss_factor_d_decile = case when trim(lower(cast(det.h_fss_factor_d_decile as varchar(50)))) in ('u', '', ' ') or det.h_fss_factor_d_decile is null then '{Unknown}' else cast(det.h_fss_factor_d_decile as varchar(50)) end,
       base.CP_h_fss_factor_d_percentile = case when trim(lower(cast(det.h_fss_factor_d_percentile as varchar(50)))) in ('u', '', ' ') or det.h_fss_factor_d_percentile is null then '{Unknown}' else cast(det.h_fss_factor_d_percentile as varchar(50)) end,
       base.CP_h_fss_factor_e_decile = case when trim(lower(cast(det.h_fss_factor_e_decile as varchar(50)))) in ('u', '', ' ') or det.h_fss_factor_e_decile is null then '{Unknown}' else cast(det.h_fss_factor_e_decile as varchar(50)) end,
       base.CP_h_fss_factor_e_percentile = case when trim(lower(cast(det.h_fss_factor_e_percentile as varchar(50)))) in ('u', '', ' ') or det.h_fss_factor_e_percentile is null then '{Unknown}' else cast(det.h_fss_factor_e_percentile as varchar(50)) end,
       base.CP_h_fss_factor_f_decile = case when trim(lower(cast(det.h_fss_factor_f_decile as varchar(50)))) in ('u', '', ' ') or det.h_fss_factor_f_decile is null then '{Unknown}' else cast(det.h_fss_factor_f_decile as varchar(50)) end,
       base.CP_h_fss_factor_f_percentile = case when trim(lower(cast(det.h_fss_factor_f_percentile as varchar(50)))) in ('u', '', ' ') or det.h_fss_factor_f_percentile is null then '{Unknown}' else cast(det.h_fss_factor_f_percentile as varchar(50)) end,
       base.CP_h_fss_group = case when trim(lower(cast(det.h_fss_group as varchar(50)))) in ('u', '', ' ') or det.h_fss_group is null then '{Unknown}' else cast(det.h_fss_group as varchar(50)) end,
       base.CP_h_fss_v3_group = case when trim(lower(cast(det.h_fss_v3_group as varchar(50)))) in ('u', '', ' ') or det.h_fss_v3_group is null then '{Unknown}' else cast(det.h_fss_v3_group as varchar(50)) end,
       base.CP_h_fss_v3_type = case when trim(lower(cast(det.h_fss_v3_type as varchar(50)))) in ('u', '', ' ') or det.h_fss_v3_type is null then '{Unknown}' else cast(det.h_fss_v3_type as varchar(50)) end,
       base.CP_h_household_composition = case when trim(lower(cast(det.h_household_composition as varchar(50)))) in ('u', '', ' ') or det.h_household_composition is null then '{Unknown}' else cast(det.h_household_composition as varchar(50)) end,
       base.CP_h_income_band_v2          = case
                                             when trim(lower(cast(det.h_income_band_v2 as varchar(50)))) in ('u', '', ' ') or det.h_income_band_v2 is null then '{Unknown}'
                                             when det.h_income_band_v2 = '0' then '01) <= £14999'
                                             when det.h_income_band_v2 = '1' then '02) £15,000 - £19,999'
                                             when det.h_income_band_v2 = '2' then '03) £20,000 - £29,999'
                                             when det.h_income_band_v2 = '3' then '04) £30,000 - £39,999'
                                             when det.h_income_band_v2 = '4' then '05) £40,000 - £49,999'
                                             when det.h_income_band_v2 = '5' then '06) £50,000 - £59,999'
                                             when det.h_income_band_v2 = '6' then '07) £60,000 - £69,999'
                                             when det.h_income_band_v2 = '7' then '08) £70,000 - £99,999'
                                             when det.h_income_band_v2 = '8' then '09) £100,000 - £149,999'
                                             when det.h_income_band_v2 = '9' then '10) £150,000+'
                                               else '{Unknown}'
                                        end,
       base.CP_h_income_value_v2 = case when trim(lower(cast(det.h_income_value_v2 as varchar(50)))) in ('u', '', ' ') or det.h_income_value_v2 is null then '{Unknown}' else cast(det.h_income_value_v2 as varchar(50)) end,
       base.CP_h_length_of_residency = case when trim(lower(cast(det.h_length_of_residency as varchar(50)))) in ('u', '', ' ') or det.h_length_of_residency is null then '{Unknown}' else cast(det.h_length_of_residency as varchar(50)) end,
       base.CP_h_length_of_residency_coarse = case when trim(lower(cast(det.h_length_of_residency_coarse as varchar(50)))) in ('u', '', ' ') or det.h_length_of_residency_coarse is null then '{Unknown}' else cast(det.h_length_of_residency_coarse as varchar(50)) end,
       base.CP_h_lifestage = case when trim(lower(cast(det.h_lifestage as varchar(50)))) in ('u', '', ' ') or det.h_lifestage is null then '{Unknown}' else cast(det.h_lifestage as varchar(50)) end,
       base.CP_h_mosaic_ni_group = case when trim(lower(cast(det.h_mosaic_ni_group as varchar(50)))) in ('u', '', ' ') or det.h_mosaic_ni_group is null then '{Unknown}' else cast(det.h_mosaic_ni_group as varchar(50)) end,
       base.CP_h_mosaic_ni_type = case when trim(lower(cast(det.h_mosaic_ni_type as varchar(50)))) in ('u', '', ' ') or det.h_mosaic_ni_type is null then '{Unknown}' else cast(det.h_mosaic_ni_type as varchar(50)) end,
       base.CP_h_mosaic_scotland_group = case when trim(lower(cast(det.h_mosaic_scotland_group as varchar(50)))) in ('u', '', ' ') or det.h_mosaic_scotland_group is null then '{Unknown}' else cast(det.h_mosaic_scotland_group as varchar(50)) end,
       base.CP_h_mosaic_scotland_segment_alternative = case when trim(lower(cast(det.h_mosaic_scotland_segment_alternative as varchar(50)))) in ('u', '', ' ') or det.h_mosaic_scotland_segment_alternative is null then '{Unknown}' else cast(det.h_mosaic_scotland_segment_alternative as varchar(50)) end,
       base.CP_h_mosaic_scotland_type = case when trim(lower(cast(det.h_mosaic_scotland_type as varchar(50)))) in ('u', '', ' ') or det.h_mosaic_scotland_type is null then '{Unknown}' else cast(det.h_mosaic_scotland_type as varchar(50)) end,
       base.CP_h_mosaic_uk_2003_group = case when trim(lower(cast(det.h_mosaic_uk_2003_group as varchar(50)))) in ('u', '', ' ') or det.h_mosaic_uk_2003_group is null then '{Unknown}' else cast(det.h_mosaic_uk_2003_group as varchar(50)) end,
       base.CP_h_mosaic_uk_2003_segment = case when trim(lower(cast(det.h_mosaic_uk_2003_segment as varchar(50)))) in ('u', '', ' ') or det.h_mosaic_uk_2003_segment is null then '{Unknown}' else cast(det.h_mosaic_uk_2003_segment as varchar(50)) end,
       base.CP_h_mosaic_uk_2003_segment_alternative = case when trim(lower(cast(det.h_mosaic_uk_2003_segment_alternative as varchar(50)))) in ('u', '', ' ') or det.h_mosaic_uk_2003_segment_alternative is null then '{Unknown}' else cast(det.h_mosaic_uk_2003_segment_alternative as varchar(50)) end,
       base.CP_h_mosaic_uk_2003_type = case when trim(lower(cast(det.h_mosaic_uk_2003_type as varchar(50)))) in ('u', '', ' ') or det.h_mosaic_uk_2003_type is null then '{Unknown}' else cast(det.h_mosaic_uk_2003_type as varchar(50)) end,
       base.CP_h_mosaic_uk_group = case when trim(lower(cast(det.h_mosaic_uk_group as varchar(50)))) in ('u', '', ' ') or det.h_mosaic_uk_group is null then '{Unknown}' else cast(det.h_mosaic_uk_group as varchar(50)) end,
       base.CP_h_mosaic_uk_second_best_type = case when trim(lower(cast(det.h_mosaic_uk_second_best_type as varchar(50)))) in ('u', '', ' ') or det.h_mosaic_uk_second_best_type is null then '{Unknown}' else cast(det.h_mosaic_uk_second_best_type as varchar(50)) end,
       base.CP_h_mosaic_uk_segment = case when trim(lower(cast(det.h_mosaic_uk_segment as varchar(50)))) in ('u', '', ' ') or det.h_mosaic_uk_segment is null then '{Unknown}' else cast(det.h_mosaic_uk_segment as varchar(50)) end,
       base.CP_h_mosaic_uk_segment_alternative = case when trim(lower(cast(det.h_mosaic_uk_segment_alternative as varchar(50)))) in ('u', '', ' ') or det.h_mosaic_uk_segment_alternative is null then '{Unknown}' else cast(det.h_mosaic_uk_segment_alternative as varchar(50)) end,
       base.CP_h_mosaic_uk_type = case when trim(lower(cast(det.h_mosaic_uk_type as varchar(50)))) in ('u', '', ' ') or det.h_mosaic_uk_type is null then '{Unknown}' else cast(det.h_mosaic_uk_type as varchar(50)) end,
       base.CP_h_mosaic_uk_type_affinity_percentile = case when trim(lower(cast(det.h_mosaic_uk_type_affinity_percentile as varchar(50)))) in ('u', '', ' ') or det.h_mosaic_uk_type_affinity_percentile is null then '{Unknown}' else cast(det.h_mosaic_uk_type_affinity_percentile as varchar(50)) end,
       base.CP_h_number_of_adults = case when trim(lower(cast(det.h_number_of_adults as varchar(50)))) in ('u', '', ' ') or det.h_number_of_adults is null then '{Unknown}' else cast(det.h_number_of_adults as varchar(50)) end,
       base.CP_h_number_of_bedrooms = case when trim(lower(cast(det.h_number_of_bedrooms as varchar(50)))) in ('u', '', ' ') or det.h_number_of_bedrooms is null then '{Unknown}' else cast(det.h_number_of_bedrooms as varchar(50)) end,
       base.CP_h_number_of_children_in_household_2011 = case when trim(lower(cast(det.h_number_of_children_in_household_2011 as varchar(50)))) in ('u', '', ' ') or det.h_number_of_children_in_household_2011 is null then '{Unknown}' else cast(det.h_number_of_children_in_household_2011 as varchar(50)) end,
       base.CP_h_outstanding_mortgage_v2 = case when trim(lower(cast(det.h_outstanding_mortgage_v2 as varchar(50)))) in ('u', '', ' ') or det.h_outstanding_mortgage_v2 is null then '{Unknown}' else cast(det.h_outstanding_mortgage_v2 as varchar(50)) end,
       base.CP_h_presence_of_child_aged_0_4_2011 = case when trim(lower(cast(det.h_presence_of_child_aged_0_4_2011 as varchar(50)))) in ('u', '', ' ') or det.h_presence_of_child_aged_0_4_2011 is null then '{Unknown}' else cast(det.h_presence_of_child_aged_0_4_2011 as varchar(50)) end,
       base.CP_h_presence_of_child_aged_12_17_2011 = case when trim(lower(cast(det.h_presence_of_child_aged_12_17_2011 as varchar(50)))) in ('u', '', ' ') or det.h_presence_of_child_aged_12_17_2011 is null then '{Unknown}' else cast(det.h_presence_of_child_aged_12_17_2011 as varchar(50)) end,
       base.CP_h_presence_of_child_aged_5_11_2011 = case when trim(lower(cast(det.h_presence_of_child_aged_5_11_2011 as varchar(50)))) in ('u', '', ' ') or det.h_presence_of_child_aged_5_11_2011 is null then '{Unknown}' else cast(det.h_presence_of_child_aged_5_11_2011 as varchar(50)) end,
       base.CP_h_presence_of_young_person_at_address = case when trim(lower(cast(det.h_presence_of_young_person_at_address as varchar(50)))) in ('u', '', ' ') or det.h_presence_of_young_person_at_address is null then '{Unknown}' else cast(det.h_presence_of_young_person_at_address as varchar(50)) end,
       base.CP_h_property_council_taxation = case when trim(lower(cast(det.h_property_council_taxation as varchar(50)))) in ('u', '', ' ') or det.h_property_council_taxation is null then '{Unknown}' else cast(det.h_property_council_taxation as varchar(50)) end,
       base.CP_h_property_type = case when trim(lower(cast(det.h_property_type as varchar(50)))) in ('u', '', ' ') or det.h_property_type is null then '{Unknown}' else cast(det.h_property_type as varchar(50)) end,
       base.CP_h_property_type_coarse = case when trim(lower(cast(det.h_property_type_coarse as varchar(50)))) in ('u', '', ' ') or det.h_property_type_coarse is null then '{Unknown}' else cast(det.h_property_type_coarse as varchar(50)) end,
       base.CP_h_property_type_v2 = case when trim(lower(cast(det.h_property_type_v2 as varchar(50)))) in ('u', '', ' ') or det.h_property_type_v2 is null then '{Unknown}' else cast(det.h_property_type_v2 as varchar(50)) end,
       base.CP_h_residence_type_v2 = case when trim(lower(cast(det.h_residence_type_v2 as varchar(50)))) in ('u', '', ' ') or det.h_residence_type_v2 is null then '{Unknown}' else cast(det.h_residence_type_v2 as varchar(50)) end,
       base.CP_h_shareholding_value = case when trim(lower(cast(det.h_shareholding_value as varchar(50)))) in ('u', '', ' ') or det.h_shareholding_value is null then '{Unknown}' else cast(det.h_shareholding_value as varchar(50)) end,
       base.CP_h_tenure = case when trim(lower(cast(det.h_tenure as varchar(50)))) in ('u', '', ' ') or det.h_tenure is null then '{Unknown}' else cast(det.h_tenure as varchar(50)) end,
       base.CP_h_tenure_v2 = case when trim(lower(cast(det.h_tenure_v2 as varchar(50)))) in ('u', '', ' ') or det.h_tenure_v2 is null then '{Unknown}' else cast(det.h_tenure_v2 as varchar(50)) end

   from EPL_Attr_Tmp_ConProp_Data det
 where base.cb_key_household *= det.cb_key_household;
commit;

/*
commit;

select 'h_affluence_v2' as Field, CP_h_affluence_v2 as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_age_coarse' as Field, CP_h_age_coarse as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_age_fine' as Field, CP_h_age_fine as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_ccjs_number' as Field, CP_h_ccjs_number as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_ccjs_value' as Field, CP_h_ccjs_value as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_council_tax_band_wales_2003' as Field, CP_h_council_tax_band_wales_2003 as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_equivalised_income_band' as Field, CP_h_equivalised_income_band as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all

select 'h_family_lifestage_2011' as Field, CP_h_family_lifestage_2011 as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_fss_factor_a_decile' as Field, CP_h_fss_factor_a_decile as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_fss_factor_a_percentile' as Field, CP_h_fss_factor_a_percentile as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_fss_factor_b_decile' as Field, CP_h_fss_factor_b_decile as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_fss_factor_b_percentile' as Field, CP_h_fss_factor_b_percentile as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_fss_factor_c_decile' as Field, CP_h_fss_factor_c_decile as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_fss_factor_c_percentile' as Field, CP_h_fss_factor_c_percentile as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_fss_factor_d_decile' as Field, CP_h_fss_factor_d_decile as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_fss_factor_d_percentile' as Field, CP_h_fss_factor_d_percentile as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_fss_factor_e_decile' as Field, CP_h_fss_factor_e_decile as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_fss_factor_e_percentile' as Field, CP_h_fss_factor_e_percentile as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_fss_factor_f_decile' as Field, CP_h_fss_factor_f_decile as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_fss_factor_f_percentile' as Field, CP_h_fss_factor_f_percentile as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_fss_group' as Field, CP_h_fss_group as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_fss_v3_group' as Field, CP_h_fss_v3_group as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_fss_v3_type' as Field, CP_h_fss_v3_type as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_household_composition' as Field, CP_h_household_composition as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_income_band_v2' as Field, CP_h_income_band_v2 as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all

select 'h_length_of_residency' as Field, CP_h_length_of_residency as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_length_of_residency_coarse' as Field, CP_h_length_of_residency_coarse as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_lifestage' as Field, CP_h_lifestage as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_mosaic_ni_group' as Field, CP_h_mosaic_ni_group as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_mosaic_ni_type' as Field, CP_h_mosaic_ni_type as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_mosaic_scotland_group' as Field, CP_h_mosaic_scotland_group as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_mosaic_scotland_segment_alternative' as Field, CP_h_mosaic_scotland_segment_alternative as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_mosaic_scotland_type' as Field, CP_h_mosaic_scotland_type as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_mosaic_uk_2003_group' as Field, CP_h_mosaic_uk_2003_group as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_mosaic_uk_2003_segment' as Field, CP_h_mosaic_uk_2003_segment as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_mosaic_uk_2003_segment_alternative' as Field, CP_h_mosaic_uk_2003_segment_alternative as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_mosaic_uk_2003_type' as Field, CP_h_mosaic_uk_2003_type as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_mosaic_uk_group' as Field, CP_h_mosaic_uk_group as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_mosaic_uk_second_best_type' as Field, CP_h_mosaic_uk_second_best_type as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_mosaic_uk_segment' as Field, CP_h_mosaic_uk_segment as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_mosaic_uk_segment_alternative' as Field, CP_h_mosaic_uk_segment_alternative as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_mosaic_uk_type' as Field, CP_h_mosaic_uk_type as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_mosaic_uk_type_affinity_percentile' as Field, CP_h_mosaic_uk_type_affinity_percentile as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_number_of_adults' as Field, CP_h_number_of_adults as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_number_of_bedrooms' as Field, CP_h_number_of_bedrooms as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_number_of_children_in_household_2011' as Field, CP_h_number_of_children_in_household_2011 as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_outstanding_mortgage_v2' as Field, CP_h_outstanding_mortgage_v2 as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_presence_of_child_aged_0_4_2011' as Field, CP_h_presence_of_child_aged_0_4_2011 as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_presence_of_child_aged_12_17_2011' as Field, CP_h_presence_of_child_aged_12_17_2011 as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_presence_of_child_aged_5_11_2011' as Field, CP_h_presence_of_child_aged_5_11_2011 as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_presence_of_young_person_at_address' as Field, CP_h_presence_of_young_person_at_address as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_property_council_taxation' as Field, CP_h_property_council_taxation as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_property_type' as Field, CP_h_property_type as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_property_type_coarse' as Field, CP_h_property_type_coarse as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_property_type_v2' as Field, CP_h_property_type_v2 as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_residence_type_v2' as Field, CP_h_residence_type_v2 as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_shareholding_value' as Field, CP_h_shareholding_value as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_tenure' as Field, CP_h_tenure as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value union all
select 'h_tenure_v2' as Field, CP_h_tenure_v2 as Value, count(*) as Cnt from EPL_70_Attribution_Base_Table group by Field, Value

order by 1, 2
*/



  -- ##############################################################################################################
  -- ##############################################################################################################
commit;
select *
  from EPL_70_Attribution_Base_Table
 where Sample_Type_1 = 'Test';
output to 'D:\Temp\SBE\Modelling_Data_All_TEST.csv' format ASCII ;

select *
  from EPL_70_Attribution_Base_Table
 where Sample_Type_1 = 'Training';
output to 'D:\Temp\SBE\Modelling_Data_All_TRAINING.csv' format ASCII ;

select *
  from EPL_70_Attribution_Base_Table
 where Sample_Type_1 = 'Validation';
output to 'D:\Temp\SBE\Modelling_Data_All_VALIDATION.csv' format ASCII ;



select *
  from EPL_70_Attribution_Base_Table
 where Sample_Type_2 = 'Test';
output to 'D:\Temp\SBE\Modelling_Data_Majority_TEST.csv' format ASCII ;

select *
  from EPL_70_Attribution_Base_Table
 where Sample_Type_2 = 'Training';
output to 'D:\Temp\SBE\Modelling_Data_Majority_TRAINING.csv' format ASCII ;

select *
  from EPL_70_Attribution_Base_Table
 where Sample_Type_2 = 'Validation';
output to 'D:\Temp\SBE\Modelling_Data_Majority_VALIDATION.csv' format ASCII ;






  -- ##############################################################################################################
  -- ##### Scoring                                                                                            #####
  -- ##############################################################################################################
  -- CHURN
if object_id('EPL_75_Attribution_Scores_All_Churn') is not null then drop table EPL_75_Attribution_Scores_All_Churn end if;
create table EPL_75_Attribution_Scores_All_Churn (
    Pk_Identifier                           bigint            identity,
    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name(),
    Account_Number                          varchar(20)       null      default null,
    Target_1_Score                          real              null      default null
);
create unique hg   index idx01 on EPL_75_Attribution_Scores_All_Churn(Account_Number);

load table EPL_75_Attribution_Scores_All_Churn (
    Account_Number',',
    Target_1_Score'\n'
)
FROM '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Sebastian/Scores - All CHURN.csv'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
DELIMITED BY ','
;
commit;

select * from EPL_75_Attribution_Scores_All_Churn;


select
      Model_Decile,
      sum(Resp_Flag) as Respondents,
      count(*) as Cnt
  from (select
              a.Account_Number,
              b.Target_1,
              case when b.Target_1 <> 'No change' then 1 else 0 end as Resp_Flag,
              a.Target_1_Score,
              cast(a.Target_1_Score * 100 as bigint) as Score,
              ntile(10) OVER (order by a.Target_1_Score) AS Model_Decile
          from EPL_75_Attribution_Scores_All_Churn a,
               EPL_70_Attribution_Base_Table b
         where a.Account_Number = b.Account_Number
           and b.Target_1 in ('Churn risk', 'No change')) det
 group by Model_Decile;



  -- DOWNGRADE
if object_id('EPL_75_Attribution_Scores_All_Downgrade') is not null then drop table EPL_75_Attribution_Scores_All_Downgrade end if;
create table EPL_75_Attribution_Scores_All_Downgrade (
    Pk_Identifier                           bigint            identity,
    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name(),
    Account_Number                          varchar(20)       null      default null,
    Target_1_Score                          real              null      default null
);
create unique hg   index idx01 on EPL_75_Attribution_Scores_All_Downgrade(Account_Number);

load table EPL_75_Attribution_Scores_All_Downgrade (
    Account_Number',',
    Target_1_Score'\n'
)
FROM '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Sebastian/Scores - All DOWNGRADE.csv'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
DELIMITED BY ','
;
commit;

select * from EPL_75_Attribution_Scores_All_Downgrade;


select
      Model_Decile,
      sum(Resp_Flag) as Respondents,
      count(*) as Cnt
  from (select
              a.Account_Number,
              b.Target_1,
              case when b.Target_1 <> 'No change' then 1 else 0 end as Resp_Flag,
              a.Target_1_Score,
              cast(a.Target_1_Score * 100 as bigint) as Score,
              ntile(10) OVER (order by a.Target_1_Score) AS Model_Decile
          from EPL_75_Attribution_Scores_All_Downgrade a,
               EPL_70_Attribution_Base_Table b
         where a.Account_Number = b.Account_Number
           and b.Target_1 in ('Downgrade risk', 'No change')) det
 group by Model_Decile;










  -- DOWNGRADE
if object_id('EPL_75_Attribution_Scores_All_Downgrade_2') is not null then drop table EPL_75_Attribution_Scores_All_Downgrade_2 end if;
create table EPL_75_Attribution_Scores_All_Downgrade_2 (
    Pk_Identifier                           bigint            identity,
    Updated_On                              datetime          not null  default timestamp,
    Updated_By                              varchar(30)       not null  default user_name(),
    Account_Number                          varchar(20)       null      default null,
    Target_1_Outcome                        varchar(50)       null      default null,
    Target_1_Score                          real              null      default null
);
create unique hg   index idx01 on EPL_75_Attribution_Scores_All_Downgrade_2(Account_Number);

load table EPL_75_Attribution_Scores_All_Downgrade_2 (
    Account_Number',',
    """Target_1_Outcome"""',',
    Target_1_Score'\n'
)
FROM '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Sebastian/Scores - All DOWNGRADE 2.csv'
QUOTES OFF
ESCAPES OFF
NOTIFY 1000
DELIMITED BY ','
;
commit;

select * from EPL_75_Attribution_Scores_All_Downgrade_2;


select
      Model_Decile,
      sum(Resp_Flag) as Respondents,
      count(*) as Cnt
  from (select
              a.Account_Number,
              b.Target_1,
              case when b.Target_1 <> 'No change' then 1 else 0 end as Resp_Flag,
              a.Target_1_Score,
              cast(a.Target_1_Score * 100 as bigint) as Score,
              ntile(10) OVER (order by a.Target_1_Score) AS Model_Decile
          from EPL_75_Attribution_Scores_All_Downgrade_2 a,
               EPL_70_Attribution_Base_Table b
         where a.Account_Number = b.Account_Number
           and b.Target_1 in ('Downgrade risk', 'No change')) det
 group by Model_Decile;




