/*###############################################################################
# Created on:   25/11/2013
# Created by:   Sebastian Bedanszynski (SBE)
# Description:  Opinion formers - account attributes
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 25/11/2013  SBE   Initial version
#
###############################################################################*/





create variable @varStartDate                   date;
create variable @varEndDate                     date;

set @varStartDate = '2013-09-16';
set @varEndDate   = '2013-10-13';



-- ##############################################################################################################
-- ##### STEP 1.0 - creating universe                                                                       #####
-- ##############################################################################################################
  -- Get weights for the period and calculate median weight
if object_id('OpForm_tmp_Account_Weights') is not null then drop table OpForm_tmp_Account_Weights end if;
select
      Account_Number,
      Cb_Key_Household,
      Scaling_Date,
      Scaling_Weight,
      rank() over (partition by Account_Number order by Scaling_Weight, Scaling_Date) as Weight_Rank,
      cast(0 as smallint) as Days_Data_Returned,
      cast(0 as bit) as Median_Scaling_Weight_Flag
  into OpForm_tmp_Account_Weights
  from (select
              itv.Account_Number,
              cast(null as bigint) as Cb_Key_Household,
              wgh.Scaling_Day as Scaling_Date,
              max(wgh.Weighting) as Scaling_Weight
          from VESPA_Analysts.SC2_Intervals itv,
               VESPA_Analysts.SC2_Weightings wgh
         where itv.Scaling_Segment_Id = wgh.Scaling_Segment_Id
           and itv.Reporting_Starts <= wgh.Scaling_Day
           and itv.Reporting_Ends >= wgh.Scaling_Day
           and wgh.Scaling_Day between @varStartDate and @varEndDate
         group by itv.Account_Number, wgh.Scaling_Day) det;
commit;

create        hg index idx01 on OpForm_tmp_Account_Weights(Account_Number);
create        hg index idx02 on OpForm_tmp_Account_Weights(Cb_Key_Household);
create      date index idx03 on OpForm_tmp_Account_Weights(Scaling_Date);

update OpForm_tmp_Account_Weights base
   set base.Cb_Key_Household = det.Cb_Key_Household
  from sk_prod.Cust_Single_Account_View det
 where base.Account_Number = det.Account_Number;
commit;


update OpForm_tmp_Account_Weights base
   set base.Days_Data_Returned  = det.Days_Data_Returned,
       base.Median_Scaling_Weight_Flag   = case                                                                      -- Flag mid-record, if even number take the "lower" of the two competing
                                              when base.Weight_Rank = ceil(1.0 * det.Days_Data_Returned / 2) then 1
                                                else 0
                                            end
  from (select
              Account_Number,
              count(*) as Days_Data_Returned
          from OpForm_tmp_Account_Weights
         group by Account_Number) det
 where base.Account_Number = det.Account_Number;
commit;


if object_id('OpForm_01_Account_Attributes') is not null then drop table OpForm_01_Account_Attributes end if;
create table OpForm_01_Account_Attributes (
      Id                                  bigint            identity,
      Account_Number                      varchar(20)                default null,
      Cb_Key_Household                    bigint            null     default null,
      Panel_Id                            tinyint           not null default 0,

      Median_Scaling_Weight               decimal(15, 6)    null     default 0,
      Median_Scaling_Weight_Date          date              null     default null,
      Scaling_Weight                      decimal(15, 6)    null     default 0,
      Scaling_Weight_Date                 date              null     default null,

      Days_Data_Returned                  smallint          null     default 0,
      Days_Period                         smallint          null     default 0,

        -- Package/product portfolio variables
      Ent_DTV_Sub                         bit               null     default 0,
      Movmt_DTV_Sub                       bit               null     default 0,

        -- Demographics
      HH_Composition                      varchar(50)       null     default 'z) Unknown',
      HH_Lifestage                        varchar(50)       null     default 'z) Unknown',
      Mirror_ABC1                         varchar(50)       null     default 'Unknown',
      Kids_Age_le4                        varchar(50)       null     default 'Unknown',
      Kids_Age_4to9                       varchar(50)       null     default 'Unknown',
      Kids_Age_10to15                     varchar(50)       null     default 'Unknown',
      Mosaic                              varchar(50)       null     default 'Unknown',
      H_Affluence                         varchar(50)       null     default 'Unknown',
      Region                              varchar(50)       null     default 'Unknown',

      Updated_On                          datetime          not null default timestamp,
      Updated_By                          varchar(30)       not null default user_name()
);

create unique hg index idx01 on OpForm_01_Account_Attributes(Account_Number);
create        hg index idx02 on OpForm_01_Account_Attributes(Cb_Key_Household);
grant select on OpForm_01_Account_Attributes to vespa_group_low_security;


  -- Get list of those who will be included in the aggregation
insert into OpForm_01_Account_Attributes
       (Account_Number, Cb_Key_Household, Panel_Id, Median_Scaling_Weight,
        Median_Scaling_Weight_Date, Days_Data_Returned, Days_Period)
  select
        Account_Number,
        Cb_Key_Household,
        12,
        Scaling_Weight,
        Scaling_Date,
        Days_Data_Returned,
        datediff(day, cast(@varStartDate as date), cast(@varEndDate as date)) + 1
    from OpForm_tmp_Account_Weights
   where Median_Scaling_Weight_Flag = 1;
commit;


update OpForm_01_Account_Attributes base
   set base.Scaling_Weight       = sc.calculated_scaling_weight,
       base.Scaling_Weight_Date  = '2013-09-22'
  from (select
              itv.Account_Number,
              wgh.Scaling_Day,
              max(wgh.Weighting) as calculated_scaling_weight
          from VESPA_Analysts.SC2_Intervals itv,
               VESPA_Analysts.SC2_Weightings wgh
         where itv.Scaling_Segment_Id = wgh.Scaling_Segment_Id
           and itv.Reporting_Starts <= wgh.Scaling_Day
           and itv.Reporting_Ends >= wgh.Scaling_Day
           and wgh.Scaling_Day = '2013-09-22'
         group by itv.Account_Number, wgh.Scaling_Day) sc
 where base.Account_Number = sc.Account_Number;
commit;


-- ##############################################################################################################
-- ##### STEP 2.0 - get package/product portfolio at the end of the period                                  #####
-- ##############################################################################################################
if object_id('OpForm_tmp_Account_Portfolio_Snapshot') is not null then drop table OpForm_tmp_Account_Portfolio_Snapshot end if;
select
      base.Account_Number,

        -- ##### DTV subscription #####
      max(case
            when csh.subscription_sub_type = 'DTV Primary Viewing' and csh.status_code in ('AC', 'PC', 'AB') then 1
              else 0
          end) as DTV_Sub

  into OpForm_tmp_Account_Portfolio_Snapshot
  from OpForm_01_Account_Attributes base
          inner join sk_prod.cust_subs_hist csh             on base.Account_Number = csh.Account_Number
                                                           and csh.effective_from_dt <= @varEndDate
                                                           and csh.effective_to_dt > @varEndDate
 group by base.Account_Number;
commit;

create unique hg index idx01 on OpForm_tmp_Account_Portfolio_Snapshot(Account_Number);


  -- Append to the main table
update OpForm_01_Account_Attributes base
   set base.Ent_DTV_Sub                   = det.DTV_Sub
  from OpForm_tmp_Account_Portfolio_Snapshot det
 where base.Account_Number = det.Account_Number;
commit;


-- ##############################################################################################################
-- ##### STEP 3.0 - getting package/product spin-downs/downgrades                                           #####
-- ##############################################################################################################
  -- Get all active subscription Ids within the period, so further analysis is based at subscription level,
  -- subsequently aggregated to account level
if object_id('OpForm_tmp_Subscription_Base') is not null then drop table OpForm_tmp_Subscription_Base end if;
select
      csh.Subscription_Id,
      cast(0 as bit) as Dummy                                                                               -- Sybase does not like single-column permanent tables
  into OpForm_tmp_Subscription_Base
  from OpForm_01_Account_Attributes base,
       sk_prod.cust_subs_hist csh
 where base.Account_Number = csh.Account_Number
   and csh.effective_from_dt <= @varEndDate                                                                -- Active at any point within the period
   and csh.effective_to_dt > @varStartDate
   and csh.effective_from_dt < csh.effective_to_dt                                                          -- Must remain active for 1 day at least
   and csh.status_code in ('AC', 'PC', 'AB')
   and csh.subscription_sub_type = 'DTV Primary Viewing'
 group by csh.Subscription_Id;
commit;

create unique hg index idx01 on OpForm_tmp_Subscription_Base(Subscription_Id);


  -- Now calculate active/inactive flag for each record for each package/sub within the period
if object_id('OpForm_tmp_Subscription_History') is not null then drop table OpForm_tmp_Subscription_History end if;
select
      csh.Subscription_Id,
      csh.subscription_sub_type,

        -- ##### DTV #####
      case
        when csh.status_code in ('AC', 'PC', 'AB') then 1
          else 0
      end as Act_Sub

  into OpForm_tmp_Subscription_History
  from OpForm_tmp_Subscription_Base base,
       sk_prod.cust_subs_hist csh,
       sk_prod.cust_entitlement_lookup cel
 where base.Subscription_Id = csh.Subscription_Id
   and csh.effective_from_dt <= @varEndDate                                                                -- Active at any point within the period
   and csh.effective_to_dt > @varStartDate
   and csh.effective_from_dt < csh.effective_to_dt                                                          -- Status must last at least 1 day
   and csh.current_short_description = cel.short_description;
commit;

create        hg index idx01 on OpForm_tmp_Subscription_History(Subscription_Id);
create        lf index idx02 on OpForm_tmp_Subscription_History(subscription_sub_type);


  -- Summarise by subscription Id and count how many records have relevant package active and how many records there are overall per subscription
if object_id('OpForm_tmp_Subscription_Summary') is not null then drop table OpForm_tmp_Subscription_Summary end if;
select
      Subscription_Id,
      sum(case when subscription_sub_type = 'DTV Primary Viewing' then Act_Sub else 0 end)                        as Act_DTV_Sub_Recs,
      sum(case when subscription_sub_type = 'DTV Primary Viewing' then 1 else 0 end)                              as All_DTV_Sub_Recs

  into OpForm_tmp_Subscription_Summary
  from OpForm_tmp_Subscription_History
 group by Subscription_Id;
commit;

create unique hg index idx01 on OpForm_tmp_Subscription_Summary(Subscription_Id);

  -- Account Number <=> Subscription ID lookup is also required
if object_id('OpForm_tmp_Account_Subscription_Lookup') is not null then drop table OpForm_tmp_Account_Subscription_Lookup end if;
select
      csh.Account_Number,
      base.Subscription_Id
  into OpForm_tmp_Account_Subscription_Lookup
  from OpForm_tmp_Subscription_Base base,
       sk_prod.cust_subs_hist csh
 where base.Subscription_Id = csh.Subscription_Id
 group by csh.Account_Number, base.Subscription_Id;
commit;

create        hg index idx01 on OpForm_tmp_Account_Subscription_Lookup(Account_Number);
create        hg index idx02 on OpForm_tmp_Account_Subscription_Lookup(Subscription_Id);


  -- Append to the main table
  -- The rule is that when number of "active" records for each package is lower than total number of records for that package/sub, then
  -- one can conclude there was a "movement". However, when there are multiple subscriptions per account, at least one must remain active for the
  -- entire period for the account to qualify
update OpForm_01_Account_Attributes base
   set base.Movmt_DTV_Sub                   = det.DTV_Sub
  from (select
              acc.Account_Number,
              min(case when Act_DTV_Sub_Recs                < All_DTV_Sub_Recs        then 1 else 0 end) as DTV_Sub
          from OpForm_tmp_Account_Subscription_Lookup acc,
               OpForm_tmp_Subscription_Summary sub
         where acc.Subscription_Id = sub.Subscription_Id
         group by acc.Account_Number) det
 where base.Account_Number = det.Account_Number;
commit;


  -- Now set to null movement fields for not relevant subscriptions to avoid confusion
update OpForm_01_Account_Attributes base
   set base.Movmt_DTV_Sub                   = case when det.DTV_Sub                   = 0 then null else base.Movmt_DTV_Sub                 end
  from OpForm_tmp_Account_Portfolio_Snapshot det
 where base.Account_Number = det.Account_Number;
commit;


-- ##############################################################################################################
-- ##### STEP 4.0 - append demographic information                                                          #####
-- ##############################################################################################################
if object_id('OpForm_tmp_HH_Comp') is not null then drop table OpForm_tmp_HH_Comp end if;
SELECT
    cv.cb_key_household,
    cv.cb_key_family,
    cv.cb_key_individual,
    min(cv.cb_row_id) as cb_row_id,
    max(cv.h_household_composition) as h_household_composition,
    max(h_lifestage) as h_lifestage,
    max(pp.p_head_of_household) as p_head_of_household
INTO OpForm_tmp_HH_Comp
FROM sk_prod.EXPERIAN_CONSUMERVIEW cv,
     sk_prod.PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD pp
WHERE cv.exp_cb_key_db_individual = pp.exp_cb_key_db_individual
 AND cv.cb_key_individual is not null
GROUP BY cv.cb_key_household, cv.cb_key_family, cv.cb_key_individual;
COMMIT;

CREATE LF INDEX idx1 on OpForm_tmp_HH_Comp(p_head_of_household);
CREATE HG INDEX idx2 on OpForm_tmp_HH_Comp(cb_key_family);
CREATE HG INDEX idx3 on OpForm_tmp_HH_Comp(cb_key_individual);


if object_id('OpForm_tmp_HH_Comp_Head_Rank') is not null then drop table OpForm_tmp_HH_Comp_Head_Rank end if;
SELECT  cb_key_household
       ,cb_row_id
       ,rank() over(partition by cb_key_family     ORDER BY p_head_of_household desc,  cb_row_id desc) as rank_fam
       ,rank() over(partition by cb_key_individual ORDER BY p_head_of_household desc,  cb_row_id desc) as rank_ind
       ,h_household_composition -- may as well pull out the item we need given we're ranking and deleting
       ,h_lifestage
INTO OpForm_tmp_HH_Comp_Head_Rank
FROM OpForm_tmp_HH_Comp
WHERE cb_key_individual IS not NULL
 AND cb_key_individual <> 0;
commit;

DELETE FROM OpForm_tmp_HH_Comp_Head_Rank WHERE rank_fam <> 1 AND rank_ind <> 1;
commit;

CREATE INDEX index_ac on OpForm_tmp_HH_Comp_Head_Rank (cb_key_household);
COMMIT;

update OpForm_01_Account_Attributes base
   set base.HH_Composition = case det.h_household_composition
                               when '00' THEN 'a) Families'
                               when '01' THEN 'b) Extended family'
                               when '02' THEN 'c) Extended household'
                               when '03' THEN 'd) Pseudo family'
                               when '04' THEN 'e) Single male'
                               when '05' THEN 'f) Single female'
                               when '06' THEN 'g) Male homesharers'
                               when '07' THEN 'h) Female homesharers'
                               when '08' THEN 'i) Mixed homesharers'
                               when '09' THEN 'j) Abbreviated male families'
                               when '10' THEN 'k) Abbreviated female families'
                               when '11' THEN 'l) Multi-occupancy dwelling'
                                 else 'z) Unknown'
                             end,
      base.HH_Lifestage     = CASE h_lifestage
                                WHEN '00'  THEN 'a) Very young family'
                                WHEN '01'  THEN 'b) Very young single'
                                WHEN '02'  THEN 'c) Very young homesharers'
                                WHEN '03'  THEN 'd) Young family'
                                WHEN '04'  THEN 'e) Young single'
                                WHEN '05'  THEN 'f) Young homesharers'
                                WHEN '06'  THEN 'g) Mature family'
                                WHEN '07'  THEN 'h) Mature singles'
                                WHEN '08'  THEN 'i) Mature homesharers'
                                WHEN '09'  THEN 'j) Older family'
                                WHEN '10'  THEN 'k) Older single'
                                WHEN '11'  THEN 'l) Older homesharers'
                                WHEN '12'  THEN 'm) Elderly family'
                                WHEN '13'  THEN 'n) Elderly single'
                                WHEN '14'  THEN 'o) Elderly homesharers'
                                  ELSE            'z) Unknown'
                              END
  from OpForm_tmp_HH_Comp_Head_Rank det
 where base.cb_key_household = det.cb_key_household
   and rank_fam = 1;
commit;


update OpForm_01_Account_Attributes base
   set base.Mirror_ABC1         = case
                                    when det.Mirror_ABC1 is null then 'Unknown'
                                    when upper(det.Mirror_ABC1) in ('U', 'UNCLASSIFIED', 'UNKNOWN') then 'Unknown'
                                      else det.Mirror_ABC1
                                  end,
       base.Kids_Age_le4        = case
                                    when det.Kids_Age_le4 is null then 'Unknown'
                                    when upper(det.Kids_Age_le4) in ('U', 'UNCLASSIFIED', 'UNKNOWN') then 'Unknown'
                                      else det.Kids_Age_le4
                                  end,
       base.Kids_Age_4to9       = case
                                    when det.Kids_Age_4to9 is null then 'Unknown'
                                    when upper(det.Kids_Age_4to9) in ('U', 'UNCLASSIFIED', 'UNKNOWN') then 'Unknown'
                                      else det.Kids_Age_4to9
                                  end,
       base.Kids_Age_10to15     = case
                                    when det.Kids_Age_10to15 is null then 'Unknown'
                                    when upper(det.Kids_Age_10to15) in ('U', 'UNCLASSIFIED', 'UNKNOWN') then 'Unknown'
                                      else det.Kids_Age_10to15
                                  end,
       base.Mosaic              = case
                                    when det.Demographic is null then 'Unknown'
                                    when upper(det.Demographic) in ('U', 'UNCLASSIFIED', 'UNKNOWN') then 'Unknown'
                                      else det.Demographic
                                  end,
       base.H_Affluence         = case
                                    when det.H_affluence is null then 'Unknown'
                                    when upper(det.H_affluence) in ('U', 'UNCLASSIFIED', 'UNKNOWN') then 'Unknown'
                                      else det.H_affluence
                                  end,
       base.Region              = case
                                    when det.Region is null then 'Unknown'
                                    when upper(det.Region) in ('U', 'UNCLASSIFIED', 'UNKNOWN') then 'Unknown'
                                      else det.Region
                                  end
  from sk_prod.adsmart det
 where base.account_number = det.account_number;
commit;


  -- Manual hack to align unknown value, should not be needed anymore
/*
select distinct H_Affluence from OpForm_01_Account_Attributes;
select distinct HH_Composition from OpForm_01_Account_Attributes;
select distinct HH_Lifestage from OpForm_01_Account_Attributes;
select distinct Region from OpForm_01_Account_Attributes;

update OpForm_01_Account_Attributes set H_Affluence = 'Unknown' where H_Affluence = 'z) Unknown'; commit;
update OpForm_01_Account_Attributes set HH_Composition = 'z) Unknown' where HH_Composition = 'Unknown'; commit;
update OpForm_01_Account_Attributes set HH_Lifestage = 'z) Unknown' where HH_Lifestage = 'Unknown'; commit;
update OpForm_01_Account_Attributes set Region = 'Unknown' where Region = 'z) Unknown'; commit;
*/


  -- ##############################################################################################################
  -- ##############################################################################################################








