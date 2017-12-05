/*###############################################################################
# Created on:   05/11/2013
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  Account attributes for SkyGo analysis - (based on the Enablement process)
#
# List of steps:
#               STEP 0.1 - preparing environment
#               STEP 1.0 - creating universe
#               STEP 2.0 - getting package/product portfolio at the end of the period
#               STEP 3.0 - getting package/product spin-downs/downgrades
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#     - sk_prod.cust_subs_hist
#     - sk_prod.cust_single_account_view
#     - sk_prod.cust_entitlement_lookup
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 05/11/2013  SBE   Initial version (based on the Enablement process)
#
###############################################################################*/


  -- ##############################################################################################################
  -- ##### STEP 0.1 - preparing environment                                                                   #####
  -- ##############################################################################################################
-- if object_id('VAggrAnal_SkyGo_Account_Attributes') is not null then drop table VAggrAnal_SkyGo_Account_Attributes end if;
create table VAggrAnal_SkyGo_Account_Attributes (
      Id                                  bigint            null     identity,
      Account_Number                      varchar(20)                default null,
      Country                             varchar(10)       null     default null,
      Account_Type                        varchar(20)       null     default null,

        -- Package/product portfolio variables
      Ent_DTV_Sub                         bit               null     default 0,
      Ent_DTV_Pack_Ent                    bit               null     default 0,
      Ent_DTV_Pack_Ent_Extra              bit               null     default 0,
      Ent_DTV_Pack_Ent_Extra_Plus         bit               null     default 0,
      Ent_DTV_Prem_Sports                 bit               null     default 0,
      Ent_DTV_Prem_Movies                 bit               null     default 0,
      Ent_HD_Sub                          bit               null     default 0,
      Ent_TV3D_Sub                        bit               null     default 0,
      Ent_SkyGo_Extra_Sub                 bit               null     default 0,

        -- Package/product movement flags
      Movmt_DTV_Sub                       bit               null     default 0,
      Movmt_DTV_Pack_Ent                  bit               null     default 0,
      Movmt_DTV_Pack_Ent_Extra            bit               null     default 0,
      Movmt_DTV_Pack_Ent_Extra_Plus       bit               null     default 0,
      Movmt_DTV_Prem_Sports               bit               null     default 0,
      Movmt_DTV_Prem_Movies               bit               null     default 0,
      Movmt_HD_Sub                        bit               null     default 0,
      Movmt_TV3D_Sub                      bit               null     default 0,
      Movmt_SkyGo_Extra_Sub               bit               null     default 0,

      Updated_On                          datetime          not null default timestamp,
      Updated_By                          varchar(30)       not null default user_name()
);

create unique hg index idx01 on VAggrAnal_SkyGo_Account_Attributes(Account_Number);
grant select on VAggrAnal_SkyGo_Account_Attributes to vespa_group_low_security;


  -- ###############################################################################
  -- ##### Set up environment                                                  #####
  -- ###############################################################################
create variable @varStartDate date;
create variable @varEndDate date;

set @varStartDate = '2013-09-01';
set @varEndDate   = '2013-09-30';



-- ##############################################################################################################
-- ##### STEP 1.0 - creating universe                                                                       #####
-- ##############################################################################################################
truncate table bednaszs.VAggrAnal_SkyGo_Account_Attributes;

insert into VAggrAnal_SkyGo_Account_Attributes (Account_Number)
  select
        Account_Number
    from sk_prod.cust_subs_hist csh
   where csh.effective_from_dt <= @varEndDate
     and csh.effective_to_dt > @varEndDate
     and csh.subscription_sub_type = 'DTV Primary Viewing'
     and csh.status_code in ('AC', 'PC', 'AB')
 group by csh.Account_Number;
commit;


update VAggrAnal_SkyGo_Account_Attributes base
   set base.Country = sav.pty_country_code,
       base.Account_Type = sav.acct_type
  from sk_prod.cust_single_account_view sav
 where base.account_number = sav.account_number;
commit;



-- ##############################################################################################################
-- ##### STEP 2.0 - get package/product portfolio at the end of the period                                  #####
-- ##############################################################################################################
if object_id('VAggrAnal_tmp_Account_Portfolio_Snapshot') is not null then drop table VAggrAnal_tmp_Account_Portfolio_Snapshot end if;
select
      base.Account_Number,

        -- ##### DTV subscription #####
      max(case
            when csh.subscription_sub_type = 'DTV Primary Viewing' and csh.status_code in ('AC', 'PC', 'AB') then 1
              else 0
          end) as DTV_Sub,

        -- ##### Entertainment #####
      max(case
            when cel.mixes = 0                                                                then 1      -- Entertainment
            when cel.mixes = 1 and (cel.style_culture = 1 or cel.variety = 1)                 then 1      -- Entertainment
            when cel.mixes = 2 and (cel.style_culture + cel.variety = 2)                      then 1      -- Entertainment
              else 0
          end) as DTV_Pack_Ent,

        -- ##### Entertainment Extra #####
      max(case
            when cel.mixes = 0                                                                then 0      -- Entertainment
            when cel.mixes = 1 and (cel.style_culture = 1 or cel.variety = 1)                 then 0      -- Entertainment
            when cel.mixes = 2 and (cel.style_culture + cel.variety = 2)                      then 0      -- Entertainment
            when cel.product_sk in      ( 43672,43669,43670,43664,43667,43663,43668,43673,
                                          43677,43674,43676,43666,43665,43662,43671,43675)    then 0      -- Entertainment Extra Plus
            when cel.mixes > 0                                                                then 1      -- Entertainment Extra
              else 0
          end) as DTV_Pack_Ent_Extra,

        -- ##### Entertainment Extra Plus #####
      max(case
            when cel.mixes = 0                                                                then 0      -- Entertainment
            when cel.mixes = 1 and (cel.style_culture = 1 or cel.variety = 1)                 then 0      -- Entertainment
            when cel.mixes = 2 and (cel.style_culture + cel.variety = 2)                      then 0      -- Entertainment
            when cel.product_sk in      ( 43672,43669,43670,43664,43667,43663,43668,43673,
                                          43677,43674,43676,43666,43665,43662,43671,43675)    then 1      -- Entertainment Extra Plus
            when cel.mixes > 0                                                                then 0      -- Entertainment Extra
              else 0
          end) as DTV_Pack_Ent_Extra_Plus,

        -- ##### Sports premium #####
      max(case
            when cel.prem_sports > 0                                                          then 1
              else 0
          end) as Prem_Sports,

        -- ##### Movies premium #####
      max(case
            when cel.prem_movies > 0                                                          then 1
              else 0
          end) as Prem_Movies,

        -- ##### HD subscription #####
      max(case
            when csh.subscription_sub_type = 'DTV HD' and csh.status_code in ('AC', 'PC', 'AB') then 1
              else 0
          end) as HD_Sub,

        -- ##### 3D TV subscription #####
      max(case
            when csh.subscription_sub_type = '3DTV' and csh.status_code in ('AC', 'PC', 'AB') then 1
              else 0
          end) as TV3D_Sub,

        -- ##### SkyGo Extra subscription #####
      max(case
            when csh.subscription_sub_type = 'Sky Go Extra' and csh.status_code in ('AC', 'PC', 'AB') then 1
              else 0
          end) as SkyGo_Extra_Sub

  into VAggrAnal_tmp_Account_Portfolio_Snapshot
  from bednaszs.VAggrAnal_SkyGo_Account_Attributes base
          inner join sk_prod.cust_subs_hist csh             on base.Account_Number = csh.Account_Number
                                                           and csh.effective_from_dt <= @varEndDate
                                                           and csh.effective_to_dt > @varEndDate
          left join sk_prod.cust_entitlement_lookup as cel  on csh.current_short_description = cel.short_description
 group by base.Account_Number;
commit;

create unique hg index idx01 on VAggrAnal_tmp_Account_Portfolio_Snapshot(Account_Number);


  -- Append to the main table
update bednaszs.VAggrAnal_SkyGo_Account_Attributes base
   set base.Ent_DTV_Sub                   = det.DTV_Sub,
       base.Ent_DTV_Pack_Ent              = det.DTV_Pack_Ent,
       base.Ent_DTV_Pack_Ent_Extra        = det.DTV_Pack_Ent_Extra,
       base.Ent_DTV_Pack_Ent_Extra_Plus   = det.DTV_Pack_Ent_Extra_Plus,
       base.Ent_DTV_Prem_Sports           = det.Prem_Sports,
       base.Ent_DTV_Prem_Movies           = det.Prem_Movies,
       base.Ent_HD_Sub                    = det.HD_Sub,
       base.Ent_TV3D_Sub                  = det.TV3D_Sub,
       base.Ent_SkyGo_Extra_Sub           = det.SkyGo_Extra_Sub
  from VAggrAnal_tmp_Account_Portfolio_Snapshot det
 where base.Account_Number = det.Account_Number;
commit;



-- ##############################################################################################################
-- ##### STEP 3.0 - getting package/product spin-downs/downgrades                                           #####
-- ##############################################################################################################
  -- Get all active subscription Ids within the period, so further analysis is based at subscription level,
  -- subsequently aggregated to account level
if object_id('VAggrAnal_tmp_Subscription_Base') is not null then drop table VAggrAnal_tmp_Subscription_Base end if;
select
      csh.Subscription_Id,
      cast(0 as bit) as Dummy                                                                               -- Sybase does not like single-column permanent tables
  into VAggrAnal_tmp_Subscription_Base
  from bednaszs.VAggrAnal_SkyGo_Account_Attributes base,
       sk_prod.cust_subs_hist csh
 where base.Account_Number = csh.Account_Number
   and csh.effective_from_dt <= @varEndDate                                                                -- Active at any point within the period
   and csh.effective_to_dt > @varStartDate
   and csh.effective_from_dt < csh.effective_to_dt                                                          -- Must remain active for 1 day at least
   and csh.status_code in ('AC', 'PC', 'AB')
   and (                                                                                                    -- Only relevant subscriptions to speed up later on
        csh.subscription_sub_type = 'DTV Primary Viewing' or
        csh.subscription_sub_type = 'DTV HD' or
        csh.subscription_sub_type = '3DTV' or
        csh.subscription_sub_type = 'Sky Go Extra'
       )
 group by csh.Subscription_Id;
commit;

create unique hg index idx01 on VAggrAnal_tmp_Subscription_Base(Subscription_Id);


  -- Now calculate active/inactive flag for each record for each package/sub within the period
if object_id('VAggrAnal_tmp_Subscription_History') is not null then drop table VAggrAnal_tmp_Subscription_History end if;
select
      csh.Subscription_Id,
      csh.subscription_sub_type,

        -- ##### DTV #####
      case
        when csh.status_code in ('AC', 'PC', 'AB') then 1
          else 0
      end as Act_Sub,

        -- ##### Entertainment #####
      case
        when cel.mixes = 0                                                                then 1      -- Entertainment
        when cel.mixes = 1 and (cel.style_culture = 1 or cel.variety = 1)                 then 1      -- Entertainment
        when cel.mixes = 2 and (cel.style_culture + cel.variety = 2)                      then 1      -- Entertainment
          else 0
      end as Act_DTV_Pack_Ent,

        -- ##### Entertainment Extra #####
      case
        when cel.mixes = 0                                                                then 0      -- Entertainment
        when cel.mixes = 1 and (cel.style_culture = 1 or cel.variety = 1)                 then 0      -- Entertainment
        when cel.mixes = 2 and (cel.style_culture + cel.variety = 2)                      then 0      -- Entertainment
        when cel.product_sk in      ( 43672,43669,43670,43664,43667,43663,43668,43673,
                                      43677,43674,43676,43666,43665,43662,43671,43675)    then 0      -- Entertainment Extra Plus
        when cel.mixes > 0                                                                then 1      -- Entertainment Extra
          else 0
      end as Act_DTV_Pack_Ent_Ext,

        -- ##### Entertainment Extra Plus #####
      case
        when cel.mixes = 0                                                                then 0      -- Entertainment
        when cel.mixes = 1 and (cel.style_culture = 1 or cel.variety = 1)                 then 0      -- Entertainment
        when cel.mixes = 2 and (cel.style_culture + cel.variety = 2)                      then 0      -- Entertainment
        when cel.product_sk in      ( 43672,43669,43670,43664,43667,43663,43668,43673,
                                      43677,43674,43676,43666,43665,43662,43671,43675)    then 1      -- Entertainment Extra Plus
        when cel.mixes > 0                                                                then 0      -- Entertainment Extra
          else 0
      end as Act_DTV_Pack_Ent_Ext_Plus,

        -- ##### Sports premium #####
      case
        when cel.prem_sports > 0                                                          then 1
          else 0
      end as Act_Prem_Sports,

        -- ##### Movies premium #####
      case
        when cel.prem_movies > 0                                                          then 1
          else 0
      end as Act_Prem_Movies

  into VAggrAnal_tmp_Subscription_History
  from VAggrAnal_tmp_Subscription_Base base,
       sk_prod.cust_subs_hist csh,
       sk_prod.cust_entitlement_lookup cel
 where base.Subscription_Id = csh.Subscription_Id
   and csh.effective_from_dt <= @varEndDate                                                                -- Active at any point within the period
   and csh.effective_to_dt > @varStartDate
   and csh.effective_from_dt < csh.effective_to_dt                                                          -- Status must last at least 1 day
   and csh.current_short_description = cel.short_description;
commit;

create        hg index idx01 on VAggrAnal_tmp_Subscription_History(Subscription_Id);
create        lf index idx02 on VAggrAnal_tmp_Subscription_History(subscription_sub_type);


  -- Summarise by subscription Id and count how many records have relevant package active and how many records there are overall per subscription
if object_id('VAggrAnal_tmp_Subscription_Summary') is not null then drop table VAggrAnal_tmp_Subscription_Summary end if;
select
      Subscription_Id,

      sum(case when subscription_sub_type = 'DTV Primary Viewing' then Act_Sub else 0 end)                        as Act_DTV_Sub_Recs,
      sum(case when subscription_sub_type = 'DTV Primary Viewing' then Act_DTV_Pack_Ent else 0 end)               as Act_DTV_Pack_Ent_Recs,
      sum(case when subscription_sub_type = 'DTV Primary Viewing' then Act_DTV_Pack_Ent_Ext else 0 end)           as Act_DTV_Pack_Ent_Ext_Recs,
      sum(case when subscription_sub_type = 'DTV Primary Viewing' then Act_DTV_Pack_Ent_Ext_Plus else 0 end)      as Act_DTV_Pack_Ent_Ext_Plus_Recs,
      sum(case when subscription_sub_type = 'DTV Primary Viewing' then Act_Prem_Sports else 0 end)                as Act_Prem_Sports_Recs,
      sum(case when subscription_sub_type = 'DTV Primary Viewing' then Act_Prem_Movies else 0 end)                as Act_Prem_Movies_Recs,
      sum(case when subscription_sub_type = 'DTV HD' then Act_Sub else 0 end)                                     as Act_HD_Sub_Recs,
      sum(case when subscription_sub_type = '3DTV' then Act_Sub else 0 end)                                       as Act_TV3D_Sub_Recs,
      sum(case when subscription_sub_type = 'Sky Go Extra' then Act_Sub else 0 end)                               as Act_SkyGo_Extra_Sub_Recs,

      sum(case when subscription_sub_type = 'DTV Primary Viewing' then 1 else 0 end)                              as All_DTV_Sub_Recs,
      sum(case when subscription_sub_type = 'DTV HD' then 1 else 0 end)                                           as All_HD_Sub_Recs,
      sum(case when subscription_sub_type = '3DTV' then 1 else 0 end)                                             as All_TV3D_Sub_Recs,
      sum(case when subscription_sub_type = 'Sky Go Extra' then 1 else 0 end)                                     as All_SkyGo_Extra_Sub_Recs

  into VAggrAnal_tmp_Subscription_Summary
  from VAggrAnal_tmp_Subscription_History
 group by Subscription_Id;
commit;

create unique hg index idx01 on VAggrAnal_tmp_Subscription_Summary(Subscription_Id);


  -- Account Number <=> Subscription ID lookup is also required
if object_id('VAggrAnal_tmp_Account_Subscription_Lookup') is not null then drop table VAggrAnal_tmp_Account_Subscription_Lookup end if;
select
      csh.Account_Number,
      base.Subscription_Id
  into VAggrAnal_tmp_Account_Subscription_Lookup
  from VAggrAnal_tmp_Subscription_Base base,
       sk_prod.cust_subs_hist csh
 where base.Subscription_Id = csh.Subscription_Id
 group by csh.Account_Number, base.Subscription_Id;
commit;

create        hg index idx01 on VAggrAnal_tmp_Account_Subscription_Lookup(Account_Number);
create        hg index idx02 on VAggrAnal_tmp_Account_Subscription_Lookup(Subscription_Id);


  -- Append to the main table
  -- The rule is that when number of "active" records for each package is lower than total number of records for that package/sub, then
  -- one can conclude there was a "movement". However, when there are multiple subscriptions per account, at least one must remain active for the
  -- entire period for the account to qualify
update bednaszs.VAggrAnal_SkyGo_Account_Attributes base
   set base.Movmt_DTV_Sub                   = det.DTV_Sub,
       base.Movmt_DTV_Pack_Ent              = det.DTV_Pack_Ent,
       base.Movmt_DTV_Pack_Ent_Extra        = det.DTV_Pack_Ent_Extra,
       base.Movmt_DTV_Pack_Ent_Extra_Plus   = det.DTV_Pack_Ent_Extra_Plus,
       base.Movmt_DTV_Prem_Sports           = det.DTV_Prem_Sports,
       base.Movmt_DTV_Prem_Movies           = det.DTV_Prem_Movies,
       base.Movmt_HD_Sub                    = det.HD_Sub,
       base.Movmt_TV3D_Sub                  = det.TV3D_Sub,
       base.Movmt_SkyGo_Extra_Sub           = det.SkyGo_Extra_Sub
  from (select
              acc.Account_Number,
              min(case when Act_DTV_Sub_Recs                < All_DTV_Sub_Recs          then 1 else 0 end) as DTV_Sub,
              min(case when Act_DTV_Pack_Ent_Recs           < All_DTV_Sub_Recs          then 1 else 0 end) as DTV_Pack_Ent,
              min(case when Act_DTV_Pack_Ent_Ext_Recs       < All_DTV_Sub_Recs          then 1 else 0 end) as DTV_Pack_Ent_Extra,
              min(case when Act_DTV_Pack_Ent_Ext_Plus_Recs  < All_DTV_Sub_Recs          then 1 else 0 end) as DTV_Pack_Ent_Extra_Plus,
              min(case when Act_Prem_Sports_Recs            < All_DTV_Sub_Recs          then 1 else 0 end) as DTV_Prem_Sports,
              min(case when Act_Prem_Movies_Recs            < All_DTV_Sub_Recs          then 1 else 0 end) as DTV_Prem_Movies,
              min(case when Act_HD_Sub_Recs                 < All_HD_Sub_Recs           then 1 else 0 end) as HD_Sub,
              min(case when Act_TV3D_Sub_Recs               < All_TV3D_Sub_Recs         then 1 else 0 end) as TV3D_Sub,
              min(case when Act_SkyGo_Extra_Sub_Recs        < All_SkyGo_Extra_Sub_Recs  then 1 else 0 end) as SkyGo_Extra_Sub
          from VAggrAnal_tmp_Account_Subscription_Lookup acc,
               VAggrAnal_tmp_Subscription_Summary sub
         where acc.Subscription_Id = sub.Subscription_Id
         group by acc.Account_Number) det
 where base.Account_Number = det.Account_Number;
commit;


  -- Now set to null movement fields for not relevant subscriptions to avoid confusion
update bednaszs.VAggrAnal_SkyGo_Account_Attributes base
   set base.Movmt_DTV_Sub                   = case when det.DTV_Sub                   = 0 then null else base.Movmt_DTV_Sub                 end,
       base.Movmt_DTV_Pack_Ent              = case when det.DTV_Pack_Ent              = 0 then null else base.Movmt_DTV_Pack_Ent            end,
       base.Movmt_DTV_Pack_Ent_Extra        = case when det.DTV_Pack_Ent_Extra        = 0 then null else base.Movmt_DTV_Pack_Ent_Extra      end,
       base.Movmt_DTV_Pack_Ent_Extra_Plus   = case when det.DTV_Pack_Ent_Extra_Plus   = 0 then null else base.Movmt_DTV_Pack_Ent_Extra_Plus end,
       base.Movmt_DTV_Prem_Sports           = case when det.Prem_Sports               = 0 then null else base.Movmt_DTV_Prem_Sports         end,
       base.Movmt_DTV_Prem_Movies           = case when det.Prem_Movies               = 0 then null else base.Movmt_DTV_Prem_Movies         end,
       base.Movmt_HD_Sub                    = case when det.HD_Sub                    = 0 then null else base.Movmt_HD_Sub                  end,
       base.Movmt_TV3D_Sub                  = case when det.TV3D_Sub                  = 0 then null else base.Movmt_TV3D_Sub                end,
       base.Movmt_SkyGo_Extra_Sub           = case when det.SkyGo_Extra_Sub           = 0 then null else base.Movmt_SkyGo_Extra_Sub         end
  from VAggrAnal_tmp_Account_Portfolio_Snapshot det
 where base.Account_Number = det.Account_Number;
commit;



  -- ##############################################################################################################








