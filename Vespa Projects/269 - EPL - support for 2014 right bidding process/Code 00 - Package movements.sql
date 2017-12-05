/*###############################################################################
# Created on:   18/02/2014
# Created by:   Sebastian Bednaszynski(SBE)
# Description:
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 18/02/2014  SBE   Initial version
# 10/06/2014  SBE   Script parametrised for easier refreshing
#
###############################################################################*/


  -- ##############################################################################################################
  -- ##### Set up dates                                                                                       #####
  -- ##############################################################################################################
create variable @varDateA date;
create variable @varDateB date;
set @varDateA = '2013-04-15';
set @varDateB = '2014-04-15';


  -- ##############################################################################################################
  -- ##### Snapshot 1                                                                                         #####
  -- ##############################################################################################################
if object_id('SPG_PackMvmt_01_Accounts_2013') is not null then drop table SPG_PackMvmt_01_Accounts_2013 end if;
select
      Account_Number,
      cast('???' as varchar(20)) as Account_Type,
      cast('???' as varchar(10)) as Country,
      max(case
            when b.prem_sports = 2 and b.prem_movies in (1, 2) then 'Top Tier'
            when b.prem_sports in (1, 2) and b.prem_movies = 2 then 'Top Tier'
            when b.prem_sports = 2 and b.prem_movies in (0, 1) then 'Dual Sports'
            when b.prem_sports in (0, 1) and b.prem_movies = 2 then 'Dual Movies'
              else 'Basic only'
          end) as Premium_Package,
      cast(0 as bit) as HD_Basic,
      cast(0 as bit) as HD_Premium,
      cast('no HD pack' as varchar(30)) as HD,
      cast('no Multiscreen' as varchar(30)) as Multiscreen,
      cast('no ESPN' as varchar(10)) as ESPN,
      cast('no Sky BB' as varchar(10)) as Broadband,
      cast(0 as bigint) as Num_Sport_Downgrade_Events,
      cast('No' as varchar(3)) as Sport_Downgrade_Event
  into SPG_PackMvmt_01_Accounts_2013
  from sk_prod.cust_subs_hist a,
       sk_prod.cust_entitlement_lookup b
 where a.current_short_description = b.short_description
   and a.subscription_sub_type = 'DTV Primary Viewing'
   and a.status_code in ('AB', 'PC', 'AC')
   and a.effective_from_dt  < @varDateA
   and a.effective_to_dt >= @varDateA
 group by Account_Number;
commit;
create unique hg index idx1 on SPG_PackMvmt_01_Accounts_2013(Account_Number);


  -- Update SAV attributes
update SPG_PackMvmt_01_Accounts_2013 a
   set Account_Type   = case
                          when sav.acct_type is null then '???'
                            else sav.acct_type
                        end,
       Country        = case
                          when sav.pty_country_code is null then '???'
                            else sav.pty_country_code
                        end
  from sk_prod.cust_single_account_view sav
 where a.Account_Number = sav.Account_Number;
commit;


  -- Update other package attributes
update SPG_PackMvmt_01_Accounts_2013 a
   set a.HD_Basic         = b.HD_Basic_Flag,
       a.HD_Premium       = b.HD_Premium_Flag,
       a.HD               = case
                              when HD_Basic_Flag = 1 and HD_Premium_Flag = 0 then 'HD Basic pack'
                              when HD_Basic_Flag = 0 and HD_Premium_Flag = 1 then 'HD Premium pack'
                              when HD_Basic_Flag = 1 and HD_Premium_Flag = 1 then 'HD Basic & Premium packs'
                                else 'no HD pack'
                            end,
       a.Multiscreen      = case
                              when b.Multiscreen = 1 then 'Multiscreen'
                                else 'no Multiscreen'
                            end,
       a.Broadband        = case
                              when b.BB_Flag = 1 then 'Sky BB'
                                else 'no Sky BB'
                            end,
       a.ESPN             = case
                              when b.ESPN_Flag = 1 then 'ESPN'
                                else 'no ESPN'
                            end
  from (select
              Account_Number,
              max(case
                    when subscription_sub_type = 'Broadband DSL Line' and
                         (       status_code in ('AC','AB')
                             or (status_code='PC' AND prev_status_code not in ('?','RQ','AP','UB','BE','PA') )
                             or (status_code='CF' AND prev_status_code='PC'                                  )
                             or (status_code='AP' AND sale_type='SNS Bulk Migration'                         )
                          ) then 1
                      else 0
                  end) as BB_Flag,

              max(case
                    when subscription_sub_type = 'DTV HD' and status_code in ('AC','AB','PC') then 1
                      else 0
                  end) as HD_Basic_Flag,

              max(case
                    when subscription_sub_type = 'HD Pack' and status_code in ('AC','AB','PC') then 1
                    when subscription_sub_type = 'DTV HD' and status_code in ('AC','AB','PC') and current_short_description in ('SKY_HD') then 1
                      else 0
                  end) as HD_Premium_Flag,

              max(case
                    when subscription_sub_type = 'DTV Extra Subscription' and status_code in ('AC','AB','PC') then 1
                      else 0
                  end) as Multiscreen,

              max(case
                    when subscription_type = 'A-LA-CARTE' and  subscription_sub_type = 'ESPN' and
                         status_code in ('AC','AB','PC') then 1
                      else 0
                  end) as ESPN_Flag

          from sk_prod.cust_subs_hist a
         where a.effective_from_dt  < @varDateA
           and a.effective_to_dt >= @varDateA
         group by Account_Number) b
 where a.Account_Number = b.Account_Number;
commit;


  -- Update sports downgrades
update SPG_PackMvmt_01_Accounts_2013 a
   set a.Num_Sport_Downgrade_Events = case
                                        when a.Premium_Package in ('Top Tier', 'Dual Sports') then b.Num_Sports_Downgrades
                                          else 0
                                      end,
       a.Sport_Downgrade_Event      = case
                                        when a.Premium_Package in ('Top Tier', 'Dual Sports') and b.Num_Sports_Downgrades > 0 then 'Yes'
                                          else 'No'
                                      end
  from (select
              Account_Number,
              sum(case
                    when b.prem_sports < 2 then 1
                      else 0
                  end) as Num_Sports_Downgrades
          from sk_prod.cust_subs_hist a,
               sk_prod.cust_entitlement_lookup b
         where a.current_short_description = b.short_description
           and a.subscription_sub_type = 'DTV Primary Viewing'
           and a.status_code in ('AB', 'PC', 'AC')
           and a.effective_from_dt  < @varDateB
           and a.effective_to_dt >= @varDateA
         group by Account_Number) b
 where a.Account_Number = b.Account_Number;
commit;


  -- Frequency check
select Account_Type               , count(*) as Cnt from SPG_PackMvmt_01_Accounts_2013 group by Account_Type               ;
select Country                    , count(*) as Cnt from SPG_PackMvmt_01_Accounts_2013 group by Country                    ;
select Premium_Package            , count(*) as Cnt from SPG_PackMvmt_01_Accounts_2013 group by Premium_Package            ;
select HD                         , count(*) as Cnt from SPG_PackMvmt_01_Accounts_2013 group by HD                         ;
select Multiscreen                , count(*) as Cnt from SPG_PackMvmt_01_Accounts_2013 group by Multiscreen                ;
select ESPN                       , count(*) as Cnt from SPG_PackMvmt_01_Accounts_2013 group by ESPN                       ;
select Broadband                  , count(*) as Cnt from SPG_PackMvmt_01_Accounts_2013 group by Broadband                  ;
select Num_Sport_Downgrade_Events , count(*) as Cnt from SPG_PackMvmt_01_Accounts_2013 group by Num_Sport_Downgrade_Events ;
select Sport_Downgrade_Event      , count(*) as Cnt from SPG_PackMvmt_01_Accounts_2013 group by Sport_Downgrade_Event      ;



  -- ##############################################################################################################
  -- ##### Snapshot 2                                                                                         #####
  -- ##############################################################################################################
if object_id('SPG_PackMvmt_01_Accounts_2014') is not null then drop table SPG_PackMvmt_01_Accounts_2014 end if;
select
      Account_Number,
      cast('???' as varchar(20)) as Account_Type,
      cast('???' as varchar(10)) as Country,
      max(case
            when b.prem_sports = 2 and b.prem_movies in (1, 2) then 'Top Tier'
            when b.prem_sports in (1, 2) and b.prem_movies = 2 then 'Top Tier'
            when b.prem_sports = 2 and b.prem_movies in (0, 1) then 'Dual Sports'
            when b.prem_sports in (0, 1) and b.prem_movies = 2 then 'Dual Movies'
              else 'Basic only'
          end) as Premium_Package,
      cast(0 as bit) as HD_Basic,
      cast(0 as bit) as HD_Premium,
      cast('no HD pack' as varchar(30)) as HD,
      cast('no Multiscreen' as varchar(30)) as Multiscreen,
      cast('no ESPN' as varchar(10)) as ESPN,
      cast('no Sky BB' as varchar(10)) as Broadband,
      cast(0 as bigint) as Num_Sport_Downgrade_Events,
      cast('No' as varchar(3)) as Sport_Downgrade_Event
  into SPG_PackMvmt_01_Accounts_2014
  from sk_prod.cust_subs_hist a,
       sk_prod.cust_entitlement_lookup b
 where a.current_short_description = b.short_description
   and a.subscription_sub_type = 'DTV Primary Viewing'
   and a.status_code in ('AB', 'PC', 'AC')
   and a.effective_from_dt  < @varDateB
   and a.effective_to_dt >= @varDateB
 group by Account_Number;
commit;
create unique hg index idx1 on SPG_PackMvmt_01_Accounts_2014(Account_Number);


  -- Update SAV attributes
update SPG_PackMvmt_01_Accounts_2014 a
   set Account_Type   = case
                          when sav.acct_type is null then '???'
                            else sav.acct_type
                        end,
       Country        = case
                          when sav.pty_country_code is null then '???'
                            else sav.pty_country_code
                        end
  from sk_prod.cust_single_account_view sav
 where a.Account_Number = sav.Account_Number;
commit;


  -- Update other package attributes
update SPG_PackMvmt_01_Accounts_2014 a
   set a.HD_Basic         = b.HD_Basic_Flag,
       a.HD_Premium       = b.HD_Premium_Flag,
       a.HD               = case
                              when HD_Basic_Flag = 1 and HD_Premium_Flag = 0 then 'HD Basic pack'
                              when HD_Basic_Flag = 0 and HD_Premium_Flag = 1 then 'HD Premium pack'
                              when HD_Basic_Flag = 1 and HD_Premium_Flag = 1 then 'HD Basic & Premium packs'
                                else 'no HD pack'
                            end,
       a.Multiscreen      = case
                              when b.Multiscreen = 1 then 'Multiscreen'
                                else 'no Multiscreen'
                            end,
       a.Broadband        = case
                              when b.BB_Flag = 1 then 'Sky BB'
                                else 'no Sky BB'
                            end,
       a.ESPN             = case
                              when b.ESPN_Flag = 1 then 'ESPN'
                                else 'no ESPN'
                            end
  from (select
              Account_Number,
              max(case
                    when subscription_sub_type = 'Broadband DSL Line' and
                         (       status_code in ('AC','AB')
                             or (status_code='PC' AND prev_status_code not in ('?','RQ','AP','UB','BE','PA') )
                             or (status_code='CF' AND prev_status_code='PC'                                  )
                             or (status_code='AP' AND sale_type='SNS Bulk Migration'                         )
                          ) then 1
                      else 0
                  end) as BB_Flag,

              max(case
                    when subscription_sub_type = 'DTV HD' and status_code in ('AC','AB','PC') then 1
                      else 0
                  end) as HD_Basic_Flag,

              max(case
                    when subscription_sub_type = 'HD Pack' and status_code in ('AC','AB','PC') then 1
                    when subscription_sub_type = 'DTV HD' and status_code in ('AC','AB','PC') and current_short_description in ('SKY_HD') then 1
                      else 0
                  end) as HD_Premium_Flag,

              max(case
                    when subscription_sub_type = 'DTV Extra Subscription' and status_code in ('AC','AB','PC') then 1
                      else 0
                  end) as Multiscreen,

              /*max(case
                    when subscription_type = 'A-LA-CARTE' and  subscription_sub_type = 'ESPN' and
                         status_code in ('AC','AB','PC') then 1
                      else 0
                  end) as ESPN_Flag*/
              max(0) as ESPN_Flag

          from sk_prod.cust_subs_hist a
         where a.effective_from_dt  < @varDateB
           and a.effective_to_dt >= @varDateB
         group by Account_Number) b
 where a.Account_Number = b.Account_Number;
commit;


  -- Update sports downgrades
update SPG_PackMvmt_01_Accounts_2014 a
   set a.Num_Sport_Downgrade_Events = case
                                        when a.Premium_Package in ('Top Tier', 'Dual Sports') then b.Num_Sports_Downgrades
                                          else 0
                                      end,
       a.Sport_Downgrade_Event      = case
                                        when a.Premium_Package in ('Top Tier', 'Dual Sports') and b.Num_Sports_Downgrades > 0 then 'Yes'
                                          else 'No'
                                      end
  from (select
              Account_Number,
              sum(case
                    when b.prem_sports < 2 then 1
                      else 0
                  end) as Num_Sports_Downgrades
          from sk_prod.cust_subs_hist a,
               sk_prod.cust_entitlement_lookup b
         where a.current_short_description = b.short_description
           and a.subscription_sub_type = 'DTV Primary Viewing'
           and a.status_code in ('AB', 'PC', 'AC')
           and a.effective_from_dt  < @varDateB
           and a.effective_to_dt >= @varDateA
         group by Account_Number) b
 where a.Account_Number = b.Account_Number;
commit;


  -- Frequency check
select Account_Type               , count(*) as Cnt from SPG_PackMvmt_01_Accounts_2014 group by Account_Type               ;
select Country                    , count(*) as Cnt from SPG_PackMvmt_01_Accounts_2014 group by Country                    ;
select Premium_Package            , count(*) as Cnt from SPG_PackMvmt_01_Accounts_2014 group by Premium_Package            ;
select HD                         , count(*) as Cnt from SPG_PackMvmt_01_Accounts_2014 group by HD                         ;
select Multiscreen                , count(*) as Cnt from SPG_PackMvmt_01_Accounts_2014 group by Multiscreen                ;
select ESPN                       , count(*) as Cnt from SPG_PackMvmt_01_Accounts_2014 group by ESPN                       ;
select Broadband                  , count(*) as Cnt from SPG_PackMvmt_01_Accounts_2014 group by Broadband                  ;
select Num_Sport_Downgrade_Events , count(*) as Cnt from SPG_PackMvmt_01_Accounts_2014 group by Num_Sport_Downgrade_Events ;
select Sport_Downgrade_Event      , count(*) as Cnt from SPG_PackMvmt_01_Accounts_2014 group by Sport_Downgrade_Event      ;



  -- ##############################################################################################################
  -- ##### Manual hack to combat some data oddities and group HD packs                                        #####
  -- ##############################################################################################################
update SPG_PackMvmt_01_Accounts_2013
   set HD             = case
                          when trim(Premium_Package) = 'Basic only' and (HD_Basic = 1 or HD_Premium = 1) then 'HD pack (any)'
                          when trim(Premium_Package) in ('Dual Movies', 'Dual Sports', 'Top Tier') and trim(HD) = 'HD Premium pack' then 'HD Basic & Premium packs'   -- Merge-in tiny volume accounts
                          when trim(Premium_Package) in ('Dual Movies', 'Dual Sports', 'Top Tier') and trim(HD) = 'HD Basic pack' then 'HD Basic & Premium packs'     -- Merge-in tiny volume accounts
                            else HD
                        end;
commit;

update SPG_PackMvmt_01_Accounts_2014
   set HD             = case
                          when trim(Premium_Package) = 'Basic only' and (HD_Basic = 1 or HD_Premium = 1) then 'HD pack (any)'
                          when trim(Premium_Package) in ('Dual Movies', 'Dual Sports', 'Top Tier') and trim(HD) = 'HD Premium pack' then 'HD Basic & Premium packs'   -- Merge-in tiny volume accounts
                            else HD
                        end;
commit;



  -- ##############################################################################################################
  -- ##### Join snapshots                                                                                     #####
  -- ##############################################################################################################
if object_id('SPG_PackMvmt_02_Accounts_Joined') is not null then drop table SPG_PackMvmt_02_Accounts_Joined end if;
select
      coalesce(a.Account_Number, b.Account_Number) as Account_Number,

      case when a.Account_Type               is null then 'Inactive in 2013' else a.Account_Type                end as Account_Type_2013,
      case when a.Country                    is null then 'Inactive in 2013' else a.Country                     end as Country_2013,
      case when a.Premium_Package            is null then 'Inactive in 2013' else a.Premium_Package             end as Premium_Package_2013,
      case when a.HD                         is null then 'Inactive in 2013' else a.HD                          end as HD_2013,
      case when a.Multiscreen                is null then 'Inactive in 2013' else a.Multiscreen                 end as Multiscreen_2013,
      case when a.ESPN                       is null then 'Inactive in 2013' else a.ESPN                        end as ESPN_2013,
      case when a.Broadband                  is null then 'Inactive in 2013' else a.Broadband                   end as Broadband_2013,
      case when a.Sport_Downgrade_Event      is null then 'Inactive in 2014' else a.Sport_Downgrade_Event       end as Sport_Downgrade_Event_2013,

      case when b.Account_Type               is null then 'Inactive in 2014' else b.Account_Type                end as Account_Type_2014,
      case when b.Country                    is null then 'Inactive in 2014' else b.Country                     end as Country_2014,
      case when b.Premium_Package            is null then 'Inactive in 2014' else b.Premium_Package             end as Premium_Package_2014,
      case when b.HD                         is null then 'Inactive in 2014' else b.HD                          end as HD_2014,
      case when b.Multiscreen                is null then 'Inactive in 2014' else b.Multiscreen                 end as Multiscreen_2014,
      case when b.ESPN                       is null then 'Inactive in 2014' else b.ESPN                        end as ESPN_2014,
      case when b.Broadband                  is null then 'Inactive in 2014' else b.Broadband                   end as Broadband_2014,
      case when b.Sport_Downgrade_Event      is null then 'Inactive in 2014' else b.Sport_Downgrade_Event       end as Sport_Downgrade_Event_2014,

      case
        when a.Premium_Package in ('Top Tier', 'Dual Sports') and b.Premium_Package in ('Top Tier', 'Dual Sports') and a.Sport_Downgrade_Event = 'Yes' then 'Sports downgrade'
          else 'no Sports downgrade'
      end as Sports_Downgrade_Event

  into SPG_PackMvmt_02_Accounts_Joined
  from SPG_PackMvmt_01_Accounts_2013 a full join SPG_PackMvmt_01_Accounts_2014 b on a.Account_Number = b.Account_Number;
commit;


  -- Frequency check
select Account_Type_2013, Account_Type_2014, count(*) as Cnt from SPG_PackMvmt_02_Accounts_Joined group by Account_Type_2013, Account_Type_2014 order by 1, 2;
select Country_2013, Country_2014, count(*) as Cnt from SPG_PackMvmt_02_Accounts_Joined group by Country_2013, Country_2014 order by 1, 2;
select Premium_Package_2013, Premium_Package_2014, count(*) as Cnt from SPG_PackMvmt_02_Accounts_Joined group by Premium_Package_2013, Premium_Package_2014 order by 1, 2;
select HD_2013, HD_2014, count(*) as Cnt from SPG_PackMvmt_02_Accounts_Joined group by HD_2013, HD_2014 order by 1, 2;
select Multiscreen_2013, Multiscreen_2014, count(*) as Cnt from SPG_PackMvmt_02_Accounts_Joined group by Multiscreen_2013, Multiscreen_2014 order by 1, 2;
select ESPN_2013, ESPN_2014, count(*) as Cnt from SPG_PackMvmt_02_Accounts_Joined group by ESPN_2013, ESPN_2014 order by 1, 2;
select Broadband_2013, Broadband_2014, count(*) as Cnt from SPG_PackMvmt_02_Accounts_Joined group by Broadband_2013, Broadband_2014 order by 1, 2;
select Premium_Package_2013, Premium_Package_2014, Sport_Downgrade_Event_2013, Sport_Downgrade_Event_2014, count(*) as Cnt from SPG_PackMvmt_02_Accounts_Joined group by Premium_Package_2013, Premium_Package_2014, Sport_Downgrade_Event_2013, Sport_Downgrade_Event_2014 order by 1, 2, 3;
select Sports_Downgrade_Event, count(*) as Cnt from SPG_PackMvmt_02_Accounts_Joined group by Sports_Downgrade_Event order by 1;



  -- ##############################################################################################################
  -- ##############################################################################################################
  -- Export
select
      Account_Type_2013,
      Country_2013,
      case
        when trim(Premium_Package_2013) = 'Inactive in 2013' then 'Inactive in 2013'
          else trim(Premium_Package_2013) || ', ' || trim(HD_2013) || ', ' || trim(Multiscreen_2013) || ', ' || trim(ESPN_2013) || ', ' || trim(Broadband_2013)
      end as Package_2013,

      Account_Type_2014,
      Country_2014,
      case
        when trim(Premium_Package_2014) = 'Inactive in 2014' then 'Inactive in 2014'
          else trim(Premium_Package_2014) || ', ' || trim(HD_2014) || ', ' || trim(Multiscreen_2014) || ', ' || trim(Broadband_2014)
      end as Package_2014,

      Sports_Downgrade_Event,
      count(*) as Cnt

  from SPG_PackMvmt_02_Accounts_Joined
 where Account_Type_2013 not in ('???', 'Non-Standard')
   and Account_Type_2014 not in ('???', 'Non-Standard')
   and Country_2013 <> '???'
   and Country_2014 <> '???'
 group by
      Account_Type_2013,
      Country_2013,
      Package_2013,

      Account_Type_2014,
      Country_2014,
      Package_2014,

      Sports_Downgrade_Event;



  -- ##############################################################################################################
  -- ###### Other package downgrades                                                                         ######
  -- ##############################################################################################################
  -- Get a list of all sports downgrades (lasting more than 1 day)
if object_id('SPG_PackMvmt_03_Sports') is not null then drop table SPG_PackMvmt_03_Sports end if;
create table SPG_PackMvmt_03_Sports (
      Pk                            bigint  identity,
      Account_Number                varchar(20)   null default null,
      Sports_Mvmnt_Dt               date          null default null,
      Prev_Sports                   tinyint       null default 0,
      Curr_Sports                   tinyint       null default 0,
      Sports_Movement               smallint      null default 0,

      Movies                        tinyint       null default 0,
      Movies_End                    tinyint       null default 0,
      HD                            tinyint       null default 0,
      HD_End                        tinyint       null default 0,
      BB                            tinyint       null default 0,
      BB_End                        tinyint       null default 0,
      Extra                         tinyint       null default 0,
      Extra_End                     tinyint       null default 0,
      DTV                           tinyint       null default 0,
      DTV_End                       tinyint       null default 0

);
create        hg index idx1 on SPG_PackMvmt_03_Sports(Account_Number);
create      date index idx2 on SPG_PackMvmt_03_Sports(Sports_Mvmnt_Dt);


insert into SPG_PackMvmt_03_Sports (Account_Number, Sports_Mvmnt_Dt, Prev_Sports, Curr_Sports, Sports_Movement)
select
      base.Account_Number,
      Effective_From_Dt as Sports_Mvmnt_Dt,
      max(prev.prem_sports) as Prev_Sports,
      max(curr.prem_sports) as Curr_Sports,
      max(
          cast(case
                 when curr.prem_sports < prev.prem_sports then -1
                 when curr.prem_sports > prev.prem_sports then 1
                   else 0
               end as smallint)
          ) as Sports_Movement
  from SPG_PackMvmt_01_Accounts_2013 base,
       sk_prod.cust_subs_hist a,
       sk_prod.cust_entitlement_lookup curr,
       sk_prod.cust_entitlement_lookup prev
 where base.Account_Number = a.Account_Number
   and a.current_short_description = curr.short_description
   and a.previous_short_description = prev.short_description
   and a.subscription_sub_type = 'DTV Primary Viewing'
   and a.status_code in ('AB', 'PC', 'AC')
   and a.effective_from_dt >= @varDateA
   and a.effective_from_dt  < @varDateB
   and a.effective_from_dt < a.effective_to_dt
   and a.ent_cat_prod_changed = 'Y'
   and curr.prem_sports <> prev.prem_sports
 group by base.Account_Number, Sports_Mvmnt_Dt;
commit;

/*
--Check

select base.Account_Number, Effective_From_Dt, Effective_To_Dt Prev_Sports, Curr_Sports,
      cast(case
             when curr.prem_sports < prev.prem_sports then -1
             when curr.prem_sports > prev.prem_sports then 1
               else 0
           end as smallint) as Sports_Movement
  from sk_prod.cust_subs_hist a, sk_prod.cust_entitlement_lookup curr, sk_prod.cust_entitlement_lookup prev
 where a.Account_Number in ('')
   and a.current_short_description = curr.short_description
   and a.previous_short_description = prev.short_description
   and a.subscription_sub_type = 'DTV Primary Viewing'
   and a.status_code in ('AB', 'PC', 'AC')
   and a.effective_from_dt >= @varDateA
   and a.effective_from_dt  < @varDateB
   and a.ent_cat_prod_changed = 'Y'
   and a.effective_from_dt < a.effective_to_dt
   and curr.prem_sports <> prev.prem_sports;
*/

  -- ##############################################################################################################
  -- Movies at Sports downgrade time
update SPG_PackMvmt_03_Sports base
   set base.Movies      = ent.Prem_Movies
  from sk_prod.cust_subs_hist a,
       sk_prod.cust_entitlement_lookup ent
 where base.Account_Number = a.Account_Number
   and a.current_short_description = ent.short_description
   and a.subscription_sub_type = 'DTV Primary Viewing'
   and a.status_code in ('AB', 'PC', 'AC')
   and a.effective_from_dt <= base.Sports_Mvmnt_Dt - 1
   and a.effective_to_dt   > base.Sports_Mvmnt_Dt - 1
   and a.effective_from_dt < a.effective_to_dt;
commit;

  -- Movies at the end of observation period
update SPG_PackMvmt_03_Sports base
   set base.Movies_End  = ent.Prem_Movies
  from sk_prod.cust_subs_hist a,
       sk_prod.cust_entitlement_lookup ent
 where base.Account_Number = a.Account_Number
   and a.current_short_description = ent.short_description
   and a.subscription_sub_type = 'DTV Primary Viewing'
   and a.status_code in ('AB', 'PC', 'AC')
   and a.effective_from_dt <= base.Sports_Mvmnt_Dt + 7
   and a.effective_to_dt   > base.Sports_Mvmnt_Dt + 7
   and a.effective_from_dt < a.effective_to_dt;
commit;


  -- ##############################################################################################################
  -- HD at Sports downgrade time
update SPG_PackMvmt_03_Sports base
   set base.HD          = 1
  from sk_prod.cust_subs_hist a
 where base.Account_Number = a.Account_Number
   and (
        a.subscription_sub_type = 'HD Pack'
        or
        a.subscription_sub_type = 'DTV HD'
       )
   and a.status_code in ('AC','AB','PC')
   and a.effective_from_dt <= base.Sports_Mvmnt_Dt - 1
   and a.effective_to_dt   > base.Sports_Mvmnt_Dt - 1
   and a.effective_from_dt < a.effective_to_dt;
commit;

  -- HD at the end of observation period
update SPG_PackMvmt_03_Sports base
   set base.HD_End      = 1
  from sk_prod.cust_subs_hist a
 where base.Account_Number = a.Account_Number
   and (
        a.subscription_sub_type = 'HD Pack'
        or
        a.subscription_sub_type = 'DTV HD'
       )
   and a.status_code in ('AC','AB','PC')
   and a.effective_from_dt <= base.Sports_Mvmnt_Dt + 35
   and a.effective_to_dt   > base.Sports_Mvmnt_Dt + 35
   and a.effective_from_dt < a.effective_to_dt;
commit;


  -- ##############################################################################################################
  -- BB at Sports downgrade time
update SPG_PackMvmt_03_Sports base
   set base.BB          = 1
  from sk_prod.cust_subs_hist a
 where base.Account_Number = a.Account_Number
   and a.subscription_sub_type = 'Broadband DSL Line'
   and (       status_code in ('AC','AB')
           or (status_code='PC' AND prev_status_code not in ('?','RQ','AP','UB','BE','PA') )
           or (status_code='CF' AND prev_status_code='PC'                                  )
           or (status_code='AP' AND sale_type='SNS Bulk Migration'                         )
        )
   and a.effective_from_dt <= base.Sports_Mvmnt_Dt - 1
   and a.effective_to_dt   > base.Sports_Mvmnt_Dt - 1
   and a.effective_from_dt < a.effective_to_dt;
commit;

  -- BB at the end of observation period
update SPG_PackMvmt_03_Sports base
   set base.BB_End      = 1
  from sk_prod.cust_subs_hist a
 where base.Account_Number = a.Account_Number
   and a.subscription_sub_type = 'Broadband DSL Line'
   and (       status_code in ('AC','AB')
           or (status_code='PC' AND prev_status_code not in ('?','RQ','AP','UB','BE','PA') )
           or (status_code='CF' AND prev_status_code='PC'                                  )
           or (status_code='AP' AND sale_type='SNS Bulk Migration'                         )
        )
   and a.effective_from_dt <= base.Sports_Mvmnt_Dt + 35
   and a.effective_to_dt   > base.Sports_Mvmnt_Dt + 35
   and a.effective_from_dt < a.effective_to_dt;
commit;


  -- ##############################################################################################################
  -- Extra (multiroom) at Sports downgrade time
update SPG_PackMvmt_03_Sports base
   set base.Extra       = 1
  from sk_prod.cust_subs_hist a
 where base.Account_Number = a.Account_Number
   and a.subscription_sub_type = 'DTV Extra Subscription'
   and a.status_code in ('AC','AB','PC')
   and a.effective_from_dt <= base.Sports_Mvmnt_Dt - 1
   and a.effective_to_dt   > base.Sports_Mvmnt_Dt - 1
   and a.effective_from_dt < a.effective_to_dt;
commit;

  -- Extra (multiroom) at the end of observation period
update SPG_PackMvmt_03_Sports base
   set base.Extra_End   = 1
  from sk_prod.cust_subs_hist a
 where base.Account_Number = a.Account_Number
   and a.subscription_sub_type = 'DTV Extra Subscription'
   and a.status_code in ('AC','AB','PC')
   and a.effective_from_dt <= base.Sports_Mvmnt_Dt + 35
   and a.effective_to_dt   > base.Sports_Mvmnt_Dt + 35
   and a.effective_from_dt < a.effective_to_dt;
commit;


  -- ##############################################################################################################
  -- DTV at Sports downgrade time
update SPG_PackMvmt_03_Sports base
   set base.DTV         = 1
  from sk_prod.cust_subs_hist a
 where base.Account_Number = a.Account_Number
   and a.subscription_sub_type = 'DTV Primary Viewing'
   and a.status_code in ('AB', 'PC', 'AC')
   and a.effective_from_dt <= base.Sports_Mvmnt_Dt - 1
   and a.effective_to_dt   > base.Sports_Mvmnt_Dt - 1
   and a.effective_from_dt < a.effective_to_dt;
commit;

  -- DTV at the end of observation period
update SPG_PackMvmt_03_Sports base
   set base.DTV_End     = 1
  from sk_prod.cust_subs_hist a
 where base.Account_Number = a.Account_Number
   and a.subscription_sub_type = 'DTV Primary Viewing'
   and a.status_code in ('AB', 'PC', 'AC')
   and a.effective_from_dt <= base.Sports_Mvmnt_Dt + 35
   and a.effective_to_dt   > base.Sports_Mvmnt_Dt + 35
   and a.effective_from_dt < a.effective_to_dt;
commit;


  -- ##############################################################################################################
  -- ##############################################################################################################
  -- Export
select
      a.Account_Type as Account_Type_2013,
      a.Country as Country_2013,
      trim(a.Premium_Package) || ', ' || trim(a.HD) || ', ' || trim(a.ESPN) || ', ' || trim(a.Broadband) as Package_2013,

      count(distinct a.Account_Number) as Accounts,
      count(distinct case
                       when c.Account_Number is null then a.Account_Number
                         else null
                     end) as DTV_Inactive_2014,
      count(distinct case
                       when b.Prev_Sports = 2 and b.Curr_Sports = 0 then a.Account_Number
                         else null
                     end) as Sport_Downgrades_Accounts,

      sum(case
            when b.Prev_Sports = 2 and b.Curr_Sports = 0 then 1
              else 0
          end) as Sports_Downgrades,
      sum(case
            when b.Prev_Sports = 2 and b.Curr_Sports = 0 and (b.Movies > b.Movies_End or b.DTV > b.DTV_End) then 1
              else 0
          end) as Movies_Downgrades,
      sum(case
            when b.Prev_Sports = 2 and b.Curr_Sports = 0 and (b.HD > b.HD_End or b.DTV > b.DTV_End)  then 1
              else 0
          end) as HD_Downgrades,
      sum(case
            when b.Prev_Sports = 2 and b.Curr_Sports = 0 and (b.BB > b.BB_End or b.DTV > b.DTV_End)  then 1
              else 0
          end) as BB_Downgrades,
      sum(case
            when b.Prev_Sports = 2 and b.Curr_Sports = 0 and (b.Extra > b.Extra_End or b.DTV > b.DTV_End)  then 1
              else 0
          end) as Extra_Downgrades,
      sum(case
            when b.Prev_Sports = 2 and b.Curr_Sports = 0 and b.DTV > b.DTV_End then 1
              else 0
          end) as DTV_Chuners

  from SPG_PackMvmt_01_Accounts_2013 a
          left join SPG_PackMvmt_03_Sports b on a.Account_Number = b.Account_Number and b.Sports_Movement = -1
          left join SPG_PackMvmt_01_Accounts_2014 c on a.Account_Number = c.Account_Number
 where Account_Type_2013 not in ('???', 'Non-Standard')
   and Country_2013 <> '???'
 group by
      Account_Type_2013,
      Country_2013,
      Package_2013;
























