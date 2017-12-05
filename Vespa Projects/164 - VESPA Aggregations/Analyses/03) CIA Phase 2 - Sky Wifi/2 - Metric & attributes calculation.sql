/*###############################################################################
# Created on:   17/10/2013
# Created by:   Sebastian Bednaszynski (SBE)
# Description:  CIA Phase 2 (Sky Wifi) - account attributes & metric creation
#               for aggregations
#
# List of steps:
#               STEP 1 - Account attributes & Wifi usage calculation
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# => Tables/objects required:
#     - ngm.wifi_sessions_20131015 (manual extract loaded into Sybase)
#     - sk_prod.cust_subs_hist
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 11/11/2013  SBE   Initial version
#
###############################################################################*/



-- ##############################################################################################################
-- ##### STEP 1 - Account attributes & Wifi usage calculation                                               #####
-- ##############################################################################################################
create table VAggrAnal_SkyWifi_Account_Summary (
      Id                                  bigint            identity,
      Account_Number                      varchar(20)       null      default null,
      Owning_Cust_Account_Id              varchar(50)       null      default null,
      Days_Wifi_Entitlement               smallint          null      default 0,

      Bytes_Transferred_In                bigint            null      default 0,
      Bytes_Transferred_Out               bigint            null      default 0,
      Bytes_Transferred_Total             bigint            null      default 0,
      Total_Wifi_Active_Days              smallint          null      default 0,

      Updated_On                          datetime          default timestamp,
      Updated_By                          varchar(30)       default user_name()

);

create hg index idx1 on VAggrAnal_SkyWifi_Account_Summary(Account_number);
create hg index idx2 on VAggrAnal_SkyWifi_Account_Summary(Owning_Cust_Account_Id);


insert into VAggrAnal_SkyWifi_Account_Summary (Account_Number, Owning_Cust_Account_Id, Days_Wifi_Entitlement)
  select
        Account_Number,
        min(Owning_Cust_Account_Id) as Owning_Cust_Account_Id,
        sum(
            datediff(
                      day,
                      case
                        when effective_from_dt < '2013-05-01' then cast('2013-05-01' as date)
                          else effective_from_dt
                      end,
                      case
                        when effective_to_dt > '2013-05-31' then cast('2013-06-01' as date)
                          else effective_to_dt
                      end
                    )
            ) as Days_Wifi_Entitlement
    into VAggrAnal_SkyWifi_Account_Summary
    from sk_prod.cust_subs_hist
   where effective_from_dt <= '2013-05-31'
     and effective_to_dt > '2013-05-01'
     and Account_Number is not null
     and subscription_sub_type = 'CLOUDWIFI'
     and (
           status_code IN ('AC', 'AB')
           OR
           (status_code = 'PC' AND prev_status_code NOT IN ('?', 'RQ', 'AP', 'UB', 'BE', 'PA'))
           OR
           (status_code = 'CF' AND prev_status_code = 'PC')
           OR
           (status_code = 'AP' AND sale_type = 'SNS Bulk Migration')
         )
     and effective_from_dt < effective_to_dt
     -- and current_product_sk IN (43373, 42128, 42131) -------- Need to be unlimited and Connect customers!! Not required now
   group by Account_Number;
commit;


  -- Wifi usage
update VAggrAnal_SkyWifi_Account_Summary base
   set base.Bytes_Transferred_In      = det.Bytes_Transferred_In,
       base.Bytes_Transferred_Out     = det.Bytes_Transferred_Out,
       base.Bytes_Transferred_Total   = det.Bytes_Transferred_Total,
       base.Total_Wifi_Active_Days    = det.Total_Wifi_Active_Days
  from (select
              Account_Number,
              sum(Bytes_In) as Bytes_Transferred_In,
              sum(Bytes_Out) as Bytes_Transferred_Out,
              sum(Bytes_In + Bytes_Out) as Bytes_Transferred_Total,
              count(distinct date(Start_Time)) as Total_Wifi_Active_Days
          from ngm.wifi_sessions_20131015
         where (Bytes_In > 0 or Bytes_Out > 0) and duration > 0
           and date(Start_Time) between '2013-05-01'and '2013-05-31'
         group by Account_Number) det
 where base.Account_Number = det.Account_Number;
commit;



-- ##############################################################################################################
-- ##############################################################################################################
















