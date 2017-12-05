/*
dba.sp_drop_table 'CITeam','Broadband_Comms_Pipeline'
--dba.sp_create_table 'oce01','Broadband_Comms_Pipeline',
dba.sp_create_table 'CITeam','Broadband_Comms_Pipeline',
   'Subs_Year integer default null, '
|| 'Subs_Week_Of_Year integer default null, '
|| 'Subs_Week_And_Year integer default null, '
|| 'Event_Dt date default null, '
|| 'Order_ID varchar(50) default null, '
|| 'Account_Number varchar(20) default null, '
|| 'Country varchar(3) default null, '
|| 'Created_By_ID varchar(50) default null,'
|| 'prev_status_code varchar(5) default null, '
|| 'status_code varchar(5) default null, '
|| 'Enter_SysCan tinyint default 0,'    --OC comments: if the possible values will be only 0 & 1, can we assign data type as bit instead of tinyint? If there are plans to have otehr values for other movements, such as 2 for leaving this status, then I also agree to leave it as tinyint
|| 'Enter_CusCan tinyint default 0,'
|| 'Enter_HM tinyint default 0,'
|| 'Enter_3rd_Party tinyint default 0, '
|| 'PC_Effective_To_Dt date default null, '
|| 'PC_Future_Sub_Effective_Dt date default null, '
|| 'PC_Next_Status_Code varchar(4) default null, '
|| 'AB_Effective_To_Dt date default null, '
|| 'AB_Future_Sub_Effective_Dt date default null, '
|| 'AB_Next_Status_Code varchar(4) default null, '
|| 'BCRQ_Effective_To_Dt date default null, '
|| 'BCRQ_Future_Sub_Effective_Dt date default null, '
|| 'BCRQ_Next_Status_Code varchar(4) default null '

*/

Drop variable if exists Refresh_dt; Create variable Refresh_dt date;
Set Refresh_dt = (Select max(Event_Dt) - 6 * 7 from CITeam.Broadband_Comms_Pipeline);
--select Refresh_dt;

delete from CITeam.Broadband_Comms_Pipeline where event_dt >= Refresh_Dt;

insert into
--delete from
CITeam.Broadband_Comms_Pipeline
select
Cal.Subs_Year ,  --as Subs_Year
-- Cal.Subs_Quarter_of_year ,
Cal.subs_week_of_year , --as Subs_Week_Of_Year
Cal.Subs_week_and_year  , --as Subs_Week_And_Year
cast(WH_PH_SUBS_HIST.EFFECTIVE_FROM_DT as date) , -- [Status Start Date] --as Event_Dt
WH_PH_SUBS_HIST.Order_ID,
WH_PH_SUBS_HIST.account_number,
CASE WHEN WH_PH_SUBS_HIST.currency_code='GBP' THEN 'UK'
     WHEN WH_PH_SUBS_HIST.currency_code='EUR' THEN 'ROI'
     ELSE 'Other' END , --AS Country
WH_PH_SUBS_HIST.created_by_id,
WH_PH_SUBS_HIST.prev_status_code,
WH_PH_SUBS_HIST.status_code,
CASE WHEN
--prev_status_code in ('AC','PC','CF' ) --Previously ACTIVE or other cuscan pipeline statuses
        ( WH_PH_SUBS_HIST.status_code in ('AB')
          or (WH_PH_SUBS_HIST.STATUS_CODE  = 'BCRQ' AND WH_PH_SUBS_HIST.PREV_STATUS_CODE IN ('PC','CF')
          --and WH_PH_SUBS_HIST.STATUS_REASON_CODE IN ('79','80' )
          )
          )
          and WH_ADDRESS_ROLE.AD_CREATED_DT IS NULL  /* means 'NOT Home move' */
          and WH_PH_SUBS_HIST.STATUS_REASON_CODE <> '900' /* means 'NOT 3rd party' */
     THEN 1 ELSE 0 END , --AS Enter_SysCan AB
CASE WHEN (
         (   (WH_PH_SUBS_HIST.Status_Code IN ('PC') AND WH_PH_SUBS_HIST.prev_status_code in ('AC','PT','AB'))                   -- status 'PO' is excluded as per excel definition
          or (WH_PH_SUBS_HIST.Status_Code IN ('PC') AND WH_PH_SUBS_HIST.PREV_STATUS_CODE IN ('BCRQ') and PREVPH.STATUS_REASON_CODE IN ('80'))
          )
          and WH_ADDRESS_ROLE.AD_CREATED_DT IS NULL  /* means 'NOT Home move' */
          and WH_PH_SUBS_HIST.STATUS_REASON_CODE not in ( '900' ) /* means 'NOT 3rd party' */
          )
     THEN 1 ELSE 0 END , --AS Enter_CusCan PC

CASE WHEN (WH_ADDRESS_ROLE.AD_CREATED_DT IS NOT NULL)  /* means 'Home move' */
     THEN 1 ELSE 0 END ,  --AS Enter_HM

CASE WHEN (WH_PH_SUBS_HIST.STATUS_REASON_CODE in ( '900' ,'84' ) /* status reason is BB Third Party Cancellation, resulting in [Dummy Number 2] is 1 */
           AND WH_ADDRESS_ROLE.AD_CREATED_DT IS NULL)  /* means 'No Homemove' */
     THEN 1 ELSE 0 END, -- AS Enter_3rd_Party
NULL, --PC_Effective_To_Dt,
NULL, --PC_Future_Sub_Effective_Dt,
NULL, --PC_Next_Status_Code,
NULL, --AB_Effective_To_Dt,
NULL, --AB_Future_Sub_Effective_Dt,
NULL, --AB_Next_Status_Code,
NULL, --BCRQ_Effective_To_Dt,
NULL, --BCRQ_Future_Sub_Effective_Dt,
NULL --BCRQ_Next_Status_Code

FROM
--We get Funnel Entry from here
Cust_Subs_Hist WH_PH_SUBS_HIST
LEFT OUTER JOIN
CUST_ALL_ADDRESS WH_ADDRESS_ROLE
ON    WH_PH_SUBS_HIST.OWNING_CUST_ACCOUNT_ID = WH_ADDRESS_ROLE.CUST_ACCOUNT_ID
                AND
                cast(WH_PH_SUBS_HIST.EFFECTIVE_FROM_DT as date)= CAST(WH_ADDRESS_ROLE.AD_CREATED_DT as date)
                AND
                WH_ADDRESS_ROLE.CHANGE_REASON_CODE in ('MHWITHINST','MHNOINST' )
--/*
LEFT OUTER JOIN
Cust_Subs_Hist PREVPH
 ON (WH_PH_SUBS_HIST.SUBSCRIPTION_ID = PREVPH.SUBSCRIPTION_ID AND
         WH_PH_SUBS_HIST.STATUS_START_DT = PREVPH.STATUS_END_DT AND
         WH_PH_SUBS_HIST.STATUS_START_DT > PREVPH.STATUS_START_DT AND
         PREVPH.STATUS_CODE_CHANGED  =  'Y' AND
         WH_PH_SUBS_HIST.PREV_STATUS_CODE = PREVPH.STATUS_CODE AND
         PREVPH.SUBSCRIPTION_SUB_TYPE = 'Broadband DSL Line' AND
         WH_PH_SUBS_HIST.SUBSCRIPTION_SUB_TYPE = 'Broadband DSL Line'AND
         cast(WH_PH_SUBS_HIST.FIRST_ACTIVATION_DT as date) < '9999-01-01' )
--*/
INNER JOIN Sky_Calendar Cal
on WH_PH_SUBS_HIST.EFFECTIVE_FROM_DT =Cal.Calendar_date

where
WH_PH_SUBS_HIST.EFFECTIVE_FROM_DT >= Refresh_Dt --is not null and
and WH_PH_SUBS_HIST.SUBSCRIPTION_SUB_TYPE = 'Broadband DSL Line'
and
cast(WH_PH_SUBS_HIST.FIRST_ACTIVATION_DT as date) < '9999-01-01'
AND
(
        (       WH_PH_SUBS_HIST.STATUS_CODE  IN  (  'AB','PC' ) AND WH_PH_SUBS_HIST.PREV_STATUS_CODE IN  ( 'AC','AB','PC' ) )
        OR
        (       WH_PH_SUBS_HIST.STATUS_CODE  = 'AB' AND WH_PH_SUBS_HIST.PREV_STATUS_CODE = 'CF')
        OR
        (       WH_PH_SUBS_HIST.STATUS_CODE  = 'PC' AND WH_PH_SUBS_HIST.PREV_STATUS_CODE = 'PT')

--*********************************************************************************************************************************************************************************
--ADDED FOR TAY CHANGES - MICK PARSLEY 26-06-2013
--*********************************************************************************************************************************************************************************
        OR
        (       WH_PH_SUBS_HIST.STATUS_CODE  = 'BCRQ' AND WH_PH_SUBS_HIST.PREV_STATUS_CODE IN ('AB','PT') AND WH_PH_SUBS_HIST.STATUS_REASON_CODE IN ('900','84'))
        OR
        (       WH_PH_SUBS_HIST.STATUS_CODE  = 'BCRQ' AND WH_PH_SUBS_HIST.PREV_STATUS_CODE IN ('PC','CF') AND WH_PH_SUBS_HIST.STATUS_REASON_CODE IN ('79','80'))
        OR
--Return from BCRQ to a different funnel from start excluding those that switched at point of entry to BCRQ
        (
            PREVPH.PREV_STATUS_CODE = 'AB' AND
            WH_PH_SUBS_HIST.PREV_STATUS_CODE = 'BCRQ'
            AND PREVPH.STATUS_REASON_CODE NOT IN ('900','84')
            AND WH_PH_SUBS_HIST.STATUS_CODE = 'PC'
        )
        OR
        (
            PREVPH.PREV_STATUS_CODE = 'PC' AND
            WH_PH_SUBS_HIST.PREV_STATUS_CODE = 'BCRQ'
            AND PREVPH.STATUS_REASON_CODE NOT IN ('79','80')
            AND WH_PH_SUBS_HIST.STATUS_CODE = 'AB')
        )
--*********************************************************************************************************************************************************************************
--*********************************************************************************************************************************************************************************
AND
WH_PH_SUBS_HIST.STATUS_CODE_CHANGED  =  'Y'
AND
--The hard coded date here is to restrict the amount of data being held in memory when using derived dates and must be set to a date before the earliest date to be reported
--if needed, restriction can be removed
WH_PH_SUBS_HIST.EFFECTIVE_TO_DT >= '2012-06-01 00:00:00';

commit;


--------------------------------------------------------------------------------------
------------------------- Add Future Subs Effective Dt -------------------------------
--------------------------------------------------------------------------------------

--add PC_Future_Effective_Dt
Drop table if exists #PC_Future_Effective_Dt;

Select MoR.account_number,MoR.event_dt,
        csh.status_end_dt status_end_dt,
        csh.future_sub_effective_dt,
        csh.effective_from_datetime,
        csh.effective_to_datetime,
        row_number() over(partition by MoR.account_number,MoR.event_dt order by csh.effective_from_datetime desc) PC_Rnk
into #PC_Future_Effective_Dt
from CITeam.Broadband_Comms_Pipeline MoR
     inner join
     cust_subs_hist csh
     on csh.account_number = MoR.account_number
        and csh.status_start_dt = MoR.Event_dt
        and csh.status_end_dt >= Refresh_dt
        and csh.subscription_sub_type = 'Broadband DSL Line'
--OC: is the constraint OWNING_CUST_ACCOUNT_ID  >  '1' in following line required in the next statements?
        and csh.OWNING_CUST_ACCOUNT_ID  >  '1'
        and csh.STATUS_CODE_CHANGED  =  'Y'
        and csh.status_code  = 'PC'
where
--Same_Day_Cancels > 0 or PC_Pending_Cancellations > 0 or Same_Day_PC_Reactivations > 0
    (MoR.PC_Effective_To_Dt is null or MoR.PC_Future_Sub_Effective_Dt is null)
;

commit;
create hg index idx_1 on #PC_Future_Effective_Dt(account_number);
create date index idx_2 on #PC_Future_Effective_Dt(event_dt);
create lf index idx_3 on #PC_Future_Effective_Dt(PC_Rnk);

Delete from #PC_Future_Effective_Dt where PC_Rnk > 1;

commit;

Update CITeam.Broadband_Comms_Pipeline MoR
Set MoR.PC_Future_Sub_Effective_Dt = PC.future_sub_effective_dt
from CITeam.Broadband_Comms_Pipeline MoR
     inner join
     #PC_Future_Effective_Dt PC
     on PC.account_number = MoR.account_number
        and pc.event_dt = MoR.event_dt;

--add BCRQ_Future_Effective_Dt
Drop table if exists #BCRQ_Future_Effective_Dt;

Select MoR.account_number,MoR.event_dt,
        csh.status_end_dt status_end_dt,
        csh.future_sub_effective_dt,
        csh.effective_from_datetime,
        csh.effective_to_datetime,
        row_number() over(partition by MoR.account_number,MoR.event_dt order by csh.effective_from_datetime desc) PC_Rnk
into #BCRQ_Future_Effective_Dt
from CITeam.Broadband_Comms_Pipeline MoR
     inner join
     cust_subs_hist csh
     on csh.account_number = MoR.account_number
        and csh.status_start_dt = MoR.Event_dt
        and csh.status_end_dt >= Refresh_dt
        and csh.subscription_sub_type = 'Broadband DSL Line'
--OC: is the constraint OWNING_CUST_ACCOUNT_ID  >  '1' in following line required in the next statements?
        and csh.OWNING_CUST_ACCOUNT_ID  >  '1'
        and csh.STATUS_CODE_CHANGED  =  'Y'
        and csh.status_code  = 'BCRQ'
where
--Same_Day_Cancels > 0 or PC_Pending_Cancellations > 0 or Same_Day_PC_Reactivations > 0
    (MoR.BCRQ_Effective_To_Dt is null or MoR.BCRQ_Future_Sub_Effective_Dt is null)
;

commit;
create hg index idx_1 on #BCRQ_Future_Effective_Dt(account_number);
create date index idx_2 on #BCRQ_Future_Effective_Dt(event_dt);
create lf index idx_3 on #BCRQ_Future_Effective_Dt(PC_Rnk);

Delete from #BCRQ_Future_Effective_Dt where PC_Rnk > 1;

commit;

Update CITeam.Broadband_Comms_Pipeline MoR
Set MoR.BCRQ_Future_Sub_Effective_Dt = BCRQ.future_sub_effective_dt
from CITeam.Broadband_Comms_Pipeline MoR
     inner join
     #BCRQ_Future_Effective_Dt BCRQ
     on BCRQ.account_number = MoR.account_number
        and BCRQ.event_dt = MoR.event_dt;

--add AB_Future_Sub_Effective_Dt
Update CITeam.Broadband_Comms_Pipeline MoR
--OC: can we confirm +50 is correct also in the case of BB?
Set MoR.AB_Future_Sub_Effective_Dt = Cast(event_dt + 50 as date)
where STATUS_CODE = 'AB' and
(AB_Future_Sub_Effective_Dt is null);



--------------------------------------------------------------------------------------
--------------------------- Add PC Effective To Dt -----------------------------------
--------------------------------------------------------------------------------------
Select MoR.account_number,
       MoR.event_dt,
       CSH.status_start_dt PC_Effective_To_dt,
       csh.status_code Next_Status_Code,
       Row_number() over(partition by MoR.account_number,MoR.event_dt order by status_start_dt desc) Status_change_rnk
into #PC_Status_Change
from CITeam.Broadband_Comms_Pipeline MoR
    inner join cust_subs_hist CSH
    on CSH.account_number = MoR.account_number
       and CSH.prev_status_start_dt = MoR.event_dt
       and csh.subscription_sub_type = 'Broadband DSL Line'
where csh.status_start_dt >= MoR.event_dt
       and csh.status_end_dt >= Refresh_dt
       and CSH.prev_status_code = 'PC' and CSH.status_code != 'PC' and status_code_changed = 'Y'
       and MoR.status_code = 'PC'
--       and (Same_Day_Cancels > 0 or PC_Pending_Cancellations > 0 or Same_Day_PC_Reactivations > 0)
;


Update CITeam.Broadband_Comms_Pipeline
Set PC_Effective_To_Dt = CSH.PC_Effective_To_dt,
    PC_Next_Status_Code = CSH.Next_Status_Code
from CITeam.Broadband_Comms_Pipeline MoR
     inner join
     #PC_Status_Change CSH
     on CSH.account_number = MoR.account_number
        and CSH.event_dt = MoR.event_dt
where Status_change_rnk = 1
       and MoR.status_code = 'PC';


commit;


--------------------------------------------------------------------------------------
--------------------------- Add AB Effective To Dt -----------------------------------
--------------------------------------------------------------------------------------
Select MoR.account_number,
       MoR.event_dt,
       CSH.status_start_dt AB_Effective_To_dt,
       csh.status_code Next_Status_Code,
       Row_number() over(partition by MoR.account_number,MoR.event_dt order by status_start_dt desc) Status_change_rnk
into #AB_Status_Change
from CITeam.Broadband_Comms_Pipeline MoR
     inner join
     cust_subs_hist CSH
     on CSH.account_number = MoR.account_number
       and CSH.prev_status_start_dt = MoR.event_dt
       and csh.subscription_sub_type = 'Broadband DSL Line'
where csh.status_start_dt >= MoR.event_dt
       and csh.status_end_dt >= Refresh_dt
       and CSH.prev_status_code = 'AB' and CSH.status_code != 'AB' and status_code_changed = 'Y'
       and MoR.status_code = 'AB'
       ;


Update CITeam.Broadband_Comms_Pipeline
Set AB_Effective_To_Dt = CSH.AB_Effective_To_dt,
    AB_Next_Status_Code = CSH.Next_Status_Code
from CITeam.Broadband_Comms_Pipeline MoR
     inner join
     #AB_Status_Change CSH
     on CSH.account_number = MoR.account_number
        and CSH.event_dt = MoR.event_dt
where Status_change_rnk = 1
       and MoR.status_code = 'AB'
;


commit;


--------------------------------------------------------------------------------------
--------------------------- Add BCRQ Effective To Dt -----------------------------------
--------------------------------------------------------------------------------------
Select MoR.account_number,
       MoR.event_dt,
       CSH.status_start_dt AB_Effective_To_dt,
       csh.status_code Next_Status_Code,
       Row_number() over(partition by MoR.account_number,MoR.event_dt order by status_start_dt desc) Status_change_rnk
into #BCRQ_Status_Change
from CITeam.Broadband_Comms_Pipeline MoR
     inner join
     cust_subs_hist CSH
     on CSH.account_number = MoR.account_number
       and CSH.prev_status_start_dt = MoR.event_dt
       and csh.subscription_sub_type = 'Broadband DSL Line'
where csh.status_start_dt >= MoR.event_dt
       and csh.status_end_dt >= Refresh_dt
       and CSH.prev_status_code = 'BCRQ' and CSH.status_code != 'BCRQ' and status_code_changed = 'Y'
       and MoR.status_code = 'BCRQ'
       ;


Update CITeam.Broadband_Comms_Pipeline
Set BCRQ_Effective_To_Dt = CSH.AB_Effective_To_dt,
    BCRQ_Next_Status_Code = CSH.Next_Status_Code
from CITeam.Broadband_Comms_Pipeline MoR
     inner join
     #BCRQ_Status_Change CSH
     on CSH.account_number = MoR.account_number
        and CSH.event_dt = MoR.event_dt
where Status_change_rnk = 1
       and MoR.status_code = 'BCRQ'
;

commit;



GO
