/*
dba.sp_drop_table 'CITeam','FORECAST_New_Cust_Sample'
dba.sp_create_table 'CITeam','FORECAST_New_Cust_Sample',
   'end_date date default null, '
|| 'year integer default null,'
|| 'week integer default null,'
|| 'year_week integer default null,'
|| 'account_number varchar(20) default null,'
|| 'dtv_status_code varchar(2) default null,'
|| 'DTV_PC_Future_Sub_Effective_Dt date default null, '
|| 'DTV_AB_Future_Sub_Effective_Dt date default null, '
|| 'BB_Segment varchar(30) default null,'
|| 'prem_segment varchar(7) default null,'
|| 'Simple_Segments varchar(13) default null,'
|| 'country varchar(3) default null,'
|| 'Affluence varchar(10) default null,'
|| 'package_desc varchar(50) default null,'
|| 'offer_length_DTV varchar(18) default null,'
|| 'curr_offer_start_date_DTV date default null,'
|| 'Curr_Offer_end_Date_Intended_DTV date default null,'
|| 'Prev_offer_end_date_DTV date default null,'
|| 'Time_To_Offer_End_DTV varchar(28) default null,'
|| 'curr_offer_start_date_BB date default null,'
|| 'Curr_Offer_end_Date_Intended_BB date default null,'
|| 'Prev_offer_end_date_BB date default null,'
|| 'Time_To_Offer_End_BB varchar(28) default null,'
|| 'curr_offer_start_date_LR date default null,'
|| 'Curr_Offer_end_Date_Intended_LR date default null,'
|| 'Prev_offer_end_date_LR date default null,'
|| 'Time_To_Offer_End_LR varchar(28) default null,'
|| 'DTV_BB_LR_offer_end_dt date default null,'
|| 'Time_To_Offer_End varchar(28) default null,'
|| 'DTV_Tenure varchar(5) default null,'
|| 'dtv_act_date date default null,'
|| 'Time_Since_Last_TA_call varchar(28) default null,'
|| 'Last_TA_Call_dt date default null,'
|| 'Time_Since_Last_AB varchar(24) default null,'
|| 'Last_AB_Dt date default null,'
|| 'Previous_AB_Count varchar(18) default null,'
|| 'Previous_Abs smallint default null,'
|| 'CusCan_Forecast_Segment varchar(100) default null,'
|| 'SysCan_Forecast_Segment varchar(100) default null,'
|| 'DTV_Activation_Type varchar(11) default null,'
|| 'HD_segment varchar(70) default null'

Select top 100 * from CITeam.FORECAST_New_Cust_Sample;
*/

Create variable LV integer;

Set LV = 201601;

Create variable Obs_Dt date;

-- First you need to impersonate CITeam
Setuser CITeam;




-- Drop procedure if exists CITeam.Build_Forecast_New_Cust_Sample;

Create procedure CITeam.Build_Forecast_New_Cust_Sample(In LV integer)
BEGIN

Declare Obs_Dt date;

Delete from CITeam.FORECAST_New_Cust_Sample;

Set Obs_Dt = (Select max(calendar_date) from citeam.subs_calendar(LV/100 -1,LV/100) where Subs_Week_And_Year < LV);

SET TEMPORARY OPTION Query_Temp_Space_Limit = 0;

Insert into CITeam.FORECAST_New_Cust_Sample
select
end_date
,cast(null as integer)  as year
,Cast(null as integer) as week
,Cast(null as integer) as year_week
,account_number
,dtv_status_code
,Cast(null as date) DTV_PC_Future_Sub_Effective_Dt
,Cast(null as date) DTV_AB_Future_Sub_Effective_Dt
,Case when BB_Active > 0 then 'BB' else 'Non BB' end BB_Segment
,Case when sports > 0 and movies > 0 then 'TopTier'
      when sports > 0                then 'Sports'
      when movies > 0                then 'Movies'
      when DTV_Active = 1            then 'Basic'
end prem_segment
,case
        when trim(simple_segment) in ('1 Secure')       then '1 Secure'
        when trim(simple_segment) in ('2 Start', '3 Stimulate','2 Stimulate')  then '2 Stimulate'
        when trim(simple_segment) in ('4 Support','3 Support')      then '3 Support'
        when trim(simple_segment) in ('5 Stabilise','4 Stabilise')    then '4 Stabilise'
        else 'Other/Unknown'
end as Simple_Segments
-- ,simple_segment
,Case when ROI > 0 then 'ROI' else 'UK' end as country
,Affluence_Bands as Affluence
,Case when trim(package_desc) in ('Variety','Kids,Mix,World') or package_desc is null then 'Variety'
      when package_desc is null then 'Original'
      when package_desc = 'Other' then 'Original'
      else package_desc
end package_desc
, case
        when 1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 <= 3  then 'Offer Length 3M'
        when (1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 >3) and (1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 <= 6) then 'Offer Length 6M'
        when (1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 >6) and (1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 <= 9) then 'Offer Length 9M'
        when (1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 >9) and (1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 <= 12) then 'Offer Length 12M'
        when 1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 > 12  then 'Offer Length 12M +'
        when Curr_Offer_end_Date_Intended_DTV is null then 'No Offer'
  end as offer_length_DTV
,curr_offer_start_date_DTV
,Curr_Offer_end_Date_Intended_DTV
,Prev_offer_end_date_DTV
,case
    when Curr_Offer_end_Date_Intended_DTV between (end_date + 1) and (end_date + 7)   then 'Offer Ending in Next 1 Wks'
    when Curr_Offer_end_Date_Intended_DTV between (end_date + 8) and (end_date + 14)  then 'Offer Ending in Next 2-3 Wks'
    when Curr_Offer_end_Date_Intended_DTV between (end_date + 15) and (end_date + 21) then 'Offer Ending in Next 2-3 Wks'
    when Curr_Offer_end_Date_Intended_DTV between (end_date + 22) and (end_date + 28) then 'Offer Ending in Next 4-6 Wks'
    when Curr_Offer_end_Date_Intended_DTV between (end_date + 29) and (end_date + 35) then 'Offer Ending in Next 4-6 Wks'
    when Curr_Offer_end_Date_Intended_DTV between (end_date + 36) and (end_date + 42) then 'Offer Ending in Next 4-6 Wks'
    when Curr_Offer_end_Date_Intended_DTV > (end_date + 42)                           then 'Offer Ending in 7+ Wks'

    when Prev_offer_end_date_DTV between (end_date - 7) and end_date         then 'Offer Ended in last 1 Wks'
    when Prev_offer_end_date_DTV between (end_date - 14) and (end_date - 8)  then 'Offer Ended in last 2-3 Wks'
    when Prev_offer_end_date_DTV between (end_date - 21) and (end_date - 15) then 'Offer Ended in last 2-3 Wks'
    when Prev_offer_end_date_DTV between (end_date - 28) and (end_date - 22) then 'Offer Ended in last 4-6 Wks'
    when Prev_offer_end_date_DTV between (end_date - 35) and (end_date - 29) then 'Offer Ended in last 4-6 Wks'
    when Prev_offer_end_date_DTV between (end_date - 42) and (end_date - 36) then 'Offer Ended in last 4-6 Wks'
    when Prev_offer_end_date_DTV < (end_date - 42)                           then 'Offer Ended 7+ Wks'
    else 'No Offer End DTV'
  end as Time_To_Offer_End_DTV

,curr_offer_start_date_BB
,Curr_Offer_end_Date_Intended_BB
,Prev_offer_end_date_BB
,case
    when Curr_Offer_end_Date_intended_BB between (end_date + 1) and (end_date + 7)   then 'Offer Ending in Next 1 Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 8) and (end_date + 14)  then 'Offer Ending in Next 2-3 Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 15) and (end_date + 21) then 'Offer Ending in Next 2-3 Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 22) and (end_date + 28) then 'Offer Ending in Next 4-6 Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 29) and (end_date + 35) then 'Offer Ending in Next 4-6 Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 36) and (end_date + 42) then 'Offer Ending in Next 4-6 Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 43) and (end_date + 49) then 'Offer Ending in 7+ Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 50) and (end_date + 56) then 'Offer Ending in 7+ Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 57) and (end_date + 63) then 'Offer Ending in 7+ Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 64) and (end_date + 70) then 'Offer Ending in 7+ Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 71) and (end_date + 77) then 'Offer Ending in 7+ Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 78) and (end_date + 84) then 'Offer Ending in 7+ Wks'
    when Curr_Offer_end_Date_intended_BB between (end_date + 85) and (end_date + 91) then 'Offer Ending in 7+ Wks'
    when Curr_Offer_end_Date_intended_BB >= (end_date + 92)                          then 'Offer Ending in 7+ Wks'


    when Prev_offer_end_Date_BB between (end_date - 7) and end_date         then 'Offer Ended in last 1 Wks'
    when Prev_offer_end_Date_BB between (end_date - 14) and (end_date - 8)  then 'Offer Ended in last 2-3 Wks'
    when Prev_offer_end_Date_BB between (end_date - 21) and (end_date - 15) then 'Offer Ended in last 2-3 Wks'
    when Prev_offer_end_Date_BB between (end_date - 28) and (end_date - 22) then 'Offer Ended in last 4-6 Wks'
    when Prev_offer_end_Date_BB between (end_date - 35) and (end_date - 29) then 'Offer Ended in last 4-6 Wks'
    when Prev_offer_end_Date_BB between (end_date - 42) and (end_date - 36) then 'Offer Ended in last 4-6 Wks'
    when Prev_offer_end_Date_BB between (end_date - 49) and (end_date - 43) then 'Offer Ended 7+ Wks'
    when Prev_offer_end_Date_BB between (end_date - 56) and (end_date - 50) then 'Offer Ended 7+ Wks'
    when Prev_offer_end_Date_BB between (end_date - 63) and (end_date - 57) then 'Offer Ended 7+ Wks'
    when Prev_offer_end_Date_BB between (end_date - 70) and (end_date - 64) then 'Offer Ended 7+ Wks'
    when Prev_offer_end_Date_BB between (end_date - 77) and (end_date - 71) then 'Offer Ended 7+ Wks'
    when Prev_offer_end_Date_BB between (end_date - 84) and (end_date - 78) then 'Offer Ended 7+ Wks'
    when Prev_offer_end_Date_BB between (end_date - 91) and (end_date - 85) then 'Offer Ended 7+ Wks'
    when Prev_offer_end_Date_BB <= (end_date - 92)                        then 'Offer Ended 7+ Wks'
    when Prev_offer_end_Date_BB is null then 'Null'
    when Curr_Offer_end_Date_intended_BB is null then 'Null'
    else 'No Offer End BB'
end as Time_To_Offer_End_BB

,curr_offer_start_date_LR
,Curr_Offer_end_Date_Intended_LR
,Prev_offer_end_date_LR
  ,case
    when Curr_Offer_end_Date_Intended_LR between (end_date + 1) and (end_date + 7)   then 'Offer Ending in Next 1 Wks'
    when Curr_Offer_end_Date_Intended_LR between (end_date + 8) and (end_date + 14)  then 'Offer Ending in Next 2-3 Wks'
    when Curr_Offer_end_Date_Intended_LR between (end_date + 15) and (end_date + 21) then 'Offer Ending in Next 2-3 Wks'
    when Curr_Offer_end_Date_Intended_LR between (end_date + 22) and (end_date + 28) then 'Offer Ending in Next 4-6 Wks'
    when Curr_Offer_end_Date_Intended_LR between (end_date + 29) and (end_date + 35) then 'Offer Ending in Next 4-6 Wks'
    when Curr_Offer_end_Date_Intended_LR between (end_date + 36) and (end_date + 42) then 'Offer Ending in Next 4-6 Wks'
    when Curr_Offer_end_Date_Intended_LR > (end_date + 42)                           then 'Offer Ending in 7+ Wks'

    when Prev_offer_end_date_LR between (end_date - 7) and end_date         then 'Offer Ended in last 1 Wks'
    when Prev_offer_end_date_LR between (end_date - 14) and (end_date - 8)  then 'Offer Ended in last 2-3 Wks'
    when Prev_offer_end_date_LR between (end_date - 21) and (end_date - 15) then 'Offer Ended in last 2-3 Wks'
    when Prev_offer_end_date_LR between (end_date - 28) and (end_date - 22) then 'Offer Ended in last 4-6 Wks'
    when Prev_offer_end_date_LR between (end_date - 35) and (end_date - 29) then 'Offer Ended in last 4-6 Wks'
    when Prev_offer_end_date_LR between (end_date - 42) and (end_date - 36) then 'Offer Ended in last 4-6 Wks'
    when Prev_offer_end_date_LR < (end_date - 42)                           then 'Offer Ended 7+ Wks'
    else 'No Offer LR'
  end as Time_To_Offer_End_LR

,DTV_BB_LR_offer_end_dt
,case
    when DTV_BB_LR_offer_end_dt between (end_date + 1) and (end_date + 7)   then 'Offer Ending in Next 1 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date + 8) and (end_date + 14)  then 'Offer Ending in Next 2-3 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date + 15) and (end_date + 21) then 'Offer Ending in Next 2-3 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date + 22) and (end_date + 28) then 'Offer Ending in Next 4-6 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date + 29) and (end_date + 35) then 'Offer Ending in Next 4-6 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date + 36) and (end_date + 42) then 'Offer Ending in Next 4-6 Wks'
    when DTV_BB_LR_offer_end_dt > (end_date + 42)                           then 'Offer Ending in 7+ Wks'

    when DTV_BB_LR_offer_end_dt between (end_date - 7) and end_date         then 'Offer Ended in last 1 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date - 14) and (end_date - 8)  then 'Offer Ended in last 2-3 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date - 21) and (end_date - 15) then 'Offer Ended in last 2-3 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date - 28) and (end_date - 22) then 'Offer Ended in last 4-6 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date - 35) and (end_date - 29) then 'Offer Ended in last 4-6 Wks'
    when DTV_BB_LR_offer_end_dt between (end_date - 42) and (end_date - 36) then 'Offer Ended in last 4-6 Wks'
    when DTV_BB_LR_offer_end_dt < (end_date - 42)                           then 'Offer Ended 7+ Wks'
    else 'No Offer'
  end as Time_To_Offer_End
,case when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*10,0)   then 'M10'
      when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*14,0)    then 'M14'
      when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*2*12,0)  then 'M24'
      when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*3*12,0)  then 'Y03'
      when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*5*12,0)  then 'Y05'
      when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) >=  round(365/12*5*12,0) then 'Y05+'
      else 'YNone'
end as DTV_Tenure
,dtv_act_date
,Case when Last_TA_Call_dt is null then 'No Prev TA Calls'
     when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7  = 0 then '0 Wks since last TA Call'
     when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 = 1 then '01 Wks since last TA Call'
     when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 between 2 and 5 then '02-05 Wks since last TA Call'
     when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 between 6 and 35 then '06-35 Wks since last TA Call'
     when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 between 36 and 41 then '36-46 Wks since last TA Call'
     when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 between 42 and 46 then '36-46 Wks since last TA Call'
     when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 = 47 then '47 Wks since last TA Call'
     when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 between 48 and 52 then '48-52 Wks since last TA Call'
     when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 between 53 and 60 then '53-60 Wks since last TA Call'
     when (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 > 60 then '61+ Wks since last TA Call'
--      when Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer) >= 52*2*7 then 'Last TA > 2 Yrs Ago'
     Else ''
--      (Cast(end_date as integer) - Cast(Last_TA_Call_dt as integer))/7 || ' Wks since last TA Call'
End Time_Since_Last_TA_call
,Last_TA_Call_dt
,Case when  Last_AB_Dt  is null then 'No Prev AB Calls'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 0 then '0 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 1 then '1-2 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 2 then '1-2 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 3 then '3 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 4 then '4 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 5 then '5-7 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 6 then '5-7 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 7 then '5-7 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 8 then '8-12 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 9 then '8-12 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 10 then '8-12 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 11 then '8-12 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 = 12 then '8-12 Mnths since last AB'
     when (Cast(end_date as integer) - Cast(Last_AB_Dt as integer))/31 > 12 then '12+ Mnths since last AB'
     Else ''
end as  Time_Since_Last_AB
,Last_AB_Dt
,case
        when Previous_Abs = 0 then '0 Previous_Abs'
        when Previous_Abs = 1 then '1 Previous_Abs'
        when Previous_Abs = 2 then '2 Previous_Abs'
        when Previous_Abs = 3 then '3 Previous_Abs'
        when Previous_Abs = 4 then '4-7 Previous_Abs'
        when Previous_Abs = 5 then '4-7 Previous_Abs'
        when Previous_Abs = 6 then '4-7 Previous_Abs'
        when Previous_Abs = 7 then '4-7 Previous_Abs'
        when Previous_Abs = 8 then '8-10 Previous_Abs'
        when Previous_Abs = 9 then '8-10 Previous_Abs'
        when Previous_Abs = 10 then '8-10 Previous_Abs'
        when Previous_Abs = 11 then '11-15 Previous_Abs'
        when Previous_Abs = 12 then '11-15 Previous_Abs'
        when Previous_Abs = 13 then '11-15 Previous_Abs'
        when Previous_Abs = 14 then '11-15 Previous_Abs'
        when Previous_Abs = 15 then '11-15 Previous_Abs'
        when Previous_Abs >= 16 then '16 + Previous_Abs'
  else ''
end as Previous_AB_Count
,Previous_Abs
,Cast(null as varchar(100)) as CusCan_Forecast_Segment
,Cast(null as varchar(100)) as SysCan_Forecast_Segment

,Case when dtv_latest_act_date between (end_date-6) and end_date and dtv_first_act_date < dtv_latest_act_date then 'Reinstate'
      when dtv_latest_act_date between (end_date-6) and end_date and (dtv_first_act_date = dtv_latest_act_date) then 'Acquisition'
End as DTV_Activation_Type
,HD_segment
from citeam.cust_fcast_weekly_base
where end_date between Obs_Dt - 5* 7 and Obs_Dt
    and dtv_active =1
    and dtv_latest_act_date between (end_date-6) and end_date -- New customers
    and DTV_Activation_Type is not null
;

Update CITeam.FORECAST_New_Cust_Sample sample
Set DTV_PC_Future_Sub_Effective_Dt = MoR.PC_Future_Sub_Effective_Dt
from CITeam.FORECAST_New_Cust_Sample sample
     inner join
     CITeam.Master_of_Retention MoR
     on MoR.account_number = sample.account_number
        and MoR.PC_Future_Sub_Effective_Dt > sample.end_date
        and MoR.event_dt <= sample.end_date
        and (MoR.PC_effective_to_dt >sample.end_date or MoR.PC_effective_to_dt is null)
where sample.DTV_Status_Code = 'PC';

Update CITeam.FORECAST_New_Cust_Sample sample
Set DTV_AB_Future_Sub_Effective_Dt = MoR.AB_Future_Sub_Effective_Dt
from CITeam.FORECAST_New_Cust_Sample sample
     inner join
     CITeam.Master_of_Retention MoR
     on MoR.account_number = sample.account_number
        and MoR.AB_Future_Sub_Effective_Dt > sample.end_date
        and MoR.event_dt <= sample.end_date
        and (MoR.AB_effective_to_dt >sample.end_date or MoR.AB_effective_to_dt is null)
where sample.DTV_Status_Code = 'AB';

Update CITeam.FORECAST_New_Cust_Sample sample
Set DTV_Status_Code = 'AC'
where DTV_AB_Future_Sub_Effective_Dt is null and DTV_PC_Future_Sub_Effective_Dt is null;

-- sp_columns 'new_customers_sample'

END;



-- Grant execute rights to the members of CITeam
grant execute on CITeam.Build_Forecast_New_Cust_Sample to CITeam;

-- Change back to your account
Setuser;

-- Test it
Call CITeam.Build_Forecast_New_Cust_Sample(201601);

-- Select top 10000 * from CITeam.Build_Forecast_New_Cust_Sample(201601)


