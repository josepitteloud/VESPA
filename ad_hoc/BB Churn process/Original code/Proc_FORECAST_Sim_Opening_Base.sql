/*

dba.sp_drop_table 'CITeam','FORECAST_Base_Sample'
dba.sp_create_table 'CITeam','FORECAST_Base_Sample',
   'account_number varchar(20),'
|| 'end_date date,'
|| 'subs_week_and_year integer,'
|| 'subs_week_of_year tinyint,'
|| 'weekid bigint,'
|| 'DTV_Status_Code varchar(2),'
|| 'BB_Segment varchar(30) default null, '
|| 'prem_segment varchar(7),'
|| 'Simple_Segments varchar(13),'
|| 'country char(3),'
|| 'Affluence varchar(10),'
|| 'package_desc varchar(50),'
|| 'offer_length_DTV varchar(18),'
|| 'curr_offer_start_date_DTV date,'
|| 'Curr_Offer_end_Date_Intended_DTV date,'
|| 'Prev_offer_end_date_DTV date,'
|| 'Time_To_Offer_End_DTV char(28),'
|| 'curr_offer_start_date_BB date,'
|| 'Curr_Offer_end_Date_Intended_BB date,'
|| 'Prev_offer_end_date_BB date,'
|| 'Time_To_Offer_End_BB varchar(28),'
|| 'curr_offer_start_date_LR date,'
|| 'Curr_Offer_end_Date_Intended_LR date,'
|| 'Prev_offer_end_date_LR date,'
|| 'Time_To_Offer_End_LR varchar(28),'
|| 'DTV_BB_LR_offer_end_dt date,'
|| 'Time_To_Offer_End varchar(28),'
|| 'DTV_Tenure varchar(5),'
|| 'dtv_act_date date,'
|| 'Time_Since_Last_TA_call varchar(28),'
|| 'Last_TA_Call_dt date,'
|| 'Time_Since_Last_AB varchar(24),'
|| 'Last_AB_Dt date,'
|| 'Previous_AB_Count varchar(18),'
|| 'Previous_Abs smallint,'
|| 'DTV_PC_Future_Sub_Effective_Dt date default null, '
|| 'DTV_AB_Future_Sub_Effective_Dt date default null, '
|| 'CusCan_Forecast_Segment varchar(100),'
|| 'SysCan_Forecast_Segment varchar(100),'
|| 'DTV_Activation_Type varchar(100),'
|| 'dtv_latest_act_date date,'
|| 'dtv_first_act_date date,'
|| 'HD_segment varchar(70),'

|| 'rand_sample float, '
|| 'sample varchar(10) '





|| 'rand_action_Cuscan float,'
|| 'rand_action_Syscan decimal(20,18),'
|| 'rand_TA_Vol float,'
|| 'rand_WC_Vol float,'
|| 'rand_TA_Save_Vol float,'
|| 'rand_WC_Save_Vol float,'
|| 'rand_TA_DTV_Offer_Applied float,'
|| 'rand_NonTA_DTV_Offer_Applied float,'
|| 'rand_TA_DTV_PC_Vol float,'
|| 'rand_WC_DTV_PC_Vol float,'
|| 'rand_Other_DTV_PC_Vol float'

Select top 1000 * from CITeam.FORECAST_Base_Sample
*/


/*

Create variable Forecast_Start_Wk integer;
Set Forecast_Start_Wk = 201601;
Create variable base_date date;
create variable true_sample_rate float ;
Create Variable multiplier bigint ;
Create variable sample_pct float;

Set sample_pct = 0.25;

-- First you need to impersonate CITeam
Setuser CITeam;
*/
Drop procedure if exists Forecast_Create_Opening_Base;

Create procedure Forecast_Create_Opening_Base(In Forecast_Start_Wk integer,In sample_pct float)
BEGIN

Declare base_date date;
Declare true_sample_rate float;
Declare multiplier  bigint;
Set multiplier = DATEPART(millisecond,now())+738;

SET TEMPORARY OPTION Query_Temp_Space_Limit = 0;


-- create the base week
Select * into #Sky_Calendar from Subs_Calendar(Forecast_Start_Wk/100-1,Forecast_Start_Wk/100);
set base_date = (select max(calendar_date - 7) from #sky_calendar where subs_week_and_year = Forecast_Start_Wk);
-- select base_date;



-- 2.1 Base To Be Simulated
Set multiplier = DATEPART(millisecond,now())+1;

-- drop table if exists #base_sample;
Delete from FORECAST_Base_Sample;

Insert into FORECAST_Base_Sample
select
 account_number
,end_date
,cast(subs_week_and_year as integer)
,subs_week_of_year
,(subs_year-2010)*52+subs_week_of_year as weekid
,DTV_Status_Code
,Case when BB_Active > 0 then 'BB' else 'Non BB' end BB_Segment
,Case when sports > 0 and movies > 0 then 'TopTier'
      when sports > 0                then 'Sports'
      when movies > 0                then 'Movies'
      when DTV_Active = 1            then 'Basic'
end as prem_segment
,case
        when trim(simple_segment) in ('1 Secure')       then '1 Secure'
        when trim(simple_segment) in ('2 Start', '3 Stimulate','2 Stimulate')  then '2 Stimulate'
        when trim(simple_segment) in ('4 Support','3 Support')      then '3 Support'
        when trim(simple_segment) in ('5 Stabilise','4 Stabilise')    then '4 Stabilise'
        else 'Other/Unknown'
end as Simple_Segments
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

--,Cast(null as varchar(100)) as Time_To_Offer_End_BB

,case
  when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*1,0)     then 'M01'
  when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*10,0)    then 'M10'
  when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*14,0)    then 'M14'
  when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*2*12,0)  then 'M24'
  when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*3*12,0)  then 'Y03'
  when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*5*12,0)  then 'Y05'
  when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) >=  round(365/12*5*12,0) then 'Y05+'
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
,Cast(null as date) as DTV_PC_Future_Sub_Effective_Dt
,Cast(null as date) as DTV_AB_Future_Sub_Effective_Dt
,Cast(null as varchar(100)) as CusCan_Forecast_Segment
,Cast(null as varchar(100)) as SysCan_Forecast_Segment
,Cast(null as varchar(100)) as DTV_Activation_Type
-- ,Case when dtv_latest_act_date between (end_date-6) and end_date and dtv_first_act_date < dtv_latest_act_date then 'Reinstate'
--       when dtv_latest_act_date between (end_date-6) and end_date and (dtv_first_act_date = dtv_latest_act_date) then 'Acquisition'
-- End as DTV_Activation_Type
,dtv_latest_act_date
,dtv_first_act_date
,HD_segment
,rand(number(*)*multiplier) as rand_sample
-- ,Cast(null as float) as sample_rnk_prctl
,cast(null as Varchar(10)) as sample

-- into #base_sample
from citeam.cust_fcast_weekly_base --_2015Q4
where end_date = base_date
      and dtv_active =1
      and dtv_act_date is not null
;


Update FORECAST_Base_Sample sample
Set DTV_PC_Future_Sub_Effective_Dt = MoR.PC_Future_Sub_Effective_Dt
from FORECAST_Base_Sample sample
     inner join
     CITeam.Master_of_Retention MoR
     on MoR.account_number = sample.account_number
        and MoR.PC_Future_Sub_Effective_Dt > sample.end_date
        and MoR.event_dt <= sample.end_date
        and (MoR.PC_effective_to_dt >sample.end_date or MoR.PC_effective_to_dt is null)
where sample.DTV_Status_Code = 'PC';

Update FORECAST_Base_Sample sample
Set DTV_Status_Code = 'AC'
where DTV_Status_Code = 'PC' and DTV_PC_Future_Sub_Effective_Dt is null;

Update FORECAST_Base_Sample sample
Set DTV_AB_Future_Sub_Effective_Dt = MoR.AB_Future_Sub_Effective_Dt
from FORECAST_Base_Sample sample
     inner join
     CITeam.Master_of_Retention MoR
     on MoR.account_number = sample.account_number
        and MoR.AB_Future_Sub_Effective_Dt > sample.end_date
        and MoR.event_dt <= sample.end_date
        and (MoR.AB_effective_to_dt >sample.end_date or MoR.AB_effective_to_dt is null)
where sample.DTV_Status_Code = 'AB';

Update FORECAST_Base_Sample sample
Set DTV_Status_Code = 'AC'
where DTV_Status_Code = 'AB' and DTV_AB_Future_Sub_Effective_Dt is null;

Update FORECAST_Base_Sample
Set CusCan_Forecast_Segment = csl.cuscan_forecast_segment
from FORECAST_Base_Sample flt
     inner join
     CITeam.CusCan_Segment_Lookup csl
     on csl.dtv_tenure = flt.dtv_tenure
        and csl.Time_Since_Last_TA_Call = flt.Time_Since_Last_TA_Call
        and csl.Offer_Length_DTV = flt.Offer_Length_DTV
        and csl.Time_To_Offer_End_DTV = flt.Time_To_Offer_End_DTV
        and csl.package_desc = flt.package_desc;


Update FORECAST_Base_Sample flt
Set SysCan_Forecast_Segment = ssl.SysCan_Forecast_Segment
from FORECAST_Base_Sample flt
     inner join
     SysCan_Segment_Lookup ssl
     on ssl.Time_Since_Last_AB = flt.Time_Since_Last_AB
        and ssl.dtv_tenure = flt.dtv_tenure
        and ssl.Affluence = flt.Affluence
        and ssl.simple_segments = flt.simple_segments
        and ssl.Previous_AB_Count = flt.Previous_AB_Count;


--sample to speed up processing
update FORECAST_Base_Sample
set sample = case when rand_sample < sample_pct then 'A' else 'B' end;


-- Select subs_week_and_year, count(*) as n, count(distinct account_number) as d, n-d as dups from Forecast_Loop_Table group by subs_week_and_year;
-- set true_sample_rate = (select sum(case when sample='A' then cast(1 as float) else 0 end)/count(*) from #base_sample);

END;




-- Grant execute rights to the members of CITeam
grant execute on Forecast_Create_Opening_Base to CITeam;
/*
-- Change back to your account
Setuser;

-- Test it
Select top 1000 * from CITeam.Forecast_Create_Opening_Base(201601,0.25);

