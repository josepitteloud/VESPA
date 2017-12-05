/*

Select top 1000 account_number,end_date,27 as payment_due_day_of_month,DTV_AB,
Case when day(end_date) < payment_due_day_of_month
          then Cast('' || year(dateadd(month,-1,end_date)) || '-' || month(dateadd(month,-1,end_date)) || '-' || payment_due_day_of_month as date)
     when day(end_date) >= payment_due_day_of_month
          then Cast('' || year(end_date) || '-' || month(end_date) || '-' || payment_due_day_of_month as date)
end as Last_Payment_Dt,
Cast(end_date-Case when day(end_date) < payment_due_day_of_month
          then Cast('' || year(dateadd(month,-1,end_date)) || '-' || month(dateadd(month,-1,end_date)) || '-' || payment_due_day_of_month as date)
     when day(end_date) >= payment_due_day_of_month
          then Cast('' || year(end_date) || '-' || month(end_date) || '-' || payment_due_day_of_month as date)
end as integer) as Days_Since_Last_Payment_Dt
from citeam.cust_fcast_weekly_base
where end_date >= '2016-06-30' --payment_due_day_of_month is not null

Select
Cast(end_date-Case when day(end_date) < payment_due_day_of_month
          then Cast('' || year(dateadd(month,-1,end_date)) || '-' || month(dateadd(month,-1,end_date)) || '-' || payment_due_day_of_month as date)
     when day(end_date) >= payment_due_day_of_month
          then Cast('' || year(end_date) || '-' || month(end_date) || '-' || payment_due_day_of_month as date)
end as integer) as Days_Since_Last_Payment_Dt,
count(*) Customers,
sum(DTV_AB) ABs,
Cast(ABs as float)/Customers as DTV_AB_Rate
from citeam.cust_fcast_weekly_base
where end_date >= '2016-06-30' --payment_due_day_of_month is not null
        and dtv_active  > 0
group by Days_Since_Last_Payment_Dt;

/*
dba.sp_drop_table 'CITeam','DTV_FCAST_WEEKLY_BASE'
dba.sp_create_table 'CITeam','DTV_FCAST_WEEKLY_BASE',
   'Downgrade_View varchar(20) default null, '
|| 'Subs_Year smallint default null,'
|| 'Subs_Quarter tinyint default null,'
|| 'Subs_Week smallint default null,'
|| 'Subs_Week_And_Year integer default null,'
|| 'End_Date date default null, '
|| 'Account_Number varchar(20) default null, '
|| 'DTV_Tenure varchar(20) default null, '
|| 'Package_Desc varchar(50) default null, '
|| 'Prem_Segment varchar(30), '
|| 'HD_segment varchar(50) default null, '
|| 'Sports_Tenure varchar(20) default null, '
|| 'Movies_Tenure varchar(20) default null, '
|| 'Simple_Segment varchar(50) default null, '

|| 'Time_Since_Last_TA_call varchar(30) default null, '
|| 'Time_Since_Last_AB varchar(30) default null, '
|| 'Previous_ABs varchar(20) default null, '
|| 'Affluence varchar(10) default null, '
|| 'Country varchar(3) default null, '
|| 'Offer_Length_DTV varchar(30) default null, '
|| 'Time_To_Offer_End_DTV varchar(30) default null, '
|| 'Time_To_Offer_End_BB varchar(30) default null, '
|| 'Time_To_Offer_End_LR varchar(30) default null, '
|| 'Time_To_Offer_End varchar(30) default null, '
|| 'DTV_Status_Code     varchar(2) default null, '
|| 'New_Customer bit default 0, '
|| 'CusCan_Forecast_Segment varchar(50) default null, '
|| 'SysCan_Forecast_Segment varchar(50) default null, '
|| 'Placeholder_1 varchar(50) default null, '
|| 'Placeholder_2 varchar(50) default null, '


|| 'DTV_Active float default 0, '
|| 'BB_Active float default 0, '
|| 'DTV_AB float default 0, '
|| 'TA_Event_Count float default 0, '
|| 'Unique_TA_Caller float default 0, '
|| 'TA_Save_Count float default 0, '
|| 'TA_Non_Save_Count float default 0, '
|| 'Offer_Applied_DTV float default 0, '
|| 'Web_Chat_TA_Cnt float default 0, '
|| 'Web_Chat_TA_Customers float default 0, '
|| 'Web_Chat_TA_Not_Saved float default 0, '
|| 'Web_Chat_TA_Saved float default 0, '

|| 'TA_DTV_Offer_Applied float default 0, '
|| 'TA_DTV_PC float default 0, '
|| 'WC_DTV_PC float default 0, '
|| 'Accessibility_DTV_PC float default 0, '
|| 'Min_Term_PC float default 0, '
|| 'Other_PC float default 0, '
|| 'DTV_PC float default 0, '

|| 'PO_Pipeline_Cancellations float default 0, '
|| 'Same_Day_Cancels float default 0, '
|| 'SC_Gross_Terminations float default 0, '
|| 'TA_Sky_Plus_Save float default 0,'
|| 'WC_Sky_Plus_Save float default 0,'
|| 'DTV_Status_Code_EoW varchar(2) default null '




create hg index idx on CITeam.DTV_FCAST_WEEKLY_BASE(Account_Number);
create lf index idx_1 on CITeam.DTV_FCAST_WEEKLY_BASE(Downgrade_View);
create lf index idx_2 on CITeam.DTV_FCAST_WEEKLY_BASE(Subs_Year);
create lf index idx_3 on CITeam.DTV_FCAST_WEEKLY_BASE(Subs_Quarter);
create lf index idx_4 on CITeam.DTV_FCAST_WEEKLY_BASE(Subs_Week);
create lf index idx_5 on CITeam.DTV_FCAST_WEEKLY_BASE(Subs_Week_And_Year);

create lf index idx_6 on CITeam.DTV_FCAST_WEEKLY_BASE(DTV_Tenure);
create lf index idx_7 on CITeam.DTV_FCAST_WEEKLY_BASE(Package_Desc);
create lf index idx_8 on CITeam.DTV_FCAST_WEEKLY_BASE(Prem_Segment);
create lf index idx_9 on CITeam.DTV_FCAST_WEEKLY_BASE(HD_segment);
create lf index idx_10 on CITeam.DTV_FCAST_WEEKLY_BASE(Simple_Segment);
create lf index idx_11 on CITeam.DTV_FCAST_WEEKLY_BASE(Time_Since_Last_TA_call);
create lf index idx_12 on CITeam.DTV_FCAST_WEEKLY_BASE(Time_Since_Last_AB);

create lf index idx_13 on CITeam.DTV_FCAST_WEEKLY_BASE(Country);
create lf index idx_14 on CITeam.DTV_FCAST_WEEKLY_BASE(Time_To_Offer_End_DTV);
create lf index idx_15 on CITeam.DTV_FCAST_WEEKLY_BASE(Time_To_Offer_End_BB);
create lf index idx_16 on CITeam.DTV_FCAST_WEEKLY_BASE(Time_To_Offer_End_LR);
create lf index idx_17 on CITeam.DTV_FCAST_WEEKLY_BASE(Time_To_Offer_End);
create lf index idx_18 on CITeam.DTV_FCAST_WEEKLY_BASE(Previous_ABs);
create lf index idx_19 on CITeam.DTV_FCAST_WEEKLY_BASE(CusCan_Forecast_Segment);
create lf index idx_20 on CITeam.DTV_FCAST_WEEKLY_BASE(SysCan_Forecast_Segment);

create lf index idx_21 on CITeam.DTV_FCAST_WEEKLY_BASE(End_Date);
create lf index idx_22 on CITeam.DTV_FCAST_WEEKLY_BASE(Placeholder_1);
create lf index idx_23 on CITeam.DTV_FCAST_WEEKLY_BASE(Placeholder_2);

create lf index idx_24 on CITeam.DTV_FCAST_WEEKLY_BASE(_24MF_BB_Offer);
create lf index idx_25 on CITeam.DTV_FCAST_WEEKLY_BASE(Had_Offer_In_Last_Year);

Select top 100 * from CITeam.DTV_FCAST_WEEKLY_BASE
*/
/*
Alter table CITeam.DTV_FCAST_WEEKLY_BASE
Add (DTV_Status_Code_EoW varchar(2) default null);

Had_Offer_In_Last_Year varchar(10) default 'No'
);

Update CITeam.DTV_FCAST_WEEKLY_BASE
Set _24MF_BB_Offer = 'Not On Offer';

Create variable @loop_dt date;
Set @Loop_Dt = (Select max(end_date) from citeam.cust_fcast_weekly_base);
-- Select @Loop_Dt

While @Loop_Dt >= (Select min(end_date) from citeam.cust_fcast_weekly_base) Loop

Update CITeam.DTV_FCAST_WEEKLY_BASE base
Set _24MF_BB_Offer = Case when source.curr_offer_end_date_Intended_BB >= source.end_date then 'On 24MF BB Offer'
                          when source.Prev_offer_end_date_BB > source.end_date - 7*7 then '24MF BB Ended Lst 7W'
                     end
from CITeam.DTV_FCAST_WEEKLY_BASE base
     inner join
     CITeam.Cust_Fcast_Weekly_Base source
     on source.account_number = base.account_number
        and source.end_date = base.end_date
where   base.end_date = @Loop_dt
    and source.end_date = @Loop_dt
    and source.Offer_ID_BB in
(
81069,
81082,
81083,
81084,
81248,
81249,
81250,
81266,
81277,
81286,
81338,
81471,
81692,
82732,
82793
)
    and base.downgrade_view = 'Actuals';

-- Delete from looped_update
Insert into Looped_Update Select @Loop_dt,now();

Set @Loop_Dt = @Loop_Dt - 7;

End Loop




Set @Loop_Dt = (Select max(end_date) from citeam.DTV_fcast_weekly_base where downgrade_view = 'LV 201601 V16');
-- Select @Loop_Dt


While @Loop_Dt >= (Select min(end_date) from citeam.DTV_fcast_weekly_base where downgrade_view = 'LV 201601 V16') Loop

Update CITeam.DTV_FCAST_WEEKLY_BASE base
Set _24MF_BB_Offer = 'Not On Offer'
where end_date = @Loop_dt;

Update CITeam.DTV_FCAST_WEEKLY_BASE base
Set _24MF_BB_Offer = Case when source.curr_offer_end_date_Intended_BB >= base.end_date then 'On 24MF BB Offer'
                          when source.Prev_offer_end_date_BB > base.end_date - 7*7 then '24MF BB Ended Lst 7W'
                          else 'Not On Offer'
                     end
from CITeam.DTV_FCAST_WEEKLY_BASE base
     inner join
     CITeam.Cust_Fcast_Weekly_Base source
     on source.account_number = base.account_number
        and source.end_date = '2016-06-30'
where   base.end_date = @Loop_dt
    and source.end_date = '2016-06-30'
    and source.Offer_ID_BB in
(
81069,
81082,
81083,
81084,
81248,
81249,
81250,
81266,
81277,
81286,
81338,
81471,
81692,
82732,
82793
)
    and base.downgrade_view = 'LV 201601 V16';

-- Delete from looped_update
Insert into Looped_Update Select @Loop_dt,now();

Set @Loop_Dt = @Loop_Dt - 7;

End Loop;

Update CITeam.DTV_FCAST_WEEKLY_BASE base
Set _24MF_BB_Offer = 'Not On Offer'
where _24MF_BB_Offer is NULL;


81069   Broadband Unlimited Free for 24 Months With Sky Sports - Existing UK Customers
81082   Broadband Unlimited Free for 24 Months - Existing UK Customers
81083   Broadband Unlimited Free for 24 Months With Sports - Existing UK Customers
81084   Broadband Unlimited Free for 24 Months with Sports and Movies - Existing UK Customers
81248   Broadband Unlimited Free for 24 Months with Sports and Line Rental (18M New Min Term)
81249   Broadband Unlimited Free for 24 Months with Sports and Line Rental - UK New Customers
81250   Broadband Unlimited Free for 24 Months with Sports and Line Rental (18M New Min Term)
-- 81251        Broadband Connect at 10GBP for 24 Months with Sports and Line Rental (12M New Min Term)
81266   Broadband Unlimited Free for 24 Months with Sports and Line Rental - UK Existing Customers
-- 81267        Broadband Connect at 10GBP for 24 Months with Sports and Line Rental - UK New Customers
81277   Broadband Unlimited Free for 24 Months (12M New Min Term)
81286   Broadband Unlimited free for 24 Months with Sports (18M New Min Term)
-- 81315        Broadband Connect at 10GBP for 24 Months with Sports and Line Rental - UK Existing Customers
81338   Sky Broadband Unlimited Free for 24 Months with Sports and Line Rental - Existing UK Customers
-- 81340        Sky Broadband Connect at 10 GBP for 24 Months with Sports and Line Rental - Existing UK Customers
-- 81434        Broadband Connect at 10 GBP for 24 Months with Sky Sports - Existing UK Customers
81471   Broadband Unlimited Free for 24 Months with Sports (18M New Min Term)
81692   Broadband Unlimited Free for 24 Months with Sky Sports - Existing UK Customers
82732   Broadband Unlimited Free for 24 Months with Line Rental and Sports (18M New Min Term)
82793   Broadband Unlimited Free for 24 Months with Line Rental and Sports (24M New Min Term)










Set @Loop_Dt = (Select max(end_date) from citeam.cust_fcast_weekly_base);

While @Loop_Dt >= (Select min(end_date) from citeam.cust_fcast_weekly_base) Loop

Update CITeam.DTV_FCAST_WEEKLY_BASE base
Set Had_Offer_In_Last_Year = 'No'
where   base.end_date = @Loop_dt

Update CITeam.DTV_FCAST_WEEKLY_BASE base
Set Had_Offer_In_Last_Year = 'Yes'
from CITeam.DTV_FCAST_WEEKLY_BASE base
     inner join
     CITeam.Cust_Fcast_Weekly_Base source
     on source.account_number = base.account_number
        and source.end_date = base.end_date
        and (source.Prev_offer_end_date_DTV >= source.end_date - 365
            or source.Prev_offer_end_date_BB >= source.end_date - 365
            or source.Prev_offer_end_date_LR >= source.end_date - 365)
where   base.end_date = @Loop_dt
    and source.end_date = @Loop_dt
    and base.downgrade_view = 'Actuals';

Update CITeam.DTV_FCAST_WEEKLY_BASE base
Set Had_Offer_In_Last_Year = 'On Offer'
from CITeam.DTV_FCAST_WEEKLY_BASE base
     inner join
     CITeam.Cust_Fcast_Weekly_Base source
     on source.account_number = base.account_number
        and source.end_date = base.end_date
        and (source.curr_offer_end_date_Intended_DTV is not null
            or source.curr_offer_end_date_Intended_BB is not null
            or source.curr_offer_end_date_Intended_LR is not null)
where   base.end_date = @Loop_dt
    and source.end_date = @Loop_dt
    and base.downgrade_view = 'Actuals';


-- Delete from looped_update
Insert into Looped_Update Select @Loop_dt,now();

Set @Loop_Dt = @Loop_Dt - 7;

End Loop



Set @Loop_Dt = (Select min(end_date) from citeam.DTV_fcast_weekly_base where downgrade_view = 'LV 201601 V16');
-- Select @Loop_Dt


While @Loop_Dt <= (Select max(end_date) from citeam.DTV_fcast_weekly_base where downgrade_view = 'LV 201601 V16')
Loop

Update CITeam.DTV_FCAST_WEEKLY_BASE base
Set Had_Offer_In_Last_Year = 'No'
where   base.end_date = @Loop_dt
        and base.downgrade_view = 'LV 201601 V16';

Update CITeam.DTV_FCAST_WEEKLY_BASE base
Set Had_Offer_In_Last_Year =
        Case when source.curr_offer_end_date_Intended_DTV > base.end_date
                    or source.curr_offer_end_date_Intended_BB > base.end_date
                    or source.curr_offer_end_date_Intended_LR > base.end_date
              then 'On Offer'
             when source.Prev_offer_end_date_DTV >= base.end_date - 365
                    or source.Prev_offer_end_date_BB >= base.end_date - 365
                    or source.Prev_offer_end_date_LR >= base.end_date - 365
              then 'Yes'
              else 'No'
        end
from CITeam.DTV_FCAST_WEEKLY_BASE base
     left join
     CITeam.Cust_Fcast_Weekly_Base source
     on source.account_number = base.account_number
        and source.end_date = '2016-06-30'
where   base.end_date = @Loop_dt
    and source.end_date = '2016-06-30'
    and base.downgrade_view = 'LV 201601 V16';

commit;

Update CITeam.DTV_FCAST_WEEKLY_BASE base
Set Had_Offer_In_Last_Year =
        Case when source.curr_offer_end_date_Intended_DTV > source.end_date and base.Had_Offer_In_Last_Year in ('Yes','No') then 'On Offer'
             when source.Prev_offer_end_date_DTV >= source.end_date - 365 and base.Had_Offer_In_Last_Year in ('No') then 'Yes'
             else base.Had_Offer_In_Last_Year
        end
from CITeam.DTV_FCAST_WEEKLY_BASE base
     inner join
     menziesm.FORECAST_Looped_Sim_Output_Platform_201601_V15 source
     on source.account_number = base.account_number
        and source.end_date = base.end_date
where   base.end_date = @Loop_dt
    and source.end_date = @Loop_dt
    and base.downgrade_view = 'LV 201601 V16'
    ;


-- Delete from looped_update
Insert into Looped_Update Select @Loop_dt,now();

Set @Loop_Dt = @Loop_Dt + 7;

End Loop


Select subs_week_and_year,count(*)*4 Customers
from menziesm.FORECAST_Looped_Sim_Output_Platform_201601_V15 source
where curr_offer_end_date_Intended_DTV > end_date
group by subs_week_and_year


Select top 1000 * from citeam.dtv_fcast_weekly_base where end_date = '2016-11-24' and downgrade_view = 'Actuals'

Select Downgrade_View,Subs_Year,Subs_Quarter,subs_week_and_year,Prem_Segment,Case when BB_Active > 0 then 1 else 0 end as BB_Active,Had_Offer_In_Last_Year,_24MF_BB_Offer,Cast(null as varchar(40)) as Karl_Segment
,sum(DTV_Active) --count(*)* Case when Downgrade_View = 'Actuals' then 1 else 4 end
    as Customers
,sum(TA_Event_Count)TA_Events
into --drop table
Karl_TA_Events
from citeam.dtv_fcast_weekly_base
-- where subs_week_and_year between 201601 and 201652
group by Downgrade_View,Subs_Year,Subs_Quarter,subs_week_and_year,Prem_Segment,BB_Active,Had_Offer_In_Last_Year,_24MF_BB_Offer;


Update Karl_TA_Events
Set _24MF_BB_Offer = 'Not On Offer'
where _24MF_BB_Offer is NULL;


Update Karl_TA_Events
Set Karl_Segment = Case when Had_Offer_In_Last_Year = 'On Offer' and _24MF_BB_Offer in ('On 24MF BB Offer','24MF BB Ended Lst 7W')   then '24 MF Offer'
                        when Had_Offer_In_Last_Year = 'On Offer'                                                                     then 'On Non 24MF BB Offer'
                        when Had_Offer_In_Last_Year = 'Yes'                                                                           then 'Had Offer in Last Yr'
                        when Had_Offer_In_Last_Year = 'No' and Prem_Segment in ('Sports','TopTier') and BB_Active = 1                then 'Sports & BB'
                        when Had_Offer_In_Last_Year = 'No' and Prem_Segment in ('Basic') and BB_Active = 1                           then 'Basic Trple Play'
                        else 'Other - Not on offer'
                    end;


Select *
-- ,Count(distinct Subs_week_and_year) over(partition by subs_year,subs_quarter_of_year) Wks_In_Qtr
from Karl_TA_Events
where downgrade_view = 'LV 201601 V15'
        or (Downgrade_View = 'Actuals' and subs_week_and_year <= 201616)














*/
/*
Call CITEAM.UPDATE_DTV_FCAST_WEEKLY_BASE('2016-11-03','2016-12-08')
---------------------------------------------------------------------------------------------
-- Create Procedure to insert actuals into DTV fcast base table -----------------------------
---------------------------------------------------------------------------------------------
-- First you need to impersonate CITeam
Setuser CITeam;


*/


Drop Procedure if exists UPDATE_DTV_FCAST_WEEKLY_BASE;
Create procedure UPDATE_DTV_FCAST_WEEKLY_BASE(IN Start_End_Dt date,IN End_End_Date date)
SQL SECURITY DEFINER


BEGIN
SET TEMPORARY OPTION Query_Temp_Space_Limit = 0;

Delete from DTV_FCAST_WEEKLY_BASE
where end_date between Start_End_Dt and End_End_Date
-- in (Select distinct end_date from citeam.cust_fcast_weekly_update)
        and Downgrade_View = 'Actuals';

Insert into DTV_FCAST_WEEKLY_BASE(Downgrade_View,End_Date,Account_Number,Subs_Year,Subs_Quarter,Subs_Week,Subs_Week_And_Year
,DTV_Status_Code
,Affluence
,HD_Segment
,Prem_Segment
,sports_tenure
,movies_tenure
,Simple_Segment
,Package_Desc
,Country
,Offer_Length_DTV
,Time_To_Offer_End_DTV
,Time_To_Offer_End_BB
,Time_To_Offer_End_LR
,Time_To_Offer_End
,DTV_Tenure
,Time_Since_Last_TA_call
,Time_Since_Last_AB
,Previous_ABs

,New_Customer,CusCan_Forecast_Segment,SysCan_Forecast_Segment
,DTV_Active
,BB_Active
,DTV_AB
,TA_Event_Count
,Unique_TA_Caller
,TA_Save_Count
,TA_Non_Save_Count
,Offer_Applied_DTV

,Web_Chat_TA_Cnt
,Web_Chat_TA_Customers
,Web_Chat_TA_Not_Saved
,Web_Chat_TA_Saved

,TA_DTV_Offer_Applied
,TA_DTV_PC
,WC_DTV_PC
,Accessibility_DTV_PC
,Min_Term_PC
,Other_PC
,DTV_PC
,PO_Pipeline_Cancellations
,Same_Day_Cancels
,SC_Gross_Terminations

)
Select

'Actuals' as Downgrade_View
,End_Date
,Account_Number
,null as Subs_Year
,null as Subs_Quarter
,null as Subs_Week
,null as Subs_Week_And_Year

,DTV_Status_Code
,affluence_bands as Affluence
,HD_Segment
,Case when sports > 0 and movies > 0 then 'TopTier'
      when sports > 0                then 'Sports'
      when movies > 0                then 'Movies'
      when DTV_Active = 1            then 'Basic'
 end as Prem_Segment

,Case when (end_date - sports_act_date) <=  730 then 'A.<2 Yrs'
      when (end_date - sports_act_date) <= 1825 then 'B.<5 Yrs'
      when (end_date - sports_act_date) <= 3650 then 'C.<10 Yrs'
      when ( end_date- sports_act_date) >  3650 then 'D.10+ Yrs'
      else null
 end as sports_tenure
,Case when (end_date - movies_act_date) <=  730 then 'A.<2 Yrs'
      when (end_date - movies_act_date) <= 1825 then 'B.<5 Yrs'
      when (end_date - movies_act_date) <= 3650 then 'C.<10 Yrs'
      when ( end_date- movies_act_date) >  3650 then 'D.10+ Yrs'
      else null
 end as Movies_tenure

 ,case
    when trim(simple_segment) in ('1 Secure')       then '1 Secure'
    when trim(simple_segment) in ('2 Start', '3 Stimulate','2 Stimulate')  then '2 Stimulate'
    when trim(simple_segment) in ('4 Support','3 Support')      then '3 Support'
    when trim(simple_segment) in ('5 Stabilise','4 Stabilise')    then '4 Stabilise'
    else 'Other/Unknown'
 end as Simple_Segment

,Case when trim(package_desc) in ('Variety','Kids,Mix,World') or package_desc is null then 'Variety'
      when package_desc is null then 'Original'
      when package_desc = 'Other' then 'Original'
      else package_desc
 end Package_Desc

,Case when ROI > 0 then 'ROI' else 'UK' end as Country

, case
    when 1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 <= 3  then 'Offer Length 3M'
    when (1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 >3) and (1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 <= 6) then 'Offer Length 6M'
    when (1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 >6) and (1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 <= 9) then 'Offer Length 9M'
    when (1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 >9) and (1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 <= 12) then 'Offer Length 12M'
    when 1+ (Curr_Offer_end_Date_Intended_DTV - curr_offer_start_date_DTV) / 31 > 12  then 'Offer Length 12M +'
    when Curr_Offer_end_Date_Intended_DTV is null then 'No Offer'
    when curr_offer_start_date is null then 'No Offer'
 end as Offer_Length_DTV

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
    else 'No Offer End LR'
  end as Time_To_Offer_End_LR

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
,case
  when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*1,0)     then 'M01'
  when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*10,0)    then 'M10'
  when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*14,0)    then 'M14'
  when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*2*12,0)  then 'M24'
  when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*3*12,0)  then 'Y03'
  when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) <  round(365/12*5*12,0)  then 'Y05'
  when  Cast(end_date as integer)  - Cast(dtv_act_date as integer) >=  round(365/12*5*12,0) then 'Y05+'
end as DTV_Tenure
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
 Else ''
End Time_Since_Last_TA_call

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

,case
    when base.Previous_Abs = 0 then '0 Previous_Abs'
    when base.Previous_Abs = 1 then '1 Previous_Abs'
    when base.Previous_Abs = 2 then '2 Previous_Abs'
    when base.Previous_Abs = 3 then '3 Previous_Abs'
    when base.Previous_Abs = 4 then '4-7 Previous_Abs'
    when base.Previous_Abs = 5 then '4-7 Previous_Abs'
    when base.Previous_Abs = 6 then '4-7 Previous_Abs'
    when base.Previous_Abs = 7 then '4-7 Previous_Abs'
    when base.Previous_Abs = 8 then '8-10 Previous_Abs'
    when base.Previous_Abs = 9 then '8-10 Previous_Abs'
    when base.Previous_Abs = 10 then '8-10 Previous_Abs'
    when base.Previous_Abs = 11 then '11-15 Previous_Abs'
    when base.Previous_Abs = 12 then '11-15 Previous_Abs'
    when base.Previous_Abs = 13 then '11-15 Previous_Abs'
    when base.Previous_Abs = 14 then '11-15 Previous_Abs'
    when base.Previous_Abs = 15 then '11-15 Previous_Abs'
    when base.Previous_Abs >= 16 then '16 + Previous_Abs'
  else ''
end as Previous_ABs


,case when dtv_act_date between (end_date-6) and end_date then 1 else 0 end as New_Customer
,null as CusCan_Forecast_Segment
,null as SysCan_Forecast_Segment

,DTV_Active
,BB_Active
,DTV_AB
,TA_Call_Count as TA_Event_Count
,TA_Call_Flag as Unique_TA_Caller
,TA_Save_Count
,TA_Call_Count - base.TA_Save_Count as TA_Non_Save_Count
,Cast(0 as tinyint) Offer_Applied_DTV
-- case when base.offer_applied_DTV > 0
--                 and base.dtv_active=1
--                 and base.DTV_PC = 0
--                 and base.DTV_AB = 0
--                 and base.SC_Gross_Terminations = 0
--                 and base.PO_Pipeline_Cancellations = 0
--       then 1
--       else 0
-- end as Offer_Applied_DTV

,Coalesce(WebChat_TA_Saved,0) + Coalesce(WebChat_TA_Not_Saved,0) Web_Chat_TA_Cnt
,Case when Coalesce(WebChat_TA_Saved,0) > 0  or  Coalesce(WebChat_TA_Not_Saved,0) > 0 then 1 else 0 end as Web_Chat_TA_Customers
,Coalesce(WebChat_TA_Not_Saved,0) as Web_Chat_TA_Not_Saved
,Coalesce(WebChat_TA_Saved,0) as Web_Chat_TA_Saved

-- ,case when base.offer_applied_DTV > 0
--                 and base.dtv_active=1
--                 and base.DTV_PC = 0
--                 and base.DTV_AB = 0
--                 and base.SC_Gross_Terminations = 0
--                 and base.PO_Pipeline_Cancellations = 0
--                 and base.ta_call_flag > 0
--       then 1
--       else 0
-- end as
,Cast(0 as tinyint) as TA_DTV_Offer_Applied
,TA_DTV_PC
,WC_DTV_PC
,Accessibility_DTV_PC
,Min_Term_PC
,Other_PC
,DTV_PC
,PO_Pipeline_Cancellations
,Same_Day_Cancels
,SC_Gross_Terminations
from citeam.cust_fcast_weekly_base base
where end_date between Start_End_Dt and End_End_Date
-- in (Select distinct end_date from citeam.cust_fcast_weekly_update)
        and end_date < today()-datepart(weekday,today()+2)
        and DTV_Active = 1
        and dtv_act_date is not null;

-- Update CITEAM.DTV_FCAST_WEEKLY_BASE base
-- Set TA_DTV_Offer_Applied = 1
-- from CITEAM.DTV_FCAST_WEEKLY_BASE base
--      inner join
--      CITeam.Combined_Retention_Report crr
--      on crr.account_number = base.account_number
--         and crr.event_dt between base.end_date + 1 and base.end_date + 7
-- where base.end_date in (Select distinct end_date from citeam.cust_fcast_weekly_update)

Update DTV_FCAST_WEEKLY_BASE base
Set --Offer_Applied_DTV = 0,
    TA_DTV_Offer_Applied = 0
where --base.end_date between Start_End_Dt and End_End_Date
        --and
downgrade_view = 'Actuals';


-- Update CITEAM.DTV_FCAST_WEEKLY_BASE base
-- Set Offer_Applied_DTV = 1
-- from CITEAM.DTV_FCAST_WEEKLY_BASE base
--      inner join
--      CITeam.offer_usage_all oua
--      on oua.account_number = base.account_number
--         and oua.offer_start_dt_Actual between base.end_date + 1 and base.end_date + 7
--         and oua.offer_start_dt_Actual = oua.Whole_offer_start_dt_Actual
--         and oua.offer_end_dt_Actual > oua.offer_start_dt_Actual
--         and oua.subs_type = 'DTV Primary Viewing'
--         and lower(oua.offer_dim_description) not like '%price protection%'
-- where base.end_date between Start_End_Dt and End_End_Date
--         and
-- downgrade_view = 'Actuals';
-- 


drop table if exists #TA_DTV_Offer_Applied;
Select crr.account_number,
       crr.event_dt - datepart(weekday,crr.event_dt+2) end_date
into #TA_DTV_Offer_Applied
from CITeam.Combined_Retention_Report crr
     inner join
     CITeam.offer_usage_all oua
     on oua.account_number = crr.account_number
        and oua.offer_start_dt_Actual = crr.event_dt
        and oua.offer_start_dt_Actual = oua.Whole_offer_start_dt_Actual
        and oua.offer_end_dt_Actual > oua.offer_start_dt_Actual
        and oua.subs_type = 'DTV Primary Viewing'
        and lower(oua.offer_dim_description) not like '%price protection%'
        and crr.TA_Channel = 'Voice'
-- where crr.event_dt > Start_End_Dt
group by crr.account_number,
         end_date
;

commit;

Update CITEAM.DTV_FCAST_WEEKLY_BASE base
Set TA_DTV_Offer_Applied = 1
from CITEAM.DTV_FCAST_WEEKLY_BASE base
     inner join
     #TA_DTV_Offer_Applied oua
     on oua.account_number = base.account_number
        and oua.end_date = base.end_date
where downgrade_view = 'Actuals';


Select *
into #Subs_Calendar
from CITeam.Subs_Calendar(2012,2020);

commit;

Create date index idx_1 on #Subs_Calendar(calendar_date);
-- Create lf index idx_2 on #Proc_Sky_Calendar(Subs_Year);
-- Create lf index idx_3 on #Proc_Sky_Calendar(Subs_Week_of_Year);
-- Create lf index idx_4 on #Proc_Sky_Calendar(Subs_Quarter_of_Year);
-- Create lf index idx_5 on #Proc_Sky_Calendar(Subs_Last_Day_Of_Week);

Update CITEAM.DTV_FCAST_WEEKLY_BASE Outer_Base_Table
Set CusCan_Forecast_Segment = case when Outer_Base_Table.DTV_status_code = 'AC' then csl.cuscan_forecast_segment else Outer_Base_Table.DTV_status_code end
from CITEAM.DTV_FCAST_WEEKLY_BASE Outer_Base_Table
    inner join
    CITeam.CusCan_Segment_Lookup csl
    on csl.dtv_tenure = Outer_Base_Table.DTV_tenure
        and csl.Time_Since_Last_TA_Call = Outer_Base_Table.Time_Since_Last_TA_Call
        and csl.Offer_Length_DTV = Outer_Base_Table.Offer_Length_DTV
        and csl.Time_To_Offer_End_DTV = Outer_Base_Table.Time_To_Offer_End_DTV
        and csl.package_desc = Outer_Base_Table.package_desc
        and csl.Country = Outer_Base_Table.Country
where Outer_Base_Table.CusCan_Forecast_Segment is null
        and Downgrade_View = 'Actuals'
;

Update CITEAM.DTV_FCAST_WEEKLY_BASE Outer_Base_Table
Set SysCan_Forecast_Segment = case when Outer_Base_Table.DTV_status_code = 'AC' then ssl.Syscan_forecast_segment else Outer_Base_Table.DTV_status_code end
from CITEAM.DTV_FCAST_WEEKLY_BASE Outer_Base_Table
     inner join
     CITeam.SysCan_Segment_Lookup ssl
     on ssl.Time_Since_Last_AB = Outer_Base_Table.Time_Since_Last_AB
        and ssl.dtv_tenure = Outer_Base_Table.DTV_Tenure
        and ssl.Affluence = Outer_Base_Table.Affluence
        and ssl.simple_segments = Outer_Base_Table.simple_segment
        and ssl.Previous_AB_Count = Outer_Base_Table.Previous_ABs
where Outer_Base_Table.SysCan_Forecast_Segment is null
        and Downgrade_View = 'Actuals';

Select * into #Segment_Base from Simmonsr. REVISED_REDDY_q3_v2_forecast_base; -- simmonsr.REVISED_REDDY_q3_forecast_base;

Update CITeam.DTV_FCAST_WEEKLY_BASE a
Set Placeholder_1 = b.new_segment1
from CITeam.DTV_FCAST_WEEKLY_BASE a
     inner join
     #Segment_Base b
     on a.account_number = b.account_number
where a.subs_week_and_year >= 201614
        and a.end_date between Start_End_Dt and End_End_Date
        and Downgrade_View = 'Actuals';


--------------------------------------------------------------
--- Events from customers not in the base at start of week ---
--------------------------------------------------------------
Insert into CITEAM.DTV_FCAST_WEEKLY_BASE
(
Downgrade_View,Subs_Year,Subs_Quarter,Subs_Week,Subs_Week_And_Year,
New_Customer,
TA_Event_Count,Unique_TA_Caller,TA_Save_Count,TA_Non_Save_Count
)

Select
'Actuals' as Downgrade_View,
crr.subs_year,
Case when crr.subs_week_and_year % 100 between  1 and 13 then 1
     when crr.subs_week_and_year % 100 between 14 and 26 then 2
     when crr.subs_week_and_year % 100 between 27 and 39 then 3
     when crr.subs_week_and_year % 100 between 40 and 53 then 4
end as Subs_Quarter,
crr.subs_week_and_year % 100 as Subs_Week,
crr.subs_week_and_year,
1 as New_Customer,
sum(turnaround_saved+turnaround_not_saved) TA_Event_Count,
Count(distinct crr.account_number) Unique_TA_Caller,
sum(turnaround_saved) TA_Save_Count,
sum(turnaround_not_saved) TA_Non_Save_Count
from citeam.combined_retention_report crr
     left join
     CITeam.DTV_Fcast_Weekly_Base base
     on base.account_number = crr.account_number
        and Cast(crr.event_dt - datepart(weekday,event_dt+2) as date) = base.end_date
        and base.downgrade_view = 'Actuals'
where crr.event_dt - datepart(weekday,event_dt+2) between Start_End_Dt and Start_End_Dt
    and crr.TA_channel = 'Voice'
    and base.account_number is null
group by crr.subs_year,crr.subs_week_and_year
;

-- Delete from CITEAM.DTV_FCAST_WEEKLY_BASE where downgrade_view = 'Actuals' and end_date is null

-- Select * into #Subs_Calendar from citeam.subs_calendar (2012,2016);
--
Update CITEAM.DTV_FCAST_WEEKLY_BASE base
Set Subs_Year = Cast(sc.Subs_year as integer)
,Subs_Quarter = Cast(sc.Subs_quarter_of_year as integer)
,Subs_Week = Cast(sc.Subs_week_of_year as integer)
,Subs_Week_And_Year = Cast(sc.Subs_Week_and_year as integer)
from CITEAM.DTV_FCAST_WEEKLY_BASE base
     inner join
     #Subs_Calendar as sc
     on sc.calendar_date = base.end_date + 7
where base.Subs_Year is null
;


END;



-- Grant execute rights to the members of CITeam
grant execute on UPDATE_DTV_FCAST_WEEKLY_BASE to CITeam;
/*
-- Test it
     Execute CITeam.UPDATE_DTV_FCAST_WEEKLY_BASE;

-- Change back to your account
     Setuser;
	 














-----------------------------------------------------------------------------
-- Create Procedure to insert forecast into DTV fcast base table ------------
-----------------------------------------------------------------------------
-- First you need to impersonate CITeam
Setuser CITeam;

Create variable var_Downgrade_View varchar(20); Set var_Downgrade_View = 'LV 201601 V15';
Create variable Sample_Rate float; Set  Sample_Rate = 0.25;
Create variable  Forecast_Start_Wk integer; Set Forecast_Start_Wk = 201601;
	 */
 Drop Procedure if exists INSERT_LV_INTO_DTV_FCAST_WEEKLY_BASE;
Create procedure INSERT_LV_INTO_DTV_FCAST_WEEKLY_BASE(IN var_Downgrade_View varchar(20),IN Sample_Rate float,IN Forecast_Start_Wk integer)
SQL SECURITY INVOKER

BEGIN

If var_Downgrade_View = 'Actuals' then return end if;

-- Declare var_Downgrade_View as varchar(20));
-- Set var_Downgrade_View = 'LV 201601 V13'
-- sp_columns 'DTV_FCAST_WEEKLY_BASE'


Delete from DTV_FCAST_WEEKLY_BASE
where trim(Downgrade_View) = var_Downgrade_View;

Insert into DTV_FCAST_WEEKLY_BASE
(Downgrade_View
,End_Date
,Account_Number
,Subs_Year
,Subs_Quarter
,Subs_Week
,Subs_Week_And_Year
,DTV_Status_Code
,Affluence
,HD_Segment
,Prem_Segment
,sports_tenure
,movies_tenure
,Simple_Segment
,Package_Desc
,Country
,Offer_Length_DTV
,Time_To_Offer_End_DTV
,Time_To_Offer_End_BB
,Time_To_Offer_End_LR
,Time_To_Offer_End
,DTV_Tenure
,Time_Since_Last_TA_call
,Time_Since_Last_AB
,Previous_ABs

,New_Customer
,CusCan_Forecast_Segment
,SysCan_Forecast_Segment
,DTV_Active
,BB_Active
,DTV_AB
,TA_Event_Count
,Unique_TA_Caller
,TA_Save_Count
,TA_Non_Save_Count
,Offer_Applied_DTV

,Web_Chat_TA_Cnt
,Web_Chat_TA_Customers
,Web_Chat_TA_Not_Saved
,Web_Chat_TA_Saved

,TA_DTV_Offer_Applied
,TA_DTV_PC
,WC_DTV_PC
,Accessibility_DTV_PC
,Min_Term_PC
,Other_PC
,DTV_PC
,PO_Pipeline_Cancellations
,Same_Day_Cancels
,SC_Gross_Terminations
,DTV_Status_Code_EoW
)

Select
 var_Downgrade_View as Downgrade_View
,End_Date
,Account_Number
,Subs_Week_And_Year/100 as Subs_Year
,Case when subs_week_of_year between 1  and 13 then 1
      when subs_week_of_year between 14 and 26 then 2
      when subs_week_of_year between 27 and 39 then 3
      when subs_week_of_year between 40 and 53 then 4
 end Subs_Quarter
,subs_week_of_year as Subs_Week
,Subs_Week_And_Year

,null as DTV_Status_Code
,Affluence
,HD_Segment
,Prem_Segment
,null as sports_tenure
,null as Movies_tenure
,Simple_Segments as Simple_Segment
,Package_Desc
,Country
,Offer_Length_DTV

,Time_To_Offer_End_DTV
,null as Time_To_Offer_End_BB
,null as Time_To_Offer_End_LR
,null as Time_To_Offer_End
,DTV_Tenure
,Time_Since_Last_TA_call
,Time_Since_Last_AB
,Previous_AB_Count as Previous_ABs

,case when dtv_latest_act_date between end_date + 1 and end_date + 7 then 1 else 0 end as New_Customer
,CusCan_Forecast_Segment
,SysCan_Forecast_Segment

,Cast(Case when DTV_Activation_Type is null then 1 else 0 end as float)/Sample_Rate as DTV_Active
,Cast(Case when BB_Segment = 'BB' then 1 else 0 end as float)/Sample_Rate as BB_Active
,Cast(base.DTV_AB as float)/Sample_Rate as DTV_AB
,Cast(base.TA_Call_Count as float)/Sample_Rate as TA_Event_Count
,Cast(base.TA_Call_Cust as float)/Sample_Rate as Unique_TA_Caller
,Cast(base.TA_Saves as float)/Sample_Rate as TA_Save_Count
,Cast(base.TA_Call_Count - base.TA_Saves as float)/Sample_Rate as TA_Non_Save_Count
,Cast(base.DTV_Offer_Applied as float)/Sample_Rate as Offer_Applied_DTV

,Cast(base.WC_Call_Count as float)/Sample_Rate as Web_Chat_TA_Cnt
,Cast(base.WC_Call_Cust as float)/Sample_Rate as Web_Chat_TA_Customers
,Cast(base.WC_Call_Count - base.WC_Call_Cust as float)/Sample_Rate as Web_Chat_TA_Not_Saved
,Cast(base.WC_Saves as float)/Sample_Rate as  Web_Chat_TA_Saved

,Cast(case when base.DTV_Offer_Applied > 0
--                 and base.dtv_active=1
                and base.TA_DTV_PC = 0
                and base.WC_DTV_PC = 0
--                 and base.TA_Sky_Plus_Save = 0
--                 and base.WC_Sky_Plus_Save = 0
--                 and base.Other_PC > 0
                and base.DTV_AB = 0
                and base.SysCan = 0
                and base.CusCan = 0
                and base.TA_Call_Cust > 0
      then 1
      else 0
end as float)/Sample_Rate as TA_DTV_Offer_Applied
,Cast(base.TA_DTV_PC as float)/Sample_Rate
,Cast(base.WC_DTV_PC as float)/Sample_Rate
,Cast(null as float)/Sample_Rate as Accessibility_DTV_PC
,Cast(null as float)/Sample_Rate as Min_Term_PC
,Cast(base.Other_DTV_PC as float)/Sample_Rate as Other_DTV_PC
,Cast(null as float)/Sample_Rate as DTV_PC
,Cast(base.CusCan as float)/Sample_Rate as CusCan
,Cast(null as float)/Sample_Rate as Same_Day_Cancels
,Cast(base.SysCan as float)/Sample_Rate as SysCan
,DTV_Status_Code_EoW

from /*citeam.*/ FORECAST_Looped_Sim_Output_Platform base
;





Drop table if exists #Fcast_New_Cust_TA_Events;
Select
end_date,
subs_year,
Subs_Quarter,
Subs_Week,
subs_week_and_year,
New_Customer,
TA_Event_Count,
Unique_TA_Caller,
TA_Save_Count,
TA_Non_Save_Count,
Dense_Rank() over(partition by Subs_Week order by Subs_Year desc) Week_Rnk
into #Fcast_New_Cust_TA_Events
from DTV_FCAST_WEEKLY_BASE
where account_number is null and downgrade_view = 'Actuals' and New_Customer = 1
        and Subs_Week_And_Year < Forecast_Start_Wk;

Drop table if exists #Wks;
Select distinct subs_year,Subs_Quarter,Subs_Week,subs_week_and_year
into #Wks
from DTV_FCAST_WEEKLY_BASE
where downgrade_view = var_Downgrade_View;

Insert into DTV_FCAST_WEEKLY_BASE
(
Downgrade_View,Subs_Year,Subs_Quarter,Subs_Week,Subs_Week_And_Year,
New_Customer,
TA_Event_Count,Unique_TA_Caller,TA_Save_Count,TA_Non_Save_Count
)
Select
var_Downgrade_View as Downgrade_View,
base.subs_year,
base.Subs_Quarter,
base.Subs_Week,
base.subs_week_and_year,
New_Cust.New_Customer,
New_Cust.TA_Event_Count,
New_Cust.Unique_TA_Caller,
New_Cust.TA_Save_Count,
New_Cust.TA_Non_Save_Count
from #Fcast_New_Cust_TA_Events New_Cust
     inner join
     #Wks base
     on (New_Cust.Subs_Week = base.Subs_Week or (New_Cust.Subs_Week = 52 and base.Subs_Week = 53))
            and Week_Rnk = 1
;

END;

-- Grant execute rights to the members of CITeam
grant execute on INSERT_LV_INTO_DTV_FCAST_WEEKLY_BASE to CITeam;
/*
-- Change back to your account
     Setuser;

-- Test it
     Call CITeam.INSERT_LV_INTO_DTV_FCAST_WEEKLY_BASE('LV201601 V16',0.25,201601);

Update CITEAM.DTV_FCAST_WEEKLY_BASE base
Set DTV_Tenure = null;

commit;
Update CITEAM.DTV_FCAST_WEEKLY_BASE base
Set DTV_Tenure =
case
  when  Cast(source.end_date as integer)  - Cast(source.dtv_act_date as integer) <  round(365/12*1,0)     then 'M01'
  when  Cast(source.end_date as integer)  - Cast(source.dtv_act_date as integer) <  round(365/12*10,0)    then 'M10'
  when  Cast(source.end_date as integer)  - Cast(source.dtv_act_date as integer) <  round(365/12*14,0)    then 'M14'
  when  Cast(source.end_date as integer)  - Cast(source.dtv_act_date as integer) <  round(365/12*2*12,0)  then 'M24'
  when  Cast(source.end_date as integer)  - Cast(source.dtv_act_date as integer) <  round(365/12*3*12,0)  then 'Y03'
  when  Cast(source.end_date as integer)  - Cast(source.dtv_act_date as integer) <  round(365/12*5*12,0)  then 'Y05'
  when  Cast(source.end_date as integer)  - Cast(source.dtv_act_date as integer) >=  round(365/12*5*12,0) then 'Y05+'
end
from CITEAM.DTV_FCAST_WEEKLY_BASE base
     inner join
     CITEAM.cust_FCAST_WEEKLY_BASE source
     on source.account_number = base.account_number
        and source.end_date = base.end_date;

Update CITEAM.DTV_FCAST_WEEKLY_BASE Outer_Base_Table
Set CusCan_Forecast_Segment = null
where Downgrade_View = 'Actuals';

Update CITEAM.DTV_FCAST_WEEKLY_BASE Outer_Base_Table
Set CusCan_Forecast_Segment = case when Outer_Base_Table.DTV_status_code = 'AC' then csl.cuscan_forecast_segment else Outer_Base_Table.DTV_status_code end
from CITEAM.DTV_FCAST_WEEKLY_BASE Outer_Base_Table
    inner join
    CITeam.CusCan_Segment_Lookup csl
    on csl.dtv_tenure = Outer_Base_Table.DTV_tenure
        and csl.Time_Since_Last_TA_Call = Outer_Base_Table.Time_Since_Last_TA_Call
        and csl.Offer_Length_DTV = Outer_Base_Table.Offer_Length_DTV
        and csl.Time_To_Offer_End_DTV = Outer_Base_Table.Time_To_Offer_End_DTV
        and csl.package_desc = Outer_Base_Table.package_desc
        and csl.Country = Outer_Base_Table.Country
where Downgrade_View = 'Actuals';


Delete from CITeam.DTV_Fcast_Weekly_Base
where downgrade_view = 'LV 201601 V11';

Update CITeam.DTV_Fcast_Weekly_Base
Set downgrade_view = 'LV 201601 V11'
where downgrade_view = 'LVX201601 V11';
*/