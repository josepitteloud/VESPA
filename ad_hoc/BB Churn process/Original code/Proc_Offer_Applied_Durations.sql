/*
Select top 1000 * from master_of_retention where PC_effective_to_dt >= '2016-07-01' and Next_status_code = 'AC'


Create variable Forecast_Start_Wk integer; Set Forecast_Start_Wk = 201601;
Create variable Num_Wks integer; Set Num_Wks = 6;

Setuser CITeam;
*/
Drop procedure if exists Offer_Applied_Duration_Dist;

Create Procedure Offer_Applied_Duration_Dist(IN Forecast_Start_Wk integer,Num_Wks integer)
RESULT(Offer_segment varchar(30),
       Total_Offer_Duration_Mth integer,
       Weekly_Avg_New_Offers integer,
       Total_New_Offers integer,
       Cum_New_Offers integer,
       Dur_Pctl_Lower_Bound float,
       Dur_Pctl_Upper_Bound float
       )
BEGIN

Select * into #Sky_Calendar from subs_calendar(Forecast_Start_Wk/100-1,Forecast_Start_Wk/100);

Select
--     Case overall_offer_segment_grouped_1
--          when 'Winback' then 'Activations'
--          when 'PAT(Value)&CoE' then 'Other'
--          when 'Other-Unknown' then 'Other'
--          when 'ReInstate' then 'Activations'
--          when 'Price Protection' then 'Price Protection'
--          when 'NonRetention Offers' then 'Other'
--          when 'BB Acquisition/Upgrade' then 'Other'
--          when 'Other Retention' then 'TA'
--          when 'DTH Acquisition' then 'Activations'
--          when 'TA' then 'TA'
--          when 'Package Changes/Upgrades' then 'Other'
--     end overall_offer_segment_grouped_1,
--     overall_offer_segment,
    Case overall_offer_segment
        when '1.(DTH)A1.Acquisition' then 'Activations'
        when '1.(DTH)B1.Winback' then 'Activations'
        when '1.(DTH)B2.TA' then 'TA'
        when '1.(DTH)B3.CoE' then 'Other'
        when '1.(DTH)B4.PAT' then 'Other'
        when '1.(DTH)B5.Pipeline ReInstate' then 'Reactivations'
        when '1.(DTH)B6.Other Retention' then 'TA'
        when '1.(DTH)C1.DTV Package Movement' then 'Other'
        when '1.(DTH)D1.Offer On Call' then 'Other'
        when '1.(DTH)D3.Other' then 'Other'
        when '2.(BB)A1.Acquisition/Upgrade' then 'Activations'
        when '2.(BB)B1.TA' then 'TA'
        when '2.(BB)B2.CoE' then 'Other'
        when '2.(BB)B3.PAT' then 'Other'
        when '2.(BB)B4.Pipeline ReInstate' then 'Reactivations'
        when '2.(BB)B5.Other Retention' then 'Reactivations'
        when '2.(BB)C1.BB Package Movement' then 'Other'
        when '2.(BB)C2.Offer On Call' then 'Other'
        when '2.(BB)C4.Other' then 'Other'
    end overall_offer_segment,
    Total_Offer_Duration_Mth,
    count(*)/Num_Wks Weekly_Avg_New_Offers,
    Sum(Weekly_Avg_New_Offers) over(partition by overall_offer_segment) Total_New_Offers,
    Sum(Weekly_Avg_New_Offers) over(partition by overall_offer_segment order by Total_Offer_Duration_Mth) Cum_New_Offers,
    Cast(Cum_New_Offers as float)/Total_New_Offers as Pctl_New_Offers,
    Row_Number() over(partition by overall_offer_segment order by Total_Offer_Duration_Mth) Dur_Rnk
into #Offer_Dur
from citeam.offer_usage_all oua
where offer_start_dt_Actual between (Select max(calendar_date - 7 - Num_Wks*7 + 1) from #sky_calendar where subs_week_and_year = Forecast_Start_Wk)
                                and (Select max(calendar_date - 7) from #sky_calendar where subs_week_and_year = Forecast_Start_Wk)
        and Total_Offer_Duration_Mth <= 36
        and offer_start_dt_Actual = Whole_Offer_Start_Dt_Actual
        and Subs_Type = 'DTV Primary Viewing'
        and lower(offer_dim_description) not like '%price protection%'
        and oua.overall_offer_segment_grouped_1 != 'Price Protection'
group by overall_offer_segment,Total_Offer_Duration_Mth--,overall_offer_segment_grouped_1,x_overall_offer_segment
;


Select
--     dur1.overall_offer_segment_grouped_1,
    dur1.overall_offer_segment,
--     dur1.x_overall_offer_segment,
    dur1.Total_Offer_Duration_Mth,
    dur1.Weekly_Avg_New_Offers,
    dur1.Total_New_Offers,
    dur1.Cum_New_Offers,
    Coalesce(dur2.Pctl_New_Offers,0) Dur_Pctl_Lower_Bound,
    dur1.Pctl_New_Offers Dur_Pctl_Upper_Bound
from #Offer_Dur dur1
     left join
     #Offer_Dur dur2
     on dur2.overall_offer_segment = dur1.overall_offer_segment
        and dur2.Dur_Rnk = dur1.Dur_Rnk - 1

END;

Grant execute on Offer_Applied_Duration_Dist to CITeam;
/*
Setuser;

Select * from CITeam.Offer_Applied_Duration_Dist(201601,6)

*/

