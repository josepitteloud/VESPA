/*------------------------------------------------------------------------------
        Project: FOOTBALL LEAGUES -Update for David Grassham 
        Version: 3
        Created: 2013-07-23
        Lead: Kate Sargent / Liz Prescott
        Analyst: Dan Barnett
        SK Prod: 5
*/------------------------------------------------------------------------------
--select top 100 * rom shaha.F_DATA_CUBE_WITH_SEGMENTS___EXCLUDES_ACCOUNTS_WITHOUT_SCALE;
---Uses data created in FOOTBALL LEAGUES V3.sql created by Amisha Shah

SELECT
Loyalty_Champions_League_ITV 
,Loyalty_Champions_League_Sky 
,case when Loyalty_Premier_League_Sky in ('High','Medium') then 'High/Medium' else Loyalty_Premier_League_Sky end as Premier_League_Sky
,case when Loyalty_Football_League_Sky in ('High','Medium') then 'High/Medium' else Loyalty_Football_League_Sky end as Football_League_Sky

, case when Value_Segment in ('Gold','Platinum','Silver') then 'Gold/Platinum/Silver' when  Value_Segment in ('Bronze','Copper') then 'Bronze/Copper' else Value_Segment end as grouped_value_segment
,Value_Segment
, Broadband
,ESPN_Subscription
, Sky_Sports_Subscription
, SUM(Scaled_Accounts_for_VESPA_Accounts_With_Scale) Scaled_Accounts_for_VESPA_Accounts_With_Scale
from shaha.F_DATA_CUBE_WITH_SEGMENTS___EXCLUDES_ACCOUNTS_WITHOUT_SCALE
group by 
Loyalty_Champions_League_ITV 
,Loyalty_Champions_League_Sky 
,Premier_League_Sky 
,Football_League_Sky 
,Value_Segment
, grouped_value_segment
, Broadband
,ESPN_Subscription
, Sky_Sports_Subscription
;

commit;

----Create Output to derive Part 3 of Request (High Tues/Weds Viewing for Champions League).



--------------------------------------------------------------------------------
--Code adapted from FOOTBALL LEAGUES V3.sql created by Amisha Shah to look at Tuesday/Wednesday Split rather than channel specific

---PART L: --- loyalty by champions league pick
          --- weighted games watched = actual observed / (actual available on days tv viewed) * (total available across all match days)
--------------------------------------------------------------------------------

----Create F_Total_CL_Pick table in dbarnett schema as shaha scheme version unavailable
--drop table dbarnett.F_Daily_CL_Pick;
SELECT
             Fixtures_League
       ,     Fixtures_League_Rights
       ,     Fixtures_Pick
      ,     Fixtures_Date
       , dateformat(Fixtures_Date,'DDD') as fixture_day
       ,     COUNT(*) Daily_Games
       ,     SUM(programme_instance_duration) Daily_Duration

INTO   dbarnett.F_Daily_CL_Pick

FROM
(SELECT Fixtures_League_Rights
       ,    Fixtures_League
       ,    Fixtures_Pick
       ,    Fixtures_Date
       ,    Fixtures_Time
       ,    max(programme_instance_duration) programme_instance_duration
FROM   shaha.F_Fixtures_EPG
WHERE  Fixtures_League = 'Champions League'
GROUP  BY Fixtures_League_Rights
       ,    Fixtures_League
       ,    Fixtures_Pick
       ,    Fixtures_Date
       ,    Fixtures_Time
) A

GROUP BY     Fixtures_League
       ,     Fixtures_League_Rights
       ,     Fixtures_Pick      
 ,     Fixtures_Date
,fixture_day;

--select * from dbarnett.F_Daily_CL_Pick;


--drop table dbarnett.F_Total_CL_Pick;

SELECT
Fixtures_League
, Fixtures_League_Rights
--, Fixtures_Pick
,     SUM(Daily_Games)    Total_Games
,     SUM(Daily_Duration) Total_Duration
INTO   dbarnett.F_Total_CL_Pick
FROM   dbarnett.F_Daily_CL_Pick
GROUP  BY
Fixtures_League
, Fixtures_League_Rights
--, Fixtures_Pick
;
--drop table dbarnett.F_Viewing_Engagements_Weighted_by_day;

--drop table dbarnett.F_Box_Returns_Weights;

SELECT
a.Account_Number
 ,     b.Fixtures_League
 ,     b.Fixtures_League_Rights
 ,     b.Fixtures_Pick
 ,     b.Fixtures_Date
,fixture_day
 ,     sum(b.daily_games) Daily_Games
 ,     sum(b.daily_duration) Daily_Duration

INTO   dbarnett.F_Box_Returns_Weights

FROM   shaha.F_Box_Returns a -- capped viewing data on match days

       JOIN dbarnett.F_Daily_CL_Pick b -- content available on days box returned data
       ON a.Viewing_Date = b.Fixtures_Date

GROUP  BY a.Account_Number
 ,     b.Fixtures_League
 ,     b.Fixtures_League_Rights
 ,     b.Fixtures_Pick
 ,     b.Fixtures_Date
,fixture_day
;
commit;
--select top 500 * from shaha.F_Viewing_Engagements_CL_Pick order by account_number

--Get List of all watched content by Day--

select account_number
       , dateformat(Fixtures_Date,'DDD') as fixture_day
,sum(games_watched) as games_watched_on_day_of_week
into #games_watched_by_day_of_week
from shaha.F_Viewing_Engagements_CL_Pick
group by account_number
       , fixture_day
;

--Get List of all watched content by Rights Holder--

select account_number
       , fixtures_league_rights
,sum(games_watched) as games_watched_by_rights_holder
into #games_watched_by_rights_holder
from shaha.F_Viewing_Engagements_CL_Pick
group by account_number
       , fixtures_league_rights
;
commit;

--Get List of all watched content by Distinct Date--

select account_number
       , fixtures_date
,max(case when games_watched>=1 then 1 else 0 end) as date_watched_any_game
into #days_watching_content
from shaha.F_Viewing_Engagements_CL_Pick
group by account_number
       , fixtures_date
;
commit;

--select top 100 * from #days_watching_content;

----Create Counts of Possible Days/Games watchable from Box Return Data----

--Number of Possible Games by Day of week

select account_number
,fixture_day
,sum(Daily_Games) as total_games_possible_to_view
into #possible_games_viewable
from dbarnett.F_Box_Returns_Weights
group by account_number
,fixture_day
;

--Number of Possible Games by Rights Holder

select account_number
,fixtures_league_rights
,sum(Daily_Games) as total_games_possible_to_view
into #possible_games_viewable_rights_holder
from dbarnett.F_Box_Returns_Weights
group by account_number
,fixtures_league_rights
;

--Distinct Dates

select account_number
,count(distinct fixtures_date) as distinct_dates
into #possible_viewing_dates
from dbarnett.F_Box_Returns_Weights
group by account_number
;

--select top 500 * from #possible_games_viewable;

---Match Possible To actual--

---Day of Week---
--drop table #day_of_week;
select a.account_number
,a.fixture_day
,total_games_possible_to_view
,case when games_watched_on_day_of_week is null then 0 else games_watched_on_day_of_week end as games_watched_on_day

into #day_of_week
from #possible_games_viewable as a
left outer join #games_watched_by_day_of_week as b
on a.account_number = b.account_number and a.fixture_day=b.fixture_day
;


--drop table #day_of_week_segmentation;
select account_number
,min (case   when fixture_day = 'TUE' and games_watched_on_day =0 then '04: None'
        when fixture_day = 'TUE' and cast(games_watched_on_day as float) / total_games_possible_to_view >=0.5 then '01: High' 
        when fixture_day = 'TUE' and cast(games_watched_on_day as float) / total_games_possible_to_view >=0.2 then '02: Medium'
        when fixture_day = 'TUE' and cast(games_watched_on_day as float) / total_games_possible_to_view >=0 then '03: Low'
else '04: None' end) as watched_percentage_TUE
,min (case   when fixture_day = 'WED' and games_watched_on_day =0 then '04: None'
        when fixture_day = 'WED' and cast(games_watched_on_day as float) / total_games_possible_to_view >=0.5 then '01: High' 
        when fixture_day = 'WED' and cast(games_watched_on_day as float) / total_games_possible_to_view >=0.2 then '02: Medium'
        when fixture_day = 'WED' and cast(games_watched_on_day as float) / total_games_possible_to_view >=0 then '03: Low'
else '04: None' end) as watched_percentage_WED
into #day_of_week_segmentation
from #day_of_week
group by account_number

--select top 500 * from #day_of_week_segmentation order by account_number

--select watched_percentage_TUE ,watched_percentage_WED , count(*) from #day_of_week_segmentation group by watched_percentage_TUE ,watched_percentage_WED 

--select top 500 * from #games_watched_by_rights_holder;


----Match back to Master Table for output---
select  case when watched_percentage_TUE is null then '04: None' else watched_percentage_TUE end as CL_Tue
, case when watched_percentage_WED is null then '04: None' else watched_percentage_WED end as CL_Wed
, Broadband
,ESPN_Subscription
, Sky_Sports_Subscription
, SUM(Scaling_Weight)  Scaled_Accounts_for_VESPA_Accounts_With_Scale
from shaha.F_DATA_CUBE as a
left outer join #day_of_week_segmentation as b
on a.account_number = b.account_number
where Scaling_Weight >0
group by CL_Tue
, CL_Wed
, Broadband
,ESPN_Subscription
, Sky_Sports_Subscription
;


---repeat for Sky/ITV Split---

select a.account_number
,a.fixtures_league_rights
,total_games_possible_to_view
,case when games_watched_by_rights_holder is null then 0 else games_watched_by_rights_holder end as total_games_watched_by_rights_holder

into #rights_holder
from #possible_games_viewable_rights_holder as a
left outer join #games_watched_by_rights_holder as b
on a.account_number = b.account_number and a.fixtures_league_rights=b.fixtures_league_rights
;


--drop table #rights_holder_segmentation;
select account_number
,max(case when fixtures_league_rights = 'SKY' then total_games_watched_by_rights_holder else 0 end) as sky_games_viewed
,max(case when fixtures_league_rights = 'SKY' then cast(total_games_watched_by_rights_holder as float) else 0 end) as sky_games_viewed_float
,max(case when fixtures_league_rights = 'SKY' then cast(total_games_watched_by_rights_holder as float) / total_games_possible_to_view else 0 end) as sky_viewing
,min (case   when fixtures_league_rights = 'SKY' and total_games_watched_by_rights_holder =0 then '04: None'
        when fixtures_league_rights = 'SKY' and cast(total_games_watched_by_rights_holder as float) / total_games_possible_to_view >=0.5 then '01: High' 
        when fixtures_league_rights = 'SKY' and cast(total_games_watched_by_rights_holder as float) / total_games_possible_to_view >=0.2 then '02: Medium'
        when fixtures_league_rights = 'SKY' and cast(total_games_watched_by_rights_holder as float) / total_games_possible_to_view >=0 then '03: Low'
else '04: None' end) as watched_percentage_SKY
,min (case   when fixtures_league_rights = 'ITV' and total_games_watched_by_rights_holder =0 then '04: None'
        when fixtures_league_rights = 'ITV' and cast(total_games_watched_by_rights_holder as float) / total_games_possible_to_view >=0.5 then '01: High' 
        when fixtures_league_rights = 'ITV' and cast(total_games_watched_by_rights_holder as float) / total_games_possible_to_view >=0.2 then '02: Medium'
        when fixtures_league_rights = 'ITV' and cast(total_games_watched_by_rights_holder as float) / total_games_possible_to_view >=0 then '03: Low'
else '04: None' end) as watched_percentage_ITV
into #rights_holder_segmentation
from #rights_holder
group by account_number
;
--select * from #rights_holder_segmentation;

----Match back to Master Table for output---
select  case when watched_percentage_SKY is null then '04: None' else watched_percentage_SKY end as CL_Sky
, case when watched_percentage_ITV is null then '04: None' else watched_percentage_ITV end as CL_ITV
, Broadband
,ESPN_Subscription
, Sky_Sports_Subscription
,  CASE WHEN  Sky_Sports_Subscription = 1
     AND Loyalty_Champions_League_SKY = 'High' and Highest_SOV_on_Sky_Sports = 'Champions_League'
     THEN '1a. Has SS, CL has highest SOV on SS, High CL Loyalty on SS (Highest Risk)'
     WHEN  Sky_Sports_Subscription = 1
     AND Loyalty_Champions_League_SKY = 'Medium' and Highest_SOV_on_Sky_Sports = 'Champions_League'
     THEN '1b. Has SS, CL has highest SOV on SS, Medium CL Loyalty on SS (High Risk)'
     WHEN  Sky_Sports_Subscription = 1
     AND Loyalty_Champions_League_SKY = 'High' and Highest_SOV_on_Sky_Sports <> 'Champions_League'
     THEN '2a. Has SS, CL does NOT have highest SOV on SS, High CL Loyalty on SS (High Risk)'
     WHEN  Sky_Sports_Subscription = 1
     AND Loyalty_Champions_League_SKY = 'Medium' and Highest_SOV_on_Sky_Sports <> 'Champions_League'
     THEN '2b. Has SS, CL does NOT have highest SOV on SS, Medium CL Loyalty on SS (Medium Risk)'
 WHEN  Sky_Sports_Subscription = 1
     AND Loyalty_Champions_League_SKY = 'Low'
     THEN '3. Has SS, Low CL Loyalty on SS (Low Risk)'
  WHEN  Sky_Sports_Subscription = 1
     AND Loyalty_Champions_League_SKY = 'None'
     THEN '4. Has SS, No CL Loyalty on SS (Lowest Risk)'
WHEN  Sky_Sports_Subscription = 0
 AND Loyalty_Champions_League_SKY = 'None' AND Loyalty_Champions_League_ITV = 'None' AND Loyalty_Premier_League_ESPN <> 'None'
     THEN  '5. No SS, No CL Loyalty on ITV, PL Loyalty on ESPN (Unaffected)'
 WHEN  Sky_Sports_Subscription = 0
 AND Loyalty_Champions_League_SKY = 'None' AND Loyalty_Champions_League_ITV <> 'None'
     THEN  '6. No SS, CL Loyalty on ITV (Upgrade Opportunity)'
     ELSE '00. NO SEGMENT'
     END AS Champions_League_Segment
, SUM(Scaling_Weight)  Scaled_Accounts_for_VESPA_Accounts_With_Scale
from shaha.F_DATA_CUBE as a
left outer join #rights_holder_segmentation as b
on a.account_number = b.account_number
where Scaling_Weight >0
group by CL_SKY
, CL_ITV
, Broadband
,ESPN_Subscription
, Sky_Sports_Subscription
,Champions_League_Segment
;

commit;

