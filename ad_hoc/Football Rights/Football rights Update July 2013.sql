/*------------------------------------------------------------------------------
        Project: FOOTBALL LEAGUES -Update for David Grassham 
        Version: 1
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


--drop table #day_of_week_segmentation;
select account_number
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


----Match back to Master Table for output---
select  case when watched_percentage_SKY is null then '04: None' else watched_percentage_SKY end as CL_Tue
, case when watched_percentage_ITV is null then '04: None' else watched_percentage_WED end as CL_Wed
, Broadband
,ESPN_Subscription
, Sky_Sports_Subscription
, SUM(Scaling_Weight)  Scaled_Accounts_for_VESPA_Accounts_With_Scale
from shaha.F_DATA_CUBE as a
left outer join #rights_holder_segmentation as b
on a.account_number = b.account_number
where Scaling_Weight >0
group by CL_Tue
, CL_Wed
, Broadband
,ESPN_Subscription
, Sky_Sports_Subscription
;


































/*


---Repeat For A Tuesday/Wednesday Split---
select  -- select top 100 * from F_Viewing_Engagements_Weighted
a.Account_Number
 ,     a.Fixtures_League
, dateformat(a.Fixtures_Date,'DDD') as fixture_day
 ,     sum(isnull(cast(c.Games_Watched AS FLOAT),0)) / SUM(a.Daily_Games) * MAX(d.Total_Games)  AS  Games_Watched
 ,     sum(isnull(cast(c.Games_Watched AS FLOAT),0)) / SUM(a.Daily_Games) AS Games_Watched__Percentage
 ,     sum(isnull(cast(c.viewing_duration AS FLOAT),0)) / SUM(a.Daily_Duration) * MAX(d.Total_Duration)  AS  Viewing_Duration
 ,     sum(isnull(cast(c.viewing_duration AS FLOAT),0)) / SUM(a.Daily_Duration)   AS  Viewing_Duration__Percentage

into dbarnett.F_Viewing_Engagements_Weighted_by_day

from dbarnett.F_Box_Returns_Weights a

 left outer  JOIN shaha.F_Viewing_Engagements_CL_Pick c -- viewed content
                 ON a.Account_Number = c.account_number
                  and a.Fixtures_League_Rights = c.Fixtures_League_Rights
                       AND a.Fixtures_League = c.Fixtures_League
--                       AND a.Fixtures_Date   = c.Fixtures_Date
--                       AND a.Fixtures_Pick = c.Fixtures_Pick

   JOIN dbarnett.F_Total_CL_Pick d -- total content broadcasted
       ON a.Fixtures_League_Rights = d.Fixtures_League_Rights
       AND a.Fixtures_League = d.Fixtures_League
--           AND a.Fixtures_Pick = d.Fixtures_Pick
group by
a.Account_Number
 ,     a.Fixtures_League
-- ,     a.Fixtures_League_Rights
-- ,     a.Fixtures_Pick
,fixture_day
;
--select top 500 * from dbarnett.F_Viewing_Engagements_Weighted_by_day order by account_number;
--select top 500 * from dbarnett.F_Total_CL_Pick;

--- loyalty
-- drop table dbarnett.F_Loyalty_Champions_Leaugue_Breakdown_by_day;commit;
SELECT  
account_number
, MAX(CASE WHEN  v.Fixtures_League =  'Champions League'  AND fixture_day='TUE' and  v.Games_Watched__Percentage = 0 THEN 'None'
       WHEN  v.Fixtures_League =  'Champions League' AND fixture_day='TUE' AND v.Games_Watched__Percentage > 0 AND v.Games_Watched__Percentage < 0.2 THEN 'Low'
       WHEN  v.Fixtures_League =  'Champions League' AND fixture_day='TUE' AND v.Games_Watched__Percentage >= 0.2 AND v.Games_Watched__Percentage < 0.5 THEN 'Medium'
       WHEN  v.Fixtures_League =  'Champions League' AND fixture_day='TUE' AND v.Games_Watched__Percentage >= 0.5  THEN 'High' else 'None' 
       END)   Loyalty_Champions_League_Tue
, MAX(CASE WHEN  v.Fixtures_League =  'Champions League'  AND fixture_day='WED' and  v.Games_Watched__Percentage = 0 THEN 'None'
       WHEN  v.Fixtures_League =  'Champions League' AND fixture_day='WED' AND v.Games_Watched__Percentage > 0 AND v.Games_Watched__Percentage < 0.2 THEN 'Low'
       WHEN  v.Fixtures_League =  'Champions League' AND fixture_day='WED' AND v.Games_Watched__Percentage >= 0.2 AND v.Games_Watched__Percentage < 0.5 THEN 'Medium'
       WHEN  v.Fixtures_League =  'Champions League' AND fixture_day='WED' AND v.Games_Watched__Percentage >= 0.5  THEN 'High'  else 'None' 
       END)   Loyalty_Champions_League_Wed

INTO  dbarnett.F_Loyalty_Champions_Leaugue_Breakdown_by_day
from dbarnett.F_Viewing_Engagements_Weighted_by_day v
GROUP BY account_number;


--select Loyalty_Champions_League_Tue, Loyalty_Champions_League_Wed , count(*) from dbarnett.F_Loyalty_Champions_Leaugue_Breakdown_by_day group by Loyalty_Champions_League_Tue, Loyalty_Champions_League_Wed order by Loyalty_Champions_League_Tue, Loyalty_Champions_League_Wed

--select Loyalty_Champions_League , count(*) from dbarnett.F_Loyalty_Champions_Leaugue_Breakdown_overall group by Loyalty_Champions_League


drop table F_Box_Returns_Weights;
drop table F_Viewing_Engagements_Weighted;


----Match back to Master Table for output---
select  case when Loyalty_Champions_League_Tue is null then 'None' else Loyalty_Champions_League_Tue end as CL_Tue
, case when Loyalty_Champions_League_Wed is null then 'None' else Loyalty_Champions_League_Wed end as CL_Wed
, Broadband
,ESPN_Subscription
, Sky_Sports_Subscription
, SUM(Scaling_Weight)  Scaled_Accounts_for_VESPA_Accounts_With_Scale
from shaha.F_DATA_CUBE as a
left outer join dbarnett.F_Loyalty_Champions_Leaugue_Breakdown_by_day as b
on a.account_number = b.account_number
where Scaling_Weight >0
group by CL_Tue
, CL_Wed
, Broadband
,ESPN_Subscription
, Sky_Sports_Subscription
;

--select top 100 * from shaha.F_DATA_CUBE




/* Test Code


select top 100 * from  shaha.F_Viewing_Engagements_Weighted;

commit;

SELECT
case when Loyalty_Champions_League_SKY_Tuesday_SS4 in ('High', 'Medium') then 1 else 0 end  'Medium/High Loyalty to Tuesday SS4'

, SUM(Scaled_Accounts_for_VESPA_Accounts_With_Scale) Scaled_Accounts_for_VESPA_Accounts_With_Scale
from shaha.F_DATA_CUBE_WITH_SEGMENTS___EXCLUDES_ACCOUNTS_WITHOUT_SCALE
group by case when Loyalty_Champions_League_SKY_Tuesday_SS4 in ('High', 'Medium') then 1 else 0 end



SELECT
case when Loyalty_Champions_League_ITV in ('High', 'Medium') then 1 else 0 end  as Medium_High_Loyalty_to_Champions_League_ITV
,case when Loyalty_Champions_League_Sky in ('High', 'Medium') then 1 else 0 end  as Medium_High_Loyalty_to_Champions_League_SKY
,case when Loyalty_Premier_League_Sky in ('High', 'Medium') then 1 else 0 end  as Medium_High_Loyalty_to_Premier_League_SKY
,case when Loyalty_Football_League_Sky in ('High', 'Medium') then 1 else 0 end  as Medium_High_Loyalty_Football_League_Sky
, Value_Segment
, Broadband
,ESPN_Subscription
, SUM(Scaled_Accounts_for_VESPA_Accounts_With_Scale) Scaled_Accounts_for_VESPA_Accounts_With_Scale
from shaha.F_DATA_CUBE_WITH_SEGMENTS___EXCLUDES_ACCOUNTS_WITHOUT_SCALE
group by 
Medium_High_Loyalty_to_Champions_League_ITV
,Medium_High_Loyalty_to_Champions_League_SKY
,Medium_High_Loyalty_to_Premier_League_SKY
,Medium_High_Loyalty_Football_League_Sky
, Value_Segment
, Broadband
,ESPN_Subscription





---

SELECT
a.Account_Number
 ,     b.Fixtures_League
 ,     b.Fixtures_League_Rights
 ,     b.Fixtures_Pick
 ,     b.Fixtures_Date
 ,     sum(b.daily_games) Daily_Games
 ,     sum(b.daily_duration) Daily_Duration

INTO   F_Box_Returns_Weights

FROM   shaha.F_Box_Returns a -- capped viewing data on match days

       JOIN shaha.F_Daily_CL_Pick b -- content available on days box returned data
       ON a.Viewing_Date = b.Fixtures_Date

GROUP  BY a.Account_Number
 ,     b.Fixtures_League
 ,     b.Fixtures_League_Rights
 ,     b.Fixtures_Pick
 ,     b.Fixtures_Date;


select  -- select top 100 * from F_Viewing_Engagements_Weighted
a.Account_Number
 ,     a.Fixtures_League
 ,     a.Fixtures_League_Rights
 ,     a.Fixtures_Pick
 ,     sum(isnull(cast(c.Games_Watched AS FLOAT),0)) / SUM(a.Daily_Games) * MAX(d.Total_Games)  AS  Games_Watched
 ,     sum(isnull(cast(c.Games_Watched AS FLOAT),0)) / SUM(a.Daily_Games) AS Games_Watched__Percentage
 ,     sum(isnull(cast(c.viewing_duration AS FLOAT),0)) / SUM(a.Daily_Duration) * MAX(d.Total_Duration)  AS  Viewing_Duration
 ,     sum(isnull(cast(c.viewing_duration AS FLOAT),0)) / SUM(a.Daily_Duration)   AS  Viewing_Duration__Percentage

into F_Viewing_Engagements_Weighted

from shaha.F_Box_Returns_Weights a

 left outer  JOIN shaha.F_Viewing_Engagements_CL_Pick c -- viewed content
                 ON a.Account_Number = c.account_number
                  and a.Fixtures_League_Rights = c.Fixtures_League_Rights
                       AND a.Fixtures_League = c.Fixtures_League
                       AND a.Fixtures_Date   = c.Fixtures_Date
                       AND a.Fixtures_Pick = c.Fixtures_Pick

   JOIN shaha.F_Total_CL_Pick d -- total content broadcasted
       ON a.Fixtures_League_Rights = d.Fixtures_League_Rights
       AND a.Fixtures_League = d.Fixtures_League
           AND a.Fixtures_Pick = d.Fixtures_Pick
group by
a.Account_Number
 ,     a.Fixtures_League
 ,     a.Fixtures_League_Rights
 ,     a.Fixtures_Pick;












select top 100 * from shaha.F_Viewing_Engagements_CL_Pick order by account_number , fixtures_date


select top 100 * from shaha.F_Fixtures_EPG where fixtures_league = 'Champions League' order by fixtures_date desc
select top 100 * from shaha.F_Box_Returns 
select top 100 * from shaha.F_Viewing_Engagements_CL_Pick order by account_number , fixtures_date
select top 100 * from shaha.F_Total_CL_Pick

select top 100 * from shaha.F_Total
select top 100 * from shaha.F_Daily

select top 100 * from shaha.F_Viewing_Engagements order by account_number , fixtures_date

shaha.F_Viewing_Engagements_CL_Pick

F_Box_Returns_Weights
FROM   F_Box_Returns


SELECT
            Fixtures_League
       ,    Fixtures_League_Rights
       ,     Fixtures_Date
       ,     COUNT(*) Daily_Games
       ,     SUM(programme_instance_duration) Daily_Duration

INTO   F_Daily_TEST2

FROM
(SELECT Fixtures_League_Rights
       ,    Fixtures_League
       ,    Fixtures_Date
       ,    Fixtures_Time -- simultaneous matches from same broadcaster will not be
       ,    max(programme_instance_duration) programme_instance_duration
FROM   shaha.F_Fixtures_EPG
GROUP  BY Fixtures_League_Rights
       ,    Fixtures_League
       ,    Fixtures_Date
       ,    Fixtures_Time
) A

GROUP BY     Fixtures_League
        ,    Fixtures_League_Rights
        ,    Fixtures_Date;


select * from F_Daily_TEST2 where fixtures_league = 'Champions League' order by fixtures_league_rights, fixtures_date;

select distinct fixtures_date from F_Daily_TEST2 where fixtures_league = 'Champions League' order by fixtures_league_rights, fixtures_date;

commit;







--select * from dbarnett.F_Total_CL_Pick;

--drop table dbarnett.F_Box_Returns_Weights; drop table dbarnett.F_Viewing_Engagements_Weighted; commit;






--drop table dbarnett.F_Viewing_Engagements_Weighted; commit;
select  -- select top 100 * from F_Viewing_Engagements_Weighted
a.Account_Number
 ,     a.Fixtures_League
--,a.Fixtures_Date
 ,     sum(isnull(cast(c.Games_Watched AS FLOAT),0)) / SUM(a.Daily_Games) * MAX(d.Total_Games)  AS  Games_Watched
 ,     sum(isnull(cast(c.Games_Watched AS FLOAT),0)) / SUM(a.Daily_Games) AS Games_Watched__Percentage
 ,     sum(isnull(cast(c.viewing_duration AS FLOAT),0)) / SUM(a.Daily_Duration) * MAX(d.Total_Duration)  AS  Viewing_Duration
 ,     sum(isnull(cast(c.viewing_duration AS FLOAT),0)) / SUM(a.Daily_Duration)   AS  Viewing_Duration__Percentage

into dbarnett.F_Viewing_Engagements_Weighted

from dbarnett.F_Box_Returns_Weights a

 left outer  JOIN shaha.F_Viewing_Engagements_CL_Pick c -- viewed content
                 ON a.Account_Number = c.account_number
                  and a.Fixtures_League_Rights = c.Fixtures_League_Rights
                       AND a.Fixtures_League = c.Fixtures_League
                       AND a.Fixtures_Date   = c.Fixtures_Date
                       AND a.Fixtures_Pick = c.Fixtures_Pick

   JOIN dbarnett.F_Total_CL_Pick d -- total content broadcasted
       ON a.Fixtures_League_Rights = d.Fixtures_League_Rights
       AND a.Fixtures_League = d.Fixtures_League
           AND a.Fixtures_Pick = d.Fixtures_Pick
group by
a.Account_Number
 ,     a.Fixtures_League
-- ,     a.Fixtures_League_Rights
-- ,     a.Fixtures_Pick
--,a.Fixtures_Date
;

--select top 500 * from dbarnett.F_Viewing_Engagements_Weighted

--- loyalty

SELECT  -- drop table F_Loyalty_Champions_Leaugue_Breakdown
account_number
, MAX(CASE WHEN  v.Fixtures_League =  'Champions League'  AND v.Games_Watched__Percentage = 0 THEN 'None'
       WHEN  v.Fixtures_League =  'Champions League' AND v.Games_Watched__Percentage > 0 AND v.Games_Watched__Percentage < 0.2 THEN 'Low'
       WHEN  v.Fixtures_League =  'Champions League' AND v.Games_Watched__Percentage >= 0.2 AND v.Games_Watched__Percentage < 0.5 THEN 'Medium'
       WHEN  v.Fixtures_League =  'Champions League' AND v.Games_Watched__Percentage >= 0.5  THEN 'High'
       END)   Loyalty_Champions_League

INTO  dbarnett.F_Loyalty_Champions_Leaugue_Breakdown_Overall
from F_Viewing_Engagements_Weighted v
GROUP BY account_number;

CREATE UNIQUE HG INDEX idx1 ON  dbarnett.F_Loyalty_Champions_Leaugue_Breakdown_Overall (Account_Number);


*/