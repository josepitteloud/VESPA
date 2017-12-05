
/*------------------------------------------------------------------------------
        Project: GSK - LUCOZADE ANALYSIS
        Version: v1.
        Created: 14/01/2013
        Lead: Sarah Moore
        Analyst: Harry Gill

        Version Updates:



------------------------------------------------------------------------------

        Purpose -- same as before
        -------

        The goal is to create a proxy target audience for the lucozade camapign and profil spot viewers (lucozad and all spots). Also decile customers on thier viewing and produce outputs for the
        excel template.
-------------------------------------------------------------------------------

        SECTIONS
        --------

        Set-Up   -

        PART A   -
             A01 -
             A02 -


        PART B   -
             B01 -
             B02 -

        PART C   -
             C01 -
             C02 -

        PART D -
             D01 -
             D02 -
             D03 -
             D04 -

        PART E -
             E01 -
             E02 -
             E03 -
             E04 -


        Ouput Tables:   DO_NOT_DELETE_NBA_SCORING_2012XXXX -- THIS TABLE CONTAINS ALL SCORING for each customer
        -------

--------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------*/




--------------------------------------------------------------------------------
-- SET UP.
--------------------------------------------------------------------------------
-- create and populate variables
CREATE VARIABLE @var_period_start       datetime;
CREATE VARIABLE @var_period_end         datetime;

CREATE VARIABLE @var_sql                varchar(15000);
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @i                      integer;

-- Scaling Variables
Create variable @target_date            date;
Create variable @sky_total              numeric(28,20);
Create variable @Sample_total           numeric(28,20);
Create variable @weightings_total       numeric(28,20);
Create variable @scaling_factor         numeric(28,20);


SET @var_period_start           = '2012-06-01' --update to new period
SET @var_period_end             = '2012-06-30'




--------------------------------------------------------------------------------
-- lets create a base table
--------------------------------------------------------------------------------

-- we need those accounts whose primary box returned data for at least 50% of the campaign period -- this was established in code 1 (panel consistency)


If object_id ('consistency_table2') is not null drop table consistency_table2

-- lets select the primary boxes that returned data for at least 50% of the camapign period;
select distinct(cb_key_household) as cb_key_household
        ,max(account_number) as account_number
        ,max(subscriber_id) as subscriber_id
       ,count(distinct(cast(reporting_day as date))) as distinct_days
into consistency_table2
from the_boxes          -- this table contains all boxes that returned data over the campaign period
where primary_flag = 1 -- where the primary box returned of distinct days
group by cb_key_household

-- we only want the boxes that returned data for at least 50% of the period (primary box >50%, other boxes for any amount of time)
delete from consistency_table2 where distinct_days < 19

-- check it
select top 10 * from consistency_table2



--------------------------------------------------------------------------------
-- lets match this base table to Don's Sky bet segmentions to identify those panelists that watch alot of sport and alot of different sports:
--------------------------------------------------------------------------------

------ Determine the match rate:
select count(*) from consistency_table2
--116522


-- Don has 2 tables:

-- select top 10 * from rombaoad.V98_MainTable_SOV_Final -- 1st table
-- select top 10 * from rombaoad.V98_SkyBet_Final_Deciles -- 2nd table
-- we will use the 1st table:


select count(distinct(box.cb_key_household))
from consistency_table2 box
inner join rombaoad.V98_MainTable_SOV_Final don
on box.account_number = don.account_number
-- 103127
-- thats an 89% match rate between the two
-- above table = panel 12, 600k, 13-26aug2012 live only min6 second cap.


--------------------------------------------------------------------------
-- pull sports segment data
--------------------------------------------------------------------------



-- lets quickly determine a sensible threshold of viewing beyond which we can say alot of the program was watched (then assuming this means the viewer is possibly sporty)
drop table #test



select segment_id
       ,acctsegmentsecs_raw_alldates/60 as minutes
       ,count(distinct(account_number)) as number
       ,sum(number) over ( partition by segment_id order by minutes -- partition is not needed
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumul_numbers
into #test
from rombaoad.V98_MainTable_SOV_Final
where segment_id in ('genre_sports_American Football','genre_sports_Athletics','genre_sports_Baseball','genre_sports_Basketball'
                                                        ,'genre_sports_Boxing','genre_sports_Cricket','genre_sports_Darts','genre_sports_Equestrian'
                                                        ,'genre_sports_Extreme','genre_sports_Fishing','genre_sports_Golf','genre_sports_Ice Hockey'
                                                        ,'genre_sports_Motor Sport','genre_sports_Rugby','genre_sports_Snooker/Pool','genre_sports_Tennis'
                                                        ,'genre_sports_Watersports','genre_sports_Wintersports','genre_sports_Wrestling','genre_sports_football'
                                                        ,'genre_sports_racing','genre_sports_other','genre_sports','genre_sports_undefined')
      and account_number in (select account_number from consistency_table2) -- this has the 50% consistent panel that we are using
group by segment_id
       ,minutes
order by minutes


-- lets get all of the above ready for a pivot table - we can use this to determine the cut-off minutes required to determine if a particular sport was watched:
select * from #test




-- Pivot table results:


/*
                        Low Cut off Minutes
genre_sports_sov                295
genre_sports_American Football_sov      5
genre_sports_Athletics_sov      12
genre_sports_Baseball_sov       5
genre_sports_Basketball_sov     5
genre_sports_Boxing_sov 13
genre_sports_Cricket_sov        35
genre_sports_Darts_sov  6
genre_sports_Equestrian_sov     5
genre_sports_Extreme_sov        15
genre_sports_Fishing_sov        10
genre_sports_football_sov       60
genre_sports_Golf_sov   10
genre_sports_Ice Hockey_sov     5
genre_sports_Motor Sport_sov    10
genre_sports_Rugby_sov  10
genre_sports_Snooker/Pool_sov   8
genre_sports_Tennis_sov 7
genre_sports_Watersports_sov    6
genre_sports_Wintersports_sov   5
genre_sports_Wrestling_sov      15
genre_sports_other_sov  200
genre_sports_racing_sov 15
genre_sports_undefined_sov      90
*/

-- the panelist must have sen the above amount of minutes over the 2 week period of dons table to be considered as having watched this genre of sport;




--------------
-- now lets get all of the fields that we are interested in:
--------------



If object_id ('panel_sov') is not null drop table panel_sov

select box.cb_key_household
        ,box.account_number
        ,box.subscriber_id -- need to address the spelling
        ,matched = max(case when box.account_number = don.account_number then 1 else 0 end)

        ,Sports_decile = max(case when segment_id = 'genre_sports' then SOV_DECILE else null end) -- how much sport is watched; (depth of engadgment)


        ,breadth_of_sports1 = count(distinct( case when segment_id in ('genre_sports_American Football','genre_sports_Athletics','genre_sports_Baseball','genre_sports_Basketball'
                                                        ,'genre_sports_Boxing','genre_sports_Cricket','genre_sports_Darts','genre_sports_Equestrian'
                                                        ,'genre_sports_Extreme','genre_sports_Fishing','genre_sports_Golf','genre_sports_Ice Hockey'
                                                        ,'genre_sports_Motor Sport','genre_sports_Rugby','genre_sports_Snooker/Pool','genre_sports_Tennis'
                                                        ,'genre_sports_Watersports','genre_sports_Wintersports','genre_sports_Wrestling','genre_sports_football'
                                                        ,'genre_sports_racing','genre_sports_other')--,'genre_sports','genre_sports_undefined')
                              and segment_programs_watched > 0 then segment_id else null end))


        ,breadth_of_sports = count(distinct( case when (segment_id = 'genre_sports_American Football'    and acctsegmentsecs_raw_alldates >=  300) then 1
                                         when (segment_id = 'genre_sports_Athletics'             and acctsegmentsecs_raw_alldates >=  720) then 2
                                         when (segment_id = 'genre_sports_Baseball'              and acctsegmentsecs_raw_alldates >=  300) then 3
                                         when (segment_id = 'genre_sports_Basketball'            and acctsegmentsecs_raw_alldates >=  300) then 4
                                         when (segment_id = 'genre_sports_Boxing'                and acctsegmentsecs_raw_alldates >=  780) then 5
                                         when (segment_id = 'genre_sports_Cricket'               and acctsegmentsecs_raw_alldates >=  2100) then 6
                                         when (segment_id = 'genre_sports_Darts'                 and acctsegmentsecs_raw_alldates >=  360) then 7
                                         when (segment_id = 'genre_sports_Equestrian'            and acctsegmentsecs_raw_alldates >=  300) then 8
                                         when (segment_id = 'genre_sports_Extreme'               and acctsegmentsecs_raw_alldates >=  900) then 9
                                         when (segment_id = 'genre_sports_Fishing'               and acctsegmentsecs_raw_alldates >=  600) then 10
                                         when (segment_id = 'genre_sports_Golf'                  and acctsegmentsecs_raw_alldates >=  600) then 11
                                         when (segment_id = 'genre_sports_Ice Hockey'            and acctsegmentsecs_raw_alldates >=  300) then 12
                                         when (segment_id = 'genre_sports_Motor Sport'           and acctsegmentsecs_raw_alldates >=  600) then 13
                                         when (segment_id = 'genre_sports_Rugby'                 and acctsegmentsecs_raw_alldates >=  600) then 14
                                         when (segment_id = 'genre_sports_Snooker/Pool'          and acctsegmentsecs_raw_alldates >=  480) then 15
                                         when (segment_id = 'genre_sports_Tennis'                and acctsegmentsecs_raw_alldates >=  420) then 16
                                         when (segment_id = 'genre_sports_Watersports'           and acctsegmentsecs_raw_alldates >=  360) then 17
                                         when (segment_id = 'genre_sports_Wintersports'          and acctsegmentsecs_raw_alldates >=  300) then 18
                                         when (segment_id = 'genre_sports_Wrestling'             and acctsegmentsecs_raw_alldates >=  900) then 19
                                         when (segment_id = 'genre_sports_football'              and acctsegmentsecs_raw_alldates >=  3600) then 20
                                         when (segment_id = 'genre_sports_racing'                and acctsegmentsecs_raw_alldates >=  900) then 21
                                         when (segment_id = 'genre_sports_other'                 and acctsegmentsecs_raw_alldates >=  12000) then 22                 -- keep this ??
--                                       when (segment_id = 'genre_sports'                       and acctsegmentsecs_raw_alldates >=  17700) then 23
--                                       when (segment_id = 'genre_sports_undefined'             and acctsegmentsecs_raw_alldates >=  5400) then 24
                                         else null end))


    --    ,match = 1 -- (case when box.account_number = don.account_number then 1 else null end)
into panel_sov
from consistency_table2 box
left join rombaoad.V98_MainTable_SOV_Final don
on box.account_number = don.account_number
group by cb_key_household,box.account_number,box.subscriber_id


-- lets index Cb_key household
create  hg index idx27 on panel_sov(cb_key_household);



-- QA checks --

select top 1000 * from panel_sov

select count(*) from panel_sov

select max(breadth_of_sports) from panel_sov --16 - it worked!
-- should be a max of 22
select count(case when breadth_of_sports > breadth_of_sports1 then 1 else null end) as count from panel_sov

-- looks good
--------------------


-- lets get a distribution for good measure -- of those customers that were matched (89%)

select distinct(breadth_of_sports)
        ,count(account_number)
from panel_sov
where matched = 1
group by breadth_of_sports
order by breadth_of_sports




--- do the same for share of sports viewing (sports decile)


select distinct(Sports_decile)
        ,count(account_number)
from panel_sov
where matched = 1
group by Sports_decile
order by Sports_decile



---------------------------------------------
-- Lets append scaling to this table
---------------------------------------------

----- now run the scaling;

--execute SC2_do_weekly_segmentation '2012-06-10',0,'2013-01-10'
--EXECUTE SC2_prepare_panel_members '2012-06-10','','2013-01-10'
--EXECUTE SC2_make_weights '2012-06-10','2013-01-10',''

-- lets consolidate capping;
alter table SC2_intervals
add weighting float


update gillh.SC2_intervals
        set inn.weighting = wei.weighting
from gillh.SC2_intervals as inn
left join gillh.SC2_weightings as wei
on inn.scaling_segment_ID = wei.scaling_segment_ID

-- check it
select sum(weighting) from gillh.SC2_intervals



-- now lets add this to the panel cube


alter table panel_sov
add weighting float


update panel_sov
        set sov.weighting = wei.weighting
from panel_sov as sov
left join SC2_intervals as wei
on sov.account_number = wei.account_number


select sum(weighting) from panel_sov
-- 9436945.241539




-- ok now we have the base table for the vespa panel and some key feilds -- lets now establish the Sky base and the UK base

-------------------------------------------------------------------------------------------------------------
-- 1: the Sky base
-------------------------------------------------------------------------------------------------------------

SET @var_period_start           = '2012-06-01' --update to new period
SET @var_period_end             = '2012-06-30'




-- a: lets find all active Sky customers as of the start of the campaign; 29th Feb 2012


     SELECT   account_number
             ,cb_key_household
             ,cb_key_individual
             ,current_short_description
             ,rank() over (PARTITION BY account_number ORDER BY effective_from_dt desc, cb_row_id) AS rank
             ,convert(bit, 0)  AS uk_standard_account
       INTO v126_active_customer_base
       FROM sk_prod.cust_subs_hist
      WHERE subscription_sub_type IN ('DTV Primary Viewing')
        AND status_code IN ('AC','AB','PC')
        AND effective_from_dt    <= @var_period_start --'2012-02-29'
        AND effective_to_dt      > @var_period_start
        AND effective_from_dt    <> effective_to_dt
        AND EFFECTIVE_FROM_DT    IS NOT NULL
        AND cb_key_household     > 0
        AND cb_key_household     IS NOT NULL
        AND account_number       IS NOT NULL
        AND service_instance_id  IS NOT NULL
--9935284 Row(s) affected

-- remove duplicates
delete from v108_active_customer_base where rank > 1

-- we only want to keep UK accounts

   UPDATE v108_active_customer_base
     SET
         uk_standard_account = CASE
             WHEN b.acct_type='Standard' AND b.account_number <>'?' AND b.pty_country_code ='GBR' THEN 1
             ELSE 0 END
     FROM v108_active_customer_base AS a
     inner join sk_prod.cust_single_account_view AS b
     ON a.account_number = b.account_number

     DELETE FROM v108_active_customer_base WHERE uk_standard_account = 0

     COMMIT

-- do a quick check --
select count(*) from v126_active_customer_base -- 9,375,559 -- UK accounts active at start of campaign
select count(distinct(cb_key_household)) from v126_active_customer_base
select count(distinct(account_number)) from v126_active_customer_base

select top 10 * from v126_active_customer_base -- this has all the feilds that we are looking for!


-- lets index Cb_key household
create  hg index idx26 on v126_active_customer_base(cb_key_household);


-------------------------------------------------------------------------------------------------------------
-- 1: The UK base
-------------------------------------------------------------------------------------------------------------

-- lets have a look before we start:
select top 10 * from sk_prod.EXPERIAN_CONSUMERVIEW
select max(cb_data_date) as max, min(cb_data_date) as min from sk_prod.EXPERIAN_CONSUMERVIEW
select count(*) from sk_prod.EXPERIAN_CONSUMERVIEW -- 49million -- individuals
select count(distinct(cb_key_household)) from sk_prod.EXPERIAN_CONSUMERVIEW -- households 25 million





-- we will eventually be needing some other fields from this table so lets just pull them in now;


-- we need this table at household level - but initally i will pull this at individual level just incase we need an individual match later ( 16-24 male part )
SELECT  cb_key_individual
        ,max(Cb_Key_Household) as Cb_Key_Household

        ,max(CASE WHEN CV.p_gender = '01' THEN 'Male'
                  WHEN CV.p_gender = '02' THEN 'Female'
                ELSE                            'Unknown'       END) as gender

        ,max(CASE WHEN CV.p_age_fine = '01' THEN 'Age 26-30'
                WHEN CV.p_age_fine = '02' THEN 'Age 31-35'
                WHEN CV.p_age_fine = '03' THEN 'Age 36-40'
                WHEN CV.p_age_fine = '04' THEN 'Age 41-45'
                WHEN CV.p_age_fine = '05' THEN 'Age 46-50'
                WHEN CV.p_age_fine = '06' THEN 'Age 51-55'
                WHEN CV.p_age_fine = '07' THEN 'Age 56-60'
                WHEN CV.p_age_fine = '08' THEN 'Age 61-65'
                WHEN CV.p_age_fine = '09' THEN 'Age 66-70'
                WHEN CV.p_age_fine = '10' THEN 'Age 71-75'
                WHEN CV.p_age_fine = '11' THEN 'Age 76+'
                WHEN CV.p_age_fine = 'U' THEN  'Unclassified'
            ELSE                               'Unknown'            END) as age

        ,max(CASE WHEN CV.h_household_composition = '00' THEN 'Families'
                WHEN CV.h_household_composition = '01' THEN 'Extended family'
                WHEN CV.h_household_composition = '02' THEN 'Extended household'
                WHEN CV.h_household_composition = '03' THEN 'Pseudo family'
                WHEN CV.h_household_composition = '04' THEN 'Single male'
                WHEN CV.h_household_composition = '05' THEN 'Single female'
                WHEN CV.h_household_composition = '06' THEN 'Male homesharers'
                WHEN CV.h_household_composition = '07' THEN 'Female homesharers'
                WHEN CV.h_household_composition = '08' THEN 'Mixed homesharers'
                WHEN CV.h_household_composition = '09' THEN 'Abbreviated male families'
                WHEN CV.h_household_composition = '10' THEN 'Abbreviated female families'
                WHEN CV.h_household_composition = '11' THEN 'Multi-occupancy dwelling'
                WHEN CV.h_household_composition = 'U' THEN  'Unclassified'
            ELSE                                            'Unknown'            END) as household_composition




     ,max(h_affluence_v2)    as household_aff_raw  -- *** this will be allocated into bandings later - standardised for all future analysis

     ,max( case when h_affluence_v2 = '00' THEN '0-5%'
              when h_affluence_v2 = '01' THEN '6-10%'
              when h_affluence_v2 = '02' THEN '11-15%'
              when h_affluence_v2 = '03' THEN '16-20%'
              when h_affluence_v2 = '04' THEN '21-25%'
              when h_affluence_v2 = '05' THEN '26-30%'
              when h_affluence_v2 = '06' THEN '31-35%'
              when h_affluence_v2 = '07' THEN '36-40%'
              when h_affluence_v2 = '08' THEN '41-45%'
              when h_affluence_v2 = '09' THEN '46-50%'
              when h_affluence_v2 = '10' THEN '51-55%'
              when h_affluence_v2 = '11' THEN '56-60%'
              when h_affluence_v2 = '12' THEN '61-65%'
              when h_affluence_v2 = '13' THEN '66-70%'
              when h_affluence_v2 = '14' THEN '71-75%'
              when h_affluence_v2 = '15' THEN '76-80%'
              when h_affluence_v2 = '16' THEN '81-85%'
              when h_affluence_v2 = '17' THEN '86-90%'
              when h_affluence_v2 = '18' THEN '91-95%'
              when h_affluence_v2 = '19' THEN '96-100%'
              when h_affluence_v2 = 'U' THEN 'Default'
         ELSE                                            'Unknown'            END) as household_affluence


INTO v126_uk_Base
FROM sk_prod.EXPERIAN_CONSUMERVIEW cv
GROUP BY CV.Cb_Key_individual;
-- 47,785,183 Row(s) affected



create  hg index idx33 on v126_uk_Base(cb_key_household);
create  hg index idx21 on v126_uk_Base(cb_key_individual);

--- lets also append social grade information while here;


------
-- a: get customers Social grade from CACI tables
------

--drop table #caci_sc1

select  c.cb_row_id
        ,c.cb_key_individual
        ,c.cb_key_household
        ,c.lukcat_fr_de_nrs AS social_grade
        ,playpen.p_head_of_household
        ,rank() over(PARTITION BY c.cb_key_household ORDER BY playpen.p_head_of_household desc, c.lukcat_fr_de_nrs asc, c.cb_row_id desc) as rank_id
into caci_sc1
from sk_prod.CACI_SOCIAL_CLASS as c,
     sk_prod.PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD as playpen,
     sk_prod.experian_consumerview e
where e.exp_cb_key_individual = playpen.exp_cb_key_individual
  and e.cb_key_individual = c.cb_key_individual
  and c.cb_address_dps is NOT NULL
order by c.cb_key_household;


--de-dupe!
delete from caci_sc1 where rank_id > 1  -- more than half!



-- now lets add this data to the table


alter table v126_uk_Base
 add social_grade varchar(5);

 commit

-- HG : lots of different Values
-- LF : categories (up to 1500)


create  hg index idx1 on caci_sc1(cb_key_household);
create  lf index idx38 on caci_sc1(social_grade);



update v126_uk_Base
set uk.social_grade = cac.social_grade
from v126_uk_Base uk
 join caci_sc1 cac
on uk.cb_key_household = cac.cb_key_household



select top 10 * from v126_uk_Base

select top 10 * from v126_active_customer_base

-- we now have the Sky base and the UK base - we also have the


-------------------------------------------------------------------------------------------------------------
-- 1: Append data to the the three universe tables from above, panel, sky base, uk households
-------------------------------------------------------------------------------------------------------------


select top 10 * from v126_uk_Base

select top 10 * from v126_active_customer_base

select top 10 * from  panel_sov



-- lets index Cb_key household
create  hg index idx26 on v126_active_customer_base(cb_key_household);
create  hg index idx27 on panel_sov(cb_key_household);



-- lets add new fields to the tables for social grade;
alter table v126_active_customer_base
 add social_grade varchar(5);

alter table panel_sov
 add social_grade varchar(5);


-- lets update the feilds:
update v126_active_customer_base
set uk.social_grade = cac.social_grade
from v126_active_customer_base uk
 join caci_sc1 cac
on uk.cb_key_household = cac.cb_key_household

update panel_sov
set uk.social_grade = cac.social_grade
from panel_sov uk
 join caci_sc1 cac
on uk.cb_key_household = cac.cb_key_household




--------------------------------------------------------------------------------------------
--- LETS NOW ADD THE REMINING FEILDS;
--------------------------------------------------------------------------------------------


--- create the feilds that we need in all three tables:


alter table v126_active_customer_base
 add( gender                    varchar(6)
        ,age                     varchar(13)
        ,household_composition   varchar(30)
        ,household_aff_raw       varchar(4)
        ,household_affluence     varchar(8)
        ,package_premium         varchar(20)

-- from playpen -- these are catgorical, yes/no; PLAYPEN_EXPERIAN_LIFESTYLE
      ,Football_interest varchar(1)
      ,HORSE_RACING_interest varchar(1)
      ,RUGBY_interest varchar(1)
      ,BOATS_YACHTING_interest varchar(1)
      ,SQUASH_interest varchar(1)
      ,TENNIS_interest varchar(1)
      ,HIKING_WALKING_interest varchar(1)
      ,WATER_SPORTS_interest varchar(1)
      ,WINDSURFING_interest varchar(1)
      ,GYM_interest varchar(1)
      ,FITNESS_HEALTH_interest varchar(1)
      ,interests_keeping_fit varchar(1)


-- person level percentiles -- CURRENT
        ,sports_cricket_perc integer
        ,sports_cycling_perc integer
        ,sports_fishing_perc integer
        ,sports_football_perc integer
        ,sports_golf_perc integer
        ,sports_skiing_perc integer
        ,sports_tennis_perc integer
        ,sports_watersports_perc integer
   -- NEW
        ,interested_in_hiking_percentile integer
        ,enjoy_playing_golf_percentile integer
        ,enjoy_playing_football_percentile integer
        ,read_sport_magazines_percentile integer
        interested_in_keeping_fit_percentile integer);



select top 10 * from v126_active_customer_base

-- that is allot of additional fields and potentially very very large tables!




alter table panel_sov
 add( gender                    varchar(6)
        ,age                     varchar(13)
        ,household_composition   varchar(30)
        ,household_aff_raw       varchar(4)
        ,household_affluence     varchar(8)
        ,package_premium         varchar(20)

-- from playpen -- these are catgorical, yes/no; PLAYPEN_EXPERIAN_LIFESTYLE
      ,Football_interest varchar(1)
      ,HORSE_RACING_interest varchar(1)
      ,RUGBY_interest varchar(1)
      ,BOATS_YACHTING_interest varchar(1)
      ,SQUASH_interest varchar(1)
      ,TENNIS_interest varchar(1)
      ,HIKING_WALKING_interest varchar(1)
      ,WATER_SPORTS_interest varchar(1)
      ,WINDSURFING_interest varchar(1)
      ,GYM_interest varchar(1)
      ,FITNESS_HEALTH_interest varchar(1)
      ,interests_keeping_fit varchar(1)


-- person level percentiles
        ,sports_cricket_perc integer
        ,sports_cycling_perc integer
        ,sports_fishing_perc integer
        ,sports_football_perc integer
        ,sports_golf_perc integer
        ,sports_skiing_perc integer
        ,sports_tennis_perc integer
        ,sports_watersports_perc integer

        ,interested_in_hiking_percentile integer
        ,enjoy_playing_golf_percentile integer
        ,enjoy_playing_football_percentile integer
        ,read_sport_magazines_percentile integer
        ,interested_in_keeping_fit_percentile integer);

   select top 10 * from panel_sov



-- finally the UK base:


SELECT TOP 1000 * FROM v126_uk_Base

alter table v126_uk_Base
 add(

-- from playpen -- these are catgorical, yes/no; PLAYPEN_EXPERIAN_LIFESTYLE
      ,Football_interest varchar(1)
      ,HORSE_RACING_interest varchar(1)
      ,RUGBY_interest varchar(1)
      ,BOATS_YACHTING_interest varchar(1)
      ,SQUASH_interest varchar(1)
      ,TENNIS_interest varchar(1)
      ,HIKING_WALKING_interest varchar(1)
      ,WATER_SPORTS_interest varchar(1)
      ,WINDSURFING_interest varchar(1)
      ,GYM_interest varchar(1)
      ,FITNESS_HEALTH_interest varchar(1)
      ,interests_keeping_fit varchar(1)


-- person level percentiles
        ,sports_cricket_perc integer
        ,sports_cycling_perc integer
        ,sports_fishing_perc integer
        ,sports_football_perc integer
        ,sports_golf_perc integer
        ,sports_skiing_perc integer
        ,sports_tennis_perc integer
        ,sports_watersports_perc integer

        ,interested_in_hiking_percentile integer
        ,enjoy_playing_golf_percentile integer
        ,enjoy_playing_football_percentile integer
        ,read_sport_magazines_percentile integer
        ,interested_in_keeping_fit_percentile integer);


   select top 10 * from v126_uk_Base


----------------
-- now lets populate the fields;
----------------


-----
-- package details:
-----

-- we want this for the whole UK base;

drop table #tvpackage

SELECT          csh.account_number
                ,max(case when cel.prem_sports + cel.prem_movies  = 4   then 'Top Tier'
                     when cel.prem_sports = 2 and cel.prem_movies = 1   then 'Dual Sports Single Movies'
                     when cel.prem_sports = 2 and cel.prem_movies = 0   then 'Dual Sports'
                     when cel.prem_sports = 1 and cel.prem_movies = 2   then 'Single Sports Dual Movies'
                     when cel.prem_sports = 0 and cel.prem_movies = 2   then 'Dual Movies'
                     when cel.prem_sports = 1 and cel.prem_movies = 1   then 'Single Sports Single Movies'
                     when cel.prem_sports = 1 and cel.prem_movies = 0   then 'Single Sports'
                     when cel.prem_sports = 0 and cel.prem_movies = 1   then 'Single Movies'
                     when cel.prem_sports + cel.prem_movies = 0         then 'Basic'
                     else                                                    'Unknown'
                end) as tv_premiums,
                max(case when (music = 0 AND news_events = 0 AND kids = 0 AND knowledge = 0)
                     then 'Entertainment'
                     when (music = 1 or news_events = 1 or kids = 1 or knowledge = 1)
                     then 'Entertainment Extra'
                     else 'Unknown' end) as tv_package
into            #tvpackage
FROM            sk_prod.cust_subs_hist as csh
        inner join sk_prod.cust_entitlement_lookup as cel
                on csh.current_short_description = cel.short_description
WHERE           csh.subscription_sub_type ='DTV Primary Viewing'
AND             csh.subscription_type = 'DTV PACKAGE'
AND             csh.status_code in ('AC','AB','PC')
AND             csh.effective_from_dt < today() -- i.e. they had the same package for the whole period
AND             csh.effective_to_dt   >= today()
AND             csh.effective_from_dt != csh.effective_to_dt
group by csh.account_number ;

select count(*) from #tvpackage
--85102




-- now lets update the panel land the Sky base files:

update          panel_sov as base
set             base.package_premium = tvp.tv_premiums
from            #tvpackage as tvp
where           base.account_number = tvp.account_number

update          panel_sov as base
set             package_premium = case when package_premium is null then 'No Match' else package_premium end



update          v126_active_customer_base as base
set             base.package_premium = tvp.tv_premiums
from            #tvpackage as tvp
where           base.account_number = tvp.account_number
commit

update          v126_active_customer_base as base
set             package_premium = case when package_premium is null then 'No Match' else package_premium end





-----
-- Part 1 UPDATE EXPERIAN playpen VARIABLES:
-----


--- the percentiles in the experian tables are individual -- the panel universe is at household level (as per breif)
-- will take the max percentile per CB_key_household and use that






-- from playpen -- these are catgorical, yes/no; PLAYPEN_EXPERIAN_LIFESTYL



-- LETS AGGREGATE THIS TO HOUSEHOLD LEVEL FIRST:
DROP TABLE #PLAYPEN_EXPERIAN_LIFESTYLE

select cb_key_household
      ,max(S3_006651_data_INTR_SPOR_FBAL_ENJOY_FOOTBALL_DO) AS S3_006651_data_INTR_SPOR_FBAL_ENJOY_FOOTBALL_DO
      ,max(S3_006656_data_INTR_SPOR_ENJY_HORSE_RACING) AS S3_006656_data_INTR_SPOR_ENJY_HORSE_RACING
      ,max(S3_006666_data_INTR_SPOR_ENJY_RUGBY_DO) AS S3_006666_data_INTR_SPOR_ENJY_RUGBY_DO
      ,max(S3_006668_data_INTR_SPOR_ENJY_BOATS_YACHTING_DO) AS S3_006668_data_INTR_SPOR_ENJY_BOATS_YACHTING_DO
      ,max(S3_006674_data_INTR_SPOR_ENJY_SQUASH_DO) AS S3_006674_data_INTR_SPOR_ENJY_SQUASH_DO
      ,max(S3_006677_data_INTR_SPOR_ENJY_TENNIS_DO) AS S3_006677_data_INTR_SPOR_ENJY_TENNIS_DO
      ,max(S3_006678_data_INTR_SPOR_ENJY_HIKING_WALKING) AS S3_006678_data_INTR_SPOR_ENJY_HIKING_WALKING
      ,max(S3_006679_data_INTR_SPOR_ENJY_WATER_SPORTS_DO) AS S3_006679_data_INTR_SPOR_ENJY_WATER_SPORTS_DO
      ,max(S3_006680_data_INTR_SPOR_ENJY_WINDSURFING) AS S3_006680_data_INTR_SPOR_ENJY_WINDSURFING
      ,max(S3_010893_data_INTR_HOBB_ENJY_GOING_TO_THE_GYM) AS S3_010893_data_INTR_HOBB_ENJY_GOING_TO_THE_GYM
      ,max(S3_012171_data_INTR_SPOR_ENJY_FITNESS_HEALTH_DO) AS S3_012171_data_INTR_SPOR_ENJY_FITNESS_HEALTH_DO
      ,max(interests_keeping_fit) AS interests_keeping_fit
into #PLAYPEN_EXPERIAN_LIFESTYLE
from sk_prod.PLAYPEN_EXPERIAN_LIFESTYLE
group by cb_key_household



-- data dictionary indicates nulls are either No or Unknowns
UPDATE #PLAYPEN_EXPERIAN_LIFESTYLE
SET S3_006651_data_INTR_SPOR_FBAL_ENJOY_FOOTBALL_DO = (CASE WHEN S3_006651_data_INTR_SPOR_FBAL_ENJOY_FOOTBALL_DO IS NULL THEN 'N' ELSE S3_006651_data_INTR_SPOR_FBAL_ENJOY_FOOTBALL_DO END)
    ,S3_006656_data_INTR_SPOR_ENJY_HORSE_RACING = (CASE WHEN S3_006656_data_INTR_SPOR_ENJY_HORSE_RACING IS NULL THEN 'N' ELSE S3_006656_data_INTR_SPOR_ENJY_HORSE_RACING END)
    ,S3_006666_data_INTR_SPOR_ENJY_RUGBY_DO = (CASE WHEN S3_006666_data_INTR_SPOR_ENJY_RUGBY_DO IS NULL THEN 'N' ELSE S3_006666_data_INTR_SPOR_ENJY_RUGBY_DO END)
    ,S3_006668_data_INTR_SPOR_ENJY_BOATS_YACHTING_DO = (CASE WHEN S3_006668_data_INTR_SPOR_ENJY_BOATS_YACHTING_DO IS NULL THEN 'N' ELSE S3_006668_data_INTR_SPOR_ENJY_BOATS_YACHTING_DO END)
    ,S3_006674_data_INTR_SPOR_ENJY_SQUASH_DO = (CASE WHEN S3_006674_data_INTR_SPOR_ENJY_SQUASH_DO IS NULL THEN 'N' ELSE S3_006674_data_INTR_SPOR_ENJY_SQUASH_DO END)
    ,S3_006677_data_INTR_SPOR_ENJY_TENNIS_DO = (CASE WHEN S3_006677_data_INTR_SPOR_ENJY_TENNIS_DO IS NULL THEN 'N' ELSE S3_006677_data_INTR_SPOR_ENJY_TENNIS_DO END)
    ,S3_006678_data_INTR_SPOR_ENJY_HIKING_WALKING = (CASE WHEN S3_006678_data_INTR_SPOR_ENJY_HIKING_WALKING IS NULL THEN 'N' ELSE S3_006678_data_INTR_SPOR_ENJY_HIKING_WALKING END)
    ,S3_006679_data_INTR_SPOR_ENJY_WATER_SPORTS_DO = (CASE WHEN S3_006679_data_INTR_SPOR_ENJY_WATER_SPORTS_DO IS NULL THEN 'N' ELSE S3_006679_data_INTR_SPOR_ENJY_WATER_SPORTS_DO END)
    ,S3_006680_data_INTR_SPOR_ENJY_WINDSURFING = (CASE WHEN S3_006680_data_INTR_SPOR_ENJY_WINDSURFING IS NULL THEN 'N' ELSE S3_006680_data_INTR_SPOR_ENJY_WINDSURFING END)
    ,S3_010893_data_INTR_HOBB_ENJY_GOING_TO_THE_GYM = (CASE WHEN S3_010893_data_INTR_HOBB_ENJY_GOING_TO_THE_GYM IS NULL THEN 'N' ELSE S3_010893_data_INTR_HOBB_ENJY_GOING_TO_THE_GYM END)
    ,S3_012171_data_INTR_SPOR_ENJY_FITNESS_HEALTH_DO = (CASE WHEN S3_012171_data_INTR_SPOR_ENJY_FITNESS_HEALTH_DO IS NULL THEN 'N' ELSE S3_012171_data_INTR_SPOR_ENJY_FITNESS_HEALTH_DO END)
    ,interests_keeping_fit = (CASE WHEN interests_keeping_fit IS NULL THEN 'N' ELSE interests_keeping_fit END)




--- lets do some high level checks on this table --------------------------------------------------------------
SELECT TOP 1000 * FROM #PLAYPEN_EXPERIAN_LIFESTYLE

select count(*) from #PLAYPEN_EXPERIAN_LIFESTYLE
-- appears to cover 30% of the UK


select count(*)
        ,sum(case when S3_006651_data_INTR_SPOR_FBAL_ENJOY_FOOTBALL_DO is not null then 1 else 0 end) as S3_006651_data_INTR_SPOR_FBAL_ENJOY_FOOTBALL_DO
        ,sum(case when S3_006656_data_INTR_SPOR_ENJY_HORSE_RACING is not null then 1 else 0 end) as S3_006656_data_INTR_SPOR_ENJY_HORSE_RACING
        ,sum(case when S3_006666_data_INTR_SPOR_ENJY_RUGBY_DO is not null then 1 else 0 end) as S3_006666_data_INTR_SPOR_ENJY_RUGBY_DO
        ,sum(case when S3_006668_data_INTR_SPOR_ENJY_BOATS_YACHTING_DO is not null then 1 else 0 end) as S3_006668_data_INTR_SPOR_ENJY_BOATS_YACHTING_DO
         ,sum(case when S3_006674_data_INTR_SPOR_ENJY_SQUASH_DO is not null then 1 else 0 end) as S3_006674_data_INTR_SPOR_ENJY_SQUASH_DO
         ,sum(case when S3_006677_data_INTR_SPOR_ENJY_TENNIS_DO is not null then 1 else 0 end) as S3_006677_data_INTR_SPOR_ENJY_TENNIS_DO
         ,sum(case when S3_006678_data_INTR_SPOR_ENJY_HIKING_WALKING is not null then 1 else 0 end) as S3_006678_data_INTR_SPOR_ENJY_HIKING_WALKING
         ,sum(case when S3_006679_data_INTR_SPOR_ENJY_WATER_SPORTS_DO is not null then 1 else 0 end) as S3_006679_data_INTR_SPOR_ENJY_WATER_SPORTS_DO
         ,sum(case when S3_006680_data_INTR_SPOR_ENJY_WINDSURFING is not null then 1 else 0 end) as S3_006680_data_INTR_SPOR_ENJY_WINDSURFING
         ,sum(case when S3_010893_data_INTR_HOBB_ENJY_GOING_TO_THE_GYM is not null then 1 else 0 end) as S3_010893_data_INTR_HOBB_ENJY_GOING_TO_THE_GYM
         ,sum(case when S3_012171_data_INTR_SPOR_ENJY_FITNESS_HEALTH_DO is not null then 1 else 0 end) as S3_012171_data_INTR_SPOR_ENJY_FITNESS_HEALTH_DO
         ,sum(case when interests_keeping_fit is not null then 1 else 0 end) as interests_keeping_fit
from #PLAYPEN_EXPERIAN_LIFESTYLE
-- no nulls left but allot of No's


select top 1000 * from #PLAYPEN_EXPERIAN_LIFESTYLE


-- the above looks OK -- there wont be a great match rate but lets start updating the 3 universes:
----------------------------------------------------------------------------------------------------------------





-- now lets update the panel;
-- this will be done at HH level
update panel_sov as base
 set   Football_interest = (S3_006651_data_INTR_SPOR_FBAL_ENJOY_FOOTBALL_DO)
      ,HORSE_RACING_interest = (S3_006656_data_INTR_SPOR_ENJY_HORSE_RACING)
      ,RUGBY_interest = (S3_006666_data_INTR_SPOR_ENJY_RUGBY_DO)
      ,BOATS_YACHTING_interest = (S3_006668_data_INTR_SPOR_ENJY_BOATS_YACHTING_DO)
      ,SQUASH_interest = (S3_006674_data_INTR_SPOR_ENJY_SQUASH_DO)
      ,TENNIS_interest = (S3_006677_data_INTR_SPOR_ENJY_TENNIS_DO)
      ,HIKING_WALKING_interest = (S3_006678_data_INTR_SPOR_ENJY_HIKING_WALKING)
      ,WATER_SPORTS_interest = (S3_006679_data_INTR_SPOR_ENJY_WATER_SPORTS_DO)
      ,WINDSURFING_interest = (S3_006680_data_INTR_SPOR_ENJY_WINDSURFING)
      ,GYM_interest = (S3_010893_data_INTR_HOBB_ENJY_GOING_TO_THE_GYM)
      ,FITNESS_HEALTH_interest = (S3_012171_data_INTR_SPOR_ENJY_FITNESS_HEALTH_DO)
      ,base.interests_keeping_fit = (exp.interests_keeping_fit)
from #PLAYPEN_EXPERIAN_LIFESTYLE as exp
where base.cb_key_household = exp.cb_key_household
--54,114 updated

select top 1000 * from panel_sov -- where there are nulls there was no match between the data sets



--- update the Sky customer base
update v126_active_customer_base as base
 set   Football_interest = (S3_006651_data_INTR_SPOR_FBAL_ENJOY_FOOTBALL_DO)
      ,HORSE_RACING_interest = (S3_006656_data_INTR_SPOR_ENJY_HORSE_RACING)
      ,RUGBY_interest = (S3_006666_data_INTR_SPOR_ENJY_RUGBY_DO)
      ,BOATS_YACHTING_interest = (S3_006668_data_INTR_SPOR_ENJY_BOATS_YACHTING_DO)
      ,SQUASH_interest = (S3_006674_data_INTR_SPOR_ENJY_SQUASH_DO)
      ,TENNIS_interest = (S3_006677_data_INTR_SPOR_ENJY_TENNIS_DO)
      ,HIKING_WALKING_interest = (S3_006678_data_INTR_SPOR_ENJY_HIKING_WALKING)
      ,WATER_SPORTS_interest = (S3_006679_data_INTR_SPOR_ENJY_WATER_SPORTS_DO)
      ,WINDSURFING_interest = (S3_006680_data_INTR_SPOR_ENJY_WINDSURFING)
      ,GYM_interest = (S3_010893_data_INTR_HOBB_ENJY_GOING_TO_THE_GYM)
      ,FITNESS_HEALTH_interest = (S3_012171_data_INTR_SPOR_ENJY_FITNESS_HEALTH_DO)
      ,base.interests_keeping_fit = (exp.interests_keeping_fit)
from #PLAYPEN_EXPERIAN_LIFESTYLE as exp
where base.cb_key_household = exp.cb_key_household

select top 1000 * from v126_active_customer_base -- where there are nulls there was no match between the data sets



--- update the UK
update v126_uk_Base as base
 set   Football_interest = (S3_006651_data_INTR_SPOR_FBAL_ENJOY_FOOTBALL_DO)
      ,HORSE_RACING_interest = (S3_006656_data_INTR_SPOR_ENJY_HORSE_RACING)
      ,RUGBY_interest = (S3_006666_data_INTR_SPOR_ENJY_RUGBY_DO)
      ,BOATS_YACHTING_interest = (S3_006668_data_INTR_SPOR_ENJY_BOATS_YACHTING_DO)
      ,SQUASH_interest = (S3_006674_data_INTR_SPOR_ENJY_SQUASH_DO)
      ,TENNIS_interest = (S3_006677_data_INTR_SPOR_ENJY_TENNIS_DO)
      ,HIKING_WALKING_interest = (S3_006678_data_INTR_SPOR_ENJY_HIKING_WALKING)
      ,WATER_SPORTS_interest = (S3_006679_data_INTR_SPOR_ENJY_WATER_SPORTS_DO)
      ,WINDSURFING_interest = (S3_006680_data_INTR_SPOR_ENJY_WINDSURFING)
      ,GYM_interest = (S3_010893_data_INTR_HOBB_ENJY_GOING_TO_THE_GYM)
      ,FITNESS_HEALTH_interest = (S3_012171_data_INTR_SPOR_ENJY_FITNESS_HEALTH_DO)
      ,base.interests_keeping_fit = (exp.interests_keeping_fit)
from #PLAYPEN_EXPERIAN_LIFESTYLE as exp
where base.cb_key_household = exp.cb_key_household


select top 1000 * from v126_uk_Base-- where there are nulls there was no match between the data sets












-----
-- Part 2 UPDATE PERSON_PROPENSITIES_GRID_CUR values;
-----



-- LETS AGGREGATE THIS TO HOUSEHOLD LEVEL FIRST:

DROP TABLE #PERSON_PROPENSITIES_GRID_CUR

SELECT cb_key_household
      ,max(CAST(sports_cricket_perc_p AS INTEGER)) AS sports_cricket_perc_p
      ,max(CAST(sports_cycling_perc_p AS INTEGER)) AS sports_cycling_perc_p
      ,max(CAST(sports_fishing_perc_p AS INTEGER)) AS sports_fishing_perc_p
      ,max(CAST(sports_football_perc_p AS INTEGER)) AS sports_football_perc_p
      ,max(CAST(sports_golf_perc_p AS INTEGER)) AS sports_golf_perc_p
      ,max(CAST(sports_skiing_perc_p AS INTEGER)) AS sports_skiing_perc_p
      ,max(CAST(sports_tennis_perc_p AS INTEGER)) AS sports_tennis_perc_p
      ,max(CAST(sports_watersports_perc_p AS INTEGER)) AS sports_watersports_perc_p
into #PERSON_PROPENSITIES_GRID_CUR
        FROM sk_prod.PERSON_PROPENSITIES_GRID_CUR pp
        JOIN sk_prod.EXPERIAN_CONSUMERVIEW cv
        ON pp.ppixel = cv.p_pixel_v2 and pp.mosaicuk = cv.Pc_mosaic_uk_type
        GROUP BY cb_key_household


SELECT TOP 1000 * FROM #PERSON_PROPENSITIES_GRID_CUR

--NO NULLS!
SELECT COUNT(*) FROM #PERSON_PROPENSITIES_GRID_CUR WHERE sports_cricket_perc_p IS NULL



-- LETS UPDATE EACH OF THE FIELDS TO DECILES INSTEAD OF PERCENTILES

UPDATE #PERSON_PROPENSITIES_GRID_CUR
SET   sports_cricket_perc_p = (CASE WHEN sports_cricket_perc_p BETWEEN 01  AND 10 THEN 1
                                    WHEN sports_cricket_perc_p BETWEEN 11 AND 20 THEN 2
                                    WHEN sports_cricket_perc_p BETWEEN 21 AND 30 THEN 3
                                    WHEN sports_cricket_perc_p BETWEEN 31 AND 40 THEN 4
                                    WHEN sports_cricket_perc_p BETWEEN 41 AND 50 THEN 5
                                    WHEN sports_cricket_perc_p BETWEEN 51 AND 60 THEN 6
                                    WHEN sports_cricket_perc_p BETWEEN 61 AND 70 THEN 7
                                    WHEN sports_cricket_perc_p BETWEEN 71 AND 80 THEN 8
                                    WHEN sports_cricket_perc_p BETWEEN 81 AND 90 THEN 9
                                    WHEN sports_cricket_perc_p BETWEEN 91 AND 100 THEN 10
                                    ELSE NULL END)

     ,sports_fishing_perc_p = (CASE WHEN sports_fishing_perc_p BETWEEN 01  AND 10 THEN 1
                                    WHEN sports_fishing_perc_p BETWEEN 11 AND 20 THEN 2
                                    WHEN sports_fishing_perc_p BETWEEN 21 AND 30 THEN 3
                                    WHEN sports_fishing_perc_p BETWEEN 31 AND 40 THEN 4
                                    WHEN sports_fishing_perc_p BETWEEN 41 AND 50 THEN 5
                                    WHEN sports_fishing_perc_p BETWEEN 51 AND 60 THEN 6
                                    WHEN sports_fishing_perc_p BETWEEN 61 AND 70 THEN 7
                                    WHEN sports_fishing_perc_p BETWEEN 71 AND 80 THEN 8
                                    WHEN sports_fishing_perc_p BETWEEN 81 AND 90 THEN 9
                                    WHEN sports_fishing_perc_p BETWEEN 91 AND 100 THEN 10
                                    ELSE NULL END)

     ,sports_football_perc_p = (CASE WHEN sports_football_perc_p BETWEEN 01  AND 10 THEN 1
                                    WHEN sports_football_perc_p BETWEEN 11 AND 20 THEN 2
                                    WHEN sports_football_perc_p BETWEEN 21 AND 30 THEN 3
                                    WHEN sports_football_perc_p BETWEEN 31 AND 40 THEN 4
                                    WHEN sports_football_perc_p BETWEEN 41 AND 50 THEN 5
                                    WHEN sports_football_perc_p BETWEEN 51 AND 60 THEN 6
                                    WHEN sports_football_perc_p BETWEEN 61 AND 70 THEN 7
                                    WHEN sports_football_perc_p BETWEEN 71 AND 80 THEN 8
                                    WHEN sports_football_perc_p BETWEEN 81 AND 90 THEN 9
                                    WHEN sports_football_perc_p BETWEEN 91 AND 100 THEN 10
                                    ELSE NULL END)


     ,sports_golf_perc_p = (CASE WHEN sports_golf_perc_p BETWEEN 01  AND 10 THEN 1
                                    WHEN sports_golf_perc_p BETWEEN 11 AND 20 THEN 2
                                    WHEN sports_golf_perc_p BETWEEN 21 AND 30 THEN 3
                                    WHEN sports_golf_perc_p BETWEEN 31 AND 40 THEN 4
                                    WHEN sports_golf_perc_p BETWEEN 41 AND 50 THEN 5
                                    WHEN sports_golf_perc_p BETWEEN 51 AND 60 THEN 6
                                    WHEN sports_golf_perc_p BETWEEN 61 AND 70 THEN 7
                                    WHEN sports_golf_perc_p BETWEEN 71 AND 80 THEN 8
                                    WHEN sports_golf_perc_p BETWEEN 81 AND 90 THEN 9
                                    WHEN sports_golf_perc_p BETWEEN 91 AND 100 THEN 10
                                    ELSE NULL END)

     ,sports_skiing_perc_p = (CASE WHEN sports_skiing_perc_p BETWEEN 01  AND 10 THEN 1
                                    WHEN sports_skiing_perc_p BETWEEN 11 AND 20 THEN 2
                                    WHEN sports_skiing_perc_p BETWEEN 21 AND 30 THEN 3
                                    WHEN sports_skiing_perc_p BETWEEN 31 AND 40 THEN 4
                                    WHEN sports_skiing_perc_p BETWEEN 41 AND 50 THEN 5
                                    WHEN sports_skiing_perc_p BETWEEN 51 AND 60 THEN 6
                                    WHEN sports_skiing_perc_p BETWEEN 61 AND 70 THEN 7
                                    WHEN sports_skiing_perc_p BETWEEN 71 AND 80 THEN 8
                                    WHEN sports_skiing_perc_p BETWEEN 81 AND 90 THEN 9
                                    WHEN sports_skiing_perc_p BETWEEN 91 AND 100 THEN 10
                                    ELSE NULL END)


     ,sports_tennis_perc_p = (CASE WHEN sports_tennis_perc_p BETWEEN 01  AND 10 THEN 1
                                    WHEN sports_tennis_perc_p BETWEEN 11 AND 20 THEN 2
                                    WHEN sports_tennis_perc_p BETWEEN 21 AND 30 THEN 3
                                    WHEN sports_tennis_perc_p BETWEEN 31 AND 40 THEN 4
                                    WHEN sports_tennis_perc_p BETWEEN 41 AND 50 THEN 5
                                    WHEN sports_tennis_perc_p BETWEEN 51 AND 60 THEN 6
                                    WHEN sports_tennis_perc_p BETWEEN 61 AND 70 THEN 7
                                    WHEN sports_tennis_perc_p BETWEEN 71 AND 80 THEN 8
                                    WHEN sports_tennis_perc_p BETWEEN 81 AND 90 THEN 9
                                    WHEN sports_tennis_perc_p BETWEEN 91 AND 100 THEN 10
                                    ELSE NULL END)


     ,sports_watersports_perc_p = (CASE WHEN sports_watersports_perc_p BETWEEN 01  AND 10 THEN 1
                                    WHEN sports_watersports_perc_p BETWEEN 11 AND 20 THEN 2
                                    WHEN sports_watersports_perc_p BETWEEN 21 AND 30 THEN 3
                                    WHEN sports_watersports_perc_p BETWEEN 31 AND 40 THEN 4
                                    WHEN sports_watersports_perc_p BETWEEN 41 AND 50 THEN 5
                                    WHEN sports_watersports_perc_p BETWEEN 51 AND 60 THEN 6
                                    WHEN sports_watersports_perc_p BETWEEN 61 AND 70 THEN 7
                                    WHEN sports_watersports_perc_p BETWEEN 71 AND 80 THEN 8
                                    WHEN sports_watersports_perc_p BETWEEN 81 AND 90 THEN 9
                                    WHEN sports_watersports_perc_p BETWEEN 91 AND 100 THEN 10
                                    ELSE NULL END)

-- LETS INDEX THIS (23 MILLION ROWS)
CREATE HG INDEX INDX101 ON #PERSON_PROPENSITIES_GRID_CUR(CB_KEY_HOUSEHOLD)


--- WE NOW HAVE THE BASE TABLE OF PERCENTILES - LETS NOW UPDATE THE 3 UNIVERSE:


-- now lets update the panel;
-- this will be done at HH level
update panel_sov as base
 set     sports_cricket_perc = sports_cricket_perc_p
        ,sports_cycling_perc = sports_cycling_perc_p
        ,sports_fishing_perc = sports_fishing_perc_p
        ,sports_football_perc = sports_football_perc_p
        ,sports_golf_perc = sports_golf_perc_p
        ,sports_skiing_perc = sports_skiing_perc_p
        ,sports_tennis_perc = sports_tennis_perc_p
        ,sports_watersports_perc  = sports_watersports_perc_p
from #PERSON_PROPENSITIES_GRID_CUR as exp
where base.cb_key_household = exp.cb_key_household
--54,114 updated

select top 1000 * from panel_sov -- where there are nulls there was no match between the data sets




--- update the Sky customer base
update v126_active_customer_base as base
 set     sports_cricket_perc = sports_cricket_perc_p
        ,sports_cycling_perc = sports_cycling_perc_p
        ,sports_fishing_perc = sports_fishing_perc_p
        ,sports_football_perc = sports_football_perc_p
        ,sports_golf_perc = sports_golf_perc_p
        ,sports_skiing_perc = sports_skiing_perc_p
        ,sports_tennis_perc = sports_tennis_perc_p
        ,sports_watersports_perc  = sports_watersports_perc_p
from #PERSON_PROPENSITIES_GRID_CUR as exp
where base.cb_key_household = exp.cb_key_household

select top 1000 * from v126_active_customer_base -- where there are nulls there was no match between the data sets



--- update the UK
update v126_uk_Base as base
 set     sports_cricket_perc = sports_cricket_perc_p
        ,sports_cycling_perc = sports_cycling_perc_p
        ,sports_fishing_perc = sports_fishing_perc_p
        ,sports_football_perc = sports_football_perc_p
        ,sports_golf_perc = sports_golf_perc_p
        ,sports_skiing_perc = sports_skiing_perc_p
        ,sports_tennis_perc = sports_tennis_perc_p
        ,sports_watersports_perc  = sports_watersports_perc_p
from #PERSON_PROPENSITIES_GRID_CUR as exp
where base.cb_key_household = exp.cb_key_household


select top 1000 * from v126_uk_Base-- where there are nulls there was no match between the data sets
















-----
-- Part 2 UPDATE PERSON_PROPENSITIES_GRID_NEW!!  values;
-----


-- LETS AGGREGATE THIS TO HOUSEHOLD LEVEL FIRST:

sp_columns 'PERSON_PROPENSITIES_GRID_NEW'

DROP TABLE #PERSON_PROPENSITIES_GRID_NEW


SELECT cb_key_household
      ,max(CAST(interested_in_hiking_percentile AS INTEGER)) AS interested_in_hiking_percentile
      ,max(CAST(enjoy_playing_golf_percentile AS INTEGER)) AS enjoy_playing_golf_percentile
      ,max(CAST(enjoy_playing_football_percentile AS INTEGER)) AS enjoy_playing_football_percentile
      ,max(CAST(read_sport_magazines_percentile AS INTEGER)) AS read_sport_magazines_percentile
      ,max(CAST(interested_in_keeping_fit_percentile AS INTEGER)) AS interested_in_keeping_fit_percentile

into #PERSON_PROPENSITIES_GRID_NEW
        FROM sk_prod.PERSON_PROPENSITIES_GRID_NEW pp
        JOIN sk_prod.EXPERIAN_CONSUMERVIEW cv
        ON pp.ppixel2011 = cv.p_pixel_v2 and pp.mosaic_uk_2009_type = cv.Pc_mosaic_uk_type
        GROUP BY cb_key_household


-- LETS INDEX THIS (23 MILLION ROWS)
CREATE HG INDEX INDX102 ON #PERSON_PROPENSITIES_GRID_NEW(CB_KEY_HOUSEHOLD)
--




-- LETS UPDATE EACH OF THE FIELDS TO DECILES INSTEAD OF PERCENTILES

UPDATE #PERSON_PROPENSITIES_GRID_NEW
SET   interested_in_hiking_percentile = (CASE WHEN interested_in_hiking_percentile BETWEEN 01  AND 10 THEN 1
                                    WHEN interested_in_hiking_percentile BETWEEN 11 AND 20 THEN 2
                                    WHEN interested_in_hiking_percentile BETWEEN 21 AND 30 THEN 3
                                    WHEN interested_in_hiking_percentile BETWEEN 31 AND 40 THEN 4
                                    WHEN interested_in_hiking_percentile BETWEEN 41 AND 50 THEN 5
                                    WHEN interested_in_hiking_percentile BETWEEN 51 AND 60 THEN 6
                                    WHEN interested_in_hiking_percentile BETWEEN 61 AND 70 THEN 7
                                    WHEN interested_in_hiking_percentile BETWEEN 71 AND 80 THEN 8
                                    WHEN interested_in_hiking_percentile BETWEEN 81 AND 90 THEN 9
                                    WHEN interested_in_hiking_percentile BETWEEN 91 AND 100 THEN 10
                                    ELSE NULL END)

     ,enjoy_playing_golf_percentile = (CASE WHEN enjoy_playing_golf_percentile BETWEEN 01  AND 10 THEN 1
                                    WHEN enjoy_playing_golf_percentile BETWEEN 11 AND 20 THEN 2
                                    WHEN enjoy_playing_golf_percentile BETWEEN 21 AND 30 THEN 3
                                    WHEN enjoy_playing_golf_percentile BETWEEN 31 AND 40 THEN 4
                                    WHEN enjoy_playing_golf_percentile BETWEEN 41 AND 50 THEN 5
                                    WHEN enjoy_playing_golf_percentile BETWEEN 51 AND 60 THEN 6
                                    WHEN enjoy_playing_golf_percentile BETWEEN 61 AND 70 THEN 7
                                    WHEN enjoy_playing_golf_percentile BETWEEN 71 AND 80 THEN 8
                                    WHEN enjoy_playing_golf_percentile BETWEEN 81 AND 90 THEN 9
                                    WHEN enjoy_playing_golf_percentile BETWEEN 91 AND 100 THEN 10
                                    ELSE NULL END)

     ,enjoy_playing_football_percentile = (CASE WHEN enjoy_playing_football_percentile BETWEEN 01  AND 10 THEN 1
                                    WHEN enjoy_playing_football_percentile BETWEEN 11 AND 20 THEN 2
                                    WHEN enjoy_playing_football_percentile BETWEEN 21 AND 30 THEN 3
                                    WHEN enjoy_playing_football_percentile BETWEEN 31 AND 40 THEN 4
                                    WHEN enjoy_playing_football_percentile BETWEEN 41 AND 50 THEN 5
                                    WHEN enjoy_playing_football_percentile BETWEEN 51 AND 60 THEN 6
                                    WHEN enjoy_playing_football_percentile BETWEEN 61 AND 70 THEN 7
                                    WHEN enjoy_playing_football_percentile BETWEEN 71 AND 80 THEN 8
                                    WHEN enjoy_playing_football_percentile BETWEEN 81 AND 90 THEN 9
                                    WHEN enjoy_playing_football_percentile BETWEEN 91 AND 100 THEN 10
                                    ELSE NULL END)


     ,read_sport_magazines_percentile = (CASE WHEN read_sport_magazines_percentile BETWEEN 01  AND 10 THEN 1
                                    WHEN read_sport_magazines_percentile BETWEEN 11 AND 20 THEN 2
                                    WHEN read_sport_magazines_percentile BETWEEN 21 AND 30 THEN 3
                                    WHEN read_sport_magazines_percentile BETWEEN 31 AND 40 THEN 4
                                    WHEN read_sport_magazines_percentile BETWEEN 41 AND 50 THEN 5
                                    WHEN read_sport_magazines_percentile BETWEEN 51 AND 60 THEN 6
                                    WHEN read_sport_magazines_percentile BETWEEN 61 AND 70 THEN 7
                                    WHEN read_sport_magazines_percentile BETWEEN 71 AND 80 THEN 8
                                    WHEN read_sport_magazines_percentile BETWEEN 81 AND 90 THEN 9
                                    WHEN read_sport_magazines_percentile BETWEEN 91 AND 100 THEN 10
                                    ELSE NULL END)


     ,interested_in_keeping_fit_percentile = (CASE WHEN interested_in_keeping_fit_percentile BETWEEN 01  AND 10 THEN 1
                                    WHEN interested_in_keeping_fit_percentile BETWEEN 11 AND 20 THEN 2
                                    WHEN interested_in_keeping_fit_percentile BETWEEN 21 AND 30 THEN 3
                                    WHEN interested_in_keeping_fit_percentile BETWEEN 31 AND 40 THEN 4
                                    WHEN interested_in_keeping_fit_percentile BETWEEN 41 AND 50 THEN 5
                                    WHEN interested_in_keeping_fit_percentile BETWEEN 51 AND 60 THEN 6
                                    WHEN interested_in_keeping_fit_percentile BETWEEN 61 AND 70 THEN 7
                                    WHEN interested_in_keeping_fit_percentile BETWEEN 71 AND 80 THEN 8
                                    WHEN interested_in_keeping_fit_percentile BETWEEN 81 AND 90 THEN 9
                                    WHEN interested_in_keeping_fit_percentile BETWEEN 91 AND 100 THEN 10
                                    ELSE NULL END)



SELECT TOP 10 * FROM #PERSON_PROPENSITIES_GRID_NEW
-- ALL LOOKS WELL



---------------------------------------------------------------------------

--- WE NOW HAVE THE BASE TABLE OF PERCENTILES - LETS NOW UPDATE THE 3 UNIVERSE:


-- now lets update the panel;
-- this will be done at HH level
update panel_sov as base
 set     base.interested_in_hiking_percentile = exp.interested_in_hiking_percentile
        ,base.enjoy_playing_golf_percentile = exp.enjoy_playing_golf_percentile
        ,base.enjoy_playing_football_percentile = exp.enjoy_playing_football_percentile
        ,base.read_sport_magazines_percentile = exp.read_sport_magazines_percentile
        ,base.interested_in_keeping_fit_percentile = exp.interested_in_keeping_fit_percentile
from #PERSON_PROPENSITIES_GRID_NEW as exp
where base.cb_key_household = exp.cb_key_household
--54,114 updated

select top 1000 * from panel_sov -- where there are nulls there was no match between the data sets




--- update the Sky customer base
update v126_active_customer_base as base
 set     base.interested_in_hiking_percentile = exp.interested_in_hiking_percentile
        ,base.enjoy_playing_golf_percentile = exp.enjoy_playing_golf_percentile
        ,base.enjoy_playing_football_percentile = exp.enjoy_playing_football_percentile
        ,base.read_sport_magazines_percentile = exp.read_sport_magazines_percentile
        ,base.interested_in_keeping_fit_percentile = exp.interested_in_keeping_fit_percentile
from #PERSON_PROPENSITIES_GRID_NEW as exp
where base.cb_key_household = exp.cb_key_household


select top 1000 * from v126_active_customer_base -- where there are nulls there was no match between the data sets




--- update the UK
update v126_uk_Base as base
 set     base.interested_in_hiking_percentile = exp.interested_in_hiking_percentile
        ,base.enjoy_playing_golf_percentile = exp.enjoy_playing_golf_percentile
        ,base.enjoy_playing_football_percentile = exp.enjoy_playing_football_percentile
        ,base.read_sport_magazines_percentile = exp.read_sport_magazines_percentile
        ,base.interested_in_keeping_fit_percentile = exp.interested_in_keeping_fit_percentile
from #PERSON_PROPENSITIES_GRID_NEW as exp
where base.cb_key_household = exp.cb_key_household


select top 1000 * from v126_uk_Base-- where there are nulls there was no match between the data sets






















-----
-- package details:
-----


-- THIS HAS NOT BEEN MRUN -- SAVING THIS FOR LATER:

-- lets update age etc

update panel_sov as base
set  gender = gender
    ,age = age
    ,household_composition = household_composition
    ,household_aff_raw = household_aff_raw
    ,household_affluence = household_affluence
from v126_uk_Base                                       -- this is everyone in the UK
where cb_key_individual




select top 10 * from
panel_sov

select top 10 * from
v126_active_customer_base

v126_uk_Base



















---- lets have a look at dons table; -- could duplicate problems be coming from here?

select top 1000 * from rombaoad.V98_MainTable_SOV_Final order by account_number, segment_id



select count(*) from rombaoad.V98_MainTable_SOV_Final
-- 39,232,982

select count(distinct(account_number)) from rombaoad.V98_MainTable_SOV_Final
--603,336










--------------------------------------------------------------------------------
-- Development:
--------------------------------------------------------------------------------



-------------------------------------
-------------------------------------


-- lets get the experian variables that we are interested in:

-- household level:
select top 10 S3_006651_data_INTR_SPOR_FBAL_ENJOY_FOOTBALL_DO
      ,S3_006656_data_INTR_SPOR_ENJY_HORSE_RACING
      ,S3_006666_data_INTR_SPOR_ENJY_RUGBY_DO
      ,S3_006668_data_INTR_SPOR_ENJY_BOATS_YACHTING_DO
      ,S3_006674_data_INTR_SPOR_ENJY_SQUASH_DO
      ,S3_006677_data_INTR_SPOR_ENJY_TENNIS_DO
      ,S3_006678_data_INTR_SPOR_ENJY_HIKING_WALKING
      ,S3_006679_data_INTR_SPOR_ENJY_WATER_SPORTS_DO
      ,S3_006680_data_INTR_SPOR_ENJY_WINDSURFING
      ,S3_010893_data_INTR_HOBB_ENJY_GOING_TO_THE_GYM
      ,S3_012171_data_INTR_SPOR_ENJY_FITNESS_HEALTH_DO
      ,interests_keeping_fit
 --     ,s3_004386_data_HEAL_PERS_ALTE_MAIL_ORDER_HEALTH_SUPPLEMENTS_HAVE
from sk_prod.PLAYPEN_EXPERIAN_LIFESTYLE



-- PERSON LEVEL

--sk_prod.PERSON_PROPENSITIES_GRID_CUR

select top 10
sports_cricket_perc_p
,sports_cycling_perc_p
,sports_fishing_perc_p
,sports_football_perc_p
,sports_golf_perc_p
,sports_skiing_perc_p
,sports_tennis_perc_p
,sports_watersports_perc_p
from sk_prod.PERSON_PROPENSITIES_GRID_CUR





--sk_prod.PERSON_PROPENSITIES_GRID_NEW

select top 10
--interested_in_keeping_fit_percentile,
interested_in_hiking_percentile
,enjoy_playing_golf_percentile
,enjoy_playing_football_percentile
,interested_in_fishing_percentile
,read_sport_magazines_percentile
 from sk_prod.PERSON_PROPENSITIES_GRID_NEW



-- sp_columns 'PERSON_PROPENSITIES_GRID_CUR'







/*--------------------------------------------------------------------------------
-- SECTION 2: PART A -
--------------------------------------------------------------------------------

             A01 - IDENTIFY PRIMARY BOXES RETURNING DATA
             A02 - GET THE VIEWING DATA

--------------------------------------------------------------------------------*/


--------------------------------------------------------------------------------
--  A01 - identify boxes returning data over the period
--------------------------------------------------------------------------------


--------------------------------------------------------------------------------
-- A02 - Get the viewing data
--------------------------------------------------------------------------------


------------------------------------------------------------------
-- Step 1: identify the programs that we are interested in
------------------------------------------------------------------




--------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------


/*
--------------------------------------------------------------------------------
-- PART B - Add Additional Feilds to the Viewing data for roll ups
--------------------------------------------------------------------------------


         B01 - Add Package - based on the start of the period of analysis:
         B02 - Add HD and Sky+

-------------------------------------------------------------------------------
*/




--------------------------------------------------------------------------------
-- B01 - Add current pack
--------------------------------------------------------------------------------
