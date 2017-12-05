

/*------------------------------------------------------------------------------
        Project: Barclays Cash ISA Campaign
        Version: v1.
        Created: 23/10/2012
        Lead: Sarah Moore
        Analyst: Harry Gill
        SK Prod: 4

        Milestone: 4 **


        Introduction

        This code is part of a series of templates designed to reach milestones detailed below in preperation for the Barclays Cash ISA campaign.


        Milestone

        1.      Prepare Code to Link Spots and Programmes Data                  (Dan B)

                Check matching criteria for Spots and programs  4 (DB)
                Code linking programme and spots data
                Code to identify spot placement in break
                Test Run / QA

        2.      Prepare code to link client data and Sky base data              (Hannah)

                Create Dummy Data in accordance with file template
                Sanity Check matching criteria (cb keys)
                Code linking Client table and program data
                Test Run / QA

        3.      Prepare code to identify universes                              (Harry/Hannah)

                Code Flagging client / non client data in Sky and VESPA
                Define and code  flag for viewed spot
                Test Run / QA

        4.      Prepare code for Experian / client profiling                    (Harry)

                Identify Experian Variables to use
                Flag Experian variables
                Flag Client segments
                Prepare code for VESPA viewing Profile
                Test Run / QA

        5.      Prepare code for TV Profile                                     (Harry)

                Top programmes (eff and reach)
                Distribution of Impacts by components
                Test Run / QA

        6.      Prepare code for closed loop measurement                        (Harry)

                Check Distribution across Sky and VESPA
                Code to Flag responders and non responders
                Code Output metrics
                Test Run / QA

        7.      Design Templates                                                (Susanne)

                Match Rate Output – pre Diagnostic
                Standard Excel output for Diagnostic

        8.      Improve Efficiencies                                            (Susanne or Jon Green)

                Match Rate Output – pre Diagnostic
                Standard Excel output for Diagnostic
                Test Run / QA

        9.      Presentation Output                                             (Susanne?)

                Design Template




        CODE STRUCTURE AND SECTIONS
        --------

        Set-Up   -

        PART A   -
             A01 - IDENTIFY PRIMARY BOXES RETURNING DATA
             A02 - GET THE VIEWING DATA


        PART B   -
             B01 - ADD PACK TO THE VIEWING DATA
             B02 - ADD HS AND SKY+ FLAG

        PART C   - SCALING
             C01 - CREATE A BASE TABLE
             C02 - CALCULATE THE NORMALISED WEIGHT

        PART D - AVERAGE VIEWING PER DAY (MINUTES)
             D01 - SUMMERISE VIEWING FOR EACH CUSTOMER
             D02 - CALCULATE AVERAGE MINBUTES FOR EACH PACKAGE - BASED ON THE UK BASE
             D03 - CALCULATE EACH CUSTOMERS DEVIATION FROM THE PACKAGE  MEAN
             D04 - ALLOCATE EACH ACCOUNT TO THE RELEVANT DECILE

        PART E - SHARE OF VIEWING
             E01 - SUMMERISE SHARE OF VIEWING FOR EACH CUSTOMER
             E02 - CALCULATE SHARE OF VIEWING AVERAGE FOR EACH PACKAGE - BASED ON THE UK BASE
             E03 - CALCULATE EACH CUSTOMERS DEVIATION FROM THE PACKAGE  MEAN
             E04 - ALLOCATE EACH ACCOUNT TO THE RELEVANT DECILE


        Ouput Tables:   DO_NOT_DELETE_NBA_SCORING_2012XXXX -- THIS TABLE CONTAINS ALL SCORING for each customer
        -------

--------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------*/


-- this is the code and outputs for template 2:
-- avoiding the master promo-roll up route!

-----
-- output 10: population sizes - purchased households and segment break downs
-----

-- first we need to bring in scaling weightings into the vespa demographics tables

alter table v081_Vespa_Universe_demographics
        add weighting float;

update v081_Vespa_Universe_demographics
        set weighting = weightings
from v081_Vespa_Universe_demographics uni
join table_for_scaling scale
on uni.account_number = scale.account_number


-- select sum(weighting) from gillh.v081_Vespa_Universe_demographics v

-- the output will be produced in two parts

--part a.
select
        sum(case when social_grade in ('A','B','C1') then weighting else null end) as brought_households

--         ,sum(case when bar.barclays_customer_before_campaign = 1 and  then ves.cb_key_household else null end)) as barclays_customer_before_campaign_ves

        ,count (distinct(case when bar.cb_key_household = ves.cb_key_household
        and bar.barclays_ISA_before_campaign = 1 then ves.cb_key_household else null end)) as barclays_ISA_before_campaign_ves

        ,count (distinct(case when bar.cb_key_household = ves.cb_key_household
        and bar.barclays_cash_isa = 1 then ves.cb_key_household else null end)) as barclays_cash_isa_ves

        ,count (distinct(case when bar.cb_key_household = ves.cb_key_household
        and bar.social_grade in ('A','B','C1') then ves.cb_key_household else null end)) as _Brought_audience_ABC1_ves



        ,sum(case when UPPER(segment) in ('CLIVE','CONNIE','CHARLIE','CLARKE')then weighting else null end) as target_segment_total
                -- CHECK THIS DEFINITION - MAY BE OTHERS INSIDE
into #populationa
from v081_Vespa_Universe_demographics

-- part b.
select segment
        , sum(weighting) as households
into #populationb
from v081_Vespa_Universe_demographics
where segment is not null and segment <> 'Unknown'
group  by segment
order by segment



--------------------------------------------------------
        select * from #populationa

        select * from #populationb order by segment
--------------------------------------------------------




-----
-- output 11: camapign reach penetration
-----

-- first we need to
alter table v081_Vespa_Universe_demographics
        add barclays_spot integer default 0


select account_number
        ,max(whole_spot) as whole_spot
into #spot_watched
from Barclays_spots_viewing_table_dump
group by account_number

select top 10 * from #spot_watched


update v081_Vespa_Universe_demographics dem
        set barclays_spot = whole_spot -- case when vw.account_number = dem.account_number then 1 else 0 end
from v081_Vespa_Universe_demographics dem
inner join #spot_watched vw
on vw.account_number = dem.account_number
-- where whole_spot = 1

--- NOTE -- othere will be a number of customers in our universe that will not appear in the spots table.
--- as by definition to be included in the spots vieiwng table you must have wtached at least part of a spot
-- barclays spot 0 then includes customers who only watched part of a spot and no spots at all.

select top 10 * from v081_Vespa_Universe_demographics


--QA -- LETS HAVE A LOOK
select barclays_spot, count(*) from v081_Vespa_Universe_demographics group by barclays_spot



--desired camapign customer AND TARGET SEGMENT


SELECT sum(case when desired_campaign_customer = 1 then weighting else null end) as desired_campaign_HH
      ,sum(case when segment is not null then weighting else null end) as Target_segment -- CHECK IF THIS IS ok?? --- DEFINE EACH SEGMENT???
--                     when  segment = 'Clive'
--                    or     segment = 'Connie'
--                    or     segment = 'Charlie'
--                    or     segment = 'Clarke'

into #output11_penetration
from v081_Vespa_Universe_demographics
where barclays_spot <>0


-------------- OUTPUT --------------------
------------------------------------------
        select * from #output11_penetration
------------------------------------------



-- we are no longer doing this:

-----
-- output 11.2  : brought_audience vs responders vs eligible -----




select (case when social_grade in ('A','B','C1') then 'brought_audience' else 'Non Brought Audience' end) as flag
       ,(case when barclays_responder = 1 then 'Responders' else 'Non Responders' end) as responders
       ,(case when unused_cash_isa_balance = 1 then 'Eligible' else 'Not Eligible' end) as Eligible_for_isa
       ,sum(weighting) as households
into #output_11_brought_eligible
from v081_Vespa_Universe_demographics
group by flag
        ,responders
        ,Eligible_for_isa


-------------- OUTPUT --------------------
------------------------------------------
        select * from #output_11_brought_eligible
------------------------------------------








-----
-- output 12: promo impacts by responders and non-responders
-----

-- lets add this to the viewing table as it will be needed later on:
alter table Barclays_spots_viewing_table_dump
        add responder integer default 0
-- there may be some non matches which we then assume to have not responded


update Barclays_spots_viewing_table_dump --
        set responder = 1
from Barclays_spots_viewing_table_dump as ves
join OM114_BARCLAYS_RESPONSE as bar
on bar.cb_key_household = ves.cb_key_household

select top 10 * from Barclays_spots_viewing_table_dump

--- QA ----
select responder
        ,count(distinct(cb_key_household))
        ,sum(weightings)
from Barclays_spots_viewing_table_dump
group by responder
-- there are slightly more accounts in the viewing dump than there are in the vespa demographics table.



-- Master promo Roll up -- this will serve the outputs in the remainder of this section.

--------------
-- 1: First lest add the fields that we will need to the barclays spots vieiwng table;
--------------

-- we need to add the sales house to the barclays spots table as there is nothing to match to in the vieiwng table
alter table barclays_spots
        add sales_house varchar(25);

update barclays_spots
        set spot.sales_house = chg.primary_sales_house
from barclays_spots spot
inner join neighbom.channel_map_dev_barb_channel_group chg
on spot.log_station_code = chg.log_station_code


-- now we need to add the sales house to the viewing table -- log_station_code
alter table Barclays_spots_viewing_table_dump
        add sales_house varchar(10)

update Barclays_spots_viewing_table_dump
        set bar.sales_house = spot.sales_house
from Barclays_spots_viewing_table_dump bar
 join barclays_spots spot
        on spot_identifier = SPOT.identifier



select top 10 * from Barclays_spots_viewing_table_dump

--------------
-- 2: Get a Roll up of the vieiwng data
--------------

-- lets add the weighting to the barclays spots table - not most efficient way of doing this
-- but added post production given a problem with the sum of weightings in the
alter table Barclays_spots_viewing_table_dump
add weighting float

update Barclays_spots_viewing_table_dump
 set bar.weighting = sca.weightings
from Barclays_spots_viewing_table_dump bar
join table_for_scaling sca
on bar.account_number = sca.account_number



-- this table will be program level granular - can be rolled up later
if object_id('barclays_spots_cube') is not null drop table barclays_spots_cube

select distinct(spot.account_number)
        ,max(cb_key_individual) as cb_key_individual
        ,max(cb_key_household) as cb_key_household
        ,max(responder) as responder
        ,max(weighting) as weightings
        ,spot_position
        ,X_Viewing_Time_Of_Day
        ,agg_channel_name as channel
        ,service_key
        ,sales_house
        ,Genre_Description
        ,Sub_Genre_Description
        ,COUNT(case when whole_spot = 1 then 1 else null end) as impacts
        ,scaled_impacts = impacts*weightings
    --    ,max(last_spot)
into barclays_spots_cube
from Barclays_spots_viewing_table_dump as spot
-- join #last_spot as las
-- on las.account_number = spot.account_number
group by spot.account_number
        ,spot_position
        ,X_Viewing_Time_Of_Day
        ,agg_channel_name
        ,service_key
        ,sales_house
        ,Genre_Description
        ,Sub_Genre_Description

select top 1000 * from barclays_spots_cube


-- lets identify at account level when the last barclays spot was seen by each account_number

--drop table #last_spot

select distinct(account_number)
        ,max(case when whole_spot = 1 then viewing_date else null end) as last_spot
into #last_spot
from Barclays_spots_viewing_table_dump
group by account_number

alter table barclays_spots_cube
add last_spot date

update barclays_spots_cube
        set cub.last_spot = tmp.last_spot
from barclays_spots_cube as cub
join #last_spot as tmp
on tmp.account_number = cub.account_number



-- drop table #impacts
-- the impacts from the cube a from a combination of the flags-- lets aggregate
select distinct(cb_key_household)
        ,sum(impacts) as impacts
        ,max(responder) as responder
        ,max(weightings) as weightings
into #impacts
from barclays_spots_cube
group by cb_key_household


select top 10 * from #impacts



-- NOW LETS GET THE OUTPUT!
--   drop table #output12_impact_distribution

select impacts
       ,sum(case when responder = 1 then weightings else null end) as responder_HH             -- SHOULD BE WEIGHTINGS
       ,sum(case when responder <>1 then weightings else null end) as non_responder_HH
into #output12_impact_distribution
from #impacts
where weightings is not null -- i.e. only those accounts that are part of the universe we are studying!
group by impacts
order by impacts

-- where an account has zero spots viewed this means that they have only watched part spots as by definition they would not
-- be in the spots table had they not seen a spot.


-- lets do a little adjustment since not all the accounts that didnt watch a spot are not in the viewing table:

-- we want to know how many accounts watched a spaot less those who didnt watch a spot
-- Sky accounts, 9.5million - 8.8million watched a spot = 700k that didnt watch a spot -- thats the number we need to get
-- where spots = 0 and non_responder



-- how many people dont fit into (watched 0 spots, didnt respond)
create variable @impacted_households integer;

set @impacted_households = (select (sum(responder_HH) + sum(non_responder_HH) - sum(case when impacts = 0 then non_responder_hh else null end)) from #output12_impact_distribution)

-- now we want to know how many people are on the sky base
create variable @sky_base_hh integer;

set @sky_base_HH = (select sum(weightings) from table_for_scaling)





-- so the number of people who didnt respond and didnt see a spot are sky_base - those who watched or responded

update #output12_impact_distribution
        set non_responder_hh = ( case when impacts = 0 then (@sky_base_hh - @impacted_households) else non_responder_hh end)


-------------- OUTPUT --------------------
------------------------------------------
        select * from #output12_impact_distribution order by impacts
------------------------------------------





-----
-- output 13: identify recency and distributions of responders
-----


-- first bring in some extra fields to the response data - these will be usefull later and save multiple joins and processing!
alter table v081_barclays_Universe_demographics
        add (target integer default 0);


---******************************************* THIS NEEDS TO BE UPDATED! ************************************************ --
update v081_barclays_Universe_demographics
       set target = (case when barclays_cash_isa >0 then 1 else target end )

         -- (this definition is in place as a tester - insert correct definition )
----------------------------------------------------------------------------------------------------------------------------

select top 100 * from v081_barclays_Universe_demographics

select top 100 * from OM114_BARCLAYS_RESPONSE


-- now lets pull household level data into a new table


select top 10 * from v081_barclays_Universe_demographics

select top 10 * from v081_vespa_Universe_demographics

select sum(weighting) from v081_vespa_Universe_demographics where barclays_responder = 1 -- 173,943

select count(*) from v081_barclays_Universe_demographics where responder = 1

select count(*) from v081_barclays_Universe_demographics where responder = 1 and Sky_customer = 1 -- 149,591

select cb_key_household
        ,target
        ,sales
        ,min_application_date date
        ,max_application_date date
        ,sales integer);
        ,recency






select distinct(account_number)
        ,max(case when whole_spot = 1 then viewing_date else null end) as last_spot
into #last_spot
from Barclays_spots_viewing_table_dump
group by account_number
-- now lets get the data that we need into the customer level impacts table:
alter table barclays_spots_cube
        add( recency integer
            ,target integer default 0
            ,max_application_date date
            ,min_application_date date
            ,sales integer);




select top 10 * from barclays_spots_cube

update barclays_spots_cube
        set recency = datediff(day, last_spot, res.application_date) --days
           ,imp.target = res.target
           ,imp.desired_customer = res.desired_customer
           ,imp.application_date = res.application_date -- may need datepart here?!
 from barclays_spots_cube as imp
 join barclays_response as res
 on res.cb_key_household = imp.cb_key_household



-- now lets get the output!

-- drop table #output13_recency_distribution
select recency
       ,sum(case when responder = 1 then cast(weightings as integer) else null end) as responder_HH
       ,sum(case when responder <> 1 and target = 1 then cast(weightings as integer) else null end) as non_responder_target
       ,sum(case when responder <> 1 and target <>1 then cast(weightings as integer) else null end) as non_responder_non_target
       ,sum(case when responder <> 1 then cast(weightings as integer) else null end) as non_responders_all
into #output13_recency_distribution
from barclays_spots_cube
group by recency
order by recency
-- this output will give us any null recencies: non-purchases, are these wanted??


-------------- OUTPUT --------------------
------------------------------------------
        select * from #output13_recency_distribution order by recency
------------------------------------------





-------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------
                 --                       CODE FOR TEMPLATE 3 ROLL UPS                          --
-------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------

-- TEMPLATE 3: OUTPUT: RESPONDERS BY SALES HOUSE.

-- drop table #responders_by_sales_house

Select sales_house
       ,sum(case when responder = 1 then cast(weightings as integer) else null end) as responders_HH
       ,sum(case when responder <> 1 and spot.impacts > 2 then cast(weightings as integer) else null end) as Households_gt3_imapcts_NON_Responders

       ,sum(case when responder <> 1 then cast(weightings as integer) else null end) as non_responders_HH
       ,sum(case when responder <> 1 and spot.impacts > 2 then cast(weightings as integer) else null end) as Households_gt3_imapcts_NON_Responders

into #responders_by_sales_house
from barclays_spots_cube spot
group by sales_house
order by responders_HH desc


-------------- OUTPUT -------------------------------------------------------------
-----------------------------------------------------------------------------------
                select * from #responders_by_sales_house order by responders_HH desc
-----------------------------------------------------------------------------------




-- TEMPLATE 3: OUTPUT: RESPONDERS BY CHANNEL

-- drop table #responders_by_channel
Select channel
       ,sum(case when responder = 1 then cast(weightings as integer) else null end) as responders_HH
       ,sum(case when responder <> 1 then cast(weightings as integer) else null end) as non_responders_HH
       ,sum(case when responder = 1 and spot.impacts > 2 then cast(weightings as integer) else null end) as Households_gt3_imapcts_Responders
       ,sum(case when responder <> 1 and spot.impacts > 2 then cast(weightings as integer) else null end) as Households_gt3_imapcts_NON_Responders
into #responders_by_channel
from barclays_spots_cube spot
group by channel
order by  responders_HH desc -- suggest an ordering by the number of responders? - thats what is important
-- 145 different channels


-------------- OUTPUT -------------------------------------------------------------
-----------------------------------------------------------------------------------
                select * from #responders_by_channel order by responders_HH desc -- is this the same as >=3 impact households???
-----------------------------------------------------------------------------------




---------------------------------------------------------------------------------------------------------------
-- TEMPLATE 3: OUTPUT: RESPONDERS BY MEDIA PACK
---------------------------------------------------------------------------------------------------------------

select ska.service_key as service_key, ska.full_name, PACK.NAME,cgroup.primary_sales_house,
                (case when pack.name is null then cgroup.channel_group
                else pack.name end) as channel_category
into #packs
from vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES ska
left join
        (select a.service_key, b.name
         from vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_LANDMARK a
                join neighbom.CHANNEL_MAP_DEV_LANDMARK_CHANNEL_PACK_LOOKUP b
                        on a.sare_no between b.sare_no and b.sare_no + 999
        where a.service_key <> 0
         ) pack
        on ska.service_key = pack.service_key
left join
        (select distinct a.service_key, b.primary_sales_house, b.channel_group
         from vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_BARB a
                join neighbom.CHANNEL_MAP_DEV_BARB_CHANNEL_GROUP b
                        on a.log_station_code = b.log_station_code
                        and a.sti_code = b.sti_code
        where service_key <>0) cgroup
        on ska.service_key = cgroup.service_key
where cgroup.primary_sales_house is not null
order by cgroup.primary_sales_house, channel_category
;--438 Row(s) affected



-----------------------------Correct channel category anomolies -- media pack

if object_id('LkUpPack') is not null drop table LkUpPack

SELECT  primary_sales_house
        ,service_key
        ,full_name
        ,(case
                when service_key = 3777 OR service_key = 6756 then 'LIFESTYLE & CULTURE'
                when service_key = 4040 then 'SPORTS'
                when service_key = 1845 OR service_key = 4069 OR service_key = 1859 then 'KIDS'
                when service_key = 4006 then 'MUSIC'
                when service_key = 3621 OR service_key = 4080 then 'ENTERTAINMENT'
                when service_key = 3760 then 'DOCUMENTARIES'
                when service_key = 1757 then 'MISCELLANEOUS'
                when service_key = 3639 OR service_key = 4057 then 'Media Partners'
                                                                                ELSE channel_category END) AS channel_category
INTO LkUpPack
FROM #packs
order by primary_sales_house, channel_category
;

----------------------------------------------------------------------------------------------------------------------------

-- now lets put the media pack into the cube.
alter table barclays_spots_cube
        add media_pack varchar(25);


update barclays_spots_cube
        set cub.media_pack = tmp.channel_category
from barclays_spots_cube as cub
join LkUpPack as tmp
on tmp.service_key = cub.service_key



-- lets get the output:


-- drop table #responders_by_media_pack

Select media_pack
       ,sum(case when responder = 1 then cast(weightings as integer) else null end) as responders_HH
       ,sum(case when responder <> 1 then cast(weightings as integer) else null end) as non_responders_HH
       ,sum(case when responder = 1 and spot.impacts > 2 then cast(weightings as integer) else null end) as Households_gt3_imapcts_Responders
       ,sum(case when responder <> 1 and spot.impacts > 2 then cast(weightings as integer) else null end) as Households_gt3_imapcts_NON_Responders
into #responders_by_media_pack
from barclays_spots_cube spot
group by media_pack
order by responders_HH desc

/*
select distinct(channel_name) from barclays_spots_cube where media_pack is null -- service_key = 6130
--*channel_name*
-- Showcase -- this channel has spots for sale but is not assigned to a media pack.
*/


-------------- OUTPUT -------------------------------------------------------------
-----------------------------------------------------------------------------------
                select * from #responders_by_media_pack order by responders_HH desc -- is this the same as >=3 impact households???
-----------------------------------------------------------------------------------





-- for the media packs stuff
select top 10 * from vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES
select top 10 * from vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_LANDMARK
select top 10 * from vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_BARB
select top 10 * from neighbom.CHANNEL_MAP_DEV_BARB_CHANNEL_GROUP
select top 10 * from neighbom.CHANNEL_MAP_DEV_LANDMARK_CHANNEL_PACK_LOOKUP -- name(channel_pack)

-- my tables
select top 10 * from v081_Vespa_Universe_demographics
select top 10 * from Barclays_spots_viewing_table_dump
select top 10 * from table_for_scaling
select top 10 * from barclays_spots_cube
select top 10 * from all_barlcays_spots






































