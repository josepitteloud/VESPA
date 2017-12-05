



-- lets find out how many spots were aired for each day
select distinct(local_date_of_transmission) as dates, count(distinct(identifier)) as spots_aired
  from barclays_spots
group by dates
order by dates


-- Lets find out how many spots were watched each day;

ALTER TABLE Barclays_spots_viewing_table_dump
ADD SPOT_AIR_DATE AS DATE

UPDATE Barclays_spots_viewing_table_dump
        SET SPOT_AIR_DATE = local_date_of_transmission
FROM Barclays_spots_viewing_table_dump
JOIN barclays_spots
ON identifier = SPOT_IDENTIFIER




-- how many of the aired spots were watched??

select (case when recorded_time_utc is null then viewing_date else cast(recorded_time_utc as date) end) as viewing_date
       ,count(distinct(case when whole_spot = 1 AND SPOT_AIR_DATE = viewing_date
        or SPOT_AIR_DATE = CAST(recorded_time_utc AS DATE) then spot_identifier else null end)) as SPOTS_VIEWED

         ,count(distinct(case when whole_spot = 1 AND SPOT_AIR_DATE = viewing_date
        or SPOT_AIR_DATE = CAST(recorded_time_utc AS DATE) AND social_grade2 in ('A','B','C1') then spot_identifier else null end)) as SPOTS_VIEWED_BROUGHT_AUDIENCE

        ,count(distinct(case when whole_spot = 1 AND SPOT_AIR_DATE = viewing_date
        or SPOT_AIR_DATE = CAST(recorded_time_utc AS DATE) AND RESPONSE_TARGET = 1 then spot_identifier else null end)) as SPOTS_VIEWED_responder_target

        ,count(distinct(case when whole_spot = 1 AND SPOT_AIR_DATE = viewing_date
        or SPOT_AIR_DATE = CAST(recorded_time_utc AS DATE) AND RESPONSE_TARGET_and_unknown = 1
        then spot_identifier else null end)) as SPOTS_VIEWED_responder_target_and_unknown

        ,count(distinct(case when whole_spot = 1 AND SPOT_AIR_DATE = viewing_date
        or SPOT_AIR_DATE = CAST(recorded_time_utc AS DATE) AND Aspiration_target = 1
        then spot_identifier else null end)) as SPOTS_VIEWED_Aspiration_target

        ,count(distinct(case when whole_spot = 1 AND SPOT_AIR_DATE = viewing_date
        or SPOT_AIR_DATE = CAST(recorded_time_utc AS DATE) then spot_identifier else null end)) as Sky_base

        ,sum(whole_spot * VES.weighting) as spots_impcats_Sky


from Barclays_spots_viewing_table_dump BAR
join v081_Vespa_Universe_demographics ves
on ves.cb_key_household = bar.cb_key_household
group by viewing_date
order by viewing_date




-- what were the audience figures??
select  viewing_date
        ,sum(whole_spot * VES.weighting) as spots_impcats_Sky

        ,sum(case when social_grade2 in ('A','B','C1') then whole_spot * VES.weighting ELSE NULL END) as spots_impcats_brought_aUDIENCE

         ,sum(case when RESPONSE_TARGET = 1  then whole_spot * VES.weighting ELSE NULL END) as spots_impcats_RESPONDER_TARGET_AUDIENCE

          ,sum(case when  RESPONSE_TARGET_and_unknown = 1 then whole_spot * VES.weighting ELSE NULL END) as spots_impcats_RESPONDER_TARGET_AND_UNKNOWN_AUDIENCE

           ,sum(case when Aspiration_target = 1 then whole_spot * VES.weighting ELSE NULL END) as spots_impcats_ASPIRATION_AUDIENCE

from Barclays_spots_viewing_table_dump BAR
join v081_Vespa_Universe_demographics ves
on ves.cb_key_household = bar.cb_key_household
group by viewing_date
order by viewing_date


------------------- lets get some more details on the spots that are zero rated;

--
-- drop table #spots_viewed_days
-- 
-- 
-- 
-- select spot_identifier
--         ,count(*) as count
--         ,count(distinct(case when recorded_time_utc is null then viewing_date else cast(recorded_time_utc as date) end)) as viewing_dates
-- into #spots_viewed_days
-- from Barclays_spots_viewing_table_dump BAR
-- where cb_key_household in (select cb_key_household from v081_vespa_Universe_demographics) and whole_spot = 1
-- group by spot_identifier
-- 
-- 
-- delete from #spots_viewed_days where viewing_dates < 1
-- 
-- 
-- 
-- 
-- drop table zero_rated_spots
-- 
-- 
-- 
-- select *
-- into zero_rated_spots
-- from barclays_spots
-- where identifier not in (select spot_identifier from #spots_viewed_days2 )
-- -- 2006
-- 
-- 
-- select count(*) from zero_rated_spots -- why 2006 -- this is right
-- 
-- 
-- select count(*) from barclays_spots
-- 
-- 
-- drop table #spots_viewed_days2
-- 
-- select spot_identifier
--         ,count(*) as count
-- into #spots_viewed_days2
-- from Barclays_spots_viewing_table_dump BAR
-- where account_number in (select account_number from v081_vespa_Universe_demographics where barclays_customer = 1) and whole_spot = 1
-- group by spot_identifier
-- 
-- select count(*) from #spots_viewed_days2 where count < 1
-- 
-- 
-- 
-- -- can this be ,the answer
-- 
-- (case when recorded_time_utc is null then viewing_date else cast(recorded_time_utc as date) end) as viewing_date
--        ,count(distinct(case when whole_spot = 1 AND SPOT_AIR_DATE = viewing_date
--         or SPOT_AIR_DATE = CAST(recorded_time_utc AS DATE) then spot_identifier else null end)) as SPOTS_VIEWED
-- 
--
--- look into barclays customer s

------------------------------------------------------------------------------

------------------------------------------------------------------------------

-- investigate the caci match



------
-- STEP 3: get customers Social grade from CACI tables
------

--drop table #caci_sc1

select  c.cb_row_id
        ,c.cb_key_household
        ,c.lukcat_fr_de_nrs AS social_grade
        ,playpen.p_head_of_household
        ,rank() over(PARTITION BY c.cb_key_household ORDER BY playpen.p_head_of_household desc, c.lukcat_fr_de_nrs asc, c.cb_row_id desc) as rank_id
into #caci_sc1
from sk_prod.CACI_SOCIAL_CLASS as c,
     sk_prod.PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD as playpen,
--     sk_prod.experian_consumerview e
--where e.exp_cb_key_individual = playpen.exp_cb_key_individual
 -- e.cb_key_individual = c.cb_key_individual
 -- and c.cb_address_dps is NOT NULL
 where c.cb_key_household = playpen.exp_cb_key_household
  and c.cb_key_household in (select cb_key_household from v081_barclays_Universe_demographics)
order by c.cb_key_household;
--
-- --de-dupe!
-- delete from #caci_sc1 where rank_id > 1  -- more than half!
-- 
-- 
-- select count(*) from #caci_sc1 where social_grade <> 'Unknown'
-- 
-- select distinct(social_grade), count(*) from #caci_sc1 group by social_grade
-- 
-- 
-- select distinct(fss_v3_group), count(*), sum(weighting) from test_vespa group by fss_v3_group
--


select top 10 * from #caci_sc1


delete from #caci_sc1 where social_grade is null

drop table #max_social

select cb_key_household
        ,min(social_grade) as social_grade
into #max_social
from #caci_sc1
--where social_grade is not null
group by cb_key_household

select distinct social_grade, count(*) from #max_social group by social_grade



alter table v081_barclays_Universe_demographics
        add social_grade2 varchar(16) default 'Unknown Default'

update v081_barclays_Universe_demographics
        set social_grade2 = c.social_grade
from v081_barclays_Universe_demographics t
join #max_social c
on c.cb_key_household = t.cb_key_household



select social_grade
        ,count(*)
from v081_barclays_Universe_demographics
group by social_grade
order by social_grade


select social_grade2
        ,count(*)
from v081_barclays_Universe_demographics
group by social_grade2
order by social_grade2



-- lets get the roll ups;

select fss_v3_group
        ,social_grade2
        ,count(*)
from v081_barclays_Universe_demographics
group by fss_v3_group
        ,social_grade2
order by fss_v3_group
        ,social_grade2






select fss_v3_group
        ,social_grade2
        ,sum(weighting)
from v081_vespa_Universe_demographics
group by fss_v3_group
        ,social_grade2
order by fss_v3_group
        ,social_grade2





select sum(weighting) from v081_vespa_Universe_demographics where barclays_customer = 1 AND  IMPACTS_TOTAL >  0


select sum(weighting) from v081_vespa_Universe_demographics where social_grade2 in ('A','B','C1') AND barclays_customer = 1 AND BARCLAYS_RESPONDER = 1 and  IMPACTS_TOTAL >  0




SELECT SUM(CASE WHEN social_grade2 NOT in ('A','B','C1') AND barclays_responder = 1 and barclays_customer = 1 then weighting else null end) as non_b_resp_barc
      ,SUM(CASE WHEN social_grade2 NOT in ('A','B','C1') AND barclays_responder = 1 and barclays_customer = 0 then weighting else null end) as non_b_resp_non_barc
      ,SUM(CASE WHEN social_grade2 NOT in ('A','B','C1') AND barclays_responder = 0 and barclays_customer = 1 then weighting else null end) as non_b_nonresp_barc
      ,SUM(CASE WHEN social_grade2 NOT in ('A','B','C1') AND barclays_responder = 0 and barclays_customer = 0 then weighting else null end) as non_b_nonresp_nonbarc

      ,SUM(CASE WHEN social_grade2  in ('A','B','C1') AND barclays_responder = 1 and barclays_customer = 1 then weighting else null end) as b_resp_barc
      ,SUM(CASE WHEN social_grade2  in ('A','B','C1') AND barclays_responder = 1 and barclays_customer = 0 then weighting else null end) as b_resp_nonbarc
      ,SUM(CASE WHEN social_grade2  in ('A','B','C1') AND barclays_responder = 0 and barclays_customer = 1 then weighting else null end) as b_nonresp_barc
      ,SUM(CASE WHEN social_grade2  in ('A','B','C1') AND barclays_responder = 0 and barclays_customer = 0 then weighting else null end) as b_nonresp_nonbarc
from v081_vespa_Universe_demographics


 response_target = 1 then weighting else null end) as response_target

  ,SUM(CASE WHEN response_target = 1 and barclays_responder = 1 then weighting else null end) as response_target_responder
from v081_Vespa_Universe_demographics


----------------------------------------------------------------------------
------------------------------------------------------------------------------

-- this is the top of susannes campaign evaluation sheet ------------------------------------------------------

-- Lets get the volumes again:


SELECT   SUM(CASE WHEN response_target = 1 then weighting else null end) as respdr_target
        ,SUM(CASE WHEN response_target = 1 and barclays_responder = 1 then weighting else null end) as rspndr_target_responders
        ,SUM(CASE WHEN response_target = 1 and impacts_total >0 then weighting else null end) as rspdr_target_reach

        ,SUM(CASE WHEN response_target_and_unknown = 1 then weighting else null end) as resp_target_and_unknown
        ,SUM(CASE WHEN response_target_and_unknown = 1 and barclays_responder = 1 then weighting else null end) as respse_target_and_unknown_responders
        ,SUM(CASE WHEN response_target_and_unknown = 1 and impacts_total >0 then weighting else null end) as res_target_and_unknown_reach

        ,SUM(CASE WHEN aspiration_target = 1 then weighting else null end) as aspion_target
        ,SUM(CASE WHEN aspiration_target = 1 and barclays_responder = 1 then weighting else null end) as asptn_target_responders
        ,SUM(CASE WHEN aspiration_target = 1 and impacts_total > 0 then weighting else null end) as aspn_target_reach

        ,SUM(CASE WHEN social_grade2 in ('A','B','C1') then weighting else null end) as brought
        ,SUM(CASE WHEN social_grade2 in ('A','B','C1') and barclays_responder = 1 then weighting else null end) as broht_reponders
        ,SUM(CASE WHEN social_grade2 in ('A','B','C1') and impacts_total >0  then weighting else null end) as brght_reach

        ,SUM(weighting) as Barc_Sky
        ,SUM(CASE WHEN  barclays_responder = 1 then weighting else null end) as Barc_Sky_reponders
        ,SUM(CASE WHEN  impacts_total > 0 then weighting else null end) as Barc_Sky_reach
FROM v081_Vespa_Universe_demographics
where barclays_customer = 1




select distinct(impacts_total) from v081_vespa_Universe_demographics order by impacts_total

SELECT SUM(CASE WHEN social_grade2 NOT in ('A','B','C1') and impacts_total >0  AND barclays_responder = 1  then weighting else null end) as a
       ,SUM(CASE WHEN social_grade2 NOT in ('A','B','C1') and impacts_total >0  AND barclays_responder = 0  then weighting else null end) as b

       ,SUM(CASE WHEN social_grade2 NOT in ('A','B','C1') and impacts_total =0  AND barclays_responder = 1  then weighting else null end) as c
       ,SUM(CASE WHEN social_grade2 NOT in ('A','B','C1') and impacts_total =0  AND barclays_responder = 0  then weighting else null end) as d

       ,SUM(CASE WHEN social_grade2 in ('A','B','C1') and impacts_total >0  AND barclays_responder = 1  then weighting else null end) as e
       ,SUM(CASE WHEN social_grade2 in ('A','B','C1') and impacts_total >0  AND barclays_responder = 0  then weighting else null end) as f

       ,SUM(CASE WHEN social_grade2 in ('A','B','C1') and impacts_total =0  AND barclays_responder = 1  then weighting else null end) as g
       ,SUM(CASE WHEN social_grade2 in ('A','B','C1') and impacts_total =0  AND barclays_responder = 0  then weighting else null end) as h

from v081_vespa_Universe_demographics
where barclays_customer = 1



------------ stuff for coverage



SELECT  SUM(CASE WHEN social_grade2 in ('A','B','C1')  and impacts_total >0  AND barclays_responder = 1  then weighting else null end) as aa
       ,SUM(CASE WHEN social_grade2 in ('A','B','C1')  and impacts_total =0 AND barclays_responder = 1  then weighting else null end) as bb
       ,SUM(CASE WHEN social_grade2 in ('A','B','C1')  and impacts_total >0 AND barclays_responder = 0  then weighting else null end) as cc
       ,SUM(CASE WHEN social_grade2 in ('A','B','C1')  and impacts_total =0 AND barclays_responder = 0  then weighting else null end) as dd

       ,SUM(CASE WHEN social_grade2 not in ('A','B','C1')  and impacts_total >0  AND barclays_responder = 1  then weighting else null end) as aaa
       ,SUM(CASE WHEN social_grade2 not  in ('A','B','C1')  and impacts_total =0 AND barclays_responder = 1  then weighting else null end) as bbb
       ,SUM(CASE WHEN social_grade2  not in ('A','B','C1')  and impacts_total >0 AND barclays_responder = 0  then weighting else null end) as ccc
       ,SUM(CASE WHEN social_grade2  not in ('A','B','C1')  and impacts_total =0 AND barclays_responder = 0  then weighting else null end) as ddd

       ,SUM(CASE WHEN response_target = 1  and impacts_total >0  AND barclays_responder = 1  then weighting else null end) as a
       ,SUM(CASE WHEN response_target = 1  and impacts_total =0 AND barclays_responder = 1  then weighting else null end) as b
       ,SUM(CASE WHEN response_target = 1  and impacts_total >0 AND barclays_responder = 0  then weighting else null end) as c
       ,SUM(CASE WHEN response_target = 1  and impacts_total =0 AND barclays_responder = 0  then weighting else null end) as d

       ,SUM(CASE WHEN response_target_and_unknown = 1  and impacts_total >0  AND barclays_responder = 1  then weighting else null end) as e
       ,SUM(CASE WHEN response_target_and_unknown = 1  and impacts_total =0 AND barclays_responder = 1  then weighting else null end) as f
       ,SUM(CASE WHEN response_target_and_unknown = 1  and impacts_total >0 AND barclays_responder = 0  then weighting else null end) as g
       ,SUM(CASE WHEN response_target_and_unknown = 1  and impacts_total =0 AND barclays_responder = 0  then weighting else null end) as h

       ,SUM(CASE WHEN aspiration_target = 1  and impacts_total >0  AND barclays_responder = 1  then weighting else null end) as i
       ,SUM(CASE WHEN aspiration_target = 1  and impacts_total =0 AND barclays_responder = 1  then weighting else null end) as j
       ,SUM(CASE WHEN aspiration_target = 1  and impacts_total >0 AND barclays_responder = 0  then weighting else null end) as k
       ,SUM(CASE WHEN aspiration_target = 1  and impacts_total =0 AND barclays_responder = 0  then weighting else null end) as l

from v081_vespa_Universe_demographics
where barclays_customer = 1



----------------------------------------------------

-- This stuff is for zero rate spots --- and spot distribution





-- lets get the distribution of spots watched
select viewing_date
        ,count(distinct(case when whole_spot = 1 AND SPOT_AIR_DATE = viewing_date then spot_identifier else null end)) as Spots_viewed_aired_today
        ,count(distinct(case when whole_spot = 1 then spot_identifier else null end)) as distinct_Spots_viewed_incl_pb
        ,sum(case when whole_spot = 1 then whole_spot else null end) as spot_impacts_panel
        ,sum(whole_spot * weighting) as spots_impcats_Sky
from Barclays_spots_viewing_table_dump
group by viewing_date
order by viewing_date

select top 10 * from Barclays_spots_viewing_table_dump


-- how many of the aired spots were watched??

select (case when recorded_time_utc is null then viewing_date else cast(recorded_time_utc as date) end) as viewing_date
       ,count(distinct(case when whole_spot = 1 AND SPOT_AIR_DATE = viewing_date
        or SPOT_AIR_DATE = CAST(recorded_time_utc AS DATE) then spot_identifier else null end)) as SPOTS_VIEWED

         ,count(distinct(case when whole_spot = 1 AND SPOT_AIR_DATE = viewing_date
        or SPOT_AIR_DATE = CAST(recorded_time_utc AS DATE) AND social_grade2 in ('A','B','C1') then spot_identifier else null end)) as SPOTS_VIEWED_BROUGHT_AUDIENCE

        ,count(distinct(case when whole_spot = 1 AND SPOT_AIR_DATE = viewing_date
        or SPOT_AIR_DATE = CAST(recorded_time_utc AS DATE) AND RESPONSE_TARGET = 1 then spot_identifier else null end)) as SPOTS_VIEWED_responder_target

        ,count(distinct(case when whole_spot = 1 AND SPOT_AIR_DATE = viewing_date
        or SPOT_AIR_DATE = CAST(recorded_time_utc AS DATE) AND RESPONSE_TARGET_and_unknown = 1
        then spot_identifier else null end)) as SPOTS_VIEWED_responder_target_and_unknown

        ,count(distinct(case when whole_spot = 1 AND SPOT_AIR_DATE = viewing_date
        or SPOT_AIR_DATE = CAST(recorded_time_utc AS DATE) AND Aspiration_target = 1
        then spot_identifier else null end)) as SPOTS_VIEWED_Aspiration_target

        ,sum(whole_spot * VES.weighting) as spots_impcats_Sky


from Barclays_spots_viewing_table_dump BAR
join v081_Vespa_Universe_demographics ves
on ves.cb_key_household = bar.cb_key_household
group by viewing_date
order by viewing_date







select top 10 * from v081_Vespa_Universe_demographics



-- what were the audience figures?? for ZERO rated spots



select  viewing_date
        ,sum(whole_spot * VES.weighting) as spots_impcats_Sky

        ,sum(case when social_grade2 in ('A','B','C1') then whole_spot * VES.weighting ELSE NULL END) as spots_impcats_brought_aUDIENCE

         ,sum(case when RESPONSE_TARGET = 1  then whole_spot * VES.weighting ELSE NULL END) as spots_impcats_RESPONDER_TARGET_AUDIENCE

          ,sum(case when  RESPONSE_TARGET_and_unknown = 1 then whole_spot * VES.weighting ELSE NULL END) as spots_impcats_RESPONDER_TARGET_AND_UNKNOWN_AUDIENCE

           ,sum(case when Aspiration_target = 1 then whole_spot * VES.weighting ELSE NULL END) as spots_impcats_ASPIRATION_AUDIENCE

from Barclays_spots_viewing_table_dump BAR
join v081_Vespa_Universe_demographics ves
on ves.cb_key_household = bar.cb_key_household
group by viewing_date
order by viewing_date






-- lets find out how many spots were aired for each day
select distinct(local_date_of_transmission) as dates, count(distinct(identifier)) as spots_aired
  from barclays_spots
group by dates
order by dates






--------------------------------------------------------
-- lets find out how many barclays customers would match to the new vespa panel:

drop table november_panel

select distinct(cb_key_household) into gillh.november_panel from sk_prod.VESPA_EVENTS_VIEWED_ALL where cast(event_start_date_time_utc as date) = '2012-11-14'



select count(distinct(cb_key_household)) from november_panel -- 458,329

select count(distinct(cb_key_household)) from v081_barclays_Universe_demographics -- 7million


select count(*)
from november_panel
where cb_key_household in (select cb_key_household from v081_barclays_Universe_demographics)



select count(*)
from november_panel
where cb_key_household in (select cb_key_household from v081_barclays_Universe_demographics where responder = 1)



select count(distinct(cb_key_household)) from v081_vespa_Universe_demographics


------------------------------------------------------------------------------------

mosaic:

select distinct(cb_key_household)  into from v081_barclays_Universe_demographics where fss_v3_group  = 'Unknown Sky' and responder = 1



DROP TABLE #POSTCODES

select  cb_key_household
        ,postcode
into #postcodes
from OM114_BARCLAYS_RESPONSE
where cb_key_household in (select cb_key_household from v081_barclays_Universe_demographics where fss_v3_group = 'Unknown Sky' AND SKY_CUSTOMER = 1)
group by cb_key_household, postcode

select count(distinct(cb_key_household)) from #postcodes
-- 89988


DROP TABLE #TEST

select distinct(upper(mailable_postcode)) as postcode
        --, h_mosaic_uk_group,h_mosaic_uk_type,
        ,pc_mosaic_uk_type
        ,pc_mosaic_uk_type_desc = Case when pc_mosaic_uk_type = '01' then  'Global Power Brokers'
                                        when pc_mosaic_uk_type = '02' then  'Voices of Authority'
                                        when pc_mosaic_uk_type = '03' then  'Business Class'
                                        when pc_mosaic_uk_type = '04' then  'Serious Money'
                                        when pc_mosaic_uk_type = '05' then  'Mid-Career Climbers'
                                        when pc_mosaic_uk_type = '06' then  'Yesterdays Captains'
                                        when pc_mosaic_uk_type = '07' then  'Distinctive Success'
                                        when pc_mosaic_uk_type = '08' then  'Dormitory Villagers'
                                        when pc_mosaic_uk_type = '09' then  'Escape to the Country'
                                        when pc_mosaic_uk_type = '10' then  'Parish Guardians'
                                        when pc_mosaic_uk_type = '11' then  'Squires Among Locals'
                                        when pc_mosaic_uk_type = '12' then  'Country Loving Elders'
                                        when pc_mosaic_uk_type = '13' then  'Modern Agribusiness'
                                        when pc_mosaic_uk_type = '14' then  'Farming Today'
                                        when pc_mosaic_uk_type = '15' then  'Upland Struggle'
                                        when pc_mosaic_uk_type = '16' then  'Side Street Singles'
                                        when pc_mosaic_uk_type = '17' then  'Jacks of All Trades'
                                        when pc_mosaic_uk_type = '18' then  'Hardworking Families'
                                        when pc_mosaic_uk_type = '19' then  'Innate Conservatives'
                                        when pc_mosaic_uk_type = '20' then  'Golden Retirement'
                                        when pc_mosaic_uk_type = '21' then  'Bungalow Quietude'
                                        when pc_mosaic_uk_type = '22' then  'Beachcombers'
                                        when pc_mosaic_uk_type = '23' then  'Balcony Downsizers'
                                        when pc_mosaic_uk_type = '24' then  'Garden Suburbia'
                                        when pc_mosaic_uk_type = '25' then  'Production Managers'
                                        when pc_mosaic_uk_type = '26' then  'Mid-Market Families'
                                        when pc_mosaic_uk_type = '27' then  'Shop Floor Affluence'
                                        when pc_mosaic_uk_type = '28' then  'Asian Attainment'
                                        when pc_mosaic_uk_type = '29' then  'Footloose Managers'
                                        when pc_mosaic_uk_type = '30' then  'Soccer Dads and Mums'
                                        when pc_mosaic_uk_type = '31' then  'Domestic Comfort'
                                        when pc_mosaic_uk_type = '32' then  'Childcare Years'
                                        when pc_mosaic_uk_type = '33' then  'Military Dependants'
                                        when pc_mosaic_uk_type = '34' then  'Buy-to-Let Territory'
                                        when pc_mosaic_uk_type = '35' then  'Brownfield Pioneers'
                                        when pc_mosaic_uk_type = '36' then  'Foot on the Ladder'
                                        when pc_mosaic_uk_type = '37' then  'First to Move In'
                                        when pc_mosaic_uk_type = '38' then  'Settled Ex-Tenants'
                                        when pc_mosaic_uk_type = '39' then  'Choice Right to Buy'
                                        when pc_mosaic_uk_type = '40' then  'Legacy of Labour'
                                        when pc_mosaic_uk_type = '41' then  'Stressed Borrowers'
                                        when pc_mosaic_uk_type = '42' then  'Worn-Out Workers'
                                        when pc_mosaic_uk_type = '43' then  'Streetwise Kids'
                                        when pc_mosaic_uk_type = '44' then  'New Parents in Need'
                                        when pc_mosaic_uk_type = '45' then  'Small Block Singles'
                                        when pc_mosaic_uk_type = '46' then  'Tenement Living'
                                        when pc_mosaic_uk_type = '47' then  'Deprived View'
                                        when pc_mosaic_uk_type = '48' then  'Multicultural Towers'
                                        when pc_mosaic_uk_type = '49' then  'Re-Housed Migrants'
                                        when pc_mosaic_uk_type = '50' then  'Pensioners in Blocks'
                                        when pc_mosaic_uk_type = '51' then  'Sheltered Seniors'
                                        when pc_mosaic_uk_type = '52' then  'Meals on Wheels'
                                        when pc_mosaic_uk_type = '53' then  'Low Spending Elders'
                                        when pc_mosaic_uk_type = '54' then  'Clocking Off'
                                        when pc_mosaic_uk_type = '55' then  'Backyard Regeneration'
                                        when pc_mosaic_uk_type = '56' then  'Small Wage Owners'
                                        when pc_mosaic_uk_type = '57' then  'Back-to-Back Basics'
                                        when pc_mosaic_uk_type = '58' then  'Asian Identities'
                                        when pc_mosaic_uk_type = '59' then  'Low-Key Starters'
                                        when pc_mosaic_uk_type = '60' then  'Global Fusion'
                                        when pc_mosaic_uk_type = '61' then  'Convivial Homeowners'
                                        when pc_mosaic_uk_type = '62' then  'Crash Pad Professionals'
                                        when pc_mosaic_uk_type = '63' then  'Urban Cool'
                                        when pc_mosaic_uk_type = '64' then  'Bright Young Things'
                                        when pc_mosaic_uk_type = '65' then  'Anti-Materialists'
                                        when pc_mosaic_uk_type = '66' then  'University Fringe'
                                        when pc_mosaic_uk_type = '67' then  'Study Buddies'
                                        when pc_mosaic_uk_type = '99' then  'Unclassified'
                                        else null end
into #test
from sk_prod.experian_consumerview
where mailable_postcode in (select upper(postcode) from #postcodes)


select * from #test

select pc_mosaic_uk_type_desc
        ,count(*)
from #test
group by pc_mosaic_uk_type_desc

select count(*) from #test



---------------------------------------------------------------


--- LETS GET THE SOCIAL GRADE DETAILS FOR THE DEFINE TARGET SECTION

-- THIS OUTPUT WILL HAVE TO BE TWEAKED IN EXCEL TO GET THE PERCENTAGES TO PUT INTO SUSANNES TEMPLATE

select fss_v3_group
        , count(case when social_grade2 = 'A' then cb_key_household else null end) as barclays_Population_A
        , count(case when social_grade2 = 'B' then cb_key_household else null end) as barclays_Population_B
        , count(case when social_grade2 = 'C1' then cb_key_household else null end) as barclays_Population_C1
        , count(case when social_grade2 = 'C2' then cb_key_household else null end) as barclays_Population_C2
        , count(case when social_grade2 = 'D' then cb_key_household else null end) as barclays_Population_D
        , count(case when social_grade2 = 'E' then cb_key_household else null end) as barclays_Population_E
        , count(case when social_grade2 = 'Unknown Default' then cb_key_household else null end) as barclays_Population_Unknown
from v081_barclays_Universe_demographics
group by fss_v3_group
order by fss_v3_group


select distinct(social_grade2) from v081_barclays_Universe_demographics


select social_grade2
        ,count(case when responder = 1 then cb_key_household else null end) as responders
        ,count(cb_key_household) as barclays_base
from v081_barclays_Universe_demographics
group by social_grade2
order by social_grade2




select social_grade
        ,count(case when responder = 1 then cb_key_household else null end) as responders
        ,count(cb_key_household) as barclays_base
from v081_barclays_Universe_demographics
group by social_grade
order by social_grade






---------------------------------------------------------------------------------------------



-- more info at martins request

select sum(weighting)
from v081_vespa_Universe_demographics
where social_grade2 in ('A','B','C1')
and impacts_total > 0





select impacts_total
       ,sum(case when SOCIaL_GRADE2 IN ('A','B','C1') then weighting else null end) as abc1
       ,sum(weighting) as sky_base

from v081_Vespa_Universe_demographics
where weighting is not null
group by impacts_total
order by impacts_total


sum(



----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------

--- martin would like a detailed break down of each spot;

select top 10 * from v081_Vespa_Universe_demographics -- spot_identifier -- this is the rollup field


drop table #spot_identifier

select  bar.account_number
        ,bar.cb_key_household
        ,bar.spot_identifier
        ,impacts = COUNT(case when whole_spot = 1 then 1 else null end)
        ,weighted_impacts = impacts*weighting
        ,max(barclays_customer) as barclays_customer
        ,max(fss_v3_group) as fss_v3_group
        ,max(barclays_responder) as responder
        ,max(bar.weighting)as weighting
        ,max(ves.aspiration_target) as aspiration_target
        ,max(ves.response_target) as response_target
        ,max(response_target_and_unknown) as response_target_and_unknown
        ,max(ABC1_TARGET) as ABC1_TARGET
    --    ,max(barclays_customer) as barclays_customer
into #spot_identifier
from Barclays_spots_viewing_table_dump bar
right join v081_Vespa_Universe_demographics ves
on ves.account_number = bar.account_number
group by bar.account_number, bar.cb_key_household,bar.spot_identifier



-- lets get a roll up for each spot; who watched??

drop table #spot_impacts

select spot_identifier
        ,sum(weighting) as Sky_base
        ,sum(case when ABC1_TARGET = 1 then weighted_impacts else null end) as ABC1_Sky
        ,sum(case when ABC1_TARGET = 1 and barclays_customer = 1 then weighted_impacts else null end) as ABC1_Sky_Barclays_target
        ,sum(case when ABC1_TARGET = 1 and responder = 1 then weighted_impacts else null end) as ABC1_responders
        ,sum(case when responder = 1 then weighted_impacts else null end) as all_responders
        ,sum(case when responder = 1 and fss_v3_group <> 'Unknown Sky' then weighted_impacts else null end) as all_responders_less_unknowns
        ,sum(case when aspiration_target = 1 then weighted_impacts else null end) as aspiration_target
into #spot_impacts
from #spot_identifier
group by spot_identifier
order by spot_identifier
--24,965


select top 20 * from #spot_impacts where all_responders <> all_responders_less_unknowns

-- THE ABOVE IS WORKING FINE!


-- OK NOW WE NEED TO ADD MEDIA PAPCK TO THE BARCLAYS SPOTS:


-- now lets put the media pack into the cube.
alter table barclays_spots
        add media_pack varchar(25);


update barclays_spots
        set cub.media_pack = tmp.channel_category
from barclays_spots as cub
join LkUpPack as tmp
on tmp.service_key = cub.service_key




-- now lets put everything together

drop table Barclays_spots_details



select spot.identifier
        , local_date_of_transmission as air_date
        , sales_house
        , spot.media_pack
        , agg_channel_name
        , (case
                when datepart(weekday,local_date_of_transmission)=1 then 'Sunday'
                when datepart(weekday,local_date_of_transmission)=2 then 'Monday'
                when datepart(weekday,local_date_of_transmission)=3 then 'Tuesday'
                when datepart(weekday,local_date_of_transmission)=4 then 'Wednesday'
                when datepart(weekday,local_date_of_transmission)=5 then 'Thursday'
                when datepart(weekday,local_date_of_transmission)=6 then 'Friday'
                when datepart(weekday,local_date_of_transmission)=7 then 'Saturday'
        end) as day_aired

        ,(case when cast(local_spot_start_date_time as time) between '06:00:00' and '08:59:59' then 'Breakfast'
                when cast(local_spot_start_date_time as time) between '09:00:00' and '11:59:59' then 'Morning'
                when cast(local_spot_start_date_time as time) between '12:00:00' and '14:59:59' then 'Lunch'
                when cast(local_spot_start_date_time as time) between '15:00:00' and '17:59:59' then 'Early Prime'
                when cast(local_spot_start_date_time as time) between '18:00:00' and '20:59:59' then 'Prime'
                when cast(local_spot_start_date_time as time) between '21:00:00' and '23:59:59' then 'Late Night'
                when cast(local_spot_start_date_time as time) between '00:00:00' and '05:59:59' then 'night'
                else 'Unkown' end) as day_part

        ,genre ='This is a genre placeholder'
        ,sub_genre = 'sub genre placeholder'

        ,Sky_base
        ,ABC1_Sky
        ,ABC1_Sky_Barclays_target
        ,ABC1_responders
        ,all_responders
        ,all_responders_less_unknowns
        ,aspiration_target
into Barclays_spots_details
FROM barclays_spots spot
LEFT JOIN #spot_impacts imp
ON spot_identifier = IDENTIFIER



alter table Barclays_spots_details
add  epg_title varchar(50)



------------------------------------------------
-- the update above gives a poor match -- lets get a better measure of the genre




SELECT IDENTIFIER
        ,GENRE_DESCRIPTION
        ,SUB_GENRE_DESCRIPTION
        ,epg_title
into #spot_genre2
 FROM VESPA_Programmes_project_108 PRO
JOIN BARCLAYS_SPOTS AS SPOT
on SPOT.SERVICE_KEY = PRO.SERVICE_KEY
and utc_spot_start_date_time between tx_start_datetime_utc and tx_end_datetime_utc



update Barclays_spots_details
set spot.genre = case when gen.GENRE_DESCRIPTION is not null then GENRE_DESCRIPTION else genre end
        ,spot.sub_genre = case when gen.SUB_GENRE_DESCRIPTION is not null then SUB_GENRE_DESCRIPTION else sub_genre end
        ,spot.epg_title = gen.epg_title
from barclays_spots_details spot
left join  #spot_genre2 gen
on gen.identifier = spot.identifier



update Barclays_spots_details
set genre = (case when genre = 'This is a genre placeholder' then 'Not Matched' else genre end)

update Barclays_spots_details
set sub_genre = (case when sub_genre = 'sub genre placeholder' then 'Not Matched' else sub_genre end)

select * from Barclays_spots_details


-- lets now add the local spot start time and the day part

alter table Barclays_spots_details
add (local_spot_start_time TIME
    ,think_box_day_part varchar(12));

-- lets updatye the spot start time
UPDATE Barclays_spots_details
 SET  local_spot_start_time = cast(local_spot_start_date_time as time)
 FROM Barclays_spots_details as det
 left join barclays_spots as spot
      on det.identifier = spot.identifier

-- lets add Martins new definition of the day part:
update Barclays_spots_details
        set think_box_day_part = (case when local_spot_start_time between '06:00:00' and '08:59:59' then 'Breakfast'
                when local_spot_start_time between '09:00:00' and '17:29:59' then 'Daytime'
                when local_spot_start_time between '17:30:00' and '19:59:59' then 'Early Peak'
                when local_spot_start_time between '20:00:00' and '22:59:59' then 'Late Peak'
                when local_spot_start_time between '23:00:00' and '24:29:59' then 'Post Peak'
                when local_spot_start_time between '24:30:00' and '05:59:59' then 'Night Time'
                else 'Unkown' end)

select top 100 * from Barclays_spots_details




select identifier
        , air_date
        , sales_house
        , media_pack
        , agg_channel_name
        , day_aired
        , local_spot_start_time
        , think_box_day_part
        ,genre
        ,sub_genre
        ,epg_title
        ,Sky_base
        ,ABC1_Sky
        ,ABC1_Sky_Barclays_target
        ,ABC1_responders
        ,all_responders
        ,all_responders_less_unknowns
        ,aspiration_target
from Barclays_spots_details


-- select top 10 * from Barclays_spots_details
-- select distinct(genre), count(*) from Barclays_spots_details group by genre
--

---------------------------------------------------

-- so it seems the zero rated sopts are those that did not match back based on service key and the EPG measures.

select top 10 * from Barclays_spots_details

select distinct(identifier) from Barclays_spots_details where sky_base is null


select count(distinct(spot_identifier)) from Barclays_spots_viewing_table_dump
where spot_identifier in (select distinct(identifier) from Barclays_spots_details where sky_base is null)


select distinct(identifier) from Barclays_spots_details where genre = 'Not Matched'


-- none because the service key etc did not match back




------------------------------------------------------------------


-- lets get an idea of the viewing decile distribution for target audiences:
--abc1
--abc1 responders
--all responders



select top 10 * from v081_Vespa_Universe_demographics


select total_tv_deciles
        ,sum(weighting) as Sky Base
        ,sum(case when ABC1_TARGET = 1 then weighting else null end) as ABC1_Sky
        ,sum(case when ABC1_TARGET = 1 and barclays_responder = 1 then weighting else null end) as ABC1_respodners
        ,sum(case when barclays_responder = 1 then weighting else null end) as All_responders
from v081_Vespa_Universe_demographics
where barclays_customer = 1
group by total_tv_deciles
order by total_tv_deciles


select total_tv_deciles
        ,sum(weighting) as Sky_Base
from v081_Vespa_Universe_demographics
group by total_tv_deciles
order by total_tv_deciles



--------------------------------------------------------------------


select fss_v3_group
        ,sum(weighting)
from v081_Vespa_Universe_demographics
where barclays_responder = 1
group by fss_v3_group
order by fss_v3_group



select fss_v3_group
        , sum(case when social_grade2 = 'A' then weighting else null end) as Sky_Population_A
        , sum(case when social_grade2 = 'B' then weighting else null end) as Sky_Population_B
        , sum(case when social_grade2 = 'C1' then weighting else null end) as Sky_Population_C1
        , sum(case when social_grade2 = 'C2' then weighting else null end) as Sky_Population_C2
        , sum(case when social_grade2 = 'D' then weighting else null end) as Sky_Population_D
        , sum(case when social_grade2 = 'E' then weighting else null end) as Sky_Population_E
        , sum(case when social_grade2 = 'Unknown Default' then weighting else null end) as Sky_Population_Unknown
from v081_vespa_Universe_demographics
group by fss_v3_group
order by fss_v3_group


select distinct social_grade2 from v081_barclays_Universe_demographics



-----------------------------------------------------------------------

---- lets get age and household compsoition for those households that are unknown responders

select top 10 *
FROM v081_vespa_Universe_demographics

SELECT COUNT(*), COUNT(DISTINCT(CB_KEY_HOUSEHOLD)) FROM v081_vespa_Universe_demographics
-- THERE ARE A FEW DUPLICATES



SELECT


HOUSEHOLD_COMPOSITION

-- WHERE CAN I GET AGES?



sp_columns 'EXPERIAN_CONSUMERVIEW'
SELECT TOP 10 * FROM sk_prod.EXPERIAN_CONSUMERVIEW



select top 10   h_age_coarse -- Age is based on the individual age estimate of the head of household
        ,h_age_fine
        ,p_actual_age
        ,p_age_fine
        ,p_age_coarse  -- person level -- Age identifies the likely age of each individual living at an address
from sk_prod.EXPERIAN_CONSUMERVIEW



select cb_key_household
        ,max(p_actual_age) as max_age
into #responder_hh_max_age
from sk_prod.EXPERIAN_CONSUMERVIEW
where cb_key_household in (select cb_key_household from v081_vespa_Universe_demographics where barclays_responder = 1 )
group by cb_key_household
-- the above gets the responders who are also on the vespa panel -- not the sky base, not all respodners (from the barclasy file)


select count(distinct (cb_key_household)) from v081_vespa_Universe_demographics where barclays_responder = 1
-- 4991


select count(distinct (cb_key_household)) from #responder_hh_max_age
-- 4708 -- only this many matched

select top 10 * from #responder_hh_max_age


drop table #test


select ves.cb_key_household
        ,max(ves.HOUSEHOLD_COMPOSITION) as household_composition
        ,max(exp.max_age) as max_hh_age
        ,max(weighting) as weighting
into #test
from v081_vespa_Universe_demographics as ves
left join #responder_hh_max_age as exp
on ves.cb_key_household = exp.cb_key_household
where barclays_responder = 1
group by ves.cb_key_household


select * from #test


select fss_v3_group
 ,sum(distinct(case when


select top 10 * from v081_vespa_Universe_demographics





----------------------------------------------------------------------------------------------------
select fss_v3_group
        , sum(weighting) as Sky_BASE
from v081_vespa_Universe_demographics
WHERE BARCLAYS_CUSTOMER  = 1
group by fss_v3_group
order by fss_v3_group




select fss_v3_group
        , sum(case when have_cash_isa_experian = 1 then weighting else null end) as have_isa
        , sum(case when social_grade2 in ('A','B','C1') then weighting else null end) as ABC1
        , sum(weighting) as Sky_BASE
from v081_vespa_Universe_demographics
WHERE BARCLAYS_CUSTOMER  = 1
group by fss_v3_group
order by fss_v3_group

-- WE WILL BNEED TO PULL THE BARCLAYS _BASE SEPERATELY


select fss_v3_group
        , COUNT(CB_KEY_HOUSEHOLD) AS BARCLAYS_BASE
from v081_BARCLAYS_Universe_demographics
group by fss_v3_group
order by fss_v3_group




select fss_v3_group
        , COUNT(CB_KEY_HOUSEHOLD) AS BARCLAYS_BASE
from v081_BARCLAYS_Universe_demographics
where responder = 1
group by fss_v3_group
order by fss_v3_group





---------------------------------------------


The Main tables


grant select on v081_BARCLAYS_Universe_demographics to public

grant select on v081_BARCLAYS_Universe_demographics to public
grant select on v081_vespa_Universe_demographics to public
grant select on Barclays_spots_details to public
grant select on barclays_spots to public
grant select on Barclays_spots_viewing_table_dump to public

grant select on Project_108_viewing_table_dump_2weeks to public









v081_BARCLAYS_Universe_demographics
v081_vespa_Universe_demographics
Barclays_spots_details
barclays_spots
Barclays_spots_viewing_table_dump
Project_108_viewing_table_dump_2weeks



---------------------------------------------------------------------------------------

-- all_spots_2weeks_108 -- this table has all ,spots over the 2 week period


--

select top 10 * from Barclays_spots_details
select top 10 * from barclays_spots
select top 10 * from neighbom.barclays_spot_list

log_station_code
sti_code






select distinct(log_station_code) from barclays_spots -- range 1-6
select distinct(sti_code) from neighbom.barclays_spot_list



select top 10 * from Barclays_spots_viewing_table_dump




select spot_identifier
        ,sum(case when whole_spot = 1 then bar.weighting else null end) as impacts_all
        ,sum(case when whole_spot = 1 and social_grade2 in ('A','B','C1')  then bar.weighting else null end) as impacts_abc1
into spot_viewing
from Barclays_spots_viewing_table_dump BAR
join v081_Vespa_Universe_demographics ves
on ves.cb_key_household = bar.cb_key_household
group by spot_identifier


-- lets check that this worked!
select top 10 * from spot_viewing order by spot_identifier

select count(*) from neighbom.barclays_spot_list -- 20k





select *
into barclays_spots_viewing
from barclays_spots



alter table barclays_spots_viewing
 add (Sky_impacts integer
        ,Sky_ABC1_impacts integer
        ,barb_HOUSHEOLDS integer
        ,BARB_ABC1_HOUSEHOLD integer
        ,Matched_to_barb_file integer)


-- lets add the vespa viewing figures :
 update barclays_spots_viewing
        set Sky_impacts = impacts_all
                , Sky_ABC1_impacts = impacts_abc1
 from barclays_spots_viewing
 left join spot_viewing
 on spot_identifier = identifier


-- now lets add the barb spot vieiwng figures :
 update barclays_spots_viewing
        set barb_HOUSHEOLDS = households
                , BARB_ABC1_HOUSEHOLD = abc1_households
 from barclays_spots_viewing as ves
 left join neighbom.barclays_spot_list as barb
 on barb.sti_code = ves.sti_code
 and barb.log_station_code = ves.log_station_code
 and barb.utc_spot_start_date_time = ves.utc_spot_start_date_time


select top 100 * from barclays_spots_viewing where barb_HOUSHEOLDS is null
select top 10 * from neighbom.barclays_spot_list where households is null



select count(case when barb_HOUSHEOLDS is not null and barb_HOUSHEOLDS >0 then service_key else null end) as spots_viewed_barb
        ,count(case when barb_HOUSHEOLDS is null or barb_HOUSHEOLDS  = 0 then service_key else null end) as barb_zero_rated
from barclays_spots_viewing



select top 10 * from barclays_spots_viewing



-- lets add the vespa viewing figures :
 update barclays_spots_viewing
        set Matched_to_barb_file = (case when barb_HOUSHEOLDS is null then 0 else 1 end)





-- NOTe I HAVE LEFT JOINED BARB'S SPOT VIEWING FIGURES TO THE VESPA SPOTS META FILE UNDER THE ASSUMPTION THAT THEY ARE ALL CONTAINED IN THERE
-- the null fields for the barb viewing stats indicate there was no match as there are no null fields in neighbom.barclays_spot_list

-- so now we have a match at time, channel and STI level .





 select log_station_code
        ,sti_code
        ,utc_spot_start_date_time
        ,sum(Sky_impacts) as Sky_impacts
        ,sum(Sky_ABC1_impacts) as Sky_ABC1_impacts
        ,sum(barb_HOUSHEOLDS) as barb_HOUSHEOLDS
        ,sum(BARB_ABC1_HOUSEHOLD) as BARB_ABC1_HOUSEHOLD
        ,max(Matched_to_barb_file) as Matched_to_barb_file
 into #table
 from barclays_spots_viewing
 group by log_station_code
        ,sti_code
        ,utc_spot_start_date_time
 order by log_station_code
        ,sti_code
        ,utc_spot_start_date_time


select count(*) from #table where barb_HOUSHEOLDS = 0 or barb_HOUSHEOLDS is NULL and


-- lets also get some high level summaries

select count(case when barb_HOUSHEOLDS = 0 or barb_HOUSHEOLDS is NULL then log_station_code else null end ) as barb_not_viewed_HH
        ,count(case when BARB_ABC1_HOUSEHOLD = 0 or BARB_ABC1_HOUSEHOLD is NULL then log_station_code else null end ) as barb_not_viewed_abc1
        ,count(case when barb_HOUSHEOLDS <> 0 and barb_HOUSHEOLDS is not NULL then log_station_code else null end ) as barb_viewed_HH
        ,count(case when BARB_ABC1_HOUSEHOLD <> 0 and BARB_ABC1_HOUSEHOLD is not NULL then log_station_code else null end ) as barb_viewed_abc1
from #table
where  Matched_to_barb_file = 1



select count(case when Sky_impacts = 0 or Sky_impacts is NULL then log_station_code else null end ) as vespa_not_viewed_HH
        ,count(case when Sky_ABC1_impacts = 0 or Sky_ABC1_impacts is NULL then log_station_code else null end ) as vespa_not_viewed_abc1
        ,count(case when Sky_impacts <> 0 and Sky_impacts is not NULL then log_station_code else null end ) as vespa_viewed_HH
        ,count(case when Sky_ABC1_impacts <> 0 and Sky_ABC1_impacts is not NULL then log_station_code else null end ) as vespa_viewed_abc1
from #table




select count(*) from #table where barb_HOUSHEOLDS = 0 or barb_HOUSHEOLDS is NULL and Matched_to_barb_file = 1


select count(*) from neighbom.barclays_spot_list


