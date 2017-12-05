
-- OUTOUT CODE
-- this part of the code is focused on getting outputs for the Molson Coors analysis:

--------------------------------------------------------------------------------------------------------------------------------
-- Stella Cidre

SELECT TOP 10 * FROM starmerh.molson_coors_hh_aggregate -- household aggregate
SELECT TOP 10 * FROM starmerh.molson_coors_spot_data2 -- spot data
SELECT TOP 10 * FROM starmerh.molson_coors_viewing_data -- viewing data
SELECT TOP 10 * FROM starmerh.molson_coors_impacts -- spot table

-- table before the spot table
select top 10 * from starmerh.molson_coors_hh_exp_impacts
--------------------------------------------------------------------------------------------------------------------------------


--- COPIED TO MY SCHEMA AND WEIGHTS UPDATED
molson_coors_viewing_data
molson_coors_hh_aggregate

molson_coors_hh_impacts -- WHICH FEEDS INTO
molson_coors_hh_exp_impacts -- which feeds into
molson_coors_impacts -- all updated



select top 10 * from molson_coors_impacts


-- lets flag vespa panelists in the HH aggregate table
select * into molson_coors_hh_aggregate from starmerh.molson_coors_hh_aggregate

alter table molson_coors_hh_aggregate
add vespa_panelist integer default 0

update molson_coors_hh_aggregate as agg
 set vespa_panelist = 1
from starmerh.molson_coors_viewing_data as vw
 where vw.cb_key_household = agg.cb_key_household


-- select vespa_panelist
--        ,count(*)
-- from molson_coors_hh_aggregate
-- group by vespa_panelist

-- OUTPUT: **UNIVERSE VALIDATION**

-- WE WANT TO SIZE UP THE VARIOUS SEGMENTS ON THE UK AND SKY BASE

SELECT BUCKLE_SEGMENTS
        ,count(case when vespa_panelist = 1 then 1 else null end) as Panel
        ,COUNT(*) AS uk
        ,SUM(MIDDAY_WEIGHTINGS) AS SKY_scaled
FROM molson_coors_hh_aggregate
GROUP BY BUCKLE_SEGMENTS
order by buckle_segments


-- now lets find out how segment sizes vary on the actual Sky base

--make a copy of an exsisting base table (May 2012)
select * into Molson_coors_Sky_Base from v126_active_customer_base2

-- add and populate the segment feild
alter table Molson_coors_Sky_Base
 add BUCKLE_SEGMENTS varchar(30)

 update Molson_coors_Sky_Base as sky
        set sky.BUCKLE_SEGMENTS = agg.BUCKLE_SEGMENTS
 from Molson_coors_y_Base as sky
 left join starmerh.molson_coors_hh_aggregate as agg
 on sky.cb_key_household = agg.cb_key_household


--select top 10 * from Molson_coors_Sky_Base

 update Molson_coors_Sky_Base as sky
        set BUCKLE_SEGMENTS = case when BUCKLE_SEGMENTS is null then 'Unknown' else BUCKLE_SEGMENTS end




-- get the outputs; (universe validation tab)
SELECT BUCKLE_SEGMENTS
        ,COUNT(*) AS Sky_base_actual
FROM Molson_coors_Sky_Base
GROUP BY BUCKLE_SEGMENTS
order by buckle_segments




--------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------
--- OUTPUT: AUDIENCES TAB



SELECT SUM(CASE WHEN BROUGHT_AUDIENCE = 1 THEN MIDDAY_WEIGHTINGS ELSE NULL END) AS BOUGHT_AUDIENCE
        ,SUM(CASE WHEN ASPIRATIONAL_AUDIENCE = 1 THEN MIDDAY_WEIGHTINGS ELSE NULL END) AS ASPIRATIOANL_AUDIENCE
FROM molson_coors_hh_aggregate




SELECT TOP 10 * FROM molson_coors_hh_aggregate


-- aspirational and bought
select sum(midday_weightings)
from molson_coors_hh_aggregate
where brought_audience = 1 and
   aspirational_audience = 1

--target and bought
select sum(midday_weightings)
from molson_coors_hh_aggregate
where brought_audience = 1 and
   social_explorers = 1

--target and aspiration
select sum(midday_weightings)
from molson_coors_hh_aggregate
where aspirational_audience = 1 and
   social_explorers = 1


--target and aspiration
select sum(midday_weightings)
from molson_coors_hh_aggregate
where    social_explorers = 1

social_explorers
brought_audience
aspirational_audience

-- STELLA CIDRE OUTPUT --
-- GET CLIENT PIVOT TABLE OUTPUTTED --
drop table stella_cidre_impacts;
drop table stella_cidre_impacts;
select     barb_date_of_transmission
          ,barb_spot_start_time
          ,utc_spot_start_date_time
          ,daypart
          ,clearcast_commercial_no
          ,client_spot_flag
          ,channel_new
          ,sales_house
          ,media_pack
          ,genre_description
          ,sub_genre_description
          ,programme_name
          ,case when clearcast_commercial_no= 'MUMSTCI009040  ' then (1.33*stella_cidre_impacts)
                else stella_cidre_impacts
           end as total_impacts
          ,case when clearcast_commercial_no= 'MUMSTCI009040  ' then (1.33*brought_audience_impacts)
                else brought_audience_impacts
           end as brought_impacts
          ,case when clearcast_commercial_no= 'MUMSTCI009040  ' then (2*aspirational_audience_impacts)
                else aspirational_audience_impacts
           end as aspirational_impacts
          ,case when clearcast_commercial_no= 'MUMSTCI009040  ' then (1.33*social_explorers_impacts)
                else social_explorers_impacts
           end as se_impacts
into stella_cidre_impacts
from molson_coors_impacts
where client_spot_flag = 'Stella Cidre';



grant select on stella_cidre_impacts to gillh;
select * from stella_cidre_impacts;


-- SUB GENRE AND MEDIA PACK SUMMARY --

select     genre_description
          ,sub_genre_description
          ,sum(total_impacts) as total_impacts
          ,sum(brought_impacts) as brought_impacts
          ,sum(se_impacts) as se_impacts
from       stella_cidre_impacts
group by   genre_description
          ,sub_genre_description
order by   genre_description
          ,sub_genre_description;

-- GENRE SUMMARY --

select     genre_description
          ,sum(total_impacts) as total_impacts
          ,sum(brought_impacts) as brought_impacts
          ,sum(se_impacts) as se_impacts
from       stella_cidre_impacts
group by   genre_description;

-- SALES HOUSE SUMMARY --

select     sales_house
          ,sum(total_impacts) as total_impacts
          ,sum(brought_impacts) as brought_impacts
          ,sum(se_impacts) as se_impacts
from       stella_cidre_impacts
group by   sales_house;

-- CHANNEL GROUP SUMMARY --

select     sales_house
          ,media_pack
          ,sum(total_impacts) as total_impacts
          ,sum(brought_impacts) as brought_impacts
          ,sum(se_impacts) as se_impacts
from       stella_cidre_impacts
group by   sales_house
          ,media_pack
order by   sales_house
          ,media_pack;

-- DAYPART SUMMARY --

select     daypart
          ,sum(total_impacts) as total_impacts
          ,sum(brought_impacts) as brought_impacts
          ,sum(se_impacts) as se_impacts
from       stella_cidre_impacts
group by   daypart;

-- ALL SPOTS --
drop table mc_all_spots_impacts;
select     barb_date_of_transmission
          ,barb_spot_start_time
          ,utc_spot_start_date_time
          ,daypart
          ,clearcast_commercial_no
          ,channel_new
          ,sales_house
          ,media_pack
          ,genre_description
          ,sub_genre_description
          ,programme_name
          ,case when spot_duration=40 then (1.33*total_impacts)
                when spot_duration=50 then (1.66*total_impacts)
                when spot_duration=60 then (2*total_impacts)
                else total_impacts
           end as total_impacts
          ,case when spot_duration=40 then (1.33*brought_audience_impacts)
                when spot_duration=50 then (1.66*brought_audience_impacts)
                when spot_duration=60 then (2*brought_audience_impacts)
                else brought_audience_impacts
           end as brought_impacts
          ,case when spot_duration=40 then (2*aspirational_audience_impacts)
                when spot_duration=50 then (1.66*aspirational_audience_impacts)
                when spot_duration=60 then (2*aspirational_audience_impacts)
                else aspirational_audience_impacts
           end as aspirational_impacts
          ,case when spot_duration=40 then (1.33*social_explorers_impacts)
                when spot_duration=50 then (1.66*social_explorers_impacts)
                when spot_duration=60 then (2*social_explorers_impacts)
                else social_explorers_impacts
           end as se_impacts
into mc_all_spots_impacts
from molson_coors_all_spots_impacts;

grant select on mc_all_spots_impacts to gillh;

select * from mc_all_spots_impacts;

-- SUB GENRE AND MEDIA PACK SUMMARY --

select     genre_description
          ,sub_genre_description
          ,sum(total_impacts) as total_impacts
          ,sum(brought_impacts) as brought_impacts
          ,sum(se_impacts) as se_impacts
from       mc_all_spots_impacts
group by   genre_description
          ,sub_genre_description
order by   genre_description
          ,sub_genre_description;

-- GENRE SUMMARY --

select     genre_description
          ,sum(total_impacts) as total_impacts
          ,sum(brought_impacts) as brought_impacts
          ,sum(se_impacts) as se_impacts
from       mc_all_spots_impacts
group by   genre_description;

-- SALES HOUSE SUMMARY --

select     sales_house
          ,sum(total_impacts) as total_impacts
          ,sum(brought_impacts) as brought_impacts
          ,sum(se_impacts) as se_impacts
from       mc_all_spots_impacts
group by   sales_house;

-- CHANNEL GROUP SUMMARY --

select     sales_house
          ,media_pack
          ,sum(total_impacts) as total_impacts
          ,sum(brought_impacts) as brought_impacts
          ,sum(se_impacts) as se_impacts
from       mc_all_spots_impacts
group by   sales_house
          ,media_pack
order by   sales_house
          ,media_pack;

-- DAYPART SUMMARY --

select     daypart
          ,sum(total_impacts) as total_impacts
          ,sum(brought_impacts) as brought_impacts
          ,sum(se_impacts) as se_impacts
from       mc_all_spots_impacts
group by   daypart;























