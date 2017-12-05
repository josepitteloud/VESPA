



--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- REACH OUTPUTS:


-- need a summary of the evaluation metrics:


-- Reach for each Audience --


select sum(MIDDAY_WEIGHTINGS) as Sky
       ,sum(case when brought_audience = 1 then midday_weightings else null end) as Bought_audience
        ,sum(case when aspirational_audience = 1 then midday_weightings else null end) as aspirational_audience
        ,sum(case when social_explorers = 1 then midday_weightings else null end) as target_audience
from  starmerh.molson_coors_hh_aggregate
where stella_cidre_exposed =1
-- the above has already filtered for spot_viewed = 1



-- lets get the reach for customers who have watched >= 3 spots

if object_id ('molson_coors_all_spots_reach') is not null drop table molson_coors_all_spots_reach


select vw.cb_key_household
        ,max(buckle_segments) as buckle_segments
        ,count(case when spot_viewed = 1 then 1 else null end) as spots_viewed
        ,max(cast(agg.midday_weightings as float)) as midday_weightings
        ,max(brought_audience) as bought_audience
        ,max(aspirational_audience) as aspirational_audience
        ,max(social_explorers) as target_audience
into molson_coors_all_spots_reach
from starmerh.molson_coors_allspots_viewing_data_new2 vw
inner join starmerh.molson_coors_hh_aggregate agg
on agg.cb_key_household = vw.cb_key_household
group by vw.cb_key_household


-- check -- most customers watched <3 spots
select spots_viewed
        ,sum(midday_weightings)
from molson_coors_all_spots_reach
group by spots_viewed
order by spots_viewed



-- OUTPUT --

select sum(midday_weightings) as Sky
        ,sum(case when bought_audience = 1 then midday_weightings else null end) as bought_audience
        ,sum(case when aspirational_audience = 1 then midday_weightings else null end) as aspirational_audience
        ,sum(case when target_audience = 1 then midday_weightings else null end) as target_audience
from molson_coors_all_spots_reach
where spots_viewed >= 3



-- the number of spots seen
select count(distinct(identifier_agg)) as spots_seen
        ,count(distinct(case when agg.brought_audience = 1 then identifier_agg else null end)) as bought_audience
        ,count(distinct(case when agg.aspirational_audience = 1 then identifier_agg else null end)) as aspirational_audience
        ,count(distinct(case when agg.social_explorers = 1 then identifier_agg else null end)) as target_audience
from starmerh.molson_coors_allspots_viewing_data_new2 vw
inner join starmerh.molson_coors_hh_aggregate agg
on agg.cb_key_household = vw.cb_key_household
        and spot_viewed = 1
-- the rest of the top table is automatically filled



-------------------------
-------------------------
-- IMPACT CHARTS

select top 10 * from molson_coors_impacts

select barb_date_of_transmission
       ,sum(case when spot_duration = 40 then total_impacts*1.333
                 when spot_duration = 50 then total_impacts*1.666
                 when spot_duration = 60 then total_impacts*2
            else total_impacts end) as impacts
       ,sum(case when spot_duration = 40 then brought_audience_impacts*1.333
                 when spot_duration = 50 then brought_audience_impacts*1.666
                 when spot_duration = 60 then brought_audience_impacts*2
            else brought_audience_impacts end) as brought_audience
       ,sum(case when spot_duration = 40 then aspirational_audience_impacts*1.333
                 when spot_duration = 50 then aspirational_audience_impacts*1.666
                 when spot_duration = 60 then aspirational_audience_impacts*2
            else aspirational_audience_impacts end) as aspirational_audience
       ,sum(case when spot_duration = 40 then social_explorers_impacts*1.333
                 when spot_duration = 50 then social_explorers_impacts*1.666
                 when spot_duration = 60 then social_explorers_impacts*2
            else social_explorers_impacts end) as social_explorers
from starmerh.molson_coors_all_spots_impacts
group by barb_date_of_transmission
order by barb_date_of_transmission;



---



-------------------------
-------------------------
-- REACH BY BUCKLE SEGMENTS


select top 10 * from molson_coors_hh_aggregate


-- reach 1+
--buckle segments
select buckle_segments
        ,sum(midday_weightings) as reached
--         ,sum(case when brought_audience = 1 then midday_weightings else null end) as Bought_audience
--         ,sum(case when aspirational_audience = 1 then midday_weightings else null end) as aspirational_audience
--         ,sum(case when social_explorers = 1 then midday_weightings else null end) as target_audience
from starmerh.molson_coors_hh_aggregate
where stella_cidre_exposed >=1
group by buckle_segments
order by buckle_segments


-- brought and aspirational audiences
select -- sum(MIDDAY_WEIGHTINGS) as Sky,
       sum(case when brought_audience = 1 then midday_weightings else null end) as Bought_audience
        ,sum(case when aspirational_audience = 1 then midday_weightings else null end) as aspirational_audience
       -- ,sum(case when social_explorers = 1 then midday_weightings else null end) as target_audience
from starmerh.molson_coors_hh_aggregate
where stella_cidre_exposed >=1




-- reach 3+
-- buckle segments


select  buckle_segments
        ,sum(midday_weightings) as reached
from gillh.molson_coors_all_spots_reach
where spots_viewed >= 3
group by buckle_segments
order by buckle_segments


select sum(midday_weightings) as Sky
        ,sum(case when bought_audience = 1 then midday_weightings else null end) as bought_audience
        ,sum(case when aspirational_audience = 1 then midday_weightings else null end) as aspirational_audience
        ,sum(case when target_audience = 1 then midday_weightings else null end) as target_audience
from gillh.molson_coors_all_spots_reach
where spots_viewed >= 3











----------------------------
--------------- we want to look at the daily increase of reach gained over the campaign period - so we can only include each accout once over the period
----------------------------


select top 10 * from gillh.molson_coors_all_spots_reach


--       ,max(midday_weightings) as  midday_weightings
--        ,target_hh = 0
--        ,proxy_brought_hh = 0
--        ,proxy_brought_only_male = 0


drop table #test

select barb_date_of_transmission
       ,vw.cb_key_household
       ,max(midday_weightings) as  midday_weightings
       ,max(bought_audience) as bought_audience
       ,max(aspirational_audience) as aspirational_audience
       ,max(target_audience) as target_audience

       ,rank () over (partition by vw.cb_key_household order by barb_date_of_transmission) as rank
into #test
from starmerh.molson_coors_allspots_viewing_data_new2 vw
inner join gillh.molson_coors_all_spots_reach as re
 on re.cb_key_household = vw.cb_key_household
where   spot_viewed = 1
        and barb_date_of_transmission >='2012-08-01'
group by barb_date_of_transmission
       ,vw.cb_key_household


delete from #test where rank > 1

select top 10 * from #test

-- lets get the outputs;
select barb_date_of_transmission
       ,sum(case when bought_audience = 1 then midday_weightings else null end) as bought_audience
       ,sum(case when Target_audience = 1 then midday_weightings else null end) as Target_audience
from #test
group by barb_date_of_transmission
order by barb_date_of_transmission






-------------------------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------------------
-- LETS GET THE REACH CHARTS:
-------------------------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------------------

-- SALES HOUSE----------------------------------------------------------

select vw.cb_key_household
        ,SALES_HOUSE
       ,max(midday_weightings) as  midday_weightings
       ,max(bought_audience) as bought_audience
       ,max(aspirational_audience) as aspirational_audience
       ,max(target_audience) as target_audience
into #SALES_HOUSE1
from starmerh.molson_coors_allspots_viewing_data_new vw
inner join gillh.molson_coors_all_spots_reach as re
 on re.cb_key_household = vw.cb_key_household
where spot_viewed = 1
        and barb_date_of_transmission >='2012-08-01'
group by vw.cb_key_household, VW.SALES_HOUSE




-- this is a combination of outputs suited to susannes output sheets
select sales_house
       ,sum(midday_weightings) as Sky_base
       ,sum(case when bought_audience = 1 then midday_weightings else null end) as bought_audience
       ,sum(case when aspirational_audience = 1 then midday_weightings else null end) as aspirational_audience
       ,sum(case when target_audience = 1 then midday_weightings else null end) as target_audience
from #sales_house1
group by sales_house
order by sales_house






-- MEDIA PACK----------------------------------------------------------

drop table #media_pack

select vw.cb_key_household
        ,sales_house
        ,MEDIA_PACK
       ,max(midday_weightings) as  midday_weightings
       ,max(bought_audience) as bought_audience
       ,max(aspirational_audience) as aspirational_audience
       ,max(target_audience) as target_audience
into #MEDIA_PACK
from starmerh.molson_coors_allspots_viewing_data_new vw
inner join gillh.molson_coors_all_spots_reach as re
 on re.cb_key_household = vw.cb_key_household
where  spot_viewed = 1
        and barb_date_of_transmission >='2012-08-01'
group by vw.cb_key_household, VW.Sales_house, VW.MEDIA_PACK




-- this is a combination of outputs suited to susannes output sheets
select sales_house
        ,MEDIA_PACK
       ,sum(midday_weightings) as Sky_base
       ,sum(case when bought_audience = 1 then midday_weightings else null end) as bought_audience
       ,sum(case when aspirational_audience = 1 then midday_weightings else null end) as aspirational_audience
       ,sum(case when target_audience = 1 then midday_weightings else null end) as target_audience
from #MEDIA_PACK
group by sales_house, MEDIA_PACK
order by sales_house, MEDIA_PACK








-- CHANNEL_NEW----------------------------------------------------------

SELECT TOP 10 * FROM molson_coors_allspots_viewing_data_new2

DROP TABLE #CHANNEL_NEW

select vw.cb_key_household
        ,CHANNEL_NEW
        ,MAX(MEDIA_PACK) AS MEDIA_PACK
        ,MAX(SALES_HOUSE) AS SALES_HOUSE
       ,max(midday_weightings) as  midday_weightings
       ,max(bought_audience) as bought_audience
       ,max(aspirational_audience) as aspirational_audience
       ,max(target_audience) as target_audience
into #CHANNEL_NEW
from starmerh.molson_coors_allspots_viewing_data_new2 vw
inner join gillh.molson_coors_all_spots_reach as re
 on re.cb_key_household = vw.cb_key_household
where spot_viewed = 1
        and barb_date_of_transmission >='2012-08-01'
group by vw.cb_key_household, VW.CHANNEL_NEW


-- NEED TO ADD MEDIA PACK, SALES HOUSE AND MORE


-- this is a combination of outputs suited to susannes output sheets
select CHANNEL_NEW
        ,MEDIA_PACK
        ,SALES_HOUSE
       ,sum(midday_weightings) as Sky_base
       ,sum(case when bought_audience = 1 then midday_weightings else null end) as bought_audience
       ,sum(case when aspirational_audience = 1 then midday_weightings else null end) as aspirational_audience
       ,sum(case when target_audience = 1 then midday_weightings else null end) as target_audience
from #CHANNEL_NEW
group by CHANNEL_NEW, MEDIA_PACK, SALES_HOUSE
order by CHANNEL_NEW






-- day part ----------------------------------------------------------

SELECT TOP 10 * FROM molson_coors_allspots_viewing_data_new2

select vw.cb_key_household
        ,(case when datepart(weekday,barb_date_of_transmission)=1 then 'Sun'
                when datepart(weekday,barb_date_of_transmission)=2 then 'Mon'
                when datepart(weekday,barb_date_of_transmission)=3 then 'Tue'
                when datepart(weekday,barb_date_of_transmission)=4 then 'Wed'
                when datepart(weekday,barb_date_of_transmission)=5 then 'Thu'
                when datepart(weekday,barb_date_of_transmission)=6 then 'Fri'
                when datepart(weekday,barb_date_of_transmission)=7 then 'Sat'
        end) as Barb_day

        ,case when convert(varchar(8),utc_spot_start_date_time,108)
                between '06:00:00' and '08:59:59' then 'Breakfast Time'
                when convert(varchar(8),utc_spot_start_date_time,108)
                between '09:00:00' and '17:29:59' then 'Daytime'
                when convert(varchar(8),utc_spot_start_date_time,108)
                between '17:30:00' and '19:59:59' then 'Early Peak'
                when convert(varchar(8),utc_spot_start_date_time,108)
                between '20:00:00' and '22:59:59' then 'Late Peak'
                when convert(varchar(8),utc_spot_start_date_time,108)
                between '23:00:00' and '23:59:59' then 'Post Peak'
                when convert(varchar(8),utc_spot_start_date_time,108)
                between '00:00:00' and '00:29:59' then 'Post Peak'
                when convert(varchar(8),utc_spot_start_date_time,108)
                between '00:30:00' and '05:59:59' then 'Night Time'
           else 'Unknown'
           end as daypart

       ,max(midday_weightings) as  midday_weightings
       ,max(bought_audience) as bought_audience
       ,max(aspirational_audience) as aspirational_audience
       ,max(target_audience) as target_audience
into #DAY_PART
from starmerh.molson_coors_allspots_viewing_data_new2 vw
inner join gillh.molson_coors_all_spots_reach as re
 on re.cb_key_household = vw.cb_key_household
where  spot_viewed = 1
        and barb_date_of_transmission >='2012-08-01'
group by vw.cb_key_household, BARB_DAY, DAYPART




-- this is a combination of outputs suited to susannes output sheets
select BARB_DAY, DAYPART
       ,sum(midday_weightings) as Sky_base
       ,sum(case when bought_audience = 1 then midday_weightings else null end) as bought_audience
       ,sum(case when aspirational_audience = 1 then midday_weightings else null end) as aspirational_audience
       ,sum(case when target_audience = 1 then midday_weightings else null end) as target_audience
from #DAY_PART
group by BARB_DAY, DAYPART
order by BARB_DAY, DAYPART










-- GENRE----------------------------------------------------------

SELECT TOP 10 * FROM molson_coors_allspots_viewing_data_new2

select vw.cb_key_household
        ,GENRE_DESCRIPTION AS GENRE
       ,max(midday_weightings) as  midday_weightings
       ,max(bought_audience) as bought_audience
       ,max(aspirational_audience) as aspirational_audience
       ,max(target_audience) as target_audience
into #GENRE
from starmerh.molson_coors_allspots_viewing_data_new2 vw
inner join gillh.molson_coors_all_spots_reach as re
 on re.cb_key_household = vw.cb_key_household
where spot_viewed = 1
        and barb_date_of_transmission >='2012-08-01'
group by vw.cb_key_household, VW.GENRE_DESCRIPTION




-- this is a combination of outputs suited to susannes output sheets
select GENRE
       ,sum(midday_weightings) as Sky_base
       ,sum(case when bought_audience = 1 then midday_weightings else null end) as bought_audience
       ,sum(case when aspirational_audience = 1 then midday_weightings else null end) as aspirational_audience
       ,sum(case when target_audience = 1 then midday_weightings else null end) as target_audience
from #GENRE
group by GENRE
order by GENRE






--SUB_GENRE----------------------------------------------------------

SELECT TOP 10 * FROM molson_coors_allspots_viewing_data_new2

select vw.cb_key_household
        ,GENRE_DESCRIPTION AS GENRE
        ,SUB_GENRE_DESCRIPTION AS SUB_GENRE
       ,max(midday_weightings) as  midday_weightings
       ,max(bought_audience) as bought_audience
       ,max(aspirational_audience) as aspirational_audience
       ,max(target_audience) as target_audience
into #SUB_GENRE
from starmerh.molson_coors_allspots_viewing_data_new2 vw
inner join gillh.molson_coors_all_spots_reach as re
 on re.cb_key_household = vw.cb_key_household
where spot_viewed = 1
        and barb_date_of_transmission >='2012-08-01'
group by vw.cb_key_household, VW.GENRE_DESCRIPTION, VW.SUB_GENRE_DESCRIPTION




-- this is a combination of outputs suited to susannes output sheets
select GENRE, SUB_GENRE
       ,sum(midday_weightings) as Sky_base
       ,sum(case when bought_audience = 1 then midday_weightings else null end) as bought_audience
       ,sum(case when aspirational_audience = 1 then midday_weightings else null end) as aspirational_audience
       ,sum(case when target_audience = 1 then midday_weightings else null end) as target_audience
from #SUB_GENRE
group by GENRE, SUB_GENRE
order by GENRE, SUB_GENRE









--DAYPART----------------------------------------------------------

SELECT TOP 10 * FROM molson_coors_allspots_viewing_data_new2

select vw.cb_key_household
       ,PROGRAMME_NAME
       ,CHANNEL_NEW
       ,media_pack
       ,sales_house
       ,(case when datepart(weekday,barb_date_of_transmission)=1 then 'Sun'
                when datepart(weekday,barb_date_of_transmission)=2 then 'Mon'
                when datepart(weekday,barb_date_of_transmission)=3 then 'Tue'
                when datepart(weekday,barb_date_of_transmission)=4 then 'Wed'
                when datepart(weekday,barb_date_of_transmission)=5 then 'Thu'
                when datepart(weekday,barb_date_of_transmission)=6 then 'Fri'
                when datepart(weekday,barb_date_of_transmission)=7 then 'Sat'
        end) as Barb_day

       ,max(midday_weightings) as  midday_weightings
       ,max(bought_audience) as bought_audience
       ,max(aspirational_audience) as aspirational_audience
       ,max(target_audience) as target_audience
into #PROGRAM
from starmerh.molson_coors_allspots_viewing_data_new2 vw
inner join gillh.molson_coors_all_spots_reach as re
 on re.cb_key_household = vw.cb_key_household
where spot_viewed = 1
        and barb_date_of_transmission >='2012-08-01'
group by vw.cb_key_household
         ,PROGRAMME_NAME
       ,CHANNEL_NEW
       ,media_pack
       ,sales_house
       ,BARB_DAY




-- this is a combination of outputs suited to susannes output sheets
select PROGRAMME_NAME
       ,CHANNEL_NEW
       ,media_pack
       ,sales_house
       ,BARB_DAY
       ,sum(midday_weightings) as Sky_base
       ,sum(case when bought_audience = 1 then midday_weightings else null end) as bought_audience
       ,sum(case when aspirational_audience = 1 then midday_weightings else null end) as aspirational_audience
       ,sum(case when target_audience = 1 then midday_weightings else null end) as target_audience
from #PROGRAM
group by PROGRAMME_NAME
       ,CHANNEL_NEW
       ,media_pack
       ,sales_house
       ,BARB_DAY
order by PROGRAMME_NAME
       ,CHANNEL_NEW
       ,media_pack
       ,sales_house
       ,BARB_DAY


-----------------------------------------------------------------
-- End of Code --

























select top 10 * from molson_coors_viewing_data




-- we need a view on the number of imapacts by each audience --




select top 10 * from molson_coors_hh_aggregate where midday_weightings is not null



select top 10 * from molson_coors_viewing_data

select distinct(stella_cidre_exposed) from molson_coors_hh_aggregate


select top 10 * from molson_coors_hh_aggregate




select sum(midday_weightings))


select cb_key_household
from molson_coors_watched_TV
 where cb_key_household in

 (select distinct(cb_key_household) from molson_coors_viewing_data where clearcast_commercial_no in ('MUMSTCI009040','MUMSTCI014030'))


 and cb_key_household in (select cb_key_household from molson_coors_hh_aggregate where social_explorers = 1))





select top 10 * from molson_coors_hh_aggregate


select top 10 * from molson_coors_viewing_data

select top 10





SELECT SUM(MIDDAY_WEIGHTINGS) FROM starmerh.molson_coors_hh_aggregate












grant select on molson_coors_impacts  to public
grant select on molson_coors_hh_aggregate to public
grant select on molson_coors_viewing_data to public
grant select on molson_coors_spot_data to public

grant select on molson_coors_hh_impacts to public
grant select on molson_coors_hh_exp_impacts to public
grant select on molson_coors_Program_details to public
grant select on molson_spots2 to public



















































