


-------------------------------------------------------
----- THIS IS PHASE IV BARCLAYS PROJECT; THE ROLL UPS 2.A AND 2.B for ALL SPOTS!
-------------------------------------------------------


select top 10 * from Project_108_viewing_table_dump_2weeks

select count(*) from Project_108_viewing_table_dump_2weeks

select top 10 * from all_spots_2weeks_108_NEW



-- lets add this to the viewing table as it will be needed later on:
alter table Project_108_viewing_table_dump_2weeks
        add ( responder integer default 0
             ,Whole_spot integer default 0
             ,weighting float
             ,spot_identifier bigint);
                                                        -- there may be some non matches which we then assume to have not responded

update Project_108_viewing_table_dump_2weeks --
        set responder = 1
from Project_108_viewing_table_dump_2weeks as ves
join OM114_BARCLAYS_RESPONSE as bar
on bar.cb_key_household = ves.cb_key_household


--  Flag whole spots watched!
Update Project_108_viewing_table_dump_2weeks vw
        set vw.Whole_spot = case when (vw.recorded_time_utc < vw.utc_spot_start_date_time
                                 and  dateadd(second,vw.viewing_duration,vw.recorded_time_utc)> vw.utc_spot_end_date_time)

                                 OR (timeshifting = 'LIVE' and vw.viewing_starts < vw.utc_spot_start_date_time
                                 and  viewing_stops> vw.utc_spot_end_date_time)

                                 then 1 else 0 end
            ,vw.spot_identifier = spot.identifier

from Project_108_viewing_table_dump_2weeks  vw
join all_spots_2weeks_108_NEW       spot
on   vw.utc_spot_start_date_time    = spot.utc_spot_start_date_time
and  vw.utc_break_start_date_time = spot.utc_break_start_date_time
and  vw.service_key = spot.service_key
and  vw.vespa_name = spot.vespa_name
-- are indexed?

select top 10 * from Project_108_viewing_table_dump_2weeks



-- lets add the weighting to the barclays spots table - not most efficient way of doing this
-- but added post production given a problem with the sum of weightings in the

alter table Project_108_viewing_table_dump_2weeks
        add (weighting float)


update Project_108_viewing_table_dump_2weeks
 set bar.weighting = sca.weightings
from Project_108_viewing_table_dump_2weeks bar
join table_for_scaling sca
on bar.account_number = sca.account_number




------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
                        ---     TEMPLATE 3      --
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

-- SPREADSHEET 2.A.

---------------------
-- OUTPUT: responders vs sales house -- also need impacts
---------------------


-- now we need to add the sales house to the viewing table -- log_station_code
alter table Project_108_viewing_table_dump_2weeks
        add sales_house varchar(10)

update Project_108_viewing_table_dump_2weeks
        set bar.sales_house = spot.sales_house
from Project_108_viewing_table_dump_2weeks bar
 join all_spots_2weeks_108_NEW spot
        on spot_identifier = SPOT.identifier


        -- check it
 -- select distinct(sales_house), count(*) from Project_108_viewing_table_dump_2weeks group by sales_house


drop table #saleshouse_responders2

select   bar.cb_key_household
        ,sales_house
        ,impacts = COUNT(case when whole_spot = 1 then 1 else null end)
        ,max(responder) as responder
        ,max(bar.weighting)as weighting
        ,max(ves.aspiration_target) as aspiration_target
        ,max(ves.response_target) as response_target
        ,max(ves.response_target_and_unknown) as response_target_and_unknown
into #saleshouse_responders2
from Project_108_viewing_table_dump_2weeks bar
right join v081_Vespa_Universe_demographics ves
on ves.account_number = bar.account_number
group by bar.cb_key_household, bar.sales_house





-- All in one output table - 3x target groups and the Sky base

select sales_house
         ,sum(case when aspiration_target = 1 then weighting else null end) as aspiration_target
         ,(Sky_base - aspiration_target) as non_aspiration_target

         ,sum(case when response_target = 1 then weighting else null end) as response_target
         ,(Sky_base - response_target) as non_response_target

         ,sum(case when response_target_and_unknown = 1 then weighting else null end) as response_target_and_unknown
         ,(Sky_base - response_target_and_unknown) as non_response_target_and_unknown

        ,Sky_base = sum(weighting)
from #saleshouse_responders2
group by sales_house
order by aspiration_target desc







---------------------
-- OUTPUT:  MEDIA PACK -- also need impacts
---------------------


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



-- now lets put the media pack into the cube.
alter table Project_108_viewing_table_dump_2weeks
        add media_pack varchar(25);


update Project_108_viewing_table_dump_2weeks
        set cub.media_pack = tmp.channel_category
from Project_108_viewing_table_dump_2weeks as cub
join LkUpPack as tmp
on tmp.service_key = cub.service_key


select  bar.account_number
        ,bar.cb_key_household
        ,MEDIA_PACK
        ,impacts = COUNT(case when whole_spot = 1 then 1 else null end)
        ,max(responder) as responder
        ,max(bar.weighting)as weighting
        ,max(ves.aspiration_target) as aspiration_target
        ,max(ves.response_target) as response_target
        ,max(ves.response_target_and_unknown) as response_target_and_unknown
into #MEDIA_PACK2
from Project_108_viewing_table_dump_2weeks bar
right join v081_Vespa_Universe_demographics ves
on ves.account_number = bar.account_number
group by bar.account_number, bar.cb_key_household, bar.media_pack

-- select top 10 * from #media_pack2



-- All in one output table - 3x target groups and the Sky base

select media_pack
         ,sum(case when aspiration_target = 1 then weighting else null end) as aspiration_target
         ,(Sky_base - aspiration_target) as non_aspiration_target

         ,sum(case when response_target = 1 then weighting else null end) as response_target
         ,(Sky_base - response_target) as non_response_target

         ,sum(case when response_target_and_unknown = 1 then weighting else null end) as response_target_and_unknown
         ,(Sky_base - response_target_and_unknown) as non_response_target_and_unknown

        ,Sky_base = sum(weighting)
from #media_pack2
group by media_pack
order by aspiration_target desc


---------------------
-- OUTPUT: responders vs MEDIA PACK -- also need impacts
---------------------




select  bar.account_number
        ,bar.cb_key_household
        ,agg_channel_name as channel_name
        ,impacts = COUNT(case when whole_spot = 1 then 1 else null end)
        ,max(responder) as responder
        ,max(bar.weighting)as weighting
        ,max(ves.aspiration_target) as aspiration_target
        ,max(ves.response_target) as response_target
        ,max(ves.response_target_and_unknown) as response_target_and_unknown
into #channel2
from Project_108_viewing_table_dump_2weeks bar
right join v081_Vespa_Universe_demographics ves
on ves.account_number = bar.account_number
group by bar.account_number, bar.cb_key_household, bar.agg_channel_name



-- All in one output table - 3x target groups and the Sky base

select channel_name
         ,sum(case when aspiration_target = 1 then weighting else null end) as aspiration_target
         ,(Sky_base - aspiration_target) as non_aspiration_target

         ,sum(case when response_target = 1 then weighting else null end) as response_target
         ,(Sky_base - response_target) as non_response_target

         ,sum(case when response_target_and_unknown = 1 then weighting else null end) as response_target_and_unknown
         ,(Sky_base - response_target_and_unknown) as non_response_target_and_unknown

        ,Sky_base = sum(weighting)
from #channel2
group by channel_name
order by aspiration_target desc



---------------------
-- OUTPUT: DayPart
---------------------


select   bar.cb_key_household
        ,x_viewing_time_of_day as day_part
        ,impacts = COUNT(case when whole_spot = 1 then 1 else null end)
        ,max(responder) as responder
        ,max(bar.weighting)as weighting
        ,max(ves.aspiration_target) as aspiration_target
        ,max(ves.response_target) as response_target
        ,max(ves.response_target_and_unknown) as response_target_and_unknown
into #day_part2
from Project_108_viewing_table_dump_2weeks bar
right join v081_Vespa_Universe_demographics ves
on ves.account_number = bar.account_number
group by  bar.cb_key_household, bar.x_viewing_time_of_day




-- All in one output table - 3x target groups and the Sky base

select day_part
         ,sum(case when aspiration_target = 1 then weighting else null end) as aspiration_target
         ,(Sky_base - aspiration_target) as non_aspiration_target

         ,sum(case when response_target = 1 then weighting else null end) as response_target
         ,(Sky_base - response_target) as non_response_target

         ,sum(case when response_target_and_unknown = 1 then weighting else null end) as response_target_and_unknown
         ,(Sky_base - response_target_and_unknown) as non_response_target_and_unknown

        ,Sky_base = sum(weighting)
from #day_part2
group by day_part
order by aspiration_target desc


---------------------
-- OUTPUT: Genre
---------------------


select  bar.cb_key_household
        ,genre_description as genre
   --     ,sub_genre_description as sub_genre
        ,impacts = COUNT(case when whole_spot = 1 then 1 else null end)
        ,max(responder) as responder
        ,max(bar.weighting)as weighting
        ,max(ves.aspiration_target) as aspiration_target
        ,max(ves.response_target) as response_target
        ,max(ves.response_target_and_unknown) as response_target_and_unknown
into #genres2
from Project_108_viewing_table_dump_2weeks bar
right join v081_Vespa_Universe_demographics ves
on ves.account_number = bar.account_number
group by  bar.cb_key_household, genre




-- 1st lets get the genres
-- All in one output table - 3x target groups and the Sky base

select genre
         ,sum(case when aspiration_target = 1 then weighting else null end) as aspiration_target
         ,(Sky_base - aspiration_target) as non_aspiration_target

         ,sum(case when response_target = 1 then weighting else null end) as response_target
         ,(Sky_base - response_target) as non_response_target

         ,sum(case when response_target_and_unknown = 1 then weighting else null end) as response_target_and_unknown
         ,(Sky_base - response_target_and_unknown) as non_response_target_and_unknown

        ,Sky_base = sum(weighting)
from #genres2
group by genre
order by aspiration_target desc




-- next; lets get the sub genres

select  bar.cb_key_household
        ,genre_description as genre
       ,sub_genre_description as sub_genre
        ,impacts = COUNT(case when whole_spot = 1 then 1 else null end)
        ,max(responder) as responder
        ,max(bar.weighting)as weighting
        ,max(ves.aspiration_target) as aspiration_target
        ,max(ves.response_target) as response_target
        ,max(ves.response_target_and_unknown) as response_target_and_unknown
into #sub_genres2
from Project_108_viewing_table_dump_2weeks bar
right join v081_Vespa_Universe_demographics ves
on ves.account_number = bar.account_number
group by  bar.cb_key_household, genre, sub_genre




-- All in one output table - 3x target groups and the Sky base

select genre,sub_genre
         ,sum(case when aspiration_target = 1 then weighting else null end) as aspiration_target
         ,(Sky_base - aspiration_target) as non_aspiration_target

         ,sum(case when response_target = 1 then weighting else null end) as response_target
         ,(Sky_base - response_target) as non_response_target

         ,sum(case when response_target_and_unknown = 1 then weighting else null end) as response_target_and_unknown
         ,(Sky_base - response_target_and_unknown) as non_response_target_and_unknown

        ,Sky_base = sum(weighting)
from #sub_genres2
group by genre, sub_genre
order by aspiration_target desc








---------------------
-- OUTPUT: DAY OF THE WEEK
---------------------


select  bar.cb_key_household
        ,(case when viewing_date in ('2012-03-05','2012-03-12','2012-03-19','2012-03-26','2012-04-02','2012-04-09','2012-04-16') then 'Monday'
        when viewing_date in ('2012-03-06','2012-03-13','2012-03-20','2012-03-27','2012-04-03','2012-04-10','2012-04-17') then 'Tuesday'
        when viewing_date in ('2012-02-29','2012-03-07','2012-03-14','2012-03-21','2012-03-28','2012-04-04','2012-04-11','2012-04-18') then 'Wednesday'
        when viewing_date in ('2012-03-01','2012-03-08','2012-03-15','2012-03-22','2012-03-29','2012-04-05','2012-04-12','2012-04-19') then 'Thursday'
        when viewing_date in ('2012-03-02','2012-03-09','2012-03-16','2012-03-23','2012-03-30','2012-04-06','2012-04-13','2012-04-20') then 'Friday'
        when viewing_date in ('2012-03-03','2012-03-10','2012-03-17','2012-03-24','2012-03-31','2012-04-07','2012-04-14','2012-04-21') then 'Saturday'
        when viewing_date in ('2012-03-04','2012-03-11','2012-03-18','2012-03-25','2012-04-01','2012-04-08','2012-04-15') then 'Sunday'
        else null end) as day_of_week

        ,impacts = COUNT(case when whole_spot = 1 then 1 else null end)
        ,max(responder) as responder
        ,max(bar.weighting)as weighting
        ,max(ves.aspiration_target) as aspiration_target
        ,max(ves.response_target) as response_target
        ,max(ves.response_target_and_unknown) as response_target_and_unknown
into #day_of_week2
from Project_108_viewing_table_dump_2weeks bar
right join v081_Vespa_Universe_demographics ves
on ves.account_number = bar.account_number
group by  bar.cb_key_household, day_of_week



-- All in one output table - 3x target groups and the Sky base

select day_of_week
         ,sum(case when aspiration_target = 1 then weighting else null end) as aspiration_target
         ,(Sky_base - aspiration_target) as non_aspiration_target

         ,sum(case when response_target = 1 then weighting else null end) as response_target
         ,(Sky_base - response_target) as non_response_target

         ,sum(case when response_target_and_unknown = 1 then weighting else null end) as response_target_and_unknown
         ,(Sky_base - response_target_and_unknown) as non_response_target_and_unknown

        ,Sky_base = sum(weighting)
from #day_of_week2
group by day_of_week
order by aspiration_target desc






---------------------
-- OUTPUT: programme
---------------------


select  --bar.account_number,
        bar.cb_key_household
        ,epg_title as programme
        ,impacts = COUNT(case when whole_spot = 1 then 1 else null end)
        ,max(responder) as responder
        ,max(bar.weighting)as weighting
        ,max(ves.aspiration_target) as aspiration_target
        ,max(ves.response_target) as response_target
        ,max(ves.response_target_and_unknown) as response_target_and_unknown
into #programme2
from Project_108_viewing_table_dump_2weeks bar
right join v081_Vespa_Universe_demographics ves
on ves.account_number = bar.account_number
group by  bar.cb_key_household, programme



-- 1st lets get the program split.
-- All in one output table - 3x target groups and the Sky base

select programme
         ,sum(case when aspiration_target = 1 then weighting else null end) as aspiration_target
         ,(Sky_base - aspiration_target) as non_aspiration_target

         ,sum(case when response_target = 1 then weighting else null end) as response_target
         ,(Sky_base - response_target) as non_response_target

         ,sum(case when response_target_and_unknown = 1 then weighting else null end) as response_target_and_unknown
         ,(Sky_base - response_target_and_unknown) as non_response_target_and_unknown

        ,Sky_base = sum(weighting)
from #programme2
group by programme
order by aspiration_target desc








