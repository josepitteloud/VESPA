
-------------------------------------IMPACTS BY ALL SPOTS (Barclays universe for 1 week 29th feb to 6th Mar)

--get the Mid PIB spot from ALL spots in 1 week period

drop table BARC_all_spots_1week

select *
into BARC_all_spots_1week
from all_spots_2weeks_108_NEW
where barb_date_of_transmission between '2012-02-29' and '2012-03-06'
;
--909911 Row(s) affected



-- also need to identify the whole spots --
-- add the needed fields to the vieiwng table!
alter table gillh.Project_108_viewing_table_dump_2weeks
        add (Whole_spot integer default 0
           ,spot_identifier integer);


Update gillh.Project_108_viewing_table_dump_2weeks
        set vw.Whole_spot = case when (vw.recorded_time_utc < vw.utc_spot_start_date_time
                                 and  dateadd(second,vw.viewing_duration,vw.recorded_time_utc)> vw.utc_spot_end_date_time)

                                 OR (timeshifting = 'LIVE' and vw.viewing_starts < vw.utc_spot_start_date_time
                                 and  viewing_stops> vw.utc_spot_end_date_time)

                                 then 1 else 0 end
            ,vw.spot_identifier = spot.identifier
from gillh.Project_108_viewing_table_dump_2weeks  vw
join BARC_all_spots_1week      spot
on   vw.utc_spot_start_date_time    = spot.utc_spot_start_date_time
and  vw.utc_break_start_date_time = spot.utc_break_start_date_time
and  vw.service_key = spot.service_key
and  vw.vespa_name = spot.vespa_name
;
commit;

select top 10 * from Project_108_viewing_table_dump_2weeks

select count(distinct(spot_identifier)) from Project_108_viewing_table_dump_2weeks

---------------------------------now we need to sum up the amount of viewing per spot




--- martin would like a detailed break down of each spot;

drop table #spot_identifier

select  bar.account_number
        ,bar.cb_key_household
        ,bar.spot_identifier
        ,impacts = COUNT(case when whole_spot = 1 then 1 else null end)
        ,weighted_impacts = impacts*weighting
        ,max(barclays_customer) as barclays_customer
        ,max(barclays_responder) as responder
        ,max(bar.weighting)as weighting
        ,max(ves.aspiration_target) as aspiration_target
        ,max(ves.response_target) as response_target
        ,max(response_target_and_unknown) as response_target_and_unknown
        ,max(ABC1_TARGET) as ABC1_TARGET
    --    ,max(barclays_customer) as barclays_customer
into #spot_identifier
from gillh.Project_108_viewing_table_dump_2weeks bar                              --- put the all spots viewing table here
right join gillh.v081_Vespa_Universe_demographics ves
on ves.account_number = bar.account_number
where broadcast_date between '2012-02-29' and '2012-03-06'
group by bar.account_number, bar.cb_key_household, bar.spot_identifier
;


delete from #spot_identifier where spot_identifier not in (select identifier from BARC_all_spots_1week)

select count(*) from #spot_identifier


-- lets get a roll up for each spot; who watched??

drop table #spot_impacts

select spot_identifier
        ,sum(weighting) as Sky_base
        ,sum(case when ABC1_TARGET = 1 then weighted_impacts else null end) as ABC1_Sky
        ,sum(case when ABC1_TARGET = 1 and barclays_customer = 1 then weighted_impacts else null end) as ABC1_Sky_Barclays_target
        ,sum(case when ABC1_TARGET = 1 and responder = 1 then weighted_impacts else null end) as ABC1_responders
        ,sum(case when responder = 1 then weighted_impacts else null end) as all_responders
        ,sum(case when aspiration_target = 1 then weighted_impacts else null end) as aspiration_target
into #spot_impacts
from #spot_identifier
group by spot_identifier
order by spot_identifier
--24,965

select top 10 * from #spot_impacts

select count(*) from #spot_impacts


-- THE ABOVE IS WORKING FINE!


-- OK NOW WE NEED TO ADD MEDIA PAPCK TO THE BARCLAYS SPOTS:


-- now lets put the media pack into the cube.
alter table BARC_all_spots_1week
        add media_pack varchar(25);


update BARC_all_spots_1week
        set cub.media_pack = tmp.channel_category
from BARC_all_spots_1week   as cub
join gillh.LkUpPack as tmp
on tmp.service_key = cub.service_key


select top 10 * from BARC_all_spots_1week



-- we also need to add sales house

-- we need to add the sales house to the barclays spots table as there is nothing to match to in the vieiwng table
alter table BARC_all_spots_1week
        add sales_house varchar(25);

update BARC_all_spots_1week
        set spot.sales_house = chg.primary_sales_house
from BARC_all_spots_1week spot
inner join neighbom.channel_map_dev_barb_channel_group chg
on spot.log_station_code = chg.log_station_code

-- now lets put everything together

-- NOW LETS ADD AN AGGREGATED CHANNEL NAME



-- finally add the cleaned channel names to the main vieiwng table.
alter table BARC_all_spots_1week
        add agg_channel_name varchar(50);


update BARC_all_spots_1week
        set agg_channel_name = channel
from BARC_all_spots_1week vw
join lkupchannel lk
on vw.service_key = lk.service_key






select spot.identifier
        , local_date_of_transmission as air_date
        , spot.sales_house
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
        ,aspiration_target
into all_spots_details2
FROM BARC_all_spots_1week   spot
LEFT JOIN #spot_impacts imp
ON spot_identifier = IDENTIFIER

SELECT TOP 10 * FROM all_spots_details2


alter table all_spots_details2
add  epg_title varchar(50)



------------------------------------------------
-- the update above gives a poor match -- lets get a better measure of the genre




SELECT IDENTIFIER
        ,GENRE_DESCRIPTION
        ,SUB_GENRE_DESCRIPTION
        ,epg_title
into #spot_genre2
 FROM VESPA_Programmes_project_108 PRO
JOIN BARC_all_spots_1week AS SPOT
on SPOT.SERVICE_KEY = PRO.SERVICE_KEY
and utc_spot_start_date_time between tx_start_datetime_utc and tx_end_datetime_utc



update all_spots_details2
set spot.genre = case when gen.GENRE_DESCRIPTION is not null then GENRE_DESCRIPTION else genre end
        ,spot.sub_genre = case when gen.SUB_GENRE_DESCRIPTION is not null then SUB_GENRE_DESCRIPTION else sub_genre end
        ,spot.epg_title = gen.epg_title
from all_spots_details2 spot
left join  #spot_genre2 gen
on gen.identifier = spot.identifier



update all_spots_details2
set genre = (case when genre = 'This is a genre placeholder' then 'Not Matched' else genre end)

update all_spots_details2
set sub_genre = (case when sub_genre = 'sub genre placeholder' then 'Not Matched' else sub_genre end)




---------------------

-- lets now add the local spot start time and the day part

alter table all_spots_details2
add (local_spot_start_time TIME
    ,think_box_day_part varchar(12));

-- lets updatye the spot start time
UPDATE all_spots_details2
 SET  local_spot_start_time = cast(local_spot_start_date_time as time)
 FROM all_spots_details2 as det
 left join BARC_all_spots_1week as spot
      on det.identifier = spot.identifier

-- lets add Martins new definition of the day part:
update all_spots_details2
        set think_box_day_part = (case when local_spot_start_time between '06:00:00' and '08:59:59' then 'Breakfast'
                when local_spot_start_time between '09:00:00' and '17:29:59' then 'Daytime'
                when local_spot_start_time between '17:30:00' and '19:59:59' then 'Early Peak'
                when local_spot_start_time between '20:00:00' and '22:59:59' then 'Late Peak'
                when local_spot_start_time between '23:00:00' and '24:29:59' then 'Post Peak'
                when local_spot_start_time between '24:30:00' and '05:59:59' then 'Night Time'
                else 'Unkown' end)

select top 100 * from all_spots_details2



select top 10 * from BARC_all_spots_1week







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
        ,aspiration_target
from all_spots_details2



-- select top 10 * from Barclays_spots_details
-- select distinct(genre), count(*) from Barclays_spots_details group by genre
--



