
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


drop table spot_viewing

select spot_identifier
        ,sum(case when whole_spot = 1 then bar.weighting else null end) as impacts_all
        ,sum(case when whole_spot = 1 and social_grade2 in ('A','B','C1')  then bar.weighting else null end) as impacts_abc1
        ,sum(case when whole_spot = 1 and social_grade2 in ('A','B','C1') and barclays_customer = 1 then bar.weighting else null end) as impacts_abc1_barclays_sky
        ,sum(case when whole_spot = 1 and social_grade2 in ('A','B','C1') and responder = 1 then bar.weighting else null end) as impacts_abc1_sky_responder
        ,sum(case when whole_spot = 1 and responder = 1 then bar.weighting else null end) as impacts_sky_responder
        ,sum(case when whole_spot = 1 and responder = 1 and fss_v3_group <> 'Unknown Sky' then bar.weighting else null end) as impacts_sky_responder_less_unknowns
        ,sum(case when whole_spot = 1 and aspiration_target = 1 then bar.weighting else null end) as impacts_aspirational_target
into spot_viewing
from Barclays_spots_viewing_table_dump BAR
join v081_Vespa_Universe_demographics ves
on ves.cb_key_household = bar.cb_key_household
group by spot_identifier


-- lets check that this worked!
select top 10 * from spot_viewing order by spot_identifier

select count(*) from neighbom.barclays_spot_list -- 20k



-- lets make a new file that we can play with;
if object_id('barclays_spots_viewing') is not null drop table barclays_spots_viewing

select *
into barclays_spots_viewing
from barclays_spots



alter table barclays_spots_viewing
 add (Sky_impacts integer
        ,Sky_ABC1_impacts integer
        ,sky_barclays_abc1_impacts integer
        ,sky_responder_abc1 integer
        ,sky_responder integer
        ,sky_responder_less_unknowns integer
        ,Sky_aspirational_target integer
        ,barb_HOUSHEOLDS integer
        ,BARB_ABC1_HOUSEHOLD integer
        ,Matched_to_barb_file integer)


-- lets add the vespa viewing figures :
 update barclays_spots_viewing
        set Sky_impacts = impacts_all
                , Sky_ABC1_impacts = impacts_abc1
                , sky_barclays_abc1_impacts = impacts_abc1_barclays_sky
                , sky_responder_abc1 = impacts_abc1_sky_responder
                , sky_responder = impacts_sky_responder
                , sky_responder_less_unknowns = impacts_sky_responder_less_unknowns
                , Sky_aspirational_target = impacts_aspirational_target
 from barclays_spots_viewing
 left join spot_viewing
 on spot_identifier = identifier


select top 10 * from barclays_spots_viewing

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



drop table #table

 select log_station_code
        ,sti_code
        ,utc_spot_start_date_time
        ,max(Matched_to_barb_file) as Matched_to_barb_file
        ,sum(barb_HOUSHEOLDS) as barb_HOUSHEOLDS
        ,sum(BARB_ABC1_HOUSEHOLD) as BARB_ABC1_HOUSEHOLD
        ,sum(Sky_impacts) as Sky_impacts
        ,sum(Sky_ABC1_impacts) as Sky_ABC1_impacts
        ,sum(sky_barclays_abc1_impacts) as sky_barclays_abc1_impacts
        ,sum(sky_responder_abc1) as sky_responder_abc1
        ,sum(sky_responder) as sky_responder
        ,sum(sky_responder_less_unknowns) as sky_responder_less_unknowns
        ,sum(Sky_aspirational_target) as Sky_aspirational_target
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



select  log_station_code
        ,sti_code
        ,cast(utc_spot_start_date_time as date) as date_aired
        ,cast(utc_spot_start_date_time as time) as time_aired
        ,(case
                when datepart(weekday,utc_spot_start_date_time)=1 then 'Sunday'
                when datepart(weekday,utc_spot_start_date_time)=2 then 'Monday'
                when datepart(weekday,utc_spot_start_date_time)=3 then 'Tuesday'
                when datepart(weekday,utc_spot_start_date_time)=4 then 'Wednesday'
                when datepart(weekday,utc_spot_start_date_time)=5 then 'Thursday'
                when datepart(weekday,utc_spot_start_date_time)=6 then 'Friday'
                when datepart(weekday,utc_spot_start_date_time)=7 then 'Saturday'
        end) as day_aired
        ,think_box_day_part = (case when time_aired between '06:00:00' and '08:59:59' then 'Breakfast'
                when time_aired between '09:00:00' and '17:29:59' then 'Daytime'
                when time_aired between '17:30:00' and '19:59:59' then 'Early Peak'
                when time_aired between '20:00:00' and '22:59:59' then 'Late Peak'
                when time_aired between '23:00:00' and '24:29:59' then 'Post Peak'
                when time_aired between '24:30:00' and '05:59:59' then 'Night Time'
                else 'Unkown' end)

        ,Matched_to_barb_file
        ,barb_HOUSHEOLDS
        ,BARB_ABC1_HOUSEHOLD
        ,Sky_impacts
        ,Sky_ABC1_impacts
        ,sky_barclays_abc1_impacts
        ,sky_responder_abc1
        ,sky_responder
        ,sky_responder_less_unknowns
        ,Sky_aspirational_target
 from #table order by utc_spot_start_date_time


grant select on Barclays_spots_details to public;
grant select on barclays_spots_viewing to public;
grant select on v081_Vespa_Universe_demographics to public;
grant select on Barclays_spots_viewing_table_dump to public;


