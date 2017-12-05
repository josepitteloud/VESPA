


-------------------------------------------------------
----- THIS IS PHASE II BARCLAYS PROJECT; THE ROLL UPS
-------------------------------------------------------



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


-- lets add the weighting to the barclays spots table - not most efficient way of doing this
-- but added post production given a problem with the sum of weightings in the
alter table Barclays_spots_viewing_table_dump
add weighting float

update Barclays_spots_viewing_table_dump
 set bar.weighting = sca.weightings
from Barclays_spots_viewing_table_dump bar
join table_for_scaling sca
on bar.account_number = sca.account_number



ALTER TABLE v081_Vespa_Universe_demographics
add (ABC1_TARGET INTEGER) -- this including unknowns



ALTER TABLE v081_Vespa_Universe_demographics
add (aspiration_target integer
    ,response_target integer
    ,response_target_and_unknown integer
    ,ABC1_TARGET integer) -- this including unknowns


-- they dont seem to run together
UPDATE v081_Vespa_Universe_demographics
        set aspiration_target = (case when fss_v3_group in ('Traditional Thrift','Sunset Security','Single Endeavours','Growing Rewards'
                                                          ,'Family Interest') then 1 else 0 end)

UPDATE v081_Vespa_Universe_demographics
        set response_target = (case when fss_v3_group in ('Traditional Thrift','Sunset Security','Single Endeavours') then 1 else 0 end)


UPDATE v081_Vespa_Universe_demographics
        set response_target_and_unknown = (case when fss_v3_group in ('Traditional Thrift','Sunset Security','Single Endeavours','Unknown Sky') then 1 else 0 end)

UPDATE v081_Vespa_Universe_demographics
        set ABC1_TARGET = (case when social_grade2 in ('A','B','C1') then 1 else 0 end)


--

-- lets get the number of impacts per household - including impacts after a the sale


select  --bar.account_number,
        bar.cb_key_household
        ,impacts_total = COUNT(case when whole_spot = 1 then 1 else null end)
        ,impacts_before_application = COUNT(case when whole_spot = 1 and viewing_date <= min_application_date then 1 else null end)
        ,max(barclays_responder) as barclays_responder -- household level
into #temp2
from Barclays_spots_viewing_table_dump bar
join v081_Vespa_Universe_demographics ves
on ves.cb_key_household = bar.cb_key_household -- lets match at household
group by --bar.account_number,
 bar.cb_key_household                       -- viewing data is at account level


-- now lets add this to the vespa demographics table:

alter table v081_Vespa_Universe_demographics
        add (impacts_total integer default 0
           ,impacts_before_application integer default 0);      -- this will put any null accounts (i.e. not watched) into the 0 spots watch cells
                -- did not add scaled impacts - this is not needed yet


update v081_Vespa_Universe_demographics
        set ves.impacts_total = tmp.impacts_total
            ,ves.impacts_before_application = tmp.impacts_before_application
from v081_Vespa_Universe_demographics ves
join #temp2 tmp
on ves.cb_key_household = tmp.cb_key_household -- lets match at account level --


update v081_Vespa_Universe_demographics
        set impacts_total = (case when impacts_total is null then 0 else impacts_total end)
                ,impacts_before_application = (case when impacts_before_application is null then 0 else impacts_before_application end)




-- lets take a look at the outputs -- THIS TABLE GIVES ALL IMPACTS AND SALES - DISREGARDING IMPACTS BEFORE/AFTER THE APPLICATION

select impacts_total
       ,(sum(case when barclays_responder = 1 then weighting else null end)) as responder_HH
       -- *0.865 --applied unifmorm discounting factor to bring scaling in line with actual barclays responders
       ,(sum(case when barclays_responder = 1 and barclays_cash_isa > 0 then weighting else null end)) as responder_HH_Had_CASH_ISA
       ,(sum(case when barclays_responder = 1 and barclays_cash_isa  = 0 then weighting else null end)) as responder_HH_NO_CASH_ISA
       ,sum(case when barclays_responder <>1 then weighting else null end) as non_responder_HH
       ,sum(case when barclays_responder <> 1 and barclays_cash_isa > 0 then weighting else null end) as non_responder_HH_Had_CASH_ISA
       ,sum(case when barclays_responder <> 1 and (barclays_cash_isa  = 0 or barclays_cash_isa is null) then weighting else null end) as non_responder_HH_NO_CASH_ISA -- as
--this contains nulls
       ,sum(case when SOCIaL_GRADE2 IN ('A','B','C1') then weighting else null end) as abc1

from v081_Vespa_Universe_demographics
where weighting is not null
AND BARCLAYS_CUSTOMER = 1 -- this IS THE BARCLAYS SKY BASE!
group by impacts_total
order by impacts_total



-- ASPIRATIONAL
select impacts_total
       ,(sum(case when barclays_responder = 1 then weighting else null end)) as responder_HH
       -- *0.865 --applied unifmorm discounting factor to bring scaling in line with actual barclays responders
       ,(sum(case when barclays_responder = 1 and barclays_cash_isa > 0 then weighting else null end)) as responder_HH_Had_CASH_ISA
       ,(sum(case when barclays_responder = 1 and barclays_cash_isa  = 0 then weighting else null end)) as responder_HH_NO_CASH_ISA
       ,sum(case when barclays_responder <>1 then weighting else null end) as non_responder_HH
       ,sum(case when barclays_responder <> 1 and barclays_cash_isa > 0 then weighting else null end) as non_responder_HH_Had_CASH_ISA
       ,sum(case when barclays_responder <> 1 and (barclays_cash_isa  = 0 or barclays_cash_isa is null) then weighting else null end) as non_responder_HH_NO_CASH_ISA -- as this contains nulls
from v081_Vespa_Universe_demographics
where weighting is not null
and aspiration_target = 1
group by impacts_total
order by impacts_total


-- RESPONSE TARGET:
select impacts_total
       ,(sum(case when barclays_responder = 1 then weighting else null end)) as responder_HH
       -- *0.865 --applied unifmorm discounting factor to bring scaling in line with actual barclays responders
       ,(sum(case when barclays_responder = 1 and barclays_cash_isa > 0 then weighting else null end)) as responder_HH_Had_CASH_ISA
       ,(sum(case when barclays_responder = 1 and barclays_cash_isa  = 0 then weighting else null end)) as responder_HH_NO_CASH_ISA
       ,sum(case when barclays_responder <>1 then weighting else null end) as non_responder_HH
       ,sum(case when barclays_responder <> 1 and barclays_cash_isa > 0 then weighting else null end) as non_responder_HH_Had_CASH_ISA
       ,sum(case when barclays_responder <> 1 and (barclays_cash_isa  = 0 or barclays_cash_isa is null) then weighting else null end) as non_responder_HH_NO_CASH_ISA -- as this contains nulls
from v081_Vespa_Universe_demographics
where weighting is not null
and response_target = 1
group by impacts_total
order by impacts_total


--response_target_and_unknown
select impacts_total
       ,(sum(case when barclays_responder = 1 then weighting else null end)) as responder_HH
       -- *0.865 --applied unifmorm discounting factor to bring scaling in line with actual barclays responders
       ,(sum(case when barclays_responder = 1 and barclays_cash_isa > 0 then weighting else null end)) as responder_HH_Had_CASH_ISA
       ,(sum(case when barclays_responder = 1 and barclays_cash_isa  = 0 then weighting else null end)) as responder_HH_NO_CASH_ISA
       ,sum(case when barclays_responder <>1 then weighting else null end) as non_responder_HH
       ,sum(case when barclays_responder <> 1 and barclays_cash_isa > 0 then weighting else null end) as non_responder_HH_Had_CASH_ISA
       ,sum(case when barclays_responder <> 1 and (barclays_cash_isa  = 0 or barclays_cash_isa is null) then weighting else null end) as non_responder_HH_NO_CASH_ISA -- as this contains nulls
from v081_Vespa_Universe_demographics
where weighting is not null
and response_target_and_unknown = 1
group by impacts_total
order by impacts_total






-- OUTPUT 2: - THESE ARE ALL THE IMPACTS BEFORE THE DATE OF APPLICATION - PURCHASE ACTION.

select impacts_before_application
       ,(sum(case when barclays_responder = 1 then weighting else null end)) as responder_HH
       -- *0.865 --applied unifmorm discounting factor to bring scaling in line with actual barclays responders
       ,(sum(case when barclays_responder = 1 and barclays_cash_isa > 0 then weighting else null end)) as responder_HH_Had_CASH_ISA
       ,(sum(case when barclays_responder = 1 and barclays_cash_isa  = 0 then weighting else null end)) as responder_HH_NO_CASH_ISA
       ,sum(case when barclays_responder <>1 then weighting else null end) as non_responder_HH
       ,sum(case when barclays_responder <> 1 and barclays_cash_isa > 0 then weighting else null end) as non_responder_HH_Had_CASH_ISA
       ,sum(case when barclays_responder <> 1 and (barclays_cash_isa  = 0 or barclays_cash_isa is null) then weighting else null end) as non_responder_HH_NO_CASH_ISA -- as this contains nulls
from v081_Vespa_Universe_demographics
where weighting is not null
group by impacts_before_application
order by impacts_before_application





-- ASPIRATIONAL
select impacts_before_application
       ,(sum(case when barclays_responder = 1 then weighting else null end)) as responder_HH
       -- *0.865 --applied unifmorm discounting factor to bring scaling in line with actual barclays responders
       ,(sum(case when barclays_responder = 1 and barclays_cash_isa > 0 then weighting else null end)) as responder_HH_Had_CASH_ISA
       ,(sum(case when barclays_responder = 1 and barclays_cash_isa  = 0 then weighting else null end)) as responder_HH_NO_CASH_ISA
       ,sum(case when barclays_responder <>1 then weighting else null end) as non_responder_HH
       ,sum(case when barclays_responder <> 1 and barclays_cash_isa > 0 then weighting else null end) as non_responder_HH_Had_CASH_ISA
       ,sum(case when barclays_responder <> 1 and (barclays_cash_isa  = 0 or barclays_cash_isa is null) then weighting else null end) as non_responder_HH_NO_CASH_ISA -- as this contains nulls
from v081_Vespa_Universe_demographics
where weighting is not null
and aspiration_target = 1
group by impacts_before_application
order by impacts_before_application



-- RESPONSE TARGET:
select impacts_before_application
       ,(sum(case when barclays_responder = 1 then weighting else null end)) as responder_HH
       -- *0.865 --applied unifmorm discounting factor to bring scaling in line with actual barclays responders
       ,(sum(case when barclays_responder = 1 and barclays_cash_isa > 0 then weighting else null end)) as responder_HH_Had_CASH_ISA
       ,(sum(case when barclays_responder = 1 and barclays_cash_isa  = 0 then weighting else null end)) as responder_HH_NO_CASH_ISA
       ,sum(case when barclays_responder <>1 then weighting else null end) as non_responder_HH
       ,sum(case when barclays_responder <> 1 and barclays_cash_isa > 0 then weighting else null end) as non_responder_HH_Had_CASH_ISA
       ,sum(case when barclays_responder <> 1 and (barclays_cash_isa  = 0 or barclays_cash_isa is null) then weighting else null end) as non_responder_HH_NO_CASH_ISA -- as this contains nulls
from v081_Vespa_Universe_demographics
where weighting is not null
and response_target = 1
group by impacts_before_application
order by impacts_before_application
-- THE ABOVE IS ALL WORKING


-- response_target_and_unknown
select impacts_before_application
       ,(sum(case when barclays_responder = 1 then weighting else null end)) as responder_HH
       -- *0.865 --applied unifmorm discounting factor to bring scaling in line with actual barclays responders
       ,(sum(case when barclays_responder = 1 and barclays_cash_isa > 0 then weighting else null end)) as responder_HH_Had_CASH_ISA
       ,(sum(case when barclays_responder = 1 and barclays_cash_isa  = 0 then weighting else null end)) as responder_HH_NO_CASH_ISA
       ,sum(case when barclays_responder <>1 then weighting else null end) as non_responder_HH
       ,sum(case when barclays_responder <> 1 and barclays_cash_isa > 0 then weighting else null end) as non_responder_HH_Had_CASH_ISA
       ,sum(case when barclays_responder <> 1 and (barclays_cash_isa  = 0 or barclays_cash_isa is null) then weighting else null end) as non_responder_HH_NO_CASH_ISA -- as this contains nulls
from v081_Vespa_Universe_demographics
where weighting is not null
and response_target_and_unknown = 1
group by impacts_before_application
order by impacts_before_application
-- THE ABOVE IS ALL WORKING




-- THE ABOVE IS ALL WORKING

------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------



-----
-- output 13: SPOT RECENCY
-----


select  --bar.account_number
        bar.cb_key_household
        ,last_spot_seen = max(case when whole_spot = 1 and responder = 1 then viewing_date else null end)
        ,last_spot_seen_pre_1st_application = max(case when whole_spot = 1 and responder = 1 and viewing_date < min_application_date then viewing_date else null end)
        ,max(barclays_responder) as barclays_responder
into #recency_table
from Barclays_spots_viewing_table_dump bar
join v081_Vespa_Universe_demographics ves
on ves.account_number = bar.account_number
group by bar.cb_key_household

select top 10 * from v081_Vespa_Universe_demographics

alter table v081_Vespa_Universe_demographics
        add( last_spot_seen date
            ,last_spot_seen_pre_application date
            ,recency_Last_spot integer);


update v081_Vespa_Universe_demographics
        set ves.last_spot_seen = tmp.last_spot_seen
            ,ves.last_spot_seen_pre_application = tmp.last_spot_seen_pre_1st_application
            ,recency_Last_spot = datediff(day,last_spot_seen_pre_1st_application,min_application_date)
from v081_Vespa_Universe_demographics ves
join #recency_table tmp
on ves.cb_key_household = tmp.cb_key_household

-- Lets seperate the nulls from those that didnt watch a spot and to those who purchased on the same day as thier last spot view
update v081_Vespa_Universe_demographics
set recency_Last_spot = (case when barclays_spot = 0 then 999 else recency_Last_spot end)

update v081_Vespa_Universe_demographics
set recency_Last_spot = (case when recency_Last_spot is null then 0 else recency_Last_spot end)



select sum(weighting) from v081_Vespa_Universe_demographics


-- LETS LOOK AT THE OUTPUTS FOR RECENCY

select recency_Last_spot
       ,sum(case when barclays_responder = 1 then weighting else null end) as responder_HH
       ,(sum(case when barclays_responder = 1 and barclays_cash_isa > 0 then weighting else null end)) as responder_HH_Had_CASH_ISA -- DISCOUNTING FACTOR HAS BEEN REMOVED FROM HERE
       ,(sum(case when barclays_responder = 1 and barclays_cash_isa  = 0 then weighting else null end)) as responder_HH_NO_CASH_ISA
--        ,sum(case when barclays_responder <>1 then weighting else null end) as non_responder_HH
--        ,sum(case when barclays_responder <> 1 and barclays_cash_isa > 0 then weighting else null end) as non_responder_HH_Had_CASH_ISA
--        ,sum(case when barclays_responder <> 1 and (barclays_cash_isa  = 0 or barclays_cash_isa is null) then weighting else null end) as non_responder_HH_NO_CASH_ISA -- as this contains nulls
 --      ,sum(case when barclays_responder <>1 then weighting else null end) as non_responder_HH
from v081_Vespa_Universe_demographics
where weighting is not null
      and barclays_responder = 1
      and impacts_total >2
group by recency_Last_spot
order by recency_Last_spot
-- THE ABOVE IS ALL WORKING




--Aspirational

select recency_Last_spot
       ,sum(case when barclays_responder = 1 then weighting else null end) as responder_HH
       ,(sum(case when barclays_responder = 1 and barclays_cash_isa > 0 then weighting else null end)) as responder_HH_Had_CASH_ISA -- DISCOUNTING FACTOR HAS BEEN REMOVED FROM HERE
       ,(sum(case when barclays_responder = 1 and barclays_cash_isa  = 0 then weighting else null end)) as responder_HH_NO_CASH_ISA
--        ,sum(case when barclays_responder <>1 then weighting else null end) as non_responder_HH
--        ,sum(case when barclays_responder <> 1 and barclays_cash_isa > 0 then weighting else null end) as non_responder_HH_Had_CASH_ISA
--        ,sum(case when barclays_responder <> 1 and (barclays_cash_isa  = 0 or barclays_cash_isa is null) then weighting else null end) as non_responder_HH_NO_CASH_ISA -- as this contains nulls
 --      ,sum(case when barclays_responder <>1 then weighting else null end) as non_responder_HH
from v081_Vespa_Universe_demographics
where weighting is not null
      and barclays_responder = 1
      and impacts_total >2
      and aspiration_target = 1
group by recency_Last_spot
order by recency_Last_spot
-- THE ABOVE IS ALL WORKING




-- Response target

select recency_Last_spot
       ,sum(case when barclays_responder = 1 then weighting else null end) as responder_HH
       ,(sum(case when barclays_responder = 1 and barclays_cash_isa > 0 then weighting else null end)) as responder_HH_Had_CASH_ISA -- DISCOUNTING FACTOR HAS BEEN REMOVED FROM HERE
       ,(sum(case when barclays_responder = 1 and barclays_cash_isa  = 0 then weighting else null end)) as responder_HH_NO_CASH_ISA
--        ,sum(case when barclays_responder <>1 then weighting else null end) as non_responder_HH
--        ,sum(case when barclays_responder <> 1 and barclays_cash_isa > 0 then weighting else null end) as non_responder_HH_Had_CASH_ISA
--        ,sum(case when barclays_responder <> 1 and (barclays_cash_isa  = 0 or barclays_cash_isa is null) then weighting else null end) as non_responder_HH_NO_CASH_ISA -- as this contains nulls
 --      ,sum(case when barclays_responder <>1 then weighting else null end) as non_responder_HH
from v081_Vespa_Universe_demographics
where weighting is not null
      and barclays_responder = 1
      and impacts_total >2
      and response_target = 1
group by recency_Last_spot
order by recency_Last_spot
-- THE ABOVE IS ALL WORKING


-- response_target_and_unknown
select recency_Last_spot
       ,sum(case when barclays_responder = 1 then weighting else null end) as responder_HH
       ,(sum(case when barclays_responder = 1 and barclays_cash_isa > 0 then weighting else null end)) as responder_HH_Had_CASH_ISA -- DISCOUNTING FACTOR HAS BEEN REMOVED FROM HERE
       ,(sum(case when barclays_responder = 1 and barclays_cash_isa  = 0 then weighting else null end)) as responder_HH_NO_CASH_ISA
--        ,sum(case when barclays_responder <>1 then weighting else null end) as non_responder_HH
--        ,sum(case when barclays_responder <> 1 and barclays_cash_isa > 0 then weighting else null end) as non_responder_HH_Had_CASH_ISA
--        ,sum(case when barclays_responder <> 1 and (barclays_cash_isa  = 0 or barclays_cash_isa is null) then weighting else null end) as non_responder_HH_NO_CASH_ISA -- as this contains nulls
 --      ,sum(case when barclays_responder <>1 then weighting else null end) as non_responder_HH
from v081_Vespa_Universe_demographics
where weighting is not null
      and barclays_responder = 1
      and impacts_total >2
      and response_target_and_unknown = 1
group by recency_Last_spot
order by recency_Last_spot
-- THE ABOVE IS ALL WORKING



select count (*) from v081_Vespa_Universe_demographics
select count (*), count(distinct(cb_key_household)) from v081_Vespa_Universe_demographics


------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
                        ---     TEMPLATE 3      --
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

-- SPREADSHEET 2.A.

---------------------
-- OUTPUT: responders vs sales house -- also need impacts
---------------------



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

drop table #saleshouse_responders

select  bar.account_number
        ,bar.cb_key_household
        ,sales_house
        ,impacts = COUNT(case when whole_spot = 1 then 1 else null end)
        ,max(barclays_responder) as responder
        ,max(bar.weighting)as weighting
        ,max(ves.aspiration_target) as aspiration_target
        ,max(ves.response_target) as response_target
        ,max(response_target_and_unknown) as response_target_and_unknown
        ,max(ABC1_TARGET) as ABC1_TARGET
    --    ,max(barclays_customer) as barclays_customer
into #saleshouse_responders
from Barclays_spots_viewing_table_dump bar
right join v081_Vespa_Universe_demographics ves
on ves.account_number = bar.account_number
WHERE BARCLAYS_CUSTOMER = 1 -- this is for the barclyas base in SKY only!
group by bar.account_number, bar.cb_key_household, bar.sales_house




-- lets get the outputs;
select sales_house
        ,sum(case when responder = 1 then weighting else null end) as responders
         ,sum(case when impacts >= 3 and responder = 1 then weighting else null end) as responders_gt_3impacts
        ,sum(case when responder <> 1 then weighting else null end) as non_responders
        ,sum(case when impacts >= 3 and responder <> 1 then weighting else null end) as non_responders_gt_3impacts
from #saleshouse_responders
group by sales_house
order by responders desc

select count(*) from #saleshouse_responders where barclays_customer = 0 and responder = 1 -- 56 cases! -- why!!

--151377.289752483 -- barclays responder

--150982.478185177 -- responder


-- means there are people who responded but were not barclays customers -- look into this!>





--Aspirational
select sales_house
        ,sum(case when responder = 1 then weighting else null end) as responders
         ,sum(case when impacts >= 3 and responder = 1 then weighting else null end) as responders_gt_3impacts
        ,sum(case when responder <> 1 then weighting else null end) as non_responders
        ,sum(case when impacts >= 3 and responder <> 1 then weighting else null end) as non_responders_gt_3impacts
from #saleshouse_responders
where aspiration_target = 1
group by sales_house
order by responders desc



--NON - Aspirational
select sales_house
        ,sum(case when responder = 1 then weighting else null end) as responders
         ,sum(case when impacts >= 3 and responder = 1 then weighting else null end) as responders_gt_3impacts
        ,sum(case when responder <> 1 then weighting else null end) as non_responders
        ,sum(case when impacts >= 3 and responder <> 1 then weighting else null end) as non_responders_gt_3impacts
from #saleshouse_responders
where aspiration_target <> 1
group by sales_house
order by responders desc




-- Response target
select sales_house
        ,sum(case when responder = 1 then weighting else null end) as responders
         ,sum(case when impacts >= 3 and responder = 1 then weighting else null end) as responders_gt_3impacts
        ,sum(case when responder <> 1 then weighting else null end) as non_responders
        ,sum(case when impacts >= 3 and responder <> 1 then weighting else null end) as non_responders_gt_3impacts
from #saleshouse_responders
where response_target = 1
group by sales_house
order by responders desc



-- response_target_and_unknown
select sales_house
        ,sum(case when responder = 1 then weighting else null end) as responders
         ,sum(case when impacts >= 3 and responder = 1 then weighting else null end) as responders_gt_3impacts
        ,sum(case when responder <> 1 then weighting else null end) as non_responders
        ,sum(case when impacts >= 3 and responder <> 1 then weighting else null end) as non_responders_gt_3impacts
from #saleshouse_responders
where response_target_and_unknown = 1
group by sales_house
order by responders desc


-- this is a combination of outputs suited to susannes output sheets
select sales_house
      ,sum(case when responder = 1 then weighting else null end) as responders
        ,sum(case when responder = 0 then weighting else null end) as non_responders
        ,sum(case when response_target_and_unknown = 1 then weighting else null end) as response_target_and_unknown
        ,sum(case when response_target = 1 then weighting else null end) as response_target
        ,sum(case when aspiration_target = 1 then weighting else null end) as taspiration_target
        ,sum(case when aspiration_target = 0  then weighting else null end) as non_aspiration_target
        ,sum(case when ABC1_TARGET = 1 then weighting else null end) as ABC1
        ,sum(case when responder = 1 and ABC1_TARGET = 1 then weighting else null end) as ABC1_responder
from #saleshouse_responders
group by sales_house
order by sales_house



ABC1_TARGET


---------------------
-- OUTPUT: responders vs MEDIA PACK -- also need impacts
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
alter table Barclays_spots_viewing_table_dump
        add media_pack varchar(25);


update Barclays_spots_viewing_table_dump
        set cub.media_pack = tmp.channel_category
from Barclays_spots_viewing_table_dump as cub
join LkUpPack as tmp
on tmp.service_key = cub.service_key


drop table #media_pack

select  bar.account_number
        ,bar.cb_key_household
        ,MEDIA_PACK
        ,impacts = COUNT(case when whole_spot = 1 then 1 else null end)
        ,max(barclays_responder) as responder
        ,max(bar.weighting)as weighting
        ,max(ves.aspiration_target) as aspiration_target
        ,max(ves.response_target) as response_target
        ,max(response_target_and_unknown) as response_target_and_unknown
        ,max(ABC1_TARGET) as ABC1_TARGET
into #MEDIA_PACK
from Barclays_spots_viewing_table_dump bar
right join v081_Vespa_Universe_demographics ves
on ves.account_number = bar.account_number
WHERE BARCLAYS_CUSTOMER = 1 -- this is for the barclyas base in SKY only!
group by bar.account_number, bar.cb_key_household, bar.media_pack


-- Output
select media_pack
        ,sum(case when responder = 1 then weighting else null end) as responders
        ,sum(case when impacts >= 3 and responder = 1 then weighting else null end) as responders_gt_3impacts
        ,sum(case when responder <> 1 then weighting else null end) as non_responders
        ,sum(case when impacts >= 3 and responder <> 1 then weighting else null end) as non_responders_gt_3impacts
from #MEDIA_PACK
group by media_pack
order by media_pack






--Aspirational
select media_pack
        ,sum(case when responder = 1 then weighting else null end) as responders
        ,sum(case when impacts >= 3 and responder = 1 then weighting else null end) as responders_gt_3impacts
        ,sum(case when responder <> 1 then weighting else null end) as non_responders
        ,sum(case when impacts >= 3 and responder <> 1 then weighting else null end) as non_responders_gt_3impacts
from #MEDIA_PACK
where aspiration_target = 1
group by media_pack
order by media_pack




--NON-Aspirational
select media_pack
        ,sum(case when responder = 1 then weighting else null end) as responders
        ,sum(case when impacts >= 3 and responder = 1 then weighting else null end) as responders_gt_3impacts
        ,sum(case when responder <> 1 then weighting else null end) as non_responders
        ,sum(case when impacts >= 3 and responder <> 1 then weighting else null end) as non_responders_gt_3impacts
from #MEDIA_PACK
where aspiration_target <> 1
group by media_pack
order by media_pack


-- Response target
select media_pack
        ,sum(case when responder = 1 then weighting else null end) as responders
        ,sum(case when impacts >= 3 and responder = 1 then weighting else null end) as responders_gt_3impacts
        ,sum(case when responder <> 1 then weighting else null end) as non_responders
        ,sum(case when impacts >= 3 and responder <> 1 then weighting else null end) as non_responders_gt_3impacts
from #MEDIA_PACK
where response_target = 1
group by media_pack
order by media_pack



-- response_target_and_unknown
select media_pack
        ,sum(case when responder = 1 then weighting else null end) as responders
        ,sum(case when impacts >= 3 and responder = 1 then weighting else null end) as responders_gt_3impacts
        ,sum(case when responder <> 1 then weighting else null end) as non_responders
        ,sum(case when impacts >= 3 and responder <> 1 then weighting else null end) as non_responders_gt_3impacts
from #MEDIA_PACK
where response_target_and_unknown = 1
group by media_pack
order by media_pack




-- this is a combination of outputs suited to susannes output sheets
select media_pack
        ,sum(case when responder = 1 then weighting else null end) as responders
        ,sum(case when responder = 0 then weighting else null end) as non_responders
        ,sum(case when response_target_and_unknown = 1 then weighting else null end) as response_target_and_unknown
        ,sum(case when response_target = 1 then weighting else null end) as response_target
        ,sum(case when aspiration_target = 1 then weighting else null end) as taspiration_target
        ,sum(case when aspiration_target = 0 then weighting else null end) as non_aspiration_target
        ,sum(case when ABC1_TARGET = 1 then weighting else null end) as ABC1
        ,sum(case when responder = 1 and ABC1_TARGET = 1 then weighting else null end) as ABC1_responder
from #media_pack
-- where impacts> 0 -- seems like this may be needed
group by media_pack
order by media_pack


---------------------
-- OUTPUT: responders vs MEDIA PACK -- also need impacts
---------------------


drop table #channel

select  bar.account_number
        ,bar.cb_key_household
        ,agg_channel_name as channel_name
        ,max(media_pack) as media_pack
        ,max(sales_house) as sales_house
        ,impacts = COUNT(case when whole_spot = 1 then 1 else null end)
        ,max(barclays_responder) as responder
        ,max(bar.weighting)as weighting
        ,max(ves.aspiration_target) as aspiration_target
        ,max(ves.response_target) as response_target
        ,max(response_target_and_unknown) as response_target_and_unknown
        ,max(ABC1_TARGET) as ABC1_TARGET
into #channel
from Barclays_spots_viewing_table_dump bar
right join v081_Vespa_Universe_demographics ves
on ves.account_number = bar.account_number
where barclays_customer = 1
group by bar.account_number, bar.cb_key_household, bar.agg_channel_name



select channel_name
        ,sum(case when responder = 1 then weighting else null end) as responders
        ,sum(case when impacts >= 3 and responder = 1 then weighting else null end) as responders_gt_3impacts
        ,sum(case when responder <> 1 then weighting else null end) as non_responders
        ,sum(case when impacts >= 3 and responder <> 1 then weighting else null end) as non_responders_gt_3impacts
from #channel
group by channel_name
order by responders desc




--Aspirational
select channel_name
        ,sum(case when responder = 1 then weighting else null end) as responders
        ,sum(case when impacts >= 3 and responder = 1 then weighting else null end) as responders_gt_3impacts
        ,sum(case when responder <> 1 then weighting else null end) as non_responders
        ,sum(case when impacts >= 3 and responder <> 1 then weighting else null end) as non_responders_gt_3impacts
from #channel
where aspiration_target = 1
group by channel_name
order by responders desc




--RESPONSE TARGET
select channel_name
        ,sum(case when responder = 1 then weighting else null end) as responders
        ,sum(case when impacts >= 3 and responder = 1 then weighting else null end) as responders_gt_3impacts
        ,sum(case when responder <> 1 then weighting else null end) as non_responders
        ,sum(case when impacts >= 3 and responder <> 1 then weighting else null end) as non_responders_gt_3impacts
from #channel
where response_target = 1
group by channel_name
order by responders desc




--response_target_and_unknown
select channel_name
        ,sum(case when responder = 1 then weighting else null end) as responders
        ,sum(case when impacts >= 3 and responder = 1 then weighting else null end) as responders_gt_3impacts
        ,sum(case when responder <> 1 then weighting else null end) as non_responders
        ,sum(case when impacts >= 3 and responder <> 1 then weighting else null end) as non_responders_gt_3impacts
from #channel
where response_target_and_unknown = 1
group by channel_name
order by responders desc



-- this is a combination of outputs suited to susannes output sheets


-- this is a combination of outputs suited to susannes output sheets
select channel_name
        ,media_pack
        ,sales_house
        ,sum(case when responder = 1 then weighting else null end) as responders
        ,sum(case when responder = 0 then weighting else null end) as non_responders
        ,sum(case when response_target_and_unknown = 1 then weighting else null end) as response_target_and_unknown
        ,sum(case when response_target = 1  then weighting else null end) as response_target
        ,sum(case when aspiration_target = 1 then weighting else null end) as taspiration_target
        ,sum(case when aspiration_target = 0 then weighting else null end) as non_aspiration_target
        ,sum(case when ABC1_TARGET = 1 then weighting else null end) as ABC1
        ,sum(case when responder = 1 and ABC1_TARGET = 1 then weighting else null end) as ABC1_responder
from #channel
group by channel_name,media_pack
        ,sales_house
order by channel_name

---------------------
-- OUTPUT: DayPart
---------------------


-- select   bar.cb_key_household
--         ,x_viewing_time_of_day as day_part
--         ,impacts = COUNT(case when whole_spot = 1 then 1 else null end)
--         ,max(responder) as responder
--         ,max(bar.weighting)as weighting
--         ,max(ves.aspiration_target) as aspiration_target
--         ,max(ves.response_target) as response_target
--         ,max(response_target_and_unknown) as response_target_and_unknown
-- into #day_part
-- from Barclays_spots_viewing_table_dump bar
-- right join v081_Vespa_Universe_demographics ves
-- on ves.account_number = bar.account_number
-- group by  bar.cb_key_household, bar.x_viewing_time_of_day

-- select day_part, sum(weighting) from #day_part group by day_part order by day_part
--
-- day_part        sum(#day_part.weighting)
--
-- breakfast       2057429.67275667191
-- early prime     6481513.43472623825
-- late night      7088053.84397554398
-- lunch           4879706.46377944946
-- morning         3229544.29294967651
-- night           2185338.64781570435
-- prime           8253041.51857328415
--
--
-- -- all responders - normal
-- select day_part
--         ,sum(case when responder = 1 then weighting else null end) as responders
--         ,sum(case when impacts >= 3 and responder = 1 then weighting else null end) as responders_gt_3impacts
--         ,sum(case when responder <> 1 then weighting else null end) as non_responders
--         ,sum(case when impacts >= 3 and responder <> 1 then weighting else null end) as non_responders_gt_3impacts
-- from #day_part
-- group by day_part
-- order by responders desc
--
-- 
-- 
-- --Aspirational
-- select day_part
--         ,sum(case when responder = 1 then weighting else null end) as responders
--         ,sum(case when impacts >= 3 and responder = 1 then weighting else null end) as responders_gt_3impacts
--         ,sum(case when responder <> 1 then weighting else null end) as non_responders
--         ,sum(case when impacts >= 3 and responder <> 1 then weighting else null end) as non_responders_gt_3impacts
-- from #day_part
-- where aspiration_target = 1
-- group by day_part
-- order by responders desc
-- 
-- 
-- 
-- 
-- --RESPONSE TARGET
-- select day_part
--         ,sum(case when responder = 1 then weighting else null end) as responders
--         ,sum(case when impacts >= 3 and responder = 1 then weighting else null end) as responders_gt_3impacts
--         ,sum(case when responder <> 1 then weighting else null end) as non_responders
--         ,sum(case when impacts >= 3 and responder <> 1 then weighting else null end) as non_responders_gt_3impacts
-- from #day_part
-- where response_target = 1
-- group by day_part
-- order by responders desc
-- 
-- 
-- 
--
-- --response_target_and_unknown
-- select day_part
--         ,sum(case when responder = 1 then weighting else null end) as responders
--         ,sum(case when impacts >= 3 and responder = 1 then weighting else null end) as responders_gt_3impacts
--         ,sum(case when responder <> 1 then weighting else null end) as non_responders
--         ,sum(case when impacts >= 3 and responder <> 1 then weighting else null end) as non_responders_gt_3impacts
-- from #day_part
-- where response_target_and_unknown = 1
-- group by day_part
-- order by responders desc


--- LETS TRY OUT SOMETHING NEW:

DROP TABLE #day_part2

select   bar.cb_key_household
         ,(case when viewing_date in ('2012-03-05','2012-03-12','2012-03-19','2012-03-26','2012-04-02','2012-04-09','2012-04-16') then 'Monday'
        when viewing_date in ('2012-03-06','2012-03-13','2012-03-20','2012-03-27','2012-04-03','2012-04-10','2012-04-17') then 'Tuesday'
        when viewing_date in ('2012-02-29','2012-03-07','2012-03-14','2012-03-21','2012-03-28','2012-04-04','2012-04-11','2012-04-18') then 'Wednesday'
        when viewing_date in ('2012-03-01','2012-03-08','2012-03-15','2012-03-22','2012-03-29','2012-04-05','2012-04-12','2012-04-19') then 'Thursday'
        when viewing_date in ('2012-03-02','2012-03-09','2012-03-16','2012-03-23','2012-03-30','2012-04-06','2012-04-13','2012-04-20') then 'Friday'
        when viewing_date in ('2012-03-03','2012-03-10','2012-03-17','2012-03-24','2012-03-31','2012-04-07','2012-04-14','2012-04-21') then 'Saturday'
        when viewing_date in ('2012-03-04','2012-03-11','2012-03-18','2012-03-25','2012-04-01','2012-04-08','2012-04-15') then 'Sunday'
        else null end) as day_of_week
        ,x_viewing_time_of_day as day_part
        ,impacts = COUNT(case when whole_spot = 1 then 1 else null end)
        ,max(barclays_responder) as responder
        ,max(bar.weighting)as weighting
        ,max(ves.aspiration_target) as aspiration_target
        ,max(ves.response_target) as response_target
        ,max(response_target_and_unknown) as response_target_and_unknown
        ,max(ABC1_TARGET) as ABC1_TARGET
into #day_part2
from Barclays_spots_viewing_table_dump bar
right join v081_Vespa_Universe_demographics ves
on ves.account_number = bar.account_number
where barclays_customer = 1
group by  bar.cb_key_household, day_of_week, DAY_PART



-- all responders - normal

select day_of_week, day_part
        ,sum(case when responder = 1 then weighting else null end) as responders
        ,sum(case when impacts >= 3 and responder = 1 then weighting else null end) as responders_gt_3impacts
        ,sum(case when responder <> 1 then weighting else null end) as non_responders
        ,sum(case when impacts >= 3 and responder <> 1 then weighting else null end) as non_responders_gt_3impacts
        ,sum(weighting) as Sky
from #day_part2
group by day_of_week,day_part
order by responders desc


--Aspirational
select day_of_week,day_part
        ,sum(case when aspiration_target = 1 and responder = 1 then weighting else null end) as responders
        ,sum(case when aspiration_target = 1 and impacts >= 3 and responder = 1 then weighting else null end) as responders_gt_3impacts
        ,sum(case when aspiration_target = 1 and responder <> 1 then weighting else null end) as non_responders
        ,sum(case when aspiration_target = 1 and impacts >= 3 and responder <> 1 then weighting else null end) as non_responders_gt_3impacts
        ,sum(weighting) as Sky
from #day_part2
--where aspiration_target = 1
group by day_of_week,day_part
order by responders desc




--RESPONSE TARGET
select day_of_week,day_part
        ,sum(case when response_target = 1 and responder = 1 then weighting else null end) as responders
        ,sum(case when response_target = 1 and impacts >= 3 and responder = 1 then weighting else null end) as responders_gt_3impacts
        ,sum(case when response_target = 1 and responder <> 1 then weighting else null end) as non_responders
        ,sum(case when response_target = 1 and impacts >= 3 and responder <> 1 then weighting else null end) as non_responders_gt_3impacts
        ,sum(weighting) as Sky
from #day_part2
-- where response_target = 1
group by day_of_week,day_part
order by responders desc




--response_target_and_unknown
select day_of_week,day_part
        ,sum(case when response_target_and_unknown = 1 and responder = 1 then weighting else null end) as responders
        ,sum(case when response_target_and_unknown = 1 and impacts >= 3 and responder = 1 then weighting else null end) as responders_gt_3impacts
        ,sum(case when response_target_and_unknown = 1 and responder <> 1 then weighting else null end) as non_responders
        ,sum(case when response_target_and_unknown = 1 and impacts >= 3 and responder <> 1 then weighting else null end) as non_responders_gt_3impacts
        ,sum(weighting) as Sky
from #day_part2
-- where response_target_and_unknown = 1
group by day_of_week,day_part
order by responders desc


select top 10 * from sky_base




-- this is a combination of outputs suited to susannes output sheets
select day_of_week,day_part
        ,sum(case when responder = 1 then weighting else null end) as responders
        ,sum(case when responder = 0 then weighting else null end) as non_responders
        ,sum(case when response_target_and_unknown = 1 then weighting else null end) as response_target_and_unknown
        ,sum(case when response_target = 1  then weighting else null end) as response_target
        ,sum(case when aspiration_target = 1 then weighting else null end) as taspiration_target
        ,sum(case when aspiration_target = 0 then weighting else null end) as non_aspiration_target
        ,sum(case when ABC1_TARGET = 1 then weighting else null end) as ABC1
        ,sum(case when responder = 1 and ABC1_TARGET = 1 then weighting else null end) as ABC1_responder
from #day_part2
group by day_of_week,day_part
order by day_of_week,day_part




---------------------
-- OUTPUT: Genre
---------------------


drop table #genres

select  bar.cb_key_household
        ,genre_description as genre
   --     ,sub_genre_description as sub_genre
        ,impacts = COUNT(case when whole_spot = 1 then 1 else null end)
        ,max(barclays_responder) as responder
        ,max(bar.weighting)as weighting
        ,max(ves.aspiration_target) as aspiration_target
        ,max(ves.response_target) as response_target
        ,max(response_target_and_unknown) as response_target_and_unknown
        ,max(ABC1_TARGET) as ABC1_TARGET
into #genres
from Barclays_spots_viewing_table_dump bar
right join v081_Vespa_Universe_demographics ves
on ves.account_number = bar.account_number
where barclays_customer = 1
group by  bar.cb_key_household, genre




-- 1st lets get the genres
select distinct(genre)
        ,sum(case when responder = 1 then weighting else null end) as responders
        ,sum(case when impacts >= 3 and responder = 1 then weighting else null end) as responders_gt_3impacts
        ,sum(case when responder <> 1 then weighting else null end) as non_responders
        ,sum(case when impacts >= 3 and responder <> 1 then weighting else null end) as non_responders_gt_3impacts
from #genres
group by genre
order by responders desc




--Aspirational
select distinct(genre)
        ,sum(case when responder = 1 then weighting else null end) as responders
        ,sum(case when impacts >= 3 and responder = 1 then weighting else null end) as responders_gt_3impacts
        ,sum(case when responder <> 1 then weighting else null end) as non_responders
        ,sum(case when impacts >= 3 and responder <> 1 then weighting else null end) as non_responders_gt_3impacts
from #genres
where aspiration_target = 1
group by genre
order by responders desc



--RESPONSE TARGET
select distinct(genre)
        ,sum(case when responder = 1 then weighting else null end) as responders
        ,sum(case when impacts >= 3 and responder = 1 then weighting else null end) as responders_gt_3impacts
        ,sum(case when responder <> 1 then weighting else null end) as non_responders
        ,sum(case when impacts >= 3 and responder <> 1 then weighting else null end) as non_responders_gt_3impacts
from #genres
where response_target = 1
group by genre
order by responders desc



--response_target_and_unknown
select distinct(genre)
        ,sum(case when responder = 1 then weighting else null end) as responders
        ,sum(case when impacts >= 3 and responder = 1 then weighting else null end) as responders_gt_3impacts
        ,sum(case when responder <> 1 then weighting else null end) as non_responders
        ,sum(case when impacts >= 3 and responder <> 1 then weighting else null end) as non_responders_gt_3impacts
from #genres
where response_target_and_unknown = 1
group by genre
order by responders desc



-- this is a combination of outputs suited to susannes output sheets



-- this is a combination of outputs suited to susannes output sheets
select genre
        ,sum(case when responder = 1 then weighting else null end) as responders
        ,sum(case when responder = 0 then weighting else null end) as non_responders
        ,sum(case when response_target_and_unknown = 1 then weighting else null end) as response_target_and_unknown
        ,sum(case when response_target = 1 then weighting else null end) as response_target
        ,sum(case when aspiration_target = 1 then weighting else null end) as taspiration_target
        ,sum(case when aspiration_target = 0 then weighting else null end) as non_aspiration_target
        ,sum(case when ABC1_TARGET = 1 then weighting else null end) as ABC1
        ,sum(case when responder = 1 and ABC1_TARGET = 1 then weighting else null end) as ABC1_responder
from #genres
group by genre
order by genre









-- next; lets get the sub genres


drop table #sub_genres


select  bar.cb_key_household
        ,genre_description as genre
       ,sub_genre_description as sub_genre
        ,impacts = COUNT(case when whole_spot = 1 then 1 else null end)
        ,max(barclays_responder) as responder
        ,max(bar.weighting)as weighting
        ,max(ves.aspiration_target) as aspiration_target
        ,max(ves.response_target) as response_target
        ,max(response_target_and_unknown) as response_target_and_unknown
        ,max(ABC1_TARGET) as ABC1_TARGET
into #sub_genres
from Barclays_spots_viewing_table_dump bar
right join v081_Vespa_Universe_demographics ves
on ves.account_number = bar.account_number
where barclays_customer = 1
group by  bar.cb_key_household, genre, sub_genre



select genre,sub_genre
        ,sum(case when responder = 1 then weighting else null end) as responders
        ,sum(case when impacts >= 3 and responder = 1 then weighting else null end) as responders_gt_3impacts
        ,sum(case when responder <> 1 then weighting else null end) as non_responders
        ,sum(case when impacts >= 3 and responder <> 1 then weighting else null end) as non_responders_gt_3impacts
from #sub_genres
group by genre,sub_genre
order by responders desc




--Aspirational
select genre,sub_genre
        ,sum(case when responder = 1 then weighting else null end) as responders
        ,sum(case when impacts >= 3 and responder = 1 then weighting else null end) as responders_gt_3impacts
        ,sum(case when responder <> 1 then weighting else null end) as non_responders
        ,sum(case when impacts >= 3 and responder <> 1 then weighting else null end) as non_responders_gt_3impacts
from #sub_genres
where aspiration_target = 1
group by genre,sub_genre
order by responders desc




--RESPONSE TARGET
select genre,sub_genre
        ,sum(case when responder = 1 then weighting else null end) as responders
        ,sum(case when impacts >= 3 and responder = 1 then weighting else null end) as responders_gt_3impacts
        ,sum(case when responder <> 1 then weighting else null end) as non_responders
        ,sum(case when impacts >= 3 and responder <> 1 then weighting else null end) as non_responders_gt_3impacts
from #sub_genres
where response_target = 1
group by genre,sub_genre
order by responders desc




--response_target_and_unknown
select genre,sub_genre
        ,sum(case when responder = 1 then weighting else null end) as responders
        ,sum(case when impacts >= 3 and responder = 1 then weighting else null end) as responders_gt_3impacts
        ,sum(case when responder <> 1 then weighting else null end) as non_responders
        ,sum(case when impacts >= 3 and responder <> 1 then weighting else null end) as non_responders_gt_3impacts
from #sub_genres
where response_target_and_unknown = 1
group by genre,sub_genre
order by responders desc




-- this is a combination of outputs suited to susannes output sheets


-- this is a combination of outputs suited to susannes output sheets
select genre,sub_genre
        ,sum(case when responder = 1 then weighting else null end) as responders
        ,sum(case when responder = 0 then weighting else null end) as non_responders
        ,sum(case when response_target_and_unknown = 1 then weighting else null end) as response_target_and_unknown
        ,sum(case when response_target = 1 then weighting else null end) as response_target
        ,sum(case when aspiration_target = 1 then weighting else null end) as taspiration_target
        ,sum(case when aspiration_target = 0 then weighting else null end) as non_aspiration_target
        ,sum(case when ABC1_TARGET = 1 then weighting else null end) as ABC1
        ,sum(case when responder = 1 and ABC1_TARGET = 1 then weighting else null end) as ABC1_responder
from #sub_genres
group by genre,sub_genre
order by genre,sub_genre


---------------------
-- OUTPUT: DAY OF THE WEEK
---------------------
--
-- 
-- select  bar.cb_key_household
--         ,(case when viewing_date in ('2012-03-05','2012-03-12','2012-03-19','2012-03-26','2012-04-02','2012-04-09','2012-04-16') then 'Monday'
--         when viewing_date in ('2012-03-06','2012-03-13','2012-03-20','2012-03-27','2012-04-03','2012-04-10','2012-04-17') then 'Tuesday'
--         when viewing_date in ('2012-02-29','2012-03-07','2012-03-14','2012-03-21','2012-03-28','2012-04-04','2012-04-11','2012-04-18') then 'Wednesday'
--         when viewing_date in ('2012-03-01','2012-03-08','2012-03-15','2012-03-22','2012-03-29','2012-04-05','2012-04-12','2012-04-19') then 'Thursday'
--         when viewing_date in ('2012-03-02','2012-03-09','2012-03-16','2012-03-23','2012-03-30','2012-04-06','2012-04-13','2012-04-20') then 'Friday'
--         when viewing_date in ('2012-03-03','2012-03-10','2012-03-17','2012-03-24','2012-03-31','2012-04-07','2012-04-14','2012-04-21') then 'Saturday'
--         when viewing_date in ('2012-03-04','2012-03-11','2012-03-18','2012-03-25','2012-04-01','2012-04-08','2012-04-15') then 'Sunday'
--         else null end) as day_of_week
-- 
--         ,impacts = COUNT(case when whole_spot = 1 then 1 else null end)
--         ,max(responder) as responder
--         ,max(bar.weighting)as weighting
--         ,max(ves.aspiration_target) as aspiration_target
--         ,max(ves.response_target) as response_target
--         ,max(response_target_and_unknown) as response_target_and_unknown
-- into #day_of_week
-- from Barclays_spots_viewing_table_dump bar
-- right join v081_Vespa_Universe_demographics ves
-- on ves.account_number = bar.account_number
-- group by  bar.cb_key_household, day_of_week
--
-- 
-- 
-- 
-- -- 1st lets get the day of the week
-- select day_of_week
--         ,sum(case when responder = 1 then weighting else null end) as responders
--         ,sum(case when impacts >= 3 and responder = 1 then weighting else null end) as responders_gt_3impacts
--         ,sum(case when responder <> 1 then weighting else null end) as non_responders
--         ,sum(case when impacts >= 3 and responder <> 1 then weighting else null end) as non_responders_gt_3impacts
-- from #day_of_week
-- group by day_of_week
-- order by responders desc
-- 
-- 
-- -- just as i finish this someone tells me there is a date name function!
-- 
--
-- 
-- 
-- --ASPIRATIONAL
-- select day_of_week
--         ,sum(case when responder = 1 then weighting else null end) as responders
--         ,sum(case when impacts >= 3 and responder = 1 then weighting else null end) as responders_gt_3impacts
--         ,sum(case when responder <> 1 then weighting else null end) as non_responders
--         ,sum(case when impacts >= 3 and responder <> 1 then weighting else null end) as non_responders_gt_3impacts
-- from #day_of_week
-- where aspiration_target = 1
-- group by day_of_week
-- order by responders desc
-- 
-- 
-- 
-- 
-- --RESPONSE TARGET
-- select day_of_week
--         ,sum(case when responder = 1 then weighting else null end) as responders
--         ,sum(case when impacts >= 3 and responder = 1 then weighting else null end) as responders_gt_3impacts
--         ,sum(case when responder <> 1 then weighting else null end) as non_responders
--         ,sum(case when impacts >= 3 and responder <> 1 then weighting else null end) as non_responders_gt_3impacts
-- from #day_of_week
-- where response_target = 1
-- group by day_of_week
-- order by responders desc
-- 
-- 
-- --response_target_and_unknown
-- select day_of_week
--         ,sum(case when responder = 1 then weighting else null end) as responders
--         ,sum(case when impacts >= 3 and responder = 1 then weighting else null end) as responders_gt_3impacts
--         ,sum(case when responder <> 1 then weighting else null end) as non_responders
--         ,sum(case when impacts >= 3 and responder <> 1 then weighting else null end) as non_responders_gt_3impacts
-- from #day_of_week
-- where response_target_and_unknown = 1
-- group by day_of_week
-- order by responders desc
--



---------------------
-- OUTPUT: programme
---------------------

select top 10 datepart(weekday,broadcast_date)


from Barclays_spots_viewing_table_dump

drop table #programme

select  --bar.account_number,
        bar.cb_key_household
        ,epg_title as programme
        ,agg_channel_name as channel_name
        ,max(media_pack) as media_pack
        ,max(sales_house) as sales_house
        ,max(case
                when datepart(weekday,broadcast_date)=1 then 'Sun'
                when datepart(weekday,broadcast_date)=2 then 'Mon'
                when datepart(weekday,broadcast_date)=3 then 'Tue'
                when datepart(weekday,broadcast_date)=4 then 'Wed'
                when datepart(weekday,broadcast_date)=5 then 'Thu'
                when datepart(weekday,broadcast_date)=6 then 'Fri'
                when datepart(weekday,broadcast_date)=7 then 'Sat'
        end) as day
        ,impacts = COUNT(case when whole_spot = 1 then 1 else null end)
        ,max(barclays_responder) as responder
        ,max(bar.weighting)as weighting
        ,max(ves.aspiration_target) as aspiration_target
        ,max(ves.response_target) as response_target
        ,max(response_target_and_unknown) as response_target_and_unknown
        ,max(ABC1_TARGET) as ABC1_TARGET
into #programme
from Barclays_spots_viewing_table_dump bar
right join v081_Vespa_Universe_demographics ves
on ves.account_number = bar.account_number
where barclays_customer = 1
group by  bar.cb_key_household, programme, agg_channel_name



-- 1st lets get the genres
select programme
        ,sum(case when responder = 1 then weighting else null end) as responders
        ,sum(case when impacts >= 3 and responder = 1 then weighting else null end) as responders_gt_3impacts
        ,sum(case when responder <> 1 then weighting else null end) as non_responders
        ,sum(case when impacts >= 3 and responder <> 1 then weighting else null end) as non_responders_gt_3impacts
from #programme
group by programme
order by responders desc




--ASPIRATIONAL
select programme
        ,sum(case when responder = 1 then weighting else null end) as responders
        ,sum(case when impacts >= 3 and responder = 1 then weighting else null end) as responders_gt_3impacts
        ,sum(case when responder <> 1 then weighting else null end) as non_responders
        ,sum(case when impacts >= 3 and responder <> 1 then weighting else null end) as non_responders_gt_3impacts
from #programme
where aspiration_target = 1
group by programme
order by responders desc




--RESPONSE TARGET
select programme
        ,sum(case when responder = 1 then weighting else null end) as responders
        ,sum(case when impacts >= 3 and responder = 1 then weighting else null end) as responders_gt_3impacts
        ,sum(case when responder <> 1 then weighting else null end) as non_responders
        ,sum(case when impacts >= 3 and responder <> 1 then weighting else null end) as non_responders_gt_3impacts
from #programme
where RESPONSE_target = 1
group by programme
order by responders desc





--response_target_and_unknown
select programme
        ,sum(case when responder = 1 then weighting else null end) as responders
        ,sum(case when impacts >= 3 and responder = 1 then weighting else null end) as responders_gt_3impacts
        ,sum(case when responder <> 1 then weighting else null end) as non_responders
        ,sum(case when impacts >= 3 and responder <> 1 then weighting else null end) as non_responders_gt_3impacts
from #programme
where response_target_and_unknown = 1
group by programme
order by responders desc



-- this is a combination of outputs suited to susannes output sheets


-- this is a combination of outputs suited to susannes output sheets --
select programme, channel_name
        ,media_pack
        ,sales_house
        ,day
        ,sum(case when responder = 1 then weighting else null end) as responders
        ,sum(case when responder = 0 then weighting else null end) as non_responders
        ,sum(case when response_target_and_unknown = 1 then weighting else null end) as response_target_and_unknown
        ,sum(case when response_target = 1 then weighting else null end) as response_target
        ,sum(case when aspiration_target = 1 then weighting else null end) as taspiration_target
        ,sum(case when aspiration_target = 0 then weighting else null end) as non_aspiration_target
        ,sum(case when ABC1_TARGET = 1 then weighting else null end) as ABC1
        ,sum(case when responder = 1 and ABC1_TARGET = 1 then weighting else null end) as ABC1_responder
from #programme
group by  programme, channel_name
        ,media_pack
        ,sales_house,day
order by channel_name



-----------------------------------------------------------------------------------------
------------------------------------------------------------------------------


-- lets add a measure of part spots viewed


select  --bar.account_number,
        bar.cb_key_household
        ,part_impact = COUNT(case when whole_spot < 1 then 1 else null end)
        ,part_impacts_before_application = COUNT(case when whole_spot = 1 and viewing_date <= min_application_date then 1 else null end)
        ,max(barclays_responder) as barclays_responder -- household level
into #temp2
from Barclays_spots_viewing_table_dump bar
join v081_Vespa_Universe_demographics ves
on ves.cb_key_household = bar.cb_key_household -- lets match at household
group by --bar.account_number,
 bar.cb_key_household                       -- viewing data is at account level


-- now lets add this to the vespa demographics table:

alter table v081_Vespa_Universe_demographics
        add (part_impact integer default 0
           ,part_impacts_before_application integer default 0);      -- this will put any null accounts (i.e. not watched) into the 0 spots watch cells
                -- did not add scaled impacts - this is not needed yet


update v081_Vespa_Universe_demographics
        set ves.part_impact = tmp.part_impact
            ,ves.part_impacts_before_application = tmp.part_impacts_before_application
from v081_Vespa_Universe_demographics ves
join #temp2 tmp
on ves.cb_key_household = tmp.cb_key_household -- lets match at account level --


update v081_Vespa_Universe_demographics
        set part_impact = (case when part_impact is null then 0 else part_impact end)
                ,part_impacts_before_application = (case when part_impacts_before_application is null then 0 else part_impacts_before_application end)




-- lets take a look at the outputs -- THIS TABLE GIVES ALL IMPACTS AND SALES - DISREGARDING IMPACTS BEFORE/AFTER THE APPLICATION

select part_impact
       ,(sum(case when barclays_responder = 1 then weighting else null end)) as responder_HH
       -- *0.865 --applied unifmorm discounting factor to bring scaling in line with actual barclays responders
       ,(sum(case when barclays_responder = 1 and barclays_cash_isa > 0 then weighting else null end)) as responder_HH_Had_CASH_ISA
       ,(sum(case when barclays_responder = 1 and barclays_cash_isa  = 0 then weighting else null end)) as responder_HH_NO_CASH_ISA
       ,sum(case when barclays_responder <>1 then weighting else null end) as non_responder_HH
       ,sum(case when barclays_responder <> 1 and barclays_cash_isa > 0 then weighting else null end) as non_responder_HH_Had_CASH_ISA
       ,sum(case when barclays_responder <> 1 and (barclays_cash_isa  = 0 or barclays_cash_isa is null) then weighting else null end) as non_responder_HH_NO_CASH_ISA -- as this contains nulls
from v081_Vespa_Universe_demographics
where weighting is not null and impacts_total  = 0 or impacts_total is null
group by part_impact
order by part_impact





select top 10 * from Barclays_spots_viewing_table_dump

select sales_house
        ,media_pack
        ,agg_channel_name
into #table
from Barclays_spots_viewing_table_dump
group by sales_house
        ,media_pack
        ,agg_channel_name
order by sales_house
        ,media_pack
        ,agg_channel_name


select * from #table





select sum(case when ABC1_TARGET = 1 then weighting else null end) as ABC1
        ,sum(case when barclays_responder = 1 and ABC1_TARGET = 1 then weighting else null end) as ABC1_responder

from v081_Vespa_Universe_demographics
where barclays_customer = 1



