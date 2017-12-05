
-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
-- Project Name: Ethan Recommendation Engine POC
-- Author: Jason Thompson (jason.thompson@skyiq.co.uk)
-- Insight Collation: V284
-- Date: 10 June 2014


-- Business Brief:
--      The Ethan team have recruted approx 200 staff members to take part in their recommendation engine proof of concept
--      Viewing data will be supplied for these staff members (they have been added to the daily panel) on a daily basis to the Ethan team
--      This data will feed into their recommendation engine to produce recommendations based upon viewing

-------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------



-- Table to store account numbers/details of viewing to extract
create table ETHANRecEng_accounts (account_number varchar(20), subscriber_id bigint)
create lf index ind_acc on ETHANRecEng_accounts(account_number)

--- This script from Hoi
-- These are the staff accounts that have been added to the daily panels
-- This just needs to be run once to capture the accounts
insert into ETHANRecEng_accounts
select
  account_number
    , card_subscriber_id
from sk_prod.vespa_subscriber_status
where request_filename in (
    '2014-06-06-SKY-SKY-SPMSQ-P011-0001.xml'
  , '2014-06-06-SKY-SKY-SPMSQ-P012-0001.xml'
    )

-----------------------------------------------------------------------------
/******** RUN THIS SCRIPT ON A DAILY BASIS *********/
----------------------------------------------------------------------------------


--- A quick check on the number of accounts taking part in the poc
-- that have returned viewing data by day
select
        date(v.event_start_date_time_utc)
        ,count(distinct v.account_number)
        ,count(distinct v.subscriber_id)
        ,count(1) as event_count
from
        sk_prod.vespa_dp_prog_viewed_current v
inner join
        ETHANRecEng_accounts a
        on v.account_number = a.account_number
where
        date(v.event_start_date_time_utc) >= '2014-09-10' -- change this date as required
group by
        date(v.event_start_date_time_utc)


----------------------------------------------------------------------------------

--- Pull off the veiwing data
-- This should be run daily
-- You will need to change the date in the where clause

declare @the_date date
set @the_date = '2014-10-01' -- CHANGE THIS DATE AS REQUIRED

IF object_id('ETHAN_RecEng_viewing_data_former') IS NOT NULL
        BEGIN
            drop table ETHAN_RecEng_viewing_data_former
        END

--select *
--into ETHAN_RecEng_viewing_data_former
--from (
select
        cast(replace(v.programme_instance_name, ',', '') as varchar(200)) as programme_instance_name
        ,cast(replace(v.channel_name, ',','') as varchar(100)) as channel_name
        ,v.service_key
        ,v.service_type_description
        ,v.type_of_viewing_event
        ,v.account_number
        ,v.live_recorded
        ,v.broadcast_start_date_time_utc
        ,v.broadcast_end_date_time_utc
        ,v.event_start_date_time_utc
        ,v.event_end_date_time_utc
        ,v.instance_start_date_time_utc
        ,v.instance_end_date_time_utc
        ,v.subscriber_id
        ,v.capping_end_date_time_utc
from
        sk_prod.vespa_dp_prog_viewed_current v
inner join
        ETHANRecEng_accounts a
        on v.subscriber_id = a.subscriber_id
where
        type_of_viewing_event <> 'Non viewing event'
        and type_of_viewing_event is not null
        and date(v.event_start_date_time_utc) = @the_date

UNION

----------------------------------------------------------------------------------

-- We have been asked to also supply daily viewing data for a random selection of 800 from daily panel
-- These to be split by:
--      200 London
--      100 Scotland
--      100 Wales
--      150 Midlands
--      150 Merseyside (North West)
--      100 ROI (we don't have any ROI on panel so I have replaced with Meridian ex Channel Islands)
-- The random accounts put into the table ETHANRecEng_accounts_region800 on 01/09/2014
-- These should be put into the same file as the 200 original POC members

select
        cast(replace(v.programme_instance_name, ',', '') as varchar(200)) as programme_instance_name
        ,cast(replace(v.channel_name, ',','') as varchar(100)) as channel_name
        ,v.service_key
        ,v.service_type_description
        ,v.type_of_viewing_event
        ,v.account_number
        ,v.live_recorded
        ,v.broadcast_start_date_time_utc
        ,v.broadcast_end_date_time_utc
        ,v.event_start_date_time_utc
        ,v.event_end_date_time_utc
        ,v.instance_start_date_time_utc
        ,v.instance_end_date_time_utc
        ,v.subscriber_id
        ,v.capping_end_date_time_utc
from
        sk_prod.vespa_dp_prog_viewed_current v
inner join
        ETHANRecEng_accounts_region800 a
        on v.account_number = a.account_number
where
        type_of_viewing_event <> 'Non viewing event'
        and type_of_viewing_event is not null
        and date(v.event_start_date_time_utc) = @the_date

--) a
--commit





-- #########################################################

-- ###########################################
-- ### version that splits by time of day ####

--create time of day lookup table. This needs to be done once.
  IF object_id('ETHAN_RecEng_timeslot') IS NOT NULL
        BEGIN
            drop table ETHAN_RecEng_timeslot
        END

create table ETHAN_RecEng_timeslot (
        prepend         integer         NOT NULL,
        timeslot        integer         NOT NULL,
        description     varchar(32)     NOT NULL,
        config_desc     varchar(36)     NOT NULL,
        in_days         integer/*varchar(16)*/     NOT NULL,
        in_hours        integer/*varchar(24)*/     NOT NULL)

--  Prepend           Timeslot        Description                      config_desc                            in_days,        in_hours
insert into ETHAN_RecEng_timeslot values('10'               ,1              ,'Weekday preschool'             ,'Mon to Fri 0600:0900'                ,2     ,6)
insert into ETHAN_RecEng_timeslot values('10'               ,1              ,'Weekday preschool'             ,'Mon to Fri 0600:0900'                ,2     ,7)
insert into ETHAN_RecEng_timeslot values('10'               ,1              ,'Weekday preschool'             ,'Mon to Fri 0600:0900'                ,2     ,8)
insert into ETHAN_RecEng_timeslot values('10'               ,1              ,'Weekday preschool'             ,'Mon to Fri 0600:0900'                ,3     ,6)
insert into ETHAN_RecEng_timeslot values('10'               ,1              ,'Weekday preschool'             ,'Mon to Fri 0600:0900'                ,3     ,7)
insert into ETHAN_RecEng_timeslot values('10'               ,1              ,'Weekday preschool'             ,'Mon to Fri 0600:0900'                ,3     ,8)
insert into ETHAN_RecEng_timeslot values('10'               ,1              ,'Weekday preschool'             ,'Mon to Fri 0600:0900'                ,4     ,6)
insert into ETHAN_RecEng_timeslot values('10'               ,1              ,'Weekday preschool'             ,'Mon to Fri 0600:0900'                ,4     ,7)
insert into ETHAN_RecEng_timeslot values('10'               ,1              ,'Weekday preschool'             ,'Mon to Fri 0600:0900'                ,4     ,8)
insert into ETHAN_RecEng_timeslot values('10'               ,1              ,'Weekday preschool'             ,'Mon to Fri 0600:0900'                ,5     ,6)
insert into ETHAN_RecEng_timeslot values('10'               ,1              ,'Weekday preschool'             ,'Mon to Fri 0600:0900'                ,5     ,7)
insert into ETHAN_RecEng_timeslot values('10'               ,1              ,'Weekday preschool'             ,'Mon to Fri 0600:0900'                ,5     ,8)
insert into ETHAN_RecEng_timeslot values('10'               ,1              ,'Weekday preschool'             ,'Mon to Fri 0600:0900'                ,6     ,6)
insert into ETHAN_RecEng_timeslot values('10'               ,1              ,'Weekday preschool'             ,'Mon to Fri 0600:0900'                ,6     ,7)
insert into ETHAN_RecEng_timeslot values('10'               ,1              ,'Weekday preschool'             ,'Mon to Fri 0600:0900'                ,6     ,8)

insert into ETHAN_RecEng_timeslot values('20'               ,2              ,'Weekday Daytime'               ,'Mon to Fri 0900:1600'                ,2     ,9)
insert into ETHAN_RecEng_timeslot values('20'               ,2              ,'Weekday Daytime'               ,'Mon to Fri 0900:1600'                ,2     ,10)
insert into ETHAN_RecEng_timeslot values('20'               ,2              ,'Weekday Daytime'               ,'Mon to Fri 0900:1600'                ,2     ,11)
insert into ETHAN_RecEng_timeslot values('20'               ,2              ,'Weekday Daytime'               ,'Mon to Fri 0900:1600'                ,2     ,12)
insert into ETHAN_RecEng_timeslot values('20'               ,2              ,'Weekday Daytime'               ,'Mon to Fri 0900:1600'                ,2     ,13)
insert into ETHAN_RecEng_timeslot values('20'               ,2              ,'Weekday Daytime'               ,'Mon to Fri 0900:1600'                ,2     ,14)
insert into ETHAN_RecEng_timeslot values('20'               ,2              ,'Weekday Daytime'               ,'Mon to Fri 0900:1600'                ,2     ,15)
insert into ETHAN_RecEng_timeslot values('20'               ,2              ,'Weekday Daytime'               ,'Mon to Fri 0900:1600'                ,3     ,9)
insert into ETHAN_RecEng_timeslot values('20'               ,2              ,'Weekday Daytime'               ,'Mon to Fri 0900:1600'                ,3     ,10)
insert into ETHAN_RecEng_timeslot values('20'               ,2              ,'Weekday Daytime'               ,'Mon to Fri 0900:1600'                ,3     ,11)
insert into ETHAN_RecEng_timeslot values('20'               ,2              ,'Weekday Daytime'               ,'Mon to Fri 0900:1600'                ,3     ,12)
insert into ETHAN_RecEng_timeslot values('20'               ,2              ,'Weekday Daytime'               ,'Mon to Fri 0900:1600'                ,3     ,13)
insert into ETHAN_RecEng_timeslot values('20'               ,2              ,'Weekday Daytime'               ,'Mon to Fri 0900:1600'                ,3     ,14)
insert into ETHAN_RecEng_timeslot values('20'               ,2              ,'Weekday Daytime'               ,'Mon to Fri 0900:1600'                ,3     ,15)
insert into ETHAN_RecEng_timeslot values('20'               ,2              ,'Weekday Daytime'               ,'Mon to Fri 0900:1600'                ,4     ,9)
insert into ETHAN_RecEng_timeslot values('20'               ,2              ,'Weekday Daytime'               ,'Mon to Fri 0900:1600'                ,4     ,10)
insert into ETHAN_RecEng_timeslot values('20'               ,2              ,'Weekday Daytime'               ,'Mon to Fri 0900:1600'                ,4     ,11)
insert into ETHAN_RecEng_timeslot values('20'               ,2              ,'Weekday Daytime'               ,'Mon to Fri 0900:1600'                ,4     ,12)
insert into ETHAN_RecEng_timeslot values('20'               ,2              ,'Weekday Daytime'               ,'Mon to Fri 0900:1600'                ,4     ,13)
insert into ETHAN_RecEng_timeslot values('20'               ,2              ,'Weekday Daytime'               ,'Mon to Fri 0900:1600'                ,4     ,14)
insert into ETHAN_RecEng_timeslot values('20'               ,2              ,'Weekday Daytime'               ,'Mon to Fri 0900:1600'                ,4     ,15)
insert into ETHAN_RecEng_timeslot values('20'               ,2              ,'Weekday Daytime'               ,'Mon to Fri 0900:1600'                ,5     ,9)
insert into ETHAN_RecEng_timeslot values('20'               ,2              ,'Weekday Daytime'               ,'Mon to Fri 0900:1600'                ,5     ,10)
insert into ETHAN_RecEng_timeslot values('20'               ,2              ,'Weekday Daytime'               ,'Mon to Fri 0900:1600'                ,5     ,11)
insert into ETHAN_RecEng_timeslot values('20'               ,2              ,'Weekday Daytime'               ,'Mon to Fri 0900:1600'                ,5     ,12)
insert into ETHAN_RecEng_timeslot values('20'               ,2              ,'Weekday Daytime'               ,'Mon to Fri 0900:1600'                ,5     ,13)
insert into ETHAN_RecEng_timeslot values('20'               ,2              ,'Weekday Daytime'               ,'Mon to Fri 0900:1600'                ,5     ,14)
insert into ETHAN_RecEng_timeslot values('20'               ,2              ,'Weekday Daytime'               ,'Mon to Fri 0900:1600'                ,5     ,15)
insert into ETHAN_RecEng_timeslot values('20'               ,2              ,'Weekday Daytime'               ,'Mon to Fri 0900:1600'                ,6     ,9)
insert into ETHAN_RecEng_timeslot values('20'               ,2              ,'Weekday Daytime'               ,'Mon to Fri 0900:1600'                ,6     ,10)
insert into ETHAN_RecEng_timeslot values('20'               ,2              ,'Weekday Daytime'               ,'Mon to Fri 0900:1600'                ,6     ,11)
insert into ETHAN_RecEng_timeslot values('20'               ,2              ,'Weekday Daytime'               ,'Mon to Fri 0900:1600'                ,6     ,12)
insert into ETHAN_RecEng_timeslot values('20'               ,2              ,'Weekday Daytime'               ,'Mon to Fri 0900:1600'                ,6     ,13)
insert into ETHAN_RecEng_timeslot values('20'               ,2              ,'Weekday Daytime'               ,'Mon to Fri 0900:1600'                ,6     ,14)
insert into ETHAN_RecEng_timeslot values('20'               ,2              ,'Weekday Daytime'               ,'Mon to Fri 0900:1600'                ,6     ,15)

insert into ETHAN_RecEng_timeslot values('30'               ,3              ,'Weekend morning'               ,'Sat and Sun 0600:0900'               ,7           ,6)
insert into ETHAN_RecEng_timeslot values('30'               ,3              ,'Weekend morning'               ,'Sat and Sun 0600:0900'               ,7           ,7)
insert into ETHAN_RecEng_timeslot values('30'               ,3              ,'Weekend morning'               ,'Sat and Sun 0600:0900'               ,7           ,8)
insert into ETHAN_RecEng_timeslot values('30'               ,3              ,'Weekend morning'               ,'Sat and Sun 0600:0900'               ,1           ,6)
insert into ETHAN_RecEng_timeslot values('30'               ,3              ,'Weekend morning'               ,'Sat and Sun 0600:0900'               ,1           ,7)
insert into ETHAN_RecEng_timeslot values('30'               ,3              ,'Weekend morning'               ,'Sat and Sun 0600:0900'               ,1           ,8)

insert into ETHAN_RecEng_timeslot values('40'               ,4              ,'Weekend Daytime/Early evening' ,'Sat and Sun 0900:1900'               ,7           ,9)
insert into ETHAN_RecEng_timeslot values('40'               ,4              ,'Weekend Daytime/Early evening' ,'Sat and Sun 0900:1900'               ,7           ,10)
insert into ETHAN_RecEng_timeslot values('40'               ,4              ,'Weekend Daytime/Early evening' ,'Sat and Sun 0900:1900'               ,7           ,11)
insert into ETHAN_RecEng_timeslot values('40'               ,4              ,'Weekend Daytime/Early evening' ,'Sat and Sun 0900:1900'               ,7           ,12)
insert into ETHAN_RecEng_timeslot values('40'               ,4              ,'Weekend Daytime/Early evening' ,'Sat and Sun 0900:1900'               ,7           ,13)
insert into ETHAN_RecEng_timeslot values('40'               ,4              ,'Weekend Daytime/Early evening' ,'Sat and Sun 0900:1900'               ,7           ,14)
insert into ETHAN_RecEng_timeslot values('40'               ,4              ,'Weekend Daytime/Early evening' ,'Sat and Sun 0900:1900'               ,7           ,15)
insert into ETHAN_RecEng_timeslot values('40'               ,4              ,'Weekend Daytime/Early evening' ,'Sat and Sun 0900:1900'               ,7           ,16)
insert into ETHAN_RecEng_timeslot values('40'               ,4              ,'Weekend Daytime/Early evening' ,'Sat and Sun 0900:1900'               ,7           ,17)
insert into ETHAN_RecEng_timeslot values('40'               ,4              ,'Weekend Daytime/Early evening' ,'Sat and Sun 0900:1900'               ,7           ,18)
insert into ETHAN_RecEng_timeslot values('40'               ,4              ,'Weekend Daytime/Early evening' ,'Sat and Sun 0900:1900'               ,1           ,9)
insert into ETHAN_RecEng_timeslot values('40'               ,4              ,'Weekend Daytime/Early evening' ,'Sat and Sun 0900:1900'               ,1           ,10)
insert into ETHAN_RecEng_timeslot values('40'               ,4              ,'Weekend Daytime/Early evening' ,'Sat and Sun 0900:1900'               ,1           ,11)
insert into ETHAN_RecEng_timeslot values('40'               ,4              ,'Weekend Daytime/Early evening' ,'Sat and Sun 0900:1900'               ,1           ,12)
insert into ETHAN_RecEng_timeslot values('40'               ,4              ,'Weekend Daytime/Early evening' ,'Sat and Sun 0900:1900'               ,1           ,13)
insert into ETHAN_RecEng_timeslot values('40'               ,4              ,'Weekend Daytime/Early evening' ,'Sat and Sun 0900:1900'               ,1           ,14)
insert into ETHAN_RecEng_timeslot values('40'               ,4              ,'Weekend Daytime/Early evening' ,'Sat and Sun 0900:1900'               ,1           ,15)
insert into ETHAN_RecEng_timeslot values('40'               ,4              ,'Weekend Daytime/Early evening' ,'Sat and Sun 0900:1900'               ,1           ,16)
insert into ETHAN_RecEng_timeslot values('40'               ,4              ,'Weekend Daytime/Early evening' ,'Sat and Sun 0900:1900'               ,1           ,17)
insert into ETHAN_RecEng_timeslot values('40'               ,4              ,'Weekend Daytime/Early evening' ,'Sat and Sun 0900:1900'               ,1           ,18)

insert into ETHAN_RecEng_timeslot values('50'               ,5              ,'Weekday afternoon'             ,'Mon to Fri 1600:1900'                ,2     ,16)
insert into ETHAN_RecEng_timeslot values('50'               ,5              ,'Weekday afternoon'             ,'Mon to Fri 1600:1900'                ,2     ,17)
insert into ETHAN_RecEng_timeslot values('50'               ,5              ,'Weekday afternoon'             ,'Mon to Fri 1600:1900'                ,2     ,18)
insert into ETHAN_RecEng_timeslot values('50'               ,5              ,'Weekday afternoon'             ,'Mon to Fri 1600:1900'                ,3     ,16)
insert into ETHAN_RecEng_timeslot values('50'               ,5              ,'Weekday afternoon'             ,'Mon to Fri 1600:1900'                ,3     ,17)
insert into ETHAN_RecEng_timeslot values('50'               ,5              ,'Weekday afternoon'             ,'Mon to Fri 1600:1900'                ,3     ,18)
insert into ETHAN_RecEng_timeslot values('50'               ,5              ,'Weekday afternoon'             ,'Mon to Fri 1600:1900'                ,4     ,16)
insert into ETHAN_RecEng_timeslot values('50'               ,5              ,'Weekday afternoon'             ,'Mon to Fri 1600:1900'                ,4     ,17)
insert into ETHAN_RecEng_timeslot values('50'               ,5              ,'Weekday afternoon'             ,'Mon to Fri 1600:1900'                ,4     ,18)
insert into ETHAN_RecEng_timeslot values('50'               ,5              ,'Weekday afternoon'             ,'Mon to Fri 1600:1900'                ,5     ,16)
insert into ETHAN_RecEng_timeslot values('50'               ,5              ,'Weekday afternoon'             ,'Mon to Fri 1600:1900'                ,5     ,17)
insert into ETHAN_RecEng_timeslot values('50'               ,5              ,'Weekday afternoon'             ,'Mon to Fri 1600:1900'                ,5     ,18)
insert into ETHAN_RecEng_timeslot values('50'               ,5              ,'Weekday afternoon'             ,'Mon to Fri 1600:1900'                ,6     ,16)
insert into ETHAN_RecEng_timeslot values('50'               ,5              ,'Weekday afternoon'             ,'Mon to Fri 1600:1900'                ,6     ,17)
insert into ETHAN_RecEng_timeslot values('50'               ,5              ,'Weekday afternoon'             ,'Mon to Fri 1600:1900'                ,6     ,18)

insert into ETHAN_RecEng_timeslot values('60'               ,6              ,'Weekday Early evening'                 ,'Mon to Fri 1900:2100'   ,2 ,19)
insert into ETHAN_RecEng_timeslot values('60'               ,6              ,'Weekday Early evening'                 ,'Mon to Fri 1900:2100'   ,2 ,20)
insert into ETHAN_RecEng_timeslot values('60'               ,6              ,'Weekday Early evening'                 ,'Mon to Fri 1900:2100'   ,3 ,19)
insert into ETHAN_RecEng_timeslot values('60'               ,6              ,'Weekday Early evening'                 ,'Mon to Fri 1900:2100'   ,3 ,20)
insert into ETHAN_RecEng_timeslot values('60'               ,6              ,'Weekday Early evening'                 ,'Mon to Fri 1900:2100'   ,4 ,19)
insert into ETHAN_RecEng_timeslot values('60'               ,6              ,'Weekday Early evening'                 ,'Mon to Fri 1900:2100'   ,4 ,20)
insert into ETHAN_RecEng_timeslot values('60'               ,6              ,'Weekday Early evening'                 ,'Mon to Fri 1900:2100'   ,5 ,19)
insert into ETHAN_RecEng_timeslot values('60'               ,6              ,'Weekday Early evening'                 ,'Mon to Fri 1900:2100'   ,5 ,20)
insert into ETHAN_RecEng_timeslot values('60'               ,6              ,'Weekday Early evening'                 ,'Mon to Fri 1900:2100'   ,6 ,19)
insert into ETHAN_RecEng_timeslot values('60'               ,6              ,'Weekday Early evening'                 ,'Mon to Fri 1900:2100'   ,6 ,20)

insert into ETHAN_RecEng_timeslot values('70'               ,7              ,'Weekend Early evening'                 ,'Sat and Sun 1900:2100'   ,7 ,19)
insert into ETHAN_RecEng_timeslot values('70'               ,7              ,'Weekend Early evening'                 ,'Sat and Sun 1900:2100'   ,7 ,20)
insert into ETHAN_RecEng_timeslot values('70'               ,7              ,'Weekend Early evening'                 ,'Sat and Sun 1900:2100'   ,1 ,19)
insert into ETHAN_RecEng_timeslot values('70'               ,7              ,'Weekend Early evening'                 ,'Sat and Sun 1900:2100'   ,1 ,20)

insert into ETHAN_RecEng_timeslot values('80'               ,8              ,'Weekday Primetime'                     ,'Mon to Thu 2100:0000'   ,2 ,21)
insert into ETHAN_RecEng_timeslot values('80'               ,8              ,'Weekday Primetime'                     ,'Mon to Thu 2100:0000'   ,2 ,22)
insert into ETHAN_RecEng_timeslot values('80'               ,8              ,'Weekday Primetime'                     ,'Mon to Thu 2100:0000'   ,2 ,23)
insert into ETHAN_RecEng_timeslot values('80'               ,8              ,'Weekday Primetime'                     ,'Mon to Thu 2100:0000'   ,3 ,21)
insert into ETHAN_RecEng_timeslot values('80'               ,8              ,'Weekday Primetime'                     ,'Mon to Thu 2100:0000'   ,3 ,22)
insert into ETHAN_RecEng_timeslot values('80'               ,8              ,'Weekday Primetime'                     ,'Mon to Thu 2100:0000'   ,3 ,23)
insert into ETHAN_RecEng_timeslot values('80'               ,8              ,'Weekday Primetime'                     ,'Mon to Thu 2100:0000'   ,4 ,21)
insert into ETHAN_RecEng_timeslot values('80'               ,8              ,'Weekday Primetime'                     ,'Mon to Thu 2100:0000'   ,4 ,22)
insert into ETHAN_RecEng_timeslot values('80'               ,8              ,'Weekday Primetime'                     ,'Mon to Thu 2100:0000'   ,4 ,23)
insert into ETHAN_RecEng_timeslot values('80'               ,8              ,'Weekday Primetime'                     ,'Mon to Thu 2100:0000'   ,5 ,21)
insert into ETHAN_RecEng_timeslot values('80'               ,8              ,'Weekday Primetime'                     ,'Mon to Thu 2100:0000'   ,5 ,22)
insert into ETHAN_RecEng_timeslot values('80'               ,8              ,'Weekday Primetime'                     ,'Mon to Thu 2100:0000'   ,5 ,23)

insert into ETHAN_RecEng_timeslot values('90'               ,9              ,'Weekend Primetime'                     ,'Fri to Sun 2100:0000'   ,6 ,21)
insert into ETHAN_RecEng_timeslot values('90'               ,9              ,'Weekend Primetime'                     ,'Fri to Sun 2100:0000'   ,6 ,22)
insert into ETHAN_RecEng_timeslot values('90'               ,9              ,'Weekend Primetime'                     ,'Fri to Sun 2100:0000'   ,6 ,23)
insert into ETHAN_RecEng_timeslot values('90'               ,9              ,'Weekend Primetime'                     ,'Fri to Sun 2100:0000'   ,7 ,21)
insert into ETHAN_RecEng_timeslot values('90'               ,9              ,'Weekend Primetime'                     ,'Fri to Sun 2100:0000'   ,7 ,22)
insert into ETHAN_RecEng_timeslot values('90'               ,9              ,'Weekend Primetime'                     ,'Fri to Sun 2100:0000'   ,7 ,23)
insert into ETHAN_RecEng_timeslot values('90'               ,9              ,'Weekend Primetime'                     ,'Fri to Sun 2100:0000'   ,1 ,21)
insert into ETHAN_RecEng_timeslot values('90'               ,9              ,'Weekend Primetime'                     ,'Fri to Sun 2100:0000'   ,1 ,22)
insert into ETHAN_RecEng_timeslot values('90'               ,9              ,'Weekend Primetime'                     ,'Fri to Sun 2100:0000'   ,1 ,23)

insert into ETHAN_RecEng_timeslot values('100'               ,10             ,'Overnight'                     ,'Mon to Fri, Sat and Sun 0000:0600'   ,2 ,0)
insert into ETHAN_RecEng_timeslot values('100'               ,10             ,'Overnight'                     ,'Mon to Fri, Sat and Sun 0000:0600'   ,2 ,1)
insert into ETHAN_RecEng_timeslot values('100'               ,10             ,'Overnight'                     ,'Mon to Fri, Sat and Sun 0000:0600'   ,2 ,2)
insert into ETHAN_RecEng_timeslot values('100'               ,10             ,'Overnight'                     ,'Mon to Fri, Sat and Sun 0000:0600'   ,2 ,3)
insert into ETHAN_RecEng_timeslot values('100'               ,10             ,'Overnight'                     ,'Mon to Fri, Sat and Sun 0000:0600'   ,2 ,4)
insert into ETHAN_RecEng_timeslot values('100'               ,10             ,'Overnight'                     ,'Mon to Fri, Sat and Sun 0000:0600'   ,2 ,5)
insert into ETHAN_RecEng_timeslot values('100'               ,10             ,'Overnight'                     ,'Mon to Fri, Sat and Sun 0000:0600'   ,3 ,0)
insert into ETHAN_RecEng_timeslot values('100'               ,10             ,'Overnight'                     ,'Mon to Fri, Sat and Sun 0000:0600'   ,3 ,1)
insert into ETHAN_RecEng_timeslot values('100'               ,10             ,'Overnight'                     ,'Mon to Fri, Sat and Sun 0000:0600'   ,3 ,2)
insert into ETHAN_RecEng_timeslot values('100'               ,10             ,'Overnight'                     ,'Mon to Fri, Sat and Sun 0000:0600'   ,3 ,3)
insert into ETHAN_RecEng_timeslot values('100'               ,10             ,'Overnight'                     ,'Mon to Fri, Sat and Sun 0000:0600'   ,3 ,4)
insert into ETHAN_RecEng_timeslot values('100'               ,10             ,'Overnight'                     ,'Mon to Fri, Sat and Sun 0000:0600'   ,3 ,5)
insert into ETHAN_RecEng_timeslot values('100'               ,10             ,'Overnight'                     ,'Mon to Fri, Sat and Sun 0000:0600'   ,4 ,0)
insert into ETHAN_RecEng_timeslot values('100'               ,10             ,'Overnight'                     ,'Mon to Fri, Sat and Sun 0000:0600'   ,4 ,1)
insert into ETHAN_RecEng_timeslot values('100'               ,10             ,'Overnight'                     ,'Mon to Fri, Sat and Sun 0000:0600'   ,4 ,2)
insert into ETHAN_RecEng_timeslot values('100'               ,10             ,'Overnight'                     ,'Mon to Fri, Sat and Sun 0000:0600'   ,4 ,3)
insert into ETHAN_RecEng_timeslot values('100'               ,10             ,'Overnight'                     ,'Mon to Fri, Sat and Sun 0000:0600'   ,4 ,4)
insert into ETHAN_RecEng_timeslot values('100'               ,10             ,'Overnight'                     ,'Mon to Fri, Sat and Sun 0000:0600'   ,4 ,5)
insert into ETHAN_RecEng_timeslot values('100'               ,10             ,'Overnight'                     ,'Mon to Fri, Sat and Sun 0000:0600'   ,5 ,0)
insert into ETHAN_RecEng_timeslot values('100'               ,10             ,'Overnight'                     ,'Mon to Fri, Sat and Sun 0000:0600'   ,5 ,1)
insert into ETHAN_RecEng_timeslot values('100'               ,10             ,'Overnight'                     ,'Mon to Fri, Sat and Sun 0000:0600'   ,5 ,2)
insert into ETHAN_RecEng_timeslot values('100'               ,10             ,'Overnight'                     ,'Mon to Fri, Sat and Sun 0000:0600'   ,5 ,3)
insert into ETHAN_RecEng_timeslot values('100'               ,10             ,'Overnight'                     ,'Mon to Fri, Sat and Sun 0000:0600'   ,5 ,4)
insert into ETHAN_RecEng_timeslot values('100'               ,10             ,'Overnight'                     ,'Mon to Fri, Sat and Sun 0000:0600'   ,5 ,5)
insert into ETHAN_RecEng_timeslot values('100'               ,10             ,'Overnight'                     ,'Mon to Fri, Sat and Sun 0000:0600'   ,6 ,0)
insert into ETHAN_RecEng_timeslot values('100'               ,10             ,'Overnight'                     ,'Mon to Fri, Sat and Sun 0000:0600'   ,6 ,1)
insert into ETHAN_RecEng_timeslot values('100'               ,10             ,'Overnight'                     ,'Mon to Fri, Sat and Sun 0000:0600'   ,6 ,2)
insert into ETHAN_RecEng_timeslot values('100'               ,10             ,'Overnight'                     ,'Mon to Fri, Sat and Sun 0000:0600'   ,6 ,3)
insert into ETHAN_RecEng_timeslot values('100'               ,10             ,'Overnight'                     ,'Mon to Fri, Sat and Sun 0000:0600'   ,6 ,4)
insert into ETHAN_RecEng_timeslot values('100'               ,10             ,'Overnight'                     ,'Mon to Fri, Sat and Sun 0000:0600'   ,6 ,5)
insert into ETHAN_RecEng_timeslot values('100'               ,10             ,'Overnight'                     ,'Mon to Fri, Sat and Sun 0000:0600'   ,7 ,0)
insert into ETHAN_RecEng_timeslot values('100'               ,10             ,'Overnight'                     ,'Mon to Fri, Sat and Sun 0000:0600'   ,7 ,1)
insert into ETHAN_RecEng_timeslot values('100'               ,10             ,'Overnight'                     ,'Mon to Fri, Sat and Sun 0000:0600'   ,7 ,2)
insert into ETHAN_RecEng_timeslot values('100'               ,10             ,'Overnight'                     ,'Mon to Fri, Sat and Sun 0000:0600'   ,7 ,3)
insert into ETHAN_RecEng_timeslot values('100'               ,10             ,'Overnight'                     ,'Mon to Fri, Sat and Sun 0000:0600'   ,7 ,4)
insert into ETHAN_RecEng_timeslot values('100'               ,10             ,'Overnight'                     ,'Mon to Fri, Sat and Sun 0000:0600'   ,7 ,5)
insert into ETHAN_RecEng_timeslot values('100'               ,10             ,'Overnight'                     ,'Mon to Fri, Sat and Sun 0000:0600'   ,1 ,0)
insert into ETHAN_RecEng_timeslot values('100'               ,10             ,'Overnight'                     ,'Mon to Fri, Sat and Sun 0000:0600'   ,1 ,1)
insert into ETHAN_RecEng_timeslot values('100'               ,10             ,'Overnight'                     ,'Mon to Fri, Sat and Sun 0000:0600'   ,1 ,2)
insert into ETHAN_RecEng_timeslot values('100'               ,10             ,'Overnight'                     ,'Mon to Fri, Sat and Sun 0000:0600'   ,1 ,3)
insert into ETHAN_RecEng_timeslot values('100'               ,10             ,'Overnight'                     ,'Mon to Fri, Sat and Sun 0000:0600'   ,1 ,4)
insert into ETHAN_RecEng_timeslot values('100'               ,10             ,'Overnight'                     ,'Mon to Fri, Sat and Sun 0000:0600'   ,1 ,5)

commit


create HG index ETHAN_RecEng_timeslot_hr_idx on ETHAN_RecEng_timeslot(in_hours)
create HG index ETHAN_RecEng_timeslot_day_idx on ETHAN_RecEng_timeslot(in_days)


/*
--sunday is day 1
select dow(today())


select *
  from ETHAN_RecEng_timeslot

select prepend, description, config_desc
  from ETHAN_RecEng_timeslot
group by prepend, description, config_desc
order by prepend, description, config_desc
*/


----------------------------------------------------------------------------------
--## MAIN START -->>

--- Pull off the veiwing data
-- This should be run daily
-- You will need to change the date in the where clause

declare @the_date date
set @the_date = '2014-10-01' -- CHANGE THIS DATE AS REQUIRED

IF object_id('EthanRec_VESPA_CALENDAR_section_tmp') IS NOT NULL
        BEGIN
            drop table EthanRec_VESPA_CALENDAR_section_tmp
        END

 --Period of time we are interested in from the viewing data
    select c.utc_day_date, c.utc_time_hours,
           c.local_day_date, c.local_time_hours,
           c.daylight_savings_flag
      into EthanRec_VESPA_CALENDAR_section_tmp
      FROM sk_prod.VESPA_CALENDAR c -- could create a tempory table just with the hours we are interested in for this load, so that the inner joins on these only
     --where local_day_date between dateformat(cast('2014-08-28' as date), 'YYYY-MM-DD') and dateformat(cast('2014-09-28' as date), 'YYYY-MM-DD')
     where local_day_date = dateformat(cast(@the_date as date), 'YYYY-MM-DD')
     order by c.utc_day_date, c.utc_time_hours


create index VESPA_CALENDAR_section_tmp_date_idx on EthanRec_VESPA_CALENDAR_section_tmp(utc_day_date)
create index VESPA_CALENDAR_section_tmp_hrs_idx  on EthanRec_VESPA_CALENDAR_section_tmp(utc_time_hours)




  IF object_id('ETHAN_RecEng_viewing_data') IS NOT NULL
        BEGIN
            drop table ETHAN_RecEng_viewing_data
        END

/*
select top 100 *
from sk_prod.vespa_dp_prog_viewed_current
where channel_name like '%,%'
*/

select
        cast(replace(v.programme_instance_name, ',', '') as varchar(300)) as programme_instance_name
        ,cast(replace(v.channel_name, ',','') as varchar(200)) as channel_name
        ,v.service_key
        ,v.service_type_description
        ,v.type_of_viewing_event
        ,v.account_number
        ,v.live_recorded
        ,v.broadcast_start_date_time_utc
        ,v.broadcast_end_date_time_utc
        ,v.event_start_date_time_utc
        ,v.event_end_date_time_utc
        ,v.instance_start_date_time_utc
        ,v.instance_end_date_time_utc
        ,v.subscriber_id
        ,v.capping_end_date_time_utc
 into ETHAN_RecEng_viewing_data
from
        sk_prod.vespa_dp_prog_viewed_current v
  INNER JOIN EthanRec_VESPA_CALENDAR_section_tmp c
    on (cast(dateformat(v.instance_start_date_time_utc, 'YYYY-MM-DD') as date) = c.utc_day_date)
   and (datepart(hh, v.instance_start_date_time_utc) = c.utc_time_hours)
  INNER JOIN
        ETHANRecEng_accounts a
        on v.subscriber_id = a.subscriber_id
where type_of_viewing_event <> 'Non viewing event'
  and type_of_viewing_event is not null
 -- and c.local_day_date = @the_date

--UNION

----------------------------------------------------------------------------------

-- We have been asked to also supply daily viewing data for a random selection of 800 from daily panel
-- These to be split by:
--      200 London
--      100 Scotland
--      100 Wales
--      150 Midlands
--      150 Merseyside (North West)
--      100 ROI (we don't have any ROI on panel so I have replaced with Meridian ex Channel Islands)
-- The random accounts put into the table ETHANRecEng_accounts_region800 on 01/09/2014
-- These should be put into the same file as the 200 original POC members

INSERT into ETHAN_RecEng_viewing_data
select
        cast(replace(v.programme_instance_name, ',', '') as varchar(300)) as programme_instance_name
        ,cast(replace(v.channel_name, ',','') as varchar(200)) as channel_name
        ,v.service_key
        ,v.service_type_description
        ,v.type_of_viewing_event
        ,v.account_number
        ,v.live_recorded
        ,v.broadcast_start_date_time_utc
        ,v.broadcast_end_date_time_utc
        ,v.event_start_date_time_utc
        ,v.event_end_date_time_utc
        ,v.instance_start_date_time_utc
        ,v.instance_end_date_time_utc
        ,v.subscriber_id
        ,v.capping_end_date_time_utc
  from
        sk_prod.vespa_dp_prog_viewed_current v
  INNER JOIN EthanRec_VESPA_CALENDAR_section_tmp c
    on (cast(dateformat(v.instance_start_date_time_utc, 'YYYY-MM-DD') as date) = c.utc_day_date)
   and (datepart(hh, v.instance_start_date_time_utc) = c.utc_time_hours)
  INNER JOIN
        ETHANRecEng_accounts_region800 a
        on v.account_number = a.account_number
 where type_of_viewing_event <> 'Non viewing event'
   and type_of_viewing_event is not null
--   and c.local_day_date = @the_date


commit

--48:59 with all calendar table
--54:00 with part! - possible change to same second join as Comscore code?
--59:36 with date then account join

--- this
/*select dateformat(broadcast_start_date_time_utc, 'YYYY-MM-DD'), min(broadcast_start_date_time_utc), min(instance_start_date_time_utc)
  from sk_prod.vespa_dp_prog_viewed_current v

  INNER JOIN sk_prod.VESPA_CALENDAR c
    on cast(dateformat(v.broadcast_start_date_time_utc, 'YYYY-MM-DD') as date) = c.utc_day_date
   and datepart(dd, v.broadcast_start_date_time_utc) = c.utc_time_hours
   and c.local_day_date = '2014-09-01'


--now create slot output
select *
from ETHAN_RecEng_timeslot
*/


select  programme_instance_name,
        channel_name,
        service_key,
        service_type_description,
        type_of_viewing_event,
        prepend||account_number as account_number,
        live_recorded,
        --dow(v.broadcast_start_date_time_utc) dow,
        --datepart(hh, v.broadcast_start_date_time_utc) hours,
        broadcast_start_date_time_utc,
        broadcast_end_date_time_utc,
        event_start_date_time_utc,
        event_end_date_time_utc,
        instance_start_date_time_utc,
        instance_end_date_time_utc,
        subscriber_id,
        capping_end_date_time_utc
--select *
from ETHAN_RecEng_viewing_data v
        LEFT JOIN ETHAN_RecEng_timeslot s
on dow(v.instance_start_date_time_utc) = s.in_days
  and datepart(hh, v.instance_start_date_time_utc) = s.in_hours


--31837 Row(s) affected
--  vs.
-- 40461 Row(s) affected (former)



---##########


-- Run this every two weeks to refresh package entitlement info
--next due 13th October

declare @the_date date
set @the_date = '2014-09-23' -- CHANGE THIS DATE AS REQUIRED


 if object_id('VAggr_tmp_Account_Portfolio_Snapshot') is not null drop table VAggr_tmp_Account_Portfolio_Snapshot
      select
            base.Account_Number,

              -- ##### DTV subscription #####
            max(case
                  when csh.subscription_sub_type = 'DTV Primary Viewing' and csh.status_code in ('AC', 'PC', 'AB') then 1
                    else 0
                end) as DTV_Sub,

              -- ##### Original #####
            max(case
                  when cel.mixes = 0                                                                then 1      -- Original
                  when cel.mixes = 1 and (cel.style_culture = 1 or cel.variety = 1)                 then 1      -- Original
                  when cel.mixes = 2 and (cel.style_culture + cel.variety = 2)                      then 1      -- Original
                    else 0
                end) as DTV_Pack_Original,

              -- ##### Variety #####
            max(case
                  when cel.mixes = 0                                                                then 0      -- Original
                  when cel.mixes = 1 and (cel.style_culture = 1 or cel.variety = 1)                 then 0      -- Original
                  when cel.mixes = 2 and (cel.style_culture + cel.variety = 2)                      then 0      -- Original
                  when cel.product_sk in      ( 43672,43669,43670,43664,43667,43663,43668,43673,
                                                43677,43674,43676,43666,43665,43662,43671,43675)    then 0      -- Family
                  when cel.mixes > 0                                                                then 1      -- Variety
                    else 0
                end) as DTV_Pack_Variety,

              -- ##### Family #####
            max(case
                  when cel.mixes = 0                                                                then 0      -- Original
                  when cel.mixes = 1 and (cel.style_culture = 1 or cel.variety = 1)                 then 0      -- Original
                  when cel.mixes = 2 and (cel.style_culture + cel.variety = 2)                      then 0      -- Original
                  when cel.product_sk in      ( 43672,43669,43670,43664,43667,43663,43668,43673,
                                                43677,43674,43676,43666,43665,43662,43671,43675)    then 1      -- Family
                  when cel.mixes > 0                                                                then 0      -- Variety
                    else 0
                end) as DTV_Pack_Family,

              -- ##### Sports premium #####
            max(case
                  when cel.prem_sports > 0                                                          then 1
                    else 0
                end) as Prem_Sports,

              -- ##### Movies premium #####
            max(case
                  when cel.prem_movies > 0                                                          then 1
                    else 0
                end) as Prem_Movies,

              -- ##### HD subscription #####
            max(case
                  when csh.subscription_sub_type = 'DTV HD' and csh.status_code in ('AC', 'PC', 'AB') then 1
                    else 0
                end) as HD_Sub,

              -- ##### 3D TV subscription #####
            max(case
                  when csh.subscription_sub_type = '3DTV' and csh.status_code in ('AC', 'PC', 'AB') then 1
                    else 0
                end) as TV3D_Sub,

              -- ##### ESPN subscription #####
            max(case
                  when csh.subscription_sub_type = 'ESPN' and csh.status_code in ('AC', 'PC', 'AB') then 1
                    else 0
                end) as ESPN_Sub,

              -- ##### DTV Chelsea TV subscription #####
            max(case
                  when csh.subscription_sub_type = 'DTV Chelsea TV' and csh.status_code in ('AC', 'PC', 'AB') then 1
                    else 0
                end) as ChelseaTV_Sub,

              -- ##### DTV MUTV subscription #####
            max(case
                  when csh.subscription_sub_type = 'DTV MUTV' and csh.status_code in ('AC', 'PC', 'AB') then 1
                    else 0
                end) as MUTV_Sub,

              -- ##### MGM subscription #####
            max(case
                  when csh.subscription_sub_type = 'MGM' and csh.status_code in ('AC', 'PC', 'AB') then 1
                    else 0
                end) as MGM_Sub

        into VAggr_tmp_Account_Portfolio_Snapshot
        from (select account_number from ETHANRecEng_accounts_region800
              union all
              select account_number from ETHANRecEng_accounts)base
                inner join sk_prod.cust_subs_hist csh             on base.Account_Number = csh.Account_Number
                                                                 and csh.effective_from_dt <= @the_date
                                                                 and csh.effective_to_dt > @the_date
                left join sk_prod.cust_entitlement_lookup as cel  on csh.current_short_description = cel.short_description
       group by base.Account_Number
      commit

      create unique hg index idx01 on VAggr_tmp_Account_Portfolio_Snapshot(Account_Number)


--send this as a report
select *
from VAggr_tmp_Account_Portfolio_Snapshot
order by account_number






