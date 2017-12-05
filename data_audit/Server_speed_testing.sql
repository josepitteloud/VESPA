/********** VESPA EVENTS VIEW SPEED test_code **********/

-- Unfortunately due to the way Sybase handles it's scope, there's no
-- way to put all the queries inside a table, run them dynamically and
-- still catch the results in a variable for the report later. So we
-- just build out everything in great detail, no loops, oh well.

-- Okay, so slightly better automation, but we'd better also push the
-- historical values into the same table if it's going to sita round
-- like this. Should we also be testing say extraction and capping on
-- last three days of data? because that happens often.

-- Yeah, should deffinately add a distinct log extraction thing from
-- a daily table or something...

--if object_id('vespa_speed_testing') is not null
--   drop table vespa_speed_testing;
create table vespa_speed_testing (
    ID                      bigint not null identity primary key
    ,date_run               date
    ,test_num               varchar(10) not null
    ,test_code              varchar(1000) not null  -- because the tests we run might change slightly over time
    ,time_taken             int                     -- in minutes, rounded down
    ,test_result            bigint
);
create unique index consistency_index on vespa_speed_testing (date_run, test_num);

commit;

-- Might also have to migrate in the historical results? No, probably not.

/**** Harness setup stuff: ****/

create variable @timecatching datetime;
create variable @resultcatching bigint;
create variable @IDcatching bigint;
create variable @date_run date;

set @date_run = today();
commit;

/**** For comparison: hitting cust_subs_hist: ****/

-- Maybe we'd poke this programatically elsewhere? just because EXEC doesn't work out.
insert into vespa_speed_testing (date_run, test_num, test_code) values (@date_run, 'CSH1', 'select @resultcatching = count(1) from sk_prod.cust_subs_hist');
set @IDcatching = @@identity;
commit;
set @resultcatching = -1;
set @timecatching = now();
commit;
select @resultcatching = count(1) from sk_prod.cust_subs_hist;
commit;
update vespa_speed_testing
set time_taken = datediff(minute, @timecatching, now())
    ,test_result = @resultcatching
where ID = @IDcatching;
commit;

insert into vespa_speed_testing (date_run, test_num, test_code) values (@date_run, 'CSH2', 'select @resultcatching = count(distinct account_number) from sk_prod.cust_subs_hist');
set @IDcatching = @@identity;
commit;
set @resultcatching = -1;
set @timecatching = now();
commit;
select @resultcatching = count(distinct account_number) from sk_prod.cust_subs_hist;
commit;
update vespa_speed_testing
set time_taken = datediff(minute, @timecatching, now())
    ,test_result = @resultcatching
where ID = @IDcatching;
commit;

/******* Now going after the Vespa data *******/

insert into vespa_speed_testing (date_run, test_num, test_code) values (@date_run, 'VEV1', 'Select @resultcatching = count(1) from sk_prod.VESPA_EVENTS_VIEW');
set @IDcatching = @@identity;
commit;
set @resultcatching = -1;
set @timecatching = now();
commit;
Select @resultcatching = count(1) from sk_prod.VESPA_EVENTS_VIEW;
commit;
update vespa_speed_testing
set time_taken = datediff(minute, @timecatching, now())
    ,test_result = @resultcatching
where ID = @IDcatching;
commit;

insert into vespa_speed_testing (date_run, test_num, test_code) values (@date_run, 'VEV2', 'select @resultcatching = count(distinct account_number) from sk_prod.VESPA_EVENTS_VIEW');
set @IDcatching = @@identity;
commit;
set @resultcatching = -1;
set @timecatching = now();
commit;
select @resultcatching = count(distinct account_number) from sk_prod.VESPA_EVENTS_VIEW;
commit;
update vespa_speed_testing
set time_taken = datediff(minute, @timecatching, now())
    ,test_result = @resultcatching
where ID = @IDcatching;
commit;

insert into vespa_speed_testing (date_run, test_num, test_code) values (@date_run, 'VEV3', 'Select @resultcatching = count(distinct cb_key_household) from sk_prod.VESPA_EVENTS_VIEW');
set @IDcatching = @@identity;
commit;
set @resultcatching = -1;
set @timecatching = now();
commit;
Select @resultcatching = count(distinct cb_key_household) from sk_prod.VESPA_EVENTS_VIEW;
commit;
update vespa_speed_testing
set time_taken = datediff(minute, @timecatching, now())
    ,test_result = @resultcatching
where ID = @IDcatching;
commit;

insert into vespa_speed_testing (date_run, test_num, test_code) values (@date_run, 'VEV4', 'Select @resultcatching = count(distinct EPG_TITLE) from sk_prod.VESPA_EVENTS_VIEW');
set @IDcatching = @@identity;
commit;
set @resultcatching = -1;
set @timecatching = now();
commit;
Select @resultcatching = count(distinct EPG_TITLE) from sk_prod.VESPA_EVENTS_VIEW; -- This field is not indexed
commit;
update vespa_speed_testing
set time_taken = datediff(minute, @timecatching, now())
    ,test_result = @resultcatching
where ID = @IDcatching;
commit;

/******* Apparently DISTINCT is not efficient? *******/

insert into vespa_speed_testing (date_run, test_num, test_code) values (@date_run, 'VND1', 'select @resultcatching = count(1) from (select account_number from sk_prod.VESPA_EVENTS_VIEW group by account_number) as t');
set @IDcatching = @@identity;
commit;
set @resultcatching = -1;
set @timecatching = now();
commit;
select @resultcatching = count(1) from
(
        select account_number
        from sk_prod.VESPA_EVENTS_VIEW
        group by account_number
) as t;
commit;
update vespa_speed_testing
set time_taken = datediff(minute, @timecatching, now())
    ,test_result = @resultcatching
where ID = @IDcatching;
commit;

insert into vespa_speed_testing (date_run, test_num, test_code) values (@date_run, 'VND2', 'select @resultcatching = count(1) from (select epg_title from sk_prod.VESPA_EVENTS_VIEW group by epg_title) as t');
set @IDcatching = @@identity;
commit;
set @resultcatching = -1;
set @timecatching = now();
commit;
select @resultcatching = count(1) from
(
        select epg_title
        from sk_prod.VESPA_EVENTS_VIEW
        group by epg_title
) as t;
commit;
update vespa_speed_testing
set time_taken = datediff(minute, @timecatching, now())
    ,test_result = @resultcatching
where ID = @IDcatching;
commit;

/******* Now with some filtering and TOP'ing to see if it makes a difference *******/

-- Not yet semi-automated as we don't know how we're going to track the results...
-- nor how we're going to adjust it for something current and recent.

select top 200 *
from sk_prod.VESPA_EVENTS_VIEW
-- Also using our filter for Vespa viewing events only
where (play_back_speed is null or play_back_speed = 2) -- NULL means live, 2 is timeshifted
and x_programme_viewed_duration > 0
and Panel_id in (4, 5)
and x_type_of_viewing_event <> 'Non viewing event'
-- 31 October: Prod4 - takes 12 minutes, then rejected because it exceeds temp space limits.
-- 31 October: Prod10 - takes 12 minutes, then rejected because it exceeds temp space limits.

-- And with some programme filtering:
select top 200 *
from sk_prod.VESPA_EVENTS_VIEW
where programme_trans_sk = 201107020000002514 -- Some Wimbledon from early July
and (play_back_speed is null or play_back_speed = 2) -- NULL means live, 2 is timeshifted
and x_programme_viewed_duration > 0
and Panel_id in (4, 5)
and x_type_of_viewing_event <> 'Non viewing event'
-- 31 October: Prod4 - 15 minutes
-- 31 October: Prod10 - 7 minutes

/******* Archived data (now also held in the DB!): *******/

select
        date_run
        ,test_num
        ,time_taken
        ,test_result
from vespa_speed_testing order by left(test_num,4), date_run;
-- Guess you could also put a date filter on here to see how the most
-- recent batch handled it.
/*
2011-06-22	CSH1	0	423009925
2011-07-20	CSH1	0	431276067
2011-07-26	CSH1	0	433738503
2011-10-31	CSH1	0	464842189
2011-11-29	CSH1	0	482402783
2011-06-22	CSH2	0	22348326
2011-07-13	CSH2	0	22449943
2011-07-20	CSH2	0	22475393
2011-07-26	CSH2	0	22503248
2011-10-31	CSH2	0	22897520
2011-11-29	CSH2	0	23043641
2011-06-22	VEV1	2	276728644
2011-07-13	VEV1	7	655309470
2011-07-20	VEV1	4	768518552
2011-07-26	VEV1	5	863127100
2011-10-31	VEV1P10	5	1865564843
2011-11-01	VEV1P4	8	1868159948
2011-11-29	VEV1	2	2019734458
2011-06-22	VEV2	20	130185
2011-07-13	VEV2	42	203916
2011-07-20	VEV2	35	238026
2011-07-26	VEV2	30	245504
2011-06-22	VEV3	17	130154
2011-07-20	VEV3	22	238030
2011-07-26	VEV3	40	245685
2011-06-23	VEV4	8	18600
2011-07-20	VEV4	19	41653
2011-10-31	VEV4P10	35	52419
2011-10-31	VEV4P4	60	52419
2011-07-13	VND1	40	203916
2011-07-30	VND1	55	238026
2011-11-01	VND1P10	60	370402
2011-11-02	VND1P4	20	370403
2011-07-13	VND2	15	39874
2011-07-20	VND2	20	41654
2011-11-01	VND2P4	195	52573
2011-11-01	VND2P10	90	52573
*/

