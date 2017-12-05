-- OK, so we want to speed test MD5 vs SHA1 hashes to figure out if MD5 is much faster
-- (because SHA1 is slow!) Update: looks like SHA1 beats MD5 for speed? MD5 is acutally
-- a little bit slower, didn't see that coming. Oh well.

-- Setup and variables:

drop table #myhashspeedtesting
select
    top 10000 account_number
--    top 30000 account_number    
into #myhashspeedtesting
from Vespa_PanMan_all_households;

create table speed_test_sha1 (
        account_number          varchar(20) primary key
,        thehasg                 varchar(40)
);
create unique index forsiho on speed_test_sha1 (thehasg);
-- Keeping the double unique thing because that'll be the other big time component in the processing

create table speed_test_md5 (
        account_number          varchar(20) primary key
,        thehasg                 varchar(40)
);
create unique index forsiho on speed_test_md5 (thehasg);

create variable thetime datetime;


-- MD5 speed test: 99s for 10k items, 775 for 30k...
delete from speed_test_md5;
set thetime = now();
commit;

insert into speed_test_md5
select
    account_number
    ,hash(account_number, 'MD5')  -- It took an hour and a half to build 400k SHA1 hashes. Do we instead want to think about using MD5? Would that be faster? It's not going to hapily scale to 5m.... though another affect could also be the unique key on that column... Maybe we could grab the household key from SAV or something? But then we don't know that it's unique across account numbers.... should get much better overnight though, the whole table is only 50MB, it's all processing time to get the hashes and check the uniqueness.
from #myhashspeedtesting;

commit;
select datediff(s, thetime, now());
commit;


-- SHA1 speed test: 85 sec for 10k items, 739 sec for 30k...

delete from speed_test_SHA1;
set thetime = now();
commit;

insert into speed_test_sha1
select
    account_number
    ,hash(account_number, 'SHA1')  -- It took an hour and a half to build 400k SHA1 hashes. Do we instead want to think about using MD5? Would that be faster? It's not going to hapily scale to 5m.... though another affect could also be the unique key on that column... Maybe we could grab the household key from SAV or something? But then we don't know that it's unique across account numbers.... should get much better overnight though, the whole table is only 50MB, it's all processing time to get the hashes and check the uniqueness.
from #myhashspeedtesting;

commit;
select datediff(s, thetime, now());
commit;