-- Cleansing data (currently on QA server) for purposes of helping Sybase isolate why
-- this query is failing badly. We need to ship the data over to Sybase, but we can't
-- include any personally identifying data, so we need to cleanse the stuff. We've got
-- sample table structure in the other file, but here we need to cleanse the values
-- from the "real" tables into the publishable tables.

/* Looking generally at stuff:

select top 10 '''' + convert(varchar(20), ilu_cb_row_id) from scaling_weekly_sample;

select top 10 * from scaling_weekly_sample;

select count(1) from scaling_weekly_sample;
-- 9440868


select top 10 * from Scaling_box_level_viewing;

select count(1) from  Scaling_box_level_viewing;

*/

-- Make distinct lists of things:

select account_number
into #account_listing
from scaling_weekly_sample
union distinct select account_number from Scaling_box_level_viewing
;
-- 9440868 row(s) affected - that's our account number lookup, or at least the listing for it

select distinct service_instance_id
into #service_instance_listing
from Scaling_box_level_viewing
;
-- 11994721 row(s) affected

commit;

select min(len(service_instance_id)), max(len(service_instance_id))
from Scaling_box_level_viewing;
-- yeah, there's letters in here, we want to somehow replace these things with random hex strings...
-- The lengths vary between 7 and 23 characters, so we'll just bundle everything into 1 10 digit hex
-- thing for subscriber IDs...

select min(len(account_number)), max(len(account_number))
from  #account_listing;
-- this one is always 12 digits long.

select top 40 * from Scaling_box_level_viewing
where len(service_instance_id) = 23
-- okay there are some huge things in there...

-- Generate randomised structures with replacement items

drop table #account_replacements;
drop table #service_instance_replacements;
commit;

select account_number, right(biginttohex(convert(bigint,rand(number()) * power(16,12))),12) as account_replacement
into #account_replacements
from #account_listing;
-- OK so we've got our one-time swap list for account numbers...

select service_instance_id, right(biginttohex(convert(bigint,rand(number()) * power(16,14))),14) as service_instance_replacement
into #service_instance_replacements
from #service_instance_listing;
-- And also a one-time-swap for service instance IDs...

select top 10 * from #account_replacements;

select top 10 * from #service_instance_replacements;

select count(1) from scaling_weekly_sample
where scaling_segment_id is not null or mr_boxes is not null;
-- 

commit;
create unique index fake_pk on #service_instance_replacements (service_instance_id);
create unique index fake_pk on #account_replacements (account_number);
commit;

-- Generate replacement structures.
 
-- Wait, we need to do this with the same table creation script stuff that we built these things
-- for originally, so that we've got some consistency and know we're close to the thing that failed.

-- OK, now let's stitch this stuff together to make our updated list; later we'll
-- swap out the column values on the flags too.
insert into table1
select 
    ar.account_replacement
    ,sws.ilu_cb_row_id     
    ,sws.universe          
    ,sws.isba_tv_region    
    ,sws.ilu_hhcomposition 
    ,sws.hhcomposition     
    ,sws.tenure            
    ,sws.num_mix           
    ,sws.mix_pack          
    ,sws.package           
    ,sws.boxtype           
    ,sws.scaling_segment_id
    ,sws.mr_boxes          
    ,sws.complete_viewing  
from scaling_weekly_sample as sws
inner join #account_replacements as ar
on sws.account_number = ar.account_number;
-- 9440868 rows created
commit;

-- OK, and now for table 2:
insert into table2
select 
    sir.service_instance_replacement
    ,ar.account_replacement
    ,universe
    ,viewing_flag
    ,MR
    ,SP
    ,HD
    ,HDstb
    ,HD1TBstb
from Scaling_box_level_viewing as blv
inner join #account_replacements as ar
on blv.account_number = ar.account_number
inner join #service_instance_replacements as sir
on blv.service_instance_id = sir.service_instance_id;
-- 11994721 rows added
commit;

-- Nice, so those match the populations we expected before.

-- OK, now we have to cleanse the flag values that have string content... that's
-- only table1, there's nothing of value in table2.

-- First replacing the Consumerview link with a random bigint:
update table1
set flag01 = 1200000000000000000 + rand(number()) * 200000000000000000
-- with a bit of padding to rougly resemble the numbers actually in play

-- And everything else is a collection of categories which we can be fairly bland about:
update table1
set flag02 = 'A) flag02 value A'                                        -- there was only one flag value for this guy!
    ,flag03 = case flag03
        when 'Central Scotland'             then 'A) flag03 value A'
        when 'East Of England'              then 'K) flag03 value K'
        when 'Midlands'                     then 'C) flag03 value C'
        when 'Not Defined'                  then 'J) flag03 value J'
        when 'HTV Wales'                    then 'B) flag03 value B'
        when 'Meridian (exc. Chann'         then 'I) flag03 value I'
        when 'South West'                   then 'F) flag03 value F'
        when 'North East'                   then 'L) flag03 value L'
        when 'HTV West'                     then 'O) flag03 value O'
        when 'North Scotland'               then 'E) flag03 value E'
        when 'London'                       then 'D) flag03 value D'
        when 'Ulster'                       then 'M) flag03 value M'
        when 'Yorkshire'                    then 'G) flag03 value G'
        when 'North West'                   then 'N) flag03 value N'
        when 'Border'                       then 'H) flag03 value H' end
--    ,flag04                                                           -- flag04 is all NULLs
--    ,flag05                                                           -- flag05 is all just the default value 'L) Unknown'
--    ,flag06                                                           -- flag06 is all just the default value 'E) Unknown'
--    ,flag07                                                           -- flag07 is just some integers, nothing identifying there
    ,flag08 = case flag08
        when 'Entertainment Pack'           then 'A) flag08 value A'
        when 'Entertainment Extra'          then 'B) flag08 value B' end
    ,flag09 = case flag09
        when 'Dual Sports'                  then 'A) flag09 value A'
        when 'Basic - Ent Extra'            then 'H) flag09 value H'
        when 'Dual Movies'                  then 'D) flag09 value D'
        when 'Single Sports'                then 'F) flag09 value F'
        when 'Single Movies'                then 'G) flag09 value G'
        when 'Top Tier'                     then 'C) flag09 value C'
        when 'Basic - Ent'                  then 'B) flag09 value B'
        when 'Other Premiums'               then 'E) flag09 value E' end
    ,flag10 = case flag10
        when 'M) FDB & FDB'                 then 'L) flag10 value L'
        when 'F) HD & Skyplus'              then 'I) flag10 value I'
        when 'B) HD & No_secondary_box'     then 'J) flag10 value J'
        when 'H) HDx & HDx'                 then 'A) flag10 value A'
        when 'K) Skyplus & Skyplus'         then 'K) flag10 value K'
        when 'A) HDx & No_secondary_box'    then 'E) flag10 value E'
        when 'E) HD & HD'                   then 'M) flag10 value M'
        when 'C) Skyplus & No_secondary_box' then 'B) flag10 value B'
        when 'G) HD & FDB'                  then 'D) flag10 value D'
        when 'D) FDB & No_secondary_box'    then 'F) flag10 value F'
        when 'L) Skyplus & FDB'             then 'C) flag10 value C'
        when 'J) HDx & FDB'                 then 'H) flag10 value H'
        when 'I) HDx & Skyplus'             then 'G) flag10 value G' end
--    ,flag11                                                           -- it's always null
--    ,flag12                                                           -- it's always null
--    ,flag13                                                           -- it's always zero
;

commit;

-- cleansing is done! Now we need to reform those dangerous queries to see if they
-- still break the stuff...