-- Okay, the next request is an MBM graph. We can do that. There are a lot of
-- different bits and pieces of channels etc, but we can pull them out easy enough:

-- First list all the programmes:
drop table V33_Futurama_programme_key_dump;

select programme_trans_sk
    ,case when channel_name like '%HD%' then 1 else 0 end as HD
    ,tx_start_datetime_utc
    ,episode_number
into V33_Futurama_programme_key_dump
from sk_prod.vespa_epg_dim
where epg_title = 'Futurama'
and series_id = '249892'
and tx_date in ('20110722', '20110723')
and tx_time in ('200000', '203000');

-- a key for looping:
alter table V33_Futurama_programme_key_dump add ID tinyint identity primark key;

commit;

-- Now assemble a whole bunch of MBM stuff...

-- Yeah, okey, so the MBM stuff currently only does live and we kind of do need
-- timeshifted for this. Gonna have to update that a bit, including looping over
-- subsequent daily tables...





-- But after that's done, stitck the profiling details back on:
alter table V33_Futurama_MBM
add HD bit default 0
tx_start_datetime_utc datetime
episode_number tinyint;

update V33_Futurama_MBM
set HD = pkd.HD
    ,tx_start_datetime_utc = pkd.tx_start_datetime_utc
    ,episode_number = pkd.episode_number
from V33_Futurama_MBM as fmbm
inner join V33_Futurama_programme_key_dump as pkd
on fmbm.programme_trans_sk = pkd.programme_trans_sk;
commit;

-- Okay, now we've got the procs in, we can use them to get the timeshifted MBM too..
-- in fact, one proc gets all of the timeshifted stuff: VODSAL, 7 days, 28 weeks!

select * from V33_Futurama_programme_key_dump
order by tx_start_datetime_utc;
-- Ha, no, we want all 9 on these, because some are HD and some are SD and some are
-- episode 1 and others are episode 2...

vespa_analysts.make_Uncapped_MBM_prog_graph 201107230000014187; -- defaults to 1 minute slices
-- Yeah, that seems to work? But.. it takes about 20 minutes per programme key. Urgh.
-- Well, 20 minutes for one of the high demand ones.

alter table V33_Futurama_programme_key_dump add id tinyint not null identity primary key;

create variable @looper tinyint;
create variable @this_trans_key bigint;

create table V33_MBM_results (
    programme_trans_sk      bigint
    ,slice_start            datetime
    ,viewed_live            int
    ,viewed_VODSAL          int
    ,viewed_1_to_7_days     int
    ,viewed_8_to_28_days    int
);

set @looper = 1;

while @looper <= 16
begin

    select @this_trans_key = programme_trans_sk
    from V33_Futurama_programme_key_dump
    where id = @looper

    select *
    into #MBM_dump
    from vespa_analysts.make_Uncapped_MBM_prog_graph(@this_trans_key)

    commit
    
    insert into V33_MBM_results
    select @this_trans_key, *
    from #MBM_dump
    
    commit
    
    drop table #MBM_dump
    
    commit
    set @looper = @looper + 1

end;
    
-- And from there, we only need to summarise by episode and channel and the things
-- that are on that other lookup table.

select programme_trans_sk from V33_MBM_results
group by programme_trans_sk
having max(Viewed_Live + Viewed_VODSAL + Viewed_1_to_7_days + Viewed_8_to_28_days) = 0
/* these guys get killed:
201107230000000950
201107240000000602
201107230000000714
201107240000000936
201107230000000936
201107240000000616
201107230000000700
201107240000000950
*/

delete from V33_MBM_results
where programme_trans_sk in (
201107230000000950,
201107240000000602,
201107230000000714,
201107240000000936,
201107230000000936,
201107240000000616,
201107230000000700,
201107240000000950
);
-- Cool, now we just join that to the other programme info and we're done.

select r.*
    ,epg.channel_name
    ,case when epg.channel_name like '%HD%' then 1 else 0 end as HD
    ,epg.tx_start_datetime_utc
    ,epg.tx_end_datetime_utc
    ,epg.episode_number
from V33_MBM_results as r
inner join sk_prod.vespa_epg_dim as epg
on r.programme_trans_sk = epg.programme_trans_sk
