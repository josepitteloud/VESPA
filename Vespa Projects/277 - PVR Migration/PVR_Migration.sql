--Update of PVR Migration analysis for September 2015

      -- assets
  select asset_name
        ,min(provider_brand) as provider
        ,min(effective_from_dt) as from_dt
        ,max(effective_to_dt)   as to_dt
    into #assets
    from anytime_plus_asset_detail
   where date(effective_from_dt) <= '2015-09-30'
     and date(effective_to_dt)   >= '2015-09-01'
group by asset_name
;

      -- progs
  select case when programme_name like 'New %' then substr(programme_name,5,100) else programme_name end as programme
        ,episode_number
        ,epg_group_name
        ,genre_description
        ,sub_genre_description
        ,instance_start_date_time_utc
        ,event_start_date_time_utc
        ,count(distinct dk_programme_dim || subscriber_id) as num_viewed
        ,sum(datediff(second,instance_start_date_time_utc,instance_end_date_time_utc)) as viewed_duration
    into #progs
     from vespa_dp_prog_viewed_201509 as prg
   where playback_speed = 1
     and capped_full_flag = 0
     and date(instance_start_date_time_utc) > date(broadcast_start_date_time_utc)
     and epg_group_name <> 'Sky Push VOD'
     and programme is not null
group by programme
        ,episode_number
        ,epg_group_name
        ,genre_description
        ,sub_genre_description
        ,instance_start_date_time_utc
        ,event_start_date_time_utc
;
--40mins, 109m rows

  commit;
  create hg   index hgpro on #progs(programme);
  create dttm index dmins on #progs(instance_start_date_time_utc);
  create dttm index dmeve on #progs(event_start_date_time_utc);
  create hg   index hgvie on #progs(viewed_duration);
  create hg   index hggen on #progs(genre_description);
  create hg   index hgsub on #progs(sub_genre_description);
  create hg   index hgass on #assets(asset_name);
  create dttm index dmfro on #assets(from_dt);
  create dttm index dmtod on #assets(to_dt);

      -- main sheet
  select programme
        ,episode_number
        ,epg_group_name
        ,genre_description
        ,sub_genre_description
        ,provider
        ,sum(num_viewed) as viewed
        ,sum(viewed_duration) as duration
        ,max(case when asset_name is null then 0 else 1 end) as active_asset
    from #progs as prg
         left join #assets as ass on prg.programme = ass.asset_name
                                 and prg.instance_start_date_time_utc between from_dt and to_dt
   where 1=1
     and genre_description <> 'Sports'
     and (sub_genre_description <> 'News'
          or (programme not like '%News%' and programme not like '%Weather%')
         )
     and date(event_start_date_time_utc) between '2015-09-24' and '2015-09-30'
group by programme
        ,episode_number
        ,epg_group_name
        ,genre_description
        ,sub_genre_description
        ,provider
order by duration desc
;




select top 100 * from #assets
where asset_name='The X Factor'

select top 100 *
        ,case when asset_name is null then 0 else 1 end as active_asset
 from #progs as prg
         left join #assets as ass on prg.programme = ass.asset_name
                                 and prg.instance_start_date_time_utc between from_dt and to_dt
where programme='The X Factor'
and date(event_start_date_time_utc) between '2015-09-24' and '2015-09-30'
































select top 100 broadcast_start_date_time_utc,service_key
into #vdp
from vespa_dp_prog_viewed_201512

select * from #vdp
order by broadcast_start_date_time_utc,service_key
;
select top 200 broadcast_start_datetime,service_key,* from PROGRAMME_SCHEDULE_DIM
where service_key=2105
order by broadcast_start_datetime desc

select top 100 programme_uuid,*
from PROGRAMME_SCHEDULE_DIM as psd
inner join #vdp as vdp on psd.broadcast_start_datetime = vdp.broadcast_start_date_time_utc
                                             and psd.service_key              = vdp.service_key






select max(broadcast_start_datetime) from PROGRAMME_SCHEDULE_DIM
where broadcast_start_datetime <= '2016-03-01'







----------------V2 run in Feb 2016, on Nov 2015 data

  create table #progs(
         programme       varchar(100)
        ,genre           varchar(30)
        ,broadcast_start datetime
        ,service_key     smallint
        ,uuid            varchar(100)
        ,num_viewed      smallint
        ,viewed_duration int
        )
;

      -- progs
  insert into #progs(
         programme
        ,genre
        ,broadcast_start
        ,service_key
        ,uuid
        ,num_viewed
        ,viewed_duration
        )
  select case when programme_name like 'New %' then substr(programme_name,5,100) else programme_name end as programme
        ,case when genre_description = 'Sports' then 'Sports'
              when sub_genre_description = 'News' and programme_name like '%News%'    then 'News'
              when sub_genre_description = 'News' and programme_name like '%Weather%' then 'Weather'
              else 'Other'
          end as genre
        ,broadcast_start_date_time_utc
        ,service_key
        ,null as uuid
        ,count(distinct dk_programme_dim || subscriber_id)
        ,sum(datediff(second,instance_start_date_time_utc,instance_end_date_time_utc))
    from vespa_dp_prog_viewed_201511 as prg
   where playback_speed = 1
     and capped_full_flag = 0
     and date(instance_start_date_time_utc) > date(broadcast_start_date_time_utc)
     and epg_group_name <> 'Sky Push VOD'
     and service_key <> 65535
group by programme
        ,genre
        ,broadcast_start_date_time_utc
        ,service_key
        ,uuid
;

  update #progs as bas
     set bas.uuid = psd.programme_uuid
    from programme_schedule_dim as psd
   where psd.broadcast_start_datetime = bas.broadcast_start
     and psd.service_key              = bas.service_key
     and psd.programme_uuid not in ('unknown','(unknown)')
;

/*
      -- assets (Netezza query)
  select distinct(case when programme_uuid = '(unknown)' then programme_name else programme_uuid end) as uuid
    from final_programme_catalogue
   where date_from <= '2015-11-30'
     and date_to   >= '2015-11-01'
;
*/

  -- assets
  create table #assets(uuid varchar(100));

execute('
    load table #assets(
         uuid''\n'')
   from ''/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/asset_UUIDs_201511.csv''
escapes off
 quotes off
       ')
;

  update #assets
     set uuid = left(uuid,36)
;

  update #assets
     set uuid = left(uuid, len(uuid) - 1)
   where ascii(right(uuid, 1)) = 13
;

  select service_key
        ,max(vespa_name) as channel_name
        ,max(cast(case when channel_owner = 'Sky' then 1 else 0 end as bit)) as sky_owned
    into #channel_map
    from vespa_analysts.channel_map_prod_service_key_attributes
   where activex = 'Y'
group by service_key
;

  create hg index hguui on #progs(uuid);
  create hg index hgpro on #progs(programme);
  create lf index lfser on #progs(service_key);
  create hg index hguui on #assets(uuid);
  create unique lf index lfser on #channel_map(service_key);

  select coalesce(programme, 'Unknown') as programme
        ,coalesce(map.channel_name, 'Unknown') as channel_name
        ,coalesce(sky_owned,0) as sky_owned
        ,genre
        ,uuid
        ,case when datediff(day, broadcast_start, '2015-11-01') <= 30  then 'Within 30 days'
              when datediff(day, broadcast_start, '2015-11-01') <= 60  then '31-60 days'
              when datediff(day, broadcast_start, '2015-11-01') <= 90  then '61-90 days'
              when datediff(day, broadcast_start, '2015-11-01') <= 120 then '91-120 days'
              when datediff(day, broadcast_start, '2015-11-01') <= 150 then '120-150 days'
              when datediff(day, broadcast_start, '2015-11-01') <= 180 then '151-180 days'
              else '>180 days'
         end as dt
        ,sum(num_viewed) as viewed
        ,sum(viewed_duration) as duration
        ,count() as episodes
        ,cast(0 as bit) as active_asset_uuid_match
        ,cast(0 as bit) as active_asset_non_BBC
        ,cast(0 as bit) as active_asset_uuid_or_programme_match
        ,cast(0 as bit) as active_asset_uuid_or_programme_match_non_BBC
        ,cast(0 as bit) as active_asset_uuid_match_or_unmatched_channel
        ,cast(0 as bit) as active_asset_uuid_match_or_unmatched_channel_or_more_than30days
    into #results
    from #progs as prg
         left join #channel_map as map on prg.service_key = map.service_key
group by programme
        ,map.channel_name
        ,sky_owned
        ,genre
        ,uuid
        ,dt
;

  commit;
  create hg index hguui on #results(uuid);
  create hg index hgpro on #results(programme);

  update #results as bas
     set active_asset_uuid_match = 1
        ,active_asset_uuid_or_programme_match = 1
        ,active_asset_uuid_match_or_unmatched_channel = 1
        ,active_asset_uuid_match_or_unmatched_channel_or_more_than30days = 1
    from #assets as ass
   where bas.uuid = ass.uuid
;

  update #results as bas
     set active_asset_non_BBC = 1
        ,active_asset_uuid_or_programme_match_non_BBC = 1
    from #assets as ass
   where bas.uuid = ass.uuid
     and channel_name not like '%BBC%'
;

  update #results as bas
     set active_asset_uuid_or_programme_match = 1
        ,active_asset_uuid_match_or_unmatched_channel = 1
        ,active_asset_uuid_match_or_unmatched_channel_or_more_than30days = 1
    from #assets as ass
   where bas.programme = ass.uuid
;

  update #results as bas
     set active_asset_uuid_or_programme_match_non_BBC = 1
    from #assets as ass
   where bas.programme = ass.uuid
     and channel_name not like '%BBC%'
;

  update #results as bas
     set active_asset_uuid_match_or_unmatched_channel_or_more_than30days = 1
   where dt <> 'Within 30 days'
;

  select channel_name
    into #unmatched_channels
    from (
         select channel_name
           ,sum(active_asset_uuid_match) as cow
           from #results
       group by channel_name
         having cow = 0
         ) as sub
;

  update #results as bas
     set active_asset_uuid_match_or_unmatched_channel = 1
        ,active_asset_uuid_match_or_unmatched_channel_or_more_than30days = 1
    from #unmatched_channels as unm
   where bas.channel_name = unm.channel_name
;

  create table #results2(
         id int identity primary key
        ,programme varchar(100)
        ,channel_name varchar(50)
        ,sky_owned bit default 0
        ,genre varchar(30)
        ,dt varchar(20)
        ,viewed int
        ,duration int
        ,episodes int
        ,active_asset_uuid_match bit default 0
        ,active_asset_non_BBC bit default 0
        ,active_asset_uuid_or_programme_match bit default 0
        ,active_asset_uuid_or_programme_match_non_BBC bit default 0
        ,unmatched_channel bit default 0
        ,active_asset_uuid_match_or_unmatched_channel bit default 0
        ,active_asset_uuid_match_or_unmatched_channel_or_more_than30days bit default 0
        )
;

  insert into #results2(
         programme
        ,channel_name
        ,sky_owned
        ,genre
        ,dt
        ,viewed
        ,duration
        ,episodes
        ,active_asset_uuid_match
        ,active_asset_non_BBC
        ,active_asset_uuid_or_programme_match
        ,active_asset_uuid_or_programme_match_non_BBC
        ,unmatched_channel
        ,active_asset_uuid_match_or_unmatched_channel
        ,active_asset_uuid_match_or_unmatched_channel_or_more_than30days
        )
  select programme
        ,bas.channel_name
        ,sky_owned
        ,genre
        ,dt
        ,sum(viewed) as viewed
        ,sum(duration) as duration
        ,sum(episodes) as episodes
        ,max(active_asset_uuid_match)                                         as active_asset_uuid_match
        ,max(active_asset_non_BBC)                                            as active_asset_non_BBC
        ,max(active_asset_uuid_or_programme_match)                            as active_asset_uuid_or_programme_match
        ,max(active_asset_uuid_or_programme_match_non_BBC)                    as active_asset_uuid_or_programme_match_non_BBC
        ,max(case when unm.channel_name is null then 0 else 1 end)            as unmatched_channel
        ,max(active_asset_uuid_match_or_unmatched_channel)                    as active_asset_uuid_match_or_unmatched_channel
        ,max(active_asset_uuid_match_or_unmatched_channel_or_more_than30days) as active_asset_uuid_match_or_unmatched_channel_or_more_than30days
    from #results as bas
         left join #unmatched_channels as unm on bas.channel_name = unm.channel_name
group by programme
        ,bas.channel_name
        ,sky_owned
        ,genre
        ,dt
order by duration desc
;

  create table #results3(
         id int
        ,programme varchar(100)
        ,channel_name varchar(50)
        ,sky_owned bit default 0
        ,genre varchar(30)
        ,dt varchar(20)
        ,viewed int
        ,duration int
        ,episodes int
        ,active_asset_uuid_match bit default 0
        ,active_asset_non_BBC bit default 0
        ,active_asset_uuid_or_programme_match bit default 0
        ,active_asset_uuid_or_programme_match_non_BBC bit default 0
        ,unmatched_channel bit default 0
        ,active_asset_uuid_match_or_unmatched_channel bit default 0
        ,active_asset_uuid_match_or_unmatched_channel_or_more_than30days bit default 0
        ,cumulative_uuid_matches float
        ,cumulative_non_BBC_matches float
        ,cumulative_uuid_or_programme_matches float
        ,cumulative_uuid_or_programme_non_BBC_matches float
        ,cumulative_viewed_time float
        ,cumulative_uuid_match_or_unmatched_channel float
        ,cumulative_uuid_match_or_unmatched_channel_or_more_than30days float
        )
;

  insert into #results3(
         id
        ,programme
        ,channel_name
        ,sky_owned
        ,genre
        ,dt
        ,viewed
        ,duration
        ,episodes
        ,active_asset_uuid_match
        ,active_asset_non_BBC
        ,active_asset_uuid_or_programme_match
        ,active_asset_uuid_or_programme_match_non_BBC
        ,unmatched_channel
        ,active_asset_uuid_match_or_unmatched_channel
        ,active_asset_uuid_match_or_unmatched_channel_or_more_than30days
        ,cumulative_uuid_matches
        ,cumulative_non_BBC_matches
        ,cumulative_uuid_or_programme_matches
        ,cumulative_uuid_or_programme_non_BBC_matches
        ,cumulative_viewed_time
        ,cumulative_uuid_match_or_unmatched_channel
        ,cumulative_uuid_match_or_unmatched_channel_or_more_than30days
        )
  select id
        ,programme
        ,bas.channel_name
        ,sky_owned
        ,genre
        ,dt
        ,viewed
        ,duration
        ,episodes
        ,active_asset_uuid_match
        ,active_asset_non_BBC
        ,active_asset_uuid_or_programme_match
        ,active_asset_uuid_or_programme_match_non_BBC
        ,unmatched_channel
        ,active_asset_uuid_match_or_unmatched_channel
        ,active_asset_uuid_match_or_unmatched_channel_or_more_than30days
        ,sum(active_asset_uuid_match)                                         over (order by id rows between unbounded preceding and current row) as cumulative_uuid_matches
        ,sum(active_asset_non_BBC)                                            over (order by id rows between unbounded preceding and current row) as cumulative_non_BBC_matches
        ,sum(active_asset_uuid_or_programme_match)                            over (order by id rows between unbounded preceding and current row) as cumulative_uuid_or_programme_matches
        ,sum(active_asset_uuid_or_programme_match_non_BBC)                    over (order by id rows between unbounded preceding and current row) as cumulative_uuid_or_programme_non_BBC_matches
        ,sum(viewed)                                                          over (order by id rows between unbounded preceding and current row) as cumulative_viewed_time
        ,sum(active_asset_uuid_match_or_unmatched_channel)                    over (order by id rows between unbounded preceding and current row) as cumulative_uuid_match_or_unmatched_channel
        ,sum(active_asset_uuid_match_or_unmatched_channel_or_more_than30days) over (order by id rows between unbounded preceding and current row) as cumulative_uuid_match_or_unmatched_channel_or_more_than30days
    from #results2 as bas
group by id
        ,programme
        ,channel_name
        ,sky_owned
        ,genre
        ,dt
        ,viewed
        ,duration
        ,episodes
        ,active_asset_uuid_match
        ,active_asset_non_BBC
        ,active_asset_uuid_or_programme_match
        ,active_asset_uuid_or_programme_match_non_BBC
        ,unmatched_channel
        ,active_asset_uuid_match_or_unmatched_channel
        ,active_asset_uuid_match_or_unmatched_channel_or_more_than30days
;

  create variable @sum_viewed int;
  select @sum_viewed = sum(viewed)
    from #results2
;

  update #results3
     set cumulative_uuid_matches                                       = 1.0 * cumulative_uuid_matches / id
        ,cumulative_non_BBC_matches                                    = 1.0 * cumulative_non_BBC_matches / id
        ,cumulative_uuid_or_programme_matches                          = 1.0 * cumulative_uuid_or_programme_matches / id
        ,cumulative_uuid_or_programme_non_BBC_matches                  = 1.0 * cumulative_uuid_or_programme_non_BBC_matches / id
        ,cumulative_viewed_time                                        = 1.0 * cumulative_viewed_time / @sum_viewed
        ,cumulative_uuid_match_or_unmatched_channel                    = 1.0 * cumulative_uuid_match_or_unmatched_channel / id
        ,cumulative_uuid_match_or_unmatched_channel_or_more_than30days = 1.0 * cumulative_uuid_match_or_unmatched_channel_or_more_than30days / id
;

      -- all results
  select * from #results3;

  create table #results3_without_news(
         id int
        ,programme varchar(100)
        ,channel_name varchar(50)
        ,sky_owned bit default 0
        ,genre varchar(30)
        ,dt varchar(20)
        ,viewed int
        ,duration int
        ,episodes int
        ,active_asset_uuid_match bit default 0
        ,active_asset_non_BBC bit default 0
        ,active_asset_uuid_or_programme_match bit default 0
        ,active_asset_uuid_or_programme_match_non_BBC bit default 0
        ,active_asset_uuid_match_or_unmatched_channel_or_more_than30days bit default 0
        ,unmatched_channel bit default 0
        ,active_asset_uuid_match_or_unmatched_channel bit default 0
        ,cumulative_uuid_matches float
        ,cumulative_non_BBC_matches float
        ,cumulative_uuid_or_programme_matches float
        ,cumulative_uuid_or_programme_non_BBC_matches float
        ,cumulative_viewed_time float
        ,cumulative_uuid_match_or_unmatched_channel float
        ,cumulative_uuid_match_or_unmatched_channel_or_more_than30days float
        )
;

  insert into #results3_without_news(
         id
        ,programme
        ,channel_name
        ,sky_owned
        ,genre
        ,dt
        ,viewed
        ,duration
        ,episodes
        ,active_asset_uuid_match
        ,active_asset_non_BBC
        ,active_asset_uuid_or_programme_match
        ,active_asset_uuid_or_programme_match_non_BBC
        ,active_asset_uuid_match_or_unmatched_channel_or_more_than30days
        ,unmatched_channel
        ,active_asset_uuid_match_or_unmatched_channel
        ,cumulative_uuid_matches
        ,cumulative_non_BBC_matches
        ,cumulative_uuid_or_programme_matches
        ,cumulative_uuid_or_programme_non_BBC_matches
        ,cumulative_viewed_time
        ,cumulative_uuid_match_or_unmatched_channel
        ,cumulative_uuid_match_or_unmatched_channel_or_more_than30days
        )
  select id
        ,programme
        ,bas.channel_name
        ,sky_owned
        ,genre
        ,dt
        ,viewed
        ,duration
        ,episodes
        ,active_asset_uuid_match
        ,active_asset_non_BBC
        ,active_asset_uuid_or_programme_match
        ,active_asset_uuid_or_programme_match_non_BBC
        ,unmatched_channel
        ,active_asset_uuid_match_or_unmatched_channel
        ,active_asset_uuid_match_or_unmatched_channel_or_more_than30days
        ,sum(active_asset_uuid_match)                                         over (order by id rows between unbounded preceding and current row) as cumulative_uuid_matches
        ,sum(active_asset_non_BBC)                                            over (order by id rows between unbounded preceding and current row) as cumulative_non_BBC_matches
        ,sum(active_asset_uuid_or_programme_match)                            over (order by id rows between unbounded preceding and current row) as cumulative_uuid_or_programme_matches
        ,sum(active_asset_uuid_or_programme_match_non_BBC)                    over (order by id rows between unbounded preceding and current row) as cumulative_uuid_or_programme_non_BBC_matches
        ,sum(viewed)                                                          over (order by id rows between unbounded preceding and current row) as cumulative_viewed_time
        ,sum(active_asset_uuid_match_or_unmatched_channel)                    over (order by id rows between unbounded preceding and current row) as cumulative_uuid_match_or_unmatched_channel
        ,sum(active_asset_uuid_match_or_unmatched_channel_or_more_than30days) over (order by id rows between unbounded preceding and current row) as cumulative_uuid_match_or_unmatched_channel_or_more_than30days
    from #results2 as bas
   where genre = 'Other'
group by id
        ,programme
        ,channel_name
        ,sky_owned
        ,genre
        ,dt
        ,viewed
        ,duration
        ,episodes
        ,active_asset_uuid_match
        ,active_asset_non_BBC
        ,active_asset_uuid_or_programme_match
        ,active_asset_uuid_or_programme_match_non_BBC
        ,unmatched_channel
        ,active_asset_uuid_match_or_unmatched_channel
        ,active_asset_uuid_match_or_unmatched_channel_or_more_than30days
;

  select @sum_viewed = sum(viewed)
    from #results2
   where genre = 'Other'
;

  update #results3_without_news
     set cumulative_uuid_matches                                         = 1.0 * cumulative_uuid_matches / id
        ,cumulative_non_BBC_matches                                      = 1.0 * cumulative_non_BBC_matches / id
        ,cumulative_uuid_or_programme_matches                            = 1.0 * cumulative_uuid_or_programme_matches / id
        ,cumulative_uuid_or_programme_non_BBC_matches                    = 1.0 * cumulative_uuid_or_programme_non_BBC_matches / id
        ,cumulative_viewed_time                                          = 1.0 * cumulative_viewed_time / @sum_viewed
        ,cumulative_uuid_match_or_unmatched_channel                      = 1.0 * cumulative_uuid_match_or_unmatched_channel / id
        ,cumulative_uuid_match_or_unmatched_channel_or_more_than30days   = 1.0 * cumulative_uuid_match_or_unmatched_channel_or_more_than30days / id
;

      -- results without news,sport and weather
  select * from #results3_without_news;




---
select channel_name
        ,sum(datediff(second,instance_start_date_time_utc,instance_end_date_time_utc))
    from vespa_dp_prog_viewed_201511 as prg
   where playback_speed = 1
     and capped_full_flag = 0
     and date(instance_start_date_time_utc) > date(broadcast_start_date_time_utc)
     and epg_group_name <> 'Sky Push VOD'
     and service_key <> 65535
group by channel_name



select programme_name,count()     from vespa_dp_prog_viewed_201511 as prg
where playback_speed = 1
and channel_name = 'Sky Sports 5'
     and capped_full_flag = 0
     and date(instance_start_date_time_utc) > date(broadcast_start_date_time_utc)
group by programme_name

select count() from #results2
select channel_name,sum(duration) from #results3
where unmatched_channel=0
and active_asset_uuid_or_programme_match = 0
and channel_name not like '%BBC%'
and channel_name <> 'CBeebies'
group by channel_name


select sum(duration) from #results3
select min(duration) from #results




