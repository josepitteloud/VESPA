--Update of PVR Migration analysis for September 2015

      -- assets
  select distinct (asset_name)
    into #assets
    from anytime_plus_asset_detail
   where date(effective_from_dt) between '2015-09-01' and '2015-09-30'
;

      -- progs
  select programme_name
        ,episode_number
        ,epg_group_name
        ,genre_description
        ,sub_genre_description
        ,count(distinct dk_programme_dim || subscriber_id) as num_viewed
        ,sum(datediff(second,instance_start_date_time_utc,instance_end_date_time_utc)) as viewed_duration
    into #progs
     from vespa_dp_prog_viewed_201509 as prg
   where playback_speed = 1
     and capped_full_flag = 0
     and date(instance_start_date_time_utc) > date(broadcast_start_date_time_utc)
group by programme_name
        ,episode_number
        ,epg_group_name
        ,genre_description
        ,sub_genre_description
;

      -- assets last week of sep
  select distinct (asset_name)
    into #assets
    from anytime_plus_asset_detail
   where date(effective_from_dt) between '2015-09-24' and '2015-09-30'
;

      -- progs last week of sep
  select programme_name
        ,episode_number
        ,epg_group_name
        ,genre_description
        ,sub_genre_description
        ,count(distinct dk_programme_dim || subscriber_id) as num_viewed
        ,sum(datediff(second,instance_start_date_time_utc,instance_end_date_time_utc)) as viewed_duration
    into #progs
    from vespa_dp_prog_viewed_current as prg
   where playback_speed = 1
     and capped_full_flag = 0
     and date(instance_start_date_time_utc) > date(broadcast_start_date_time_utc)
     and date(event_start_date_time_utc) between '2015-09-24' and '2015-09-30'
group by programme_name
        ,episode_number
        ,epg_group_name
        ,genre_description
        ,sub_genre_description
;

      -- main sheet
  select case when programme_name like 'New %' then substr(programme_name,5,100) else programme_name end as prog
        ,episode_number
        ,epg_group_name
        ,genre_description
        ,sub_genre_description
        ,num_viewed
        ,viewed_duration
        ,case when asset_name is null then 0 else 1 end as active_asset
    from #progs as prg
         left join #assets as ass on case when programme_name like 'New %' then substr(programme_name,4,100) else programme_name end = ass.asset_name
   where programme_name is not null
     and epg_group_name <> 'Sky Push VOD'
order by viewed_duration desc
;

      -- main sheet excluding Sport, News & Weather
  select case when programme_name like 'New %' then substr(programme_name,5,100) else programme_name end as prog
        ,episode_number
        ,epg_group_name
        ,genre_description
        ,sub_genre_description
        ,num_viewed
        ,viewed_duration
        ,case when asset_name is null then 0 else 1 end as active_asset
    from #progs as prg
         left join #assets as ass on case when programme_name like 'New %' then substr(programme_name,4,100) else programme_name end = ass.asset_name
   where programme_name is not null
     and epg_group_name <> 'Sky Push VOD'
     and genre_description <> 'Sports'
     and (sub_genre_description <> 'News'
          or (programme_name not like '%News%' and programme_name not like '%Weather%')
         )
order by viewed_duration desc
;



