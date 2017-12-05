



----Capping Phase 2 Comparison - Viewing 5th-18th feb Inclusive
--drop table dbarnett.project078_viewing_3plus_continuous_minutes;
CREATE VARIABLE @var_prog_period_start  datetime;
CREATE VARIABLE @var_prog_period_end    datetime;
CREATE VARIABLE @var_sql                varchar(15000);
CREATE VARIABLE @var_cntr               smallint;
CREATE VARIABLE @var_num_days           smallint;

  -- If your programme listing is by a date range...

----Will return an error if any of the daily AUGS tables are not created (e.g., at time of writing only from 26th Aug onwards)

SET @var_prog_period_start  = '2012-07-26';
--SET @var_prog_period_end    = '2012-07-27';
SET @var_prog_period_end    = '2012-08-06';


SET @var_cntr = 0;
SET @var_num_days = 12;

IF object_id('dbarnett.project078_viewing_3plus_continuous_minutes') IS NOT NULL DROP TABLE  dbarnett.project078_viewing_3plus_continuous_minutes;
create table dbarnett.project078_viewing_3plus_continuous_minutes
(subscriber_id bigint
,account_number varchar(20)
,programme_trans_sk bigint
,scaling_segment_id bigint  
,scaling_weighting  real
,viewing_starts_utc datetime
,viewing_starts_local datetime
,viewing_stops_utc datetime
,viewing_stops_local datetime
,viewing_start_date as date
,Channel_Name as varchar(40)
,epg_title as varchar(40) 
,tx_start_datetime_utc datetime
,tx_end_datetime_utc datetime
,tx_start_datetime_local datetime
,tx_end_datetime_local datetime
);

SET @var_sql = '
insert into dbarnett.project078_viewing_3plus_continuous_minutes
select subscriber_id
,account_number
,a.programme_trans_sk
,scaling_segment_id
,scaling_weighting
,viewing_starts as viewing_starts_utc
,case 
when dateformat(viewing_starts,''YYYY-MM-DD-HH'') between ''2010-03-28-02'' and ''2010-10-31-02'' then dateadd(hh,1,viewing_starts) 
when dateformat(viewing_starts,''YYYY-MM-DD-HH'') between ''2011-03-27-02'' and ''2011-10-30-02'' then dateadd(hh,1,viewing_starts) 
when dateformat(viewing_starts,''YYYY-MM-DD-HH'') between ''2012-03-25-02'' and ''2012-10-28-02'' then dateadd(hh,1,viewing_starts) 
                    else viewing_starts  end as viewing_starts_local
,viewing_stops as viewing_stops_utc
,case 
when dateformat(viewing_stops,''YYYY-MM-DD-HH'') between ''2010-03-28-02'' and ''2010-10-31-02'' then dateadd(hh,1,viewing_stops) 
when dateformat(viewing_stops,''YYYY-MM-DD-HH'') between ''2011-03-27-02'' and ''2011-10-30-02'' then dateadd(hh,1,viewing_stops) 
when dateformat(viewing_stops,''YYYY-MM-DD-HH'') between ''2012-03-25-02'' and ''2012-10-28-02'' then dateadd(hh,1,viewing_stops) 
                    else viewing_stops  end as viewing_stops_local
,cast (viewing_starts_local as date) as viewing_start_date
,Channel_Name 
,epg_title
,tx_start_datetime_utc
,tx_end_datetime_utc

,case 
when dateformat(tx_start_datetime_utc,''YYYY-MM-DD-HH'') between ''2010-03-28-02'' and ''2010-10-31-02'' then dateadd(hh,1,tx_start_datetime_utc) 
when dateformat(tx_start_datetime_utc,''YYYY-MM-DD-HH'') between ''2011-03-27-02'' and ''2011-10-30-02'' then dateadd(hh,1,tx_start_datetime_utc) 
when dateformat(tx_start_datetime_utc,''YYYY-MM-DD-HH'') between ''2012-03-25-02'' and ''2012-10-28-02'' then dateadd(hh,1,tx_start_datetime_utc) 
                    else tx_start_datetime_utc  end as tx_start_datetime_local

,case 
when dateformat(tx_end_datetime_utc,''YYYY-MM-DD-HH'') between ''2010-03-28-02'' and ''2010-10-31-02'' then dateadd(hh,1,tx_end_datetime_utc) 
when dateformat(tx_end_datetime_utc,''YYYY-MM-DD-HH'') between ''2011-03-27-02'' and ''2011-10-30-02'' then dateadd(hh,1,tx_end_datetime_utc) 
when dateformat(tx_end_datetime_utc,''YYYY-MM-DD-HH'') between ''2012-03-25-02'' and ''2012-10-28-02'' then dateadd(hh,1,tx_end_datetime_utc) 
                    else tx_end_datetime_utc  end as tx_end_datetime_local

from vespa_analysts.VESPA_DAILY_AUGS_##^^*^*## as a
left outer join sk_prod.vespa_epg_dim as b
on a.programme_trans_sk=b.programme_trans_sk
where timeshifting = ''LIVE''
and tx_start_datetime_local between ''2012-07-24 06:00:00'' and ''2012-08-05 05:59:59''
and 
 channel_name in (
''Sky Arts 1''
,''Sky Arts 1 HD''
,''Sky1''
,''Sky1 HD''
,''Sky Living''
,''Sky Living HD'')
' 
;    

while @var_cntr < @var_num_days
begin
    EXECUTE(replace(@var_sql,'##^^*^*##',dateformat(dateadd(day, @var_cntr, @var_prog_period_start), 'yyyymmdd')))

    commit

    set @var_cntr = @var_cntr + 1
end;


commit;

commit;
create hg index idx1 on dbarnett.project078_viewing_3plus_continuous_minutes (subscriber_id);
create hg index idx2 on dbarnett.project078_viewing_3plus_continuous_minutes (account_number);
create date index idx3 on dbarnett.project078_viewing_3plus_continuous_minutes (viewing_start_date);
--select count(*) from dbarnett.project078_viewing_3plus_continuous_minutes;

--select top 100 * from dbarnett.project078_viewing_3plus_continuous_minutes;

--Update scaling segment and weighting and add programme channel details---

update dbarnett.project078_viewing_3plus_continuous_minutes
set scaling_segment_id=b.scaling_segment_id
from dbarnett.project078_viewing_3plus_continuous_minutes  as a
left outer join vespa_analysts.SC2_intervals as b
on a.account_number = b.account_number
where  viewing_start_date  between b.reporting_starts and b.reporting_ends
commit;
create hg index idx4 on dbarnett.project078_viewing_3plus_continuous_minutes (scaling_segment_id);
commit;

--select top 100 * from vespa_analysts.SC2_weightings;
--select top 100 * from dbarnett.project078_viewing_3plus_continuous_minutes;

----Having problems with SC2_weightings table so will try creating subset and adding indicies.


select * into dbarnett.sc2_weighting_subset_for_078 from vespa_analysts.SC2_weightings where scaling_day between '2012-07-26' and '2012-08-06'; commit;


commit;
create hg index idx1 on dbarnett.sc2_weighting_subset_for_078 (scaling_day);
create hg index idx2 on dbarnett.sc2_weighting_subset_for_078 (scaling_segment_id);
grant all on dbarnett.sc2_weighting_subset_for_078 to public;
commit;

update dbarnett.project078_viewing_3plus_continuous_minutes
set scaling_weighting=b.weighting
from dbarnett.project078_viewing_3plus_continuous_minutes  as a
left outer join dbarnett.sc2_weighting_subset_for_078  as b
on  a.viewing_start_date = b.scaling_day and a.scaling_segment_id=b.scaling_segment_id
where a.scaling_segment_id is not null
commit;

---Combine HD and Non HD version

select *,'Channel Name Here' as channel_name_grouped into  dbarnett.project078_viewing_3plus_continuous_minutes_copy from  dbarnett.project078_viewing_3plus_continuous_minutes;
commit;

drop table  dbarnett.project078_viewing_3plus_continuous_minutes;
select * into  dbarnett.project078_viewing_3plus_continuous_minutes from  dbarnett.project078_viewing_3plus_continuous_minutes_copy;
drop table  dbarnett.project078_viewing_3plus_continuous_minutes_copy;

--alter table dbarnett.project078_viewing_3plus_continuous_minutes add channel_name_grouped varchar(40);

update dbarnett.project078_viewing_3plus_continuous_minutes
set channel_name_grouped= case   when channel_name ='Sky Arts 1 HD' then  'Sky Arts 1'
                                when channel_name ='Sky1 HD' then  'Sky1'
                                when channel_name ='Sky Living HD' then  'Sky Living' else channel_name end
from dbarnett.project078_viewing_3plus_continuous_minutes
;

grant all on dbarnett.project078_viewing_3plus_continuous_minutes to public;
commit;

--select * from dbarnett.project078_viewing_3plus_continuous_minutes;
--select * from vespa_analysts.scaling_dialback_intervals where account_number = '220014719664'
---Dedupe to one record per box per programme---

select account_number
,subscriber_id
,channel_name_grouped
,epg_title
,tx_start_datetime_local
,tx_end_datetime_local
,scaling_weighting
into dbarnett.project078_viewing_3plus_continuous_minutes_deduped
from dbarnett.project078_viewing_3plus_continuous_minutes
group by account_number
,subscriber_id
,channel_name_grouped
,epg_title
,tx_start_datetime_local
,tx_end_datetime_local
,scaling_weighting
;
commit;

grant all on dbarnett.project078_viewing_3plus_continuous_minutes_deduped to public;
commit;


--select top 100 * from dbarnett.project078_viewing_3plus_continuous_minutes;
--select top 100 * from dbarnett.project078_viewing_3plus_continuous_minutes_deduped;

--select viewing_start_date , count(*) , sum(case when scaling_weighting is null then 1 else 0 end) from dbarnett.project078_viewing_3plus_continuous_minutes group by viewing_start_date order by viewing_start_date;


