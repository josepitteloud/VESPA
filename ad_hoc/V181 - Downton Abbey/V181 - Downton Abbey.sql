
---V181 Downton Abbey Analysis---

--
/*
select epg.pk_programme_instance_dim
,
     epg.Channel_Name
,
     epg.programme_instance_name
,
     epg.programme_instance_duration
,
     epg.Genre_Description
,
     epg.Sub_Genre_Description
,
     epg.broadcast_start_date_time_utc
,
     epg.broadcast_end_date_time_utc
,
     epg.broadcast_daypart
,
     epg.service_type_description
,synopsis
FROM   sk_prod.Vespa_programme_schedule as epg
where upper(programme_instance_name)  like '%DOWNTON ABBEY%' and broadcast_start_date_time_utc >='2012-08-01'
and channel_name like '%HD%'
  order by broadcast_start_date_time_utc
;
commit;
*/

select epg.pk_programme_instance_dim as programme_trans_sk 
,channel_name
into #downton_abbey
FROM   sk_prod.Vespa_programme_schedule as epg
where upper(programme_instance_name)  like '%DOWNTON ABBEY%' and broadcast_start_date_time_utc ='2012-09-16 20:00:00'
 order by broadcast_start_date_time_utc
;
commit;
CREATE UNIQUE INDEX idx1 ON #downton_abbey (programme_trans_sk);

---Create Input Table----

IF object_ID ('downton_abbey_viewers') IS NOT NULL THEN
            DROP TABLE  downton_abbey_viewers
END IF;
--select * from V159_Daily_viewing_summary_churners_since_2012;
CREATE TABLE  downton_abbey_viewers
    ( 
            Account_Number                             varchar(20)  not null
            ,total_duration                             bigint
            ,weight                                     float
)
;


commit;
-- Date range of programmes to capture

CREATE VARIABLE @snapshot_start_dt              datetime;
CREATE VARIABLE @snapshot_end_dt                datetime;
CREATE VARIABLE @viewing_var_sql                varchar(5000);
CREATE VARIABLE @viewing_scanning_day           datetime;
CREATE VARIABLE @playback_snapshot_start_dt            datetime;


SET @snapshot_start_dt  = '2012-09-16'; 
SET @snapshot_end_dt    = '2012-09-23';
-- Build string with placeholder for changing daily table reference
SET @viewing_var_sql = '
        insert into downton_abbey_viewers(
         Account_Number
        ,total_duration
        ,weight
)
SELECT account_number
,sum(viewing_Duration) as total_duration
,max(Scaling_Weighting) as weight  
FROM vespa_analysts.VESPA_DAILY_AUGS_##^^*^*## a
inner JOIN #downton_abbey b
         ON a.programme_trans_sk = b.programme_trans_sk
group by account_number
;
'
;
SET @viewing_scanning_day = @snapshot_start_dt;

while @viewing_scanning_day <= @snapshot_end_dt
begin
    EXECUTE(replace(@viewing_var_sql,'##^^*^*##',dateformat(@viewing_scanning_day, 'yyyymmdd')))
    commit

    set @viewing_scanning_day = dateadd(day, 1, @viewing_scanning_day)
end

--select top 5000 * into V159_Daily_viewing_summary_churners_since_2012_test from V159_Daily_viewing_summary_churners_since_2012;commit;
commit;

---Deduplicate Table

select Account_Number
        ,sum(total_duration) as duration
,max(weight) as max_weight
into #downton_abbey_deduped
from  downton_abbey_viewers
group by Account_Number
;

/*
select sum(max_weight) as total_weight
from #downton_abbey_deduped
*/

----Get List of Accounts for Profile---

select a.account_number
,b.weighting as overall_project_weighting
into downton_abbey_sep_16_base_accounts
from  vespa_analysts.SC2_intervals as a
inner join vespa_analysts.SC2_weightings as b
on  cast('2012-09-16' as date) = b.scaling_day
and a.scaling_segment_ID = b.scaling_segment_ID
and cast('2012-09-16' as date) between a.reporting_starts and a.reporting_ends
;
commit;

--select sum(overall_project_weighting) from downton_abbey_sep_16_base_accounts;

---Add on Attributes---
alter table downton_abbey_sep_16_base_accounts add region VARCHAR(40)     DEFAULT 'UNKNOWN';
alter table downton_abbey_sep_16_base_accounts add cb_address_postcode_area VARCHAR(10)     DEFAULT 'UNKNOWN';
alter table downton_abbey_sep_16_base_accounts add cb_address_postcode_district VARCHAR(10)     DEFAULT 'UNKNOWN';
UPDATE downton_abbey_sep_16_base_accounts
SET     Region                     = CASE WHEN sav.isba_tv_region = 'Not Defined'
                                       THEN 'UNKNOWN'
                                       ELSE sav.isba_tv_region
                                   END
,cb_address_postcode_area = sav.cb_address_postcode_area
,cb_address_postcode_district = sav.cb_address_postcode_district
FROM downton_abbey_sep_16_base_accounts AS base
        INNER JOIN sk_prod.cust_single_account_view AS sav ON base.account_number = sav.account_number
;
commit;

--select cb_address_postcode_area, count(*), sum(overall_project_weighting) from downton_abbey_sep_16_base_accounts group by cb_address_postcode_area

---Add on Downton Abbey Viewed Duration---

alter table downton_abbey_sep_16_base_accounts add duration_viewed bigint     DEFAULT 0;

update downton_abbey_sep_16_base_accounts
set duration_viewed = case when b.duration is null then 0 else b.duration end
from downton_abbey_sep_16_base_accounts as a
left outer join #downton_abbey_deduped as b
on a.account_number = b.account_number
;

commit;

--select abs(duration_viewed/60) as mins , count(*) as accounts from downton_abbey_sep_16_base_accounts group by mins order by mins


select Region
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when duration_viewed>=900 then overall_project_weighting else 0 end) as weighted_accounts_with_15plus_minutes_watched
from downton_abbey_sep_16_base_accounts
group by Region
order by weighted_accounts desc
;


select cb_address_postcode_area
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when duration_viewed>=900 then overall_project_weighting else 0 end) as weighted_accounts_with_15plus_minutes_watched
from downton_abbey_sep_16_base_accounts
group by cb_address_postcode_area
order by weighted_accounts desc
;

commit;

select cb_address_postcode_district
,count(*) as unweighted_accounts
,sum(overall_project_weighting) as weighted_accounts
,sum(case when duration_viewed>=900 then overall_project_weighting else 0 end) as weighted_accounts_with_15plus_minutes_watched
from downton_abbey_sep_16_base_accounts
where cb_address_postcode_area='L'
group by cb_address_postcode_district
order by weighted_accounts desc
;



