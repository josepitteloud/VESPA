
--drop table V159_Daily_viewing_summary_non_churners;
CREATE TABLE  V159_Daily_viewing_summary_non_churners
    ( viewing_date date
            ,pay_viewing_Duration                          float
            ,non_pay_viewing_Duration                           float     
)
;
commit;

--select count(*) from  v159_all_vespa_churners_since_2012;
--select viewing_day , count(*) from V159_Daily_viewing_summary_non_churners_churners_since_2012 group by viewing_day order by viewing_day;
-- Date range of programmes to capture
CREATE VARIABLE @snapshot_start_dt              datetime;
CREATE VARIABLE @snapshot_end_dt                datetime;
CREATE VARIABLE @viewing_var_sql                varchar(5000);
CREATE VARIABLE @viewing_scanning_day           datetime;
CREATE VARIABLE @playback_snapshot_start_dt            datetime;


SET @snapshot_start_dt  = '2013-03-03';  --Had to restart loop half way through
--SET @snapshot_start_dt  = '2012-08-01';  --Original
SET @snapshot_end_dt    = '2013-05-13';

SET @viewing_var_sql = '
        insert into V159_Daily_viewing_summary_non_churners
select cast(viewing_starts as date) as viewing_date
                ,sum(case when pay_channel=1 then scaling_weighting*viewing_Duration else 0 end ) as pay_channel_Duration
                ,sum(case when pay_channel=0 then scaling_weighting*viewing_Duration else 0 end ) as non_pay_channel_Duration
from vespa_analysts.VESPA_DAILY_AUGS_##^^*^*##  as a
left outer join V159_epg_data_phase_2 as b 
on a.programme_trans_sk=b.programme_trans_sk
left outer join  v159_all_vespa_churners_since_2012 as c
on a.account_number = c.account_number
where  c.account_number is null
group by viewing_date
'
;


SET @viewing_scanning_day = @snapshot_start_dt;

while @viewing_scanning_day <= @snapshot_end_dt
begin
    EXECUTE(replace(@viewing_var_sql,'##^^*^*##',dateformat(@viewing_scanning_day, 'yyyymmdd')))
    commit

    set @viewing_scanning_day = dateadd(day, 1, @viewing_scanning_day)
end

commit;


--select top 1000 * from V159_Daily_viewing_summary_non_churners
--select top 100 * from V159_epg_data_phase_2
--select count(*) from v159_all_vespa_churners_since_2012
--viewing_date,pay_viewing_Duration,non_pay_viewing_Duration
--'2012-10-04',7.7655695E10,1.31572998E11


insert into V159_Daily_viewing_summary_non_churners
select cast(viewing_starts as date) as viewing_date
                ,sum(case when pay_channel=1 then scaling_weighting*viewing_Duration else 0 end ) as pay_channel_Duration
                ,sum(case when pay_channel=0 then scaling_weighting*viewing_Duration else 0 end ) as non_pay_channel_Duration
from vespa_analysts.VESPA_DAILY_AUGS_20121101  as a
left outer join V159_epg_data_phase_2 as b 
on a.programme_trans_sk=b.programme_trans_sk
left outer join  v159_all_vespa_churners_since_2012 as c
on a.account_number = c.account_number
where  c.account_number is null
group by viewing_date




