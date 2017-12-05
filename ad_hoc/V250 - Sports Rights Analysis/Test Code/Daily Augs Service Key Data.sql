--MACRO DECLARTION;
CREATE VARIABLE @snapshot_start_dt              datetime;
CREATE VARIABLE @snapshot_end_dt                datetime;
CREATE VARIABLE @linear_var_sql                varchar(10000);
CREATE VARIABLE @linear_scanning_day           datetime;

--SET @snapshot_start_dt  = '2012-11-01';
SET @snapshot_start_dt  = '2013-04-13';
SET @snapshot_end_dt    = '2013-10-31';
--select * from Augs_Tables_Dates_Available
--select @linear_scanning_day
--create skinng distinct accounts for cohort
--if object_id ('V226_sprint4_skinny_cohort') is not null
--then drop table V226_sprint4_skinny_cohort
--end if;
--
--select      distinct account_number
--            ,monthly_dial_back10_flag
--into        V226_sprint4_skinny_cohort
--from        lakhanis.Sprint_2_datacube
--where       monthly_dial_back10_flag=1
--order by    account_number
--;


if object_id ('v250_daily_table_test') is not null
then drop table v250_daily_table_test
end if;

create table v250_daily_table_test(
                       
                        viewing_day                    date
                        ,service_key                    int
,channel_name varchar(100)
                        ,duration_instance              int
)
;


set @linear_var_sql ='
insert into v250_daily_table_test(
viewing_day
,service_key
,channel_name
,duration_instance
)
select            cast(viewing_starts as date) as viewing_day
,service_key
,channel_name
                ,sum(viewing_duration)             as duration_instance                                  
         
from            vespa_analysts.vespa_daily_augs_##^^*^*## as a
left outer join sk_prod.Vespa_programme_schedule as b
ON a.programme_trans_sk = b.pk_programme_instance_dim
                
where           viewing_duration>=180
and             subscriber_id>0--not issue with daily aug files
group by viewing_day
,service_key,channel_name
;
'
;

SET @linear_scanning_day = @snapshot_start_dt;

while @linear_scanning_day <= @snapshot_end_dt
begin
    EXECUTE(replace(@linear_var_sql,'##^^*^*##',dateformat(@linear_scanning_day, 'yyyymmdd')))
    commit
    set @linear_scanning_day = dateadd(day, 1, @linear_scanning_day)
end
;
commit;
--select top 1 from vespa_analysts.vespa_daily_augs_20121101 
--select viewing_day , count(*) from v250_daily_table_test group by viewing_day order by viewing_day


select viewing_day , service_key ,Max(channel_name) as ch,sum(duration_instance) as tot_dur from v250_daily_table_test
where left(channel_name,10)='Sky Sports' 
group by viewing_day , service_key  order by viewing_day,ch
