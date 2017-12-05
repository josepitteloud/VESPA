IF object_id('Data_Quality_Channel_Watching') IS not NULL drop procedure Data_Quality_Channel_Watching;
go

create procedure Data_Quality_Channel_Watching
    @target_date        date = NULL     -- Date of data analyzed or date process run
    ,@CP2_build_ID     bigint = NULL   -- Logger ID (so all builds end up in same queue)
as
begin
declare @viewing_data_date datetime

set @viewing_data_date = @target_date

EXECUTE logger_add_event @CP2_build_ID , 3,'Data_Quality_Channel_Watching Start',0

--get a list of all active channels for viewing day being processed

select service_key, max(pk_channel_dim) pk_channel_dim
into #tmp_channel_key
from sk_prod.viq_channel
where date_from < @viewing_data_date
and date_to > @viewing_data_date
group by service_key

--get channel name of channel on date it was processed

select a.service_key, a.channel_name 
into #tmp_final_channel
from sk_prod.viq_channel a,
 #tmp_channel_key b
where a.pk_channel_dim = b.pk_channel_dim

--insert into main data_quality_channel_check table all totals for active channels by Live/Recorded

insert into data_quality_channel_check
(service_key, channel_name, viewing_data_date, live_recorded, num_of_instances, dq_run_id)
select a.*, @viewing_data_date viewing_data_date,b.live_recorded, count(b.pk_viewing_prog_instance_fact) num_of_instances, @CP2_build_ID
from 
#tmp_final_channel a
left outer join
data_quality_dp_data_audit b
on
(a.service_key = b.service_key)
group by a.service_key, a.channel_name, @viewing_data_date,b.live_recorded, @CP2_build_ID

commit


EXECUTE logger_add_event @CP2_build_ID , 3,'Data_Quality_Channel_Watching Table Refresh End',0


----section to populate Excel tables with the actual records that have an issue--

EXECUTE logger_add_event @CP2_build_ID , 3,'Data_Quality_Channel_Watching Issue Table Refresh Start',0

select convert(char,viewing_data_date, 112) event_date, service_key, live_recorded, max(pk_dq_chan_chk) pk_dq_chan_chk
into #tmp_pk_chan_chk
from data_quality_channel_check
group by convert(char,viewing_data_date, 112) , service_key, live_recorded

--rule 1

--gets those records with null viewing against channels we have in the system as active
select event_date, service_key
into #tmp_pk_chan_chk_null_view
from #tmp_pk_chan_chk
group by event_date, service_key
having count(distinct live_recorded) = 0


--rule 2
--gets those records with Live viewing against channels we have in the system as active

select event_date, service_key
into #tmp_pk_chan_chk_one_type_view_live
from #tmp_pk_chan_chk
group by event_date, service_key
having count(distinct live_recorded) = 1
and sum(case when live_recorded = 'LIVE' then 1 else 0 end) = 1

--rule 3

--gets those records with Recorded viewing only against channels we have in the system as active

select event_date, service_key
into #tmp_pk_chan_chk_one_type_view_rec
from #tmp_pk_chan_chk
group by event_date, service_key
having count(distinct live_recorded) = 1
and sum(case when live_recorded = 'RECORDED' then 1 else 0 end) = 1


--rule 4

--gets those records where live have fewer counts than recorded i.e. when we compare live against recorded, we have under 10% of 
--viewing for that date going to Live.

select chan.service_key,convert(char,viewing.viewing_data_date, 112) event_date,viewing.channel_name, viewing.live_recorded playback_type, 
viewing.num_of_instances instances,
datepart(month,viewing.viewing_data_date) month,datepart(year,viewing.viewing_data_date) year
into #tmp_pk_chan_chk_viewing
from
data_quality_channel_service_key_list chan
inner join
data_quality_channel_check viewing
on
chan.service_key = viewing.service_key
inner join
#tmp_pk_chan_chk listing
on
viewing.pk_dq_chan_chk = listing.pk_dq_chan_chk
where chan.current_flag = 1

select service_key, event_date,case when playback_type = 'LIVE' then instances else 0 end LIVE_Instances
INTO #TMP_CHANNEL_LIVE_INSTANCES
FROM #tmp_pk_chan_chk_viewing
WHERE playback_type = 'LIVE' 

select service_key, event_date, case when playback_type = 'RECORDED' then instances else 0 end RECORDED_Instances
INTO #TMP_CHANNEL_REC_INSTANCES
FROM #tmp_pk_chan_chk_viewing
WHERE playback_type = 'RECORDED' 

SELECT a.*, b.RECORDED_Instances 
into #tmp_channel_live_less_rec
FROM #TMP_CHANNEL_LIVE_INSTANCES a,
#TMP_CHANNEL_REC_INSTANCES b
where a.service_key = b.service_key
and a.event_date = b.event_date
and (1.0 * a.live_instances/ (a.live_instances + b.recorded_instances)) * 100 < 10


-------------------------------------------------------------------------------------------

--rule 5

---rank on channel where we have under 20% of viewing on average for a channel then flag

select a.service_key, a.viewing_data_date, num_recs,percent_rank () over (partition by a.service_key order by a.num_recs) percent_rank_val
into #tmp_channel_percent_val
from
(select a.service_key, a.viewing_data_date, sum(a.num_of_instances) num_recs
 from data_quality_channel_check a,
(select service_key, viewing_data_date, live_recorded, 
max(pk_dq_chan_chk) pk_dq_chan_chk
from data_quality_channel_check
group by service_key, viewing_data_date, live_recorded) b
where a.pk_dq_chan_chk = b.pk_dq_chan_chk
group by a.service_key, a.viewing_data_date) a

select service_key, avg(num_recs) average_val 
into #tmp_channel_average_val
from #tmp_channel_percent_val
where (percent_rank_val > 0.1 and percent_rank_val < 0.9)
group by service_key

select a.service_key, a.viewing_data_date, sum(a.num_of_instances) num_recs
into #tmp_channel_total_recs
from data_quality_channel_check a,
(select service_key, viewing_data_date, live_recorded, 
max(pk_dq_chan_chk) pk_dq_chan_chk
from data_quality_channel_check
group by service_key, viewing_data_date, live_recorded) b
where a.pk_dq_chan_chk = b.pk_dq_chan_chk
group by a.service_key, a.viewing_data_date

select a.*,b.viewing_data_date, b.num_recs,
 abs(1.0 * b.num_recs/a.average_val) val ,convert(char,b.viewing_data_date, 112) event_date
into #tmp_channel_total_recs_final
from  #tmp_channel_average_val a,
#tmp_channel_total_recs b
where a.service_key = b.service_key
-- percentage below which we want to flag currently below 20%
and val < 0.2


----------------------

--rule 5


---invalidate entire days where the data is below expected thresholds


select a.viewing_data_date, sum(num_recs) number_recs,percent_rank () over (order by sum(num_recs)) percent_rank_val
into #tmp_total_view_percent_val
from
(select a.service_key, a.viewing_data_date, sum(a.num_of_instances) num_recs
 from data_quality_channel_check a,
(select service_key, viewing_data_date, live_recorded, 
max(pk_dq_chan_chk) pk_dq_chan_chk
from data_quality_channel_check
group by service_key, viewing_data_date, live_recorded) b
where a.pk_dq_chan_chk = b.pk_dq_chan_chk
group by a.service_key, a.viewing_data_date) a
group by a.viewing_data_date

select 'avg_recs' average_recs, avg(number_recs) average_val 
into #tmp_total_average_val
from #tmp_total_view_percent_val
where (percent_rank_val > 0.1 and percent_rank_val < 0.9)

select a.*,b.viewing_data_date, b.number_recs,
 abs(1.0 * b.number_recs/a.average_val) val ,convert(char,b.viewing_data_date, 112) event_date
into #tmp_total_recs_final
from  #tmp_total_average_val a,
#tmp_total_view_percent_val b
-- percentage below which we want to flag currently below 20%
where val < 0.5


---------------------------------------------------------------------------------------------------------------

--1
select view_final.* into #tmp_pk_chan_chk_viewing_final
from #tmp_pk_chan_chk_viewing view_final
inner join
#tmp_channel_live_less_rec live_less_rec
on
view_final.service_key = live_less_rec.service_key 
and view_final.event_date = live_less_rec.event_date

--2

insert into #tmp_pk_chan_chk_viewing_final
select view_final.* 
from #tmp_pk_chan_chk_viewing view_final 
inner join
#tmp_pk_chan_chk_null_view null_view
on
view_final.service_key = null_view.service_key 
and view_final.event_date = null_view.event_date

--3

insert into #tmp_pk_chan_chk_viewing_final
select view_final.* 
from #tmp_pk_chan_chk_viewing view_final 
inner join
#tmp_pk_chan_chk_one_type_view_rec rec_view
on
view_final.service_key = rec_view.service_key 
and view_final.event_date = rec_view.event_date

--4
insert into #tmp_pk_chan_chk_viewing_final
select view_final.* 
from #tmp_pk_chan_chk_viewing view_final 
inner join
#tmp_channel_total_recs_final miss_partial_view
on
view_final.service_key = miss_partial_view.service_key 
and view_final.event_date = miss_partial_view.event_date

--5

insert into #tmp_pk_chan_chk_viewing_final
select view_final.* 
from #tmp_pk_chan_chk_viewing view_final 
inner join
#tmp_total_recs_final miss_all_view
on
view_final.event_date = miss_all_view.event_date


delete from data_quality_channel_issues_list

insert into data_quality_channel_issues_list
select * into data_quality_channel_issues_list
from #tmp_pk_chan_chk_viewing_final

commit


EXECUTE logger_add_event @CP2_build_ID , 3,'Data_Quality_Channel_Watching Issue Table Refresh End',0

EXECUTE logger_add_event @CP2_build_ID , 3,'Data_Quality_Channel_Watching Process End',0

end

go

grant execute on Data_Quality_Channel_Watching to vespa_group_low_security, sk_prodreg, buxceys, kinnairt