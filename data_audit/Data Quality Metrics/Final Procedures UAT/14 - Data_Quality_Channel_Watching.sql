IF object_id('Data_Quality_Channel_Watching') IS not NULL drop procedure Data_Quality_Channel_Watching
commit

create procedure Data_Quality_Channel_Watching
    @target_date        date = NULL     -- Date of data analyzed or date process run
    ,@CP2_build_ID     bigint = NULL   -- Logger ID (so all builds end up in same queue)
as
begin
declare @viewing_data_date datetime

set @viewing_data_date = @target_date

EXECUTE logger_add_event @RunID , 3,'Data_Quality_Channel_Watching Start',0

select service_key, max(pk_channel_dim) pk_channel_dim
into #tmp_channel_key
from sk_prod.viq_channel
where date_from < @viewing_data_date
and date_to > @viewing_data_date
group by service_key

select a.service_key, a.channel_name 
into #tmp_final_channel
from sk_prod.viq_channel a,
 #tmp_channel_key b
where a.pk_channel_dim = b.pk_channel_dim

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

EXECUTE logger_add_event @RunID , 3,'Data_Quality_Channel_Watching End',0

end

go
