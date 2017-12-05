-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
declare @viewing_date date

set @viewing_date = '2013-07-01'

-----------------get channel data that is active for the date you are analyzing

select ssp_network_id, transport_id, service_id, service_key, pk_channel_dim,
date_from, date_to
into #tmp_channel_active
from sk_prod.viq_channel
where date(date_from) <= @viewing_date
and date(date_to) > @viewing_date

-----------------get channel key from viewing table for day you are checking

select distinct prog_inst_channel_key 
into #tmp_channel_viewing_key
from sk_prod.viq_viewing_data
--where viewing_start_date_key/100 = cast (replace(cast (@viewing_date as varchar(12)),'-','') as int)
where broadcast_start_date_key/100 = cast (replace(cast (@viewing_date as varchar(12)),'-','') as int)
and prog_inst_channel_key > 0

-----------------get channel data for channels in viewing table for particular viewing day

select ssp_network_id, transport_id, service_id, service_key, pk_channel_dim,
date_from, date_to
into #tmp_channel_programme_viewing
from sk_prod.viq_channel a, #tmp_channel_viewing_key b
where a.pk_channel_dim = b.prog_inst_channel_key

-----------------compare channel to programme viewing to see what differences are

select act_chan.*, act_prog.ssp_network_id prog_ssp_network_id,
act_prog.transport_id prog_transport_id,
act_prog.service_id prog_service_id,
act_prog.service_key prog_service_key,
act_prog.pk_channel_dim prog_pk_channel_dim,
act_prog.date_from prog_date_from,
act_prog.date_to prog_date_to
into #chan_prog_view_link_chk
from
#tmp_channel_active act_chan, 
#tmp_channel_programme_viewing act_prog
where act_chan.ssp_network_id = act_prog.ssp_network_id
and act_chan.transport_id = act_prog.transport_id
and act_chan.service_id = act_prog.service_id 
and act_chan.service_key = act_prog.service_key
and act_chan.pk_channel_dim != act_prog.pk_channel_dim

--check channel against programme_schedule

select distinct dk_channel_id 
into #tmp_channel_prog_sched_active
from sk_prod.viq_programme_schedule
where dk_start_datehour/100 = cast (replace(cast (@viewing_date as varchar(12)),'-','') as int)

-----------------get channel information for the channel keys that exist on the schedule table
-----------------for a particular viewing day

select ssp_network_id, transport_id, service_id, service_key, pk_channel_dim,
date_from, date_to
into #tmp_channel_schedule
from sk_prod.viq_channel a, #tmp_channel_prog_sched_active b
where a.pk_channel_dim = b.dk_channel_id

-----------------compare channel to programme schedule for a particular viewing day

select act_chan.*, act_sched.ssp_network_id prog_ssp_network_id,
act_sched.transport_id sched_transport_id,
act_sched.service_id sched_service_id,
act_sched.service_key sched_service_key,
act_sched.pk_channel_dim sched_pk_channel_dim,
act_sched.date_from sched_date_from,
act_sched.date_to sched_date_to
into #chan_prog_sched_link_chk
from
#tmp_channel_active act_chan, 
#tmp_channel_schedule act_sched
where act_chan.ssp_network_id = act_sched.ssp_network_id
and act_chan.transport_id = act_sched.transport_id
and act_chan.service_id = act_sched.service_id 
and act_chan.service_key = act_sched.service_key
and act_chan.pk_channel_dim != act_sched.pk_channel_dim

-----------------compare viewing to programme schedule for a particular viewing day

select chan_sched.*, chan_prog.ssp_network_id prog_ssp_network_id,
chan_prog.transport_id prog_transport_id,
chan_prog.service_id prog_service_id,
chan_prog.service_key prog_service_key,
chan_prog.pk_channel_dim sched_pk_channel_dim,
chan_prog.date_from sched_date_from,
chan_prog.date_to sched_date_to
into #viewing_prog_sched_link_chk
from
#tmp_channel_schedule chan_sched,
#tmp_channel_programme_viewing chan_prog
where chan_sched.ssp_network_id = chan_prog.ssp_network_id
and chan_sched.transport_id = chan_prog.transport_id
and chan_sched.service_id = chan_prog.service_id 
and chan_sched.service_key = chan_prog.service_key
and chan_sched.pk_channel_dim != chan_prog.pk_channel_dim


---select metric result from link broken between schedule and Fact table
select * from #viewing_prog_sched_link_chk
