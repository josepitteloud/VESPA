IF object_id('Data_Quality_Channel_Watching_nondq') IS not NULL drop procedure Data_Quality_Channel_Watching_nondq

go

create procedure Data_Quality_Channel_Watching_nondq( 
  -- Date of data analyzed or date process run
  @target_date date= null, -- Logger ID (so all builds end up in same queue)
  @CP2_build_ID bigint= null ) as
begin
  declare @viewing_data_date datetime
  declare @year_month char(6)
  declare @var_sql varchar(8000)
  declare @event_date integer
  set @viewing_data_date = @target_date
  set @year_month = (select convert(char(6),replace(@viewing_data_date,'-',''),'yyyymm'))
  set @event_date = (select convert(integer,replace(@target_date,'-',''),'yyyymmdd'))
  execute logger_add_event @CP2_build_ID,3,'Data_Quality_Channel_Watching Start',0
  set @var_sql = 'select pk_viewing_prog_instance_fact, service_key, live_recorded into #tmp_viewing_instances from sk_prod.vespa_dp_prog_viewed_' || @year_month
     || ' \x0Awhere dk_event_start_datehour_dim/100 = ' || @event_date || ''
  execute(@var_sql)
  select service_key,pk_channel_dim=max(pk_channel_dim)
    into #tmp_channel_key
    from sk_prod.viq_channel
    where date_from < @viewing_data_date
    and date_to > @viewing_data_date
    group by service_key
  select a.service_key,a.channel_name
    into #tmp_final_channel
    from sk_prod.viq_channel as a
      ,#tmp_channel_key as b
    where a.pk_channel_dim = b.pk_channel_dim
  insert into data_quality_channel_check
    ( service_key,channel_name,viewing_data_date,live_recorded,num_of_instances,dq_run_id ) 
    select a.*,viewing_data_date=@viewing_data_date,b.live_recorded,num_of_instances=count(b.pk_viewing_prog_instance_fact),@CP2_build_ID
      from #tmp_final_channel as a
        left outer join #tmp_viewing_instances as b
        on(a.service_key = b.service_key)
      group by a.service_key,a.channel_name,@viewing_data_date,b.live_recorded,@CP2_build_ID
  commit work
  execute logger_add_event @CP2_build_ID,3,'Data_Quality_Channel_Watching_nondq End',0
end

grant execute on Data_Quality_Channel_Watching_nondq to vespa_group_low_security, sk_prodreg, buxceys, kinnairt