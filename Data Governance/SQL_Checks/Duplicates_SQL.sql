commit
select top 1000 * from z_logger_events
order by 1 desc
commit

select * from sk_prod.vespa_dp_prog_viewed_201402 where pk_viewing_prog_instance_fact in (
52263684272,
52263681103
)


select a.dk_event_start_datehour_dim/100, cb_change_date,count(1) from

drop table tmp_data_tk

select distinct dk_event_start_datehour_dim

declare @date_val int

select distinct dk_event_start_datehour_dim/100 date_val, 1 cnt 
into #tmp_dqvm
from sk_prod.vespa_dp_prog_viewed_201401

SELECT date_val into #temp FROM #tmp_dqvm

-- Loop through the rows of the temp table
while exists (select 1 from #temp)
begin
  set rowcount 1
  select @date_val   = date_val  from #temp -- pull one uid from the temp table
  set rowcount 0
  delete from #temp where date_val  = @date_val   -- delete that uid from the temp table

insert into tmp_data_tk
select pk_viewing_prog_instance_fact, 1
from sk_prod.vespa_dp_prog_viewed_201401
where dk_event_start_datehour_dim/100 = @date_val
group by pk_viewing_prog_instance_fact
having count(*) > 1

select count(1) from tmp_data_tk

end

commit

insert into tk_tst_pks
select a.cb_change_date, a.dk_event_start_datehour_dim/100, count(a.pk_viewing_prog_instance_fact) cnt
from sk_prod.vespa_dp_prog_viewed_201401 a, tmp_data_tk b
where a.pk_viewing_prog_instance_fact = b.pk_viewing_prog_instance_fact
group by a.cb_change_date, a.dk_event_start_datehour_dim/100

commit

select sum(cnt) from tk_tst_pks
where cb_change_date = '2014-02-14'

select sum(cnt) from tk_tst_pks
where cb_change_date = '2014-02-14'

select * into tk_tst_pks_feb from tk_tst_pks
select * into tmp_data_tk_feb from tmp_data_tk

commit

delete from tk_tst_pks
delete from tmp_data_tk

commit

(select pk_viewing_prog_instance_fact
from sk_prod.vespa_dp_prog_viewed_201402
where dk_event_start_datehour_dim/100 = 20140204
group by pk_viewing_prog_instance_fact
having count(*) > 1) a


select dk_event_start_datehour_dim/100, count(1)
from sk_prod.vespa_dp_prog_viewed_201402
group by dk_event_start_datehour_dim/100

commit

select * from tk_tst_pks


select top 10 * from z_logger_events
order by 1 desc

select 

