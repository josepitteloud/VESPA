----Production of Enlarged EPG List of Rights----
--Create Back up Version

select * into dbarnett.v250_sports_rights_epg_data_for_analysis_old from dbarnett.v250_sports_rights_epg_data_for_analysis ;
commit; 

---Add Back to Vespa Programme Schedule----------


--select top 100 * from dbarnett.v250_sports_rights_epg_data_for_analysis 

select row_number
,service_key
,broadcast_start_date_time_local
into #programmes
from dbarnett.v250_sports_rights_epg_data_for_analysis 
group by row_number
,service_key
,broadcast_start_date_time_local
;
commit;
CREATE HG INDEX idx1 ON #programmes (broadcast_start_date_time_local);
CREATE HG INDEX idx2 ON #programmes (service_key);
--Match to Programme Schedule---

select a.row_number
,b.dk_programme_instance_dim
into #get_all_dk
from #programmes as a
left outer join sk_prod.Vespa_programme_schedule as b
on a.service_key=b.service_key and a.broadcast_start_date_time_local=b.broadcast_start_date_time_local
group by a.row_number
,b.dk_programme_instance_dim
;
commit;

select row_number
,count(*) as records
into #count_by_row
from #get_all_dk
group by row_number
;

select cast(broadcast_start_date_time_local as date) as bcast_date
,sum(records) as tot_rec
,count(*) as a_rec
from #count_by_row as a
left outer join #programmes as b
on a.row_number = b.row_number
group by bcast_date
order by bcast_date


---Match back to master table to get new master--

select b.*
,a.dk_programme_instance_dim as dk_programme_instance_dim_new
into dbarnett.v250_sports_rights_epg_data_for_analysis_reworked
from #get_all_dk as a
left outer join dbarnett.v250_sports_rights_epg_data_for_analysis as b
on a.row_number=b.row_number
;

commit;

--select top 100 * from dbarnett.v250_sports_rights_epg_data_for_analysis_reworked;
--select top 100 * from dbarnett.v250_sports_rights_epg_data_for_analysis_old;


---Copy Over New dim code

update dbarnett.v250_sports_rights_epg_data_for_analysis_reworked
set dk_programme_instance_dim=dk_programme_instance_dim_new
from dbarnett.v250_sports_rights_epg_data_for_analysis_reworked
;

commit;

drop table dbarnett.v250_sports_rights_epg_data_for_analysis;
select * into dbarnett.v250_sports_rights_epg_data_for_analysis from dbarnett.v250_sports_rights_epg_data_for_analysis_reworked;
commit;
CREATE HG INDEX idx1 ON dbarnett.v250_sports_rights_epg_data_for_analysis (dk_programme_instance_dim);
commit;

