/*-----------------------------------------------------------------------------------------------------------------
        Project:V250 - Sports right Analysis Profiling
        Part - Part 03 (Load Live and Non Live along with Rights Split data)
        
        Analyst: Dan Barnett
        SK Prod: 5

        From the exported list of matches - offline (i.e, within Excel) incorrect matches are removed i.e., where 
        rights data does not look like it matches EPG data (mainly an issue for Regional channels)
        Live/Non Live split added manually along with rights splits

*/------------------------------------------------------------------------------------------------------------------

--select distinct analysis_right from dbarnett.v250_epg_live_non_live_lookup order by analysis_right;


---Add On Live Non Live Splits 
---v2 also has updated analysis right info (e.g., Day of Week for Champions League etc.,)

--drop table  dbarnett.v250_epg_live_non_live_lookup;
create table dbarnett.v250_epg_live_non_live_lookup
(row_number integer
,live integer
,analysis_right varchar(255)
)
;
commit;
input into dbarnett.v250_epg_live_non_live_lookup
from 'G:\RTCI\Lookup Tables\v250 - Sports Rights Analysis\Live and Non Live Rightsv2.csv' format ascii;

commit;



---Create Analysis Table of EPG Data ---
drop table dbarnett.v250_sports_rights_epg_data_for_analysis;
select a.*
,b.live
,b.analysis_right as analysis_right_new
into dbarnett.v250_sports_rights_epg_data_for_analysis
from dbarnett.v250_sports_rights_epg_detail as a
left outer join dbarnett.v250_epg_live_non_live_lookup as b
on a.row_number = b.row_number 
where b.live is not null
;
--select * from dbarnett.v250_sports_rights_epg_data_for_analysis where channel_name='Eurosport';

CREATE HG INDEX idx1 ON dbarnett.v250_sports_rights_epg_data_for_analysis (dk_programme_instance_dim);
commit;

update dbarnett.v250_sports_rights_epg_data_for_analysis
set broadcast_datetime_text= cast(broadcast_start_date_time_local as varchar)
from  dbarnett.v250_sports_rights_epg_data_for_analysis
where broadcast_datetime_text is null
;
commit;



----Dedup by dk_programme_instance_dim---

select dk_programme_instance_dim
,min(row_number) as first_record_per_dk
into #first_rec_per_dk
from dbarnett.v250_sports_rights_epg_data_for_analysis
group by dk_programme_instance_dim
;
--select count(*) from dbarnett.v250_sports_rights_epg_data_for_analysis
--select * from #first_rec_per_dk;
commit;
CREATE HG INDEX idx1 ON #first_rec_per_dk(first_record_per_dk);
commit;
--alter table dbarnett.v250_sports_rights_epg_data_for_analysis delete first_dk_record;
alter table dbarnett.v250_sports_rights_epg_data_for_analysis add first_dk_record bigint;

update dbarnett.v250_sports_rights_epg_data_for_analysis
set first_dk_record=case when b.first_record_per_dk is not null then 1 else 0 end
from  dbarnett.v250_sports_rights_epg_data_for_analysis as a
left outer join #first_rec_per_dk as b
on a.row_number=b.first_record_per_dk
;
commit;
--select sum(first_dk_record) , count(*) from dbarnett.v250_sports_rights_epg_data_for_analysis

delete from dbarnett.v250_sports_rights_epg_data_for_analysis where first_dk_record=0; commit;




