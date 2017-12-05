----

drop table dbarnett.v250_viewed_201301;
drop table dbarnett.v250_viewed_201302;
drop table dbarnett.v250_viewed_201303;
drop table dbarnett.v250_viewed_201304;
drop table dbarnett.v250_viewed_201305;
drop table dbarnett.v250_viewed_201306;
drop table dbarnett.v250_viewed_201307;
drop table dbarnett.v250_viewed_201308;
drop table dbarnett.v250_viewed_201309;
drop table dbarnett.v250_viewed_201310;



drop table dbarnett.v250_viewed_201304;

select case when genre_description is null then '1:Null' 
when genre_description ='Unknown' then '2:Unknown' 

else '3: Has genre' end as genre_info
,case when dk_programme_instance_dim =-1 then '1:DK =-1' else '2: DK_dim present' end as dk_info
,service_key
, sum(case when vw.capped_partial_flag = 1 then datediff(second, vw.instance_start_date_time_utc, vw.capping_end_date_time_utc)
     else datediff(second, vw.instance_start_date_time_utc, vw.instance_end_date_time_utc)
     end)  as instance_duration_v2
into dbarnett.v250_viewed_201304
from sk_prod.vespa_dp_prog_viewed_201304 as vw
where  
--capping_end_date_time_utc >instance_start_date_time_utc
--and 
panel_id = 12  and capped_full_flag = 0
and instance_start_date_time_utc < instance_end_date_time_utc   
group by genre_info
,dk_info
,service_key
;

commit;

select * from dbarnett.v250_viewed_201304;




select case when programme_instance_name is null then '1:No Name' else '2: Has Name' end as has_name
,service_key
, sum(case when vw.capped_partial_flag = 1 then datediff(second, vw.instance_start_date_time_utc, vw.capping_end_date_time_utc)
     else datediff(second, vw.instance_start_date_time_utc, vw.instance_end_date_time_utc)
     end)  as instance_duration_v2
into dbarnett.v250_viewed_201302
from sk_prod.vespa_dp_prog_viewed_201302 as vw
where  
--capping_end_date_time_utc >instance_start_date_time_utc
--and 
panel_id = 12  and capped_full_flag = 0
and instance_start_date_time_utc < instance_end_date_time_utc   
group by has_name
,service_key
;

--drop table #viewed_201303;
select case when programme_instance_name is null then '1:No Name' else '2: Has Name' end as has_name
,service_key
, sum(case when vw.capped_partial_flag = 1 then datediff(second, vw.instance_start_date_time_utc, vw.capping_end_date_time_utc)
     else datediff(second, vw.instance_start_date_time_utc, vw.instance_end_date_time_utc)
     end)  as instance_duration_v2
into dbarnett.v250_viewed_201303
from sk_prod.vespa_dp_prog_viewed_201303 as vw
where  
--capping_end_date_time_utc >instance_start_date_time_utc
--and 
panel_id = 12  and capped_full_flag = 0
and instance_start_date_time_utc < instance_end_date_time_utc   
group by has_name
,service_key
;

commit;
select * from #viewed_201303

select case when programme_instance_name is null then '1:No Name' else '2: Has Name' end as has_name
,service_key
, sum(case when vw.capped_partial_flag = 1 then datediff(second, vw.instance_start_date_time_utc, vw.capping_end_date_time_utc)
     else datediff(second, vw.instance_start_date_time_utc, vw.instance_end_date_time_utc)
     end)  as instance_duration_v2
into dbarnett.v250_viewed_201304
from sk_prod.vespa_dp_prog_viewed_201304 as vw
where  
--capping_end_date_time_utc >instance_start_date_time_utc
--and 
panel_id = 12  and capped_full_flag = 0
and instance_start_date_time_utc < instance_end_date_time_utc   
group by has_name
,service_key
;




--select * from dbarnett.v250_viewed_201304 where service_key = 4002

commit;
select case when programme_instance_name is null then '1:No Name' else '2: Has Name' end as has_name
,service_key
, sum(case when vw.capped_partial_flag = 1 then datediff(second, vw.instance_start_date_time_utc, vw.capping_end_date_time_utc)
     else datediff(second, vw.instance_start_date_time_utc, vw.instance_end_date_time_utc)
     end)  as instance_duration_v2
into dbarnett.v250_viewed_201305
from sk_prod.vespa_dp_prog_viewed_201305 as vw
where  
--capping_end_date_time_utc >instance_start_date_time_utc
--and 
panel_id = 12  and capped_full_flag = 0
and instance_start_date_time_utc < instance_end_date_time_utc   
group by has_name
,service_key
;
commit;
select case when programme_instance_name is null then '1:No Name' else '2: Has Name' end as has_name
,service_key
, sum(case when vw.capped_partial_flag = 1 then datediff(second, vw.instance_start_date_time_utc, vw.capping_end_date_time_utc)
     else datediff(second, vw.instance_start_date_time_utc, vw.instance_end_date_time_utc)
     end)  as instance_duration_v2
into dbarnett.v250_viewed_201306
from sk_prod.vespa_dp_prog_viewed_201306 as vw
where  
--capping_end_date_time_utc >instance_start_date_time_utc
--and 
panel_id = 12  and capped_full_flag = 0
and instance_start_date_time_utc < instance_end_date_time_utc   
group by has_name
,service_key
;
commit;
/*commit;
select case when programme_instance_name is null then '1:No Name' else '2: Has Name' end as has_name
,service_key
, sum(case when vw.capped_partial_flag = 1 then datediff(second, vw.instance_start_date_time_utc, vw.capping_end_date_time_utc)
     else datediff(second, vw.instance_start_date_time_utc, vw.instance_end_date_time_utc)
     end)  as instance_duration_v2
into dbarnett.v250_viewed_201307
from sk_prod.vespa_dp_prog_viewed_201307 as vw
where  
--capping_end_date_time_utc >instance_start_date_time_utc
--and 
panel_id = 12  and capped_full_flag = 0
and instance_start_date_time_utc < instance_end_date_time_utc   
group by has_name
,service_key
;
*/

select case when programme_instance_name is null then '1:No Name' else '2: Has Name' end as has_name
,service_key
, sum(case when vw.capped_partial_flag = 1 then datediff(second, vw.instance_start_date_time_utc, vw.capping_end_date_time_utc)
     else datediff(second, vw.instance_start_date_time_utc, vw.instance_end_date_time_utc)
     end)  as instance_duration_v2
into dbarnett.v250_viewed_201308
from sk_prod.vespa_dp_prog_viewed_201308 as vw
where  
--capping_end_date_time_utc >instance_start_date_time_utc
--and 
panel_id = 12  and capped_full_flag = 0
and instance_start_date_time_utc < instance_end_date_time_utc   
group by has_name
,service_key
;
commit;
select case when programme_instance_name is null then '1:No Name' else '2: Has Name' end as has_name
,service_key
, sum(case when vw.capped_partial_flag = 1 then datediff(second, vw.instance_start_date_time_utc, vw.capping_end_date_time_utc)
     else datediff(second, vw.instance_start_date_time_utc, vw.instance_end_date_time_utc)
     end)  as instance_duration_v2
into dbarnett.v250_viewed_201309
from sk_prod.vespa_dp_prog_viewed_201309 as vw
where  
--capping_end_date_time_utc >instance_start_date_time_utc
--and 
panel_id = 12  and capped_full_flag = 0
and instance_start_date_time_utc < instance_end_date_time_utc   
group by has_name
,service_key
;
commit;
select case when programme_instance_name is null then '1:No Name' else '2: Has Name' end as has_name
,service_key
, sum(case when vw.capped_partial_flag = 1 then datediff(second, vw.instance_start_date_time_utc, vw.capping_end_date_time_utc)
     else datediff(second, vw.instance_start_date_time_utc, vw.instance_end_date_time_utc)
     end)  as instance_duration_v2
into dbarnett.v250_viewed_201310
from sk_prod.vespa_dp_prog_viewed_201310 as vw
where  
--capping_end_date_time_utc >instance_start_date_time_utc
--and 
panel_id = 12  and capped_full_flag = 0
and instance_start_date_time_utc < instance_end_date_time_utc   
group by has_name
,service_key
;
commit;


select  programme_instance_name,instance_start_date_time_utc,instance_end_date_time_utc,panel_id,capped_full_flag
,case when vw.capped_partial_flag = 1 then datediff(second, vw.instance_start_date_time_utc, vw.capping_end_date_time_utc)
     else datediff(second, vw.instance_start_date_time_utc, vw.instance_end_date_time_utc)
     end from sk_prod.vespa_dp_prog_viewed_201304 as vw where service_key=4002






select case when programme_instance_name is null then '1:No Name' else '2: Has Name' end as has_name
,service_key
, sum(case when vw.capped_partial_flag = 1 then datediff(second, vw.instance_start_date_time_utc, vw.capping_end_date_time_utc)
     else datediff(second, vw.instance_start_date_time_utc, vw.instance_end_date_time_utc)
     end)  as instance_duration_v2
--into dbarnett.v250_viewed_201301
from sk_prod.vespa_dp_prog_viewed_201301 as vw
where  
--capping_end_date_time_utc >instance_start_date_time_utc
--and 
panel_id = 12  and capped_full_flag = 0 and service_key = 4002
and instance_start_date_time_utc < instance_end_date_time_utc   
group by has_name
,service_key
;
commit;




select case when programme_instance_name is null then '1:No Name' else '2: Has Name' end as has_name
,service_key
,case when vw.capped_partial_flag = 1 then datediff(second, vw.instance_start_date_time_utc, vw.capping_end_date_time_utc)
     else datediff(second, vw.instance_start_date_time_utc, vw.instance_end_date_time_utc)
     end  as instance_duration_v2

--into dbarnett.v250_viewed_2013014002
from sk_prod.vespa_dp_prog_viewed_201301 as vw
where  
--capping_end_date_time_utc >instance_start_date_time_utc
--and 
panel_id = 12  and capped_full_flag = 0
and instance_start_date_time_utc < instance_end_date_time_utc   
and service_key = 4002
group by has_name
,service_key
;





select  service_key
,channel_name
,genre_description
,programme_instance_name
,vw.capped_partial_flag
,instance_start_date_time_utc
,vw.instance_end_date_time_utc
,vw.capping_end_date_time_utc
,cast(instance_start_date_time_utc as date) as viewing_date
,case when genre_description = 'Unknown' then 'Unknown' else 'Known' end as has_genre
, case when vw.capped_partial_flag = 1 then datediff(second, vw.instance_start_date_time_utc, vw.capping_end_date_time_utc)
     else datediff(second, vw.instance_start_date_time_utc, vw.instance_end_date_time_utc)
     end  as instance_duration_v2
--into dbarnett.v250_sep_genrev2
from sk_prod.vespa_dp_prog_viewed_201304 as vw
where  
--capping_end_date_time_utc >instance_start_date_time_utc
--and 
panel_id = 12  and capped_full_flag = 0
and instance_start_date_time_utc < instance_end_date_time_utc   
and service_key =4002
and live_recorded='LIVE' 
--and cast(instance_start_date_time_utc as date) between  '2013-09-04' and '2013-09-04'
group by service_key
,channel_name
,genre_description
,programme_instance_name
,viewing_date
,has_genre
;
commit;




select  programme_name ,broadcast_start_date_time_local,broadcast_end_date_time_local,programme_instance_duration,service_key
,genre_description,sub_genre_description

from sk_prod.Vespa_programme_schedule 
where service_key=4002 and cast (broadcast_start_date_time_local as date)  between '2013-03-01' and '2013-03-03' 
order by service_key, broadcast_start_date_time_local



select  programme_name ,broadcast_start_date_time_local,broadcast_end_date_time_local,programme_instance_duration,service_key
,genre_description,sub_genre_description

from sk_prod.Vespa_programme_schedule 
where service_key=4022 and cast (broadcast_start_date_time_local as date)  between '2013-09-01' and '2013-09-30' 
order by service_key, broadcast_start_date_time_local

commit;


select  channel_name,programme_name ,broadcast_start_date_time_local,broadcast_end_date_time_local,programme_instance_duration,service_key
,genre_description,sub_genre_description

from sk_prod.Vespa_programme_schedule 
where service_key=4022 and cast (broadcast_start_date_time_local as date)  between '2013-06-28' and '2013-07-02' 
order by service_key, broadcast_start_date_time_local

commit;