

select case when dk_programme_instance_dim =-1 then '1:No Name' else '2: Has Name' end as has_name
,service_key
, sum(case when vw.capped_partial_flag = 1 then datediff(second, vw.instance_start_date_time_utc, vw.capping_end_date_time_utc)
     else datediff(second, vw.instance_start_date_time_utc, vw.instance_end_date_time_utc)
     end)  as instance_duration_v2

from sk_prod.vespa_dp_prog_viewed_201304 as vw
where  
--capping_end_date_time_utc >instance_start_date_time_utc
--and 
panel_id = 12  and capped_full_flag = 0
and instance_start_date_time_utc < instance_end_date_time_utc   

group by has_name
,service_key
;

---3802


select  programme_name ,broadcast_start_date_time_local,broadcast_end_date_time_local,programme_instance_duration,service_key
,genre_description,sub_genre_description

from sk_prod.Vespa_programme_schedule 
where service_key=3802 and cast (broadcast_start_date_time_local as date)  between '2013-04-01' and '2013-04-30' 
order by programme_instance_duration desc ,service_key, broadcast_start_date_time_local


select case when dk_programme_instance_dim =-1 then '1:No Name' else '2: Has Name' end as has_name
,cast(instance_start_date_time_utc as date) as viewing_date
, sum(case when vw.capped_partial_flag = 1 then datediff(second, vw.instance_start_date_time_utc, vw.capping_end_date_time_utc)
     else datediff(second, vw.instance_start_date_time_utc, vw.instance_end_date_time_utc)
     end)  as instance_duration_v2

from sk_prod.vespa_dp_prog_viewed_201304 as vw
where  
--capping_end_date_time_utc >instance_start_date_time_utc
--and 
panel_id = 12  and capped_full_flag = 0
and instance_start_date_time_utc < instance_end_date_time_utc   
and service_key=3802
group by has_name,viewing_date
order by has_name,viewing_date



select case when dk_programme_instance_dim =-1 then '1:No Name' else '2: Has Name' end as has_name
,programme_instance_name
,genre_description
,dk_programme_instance_dim
,cast(instance_start_date_time_utc as date) as viewing_date
,case when vw.capped_partial_flag = 1 then datediff(second, vw.instance_start_date_time_utc, vw.capping_end_date_time_utc)
     else datediff(second, vw.instance_start_date_time_utc, vw.instance_end_date_time_utc)
     end  as instance_duration_v2
,*
from sk_prod.vespa_dp_prog_viewed_201304 as vw
where  
--capping_end_date_time_utc >instance_start_date_time_utc
--and 
panel_id = 12  and capped_full_flag = 0
and instance_start_date_time_utc < instance_end_date_time_utc   
and service_key=3802
and cast(instance_start_date_time_utc as date)= '2013-04-01'

group by has_name,viewing_date
order by has_name,viewing_date


select programme_instance_duration
,channel_name
,service_key
,broadcast_start_date_time_local
from sk_prod.Vespa_programme_schedule 
where  cast (broadcast_start_date_time_local as date)  between '2012-11-01' and '2013-10-31' 
and programme_instance_duration is not null and programme_instance_duration>=3000
order by programme_instance_duration desc
;
output to 'C:\Users\DAB53\Documents\Project 250 - Sports Rights Evaluation\EPG Issues\epg_long_length.csv' format ascii;
commit;


select service_key
,channel_name
,count(*) as records
from sk_prod.vespa_dp_prog_viewed_201304
--where left(channel_name,10) = 'Sky Sports'
group by service_key
,channel_name

commit;


select programme_instance_duration
,channel_name
,service_key
,broadcast_start_date_time_local
,programme_instance_name
from sk_prod.Vespa_programme_schedule 
where  cast (broadcast_start_date_time_local as date)  between '2013-04-08' and '2013-04-11' 
and service_key=4004
order by broadcast_start_date_time_local
;

select programme_instance_duration
,channel_name
,service_key
,broadcast_start_date_time_local
,programme_instance_name
from sk_prod.Vespa_programme_schedule 
where  cast (broadcast_start_date_time_local as date)  between '2013-09-03' and '2013-09-06' 
and service_key=4004
order by broadcast_start_date_time_local
;





select programme_instance_duration
,channel_name
,service_key
,broadcast_start_date_time_local
,programme_instance_name
from sk_prod.Vespa_programme_schedule 
where  cast (broadcast_start_date_time_local as date)  between '2013-04-08' and '2013-04-11' 
and service_key=1726
order by broadcast_start_date_time_local
;



select programme_instance_duration
,channel_name
,service_key
,broadcast_start_date_time_local
,programme_instance_name
from sk_prod.Vespa_programme_schedule 
where  cast (broadcast_start_date_time_local as date)  between '2013-09-03' and '2013-09-06' 
and service_key=1726
order by broadcast_start_date_time_local
;



select programme_instance_duration
,channel_name
,service_key
,broadcast_start_date_time_local
,programme_instance_name
from sk_prod.Vespa_programme_schedule 
where  cast (broadcast_start_date_time_local as date)  between '2013-06-20' and '2013-06-23' 
and service_key=3835
order by broadcast_start_date_time_local
;



select programme_instance_duration
,channel_name
,service_key
,broadcast_start_date_time_local
,programme_instance_name
from sk_prod.Vespa_programme_schedule 
where  cast (broadcast_start_date_time_local as date)  between '2013-06-20' and '2013-06-23' 
and service_key=1306
order by broadcast_start_date_time_local
;


commit;




select  programme_instance_name ,broadcast_start_date_time_local,broadcast_end_date_time_local,programme_instance_duration,service_key
,genre_description,sub_genre_description

from sk_prod.Vespa_programme_schedule 
where service_key=4040 and cast (broadcast_start_date_time_local as date)  between '2013-03-01' and '2013-03-31' 
order by broadcast_start_date_time_local,programme_instance_duration desc ,service_key, broadcast_start_date_time_local





select programme_instance_name
,genre_description
,broadcast_start_date_time_utc
,sum(case when vw.capped_partial_flag = 1 then datediff(second, vw.instance_start_date_time_utc, vw.capping_end_date_time_utc)
     else datediff(second, vw.instance_start_date_time_utc, vw.instance_end_date_time_utc)
     end)  as instance_duration_v2

from sk_prod.vespa_dp_prog_viewed_201303 as vw
where  
--capping_end_date_time_utc >instance_start_date_time_utc
--and 
panel_id = 12  and capped_full_flag = 0
and instance_start_date_time_utc < instance_end_date_time_utc   
and service_key=4040
and cast(broadcast_start_date_time_utc as date)= '2013-03-13'
and cast(instance_start_date_time_utc as date)= '2013-03-13'

group by programme_instance_name
,genre_description
,broadcast_start_date_time_utc
order by instance_duration_v2 desc
,programme_instance_name
,genre_description
;

commit;


select programme_instance_name
,genre_description
,broadcast_start_date_time_utc
,sum(case when vw.capped_partial_flag = 1 then datediff(second, vw.instance_start_date_time_utc, vw.capping_end_date_time_utc)
     else datediff(second, vw.instance_start_date_time_utc, vw.instance_end_date_time_utc)
     end)  as instance_duration_v2

from sk_prod.vespa_dp_prog_viewed_201303 as vw
where  
--capping_end_date_time_utc >instance_start_date_time_utc
--and 
panel_id = 12  and capped_full_flag = 0
and instance_start_date_time_utc < instance_end_date_time_utc   
and service_key=3141
and cast(broadcast_start_date_time_utc as date)= '2013-03-13'
and cast(instance_start_date_time_utc as date)= '2013-03-13'

group by programme_instance_name
,genre_description
,broadcast_start_date_time_utc
order by instance_duration_v2 desc
,programme_instance_name
,genre_description
;

commit;



select  programme_name ,broadcast_start_date_time_local,broadcast_end_date_time_local,programme_instance_duration,service_key
,genre_description,sub_genre_description

from sk_prod.Vespa_programme_schedule 
where service_key=4004 and cast (broadcast_start_date_time_local as date)  between '2013-10-16' and '2013-10-20' 
order by service_key, broadcast_start_date_time_local


select  programme_name ,broadcast_start_date_time_local,broadcast_end_date_time_local,programme_instance_duration,service_key
,genre_description,sub_genre_description

from sk_prod.Vespa_programme_schedule 
where service_key=4022 and cast (broadcast_start_date_time_local as date)  between '2013-09-01' and '2013-09-30' 
order by service_key, broadcast_start_date_time_local






select programme_instance_name
,genre_description
,broadcast_start_date_time_utc
,sum(case when vw.capped_partial_flag = 1 then datediff(second, vw.instance_start_date_time_utc, vw.capping_end_date_time_utc)
     else datediff(second, vw.instance_start_date_time_utc, vw.instance_end_date_time_utc)
     end)  as instance_duration_v2

from sk_prod.vespa_dp_prog_viewed_201303 as vw
where  
--capping_end_date_time_utc >instance_start_date_time_utc
--and 
panel_id = 12  and capped_full_flag = 0
and instance_start_date_time_utc < instance_end_date_time_utc   
and service_key=4022

group by programme_instance_name
,genre_description
,broadcast_start_date_time_utc
order by instance_duration_v2 desc
,programme_instance_name
,genre_description
;


commit;