
select	stb_type
		,avg(prop)	as the_avg
		,stddev(prop)	as the_sdev
from	(
			select	date_
					,stb_type
					,count(distinct dk_serial_number) as hits_
					,count(distinct (case when theflag = 1 then dk_serial_number else null end)) as hits2
					,cast(hits2 as float) / cast(hits_ as float) as prop
			from	(
						select	date_
								,stb_type
								,dk_serial_number
								,count(1) as hits
								,count(distinct (case when dk_action_id not in (00004,00003,00002) then dk_serial_number else null end)) as checksum
								,hits - checksum as x
								,case when hits <=10 then 1 else 0 end as theflag
						from	z_pa_events_fact
						where	date_ >= '2016-08-01'
						group	by	date_
									,stb_type
									,dk_serial_number
					)	as base
			group	by	date_
						,stb_type
		)	as base2
group	by	stb_type
			
			
			
			
			
			

select	stb_type
		,hits
		,avg(y) as z
		,ntile(10) over(order by hits) as deciles
into	z_checks -- drop table z_checks;commit;

select	stb_type
		,avg(n)
from	(
			select	date_
					,stb_type
					--,hits
					,count(distinct dk_serial_number) as y
					,avg(hits)		as n
					,stddev(hits)	as z
			from	(
						select	date_
								,stb_type
								,dk_serial_number
								,count(1) as hits
								,count(distinct (case when dk_action_id not in (00004,00003,00002) then dk_serial_number else null end)) as checksum
								,hits - checksum as x
								,case when hits <=10 then 1 else 0 end as theflag
						from	z_pa_events_fact
						where	date_ >= '2016-08-01'
						group	by	date_
									,stb_type
									,dk_serial_number
					)	as base
			--where	theflag = 1
			group	by	1,2
		)	as base2
group	by	1,2
--
--group	by	date_COUNT, 
--limit	100
--			
--			
select	stb_type
		,avg(theratio) 					as avg_of_ratio
		,stddev(theratio)				as stddev_of_ratio
		,avg_of_ratio + stddev_of_ratio	as upper_limit
		,avg_of_ratio - stddev_of_ratio	as lower_limit
from	(
			select	date_
					,stb_type
					,count(distinct dk_serial_number) as n_stbs
					,count(1) as n_hits
					,cast(n_hits as float) / cast(n_stbs as float) as theratio
			from	z_pa_events_fact
			where	date_ >= '2016-08-01'
			group	by	date_
						,stb_type
		)	as base
group	by	stb_type





select * from z_checks limit 10


select	stb_type	
		,deciles
		,min(hits)	as x
		,max(hits)	as y
		,sum(z)		as n
from	z_checks
group	by	stb_type	
			,deciles
order	by	stb_type	
			,deciles