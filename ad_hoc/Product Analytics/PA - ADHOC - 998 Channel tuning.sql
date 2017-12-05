select	*
from	(
			select	date(cast(dk_date as varchar(8)))	as the_date
					,case	when substr(dk_serial_number,3,1) = 'B' then 'Sky Q Silver'
							when substr(dk_serial_number,3,1) = 'C' then 'Sky Q Box'
							when substr(dk_serial_number,3,1) = 'D' then 'Sky Q Mini'
					end		as the_stb_type
					,substr(dk_channel_id,3,4) 			as the_channel
					,count(1)							as hits
					,count(distinct dk_serial_number)	as nboxes
			from	pa_events_Fact
			where	dk_date>=20160801 --  we are after service key 1851 which is epg number = 998
			group	by	the_date
						,the_stb_type
						,the_channel
		)	as base
where	the_channel in (1022,1851)



/*
	Slicing View for 2 specific weeks
	14 - 20 (last week) 
	17 - 23 (next week)
*/ 

select	date(cast(dk_date as varchar(8)))	as the_date
		,case	when substr(dk_serial_number,3,1) = 'B' then 'Sky Q Silver'
				when substr(dk_serial_number,3,1) = 'C' then 'Sky Q Box'
				when substr(dk_serial_number,3,1) = 'D' then 'Sky Q Mini'
		end		as the_stb_type
		,substr(dk_channel_id,3,4) 			as the_channel
		,count(1)							as hits
		,count(distinct dk_serial_number)	as nboxes
from	pa_events_Fact
where	dk_date between 20161114 and 20161123
group	by	the_date
			,the_stb_type
			,the_channel