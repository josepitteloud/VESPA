select	date_
		,case	when substr(dk_serial_number,3,1) = 'B' then 'Sky Q Silver'
				when substr(dk_serial_number,3,1) = 'C' then 'Sky Q Box'
				when substr(dk_serial_number,3,1) = 'D' then 'Sky Q Mini'
		end		as the_stb_type
		,count(distinct dk_serial_number)	as reach
		,count(1)							as uptake
from	z_pa_events_fact
where	dk_trigger_id in ('userInput-KeyEvent:Key_HomeKeyPressed','userInput-KeyEvent:Key_HomeKeyReleased')
group	by	date_
			,the_stb_type

/*			
	on any given day what is the distribution of STBs entering home N number of times... 
	In other words, normally how many times in any given day STBs go into home via the home button (causing a Top Picks impression due default focus.
*/

select	home_button
		,the_stb_type
		,count(distinct dk_serial_number)	as nboxes
from	(
			select	date_
					,case	when substr(dk_serial_number,3,1) = 'B' then 'Sky Q Silver'
							when substr(dk_serial_number,3,1) = 'C' then 'Sky Q Box'
							when substr(dk_serial_number,3,1) = 'D' then 'Sky Q Mini'
					end		as the_stb_type
					,dk_serial_number
					,count(1)			as home_button
			from	z_pa_events_fact
			where	dk_trigger_id in ('userInput-KeyEvent:Key_HomeKeyPressed','userInput-KeyEvent:Key_HomeKeyReleased')
			group	by	date_
						,the_stb_type
						,dk_serial_number
		)	as base
group	by	home_button
			,the_stb_type