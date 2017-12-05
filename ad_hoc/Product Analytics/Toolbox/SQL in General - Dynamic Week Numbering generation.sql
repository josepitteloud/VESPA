
/*
	A practical example to:
	
	Create buckets of N days relative to any starting date...
	
	The query shows the mechanics of how this works...
	
*/

with	base as	(
					select	thedate
					from	(
								select date('2017-03-09 00:00:00') as thedate union
								select date('2017-03-10 00:00:00') union
								select date('2017-03-11 00:00:00') union
								select date('2017-03-12 00:00:00') union
								select date('2017-03-13 00:00:00') union
								select date('2017-03-14 00:00:00') union
								select date('2017-03-15 00:00:00') union
								select date('2017-03-16 00:00:00') union
								select date('2017-03-17 00:00:00') union
								select date('2017-03-18 00:00:00') union
								select date('2017-03-19 00:00:00') union
								select date('2017-03-20 00:00:00')
							)	as base
				)
		,ref as	(
					select	min(thedate)	as the_start
					from	base
				)
select	thedate
		,(extract(epoch from thedate - the_start)) as x
		,(x/7)+1 						as y
		,(cast(x as float) / 7.00)+1 	as y1
		,(x/3)+1						as z
		,(cast(x as float) / 3.00)+1 	as z1
		,(x/10)+1						as w
		,(cast(x as float) / 10.00)+1 	as w1
from	base
		inner join ref
		on	1=1
		
		
		
ceil(datediff(timestamp(thedate),timestamp('2016-10-14'))/N)

integer(15+ceil((datediff(timestamp(thedate),timestamp('2016-10-14'))+1)/7)) as Sky_week


