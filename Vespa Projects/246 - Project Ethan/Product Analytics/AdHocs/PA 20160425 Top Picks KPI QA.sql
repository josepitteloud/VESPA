/*

A + No. active GW boxes during March:
B + No. boxes that navigated to Top Picks at least once
C + No. boxes that performed at least one action in Top Picks (download or playback):

D + Total no. journeys to Top Picks:
E + Total no. converted journeys to Top Picks:

F + Avg no. of journeys to Top Picks per box that visited at least once
G + Avg no. of converted journeys to Top Picks per box that converted at least once

*/


/*--------------------------------------
   A + No. active GW boxes during March:
*/--------------------------------------

/*
	David has got 629 vs. 629
*/

select	count(distinct dk_serial_number) as nboxes
from	z_checking -- z_checking_march
where	dk_Date between 20160301 and 20160331
and		box_type in ('Sky Q Silver','Sky Q Box')

--OR

select	count(distinct dk_serial_number) as nboxes
from	z_checking -- z_checking_march
where	dk_Date <=20160331
and		box_type in ('Sky Q Silver','Sky Q Box')


/*--------------------------------------------------------
   B + No. boxes that navigated to Top Picks at least once
*/--------------------------------------------------------

/*
	David hast got 466 vs 466
*/

select	count(distinct dk_serial_number) as nboxes
from	z_checking -- z_checking_march
where	dk_Date between 20160301 and 20160331
and		box_type in ('Sky Q Silver','Sky Q Box')
and		gn_session = 'Top Picks'


/*--------------------------------------------------------------------------------------
   C + No. boxes that performed at least one action in Top Picks (download or playback):
*/--------------------------------------------------------------------------------------


/*
	Checking this up 356 vs calculated by David 356
*/

select	count(distinct dk_serial_number) as nboxes
from	z_checking -- z_checking_march
where	dk_Date between 20160301 and 20160331
and		box_type in ('Sky Q Silver','Sky Q Box')
and		gn_session = 'Top Picks'
and		dk_action_id in (03000,02400)



/*-------------------------------------
   D + Total no. journeys to Top Picks:
*/-------------------------------------

/*
	by David 8299 vs 8299
 */
 
 
select	count(distinct dk_date||'-'||dk_serial_number||'-'||gn_session_grain) as nTP_sessions
from	z_checking -- z_checking_march
where	dk_Date between 20160301 and 20160331
and		box_type in ('Sky Q Silver','Sky Q Box')
and		gn_session = 'Top Picks'


/*-----------------------------------------------
   E + Total no. converted journeys to Top Picks:
*/-----------------------------------------------

/*
	by David 1095 vs 1067.
	
	I am assuming that how "conversion" was denominated is by measuring journeys in which there is at least either a download or a playback.
*/


select	count(distinct dk_date||'-'||dk_serial_number||'-'||gn_session_grain) as nTP_sessions
from	z_checking -- z_checking_march
where	dk_Date between 20160301 and 20160331
and		box_type in ('Sky Q Silver','Sky Q Box')
and		gn_session = 'Top Picks'
and		dk_action_id in (03000,02400)



/*------------------------------------------------------------------------
   F + Avg no. of journeys to Top Picks per box that visited at least once
*/------------------------------------------------------------------------

/*
	by David 17.8 vs 17.8
	
	calculated as follow:
	
	D/B
	
*/



/*------------------------------------------------------------------------------------
   G + Avg no. of converted journeys to Top Picks per box that converted at least once
*/------------------------------------------------------------------------------------

/*
	by David 3.1 vs 2.9
	
	calculated as follow:
	
	E/C
*/