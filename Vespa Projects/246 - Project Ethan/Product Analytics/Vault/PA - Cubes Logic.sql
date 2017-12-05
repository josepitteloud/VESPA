/*

	Project: Ethan Product Analytics
	Author: Angel Donnarumma

	
	What is this? Why is this code important? (BRIEF)
	
	The intention is to aggregate the data we'll be enabling to 
	stakeholders to query, so the data extracts don't cause any
	impacts on the SHARED Tableau Environment related to performance
	or any similar matter.
	
	We are expecting high volume of PA data to be hold for each day (circa 70MM)
	hence deploying a solution which is at its lowest granular level is not
	ideal (again, given the high volume of data queried in a shared environment
	concurrently)
	
	Below Cubes are current PA solutions:
	
	PA CUBE 01 (Context: Volumes on Dimensions)
	PA CUBE 02 (Context: Volumes on Paths)
	PA CUBE 03 (Context: Journey Analysis)

*/

----------------------------------------------
-- PA CUBE 01 (Context: Volumes on Dimensions)
----------------------------------------------

select	dk_date
		,case 	when dk_current = 'N/A' then DK_REFERRER_ID
				else dk_current
		end		as uri
from	pa_events_fact
where	dk_date = 20151004
group	by	dk_Date
			,uri




-----------------------------------------
-- PA CUBE 02 (Context: Volumes on Paths)
-----------------------------------------

-----------------------------------------
-- PA CUBE 03 (Context: Journey Analysis)
-----------------------------------------



