/*


                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ 
             ?$$$,      I$$$ $$$$. $$$$  $$$= 
              $$$$$$$$= I$$$$$$$    $$$$.$$$  
                  :$$$$~I$$$ $$$$    $$$$$$   
               ,.   $$$+I$$$  $$$$    $$$$=   
              $$$$$$$$$ I$$$   $$$$   .$$$    
                                      $$$     
                                     $$$      
                                    $$$?

            CUSTOMER INTELLIGENCE SERVICES

--------------------------------------------------------------------------------------------------------------
**Project Name:							PRODUCTS ANALYTICS (PA)
**Analysts:                             Angel Donnarumma        (angel.donnarumma@sky.uk)
**Lead(s):                              Angel Donnarumma        (angel.donnarumma@sky.uk)
**Stakeholder:                          Product Team (David Bourdillon)
**Due Date:                             
**Project Code (Insight Collation):     N/A
**Sharepoint Folder:

**Business Brief:

        To Understand in what proportion the remotes provided for the Q box are used (Blue-tooth and Infrared)

**Sections:

	01 - Sampling
	02 - Building the metrics
	03 - Story Telling (slice and dices for outcome)
	04 - Deployment on full population
	05 - Implementing Data Structures to automate for reporting
			
**Running Time:

		None
				
--------------------------------------------------------------------------------------------------------------

*/


----------------
-- 01 - Sampling
----------------

/*

	I want to start with an initial sample of just 5 STBs for a week, whatever metrics I produce from here should be easily roll-out-able to the full population.
	
	Simply playing around with few data for fast time response.
	
*/

select	dk_serial_number
		,cast(count(distinct dk_Date) as float) / 7.00 as RQ
		,random() as x
into	z_rt_sample_group
from	(
			select	dk_Date
					,dk_serial_number
					,count(distinct remote_type) as n_remotes
			from	pa_events_Fact
			where	dk_date between 20160601 and 20160607
			and		length(remote_type) >0
			group	by	dk_Date
						,dk_serial_number
		)	as base
where	n_remotes > 1
group	by	dk_Serial_number
order	by	x desc
limit	5;

commit;

----------------------------
-- 02 - Building the metrics
----------------------------


select	a.dk_date
		,a.dk_serial_number
		,case	when substr(a.dk_serial_number,3,1) = 'B' then 'Sky Q Silver'
				when substr(a.dk_serial_number,3,1) = 'C' then 'Sky Q Box'
				when substr(a.dk_serial_number,3,1) = 'D' then 'Sky Q Mini'
		end		as stb_type
		,a.remote_type
		,count(1)												as hits
		,sum(hits) over ( 
							partition by	a.dk_Date
											,a.dk_serial_number
						)										as overall_hits
		,cast(hits as float) / cast(overall_hits as float)		as usage
from	pa_events_fact as a
		inner join z_rt_sample_group as b
		on	a.dk_serial_number	= b.dk_serial_number
where	a.dk_date between 20160601 and 20160607
and		length(a.remote_type) >0
group	by	a.dk_date
			,a.dk_serial_number
			,stb_type
			,a.remote_type

---------------------------------------------------
-- 03 - Story Telling (slice and dices for outcome)
---------------------------------------------------

-- > Done in Tableau From above Query

-------------------------------------
-- 04 - Deployment on full population
-------------------------------------

select	b.day_Date	as date_
		,a.dk_serial_number
		,case	when substr(a.dk_serial_number,3,1) = 'B' then 'Sky Q Silver'
				when substr(a.dk_serial_number,3,1) = 'C' then 'Sky Q Box'
				when substr(a.dk_serial_number,3,1) = 'D' then 'Sky Q Mini'
		end		as stb_type
		,a.remote_type
		,count(1)													as hits
		,sum(hits) over ( 
							partition by	b.day_Date
											,a.dk_serial_number
						)											as overall_hits
		,cast(hits as float) / cast(overall_hits as float)			as usage
from	pa_events_fact as a
		inner join pa_date_dim	as b
		on	a.dk_Date	= b.date_pk
where	a.dk_date >= 20160601
and		length(a.remote_type) >0
group	by	date_
			,a.dk_serial_number
			,stb_type
			,a.remote_type

--------------------------------------------------------------
-- 05 - Implementing Data Structures to automate for reporting
--------------------------------------------------------------


