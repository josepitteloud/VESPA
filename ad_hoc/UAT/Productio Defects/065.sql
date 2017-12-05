/*


                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$= ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$  ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$   ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=   ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$    ODD   ZDDDDDDDN
                                      $$$      .      $DDZ
                                     $$$             ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES
					  
--------------------------------------------------------------------------------------------------------------

**Project Name: 					PRODUCTION DEFECT 065
**Analysts:							Angel Donnarumma (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):							Jose Loureda
**Stakeholder:						Vespa Team

									
**Business Brief:

Various fields are coming null in a considerable volume on Olive Prod

 + Traffic Key
 + Genre_description
 + sub_genre
 + Channel_name
 
This is currently checked between OLIVE PROD and NETEZZA UAT because the fixture was implemented already in production
and current snapshot we have on the second environment comes from production (Meaning it also have the fix in place, release 2.6)...

**Sections:
	
	A:	Agreeing a data on both environments for the analysis
	
	
	B: 	Checking consistency between both sources on null volumen
	
		B01 : Is this case happening for the rest of the dates?
		B02 : Why are we now having a higher number of records missing the programme or channel id in them, is this an issue or is it expected?
		
		
	C:	Collecting a Sample on both environment to show discrepancies
--------------------------------------------------------------------------------------------------------------



----------------------------------------------------------------
/* A:	Agreeing a data on both environments for the analysis */
----------------------------------------------------------------

/* NETEZZA UAT */

-- Checking dates with sufficient volumen available in the EXPORT schema...
select	substring(cast(dk_event_start_datehour_dim as varchar(10)),0,9) as thedate
		,count(1)
from	smi_export..VIEWING_PROGRAMME_INSTANCE_FACT
where	dk_event_start_datehour_dim between 2013042700 and 2013050823
group	by	thedate
order	by	thedate desc 

/*
THEDATE		COUNT
20130505	92482
20130504	26,335,629 --> this is the date
20130503	1325263
20130502	63079
20130501	29357
20130430	16707
20130429	10137
20130428	6511
20130427	3678
*/

/* OLIVE PROD */

-- Checking if volumen for the picked date is in line with Olive PROD...
select	count(1)
from		sk_prod.VESPA_DP_PROG_VIEWED_201305
where		dk_event_start_datehour_dim between 2013050400 and 2013050423

/*
Count
25,965,755

-- about 400k missing... yet seems reasonable for this analysis...
*/


--------------------------------------------------------------------
/* B: 	Checking consistency between both sources on null volumen */
--------------------------------------------------------------------

/* OLIVE PROD */


select	count(1) as thetotal
		,(cast(thetotal as float)/25965755) *100 as theproportion
from	sk_prod.VESPA_DP_PROG_VIEWED_201305
where	dk_event_start_datehour_dim between 2013050400 and 2013050423 -- 25965755
and		dk_programme_dim in (null,-1) -- 403270


/* NETEZZA UAT */

select	count(1) as thetotal
		,sum(case when (DK_PROGRAMME_DIM is null or DK_PROGRAMME_DIM < 0) then 1 else 0 end) as hits -- 615401
		,(hits/thetotal)*100 as theproportion
from	smi_export..VIEWING_PROGRAMME_INSTANCE_FACT
where	dk_event_start_datehour_dim between 2013050400 and 2013050423 -- 26335629


/*

04/05/2013 seems very consistent across both sources

Only about 369K records are dropped when importing into OLIVE what sent from NETEZZA of which the 57% of those cases
had a programme id value of null or -1...


2 New questions come up:

1 - Is this case happening for the rest of the dates? 
	AD: 28/04/2013 seems very suspicious though I guess I would have to check it on DW instead of EXPORT
	
2 - Why are we now having a higher number of records missing the programme or channel id in them, is this an issue or is it expected?

*/


/* 
B01 : Is this case happening for the rest of the dates? 
*/ 

-- Checking the same for the 28/04...




