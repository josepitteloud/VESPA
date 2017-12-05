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
**Analysts:                             Angel Donnarumma        (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Angel Donnarumma        (angel.donnarumma_mirabel@skyiq.co.uk)
**Stakeholder:                          Product Team
**Due Date:                             05/02/2016
**Project Code (Insight Collation):     N/A
**Sharepoint Folder:

**Business Brief:

        

**Sections:

		
--------------------------------------------------------------------------------------------------------------

*/

select
		thedate
	,	YEAR
	,	MONTH
	,	programme_id
	,	mcn_source_id
	,	PROVIDER
	,	AUTHOR
	,	TITLE
	,	duration
	,	the_category
	,	the_playlist
	,	sum(watch_duration)	as	n_secs_watched
	,	count(1)			as	freq_playback
from	(
			SELECT
					A.pk_mcn_asset_streaming_session_id
				,	A.dk_serial_number
				,	A.dk_referrer_id												as	x
				,	A.programme_id
				,	A.PROVIDER
				,	A.YEAR
				,	A.MONTH
				,	A.AUTHOR
				,	A.duration
				,	A.TITLE
				,	A.CATEGORY
				,	A.PLAYLIST
				,	A.mcn_source_id
				,	DATE(TO_CHAR(MIN(B.DK_DATE),'99999999'))						AS	THEDATE
				,	MIN(WATCH_DURATION)												AS	WATCH_DURATION
				,	instr(x,'"')+1 													as	y1
				,	instr(substr(x,y1),'"')-1 										as	y2
				,	substr(x,y1,y2) 												as	x11
				,	case
						when substr(x11,1,1) = '/'	then	substr(x11,2)
						else								x11
					end																as	x2
				,	length(x2)-length(translate(x2,'/','')) 						as	splits
				,	instr(x2,'/')+1 												as	yy1
				,	instr(substr(x2,yy1),'/')+1										as	yy2
				,	substr(x2,0,instr(x2,'/'))										as	root_
				,	substr(x2,yy1,yy2-2)											as	split1
				,	substr(x2,yy1+yy2-1)											as	split2
				,	case
						when split1 = ''	then	split2
						else						split1
					end 															as	the_category
				,	case
						when split2 = the_category	then	null
						else								split2
					end																as	the_playlist
			FROM
							ETHAN_PA_PROD..PA_MCN_ASSET_STREAMING_FACT	AS	A
				INNER JOIN	ETHAN_PA_PROD..PA_EVENTS_FACT				AS	B	ON	A.pk_mcn_asset_streaming_session_id	=		B.mcn_asset_streaming_session_id
																				and	B.dk_referrer_id					like	'%' || A.CATEGORY || '%'
			where
					length(A.provider)	<>		27
				and	A.year				>=		2016
				-- and	A.month				>=		10
				-- and	((A.YEAR*1E2) + A.MONTH)	>=	201610
				and	A.provider			not in	('com.bskyb.help','')	-- Filter out these from the help section since they're not from Online Video section (should not matter given correct MCN catalogue)
				and	nvl(instr(translate(lower(A.programme_id),'abcdefghijklmnopqrstuvwxyz','XXXXXXXXXXXXXXXXXXXXXXXXXX'),'X'),0)	=	0	-- Filter out alphanumerics due to Top Picks since they're not from Online Video section (should not matter given correct MCN catalogue)
				and	instr(A.dk_referrer_id,'Online Videos/')	>	0
				and	A.mcn_source_id		<>		''
			GROUP BY
					A.pk_mcn_asset_streaming_session_id
				,	A.dk_serial_number
				,	A.dk_referrer_id
				,	A.programme_id
				,	A.PROVIDER
				,	A.YEAR
				,	A.MONTH
				,	A.AUTHOR
				,	A.duration
				,	A.TITLE
				,	A.CATEGORY
				,	A.PLAYLIST
				,	A.mcn_source_id
		)	as base
group by
		thedate
	,	YEAR
	,	MONTH
	,	programme_id
	,	mcn_source_id
	,	PROVIDER
	,	AUTHOR
	,	TITLE
	,	duration
	,	the_category
	,	the_playlist
