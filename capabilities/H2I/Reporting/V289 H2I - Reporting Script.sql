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
**Project Name:                                                 Skyview H2I
**Analysts:                             Angel Donnarumma        (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jason Thompson          (Jason.Thompson@skyiq.co.uk)
										,Hoi Yu Tang            (HoiYu.Tang@skyiq.co.uk)
										,Jose Pitteloud         (jose.pitteloud@skyiq.co.uk)
**Stakeholder:                          SkyIQ
                                        ,Jose Loureda           (Jose.Loureda@skyiq.co.uk)
**Due Date:                             2015-02-27
**Project Code (Insight Collation):     v289
**Sharepoint Folder:    

        http://sp-department.bskyb.com/sites/SIGEvolved/Shared%20Documents/Forms/AllItems.aspx?RootFolder=%2Fsites%2FSIGEvolved%2FShared%20Documents%2F01%20Analysis%20Requests%2FV289%20-%20Skyview%20Futures%2F01%20Plans%20Briefs%20and%20Project%20Admin                                                        
                                                                        
**Business Brief:

        This script is just to generate reports out from the H2I project

**Sections:
	
	S01 - Gross level for Total Hours Watched from both Sources at household level
	S02 - Total Hours Watched from both sources at individual level
        
--------------------------------------------------------------------------------------------------------------
*/


----------------------------------------------------------------------------------
-- S01 - Gross level for Total Hours Watched from both Sources at household level
----------------------------------------------------------------------------------

select  'H2I'	as source
		,thedate
        ,count(distinct account_number)             as sample
        ,sum(theweight)                             as sow
        ,sum(tsw)                                   as tsw_tot
        ,sum(tsws)                                  as tsws_tot
        ,avg(tsw)                                   as tsw_avg
        ,cast(tsws_tot as float)/cast(sow as float) as tsws_avg
        ,(tsw_avg/3600.00)                          as thw_avg
        ,(tsws_avg/3600.00)                         as thws_avg
from    (
            select  date(m07.event_Start_utc)                                       as thedate
                    ,m07.account_number
                    ,sum(coalesce(m07.chunk_duration_seg,m07.event_duration_seg))   as tsw
                    ,max(w.scaling_weighting)                                       as theweight
                    ,tsw*theweight                                                  as tsws
            from    v289_m07_dp_data    as m07
                    inner join V289_M11_04_VESPA_INDIVIDUAL_WEIGHTING w
                    on m07.account_number = w.account_number
                    inner join  (
                                    select  distinct
                                            account_number
                                            ,hh_person_number
                                    from    V289_M08_SKY_HH_composition
                                    where   person_head = '1'
                                )   as comp
                    on w.account_number = comp.account_number
                    and w.hh_person_number = comp.hh_person_number
            group   by  thedate
                        ,m07.account_number
        )   as base
group   by  thedate
union	all
select  'BARB'	as source
		,thedate
        ,count(distinct household_number)           as sample
        ,sum(hhweight)                              as sow
        ,sum(tmw)                                   as tmw_tot
        ,sum(tmws)                                  as tmws_tot
        ,avg(tmw)                                   as tmw_avg
        ,cast(tmws_tot as float)/cast(sow as float) as tmws_avg
        ,(tmw_avg/60.00)                            as thw_avg
        ,(tmws_avg/60.00)                           as thws_avg
from    (
            select  thedate
                    ,household_number
                    ,hhweight
                    ,sum(s_dur)     as tmw
                    ,tmw*hhweight   as tmws
            from    (
                        select  '2015-02-06'					as thedate
                                ,base.house_id					as household_number
                                ,v.session_id
                                ,sum(progwatch_duration)   		as s_dur
                                ,max(weights.processing_weight)	as hhweight
						FROM 	skybarb                         as base
								inner join  barb_weights        as weights
								on  base.house_id   = weights.household_number
								and base.person     = weights.person_number
								left join   skybarb_fullview    as v
								on  base.house_id   = v.household_number
								and base.person     = v.person_number
								and date(v.start_time_of_Session) = '2015-02-06'
						where	base.head = 1
                        group   by  thedate
                                    ,household_number
                                    ,session_id
                    )   as base1
            group   by  thedate
                        ,household_number
                        ,hhweight
        )   as base
group   by  thedate


------------------------------------------------------------------
-- S02 - Total Hours Watched from both sources at individual level
------------------------------------------------------------------


/*checking at barb side first*/


select  'BARB'	as source
		,thedate
        ,count(distinct household_number||'-'||person_number)           as sample
        ,sum(hhweight)                              as sow
        ,sum(tmw)                                   as tmw_tot
        ,sum(tmws)                                  as tmws_tot
        ,avg(tmw)                                   as tmw_avg
        ,cast(tmws_tot as float)/cast(sow as float) as tmws_avg
        ,(tmw_avg/60.00)                            as thw_avg
        ,(tmws_avg/60.00)                           as thws_avg
from    (
            select  thedate
                    ,household_number
                    ,person_number
                    ,hhweight
                    ,sum(s_dur)     as tmw
                    ,tmw*hhweight   as tmws
            from    (
                        select  date(start_time_of_session) as thedate
                                ,household_number
                                ,person_number
                                ,session_id
                                ,max(duration_of_session)   as s_dur
                                ,max(hh_weight)             as hhweight
                                ,sum(progwatch_duration)  	as duration_min
                        from    skybarb_fullview
                        where   date(start_time_of_session) = '2015-02-06'
                        group   by  thedate
                                    ,household_number
                                    ,person_number
                                    ,session_id
                    )   as base1
            group   by  thedate
                        ,household_number
                        ,person_number
                        ,hhweight
        )   as base
group   by  thedate