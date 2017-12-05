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
**Project Name:							RSMB Support
**Analysts:                             Angel Donnarumma (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jose Loureda
**Stakeholder:                          RSMB
**Due Date:                             06/12/2013
**Project Code (Insight Collation):     
**Sharepoint Folder:                    
                                                                        
**Business Brief:
	
	Extracting from Netezza the scaling segments to be in line with the weights used in Olive on VIQ table
	(These weights coming from CBI)
	
--------------------------------------------------------------------------------------------------------------
*/

/*
----------------------------------------------------------
-- SYNTAX TO EXPORT THE QUERY INTO A CSV FORMAT LOCALLY...
----------------------------------------------------------

-- to be placed after the query we want to extract...

OUTPUT TO 'G:\RTCI\Sky Projects\Vespa\Measurements and Algorithms\RSMB Audit\Phase II\<FILE NAME>.csv' FORMAT ASCII DELIMITED BY ',' QUOTE ''

*/


select  stage0.account_number
        ,stage0.scaling_enablement_dt
        ,calendar.theyw
        ,case   when cast(left(calendar.theyw,4) as integer) = 2011 then ((52*2) - cast(right(calendar.theyw,2) as integer))  + 9
                when cast(left(calendar.theyw,4) as integer) = 2012 then (52 - cast(right(calendar.theyw,2) as integer))  + 9
                else (9 - cast(right(calendar.theyw,2)as integer)) 
        end     + 1 as week_frequency
from    (
            select  account_number
                    ,min(reporting_starts)  as scaling_enablement_dt
            from    vespa_analysts.sc2_intervals
            where   reporting_starts < '2013-03-01'
            group   by  account_number
        )   as stage0
        inner join  (
                        select  distinct
                                utc_day_date    as thedate
                                ,(case  when thedate = '2012-12-31' then cast(datepart(year,thedate)as integer)+1 else datepart(year,thedate) end) || (case when utc_week_in_year < 10 then ('0' || cast(utc_week_in_year as varchar(1))) else cast(utc_week_in_year as varchar(2)) end) as theyw
                        from    sk_prod.VESPA_CALENDAR 
                        where   utc_day_date between '2011-01-01' and '2013-03-01'
                    )   as calendar
        on  stage0.scaling_enablement_dt = calendar.thedate

		

-------------------------
-- PANEL COMPOSITION BASE
-------------------------

-- SAMPLE FOR THURSDAY... (z_5_thu)
		
declare @basedate 	date

set @basedate 	= '2013-10-10'


while @basedate <= '2013-12-26' --<LIMIT>
begin
	
	insert  into z_5_thu
    select  thursday
            ,control_cell
            ,count(distinct sky) as Sky_base
            ,count(distinct vespa)    as vespa_panel
            ,sum(responsive)                   as responsive_rate
    from    (
                -- Weird Patch, removing duplicates from below joins...
                select  distinct
                        scaling.thursday
                        ,scaling.control_cell
                        ,scaling.account_number     as sky
                        ,base.account_number        as vespa
                        ,base.responsive            
                from    (
                            -- Querying for the distribution of Sky Base on the Scaling Segments...
                            select  profiling_date-1    as thursday
                			        ,scaling_segment_id as control_cell
                					,account_number
                			from    vespa_analysts.SC2_Sky_base_segment_snapshots           
                			where   profiling_date = @basedate+1
                		)   as scaling
                        left join   (
                                        -- so out of the DP enabled on each Thursday confirming from VIQ who got scaled...
                                        select  sbv.weekending-2 as thursday
                                                ,sbv.account_number
                                                ,case when viq.account_number is not null then 1 else 0 end as responsive
                                        from    vespa_analysts.vespa_sbv_hist_qualitycheck  as sbv
                                                left join sk_prod.VIQ_VIEWING_DATA_SCALING as viq
                                                on  sbv.account_number  = viq.account_number
                                                and sbv.weekending-2    = viq.adjusted_event_start_date_vespa
                                        where   sbv.weekending = @basedate+2
                                        and     sbv.panel_id = 12
                                    )   as base
                        on  scaling.account_number  = base.account_number
                        and scaling.thursday        = base.thursday
            )   as patch
    group   by  thursday
                ,control_cell
									
	set @basedate = @basedate+7

	commit
end	

-- reconciling the scaling segment categories between NETEZZA and OLIVE...
select  thursday.thursday
        ,thursday.control_cell
        ,thursday.sky_base
        ,thursday.vespa_panel
        ,thursday.responsive_rate
        ,case   when lookup.universe = 'A) Single box HH'   then 'Single Box Household Universe'
                when lookup.universe = 'B) Multiple box HH' then 'Multi Box Household Universe'
                else null   
        end     as universe_
        ,case   when lookup.isba_tv_region in ('HTV Wales','HTV West')  then 'Wales and West'
                when lookup.isba_tv_region = 'Meridian (exc. Chann'     then 'Southern'
                when lookup.isba_tv_region = 'North West'               then 'Lancashire'
                when lookup.isba_tv_region = 'Ulster'                   then 'Northern Ireland'
                when lookup.isba_tv_region = 'North Scotland'           then 'Northern Scotland'
                when lookup.isba_tv_region = 'Non-scalable'             then null
                else lookup.isba_tv_region
        end     as tv_region
        ,case   when lookup.tenure = 'A) 0-2 Years'     then '0-2 Years'
                when lookup.tenure = 'B) 3-10 Years'    then '2-10 Years'
                when lookup.tenure = 'C) 10 Years+'     then '10+ Years'
                when lookup.tenure = 'D) Unknown'       then 'Unknown'
                else null
        end     as tenure_
        ,case   when lookup.package = 'Basic - Ent'         then 'Basic'
                when lookup.package = 'Basic - Ent Extra'   then 'Basic Entertainment'
                when lookup.package = 'Dual Movies'         then 'Dual Movies (0 Sports - 2 Movies)'
                when lookup.package = 'Dual Sports'         then 'Dual Sports (2 Sports - 0 Movies)'
                when lookup.package = 'Other Premiums'      then 'Other Premium (1 & 1, 2 & 1, 1 & 2)'
                when lookup.package = 'Single Movies'       then 'Single Movies (0 Sports - 1 Movies)'
                when lookup.package = 'Single Sports'       then 'Single sports (1 Sports - 0 Movies)'
                when lookup.package = 'Top Tier'            then 'Top Tier (4 Premiums)'
                else null
        end     as package_
        ,case   when lookup.boxtype = 'A) HDx & No_secondary_box'       then 'HDx & No_secondary_box'
                when lookup.boxtype = 'B) HD & No_secondary_box'        then 'HD & No_secondary_box'
                when lookup.boxtype = 'C) Skyplus & No_secondary_box'   then 'Skyplus & No_secondary_box'
                when lookup.boxtype = 'D) FDB & No_secondary_box'       then 'FDB & No_secondary_box'
                when lookup.boxtype = 'E) HD & HD'                      then 'HD & HD'
                when lookup.boxtype = 'F) HD & Skyplus'                 then 'HD & Skyplus'
                when lookup.boxtype = 'G) HD & FDB'                     then 'HD & FDB'
                when lookup.boxtype = 'H) HDx & HDx'                    then 'HDx & HDx'
                when lookup.boxtype = 'I) HDx & Skyplus'                then 'HDx & Skyplus'
                when lookup.boxtype = 'J) HDx & FDB'                    then 'HDx & FDB'
                when lookup.boxtype = 'K) Skyplus & Skyplus'            then 'Skyplus & Skyplus'
                when lookup.boxtype = 'L) Skyplus & FDB'                then 'Skyplus & FDB'
                when lookup.boxtype = 'M) FDB & FDB'                    then 'FDB & FDB'
                else null
        end     as box_type
        ,case   when lookup.hhcomposition = '00'   then 'Families'
                when lookup.hhcomposition = '01'   then 'Extended family'
                when lookup.hhcomposition = '02'   then 'Extended household'
                when lookup.hhcomposition = '03'   then 'Pseudo family'
                when lookup.hhcomposition = '04'   then 'Single male'
                when lookup.hhcomposition = '05'   then 'Single female'
                when lookup.hhcomposition = '06'   then 'Male homesharers'
                when lookup.hhcomposition = '07'   then 'Female homesharers'
                when lookup.hhcomposition = '08'   then 'Mixed homesharers'
                when lookup.hhcomposition = '09'   then 'Abbreviated male families'
                when lookup.hhcomposition = '10'   then 'Abbreviated female families'
                when lookup.hhcomposition = '11'   then 'Multi-occupancy dwelling'
                when lookup.hhcomposition = 'U'    then 'Unclassified'
                else null
        end     as hh_composition
into    z_5_thu_final
from    z_5_thu                                             as thursday
        inner join vespa_analysts.SC2_Segments_Lookup_v2_1  as lookup
        on  thursday.control_cell = lookup.scaling_segment_id
commit
			
commit
drop table z_5_thu
commit
			
-- SAMPLE FOR SATURDAY... (z_5_sat)

declare @basedate 	date
declare @auxdate 	date

set @basedate 	= '2013-10-12'


while @basedate <= '2013-12-28' --<LIMIT>
begin

	set @auxdate	= @basedate-1
	
	insert  into z_5_satu
    select  saturday
            ,control_cell
            ,count(distinct sky) as Sky_base
            ,count(distinct vespa)    as vespa_panel
            ,sum(responsive)                   as responsive_rate
    from    (
                -- Weird Patch, removing duplicates from below joins...
                select  distinct
                        scaling.saturday
                        ,scaling.control_cell
                        ,scaling.account_number     as sky
                        ,base.account_number        as vespa
                        ,base.responsive            
                from    (
                            -- Querying for the distribution of Sky Base on the Scaling Segments...
                            select  profiling_date+1    as saturday
                			        ,scaling_segment_id as control_cell
                					,account_number
                			from    vespa_analysts.SC2_Sky_base_segment_snapshots           
                			where   profiling_date = @basedate-1
                		)   as scaling
                        left join   (
                                        -- so out of the DP enabled on each Saturday confirming from VIQ who got scaled...
                                        select  sbv.weekending as saturday
                                                ,sbv.account_number
                                                ,case when viq.account_number is not null then 1 else 0 end as responsive
                                        from    vespa_analysts.vespa_sbv_hist_qualitycheck  as sbv
                                                left join sk_prod.VIQ_VIEWING_DATA_SCALING as viq
                                                on  sbv.account_number  = viq.account_number
                                                and sbv.weekending    	= viq.adjusted_event_start_date_vespa
                                        where   sbv.weekending = @basedate
                                        and     sbv.panel_id = 12
                                    )   as base
                        on  scaling.account_number  = base.account_number
                        and scaling.saturday        = base.saturday
            )   as patch
    group   by  saturday
                ,control_cell
									
	set @basedate = @basedate+7

	commit
end	

-- reconciling the scaling segment categories between NETEZZA and OLIVE...
select  saturday.saturday
        ,saturday.control_cell
        ,saturday.sky_base
        ,saturday.vespa_panel
        ,saturday.responsive_rate
        ,case   when lookup.universe = 'A) Single box HH'   then 'Single Box Household Universe'
                when lookup.universe = 'B) Multiple box HH' then 'Multi Box Household Universe'
                else null   
        end     as universe_
        ,case   when lookup.isba_tv_region in ('HTV Wales','HTV West')  then 'Wales and West'
                when lookup.isba_tv_region = 'Meridian (exc. Chann'     then 'Southern'
                when lookup.isba_tv_region = 'North West'               then 'Lancashire'
                when lookup.isba_tv_region = 'Ulster'                   then 'Northern Ireland'
                when lookup.isba_tv_region = 'North Scotland'           then 'Northern Scotland'
                when lookup.isba_tv_region = 'Non-scalable'             then null
                else lookup.isba_tv_region
        end     as tv_region
        ,case   when lookup.tenure = 'A) 0-2 Years'     then '0-2 Years'
                when lookup.tenure = 'B) 3-10 Years'    then '2-10 Years'
                when lookup.tenure = 'C) 10 Years+'     then '10+ Years'
                when lookup.tenure = 'D) Unknown'       then 'Unknown'
                else null
        end     as tenure_
        ,case   when lookup.package = 'Basic - Ent'         then 'Basic'
                when lookup.package = 'Basic - Ent Extra'   then 'Basic Entertainment'
                when lookup.package = 'Dual Movies'         then 'Dual Movies (0 Sports - 2 Movies)'
                when lookup.package = 'Dual Sports'         then 'Dual Sports (2 Sports - 0 Movies)'
                when lookup.package = 'Other Premiums'      then 'Other Premium (1 & 1, 2 & 1, 1 & 2)'
                when lookup.package = 'Single Movies'       then 'Single Movies (0 Sports - 1 Movies)'
                when lookup.package = 'Single Sports'       then 'Single sports (1 Sports - 0 Movies)'
                when lookup.package = 'Top Tier'            then 'Top Tier (4 Premiums)'
                else null
        end     as package_
        ,case   when lookup.boxtype = 'A) HDx & No_secondary_box'       then 'HDx & No_secondary_box'
                when lookup.boxtype = 'B) HD & No_secondary_box'        then 'HD & No_secondary_box'
                when lookup.boxtype = 'C) Skyplus & No_secondary_box'   then 'Skyplus & No_secondary_box'
                when lookup.boxtype = 'D) FDB & No_secondary_box'       then 'FDB & No_secondary_box'
                when lookup.boxtype = 'E) HD & HD'                      then 'HD & HD'
                when lookup.boxtype = 'F) HD & Skyplus'                 then 'HD & Skyplus'
                when lookup.boxtype = 'G) HD & FDB'                     then 'HD & FDB'
                when lookup.boxtype = 'H) HDx & HDx'                    then 'HDx & HDx'
                when lookup.boxtype = 'I) HDx & Skyplus'                then 'HDx & Skyplus'
                when lookup.boxtype = 'J) HDx & FDB'                    then 'HDx & FDB'
                when lookup.boxtype = 'K) Skyplus & Skyplus'            then 'Skyplus & Skyplus'
                when lookup.boxtype = 'L) Skyplus & FDB'                then 'Skyplus & FDB'
                when lookup.boxtype = 'M) FDB & FDB'                    then 'FDB & FDB'
                else null
        end     as box_type
        ,case   when lookup.hhcomposition = '00'   then 'Families'
                when lookup.hhcomposition = '01'   then 'Extended family'
                when lookup.hhcomposition = '02'   then 'Extended household'
                when lookup.hhcomposition = '03'   then 'Pseudo family'
                when lookup.hhcomposition = '04'   then 'Single male'
                when lookup.hhcomposition = '05'   then 'Single female'
                when lookup.hhcomposition = '06'   then 'Male homesharers'
                when lookup.hhcomposition = '07'   then 'Female homesharers'
                when lookup.hhcomposition = '08'   then 'Mixed homesharers'
                when lookup.hhcomposition = '09'   then 'Abbreviated male families'
                when lookup.hhcomposition = '10'   then 'Abbreviated female families'
                when lookup.hhcomposition = '11'   then 'Multi-occupancy dwelling'
                when lookup.hhcomposition = 'U'    then 'Unclassified'
                else null
        end     as hh_composition
into    z_5_sat_final
from    z_5_satu                                            as saturday
        inner join vespa_analysts.SC2_Segments_Lookup_v2_1  as lookup
        on  saturday.control_cell = lookup.scaling_segment_id
commit

commit
drop table rsmb2_5_saturday
commit