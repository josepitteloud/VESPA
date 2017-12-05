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
**Project Name:							OPS 2.0
**Analysts:                             Angel Donnarumma (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jason Thompson
**Stakeholder:                          SkyIQ
**Due Date:                             20/06/2014
**Project Code (Insight Collation):     
**Sharepoint Folder:                    
                                                                        
**Business Brief:

	In here we are slicing the data we have from barb to get a sense of what we have and build assumptions of outputs,
	compare barb base against Vespa and see how two samples differ...

**Sections:
	S0		- Simplifications
	S1 		- Defining the base table for checking at Barb sample demographic
	S1.1 	- Defining the base table for checking at DP sample demographic
	S2 		- from base, Slicing size of households
	S3 		- from base, Slicing for barb's age distribution
	S3.1	- from base, Slicing for barb's age distribution with Weights applied
	S4 		- from base, slicing for barb's adults only hh
	S5 		- from base, slicing for barb's hh size distribution of adults only hh
	S6 		- from base, slicing for barb's hh size distribution of adults with children hh
	S7		- from dp, slicing for size of households
	S8		- from dp, slicing for DP's age distribution
	S8.1	- from dp, Slicing for DP's age distribution with Weights applied
	S9		- from dp, slicing for DP's adults only hh
	S10		- from dp, slicing for DP's hh size distribution of adults only hh
	S11		- from dp, slicing for DP's hh size distribution of adults with children hh
	S12		- from base, slicing for barb's age and gender distribution
	S13 	- from dp, slicing for DP's age and gender distribution
	S16		- heath-map for hours watched by age/gender(weighted barb sample for panellists with Sky)
	S16.1 	- heath-map for hours watched by hhsize/session size (weighted barb sample for panellists with Sky)
	S17		- heath-map for hours watched (weighted DP sample)
	S18		- Comparison by ageband DP vs Barb
	S19		- Comparison by hhsize DP vs Barb
--------------------------------------------------------------------------------------------------------------
*/

-----------------------
-- S0 - Simplifications
-----------------------

/*
	This section is just to create a simple name easy to remember and use...
*/

-- barb_weights
if object_id('barb_weights') is not null
    drop view barb_weights
	
commit

create view barb_weights as
select  *
from    thompsonja.BARB_Panel_Member_Responses_Weights_and_Viewing_Categories
where   file_creation_date = (select max(file_Creation_date) from thompsonja.BARB_Panel_Member_Responses_Weights_and_Viewing_Categories)
and		reporting_panel_code = 50

commit
grant select on barb_weights to vespa_group_low_security
commit

-- barb_rawview
if object_id('barb_rawview') is not null
	drop view barb_rawview
	
commit

create view barb_rawview as
select	*
from	thompsonja.BARB_PVF06_Viewing_Record_Panel_Members

commit
grant select on barb_rawview to vespa_group_low_security
commit

-- Barb_skytvs
if object_id('Barb_skytvs') is not null
	drop view Barb_skytvs
	
commit

create view Barb_skytvs as
select	*
from    thompsonja.BARB_Panel_Demographic_Data_TV_Sets_Characteristics 
where 	file_creation_date = (select max(file_creation_date) from thompsonja.BARB_Panel_Demographic_Data_TV_Sets_Characteristics)

commit
grant select on Barb_skytvs to vespa_group_low_security
commit


-----------------------------------------------------------------------
-- S1 - Defining the base table for checking at Barb sample demographic
-----------------------------------------------------------------------

/*
	Extracting the sample of panellists from Barb with Sky as the base for any analysis for the project
	at this stage we are only interested on the household demographic (hh size, sex and age of people withing the hH)
*/

if object_id('skybarb') is not null
	drop table skybarb
	
commit

select  members.*
into    skybarb
from    (
			-- defining the household demographic as needed for the most recent file loaded into barb schema...
            select  household_number					as house_id
                    ,person_number						as person
                    ,2014-datepart(year,date_of_birth) 	as age
                    ,case   when sex_code = 1 then 'Male'
                            when sex_code = 2 then 'Female'
                            else 'Uknown'
                    end     as sex
					,case   when household_status in (4,2)  then 1
                            else 0
                    end     as head
            from    thompsonja.BARB_PVF04_Individual_Member_Details
            where   date_valid_for_db1 = (select max(date_valid_for_db1) from thompsonja.BARB_PVF04_Individual_Member_Details)
        )   as members
        inner join  (
						-- this join here is to fix our sample to only those barb panellists with Sky providers...
                        select  distinct 
								household_number
                        from    Barb_skytvs
						where   reception_capability_code1 = 2
                    )   as skytvs
        on  members.house_id    = skytvs.household_number

commit
create hg index hg1	on skybarb(house_id)
create lf index lf1	on skybarb(person)
commit
grant select on skybarb to vespa_group_low_security
commit


/*
	Now constructing a table to be able to check minutes watched across all households based on Barb (weighted to show UK):
	Channel pack, household size, programme genre and the part of the day where these actions happened (breakfast, lunch, etc...)
*/

if object_id('skybarb_fullview') is not null
    drop table skybarb_fullview

commit

select  mega.*
        ,z.sex
        ,case   when z.age between 5 and 11		then '2) 5-11'  
                when z.age between 12 and 17 	then '3) 12-17'
                when z.age between 18 and 19 	then '4) 18-19'
                when z.age between 20 and 24 	then '5) 20-24'
                when z.age between 25 and 34 	then '6) 25-34'
                when z.age between 35 and 44 	then '7) 35-44'
                when z.age between 45 and 64 	then '8) 45-64'
                when z.age > 65              	then '9) 65+'  
        end     as ageband
into    skybarb_fullview
from    (
            select  sch.genre_description as programme_genre
                    ,ska.channel_pack
                    ,ska.service_key
                    ,sch.broadcast_start_date_time_local
                    ,sch.broadcast_end_date_time_local
            		,barbskyhhsize.thesize	as hhsize
                    ,base.*
            		,case when  base.session_start_date_time >= sch.broadcast_start_date_time_local then base.session_start_date_time else sch.broadcast_start_date_time_local end as x
                    ,case when  broadcast_end_date_time_local <= base.session_end_date_time then broadcast_end_date_time_local else base.session_end_date_time end as y
                    ,datediff(minute,x,y)	as progwatch_duration
            		,progwatch_duration * base.processing_weight as progscaled_duration
            from    (
            			-- multiple aggregations to derive part of the day where the viewing session took place
            			-- and a workaround to get the minutes watched per each person in the household multiplied
            			-- by their relevant weights to show the minutes watched by UK (as per barb scaling exercise)...
                        select  a.household_number
                                ,a.start_time_of_session
                                ,a.end_time_of_session
                                ,a.duration_of_session
            					,a.db1_station_code
                                ,case when start_time_of_recording is null then start_time_of_session else start_time_of_recording end as session_start_date_time
                                ,case when start_time_of_recording is null then end_time_of_session else dateadd(mi, Duration_of_session -1 , start_time_of_recording) end as session_end_date_time -- -1 because of minute attribution
                                ,case   when cast(session_start_date_time as time) between '00:00:00.000' and '05:59:00.000' then 'night'
                                        when cast(session_start_date_time as time) between '06:00:00.000' and '08:59:00.000' then 'breakfast'
                                        when cast(session_start_date_time as time) between '09:00:00.000' and '11:59:00.000' then 'morning'
                                        when cast(session_start_date_time as time) between '12:00:00.000' and '14:59:00.000' then 'lunch'
                                        when cast(session_start_date_time as time) between '15:00:00.000' and '17:59:00.000' then 'early prime'
                                        when cast(session_start_date_time as time) between '18:00:00.000' and '20:59:00.000' then 'prime'
                                        when cast(session_start_date_time as time) between '21:00:00.000' and '23:59:00.000' then 'late night'
                                end     as session_daypart
                                ,b.person_number
                                ,b.processing_weight
                                ,case when a.person_1_viewing   = 1 and person_number = 1   then b.processing_weight*a.duration_of_session else 0 end as person_1
                                ,case when a.person_2_viewing   = 1 and person_number = 2   then b.processing_weight*a.duration_of_session else 0 end as person_2
                                ,case when a.person_3_viewing   = 1 and person_number = 3   then b.processing_weight*a.duration_of_session else 0 end as person_3
                                ,case when a.person_4_viewing   = 1 and person_number = 4   then b.processing_weight*a.duration_of_session else 0 end as person_4
                                ,case when a.person_5_viewing   = 1 and person_number = 5   then b.processing_weight*a.duration_of_session else 0 end as person_5
                                ,case when a.person_6_viewing   = 1 and person_number = 6   then b.processing_weight*a.duration_of_session else 0 end as person_6
                                ,case when a.person_7_viewing   = 1 and person_number = 7   then b.processing_weight*a.duration_of_session else 0 end as person_7
                                ,case when a.person_8_viewing   = 1 and person_number = 8   then b.processing_weight*a.duration_of_session else 0 end as person_8
                                ,case when a.person_9_viewing   = 1 and person_number = 9   then b.processing_weight*a.duration_of_session else 0 end as person_9
                                ,case when a.person_10_viewing  = 1 and person_number = 10  then b.processing_weight*a.duration_of_session else 0 end as person_10
                                ,case when a.person_11_viewing  = 1 and person_number = 11  then b.processing_weight*a.duration_of_session else 0 end as person_11
                                ,case when a.person_12_viewing  = 1 and person_number = 12  then b.processing_weight*a.duration_of_session else 0 end as person_12
                                ,case when a.person_13_viewing  = 1 and person_number = 13  then b.processing_weight*a.duration_of_session else 0 end as person_13
                                ,case when a.person_14_viewing  = 1 and person_number = 14  then b.processing_weight*a.duration_of_session else 0 end as person_14
                                ,case when a.person_15_viewing  = 1 and person_number = 15  then b.processing_weight*a.duration_of_session else 0 end as person_15
                                ,case when a.person_16_viewing  = 1 and person_number = 16  then b.processing_weight*a.duration_of_session else 0 end as person_16
                                ,person_1+person_2+person_3+person_4+person_5+person_6+person_7+person_8+person_9+person_10+person_11+person_12+person_13+person_14+person_15+person_16 as theflag
                        from    barb_rawview            as a
                                inner join barb_weights as b
                                on  a.household_number      = b.household_number
                        --where   a.household_number = 4
                    )   as base
                    inner join	(
            						-- fixing barb sample to only barb panellists with Sky (table from prior step)
            						select  house_id
                                            ,max(person) as thesize
                                    from    skybarb
                                    group   by  house_id
                                )   as barbskyhhsize
            		on	base.household_number	= barbskyhhsize.house_id
            		inner join  (
            						-- mapping the db1 station code to the actual service key to find meta data for service key
            						-- done on the join after this one...
            						select  db1_station_code, service_key
            						from    thompsonja.BARB_Channel_Map
            						where   main_sk = 'Y'
            					)   as map
            		on  base.db1_station_code   = map.db1_station_code
            		inner join  (
            						-- getting metadata for service key
            						select  service_key
            								,channel_genre
                                            ,channel_pack
            						from    vespa_Analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES
            						where   active_channel = 'Y'
            					)   as ska
            		on  map.service_key         = ska.service_key
                    inner join  (
            						-- incorporating all programmes watched throughout the session
                                    select  service_key
                                            ,broadcast_start_date_time_local
                                            ,broadcast_end_date_time_local
                                            ,genre_description
                                    from    sk_prod.VESPA_PROGRAMME_SCHEDULE
                                    where   broadcast_start_date_time_utc >= '2013-01-01 00:00:00.000' 
                                    and     broadcast_start_date_time_utc < '2014-01-01 00:00:00.000'
                                )   as sch
                    on  ska.service_key                 = sch.service_key
                    and sch.broadcast_start_date_time_local   <= base.session_end_date_time
                    and sch.broadcast_end_date_time_local     > base.session_start_date_time
            where   base.theflag > 0
        )   as mega
        inner join  skybarb as z
        on  mega.household_number   = z.house_id
        and mega.person_number      = z.person


commit

create hg index hg1 on skybarb_fullview     (service_key)
create hg index hg2 on skybarb_fullview     (household_number)
create lf index lf1 on skybarb_fullview     (channel_pack)
create lf index lf2 on skybarb_fullview     (programme_genre)
create dttm index dt1 on skybarb_fullview   (start_time_of_session)
create dttm index dt2 on skybarb_fullview   (end_time_of_session)
create dttm index dt3 on skybarb_fullview   (session_start_date_time)
create dttm index dt4 on skybarb_fullview   (session_end_date_time)
commit

grant select on skybarb_fullview to vespa_group_low_security
commit

---------------------------------------------------------------------------
-- S1.1 	- Defining the base table for checking at DP sample demographic
---------------------------------------------------------------------------
/*
if object_id('skydp') is not null
	drop table skydp
	
commit

select  exp.exp_cb_key_household as thehouse
        ,case   when exp.p_gender = '0' then 'Male'
                else 'Female'
        end     as sex
        ,exp.p_actual_age   as theage
        ,exp.h_number_of_children_in_household_2011
into    skydp
from    sk_prod.experian_consumerview as exp
        inner join  (
                        select  distinct
                                sav.account_number
                                ,sav.cb_key_household
                        from    (
                                    select  account_number
                                    from    sig_single_account_view
                                    where   status_vespa = 'Enabled'
                                    and     Panel in ('VESPA','VESPA11')
                                )   as ssav
                                inner join  sk_prod.cust_single_account_view    as sav
                                on  ssav.account_number = sav.account_number
                    )   as base
        on  exp.cb_key_household    = base.cb_key_household

commit
create hg index hg1 on skydp(thehouse)
commit
grant select on skydp to vespa_group_low_security
commit
*/
-- below logic could replace above...

-- Sampling for demographics of Vespa daily panel

/*
	Note: 	this step is using a table outside this script which is pitteloudj.PIV_VESPA_HH_view
			that script is located at: 
			\Git\Vespa\ad_hoc\V289 Skyview Futures\scripct matrix 1.sql
*/

if object_id('dp_demo') is not null
    drop table dp_demo

commit

select  vh.acccount_number      as account_number
        ,vh.cb_key_household    as household
        ,vh.cb_address_line_1   as theaddress
        ,case   when ex.p_gender = '0' then 'Male'
                when ex.p_gender = '1' then 'Female'
                else 'U'
        end     as sex
        ,case   when ex.p_actual_age <=19              then '18-19'
                when ex.p_actual_age between 20 and 24 then '20-24'
                when ex.p_actual_age between 25 and 34 then '25-34'
                when ex.p_actual_age between 35 and 44 then '34-44'
                when ex.p_actual_age between 45 and 64 then '45-64'
                when ex.p_actual_age >=65              then '65+'
        end     as ageband
        ,ex.cb_key_db_person    as theperson
into    dp_demo
FROM    pitteloudj.PIV_VESPA_HH_view                AS vh
        inner JOIN sk_prod.EXPERIAN_CONSUMERVIEW    as ex 
        ON  ex.cb_key_household = vh.cb_key_household 
        AND ex.cb_address_line_1 = vh.cb_address_line_1

commit
create hg index hg1 on dp_demo(account_number)
create hg index hg2 on dp_demo(household)
create hg index hg3 on dp_demo(theaddress)

commit
grant select on dp_demo to vespa_group_low_security
commit

-- sampling from the viewing tables the same period of time we have available for Barb

if object_id('dp_viewing') is not null
	drop table dp_viewing
	
commit
	
select  service_key
		,cast(event_start_date_time_utc as date)    as thedate
		,broadcast_time_of_day                      as session_daypart
		,genre_description                          as programe_genre
		,cb_key_household
		,account_number
		,case   when capping_end_date_time_local is not null then capping_end_date_time_local 
				else instance_end_date_time_utc 
		end     as theend
		,instance_start_date_time_utc as thestart
		,case   when datediff(minute,instance_start_date_time_utc,theend) <0 then 0 
				else datediff(minute,instance_start_date_time_utc,theend) end   as mmwatched
into	dp_viewing
from    sk_prod.vespa_dp_prog_viewed_201309 as viewing
where   dk_event_start_datehour_dim between 2013091800 and 2013091823

commit
create hg index hg1 	on dp_viewing(service_key)
create hg index hg2 	on dp_viewing(cb_key_household)
create date index dt1	on dp_viewing(thedate)

commit
grant select on dp_viewing to vespa_group_low_security
commit

-- Sampling from the scaling table the weights calculated for all accounts within the period
-- under analysis...

if object_id('dp_scaling') is not null
	drop table dp_scaling
	
commit
	
select	adjusted_event_start_date_vespa	as thedate
		,account_number
		,calculated_scaling_weight
into	dp_scaling
from	sk_prod.VIQ_VIEWING_DATA_SCALING
where	adjusted_event_start_date_vespa = '2013-09-18' 

commit
create hg index hg1 	on dp_scaling(account_number)
create date index dt1	on dp_scaling(thedate)

commit
grant select on dp_scaling to vespa_group_low_security
commit

---------------------------------------------
-- S2 - from base, Slicing size of households
---------------------------------------------

if object_id('v289_barbhhsizedistribution') is not null
	drop view v289_barbhhsizedistribution
	
commit
	
create view v289_barbhhsizedistribution as
select  hhsize
		,count(1)               as hhbarbsample
        ,sum(hhsize_scaled)/10  as ukbaserep
        ,sum(case   when skyflag = 1 then 1 else 0 end)                 as hhbarbonsky_sample
        ,sum(case   when skyflag = 1 then hhsize_scaled else 0 end)/10  as ukbaseonskyrep
from    (
            select  panel.house_id
                    ,case   when onsky.house_id is not null then 1 else 0 end as skyflag
                    ,count(1) as hhsize
                    ,sum(case when panel.head = 1 then weight.processing_weight else 0 end) as hhsize_scaled
            from    (
            			-- defining the household demographic as needed for the most recent file loaded into barb schema...
                        select  household_number					as house_id
                                ,person_number						as person
                                ,2014-datepart(year,date_of_birth) 	as age
                                ,case   when sex_code = 1 then 'Male'
                                        when sex_code = 2 then 'Female'
                                        else 'Uknown'
                                end     as sex
                                ,case   when household_status in (4,2) then 1
                                        else 0
                                end     as head
                        from    thompsonja.BARB_PVF04_Individual_Member_Details
                        where   date_valid_for_db1 = (select max(date_valid_for_db1) from thompsonja.BARB_PVF04_Individual_Member_Details)
                    )   as panel
                    inner join barb_weights as weight
                    on  panel.house_id  = weight.household_number
                    and panel.person    = weight.person_number
                    left join   (
                                    select  distinct
                                            house_id
                                    from    skybarb
                                )   as onsky
                    on  onsky.house_id    = panel.house_id
            group   by  panel.house_id
                        ,skyflag
        )   as base
group   by  hhsize

/*
select  hhsize
        ,count(1)   as hits
from    (
            select house_id
                   ,count(1) as hhsize
            from   skybarb
            group  by  house_id
        )   as base
group   by  hhsize
*/

commit
grant select on v289_barbhhsizedistribution to vespa_group_low_security
commit

------------------------------------------------------
-- S3 - from base, Slicing for barb's age distribution
------------------------------------------------------

if object_id('v289_view_barbagedistribution') is not null
	drop view v289_view_barbagedistribution
	
commit

create view v289_view_barbagedistribution
as
select  case    when age between 1 and 17	then '01-17'
                when age between 18 and 19 	then '18-19'
                when age between 20 and 24 	then '20-24'
                when age between 25 and 34 	then '25-34'
                when age between 35 and 44 	then '35-44'
                when age between 45 and 64 	then '45-64'
                when age > 65              	then '65+'  
        end     as ageband
        ,count(1) as hits
from    skybarb
group   by  ageband

commit
grant select on v289_view_barbagedistribution to vespa_group_low_security
commit


-----------------------------------------------------------------------------
-- S3.1 - from base, Slicing for barb's age distribution with Weights applied
-----------------------------------------------------------------------------

if object_id('v289_barb_ageweighted_distribution') is not null
	drop view v289_barb_ageweighted_distribution
	
commit

create	view v289_barb_ageweighted_distribution as
select  case    when age between 1 and 17   then '01-17'
                when age between 18 and 19  then '18-19'
                when age between 20 and 24  then '20-24'
                when age between 25 and 34  then '25-34'
                when age between 35 and 44  then '35-44'
                when age between 45 and 64  then '45-64'
                when age > 65               then '65+'
        end     as ageband
        ,sex
        ,count(1)   as hits
        ,sum(weight.processing_weight)/10  as sow
from    skybarb               as panel
        inner join barb_weights as weight
        on  panel.house_id  = weight.household_number
        and panel.person    = weight.person_number
group   by  ageband
            ,sex
			
commit
grant select on v289_barb_ageweighted_distribution to vespa_group_low_security
commit

----------------------------------------------------
-- S4 - from base, slicing for barb's adults only hh
----------------------------------------------------

if object_id('v289_adulthhonly') is not null
	drop view v289_adulthhonly
	
commit
	
create view v289_adulthhonly
as
select  sum(case    when theminage >=16 then 1 else 0 end) as adultsonly_ge16
        ,sum(case   when theminage >=20 then 1 else 0 end) as adultsonly_ge20
        ,count(1)   as total_hh
        ,cast(adultsonly_ge16 as float)/cast(total_hh as float) as p_hhge16
        ,cast(adultsonly_ge20 as float)/cast(total_hh as float) as p_hhge20
from    (
            select  house_id
                    ,min(age)   as theminage
            from    skybarb
            group   by  house_id
        )   as base

commit
grant select on v289_adulthhonly to vespa_group_low_security
commit

----------------------------------------------------------------------------
-- S5 - from base, slicing for barb's hh size distribution of adults only hh
----------------------------------------------------------------------------

if object_id('v289_adulthhonly_agedistribution') is not null
	drop view v289_adulthhonly_agedistribution
	
commit

create view v289_adulthhonly_agedistribution
as
select  hhsize
        ,count(1)                                       as total_hits
        ,sum(case when theminage >15 then 1 else 0 end) as adultsonly_gt15
        ,sum(case when theminage >19 then 1 else 0 end) as adultsonly_gt19
        ,cast(adultsonly_gt15 as float)/cast(total_hits as float)   as p_gt15
        ,cast(adultsonly_gt19 as float)/cast(total_hits as float)   as p_gt19
from    (
            select  house_id
                    ,max(person)    as hhsize
                    ,min(age)       as theminage
            from    skybarb
            group   by  house_id
        )   as base
group   by  hhsize

commit
grant select on v289_adulthhonly_agedistribution to vespa_group_low_security
commit

-------------------------------------------------------------------------------------
-- S6 - from base, slicing for barb's hh size distribution of adults with children hh
-------------------------------------------------------------------------------------

if object_id('v289_adulthhwithchildren_agedistribution') is not null
	drop view v289_adulthhwithchildren_agedistribution
	
commit

create view v289_adulthhwithchildren_agedistribution
as
select  hhsize
        ,count(1)                                                   as total_hits
        ,sum(case when theminage <=15 then 1 else 0 end)            as adultswithchildren
        ,cast(adultswithchildren as float)/cast(total_hits as float)   as prob
from    (
            select  house_id
                    ,max(person)    as hhsize
                    ,min(age)       as theminage
            from    skybarb
            group   by  house_id
        )   as base
group   by  hhsize

commit
grant select on v289_adulthhwithchildren_agedistribution to vespa_group_low_security
commit

-----------------------------------------------
-- S7 - from dp, slicing for size of households
-----------------------------------------------

if object_id('v289_vespa_hhsize_distribution') is not null
	drop view v289_vespa_hhsize_distribution
	
commit

create view v289_vespa_hhsize_distribution
as
select  thesize
        ,count(1)       as hits
        ,cast((sum(weight)) as integer)as ukbasescaled
from    (
            select  panel.account_number
                    ,panel.household
                    ,panel.theaddress
                    ,count(1)+coalesce(max(panel.children_count),0) as thesize
                    ,min(scaling.calculated_scaling_weight)         as weight
            from    dp_demo                 as panel
                    inner join dp_scaling   as scaling
                    on  panel.account_number    = scaling.account_number
            group   by  panel.account_number
                        ,panel.household
                        ,panel.theaddress
        )   as sly1
group   by  thesize
/*
select  thesize
        ,count(1) as hits
from    (
            select  thehouse
                    ,hits + maxkids as thesize
            from    (
                        select  thehouse
                                ,count(1) as hits
                                ,coalesce(min(nkids),0) as minkids
                                ,coalesce(max(nkids),0) as maxkids
                        from    skydp   
                        group   by  thehouse
                    )   as base
        )   as slicer
group   by  thesize
*/

commit
grant select on v289_vespa_hhsize_distribution to vespa_group_low_security
commit

--------------------------------------------------
-- S8 - from dp, slicing for DP's age distribution
--------------------------------------------------

if object_id('v289_vespa_agedistribution') is not null
	drop view v289_vespa_agedistribution
	
commit

create view v289_vespa_agedistribution
as
select  '01-17'             as ageband
        ,sum(children) as hits
from    (
            select  account_number
                    ,household
                    ,theaddress
                    ,max(children_count) as children
            from    dp_demo
            group   by  account_number
                        ,household
                        ,theaddress
        )   as base
group   by  ageband
union
select  ageband
        ,count(1) as hits
from    dp_demo
group   by  ageband
/*
select  case    when theage between 16 and 20  then '16-20'
                when theage between 21 and 25  then '21-25'
                when theage between 26 and 30  then '26-30'
                when theage between 31 and 35  then '31-35'
                when theage between 36 and 40  then '36-40'
                when theage between 41 and 45  then '41-45'
                when theage between 46 and 50  then '46-50'
                when theage between 51 and 55  then '51-55'
                when theage between 56 and 60  then '56-60'
                when theage between 61 and 65  then '61-65'
                when theage > 65               then '65+'
        end     as ageband
        ,count(1) as hits
from    skydp
group   by  ageband
union   
select  '1-15'
        ,sum(children)
from    (
            select  thehouse
                    ,min(nkids) children
            from    skydp
            group   by  thehouse
        )   as base
*/
commit
grant select on v289_vespa_agedistribution to vespa_group_low_security
commit

-------------------------------------------------------------------------
-- S8.1	- from dp, Slicing for DP's age distribution with Weights applied
-------------------------------------------------------------------------

/*
	this can't be done because we do not calculate weights at individual level
	but at household level hence we would not be able to do the breakdown by
	age nor gender...
*/

------------------------------------------------
-- S9 - from dp, slicing for DP's adults only hh
------------------------------------------------

if object_id('v289_vespa_adultsonly_HH_distribution') is not null
	drop view v289_vespa_adultsonly_HH_distribution
	
commit

create view v289_vespa_adultsonly_HH_distribution
as
select  count(1)                                            as totalhh
        ,sum(case when children_flag = 0 then 1 else 0 end) as hhadultsonly
        ,cast(hhadultsonly as float)/cast(totalhh as float) as proportion
from    (
            select  distinct
                    account_number
                    ,household
                    ,theaddress
                    ,case when children_count > 0 then 1 else 0 end as children_flag
            from    dp_demo
        )   as base
/*
select  count(distinct thehouse) as thetotal
        ,sum(case when theminkids=0 then 1 else 0 end)                  as adulstonly_ge16
        ,sum(case when theminkids=0 and theminage>20 then 1 else 0 end) as adulstonly_ge20
        ,cast(adulstonly_ge16 as float)/cast(thetotal as float)         as p_ge16
        ,cast(adulstonly_ge20 as float)/cast(thetotal as float)         as p_ge20
from    (
            select  thehouse
                    ,min(theage)    as theminage
                    ,min(nkids)     as theminkids
            from    skydp
            group   by  thehouse
        )   as base
*/
commit
grant select on v289_vespa_adultsonly_HH_distribution to vespa_group_low_security
commit

-------------------------------------------------------------------------
-- S10 - from dp, slicing for DP's hh size distribution of adults only hh
-------------------------------------------------------------------------

if object_id('v289_vespa_adultsonly_hhsize_distribution') is not null
	drop view v289_vespa_adultsonly_hhsize_distribution
	
commit

create view v289_vespa_adultsonly_hhsize_distribution
as
select  thesize
        ,count(1) as hits
        ,sum(case when children_flag = 0 then 1 else 0 end) as hhadultsonly
        ,cast(hhadultsonly as float)/cast(hits as float)    as proportion
from    (
            select  account_number
                    ,household
                    ,theaddress
                    ,case when children_count > 0 then 1 else 0 end as children_flag
                    ,count(1)+coalesce(max(children_count),0) as thesize
            from    dp_demo
            group   by  account_number
                        ,household
                        ,theaddress
                        ,children_flag
        )   as base
group   by  thesize 
/*
select  thesize
        ,count(1)   as thetotal
        ,sum(case when theminkids=0 then 1 else 0 end)                      as hh_ge16
        ,sum(case when theminkids=0 and theminage>=20 then 1 else 0 end)    as hh_ge20
from    (
            select  thehouse
                    ,min(theage)    as theminage
                    ,min(nkids)     as theminkids
                    ,count(1)       as thesize
            from    skydp
            group   by  thehouse
        )   as base
group   by  thesize
*/
commit
grant select on v289_vespa_adultsonly_hhsize_distribution to vespa_group_low_security
commit

----------------------------------------------------------------------------------
-- S11 - from dp, slicing for DP's hh size distribution of adults with children hh
----------------------------------------------------------------------------------

if object_id('v289_vespa_adultswithchildren_hhsize_distribution') is not null
	drop view v289_vespa_adultswithchildren_hhsize_distribution
	
commit

create view v289_vespa_adultswithchildren_hhsize_distribution
as
select  thesize
        ,count(1) as hits
        ,sum(children_flag)                                         as hhadultswithchildren
        ,cast(hhadultswithchildren as float)/cast(hits as float)    as proportion
from    (
            select  account_number
                    ,household
                    ,theaddress
                    ,case when children_count > 0 then 1 else 0 end as children_flag
                    ,count(1)+coalesce(max(children_count),0) as thesize
            from    dp_demo
            group   by  account_number
                        ,household
                        ,theaddress
                        ,children_flag
        )   as base
group   by  thesize  
/*
select  thesize
        ,count(1)   as thetotal
        ,sum(case when theminkids>1 then 1 else 0 end)                      as hh_ge16
        ,sum(case when theminkids>1 and theminage>=20 then 1 else 0 end)    as hh_ge20
        ,cast(hh_ge16 as float)/cast(thetotal as float)                     as p_ge16
        ,cast(hh_ge20 as float)/cast(thetotal as float)                     as p_ge20
from    (
            select  thehouse
                    ,min(theage)    as theminage
                    ,min(nkids)     as theminkids
                    ,count(1)       as thesize
            from    skydp
            group   by  thehouse
        )   as base
group   by  thesize
*/
commit
grant select on v289_vespa_adultswithchildren_hhsize_distribution to vespa_group_low_security
commit

------------------------------------------------------------------
-- S12 - from base, slicing for barb's age and gender distribution
------------------------------------------------------------------

if object_id('v289_barb_agegender_distribution') is not null
	drop view v289_barb_agegender_distribution
	
commit

create view v289_barb_agegender_distribution
as
select  case    when age between 5 and 11   then '5-11'
                when age between 12 and 17  then '12-17'
                when age between 18 and 19  then '18-19'
                when age between 20 and 24  then '20-24'
                when age between 25 and 34  then '25-34'
                when age between 35 and 44  then '35-44'
                when age between 45 and 64  then '45-64'
                when age > 65               then '65+'
        end     as ageband
        ,sex
        ,count(1)   as hits
from    skybarb
group   by  ageband
            ,sex

commit
grant select on v289_barb_agegender_distribution to vespa_group_low_security
commit

--------------------------------------------------------------
-- S13 - from dp, slicing for DP's age and gender distribution
--------------------------------------------------------------

if object_id('v289_vespa_agegender_distribution') is not null
	drop view v289_vespa_agegender_distribution
	
commit

create view v289_vespa_agegender_distribution
as
select  ageband
        ,sex
        ,count(1) as hits
from    dp_demo
group   by  ageband
            ,sex
/*
select  case    when theage between 16 and 20  then '16-20'
                when theage between 21 and 25  then '21-25'
                when theage between 26 and 30  then '26-30'
                when theage between 31 and 35  then '31-35'
                when theage between 36 and 40  then '36-40'
                when theage between 41 and 45  then '41-45'
                when theage between 46 and 50  then '46-50'
                when theage between 51 and 55  then '51-55'
                when theage between 56 and 60  then '56-60'
                when theage between 61 and 65  then '61-65'
                when theage > 65               then '65+'
        end     as ageband
        ,sex
        ,count(1) as hits
from    skydp
group   by  ageband
            ,sex
*/
commit
grant select on v289_vespa_agegender_distribution to vespa_group_low_security
commit

----------------------------------------------------------------------------------	
-- S16	- heath-map for hours watch (weighted barb sample for panellists with Sky)
----------------------------------------------------------------------------------

if object_id('v289_heathmap_hourswatch_barbskyweighted') is not null
	drop view v289_heathmap_hourswatch_barbskyweighted
	
commit

create view v289_heathmap_hourswatch_barbskyweighted
as
select  cast(start_time_of_session as date) as thedate
        ,session_daypart
        ,hhsize
        ,channel_pack
        ,programme_genre
        ,household_number
        ,person_number
        ,sex
        ,ageband
        ,cast((sum(distinct progscaled_duration)/60.0) as integer)  as uk_hhwatched
from    skybarb_fullview
group   by  thedate
            ,session_daypart
            ,hhsize
            ,channel_pack
            ,programme_genre
            ,household_number
            ,person_number
            ,sex
            ,ageband

commit
grant select on v289_heathmap_hourswatch_barbskyweighted to vespa_group_low_security
commit


------------------------------------------------------------------------------------------------------------
-- S16.1 - heath-map for hours watched by hhsize/session size (weighted barb sample for panellists with Sky)
------------------------------------------------------------------------------------------------------------

if object_id('v289_heatmap_sessionsize_barbskyweighted') is not null
	drop view v289_heatmap_sessionsize_barbskyweighted
	
commit

create	view v289_heatmap_sessionsize_barbskyweighted as
select  cast(start_time_of_session as date) as thedate
        ,session_daypart
        ,hhsize
        ,channel_pack
        ,programme_genre
        ,session_id
        ,count(distinct person_number)  as session_size
        ,cast((sum(distinct progscaled_duration)/60.0) as integer)  as uk_hhwatched
from    (
            select  *
                    ,rank() over    (
                                        partition by    household_number
                                        order by        start_time_of_session
                                    )   as session_id
            from    skybarb_fullview
        )   as skybarb
group   by  thedate
            ,session_daypart
            ,hhsize
            ,channel_pack
            ,programme_genre
            ,session_id

commit
grant select on v289_heatmap_sessionsize_barbskyweighted to vespa_group_low_security
commit


---------------------------------------------------------
-- S17 - heath-map for hours watched (weighted DP sample)
---------------------------------------------------------

/*
	NOTE: 	As expected, currently we are not able to drill down into age/gender in the DP 
			(this is actually the goal of the project)
			
			Hence what available at the moment is hours watched by UK (as per DP representation)
			at household level...
*/

if object_id('v289_heathmap_hourswatch_dpweighted') is not null
	drop view v289_heathmap_hourswatch_dpweighted
	
commit

create	view v289_heathmap_hourswatch_dpweighted as
select  viewing.thedate
        ,viewing.session_daypart
        ,ska.channel_pack
        ,viewing.programe_genre
        ,viewing.cb_key_household
        ,viewing.account_number
        ,round((sum(viewing.mmwatched*scaling.calculated_scaling_weight)/60.0),2)    as hhwatched
from    dp_viewing	as viewing
        inner join  (
						-- getting metadata for service key
						select  service_key
                                ,channel_pack
						from    vespa_Analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES
						where   active_channel = 'Y'
					)   as ska
        on  viewing.service_key = ska.service_key
        inner join dp_scaling as scaling
        on  viewing.account_number  = scaling.account_number
        and viewing.thedate         = scaling.thedate
group   by  viewing.thedate
            ,viewing.session_daypart
            ,ska.channel_pack
            ,viewing.programe_genre
            ,viewing.cb_key_household
            ,viewing.account_number
			
commit

grant select on v289_heathmap_hourswatch_dpweighted to vespa_group_low_security
commit


-----------------------------------------
-- S18 - Comparison by ageband DP vs Barb
-----------------------------------------

-- ageband comparison unweighted... done at proportion for fair measure

/* NOTE: I had to create a table for this as excel did not handle this kind of queries */

if object_id('v289_comp_ageband_dpvsbarb') is not null
	drop table v289_comp_ageband_dpvsbarb
	
commit

select  dp.ageband
        ,cast(dp.hits as decimal(10,2))/cast((select sum(hits) from v289_vespa_agedistribution) as decimal(10,2))       as dphits
        ,cast(barb.hits as decimal(10,2))/cast((select sum(hits) from v289_view_barbagedistribution) as decimal(10,2))  as barbhits
        ,dphits - barbhits  as thediff
into    v289_comp_ageband_dpvsbarb
from    v289_vespa_agedistribution                  as dp
        inner join v289_view_barbagedistribution    as barb
        on  dp.ageband  = barb.ageband
		
commit
grant select on v289_comp_ageband_dpvsbarb to vespa_group_low_security
commit


----------------------------------------
-- S19 - Comparison by hhsize DP vs Barb
----------------------------------------

/* NOTE: I had to create a table for this as excel did not handle this kind of queries */

if object_id ('v289_comp_hhsize_dpvsbarb') is not null
    drop table v289_comp_hhsize_dpvsbarb

commit

select  dp.thesize  as hhsize

        ,cast(dp.hits as decimal(10,2)) / cast((select sum(hits) from v289_vespa_hhsize_distribution) as decimal(10,2))                 as DP
        ,cast(dp.ukbasescaled as decimal(10,2)) / cast((select sum(ukbasescaled) from v289_vespa_hhsize_distribution) as decimal(10,2)) as DP_scaled

        ,cast(barb.hhbarbsample as decimal(10,2)) / cast((select sum(hhbarbsample) from v289_barbhhsizedistribution) as decimal(10,2))  as BARB
        ,cast(barb.ukbaserep as decimal(10,2)) / cast((select sum(ukbaserep) from v289_barbhhsizedistribution) as decimal(10,2))        as BARB_scaled

        ,cast(barb.hhbarbonsky_sample as decimal(10,2)) / cast((select sum(hhbarbonsky_sample) from v289_barbhhsizedistribution) as decimal(10,2))  as BARBSky
        ,cast(barb.ukbaseonskyrep as decimal(10,2)) / cast((select sum(ukbaseonskyrep) from v289_barbhhsizedistribution) as decimal(10,2))          as BARBSky_scaled

        ,dp.ukbasescaled        as dp_scaled_vol
        ,barb.ukbaserep         as ba_scaled_vol
        ,barb.ukbaseonskyrep    as bs_scaled_vol

        ,DP - BARB                  as DP_B
        ,DP_scaled - BARB_scaled    as DPs_Bs
        ,DP_scaled - BARBSky_scaled as DPs_vs_BSkys
        
        ,dp_scaled_vol - ba_scaled_vol  as dps_bas_vol
        ,dp_scaled_vol - bs_scaled_vol  as dps_bss_vol

into    v289_comp_hhsize_dpvsbarb
from    v289_vespa_hhsize_distribution          as dp
        inner join v289_barbhhsizedistribution  as barb
        on  dp.thesize   = barb.hhsize
        
commit
grant select on v289_comp_hhsize_dpvsbarb to vespa_group_low_security
commit




