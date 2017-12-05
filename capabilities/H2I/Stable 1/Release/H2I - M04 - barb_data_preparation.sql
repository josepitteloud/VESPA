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
**Due Date:                             11/07/2014
**Project Code (Insight Collation):     v289
**Sharepoint Folder:    

        http://sp-department.bskyb.com/sites/SIGEvolved/Shared%20Documents/Forms/AllItems.aspx?RootFolder=%2Fsites%2FSIGEvolved%2FShared%20Documents%2F01%20Analysis%20Requests%2FV289%20-%20Skyview%20Futures%2F01%20Plans%20Briefs%20and%20Project%20Admin                                                        
                                                                        
**Business Brief:

        This Module is to prepare the extracted BARB data into a more suitable data structure for analysis...

**Module:
        
        M04: Barb Data Preparation
                        M04.0 - Initialising Environment
                        M04.1 - Preparing transient tables
                        M04.2 - Final BARB Data Preparation
                        M04.3 - Returning Results

--------------------------------------------------------------------------------------------------------------
*/


-----------------------------------
-- M04.0 - Initialising Environment
-----------------------------------

create or replace procedure ${SQLFILE_ARG001}.v289_m04_barb_data_preparation
        @processing_date date = null
as begin

        MESSAGE cast(now() as timestamp)||' | Begining M04.0 - Initialising Environment' TO CLIENT
        
        
    declare @a int
        
    select      @a = count(1)
    from        barb_weights
    
        if @a > 0
        begin
        
                MESSAGE cast(now() as timestamp)||' | @ M04.0: Initialising Environment DONE' TO CLIENT
                
-------------------------------------
-- M04.1 - Preparing transient tables
-------------------------------------

                MESSAGE cast(now() as timestamp)||' | Begining M04.1 - Preparing transient tables' TO CLIENT
                
                /*
                        Extracting the sample of panellists from Barb with Sky as the base for any analysis for the project
                        at this stage we are only interested on the household demographic (hh size, sex and age of people withing the hH)
                */


                if  exists(  select tname from syscatalog
                        where creator = ${SQLFILE_ARG001} 
                        and upper(tname) = upper('skybarb')
                        and     tabletype = 'TABLE')
                                        drop table skybarb
                        
                commit
                
                select  demo.household_number                                       as house_id
                                                ,demo.person_number                                         as person
                                                ,datediff(yy,demo.date_of_birth,@processing_date)                       as age
                                                ,case   when demo.sex_code = 1 then 'Male'
                                                                when demo.sex_code = 2 then 'Female'
                                                                else 'Unknown'
                                                end     as sex
                                                ,case   when demo.household_status in (4,2)  then 1
                                                                else 0
                                                end     as head
                                                ,s4     as digital_hh
                                into    skybarb
                                from    BARB_INDV_PANELMEM_DET  as demo
                                                INNER JOIN  (
                                                                                select  whole.household_number
                                                                                                ,min(analogue_terrestrial)      as s1
                                                                                                ,min(digital_terrestrial)       as s2
                                                                                                ,min(analogue_stallite)         as s3
                                                                                                ,min(digital_satellite)         as s4
                                                                                                ,min(analogue_cable)            as s5
                                                                                                ,min(digital_cable)                     as s6
                                                                                from    BARB_PANEL_DEMOGR_TV_CHAR   as whole
                                                                                                inner join  (
                                                                                                                                select  distinct household_number
                                                                                                                                from    BARB_PANEL_DEMOGR_TV_CHAR
                                                                                                                                where   @processing_date between date_valid_from and date_valid_to
                                                                                                                                and     (
                                                                                                                                                        reception_capability_code_1=2
                                                                                                                                                        or reception_capability_code_2=2
                                                                                                                                                        or reception_capability_code_3=2
                                                                                                                                                        or reception_capability_code_4=2
                                                                                                                                                        or reception_capability_code_5=2
                                                                                                                                                        or reception_capability_code_6=2
                                                                                                                                                        or reception_capability_code_7=2
                                                                                                                                                        or reception_capability_code_8=2
                                                                                                                                                        or reception_capability_code_9=2
                                                                                                                                                        or reception_capability_code_10=2
                                                                                                                                                )
                                                                                                                        )   as skycap
                                                                                                on  whole.household_number  = skycap.household_number
                                                                                group   by      whole.household_number
                                                                        )   as barb_sky_panelists                       
                                                ON      demo.household_number   = barb_sky_panelists.household_number
                                                INNER JOIN      barb_weights as b                                       
                                                ON      demo.household_number   = b.household_number 
                                                AND demo.person_number = b.person_number
                                where   @processing_date between demo.date_valid_from and demo.date_valid_to
                                and     demo.person_membership_status = 0

                commit
                
                create hg index hg1     on skybarb(house_id)
                create lf index lf1     on skybarb(person)
                commit
                
                grant select on skybarb to vespa_group_low_security
                commit

                MESSAGE cast(now() as timestamp)||' | @ M04.1: Preparing transient tables DONE' TO CLIENT
                
--------------------------------------
-- M04.2 - Final BARB Data Preparation
--------------------------------------
                
                MESSAGE cast(now() as timestamp)||' | Begining M04.2 - Final BARB Data Preparation' TO CLIENT
                

                /*
                        Now constructing a table to be able to check minutes watched across all households based on Barb (weighted to show UK):
                        Channel pack, household size, programme genre and the part of the day where these actions happened (breakfast, lunch, etc...)
                */
                if  exists(  select tname from syscatalog
                        where creator = ${SQLFILE_ARG001} 
                        and upper(tname) = upper('skybarb_fullview')
                        and     tabletype = 'TABLE')
                        drop table skybarb_fullview

                commit


				-- fixing barb sample to only barb panellists with Sky (table from prior step)
				/*
								a.head is a bit that is only set on (1) for the head of household
								so below multiplication will only bring the head of household weight 
				*/
				select  a.house_id
								,count(distinct a.person) as thesize
								,sum(a.head*b.processing_weight)    as hh_weight
				INTO #barbskyhhsize
				from    skybarb as a
								left join  barb_weights    as b
								on  a.house_id  = b.household_number
								and a.person    = b.person_number
				group   by  a.house_id
				having  hh_weight > 0
				
				
				
				
				
				select  mega.*
                                                ,z.sex
                                                ,case   when z.age between 0 and 11             then '0-11'
                                                                when z.age between 12 and 19    then '12-19'
                                                                when z.age between 20 and 24    then '20-24'
                                                                when z.age between 25 and 34    then '25-34'
                                                                when z.age between 35 and 44    then '35-44'
                                                                when z.age between 45 and 64    then '45-64'
                                                                when z.age >= 65                then '65+'  
                                                end     as ageband
                into    skybarb_fullview
                from    (
                                                        select  #barbskyhhsize.thesize  as hhsize
                                                                        ,#barbskyhhsize.hh_weight
                                                                        ,base.*
                                                        from    (
                                                                                -- multiple aggregations to derive part of the day where the viewing session took place
                                                                                -- and a workaround to get the minutes watched per each person in the household multiplied
                                                                                -- by their relevant weights to show the minutes watched by UK (as per barb scaling exercise)...
                                                                                select  viewing.household_number
                                                                                                ,viewing.pvf_pv2
                                                                                                ,dense_rank() over      (
                                                                                                                                                partition by    cast(viewing.local_start_time_of_session as date)
                                                                                                                                                                                ,viewing.household_number
                                                                                                                                                order by        viewing.set_number
                                                                                                                                                                                ,viewing.local_start_time_of_session
                                                                                                                                        )   as session_id
                                                                                                ,dense_rank() over      (
                                                                                                                                                        partition by    viewing.household_number
                                                                                                                                                        order by        viewing.local_tv_event_start_date_time||'-'||viewing.set_number
                                                                                                                                                )   as event_id
                                                                                                ,set_number
                                                                                                ,viewing.programme_name
                                                                                                ,local_start_time_of_session            as start_time_of_session
                                                                                                ,local_end_time_of_session          as end_time_of_session
                                                                                                ,local_tv_instance_start_date_time      as instance_start
                                                                                                ,local_tv_instance_end_date_time    as instance_end
                                                                                                ,local_tv_event_start_date_time     as event_Start
                                                                                                ,duration_of_session
                                                                                                ,db1_station_code
                                                                                                ,case when local_start_time_of_recording is null then local_start_time_of_session else local_start_time_of_recording end as session_start_date_time                                                               -- This field was to link to VPS for programme data
                                                                                                ,case when local_start_time_of_recording is null then local_end_time_of_session else dateadd(mi, Duration_of_session, local_start_time_of_recording) end as session_end_date_time -- This field was to link to VPS for programme data
                                                                                                ,case   when cast(local_start_time_of_session as time) between '00:00:00.000' and '05:59:59.000' then 'night'
                                                                                                                when cast(local_start_time_of_session as time) between '06:00:00.000' and '08:59:59.000' then 'breakfast'
                                                                                                                when cast(local_start_time_of_session as time) between '09:00:00.000' and '11:59:59.000' then 'morning'
                                                                                                                when cast(local_start_time_of_session as time) between '12:00:00.000' and '14:59:59.000' then 'lunch'
                                                                                                                when cast(local_start_time_of_session as time) between '15:00:00.000' and '17:59:59.000' then 'early prime'
                                                                                                                when cast(local_start_time_of_session as time) between '18:00:00.000' and '20:59:59.000' then 'prime'
                                                                                                                when cast(local_start_time_of_session as time) between '21:00:00.000' and '23:59:59.000' then 'late night'
                                                                                                end     as session_daypart
                                                                                                ,coalesce(viewing.service_key,181818)                   as service_key
                                                                                                ,coalesce(viewing.channel_pack,'Unknown')               as channel_pack
                                                                                                ,viewing.channel_name
                                                                                                ,viewing.genre_description as programme_genre   
                                                                                                ,weights.person_number
                                                                                                ,weights.processing_weight      as processing_weight
                                                                                                ,case when person_1_viewing   = 1 and person_number = 1   then processing_weight*duration_of_session else 0 end as person_1
                                                                                                ,case when person_2_viewing   = 1 and person_number = 2   then processing_weight*duration_of_session else 0 end as person_2
                                                                                                ,case when person_3_viewing   = 1 and person_number = 3   then processing_weight*duration_of_session else 0 end as person_3
                                                                                                ,case when person_4_viewing   = 1 and person_number = 4   then processing_weight*duration_of_session else 0 end as person_4
                                                                                                ,case when person_5_viewing   = 1 and person_number = 5   then processing_weight*duration_of_session else 0 end as person_5
                                                                                                ,case when person_6_viewing   = 1 and person_number = 6   then processing_weight*duration_of_session else 0 end as person_6
                                                                                                ,case when person_7_viewing   = 1 and person_number = 7   then processing_weight*duration_of_session else 0 end as person_7
                                                                                                ,case when person_8_viewing   = 1 and person_number = 8   then processing_weight*duration_of_session else 0 end as person_8
                                                                                                ,case when person_9_viewing   = 1 and person_number = 9   then processing_weight*duration_of_session else 0 end as person_9
                                                                                                ,case when person_10_viewing  = 1 and person_number = 10  then processing_weight*duration_of_session else 0 end as person_10
                                                                                                ,case when person_11_viewing  = 1 and person_number = 11  then processing_weight*duration_of_session else 0 end as person_11
                                                                                                ,case when person_12_viewing  = 1 and person_number = 12  then processing_weight*duration_of_session else 0 end as person_12
                                                                                                ,case when person_13_viewing  = 1 and person_number = 13  then processing_weight*duration_of_session else 0 end as person_13
                                                                                                ,case when person_14_viewing  = 1 and person_number = 14  then processing_weight*duration_of_session else 0 end as person_14
                                                                                                ,case when person_15_viewing  = 1 and person_number = 15  then processing_weight*duration_of_session else 0 end as person_15
                                                                                                ,case when person_16_viewing  = 1 and person_number = 16  then processing_weight*duration_of_session else 0 end as person_16
                                                                                                ,person_1+person_2+person_3+person_4+person_5+person_6+person_7+person_8+person_9+person_10+person_11+person_12+person_13+person_14+person_15+person_16 as theflag
                                                                                                ,broadcast_start_date_time_local
                                                                                                ,broadcast_end_date_time_local
                                                                                                ,barb_instance_duration as progwatch_duration
                                                                                                ,progwatch_duration * processing_weight as progscaled_duration
                                                                                                ,viewing.viewing_platform
                                                                                                ,viewing.sky_stb_viewing
                                                                                from    barb_daily_ind_prog_viewed  as viewing                                                                                        -- [UNCOMENT FOR FINAL VERSION]
                                                                                                inner join  barb_weights                        as weights
                                                                                                on  viewing.household_number    = weights.household_number
                                                                          
                                                                                where   viewing.sky_stb_viewing = 'Y'
                                                                                and     viewing.viewing_platform = 4 -- digitial satelite
                                                                                and             viewing.panel_or_guest_flag = 'Panel'
                                                                                and     cast(viewing.local_start_time_of_session as date) between @processing_date-29 and @processing_date
                                                                                and           viewing.pvf_pv2 = 'PVF' --                                                                                                                                      -- [UNCOMENT FOR FINAL VERSION]
                                                                        )   as base
                                                                        inner join      #barbskyhhsize ON base.household_number   = #barbskyhhsize.house_id
                                                        where   base.theflag > 0
                                                )   as mega
                                                inner join  skybarb as z
                                                on  mega.household_number   = z.house_id
                                                and mega.person_number      = z.person

                commit

                create hg index hg1 on skybarb_fullview     (service_key)
                create hg index hg2 on skybarb_fullview     (household_number)
                                create hg index hg3 on skybarb_fullview     (session_daypart)
                create lf index lf1 on skybarb_fullview     (channel_pack)
                create lf index lf2 on skybarb_fullview     (programme_genre)
                create dttm index dt1 on skybarb_fullview   (start_time_of_session)
                create dttm index dt2 on skybarb_fullview   (end_time_of_session)
                create dttm index dt3 on skybarb_fullview   (session_start_date_time)
                create dttm index dt4 on skybarb_fullview   (session_end_date_time)
                commit

                grant select on skybarb_fullview to vespa_group_low_security
                commit
				DROP TABLE #barbskyhhsize
				
				 MESSAGE cast(now() as timestamp)||' | @ M04.1: UPDATING skybarb_fullview programme genre' TO CLIENT
								 
				SELECT 
					l.programme_genre
					, vps.service_key
					, vps.broadcast_start_date_time_utc
					, vps.broadcast_end_date_time_utc
				INTO #vps
				FROM  VESPA_PROGRAMME_SCHEDULE AS vps 
				JOIN (SELECT DISTINCT service_key, DATE(session_start_date_time) dt  
						FROM skybarb_fullview 
						WHERE programme_genre IS NULL OR UPPER(programme_genre) LIKE '%UNKNOWN%' )    AS v ON v.service_key = vps.service_key AND  DATE(vps.broadcast_start_date_time_utc) = v.dt 
				LEFT join  V289_M04_Channel_Genre_Lookup l on vps.channel_genre = l.channel_genre

				UPDATE  skybarb_fullview
				SET viewing.programme_genre = COALESCE(vps.programme_genre, 'Unknown')
				FROM skybarb_fullview AS viewing
				left join #vps AS vps ON viewing.service_key = vps.service_key AND 	viewing.session_start_date_time between vps.broadcast_start_date_time_utc and vps.broadcast_end_date_time_utc
				WHERE 	viewing.programme_genre IS NULL OR UPPER(viewing.programme_genre) LIKE '%UNKNOWN%'

				MESSAGE cast(now() as timestamp)||' | @ M04.1: UPDATING VPS skybarb_fullview programme genre DONE: '||@@rowcount TO CLIENT
				
				UPDATE  skybarb_fullview
				SET viewing.programme_genre = COALESCE(ska.programme_genre, 'Unknown')
				FROM skybarb_fullview AS viewing				
				left join (select     s.service_key, l.programme_genre, s.EFFECTIVE_FROM, s.EFFECTIVE_TO
							from        vespa_Analysts.CHANNEL_MAP_PROD_SERVICE_KEY_ATTRIBUTES s
							inner join  V289_M04_Channel_Genre_Lookup l on s.channel_genre = l.channel_genre) AS ska on viewing.service_key = ska.service_key  and viewing.session_start_date_time between ska.EFFECTIVE_FROM and ska.EFFECTIVE_TO								
				WHERE 	viewing.programme_genre IS NULL OR UPPER(viewing.programme_genre) LIKE '%UNKNOWN%' 
								
								
                                
                MESSAGE cast(now() as timestamp)||' | @ M04.1: Final BARB Data Preparation DONE' TO CLIENT
        
        
        end
        
        else
        begin
        
                MESSAGE cast(now() as timestamp)||' | @ M04.0: Missing Data on base tables for Data Preparation Stage!!!' TO CLIENT
                
        end

        
----------------------------
-- M04.3 - Returning Results    
----------------------------

        MESSAGE cast(now() as timestamp)||' | M04 Finished' TO CLIENT   
        
end;
GO
commit;
grant execute on v289_m04_barb_data_preparation to vespa_group_low_security;
commit;
