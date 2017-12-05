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

        This Module goal is to generate the probability matrices from BARB data to be used for identifying
        the most likely candidate(s) of been watching TV at a given event...

**Module:

        M17: DP Data Extraction
                        M17.0 - Initialising Environment
                        M17.1 - Composing Table Name
                        M17.2 - Data Extraction
                        M17.3 - Returning Results

--------------------------------------------------------------------------------------------------------------
*/

-----------------------------------
-- M17.0 - Initialising Environment
-----------------------------------

create or replace procedure ${SQLFILE_ARG001} .V289_m17_PullVOD_data_extraction
        @event_date date = null
as begin

        MESSAGE cast(now() as timestamp)||' | Begining M17.0 - Initialising Environment' TO CLIENT


        declare @dp_tname       varchar(50)
        declare @query          varchar(3000)
        declare @from_dt        integer
        declare @to_dt          integer
        declare @default_date   timestamp
        declare @pull_vod       varchar(10)


                set @dp_tname = 'VESPA_STREAM_VOD_VIEWING_PROG_FACT_'
        set @default_date = '1970-01-01 00:00:00'
        set @pull_vod = 'On Demand'
        select  @from_dt        = cast((dateformat(@Event_date,'YYYYMMDD')||'00') as integer)
        select  @to_dt          = cast((dateformat(@Event_date,'YYYYMMDD')||'23') as integer)

        if @Event_date is null
        begin
                MESSAGE cast(now() as timestamp)||' | @ M17.0: You need to provide a Date for extraction !!!' TO CLIENT
        end
        else
        begin

            MESSAGE cast(now() as timestamp)||' | @ M17.0: Initialising Environment DONE' TO CLIENT

-------------------------------
-- M17.1 - Composing Table Name
-------------------------------

                        MESSAGE cast(now() as timestamp)||' | Begining M17.1 - Composing Table Name' TO CLIENT

                        set @dp_tname = @dp_tname||datepart(year,@Event_date)||right(('00'||cast(datepart(month,@event_date) as varchar(2))),2)

                        MESSAGE cast(now() as timestamp)||' | @ M17.1: Composing Table Name DONE: '||@dp_tname  TO CLIENT


--------------------------
-- M17.2 - Data Extraction
--------------------------

                        MESSAGE cast(now() as timestamp)||' | Begining M17.2 - Data Extraction' TO CLIENT

                        if  exists(  select tname from syscatalog
                                        where creator = ${SQLFILE_ARG001} 
                                        and upper(tname) = upper('v289_M17_vod_raw_data')
                                        and     tabletype = 'TABLE')
                                        truncate table v289_M17_vod_raw_data

                        commit

                        if  exists(  select tname from syscatalog
                                        where creator = ${SQLFILE_ARG001} 
                                        and upper(tname) = upper('v289_m17_pseudo')
                                        and     tabletype = 'TABLE')
                         -- verifying our step table is free for re-usage
                                        drop table v289_m17_pseudo

                        commit


                        -- Dedupe household keys per account from our sample
                        select
                                                        account_number
                                        ,       min(cb_key_household)  as household_key
                        into    #account_household_keys
                        from    V289_M08_SKY_HH_composition
                        WHERE PANEL_FLAG = 1
                        group by        account_number
                        commit

                        create unique hg index idx1 on #account_household_keys(account_number)
                        commit
						
						  MESSAGE cast(now() as timestamp)||' | M17.2 - Data Extraction preparation done' TO CLIENT

                        -- Now pull in data for the sample from the viewing tables
                        set @query =    'if  exists(  select tname from syscatalog where lower(creator) = ''sk_prod'' and upper(tname) = upper('''||@dp_tname||''')  ) '||
										'select  pk_viewing_programme_instance_fact as pk_viewing_prog_instance_fact'||
                                                                        ',dk_event_start_datehour as dk_event_start_datehour_dim'||
                                                                        ',dk_event_start_time' ||
                                                                        ',dk_event_end_datehour as dk_event_end_datehour_dim'||
                                                                        ',dk_event_end_time' ||
                                                                        ',dk_broadcast_start_Datehour as dk_broadcast_start_Datehour_dim'||
                                                                        ',dk_broadcast_start_time' ||
                                                                        ',dk_instance_start_datehour as dk_instance_start_datehour_dim'||
                                                                        ',dk_instance_start_time' ||
                                                                        ',duration'||
                                                                        ',case when programme_genre in (''Undefined'',''Unknown'') then ''Unknown'' else programme_genre end as genre_description'||
                                                                        ',cast(null as integer) as service_key'||
                                                                        ',c.household_key'||
                                                                        ',cast(NULL as timestamp) as event_start_date_time_utc'||
                                                                        ',cast(NULL as timestamp) as event_end_date_time_utc'||
                                                                        ',cast(a.account_number as varchar(15)) as account_number'||
                                                                        ',99 as subscriber_id'||
                                                                        ',cast(null as varchar(1)) as service_instance_id'||
                                                                        ',programme_name'||
                                                                        ',cast(NULL as timestamp) as capping_end_Date_time_utc'||
                                                                        ',dk_capped_event_end_time_datehour_dim' ||
                                                                        ',dk_capped_event_end_time_dim' ||
                                                                        ',cast(NULL as timestamp) as broadcast_start_date_time_utc'||
                                                                        ',cast(NULL as timestamp) as broadcast_end_date_time_utc'||
                                                                        ',cast(NULL as timestamp) as instance_start_date_time_utc'||
                                                                        ',cast(NULL as timestamp) as instance_end_date_time_utc'||
                                                                        ',dk_broadcast_end_Datehour as dk_broadcast_end_Datehour_dim'||
                                                                        ',dk_broadcast_end_time' ||
                                                                        ',dk_instance_end_datehour as dk_instance_end_datehour_dim' ||
                                                                        ',dk_instance_end_time' ||
                                                                        ',prog_dim_provider_id as provider_id' ||
                                                                        ',-1 as provider_id_number ' ||
                                                                        ',dk_barb_min_end_datehour ' ||
                                                                        ',dk_barb_min_end_time ' ||
                                                                        ',dk_barb_min_start_datehour ' ||
                                                                        ',dk_barb_min_start_time ' ||
                                                                        ',cast(NULL as timestamp) as barb_min_start_date_time_utc ' ||
                                                                        ',cast(NULL as timestamp) as barb_min_end_date_time_utc ' ||
                                                        'into   v289_m17_pseudo '||
                                                        'from   '||@dp_tname||' as a '||
                                                                        'inner join  #account_household_keys  as c '||
                                                                        'on cast(a.account_number as varchar(15)) = c.account_number '||
                                                        'where  dk_event_start_datehour_dim between '||@from_dt||' and '||@to_dt||' '||
                                                        'and    event_sub_type = ''On Demand'' '||
                                                        'and    prog_dim_provider_id is not null'

						 
                        execute (@query)
                        commit
						 if  NOT exists(  select tname from syscatalog where creator = ${SQLFILE_ARG001}  and upper(tname) = upper('v289_m17_pseudo') and     tabletype = 'TABLE') GOTO fatality
						
                        -- Add indices to working table
                        create hg index key1 on v289_m17_pseudo(pk_viewing_prog_instance_fact)
                        create hg index hg1 on v289_m17_pseudo(dk_event_start_datehour_dim)
                        create hg index hg2 on v289_m17_pseudo(dk_broadcast_start_datehour_dim)
                        create hg index hg3 on v289_m17_pseudo(dk_instance_start_datehour_dim)
                        commit


                        MESSAGE cast(now() as timestamp)||' | Begining M17.2 - Data Extraction Completed' TO CLIENT

                        MESSAGE cast(now() as timestamp)||' | Begining M17.2 - PATCH FOR PROGRAMME_GENRE' TO CLIENT

                        update  v289_m17_pseudo
                        set             genre_description =     case lower(trim(genre_description))     when '(unknown)'                then 'Unknown'
                                                                                                                                                        when 'entertainment'    then 'Entertainment'
                                                                                                                                                        when 'kids'                             then 'Children'
                                                                                                                                                        when 'movies'                   then 'Movies'
                                                                                                                                                        when 'music'                    then 'Music & Radio'
                                                                                                                                                        when 'news'                             then 'News & Documentaries'
                                                                                                                                                        when 'sports'                   then 'Sports'
                                                                                                                                                        else 'Unknown'
                                                                                end
                        commit


                        MESSAGE cast(now() as timestamp)||' | Begining M17.2 - PATCH FOR PROGRAMME_GENRE DONE' TO CLIENT

                        -- Update date/time fields
                        UPDATE v289_m17_pseudo p
						SET event_start_date_time_utc = CAST (CAST (LEFT(CAST (dk_event_start_datehour_dim AS VARCHAR), 8) AS DATE) || ' ' 
															|| CAST (SUBSTRING(cast(dk_event_start_time AS VARCHAR), 2, 2) || ':'
																|| SUBSTRING(cast(dk_event_start_time AS VARCHAR), 4, 2) || ':' 
																|| SUBSTRING(cast(dk_event_start_time AS VARCHAR), 6, 2) AS TIME) AS DATETIME)
						WHERE event_start_date_time_utc IS NULL 
                        commit


						UPDATE v289_m17_pseudo p
						SET event_end_date_time_utc = CAST (CAST (LEFT(CAST (dk_event_end_datehour_dim AS VARCHAR), 8) AS DATE) || ' ' 
															|| CAST (SUBSTRING(cast(dk_event_end_time AS VARCHAR), 2, 2) || ':'
																|| SUBSTRING(cast(dk_event_end_time AS VARCHAR), 4, 2) || ':' 
																|| SUBSTRING(cast(dk_event_end_time AS VARCHAR), 6, 2) AS TIME) AS DATETIME)
						WHERE event_end_date_time_utc IS NULL 
                        commit



                        UPDATE v289_m17_pseudo p
						SET capping_end_Date_time_utc = CAST (CAST (LEFT(CAST (dk_capped_event_end_time_datehour_dim AS VARCHAR), 8) AS DATE) || ' ' 
															|| CAST (SUBSTRING(cast(dk_capped_event_end_time_dim AS VARCHAR), 2, 2) || ':'
																|| SUBSTRING(cast(dk_capped_event_end_time_dim AS VARCHAR), 4, 2) || ':' 
																|| SUBSTRING(cast(dk_capped_event_end_time_dim AS VARCHAR), 6, 2) AS TIME) AS DATETIME)
						WHERE capping_end_Date_time_utc IS NULL 
                        commit

                        -- ensure every event has a capped time (i.e. not null)
                        update v289_m17_pseudo p
                        set capping_end_Date_time_utc = event_end_date_time_utc
                        where p.capping_end_Date_time_utc is null



                        UPDATE v289_m17_pseudo p
						SET instance_start_date_time_utc = CAST (CAST (LEFT(CAST (dk_instance_start_datehour_dim AS VARCHAR), 8) AS DATE) || ' ' 
															|| CAST (SUBSTRING(cast(dk_instance_start_time AS VARCHAR), 2, 2) || ':'
																|| SUBSTRING(cast(dk_instance_start_time AS VARCHAR), 4, 2) || ':' 
																|| SUBSTRING(cast(dk_instance_start_time AS VARCHAR), 6, 2) AS TIME) AS DATETIME)
						WHERE instance_start_date_time_utc IS NULL 
                        commit


                        UPDATE v289_m17_pseudo p
						SET instance_end_date_time_utc = CAST (CAST (LEFT(CAST (dk_instance_end_datehour_dim AS VARCHAR), 8) AS DATE) || ' ' 
															|| CAST (SUBSTRING(cast(dk_instance_end_time AS VARCHAR), 2, 2) || ':'
																|| SUBSTRING(cast(dk_instance_end_time AS VARCHAR), 4, 2) || ':' 
																|| SUBSTRING(cast(dk_instance_end_time AS VARCHAR), 6, 2) AS TIME) AS DATETIME)
						WHERE instance_end_date_time_utc IS NULL 
                        commit

                        -- calculate capped event duration - used for minute attribution
                        update  v289_m17_pseudo p
                        set duration = datediff(ss, event_start_date_time_utc,
                                                        case when event_end_date_time_utc <= capping_end_Date_time_utc
                                                                  then event_end_date_time_utc
                                                                  else capping_end_Date_time_utc
                                                        end
                                                )
                        commit

 -- Currently minute attribution not applied to Pull Vod so these won't work. Will have to apply logic here
               

                        update  v289_m17_pseudo p
                        set barb_min_start_date_time_utc = case when duration < 31 then null
                                                                else case when second(event_start_date_time_utc) < 31
                                                                          then datefloor(mi, event_start_date_time_utc)
                                                                          else dateceiling(mi, event_start_date_time_utc)
                                                                     end
                                                           end
                        commit


                        update  v289_m17_pseudo p
                        set barb_min_end_date_time_utc = case when duration < 31 then null
                                                                else case when second(capping_end_Date_time_utc) < 31
                                                                          then dateadd(mi, -1, datefloor(mi, capping_end_Date_time_utc))
                                                                          else datefloor(mi, capping_end_Date_time_utc)
                                                                     end
                                                         end
                        commit


                        -- For short events the barb start might be larger then start - these can't be minute attribted
                        update v289_m17_pseudo p
                        set barb_min_start_date_time_utc = null
                        where barb_min_end_date_time_utc is null
                        or p.barb_min_start_date_time_utc > p.barb_min_end_date_time_utc
                        commit

                        update v289_m17_pseudo p
                        set barb_min_end_date_time_utc = null
                        where barb_min_start_date_time_utc is null
                        or p.barb_min_start_date_time_utc > p.barb_min_end_date_time_utc
                        commit


                        -- integrating service_key for pullvod services from CM SKA

                        update  v289_m17_pseudo as m17
                        set             m17.service_key = ska.service_key
                        from    vespa_analysts.channel_map_prod_service_key_attributes   as ska
                        where   m17.provider_id = ska.provider_id
                        and     m17.event_start_date_time_utc between ska.effective_from and ska.effective_to
                        commit

                        --create hg index hg4 on v289_m17_pseudo(dk_viewing_event_dim)
                        create hg index hg5 on v289_m17_pseudo(service_key)
                        create hg index hg6 on v289_m17_pseudo(account_number)
                        create hg index hg7 on v289_m17_pseudo(subscriber_id)
                        create hg index hg8 on v289_m17_pseudo(programme_name)
                        create lf index lf1 on v289_m17_pseudo(genre_description)
                        commit


                        -- Clean up
                        drop table #account_household_keys
                        commit


                        MESSAGE cast(now() as timestamp)||' | @ M17.2: Data Extraction !!!!!SHIELD AGAINST DUPLICATED PKS!!!!!!' TO CLIENT
                        /*
                                        AD: 12-02-2015:
                                        This is a redundancy check we add as we saw data issues before where the PK of the Viewing tables is duplicated
                                        (haha sweet irony), but yeah it happens... so we shield against this few number of cases that were making the project
                                        crash at this stage
                        */
                        select  pk_viewing_prog_instance_fact
                        into    #templist
                        from    v289_m17_pseudo -- 27261399
                        group   by  pk_viewing_prog_instance_fact
                        having  count(1) > 1
                        commit

                        create unique hg index idx1 on #templist(pk_viewing_prog_instance_fact)
                        commit


                        delete from v289_m17_pseudo as a
                        from    #templist   as b
                        where   a.pk_viewing_prog_instance_fact = b.pk_viewing_prog_instance_fact

                        commit
                        drop table #templist
                        commit
                        MESSAGE cast(now() as timestamp)||' | @ M17.2: Data Extraction !!!!!SHIELD AGAINST DUPLICATED PKS!!!!!! DONE. ROWS DELETED:'||@@rowcount  TO CLIENT

                        insert  into v289_M17_vod_raw_data  (
                                                                                                        pk_viewing_prog_instance_fact
                                                                                                        ,dk_event_start_datehour_dim
                                                                                                        ,dk_event_end_datehour_dim
                                                                                                        ,dk_broadcast_start_Datehour_dim
                                                                                                        ,dk_instance_start_datehour_dim
                                                                                                        ,duration
                                                                                                        ,genre_description
                                                                                                        ,service_key
                                                                                                        ,cb_key_household
                                                                                                        ,event_start_date_time_utc
                                                                                                        ,event_end_date_time_utc
                                                                                                        ,account_number
                                                                                                        ,subscriber_id
                                                                                                        ,service_instance_id
                                                                                                        ,programme_name
                                                                                                        ,capping_end_Date_time_utc
                                                                                                        ,broadcast_start_date_time_utc
                                                                                                        ,broadcast_end_date_time_utc
                                                                                                        ,instance_start_date_time_utc
                                                                                                        ,instance_end_date_time_utc
                                                                                                        ,provider_id
                                                                                                        ,provider_id_number
                                                                                                        ,barb_min_start_date_time_utc
                                                                                                        ,barb_min_end_date_time_utc
                                                                                                )
                        select  pk_viewing_prog_instance_fact
                                        ,dk_event_start_datehour_dim
                                        ,dk_event_end_datehour_dim
                                        ,dk_broadcast_start_Datehour_dim
                                        ,dk_instance_start_datehour_dim
                                        ,duration
                                        ,genre_description
                                        ,service_key
                                        ,household_key
                                        ,event_start_date_time_utc
                                        ,event_end_date_time_utc
                                        ,account_number
                                        ,subscriber_id
                                        ,service_instance_id
                                        ,programme_name
                                        ,capping_end_Date_time_utc
                                        ,broadcast_start_date_time_utc
                                        ,broadcast_end_date_time_utc
                                        ,instance_start_date_time_utc
                                        ,instance_end_date_time_utc
                                        ,provider_id
                                        ,provider_id_number
                                        ,barb_min_start_date_time_utc
                                        ,barb_min_end_date_time_utc
                        from    v289_m17_pseudo
						
						fatality: ---- LABEL IN CASE Pull VOD is not available
                        
						MESSAGE cast(now() as timestamp)||' | @ M17.2: Data Extraction DONE ROWS:'||@@rowcount  TO CLIENT

                        commit
							if  exists(  select tname from syscatalog where creator = ${SQLFILE_ARG001}  and upper(tname) = upper('v289_m17_pseudo') and     tabletype = 'TABLE')
							drop table v289_m17_pseudo
                        commit

----------------------------
-- M17.3 - Returning Results
----------------------------

        end     --      if @Event_date is null {...}, else...

        MESSAGE cast(now() as timestamp)||' | M17 Finished' TO CLIENT

end;
GO

commit;
grant execute on v289_m17_PullVOD_data_extraction to vespa_group_low_security;
commit;
