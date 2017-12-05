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
**Project Name:							Skyview H2I
**Analysts:                             Angel Donnarumma	(angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jason Thompson		(Jason.Thompson@skyiq.co.uk)
										,Hoi Yu Tang		(HoiYu.Tang@skyiq.co.uk)
										,Jose Pitteloud		(jose.pitteloud@skyiq.co.uk)
**Stakeholder:                          SkyIQ
										,Jose Loureda		(Jose.Loureda@skyiq.co.uk)
**Due Date:                             11/07/2014
**Project Code (Insight Collation):     v289
**Sharepoint Folder:    

	http://sp-department.bskyb.com/sites/SIGEvolved/Shared%20Documents/Forms/AllItems.aspx?RootFolder=%2Fsites%2FSIGEvolved%2FShared%20Documents%2F01%20Analysis%20Requests%2FV289%20-%20Skyview%20Futures%2F01%20Plans%20Briefs%20and%20Project%20Admin                                                        
	                                                          
**Business Brief:

	This Module goal is to generate the probability matrices from BARB data to be used for identifying
	the most likely candidate(s) of been watching TV at a given event...

**Module:
	
	M05: Barb Matrices Generation
			M05.0 - Initialising Environment
			M05.1 - Aggregating transient tables 
			M05.2 - Generating Matrices
			M05.3 - Generating Default Matrices
			M05.4 - Returning Results
	
--------------------------------------------------------------------------------------------------------------
*/

-----------------------------------
-- M05.0 - Initialising Environment
-----------------------------------

create or replace procedure v289_m05_barb_Matrices_generation
as begin

	MESSAGE cast(now() as timestamp)||' | Begining M05.0 - Initialising Environment' TO CLIENT
	MESSAGE cast(now() as timestamp)||' | @ M05.0: Initialising Environment DONE' TO CLIENT

	
---------------------------------------
-- M05.1 - Aggregating transient tables
---------------------------------------

	MESSAGE cast(now() as timestamp)||' | Begining M05.1 - Aggregating transient tables' TO CLIENT
	
	/*
		Aggregating hours watched for all dimensions, this will be the base for slicing the 
		probabilities when generating the Matrices...
	*/
	
	select  cast(start_time_of_session as date) as thedate
			,lookup.segment_id
			,skybarb.hhsize
			,skybarb.sex
			,coalesce(skybarb.ageband,'Undefined') as ageband
			,cast((sum(distinct skybarb.progscaled_duration)/60.0) as integer)  as uk_hhwatched
	into	#base
	from    skybarb_fullview    as skybarb
            inner join V289_PIV_Grouped_Segments_desc    as lookup
            on  skybarb.session_daypart = lookup.daypart
            and skybarb.channel_pack    = lookup.channel_pack
            and skybarb.programme_genre = lookup.genre
	group   by  thedate
				,lookup.segment_id
				,skybarb.hhsize
				,skybarb.sex
				,skybarb.ageband
	
	commit
	
	create lf index lf1 on #base(segment_id)
	commit
	
	MESSAGE cast(now() as timestamp)||' | @ M05.1: Base Table Generation DONE' TO CLIENT
	
	
	/*
		Identifying sessions on the viewing data...
	*/
	
	select  *
			,rank() over    (
								partition by    household_number
								order by        start_time_of_session
							)   as session_id
	into	#pseudo_base2
	from    skybarb_fullview
	
	commit
	
	create lf index lf1 on #pseudo_base2(session_daypart)
	create lf index lf2 on #pseudo_base2(channel_pack)
	create lf index lf3 on #pseudo_base2(programme_genre)
	commit
	
	select  cast(skybarb.start_time_of_session as date) as thedate
            ,lookup.segment_id
            ,skybarb.hhsize
            ,skybarb.session_id
            ,count(distinct skybarb.person_number)  as session_size
            ,cast((sum(distinct skybarb.progscaled_duration)/60.0) as integer)  as uk_hhwatched
    into    #base2
    from    #pseudo_base2                                            as skybarb
            inner join V289_PIV_Grouped_Segments_desc    as lookup
            on  skybarb.session_daypart = lookup.daypart
            and skybarb.channel_pack    = lookup.channel_pack
            and skybarb.programme_genre = lookup.genre
    group   by  thedate
                ,lookup.segment_id
                ,skybarb.hhsize
                ,skybarb.session_id
    
    
    commit
    
    create lf index lf1 on #base2(segment_id)
	create lf index lf2 on #base2(hhsize)
	commit
	
	drop table #pseudo_base2
	commit
	
	MESSAGE cast(now() as timestamp)||' | @ M05.1: Base2 Table Generation DONE' TO CLIENT
	
	MESSAGE cast(now() as timestamp)||' | @ M05.1: Aggregating transient tables DONE' TO CLIENT

------------------------------
-- M05.2 - Generating Matrices
------------------------------

	MESSAGE cast(now() as timestamp)||' | Begining M05.2 - Generating Matrices' TO CLIENT
	
	-- PIV sex/age
	
	/*
		Now generating the matrix to identify who is most likely to be watching TV based
		BARB distributions by sex and age over specific part of the day, channel pack
		and programme genre...
	*/
	
	if object_id('v289_genderage_matrix') is not null
		drop table v289_genderage_matrix
		
	commit
	
	select  base.*
			,cast(base.uk_hhwatched as decimal(10,2)) / cast(totals.tot_uk_hhwatched as decimal(10,2)) as PIV
	into	v289_genderage_matrix
	from    #base        as base
			inner join  (
							select  thedate
									,segment_id
									,sum(uk_hhwatched)  as tot_uk_hhwatched
							from    #base
							group   by  thedate
										,segment_id
						)   as totals
			on  base.thedate            = totals.thedate
			and base.segment_id         = totals.segment_id
	where   totals.tot_uk_hhwatched > 0 
	
	commit
	
	create lf index lf1 on v289_genderage_matrix(segment_id)
	commit
	
	grant select on v289_genderage_matrix to vespa_group_low_security
	commit
	
	MESSAGE cast(now() as timestamp)||' | @ M05.2: Sex/Age Matrix Generation DONE (v289_genderage_matrix)' TO CLIENT
	
	-- PIV Session size
	
	/*
		This is the probability matrix to determine the size of the session, how many
		people were watching TV on a given date, an specific part of the day, household
		size, channel pack and programme genre. All based on BARB distributions...
	*/
	
	if object_id('v289_sessionsize_matrix') is not null
		drop table v289_sessionsize_matrix
		
	commit
	
	select  base.*
			,cast(base.uk_hhwatched as decimal(10,2)) / cast(totals.tot_uk_hhwatched as decimal(10,2)) as proportion
			,coalesce((SUM (proportion) OVER (PARTITION BY base.thedate,base.segment_ID, base.hhsize ORDER BY base.session_size ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)),0) AS Lower_limit
            ,SUM (proportion) OVER (PARTITION BY base.thedate,base.segment_ID, base.hhsize ORDER BY base.session_size ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS Upper_limit
	into	v289_sessionsize_matrix
	from    (
                select  thedate
                        ,segment_id
                        ,hhsize
                        ,session_size
                        ,sum(uk_hhwatched) as uk_hhwatched
                from    #base2
                group   by  thedate
                            ,segment_id
                            ,hhsize
                            ,session_size
            )   as base
			inner join  (
							select  thedate
                                    ,segment_id
									,hhsize
									,sum(uk_hhwatched) as tot_uk_hhwatched
							from    #base2
							group   by  thedate
										,segment_id
										,hhsize
						)   as totals
			on  base.thedate            = totals.thedate
			and base.segment_id         = totals.segment_id
			and base.hhsize             = totals.hhsize
    where   totals.tot_uk_hhwatched > 0
	
	commit
    
    create lf index lf1 on v289_sessionsize_matrix(segment_id)
	create lf index lf2 on v289_sessionsize_matrix(hhsize)
	commit
	
	grant select on v289_sessionsize_matrix to vespa_group_low_security
	commit
	
	MESSAGE cast(now() as timestamp)||' | @ M05.2: Session size Matrix Generation DONE (v289_sessionsize_matrix)' TO CLIENT

	MESSAGE cast(now() as timestamp)||' | @ M05.2: Generating Matrices DONE' TO CLIENT	

--------------------------------------
-- M05.3 - Generating Default Matrices
--------------------------------------
	
	MESSAGE cast(now() as timestamp)||' | Begining M05.3 - Generating Default Matrices' TO CLIENT
	
	-- working out gender/age default matrix
	
	/*
		first thing is to create a cross join table with all possible combination
		this table will be updated later on with the average of the whole month to cover
		any gaps.
		
		any remaining gap after such update will just be filled with the minimum value present
		in the matrix divided by 2 (we don't set this value to a static figure as there is the risk
		of that number been higher than the real minimum value in the matrix)
	*/
	
	MESSAGE cast(now() as timestamp)||' | @ M05.3: Constructing Gender/Age DEFAULT Matrix' TO CLIENT
	
	if object_id('v289_m05_def_genderage_matrix') is not null
		drop table v289_m05_def_genderage_matrix
		
	commit
	
	SELECT	hhsize
			,segment_id
			,sex
			,ageband
			,cast(0 as float) 			AS sum_hours_watched
			,cast(0 as float)			AS sum_hours_over_all_demog
			,cast(1.2345678 as float)	AS PIV_default
	into	v289_m05_def_genderage_matrix
	FROM 	(
				SELECT 	DISTINCT segment_id
				FROM 	V289_PIV_Grouped_Segments_desc
			)	as b
			CROSS JOIN	(
							SELECT 	DISTINCT 
									sex
									,ageband
							FROM 	v289_genderage_matrix
							union
							select	'Undefined'	as sex
									,'0-19'		as age
						)	as c
			CROSS JOIN 	(
							SELECT 	row_num AS hhsize
							FROM 	sa_rowgenerator( 1, 15 )
						)	as d
			
	commit
	
	create lf index lf1 on v289_m05_def_genderage_matrix(hhsize)
	create lf index lf2 on v289_m05_def_genderage_matrix(sex)
	create lf index lf3 on v289_m05_def_genderage_matrix(ageband)
	create hg index hg1 on v289_m05_def_genderage_matrix(segment_id)
	
	commit
	
	DELETE	FROM v289_m05_def_genderage_matrix
	WHERE 	sex not like '%Undef%' 
	AND 	ageband like '0-19%'
	
	COMMIT 
	
	MESSAGE cast(now() as timestamp)||' | @ M05.3: Gender/Age DEFAULT Matrix Template DONE' TO CLIENT
	
	SELECT	hhsize
			,segment_id
			,sex
			,ageband
			,sum_hours_watched
			,sum_hours_over_all_demog
			,PIV_default
	INTO 	#PIV
	FROM 	(
				SELECT	hhsize
						,segment_id
						,CAST	(
									case ageband	when '0-19'	then 'Undefined'
													else sex
									end	
									AS VARCHAR(10)
								) 	as sex
						,ageband
						,uk_hhwatched
						,case	when (uk_hhwatched = 0 or uk_hhwatched is null) then    1e-3
								else uk_hhwatched
						end		as uk_hhwatched_nonzero
						,sum(uk_hhwatched_nonzero) over	(	
															partition by	segment_id
																			,hhsize
																			,sex
																			,ageband
														)	as sum_hours_watched
						,	sum(uk_hhwatched_nonzero) over	(	
																partition by	segment_id
																				,hhsize
															)	as sum_hours_over_all_demog
						,1.0 * sum_hours_watched / sum_hours_over_all_demog	as PIV_default
				FROM 	v289_genderage_matrix
				WHERE 	ageband	<> 'Undefined'
			)	as	t
	GROUP 	BY	hhsize
				,segment_id
				,sex
				,ageband
				,sum_hours_watched
				,sum_hours_over_all_demog
				,PIV_default
	
	MESSAGE cast(now() as timestamp)||' | @ M05.3: Gender/Age DEFAULT Matrix values DONE' TO CLIENT
	
	UPDATE 	v289_m05_def_genderage_matrix	as a
	SET   	a.sum_hours_watched			= j.sum_hours_watched
			,a.sum_hours_over_all_demog	= j.sum_hours_over_all_demog
			,a.PIV_default				= j.PIV_default
	from	#PIV AS j 	
	where	a.hhsize = j.hhsize 
	AND 	a.segment_id = j.segment_id
	AND 	LEFT(a.sex,1) = LEFT(j.sex,1)
	AND 	LEFT(a.ageband,2) = LEFT(j.ageband,2)
	
	commit
	
	update	v289_m05_def_genderage_matrix
	set		piv_default = (select min(piv_default)/2.00 from #PIV where piv_default > 0)
	where	piv_default = 1.2345678
	
	commit
	
	drop table #piv
	grant select on v289_m05_def_genderage_matrix to vespa_group_low_security
	
	commit
	
	MESSAGE cast(now() as timestamp)||' | @ M05.3: Gender/Age DEFAULT Matrix Update DONE' TO CLIENT
	
	
	-- working out session size default matrix
	
	/*
		Pretty much applying the same concept as above (though on a different shape (Script)) 
		but essentially does the same
		
		The reason for this two versions of the same is because these were extracts from different
		scripts and I really didn't want to mess things up... it was working as intended
		so let's keep it cool...
	*/
	
	MESSAGE cast(now() as timestamp)||' | @ M05.3: Constructing Session Size DEFAULT Matrix' TO CLIENT
	
	declare @min decimal(8,7)   

    if object_id('v289_sessionsize_matrix_ID') is not null
        drop table v289_sessionsize_matrix_ID

    commit

    SELECT  segment_id
			,session_size
			,hhsize
			,SUM(uk_hhwatched) uk_hhwatched
			,SUM(uk_hhwatched) OVER (PARTITION BY segment_id, hhsize) AS total_watched
			,CAST (uk_hhwatched AS DECIMAL(11,1)) / CAST (total_watched  AS DECIMAL(11,1)) AS proportion
    INTO    v289_sessionsize_matrix_ID
	FROM    v289_sessionsize_matrix
    group   by  segment_id
    			,session_size
    			,hhsize    
	
	MESSAGE cast(now() as timestamp)||' | @ M05.3: Session Size DEFAULT Matrix Values DONE' TO CLIENT
	
	SET @min  = (
                    SELECT  min(proportion) /2
                    FROM    v289_sessionsize_matrix_ID
                    where   proportion >0
                )

	if object_id('v289_M05_def_sessionsize_matrix') is not null 
        drop table v289_M05_def_sessionsize_matrix

	commit
	
	SELECT  a.segment_ID
    		,b.hhsize
    		,sx.session_size
    		,COALESCE(c.proportion, @min)    AS proportion
    		,SUM (proportion)  OVER    (PARTITION BY a.segment_ID, b.hhsize )  AS norm
    		,coalesce((SUM (proportion)  OVER    (PARTITION BY a.segment_ID, b.hhsize  ORDER BY sx.session_size ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) / norm),0)    AS Lower_limit
    		,coalesce((SUM (proportion)  OVER    (PARTITION BY a.segment_ID, b.hhsize  ORDER BY sx.session_size ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / norm),0)    AS Upper_limit
	INTO    v289_M05_def_sessionsize_matrix
	FROM    (
                SELECT  DISTINCT 
                        segment_ID 
                FROM    V289_PIV_Grouped_Segments_desc
            )   as a
	        CROSS JOIN  (
                            SELECT  DISTINCT 
                                    CASE    WHEN hhsize >8 THEN 8 
                                            ELSE hhsize 
                                    END     as hhsize 
                            FROM    v289_sessionsize_matrix_ID
                        )   AS b
	        CROSS JOIN  (
                            SELECT  DISTINCT 
                                    CASE    WHEN hhsize >8 THEN 8 
                                            ELSE hhsize 
                                    END     as session_size 
                            FROM    v289_sessionsize_matrix_ID
                        )   AS sx
	        LEFT JOIN   v289_sessionsize_matrix_ID AS c 
            ON  a.segment_id = c.segment_id 
            AND b.hhsize = c.hhsize 
            AND c.session_size = sx.session_size 
            AND c. proportion > 0
	WHERE   b.hhsize >= sx.session_size

	DELETE FROM v289_M05_def_sessionsize_matrix
	WHERE session_size > hhsize
	
	COMMIT

	CREATE LF INDEX UW ON v289_M05_def_sessionsize_matrix(segment_ID)
	CREATE LF INDEX UQ ON v289_M05_def_sessionsize_matrix(hhsize)
	
	COMMIT

	DROP TABLE v289_sessionsize_matrix_ID
	
	GRANT select ON v289_M05_def_sessionsize_matrix  TO vespa_group_low_security	

	COMMIT 
	
	MESSAGE cast(now() as timestamp)||' | @ M05.3: Session Size DEFAULT Matrix DONE' TO CLIENT
	
	MESSAGE cast(now() as timestamp)||' | @ M05.3: Generating Default Matrices DONE' TO CLIENT	
	
----------------------------
-- M05.4 - Returning Results
----------------------------

	MESSAGE cast(now() as timestamp)||' | M05 Finished' TO CLIENT	

end;

commit;
grant execute on v289_m05_barb_Matrices_generation to vespa_group_low_security;
commit;