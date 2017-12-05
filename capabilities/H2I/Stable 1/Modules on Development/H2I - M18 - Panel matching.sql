create or replace procedure v289_m18_panel_matching
	@event_date date = null
--	@matching_ratio smallint 
as begin


	DECLARE @matching_ratio smallint
	SET @matching_ratio=70

	MESSAGE cast(now() as timestamp)||' | Begining M18.0 - Initialising Environment' TO CLIENT

	if  exists(  SELECT tname FROM syscatalog where creator = user_name() and upper(tname) = upper('test_table') and     tabletype = 'TABLE')                
	DROP TABLE test_table   commit	

	UPDATE V289_M08_SKY_HH_composition
	SET panel_flag = 1 
	FROM V289_M08_SKY_HH_composition as a 
	JOIN (SELECT DISTINCT account_number FROM VIQ_VIEWING_DATA_SCALING  WHERE adjusted_event_start_date_vespa = @event_date )   as viq ON a.account_number    = viq.account_number
	
	
	
	
	
	--- find the hhd compositions that exist in the Skybarb panel and count the household.  Apply the matching_ratio to the count to give the number of households required in our panel data
	MESSAGE cast(now() as timestamp)||' | @ M018.1: Create household compositions of SkyBarb !!!' TO CLIENT

	SELECT composition
		, ceil(count(*)*@matching_ratio) as required_h08_count 
	into  #skybarb_composition
	FROM (SELECT house_id
				, max(hhsize) as hhsize 
				, max(field1)||','||max(field2)||','||max(field3)||','||max(field4)||','||max(field5)||','||max(field6)||','||max(field7) ||','||max(field8) as composition
		FROM (SELECT *,
					   dense_rank() over (partition by list.house_id order by hhsize, comp1)  as ranking
					,CASE WHEN dense_rank() over (partition by list.house_id order by hhsize, comp1) = hhsize then 1
						  else 0 end as lastval
					,CASE WHEN dense_rank() over (partition by list.house_id order by hhsize, comp1) = 1 then hhsize||':'||comp1
						else '' end as field1
					,CASE WHEN dense_rank() over (partition by list.house_id order by hhsize, comp1) = 2 then comp1
						else '' end as field2
					,CASE WHEN dense_rank() over (partition by list.house_id order by hhsize, comp1) = 3 then comp1
						else '' end as field3
					,CASE WHEN dense_rank() over (partition by list.house_id order by hhsize, comp1) = 4 then comp1
						else '' end as field4
					,CASE WHEN dense_rank() over (partition by list.house_id order by hhsize, comp1) = 5 then comp1
						else '' end as field5
					,CASE WHEN dense_rank() over (partition by list.house_id order by hhsize, comp1) = 6 then comp1
						else '' end as field6
					,CASE WHEN dense_rank() over (partition by list.house_id order by hhsize, comp1) = 7 then comp1
						else '' end as field7
					,CASE WHEN dense_rank() over (partition by list.house_id order by hhsize, comp1) = 8 then comp1
						else '' end as field8
					, comp1 as test
					
					-- , max(composition) over (partition by list.house_id order by hhsize, comp1 rows between 1 following and 1 following)||','||comp1 as test
			FROM (SELECT comp.house_id, comp.hhsize, sexage||'*'||cnt as comp1 
					FROM (SELECT house_id, hhsize, sexage, count(*) as cnt
							FROM (SELECT sb.house_id
										, CASE  WHEN sb.age between 12 and 19     then '12-19'
												WHEN sb.age between 0 and 11     then '0-11'
												WHEN sb.age between 20 and 24   then '20-24'
												WHEN sb.age between 25 and 34   then '25-34'
												WHEN sb.age between 35 and 44   then '35-44'
												WHEN sb.age between 45 and 64   then '45-64'
												WHEN sb.age >= 65               then '65+'
												end     	as ageband
										,CASE 	WHEN sex='Female' and ageband not in ('12-19','0-11') then 'F'||ageband
												WHEN sex='Male' and ageband not in ('12-19','0-11') then 'M'||ageband
												else 'U'||ageband
												end 		as sexage
										, COUNT(person) OVER (PARTITION BY house_id) 	AS hhsize 	-- fixing barb sample to only barb panellists with Sky (table FROM prior step)
								FROM skybarb AS  sb
								--order by sb.house_id, hhsize, sexage
								) AS  x
								group by house_id, hhsize, sexage 
						)	AS 	comp
					-- 	order by comp.house_id, comp.hhsize, comp1
				)  AS list 
			) AS concat_table
			group by house_id
		) AS agg
	group by composition
	commit
	--SELECT top 100 *  FROM  skybarb_composition

	MESSAGE cast(now() as timestamp)||' | @ M018.1: Household compositions of SkyBarb completed!!! rows:'||@@rowcount TO CLIENT
	---------------------------------------------------------------------------------------------------------------------
	---------------------------------------------------------------------------------------------------------------------
	---------------------------------------------------------------------------------------------------------------------
	MESSAGE cast(now() as timestamp)||' | @ M018.2: Create household compositions FROM H08 individuals ' TO CLIENT

	SELECT house_id as account_number
		,max(field1)||','||max(field2)||','||max(field3)||','||max(field4)||','||max(field5)||','||max(field6)||','||max(field7) ||','||max(field8) as composition
	into  #H08_composition
	FROM(SELECT *	
				,dense_rank() over (partition by list.house_id order by hhsize, comp1)  as ranking
				,CASE WHEN dense_rank() over (partition by list.house_id order by hhsize, comp1) = hhsize then 1
					else 0 end as lastval
				,CASE WHEN dense_rank() over (partition by list.house_id order by hhsize, comp1) = 1 then hhsize||':'||comp1
					else '' end as field1
				,CASE WHEN dense_rank() over (partition by list.house_id order by hhsize, comp1) = 2 then comp1
					else '' end as field2
				,CASE WHEN dense_rank() over (partition by list.house_id order by hhsize, comp1) = 3 then comp1
					else '' end as field3
				,CASE WHEN dense_rank() over (partition by list.house_id order by hhsize, comp1) = 4 then comp1
					else '' end as field4
				,CASE WHEN dense_rank() over (partition by list.house_id order by hhsize, comp1) = 5 then comp1
					else '' end as field5
				,CASE WHEN dense_rank() over (partition by list.house_id order by hhsize, comp1) = 6 then comp1
					else '' end as field6
				,CASE WHEN dense_rank() over (partition by list.house_id order by hhsize, comp1) = 7 then comp1
					else '' end as field7
				,CASE WHEN dense_rank() over (partition by list.house_id order by hhsize, comp1) = 8 then comp1
					else '' end as field8
		FROM (SELECT comp.house_id, comp.hhsize, sexage||'*'||cnt as comp1 
				FROM (SELECT house_id, hhsize, sexage, count(*) as cnt
						FROM (SELECT sb.account_number as house_id, household_size as hhsize,  person_gender||person_ageband as   sexage
								FROM   V289_M08_SKY_HH_composition sb
								where panel_flag=1 and hhsize<=8
									-- currently code will exclude households bigger than 8 - will be a problem matching them to barb so we will have to decide what to do here
							--	order by sb.account_number, household_size, sexage
							) AS x
						group by house_id, hhsize, sexage 
					) AS comp
			--order by comp.house_id, comp.hhsize, comp1
			) AS list 
		) AS composition
	group by house_id
	commit

	--SELECT count( * ) FROM  h08_composition where composition= '1:M45-64*1,,,,,,,'

	MESSAGE cast(now() as timestamp)||' | @ M018.2: Creation of household compositions FROM H08 individuals complete' TO CLIENT
	---- first create list of accounts and information needed to generate compositions
	MESSAGE cast(now() as timestamp)||' | Begining M18.3 - Matching Panel' TO CLIENT
	
	SELECT  h08c.account_number
			,row_number() over (order by h08c.account_number) as row_id
			,cast(1 as float)        as random
			,h08c.composition as composition
			,sbc.required_h08_count
	into	 #aclist
	FROM	#H08_composition h08c 
	left join  #skybarb_composition AS sbc ON h08c.composition=sbc.composition

	commit
	--SELECT top 100 * FROM  #aclist where composition= '1:M45-64*1,,,,,,,'
	-- add a random number
	update   #aclist
	set     random  = cast(rand(cast(row_id as float)+datepart(us, getdate())) as float)
	commit

	--SELECT top 100 * FROM  #aclist where composition= '1:M45-64*1,,,,,,,'
	-- rank the random numbers
	SELECT *,rank() over( partition by composition order by random) as ranknum
	into #aclist2
	FROM #aclist
	commit
	--SELECT * FROM  #aclist where composition= '1:M45-64*1,,,,,,,' 
	-- SELECT by ranking

	SELECT  distinct account_number
	into #sample
	FROM     #aclist2
	where   ranknum <=   required_h08_count
	 --   and account_number = '200000873691'
	--SELECT top 100 * FROM #sample
	--SELECT top 5 * FROM V289_M08_SKY_HH_composition;
	SELECT hhc.* 
	into test_table
	FROM V289_M08_SKY_HH_composition hhc
	inner join  #sample s1 on s1.account_number =hhc.account_number
	commit
				
	UPDATE V289_M08_SKY_HH_composition
	SET PANEL_FLAG = 0
	where   account_number not in   ( SELECT  distinct account_number FROM #sample)

	UPDATE V289_M08_SKY_HH_composition
	SET 	randd = 2 --- Temp indicator to track the changes
	where   account_number in   ( SELECT  distinct account_number FROM #sample)
													
									
	MESSAGE cast(now() as timestamp)||' | @ M18.3: Matching Panel DONE!!  Rows:'|| @@rowcount TO CLIENT
	
	commit

	---- temp table tidy
	drop table  #skybarb_composition
	drop table  #H08_composition
	drop table  #aclist
	drop table  #sample
	drop table  #aclist2
END; -- END OF PROCEDURE
--------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------
commit;
grant execute on v289_m18_panel_matching to vespa_group_low_security;
commit;
