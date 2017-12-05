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
**Lead(s):                              Jose Loureda
**Stakeholder:                          Operational Reports / SIG
**Due Date:                             20/09/2013
**Project Code (Insight Collation):     
**Sharepoint Folder:                    
                                                                        
**Business Brief:

**Modules:

M06: MASVG Panel Balance
        M06.0 - Initialising environment
		M06.1 - Snapshoting current non scaling segment sample
        M06.2 - Deriving Metrics for Panel Balance
				-- scaling_segment_id
				-- non_scaling_segment_id
				-- weight
				-- viq_weight
				-- weight_dt
        M06.3 - QAing results
        M06.4 - Returning results

**Stats:

	-- running time: 8 min approx...
	
--------------------------------------------------------------------------------------------------------------
*/


-----------------------------------
-- M06.0 - Initialising environment
-----------------------------------

create or replace procedure sig_masvg_m06_panel_balance
as begin


    MESSAGE cast(now() as timestamp)||' | Beginig M06.0 - Initialising environment' TO CLIENT
	
	-- local variables...
	
	declare @profiling_thursday date

	
	/*
		below procedure is supposed to bring the previous Saturday, then we will
		subtract 2 days to get the previous Thursday...
	*/
	execute vespa_analysts.Regulars_Get_report_end_date @profiling_thursday output 
	
	set @profiling_thursday = @profiling_thursday - 2

	--  As this module is fully related to the Scaling Excercise
	--	we need to sample here the accounst used for such on the most updated
	--	batch processed...

	select	account_number,scaling_segment_id
	into 	#Scaling_weekly_sample
	from 	vespa_analysts.SC2_Sky_base_segment_snapshots
	where 	profiling_date =	(
									select	max(profiling_date)
									from 	vespa_analysts.SC2_Sky_base_segment_snapshots
								)

	commit
	create unique index fake_key on #scaling_weekly_sample(account_number)
	commit
    MESSAGE cast(now() as timestamp)||' | @ M06.0: Initialisation DONE' TO CLIENT

---------------------------------------------------------
-- M06.1 - Snapshoting current non scaling segment sample
---------------------------------------------------------

    MESSAGE cast(now() as timestamp)||' | Beginig M06.1 - Snapshoting current non scaling segment sample' TO CLIENT

	-- again, what are the accounts on latest Scaling batch we should consider...
	truncate table sig_current_non_scaling_segments
	commit
	
	insert	into sig_current_non_scaling_segments	(
														account_number
													)
    select 	distinct(account_number)
    from 	#Scaling_weekly_sample
	
    MESSAGE cast(now() as timestamp)||' | @ M06.1: Sampling Accoutns DONE' TO CLIENT

	-- Deriving: VALUE SEGMENTS 	Non Scaling Variable
	
    update	sig_current_non_scaling_segments	as segments
    set 	value_segment = coalesce(vsd.value_seg, 'Bedding In')
    from 	sk_prod.VALUE_SEGMENTS_DATA			as vsd 
	where	segments.account_number = vsd.account_number
			
    commit
    MESSAGE cast(now() as timestamp)||' | @ M06.1: Deriving Value Segments DONE' TO CLIENT

	----------------------------------------------------	
	
	-- Deriving: CONSUMER VIEW 		Non Scaling Variable
	-- Deriving: FINANCIAL STRATEGY Non Scaling Variable
	-- Deriving: MOSAIC 			Non Scaling Variable
	
	--	As SAV holds the cbKeys, we want to get all those from accounts matching our sample...
    select  sig.account_number
            ,min(sav.cb_key_individual) as cb_key_individual
	into	#active_uk_ac_lookup
    from 	sk_prod.cust_single_account_view as sav
            inner join sig_current_non_scaling_segments as sig
            on  sav.account_number = sig.account_number
	where   sav.pty_country_code = 'GBR'
    group   by  sig.account_number
	
	commit
	create unique index fake_pk on #active_uk_ac_lookup(account_number)
	commit
    MESSAGE cast(now() as timestamp)||' | @ M06.1: Extracting CB Key Individuals from SAV DONE' TO CLIENT

	/*
	!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	NOTE:	COMMENTED DUE TO BELOW BUG FOUND ON SYBASE 15.2
	!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	
	--	with the CbKeys we can derive the fields we want from Experian...
    select	exp_nodupes.account_number
            ,expe.cb_key_individual
			,expe.cb_row_id 						as consumerview_cb_row_id
			,coalesce(expe.h_mosaic_uk_group, 'U')	as MOSAIC_segment
            ,coalesce(expe.h_fss_group, 'U')        as Financial_strategy_segment
    into	#consumerview_lookup
	from 	sk_prod.experian_consumerview	as expe
            inner join  (	--	because Experian has duplicates we need to go as below...
                            select  lookup.account_number
                                    ,A.cb_key_individual
                                    ,min(A.cb_row_id)     as cb_row_id
                            from    sk_prod.experian_consumerview   as A
                                    inner join #active_uk_ac_lookup	as lookup
						                        on	A.cb_key_individual = lookup.cb_key_individual
                            group   by  lookup.account_number
                                        ,A.cb_key_individual
                        )   as exp_nodupes
            on  expe.cb_row_id = exp_nodupes.cb_row_id
	*/
	
	select  lookup.account_number
			,A.cb_key_individual
			,min(A.cb_row_id)     as consumerview_cb_row_id
	into	#consumerview_lookup
	from    sk_prod.experian_consumerview   as A
			inner join #active_uk_ac_lookup	as lookup
						on	A.cb_key_individual = lookup.cb_key_individual
	group   by  lookup.account_number
				,A.cb_key_individual
				
    commit
	
    create unique index fake_pk on  #consumerview_lookup(account_number)
    create hg index fake_hg on 	    #consumerview_lookup(cb_key_individual)
	create hg index fake_hg2 on     #consumerview_lookup(consumerview_cb_row_id)
	
	drop table #active_uk_ac_lookup
	
    commit
    MESSAGE cast(now() as timestamp)||' | @ M06.1: Getting Relevant Rows IDs from Experian per Account DONE' TO CLIENT

	/*
	!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	NOTE:	IN THE WEIRD WORLD OF SYBASE, BELOW UPDATE IS NOT VALID... WILL CRASH THE DB
			WORK AROUND THAT AS ON FOLLOWING LINES...
	!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	
     -- OK, now get those fields onto the segmentation table...
    update	sig_current_non_scaling_segments	as scnss
    set 	consumerview_cb_row_id 		= cl.consumerview_cb_row_id
			,MOSAIC_segment				= cl.MOSAIC_segment
			,Financial_strategy_segment	= cl.Financial_strategy_segment
    from 	#consumerview_lookup  				as cl
	where	scnss.account_number = cl.account_number
	
	*/
	--	So to go with the work around, lets at least get the row ids per accounts...
	update	sig_current_non_scaling_segments	as scnss
	set		consumerview_cb_row_id = cl.consumerview_cb_row_id
	from	#consumerview_lookup				as cl
	where	cl.account_number = scnss.account_number
	
	commit
	drop table #consumerview_lookup
	commit
	MESSAGE cast(now() as timestamp)||' | @ M06.1: Storing Rows IDs DONE' TO CLIENT

	--	now, weirdly enough (0) creating a mirror of the non scaling segments table
	--	and (1) filling it in by joining the tables we need and (2) then putting the records
	--	back to the original table seems to work...
	
	-- (0) Mirroring Non Scaling Segments Table...
	
	if object_id('vespa_analysts.weird_bug_fixing') is not null
		drop table weird_bug_fixing
		
	commit
	
	create table weird_bug_fixing	(
	
		account_number       		varchar(20) primary key
		,non_scaling_segment_id     int
		,value_segment              varchar(10)
		,consumerview_cb_row_id     bigint
		,MOSAIC_segment             varchar(1)
		,Financial_strategy_segment	varchar(1)
		,is_OnNet                   bit         default 0
		,uses_sky_go                bit         default 0
		
     )
	
	commit
	
	--	(1) Filling it in by joining the tables we need and getting the derivations out...
	insert	into weird_bug_fixing	(
										account_number
										,non_scaling_segment_id
										,value_segment
										,consumerview_cb_row_id
										,MOSAIC_segment
										,Financial_strategy_segment
										,is_OnNet
										,uses_sky_go
									)
	select	scnss.account_number
            ,scnss.non_scaling_segment_id
            ,scnss.value_segment
            ,scnss.consumerview_cb_row_id
            ,coalesce(expe.h_mosaic_uk_group, 'U') as MOSAIC_segment
            ,coalesce(expe.h_fss_group, 'U')          as Financial_strategy_segment
            ,scnss.is_OnNet
            ,scnss.uses_sky_go
	from	sig_current_non_scaling_segments			as scnss
			left join sk_prod.experian_consumerview	as expe
			on	scnss.consumerview_cb_row_id = expe.cb_row_id
	
    commit
    create index for_updating   on weird_bug_fixing (consumerview_cb_row_id)
	commit
    MESSAGE cast(now() as timestamp)||' | @ M06.1: Initialising Bug Fixture DONE' TO CLIENT    

	truncate table sig_current_non_scaling_segments
	MESSAGE cast(now() as timestamp)||' | @ M06.1: Trunkating Non Scaling Segment table due to Bug DONE' TO CLIENT
	
	--	(2) Putting the records back from the mirror table into the original one (F**ing weird)...
	insert into sig_current_non_scaling_segments	(
														account_number
														,non_scaling_segment_id
														,value_segment
														,consumerview_cb_row_id
														,MOSAIC_segment
														,Financial_strategy_segment
														,is_OnNet
														,uses_sky_go
													)
	select 	account_number
			,non_scaling_segment_id
			,value_segment
			,consumerview_cb_row_id
			,MOSAIC_segment
			,Financial_strategy_segment
			,is_OnNet
			,uses_sky_go
	from 	weird_bug_fixing
	
    commit
    drop table weird_bug_fixing
    commit
	MESSAGE cast(now() as timestamp)||' | @ M06.1: Deriving Consumer View, MOSAIC, Financial Strategy DONE' TO CLIENT

	----------------------------------------------------
	
	-- Deriving: SKY GO 			Non Scaling Variable
	
	/*
	-- Finally (for now) the Sky Go use marks
    select	distinct account_number
    into 	#skygousers
    from 	sk_prod.SKY_PLAYER_USAGE_DETAIL
    where 	activity_dt >= '2011-08-18'

    commit
    create unique index fakle_pk on #skygousers(account_number)
    commit

    update	sig_current_non_scaling_segments
    set 	uses_sky_go = 1
    from 	sig_current_non_scaling_segments	as segments
			inner join #skygousers as sgu 
			on segments.account_number = sgu.account_number
	*/		
	update	sig_current_non_scaling_segments	as nons
	set		uses_sky_go = 1
	from	m08_t1_account_base_stage0			as m08
    where	m08.account_number = nons.account_number
	and		m08.Skygo_subs = 1
	
    commit -- 4241198 row(s) updated
    /*
	drop table #skygousers
    commit
	*/
    MESSAGE cast(now() as timestamp)||' | @ M06.1: Deriving Sky Go DONE' TO CLIENT

	----------------------------------------------------
	
	-- Deriving: ONNET FLAG 		Non Scaling Variable
	
	-- The OnNet goes by postcode...
	
    select	scnss.account_number
            ,min(sav.cb_address_postcode)   as postcode
            ,convert(bit, 0)                as onnet
    into 	#onnet_patch
    from 	sig_current_non_scaling_segments    as scnss
            inner join  (
                            select  account_number
                                    ,cb_address_postcode
                            from    sk_prod.cust_single_account_view 
                            where 	cust_active_dtv = 1 -- OK, so we're getting account number duplicates, that's annoying...
                            and		pty_country_code = 'GBR'
                        )   as sav 
			on	sav.account_number = scnss.account_number
    group 	by	scnss.account_number -- If there are account_number duplicates, they're postcodes for an active account, so whatever...
    
    update  #onnet_patch
    set     postcode = upper(REPLACE(postcode,' ',''))
    
    commit
    create unique index fake_pk on #onnet_patch (account_number)
    create index joinsy on #onnet_patch (postcode)
    commit
    MESSAGE cast(now() as timestamp)||' | @ M06.1: Extracting Pcode from SAV DONE' TO CLIENT

    -- 1) Get BROADBAND_POSTCODE_EXCHANGE postcodes...

    SELECT	cb_address_postcode as postcode
			,MAX(mdfcode) 								as exchID
    INTO 	#bpe
    FROM 	sk_prod.BROADBAND_POSTCODE_EXCHANGE
    GROUP 	BY	postcode

    update  #bpe
    set     postcode = upper(REPLACE( postcode,' ',''))

    commit
    create unique index fake_pk on #bpe(postcode)
    commit
    MESSAGE cast(now() as timestamp)||' | @ M06.1: Extracting Pcode from Pcode Exchange DONE' TO CLIENT

    -- 2) Get BB_POSTCODE_TO_EXCHANGE postcodes...
	 
    SELECT	postcode
			,MAX(exchange_id) 					as exchID
    INTO 	#p2e
    FROM 	sk_prod.BB_POSTCODE_TO_EXCHANGE
    GROUP 	BY	postcode

    update  #p2e
    set     postcode = upper(REPLACE( postcode,' ',''))

    commit
    create unique index fake_pk on #p2e (postcode)
    commit
    MESSAGE cast(now() as timestamp)||' | @ M06.1: Extracting Pcode from BB Pcode DONE' TO CLIENT

    -- 3) Combine postcode lists taking BB_POSTCODE_TO_EXCHANGE exchange_id's where possible

    SELECT	COALESCE(#p2e.postcode, #bpe.postcode)	AS postcode
			,COALESCE(#p2e.exchID, #bpe.exchID) 	as exchange_id
			,'OFFNET' as exchange
    INTO 	#onnet_lookup
    FROM 	#bpe FULL JOIN #p2e 
			ON	#bpe.postcode = #p2e.postcode

    commit
    create unique index fake_pk on #onnet_lookup (postcode)
    commit

    -- 4) Update with latest Easynet exchange information

    UPDATE	#onnet_lookup					as base
    SET 	exchange = 'ONNET'
    FROM 	sk_prod.easynet_rollout_data 	as easy 
	where	base.exchange_id = easy.exchange_id
    and 	easy.exchange_status = 'ONNET'

	-- 5) Flag your base table with onnet exchange data. Note that this uses a postcode field with
	--   spaces removed so your table will either need to have a similar filed or use a REPLACE
	--   function in the join

    UPDATE	#onnet_patch	as base
    SET 	onnet = CASE WHEN tgt.exchange = 'ONNET'
                         THEN 1
                         ELSE 0
            END
    FROM 	#onnet_lookup	AS tgt 
	where	base.postcode = tgt.postcode
    
	commit
    MESSAGE cast(now() as timestamp)||' | @ M06.1: Constructing Pcode Lookup DONE' TO CLIENT

    update	sig_current_non_scaling_segments	as scnss
    set 	is_OnNet = op.onnet
    from 	#onnet_patch 						as op 
	where	scnss.account_number = op.account_number

    commit

    -- Clear out all those tables that got sprayed about the place:
    drop table #onnet_patch
    drop table #onnet_lookup
    drop table #p2e
    drop table #bpe
    commit
    MESSAGE cast(now() as timestamp)||' | @ M06.1: Deriving OnNet Flag DONE' TO CLIENT

	-------------------------------------------------------------------------------
	
	-- Now what is pending to do is to check how belongs to what non scaling segment...
	update	sig_current_non_scaling_segments	as scnss
    set		non_scaling_segment_id = snssl.non_scaling_segment_id
    from 	sig_non_scaling_segments_lookup 	as snssl 
	where	scnss.value_segment					= snssl.value_segment
	and 	scnss.MOSAIC_segment             	= snssl.MOSAIC_segment
	and 	scnss.Financial_strategy_segment	= snssl.Financial_strategy_segment
	and 	scnss.is_OnNet                   	= snssl.is_OnNet
	and 	scnss.uses_sky_go                	= snssl.uses_sky_go
			
	commit -- 7183647 row(s) updated
    MESSAGE cast(now() as timestamp)||' | @ M06.1: Integrating Accounts Sample to Non Scaling Segments DONE' TO CLIENT
	
---------------------------------------------
-- M06.2 - Deriving Metrics for Panel Balance
---------------------------------------------

    MESSAGE cast(now() as timestamp)||' | Beginig M06.2 - Deriving Metrics for Panel Balance' TO CLIENT

-- scaling_segment_id
	truncate table m06_t1_panel_balance_stage0
	commit

	insert	into m06_t1_panel_balance_stage0	(
													account_number
													,scaling_segment_id
												)
	select	distinct
			account_number
			,scaling_segment_id
	from	#Scaling_weekly_sample
	
	commit
	drop table #Scaling_weekly_sample
	commit
	MESSAGE cast(now() as timestamp)||' | @ M06.2: Deriving Scaling Segment ID DONE' TO CLIENT

-- non_scaling_segment_id

	update	m06_t1_panel_balance_stage0			as m06
	set		non_scaling_segment_id	= scnss.non_scaling_segment_id
	from	sig_current_non_scaling_segments	as scnss
	where	m06.account_number = scnss.account_number
			
	commit
    MESSAGE cast(now() as timestamp)||' | @ M06.2: Deriving Non Scaling Segment ID DONE' TO CLIENT
	
-- weight
-- weight_dt

	update	m06_t1_panel_balance_stage0	as m06
	set		weight		    = weights.weighting
			,weight_date    = weights.scaling_day
	from	(		
				/*
					Getting the sample of accounts scaled on the given Thursday
					and their weights assigned...
				*/
				select  inter.account_number
						,inter.scaling_segment_id
						,weight.weighting
						,weight.scaling_day
				from    vespa_analysts.SC2_Intervals    as inter
						inner join  (
										-- Getting the weights for the given Thursday
										select  *
										from    vespa_analysts.sc2_weightings
										where   scaling_day = @profiling_thursday
									)   as weight
						on  inter.scaling_segment_id = weight.scaling_segment_id
				where   @profiling_thursday	between inter.reporting_starts and inter.reporting_ends
			)	as weights
	where	m06.scaling_segment_id 	= weights.scaling_segment_id
	and		m06.account_number		= weights.account_number
	
	commit
	
-- viq_weight (extracted for the same date as above)

	update	m06_t1_panel_balance_stage0			as m06
	set		viq_weight = viq.calculated_scaling_weight
	from	sk_prod.VIQ_VIEWING_DATA_SCALING	as viq
	where	viq.account_number	= m06.account_number
	and   	viq.adjusted_event_start_date_vespa = @profiling_thursday
	
	commit		
	
	MESSAGE cast(now() as timestamp)||' | @ M06.2: Deriving Weights Values and Dates DONE' TO CLIENT

------------------------
-- M06.3 - QAing results
------------------------

----------------------------
-- M06.4 - Returning results
----------------------------

-- m06_t1_panel_balance_stage0...
    MESSAGE cast(now() as timestamp)||' | M06 Finished, table m06_t1_panel_balance_stage0 BUILT' TO CLIENT

    commit



end;

commit;
grant execute on sig_masvg_m06_panel_balance to vespa_group_low_security;
commit;