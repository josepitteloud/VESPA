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
		M08.0 - Initialising environment
		M08.1 - Snapshoting current Active UK Customers
        M08.2 - Deriving Features of Active UK Customers
				--	Skygo_subs
				--	Anytime_plus_subs
				--	box_type_subs
				--	HD_box_subs
				--	RTM
				--	prem_sports
				--prem_movies
        M08.2 - QAing results
        M08.3 - Returning results
		
**Stats:

	-- running time: 20 min approx...

--------------------------------------------------------------------------------------------------------------
*/

-----------------------------------
-- M08.0 - Initialising environment
-----------------------------------

create or replace procedure sig_masvg_m08_account_base
as begin

	MESSAGE cast(now() as timestamp)||' | Beginig M08.0 - Initialising environment' TO CLIENT
	
	-- Local Variables...
	declare @profiling_day date
	
	select	@profiling_day = max(cb_data_date) from sk_prod.cust_single_account_view
    
    commit

	MESSAGE cast(now() as timestamp)||' | @ M08.0: Initialisation DONE' TO CLIENT
	
--------------------------------------------------
-- M08.1 - Snapshoting current Active UK Customers
--------------------------------------------------
    
	MESSAGE cast(now() as timestamp)||' | Beginig M08.1 - Snapshoting current Active UK Customers' TO CLIENT
	
	select  distinct		
			account_number
			,cust_viewing_data_capture_allowed  as viewing_consent
			,cb_key_individual 				    as cb_key_individual
			,CUST_ACTIVE_DTV					as cust_active_dtv
			,pty_country_code					as UK_Standard_account
			,PROD_DTV_ACTIVATION_DT			    as cust_active_dt
	into    #temp_shelf
	from    (	
				-- Applying a rank based on recent activation date
				-- to shield up against potential duplicates...
				select  *
						,rank() over (
										partition by    account_number
										order by        prod_dtv_activation_dt desc
									)   as ranking
				from    (vespa_analysts.sig_toolbox_03_ActiveUKCust())
			)   as deduping
	where   ranking = 1            
	
	update	#temp_shelf as bas
	set 	viewing_consent = 'N'
	from 	vespa_analysts.ConsentIssue_05_Revised_Consent_Info as exc
	where 	bas.account_number = exc.account_number
	
	commit
	
	truncate table m08_t1_account_base_stage0
	commit
	
	insert	into m08_t1_account_base_stage0	(
												account_number
												,viewing_consent_flag
												,cb_key_individual
												,cust_active_dtv
												,UK_Standard_account
												,cust_active_dt
											)
	select	*
	from	#temp_shelf
	
	commit
	drop table #temp_shelf
	commit
	MESSAGE cast(now() as timestamp)||' | @ M08.1: Extracting Active UK Base DONE' TO CLIENT
	
	-- Extracting History of Active UK Customers...
	select  csh.account_number
			,csh.subscription_sub_type
			,csh.status_code
			,csh.effective_from_dt
			,csh.effective_to_dt
			,csh.subscription_type
			,csh.current_short_description
			--,csh.service_instance_id -- Don't think we're gonna need this tbh...
	into    #cshcompact
	from    sk_prod.cust_subs_hist as csh
			inner join m08_t1_account_base_stage0 as m08
			on  csh.account_number = m08.account_number
	where 	csh.subscription_sub_type IN ('DTV Primary Viewing','DTV Sky+', 'DTV Extra Subscription','DTV HD','PDL subscriptions') 
	and    	csh.status_code IN  ('AC','AB','PC')
	and     csh.effective_FROM_dt <> csh.effective_to_dt
	and   	@profiling_day between csh.effective_from_dt and csh.effective_to_dt
	
	commit
	create hg 	index fake_1 on #cshcompact(account_number)
	create lf 	index fake_2 on #cshcompact(subscription_sub_type)
	create lf 	index fake_3 on #cshcompact(status_code)
	create date index fake_4 on #cshcompact(effective_from_dt)
	create date index fake_5 on #cshcompact(effective_to_dt)
	create lf 	index fake_6 on #cshcompact(subscription_type)
	create lf 	index fake_7 on #cshcompact(current_short_description)
	commit
	MESSAGE cast(now() as timestamp)||' | @ M08.1: Compacting Active UK Base History DONE' TO CLIENT
	
---------------------------------------------------		
-- M08.2 - Deriving Features of Active UK Customers
---------------------------------------------------

	MESSAGE cast(now() as timestamp)||' | Beginig M08.2 - Deriving Features of Active UK Customers' TO CLIENT
	
	--Skygo_subs
    
    update  m08_t1_account_base_stage0  as m08
    set     skygo_subs = case when adsmart.sky_go_reg = 'Yes' then 1 else 0 end
    from    sk_prod.adsmart  as adsmart
    where   m08.account_number = adsmart.account_number

	commit
	MESSAGE cast(now() as timestamp)||' | @ M08.1: Deriving Sky Go Flag DONE' TO CLIENT
	
	--Anytime_plus_subs
	
	update 	m08_t1_account_base_stage0	as m08
    set 	Anytime_plus_subs = 1
    from 	#cshcompact    	as csh
	where 	m08.account_number = csh.account_number
	and 	csh.subscription_sub_type = 'PDL subscriptions'
	and    	csh.status_code = 'AC'
	
	commit
	MESSAGE cast(now() as timestamp)||' | @ M08.1: Deriving Anytime+ Flag DONE' TO CLIENT
	
	--box_type_subs
	
	select	* 
	into 	#stb_active 
    from	(
				select	account_number
						,service_instance_id
						,active_box_flag
						,box_installed_dt
						,box_replaced_dt
						,x_pvr_type
						,x_anytime_enabled
						,current_product_description
						,x_anytime_plus_enabled
						,x_box_type
						,CASE WHEN x_description like '%HD%2TB%' THEN 1 ELSE 0 END AS HD2TB
						,CASE WHEN x_description like '%HD%1TB%' THEN 1 ELSE 0 END AS HD1TB
						,CASE WHEN x_description like '%HD%'     THEN 1 ELSE 0 END AS HD
						,x_manufacturer
						,x_description
						,x_model_number
						,rank () over (partition by service_instance_id order by ph_non_subs_link_sk desc) as active_flag
				from 	sk_prod.cust_set_top_box
			) 	as t
	where 	active_flag = 1

    commit
    create index #stb_active_accnum on #stb_active(account_number)
    create index #stb_active_siid   on #stb_active(service_instance_id)
	commit
	
    --Creates a list of accounts with active HD capable boxes
    SELECT	stb.account_number
            ,max(HD) AS HD
            ,max(HD1TB) AS HD1TB
            ,max(HD2TB) as HD2TB
    INTO 	#hda
    FROM 	#stb_active AS stb
            INNER JOIN m08_t1_account_base_stage0 AS m08 
			on	stb.account_number = m08.account_number
    GROUP 	BY	stb.account_number
	
	commit
	create hg index fake_hg1 on #hda(account_number)
	commit
	
	SELECT  csh.account_number
            ,max(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV Primary Viewing'    THEN 1 ELSE 0  END) AS TV
            ,max(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV Sky+'               THEN 1 ELSE 0  END) AS SP
            ,max(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV Extra Subscription' THEN 1 ELSE 0  END) AS MR
            ,max(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV HD'                 THEN 1 ELSE 0  END) AS HD
            ,max(CASE  WHEN #hda.HD = 1                                         THEN 1 ELSE 0  END) AS HDstb
            ,max(CASE  WHEN #hda.HD1TB = 1                                      THEN 1 ELSE 0  END) AS HD1TBstb
            ,max(CASE  WHEN #hda.HD2TB = 1            THEN 1 ELSE 0  END) AS HD2TBstb
            ,convert(varchar(30), null) as box_type
    INTO 	#box_type
    FROM 	#cshcompact 							        AS csh
			LEFT OUTER JOIN sk_prod.cust_entitlement_lookup	as cel
			ON	csh.current_short_description = cel.short_description
			LEFT OUTER JOIN #hda 
			ON	csh.account_number = #hda.account_number 
	WHERE 	csh.SUBSCRIPTION_SUB_TYPE IN ('DTV Primary Viewing','DTV Sky+', 'DTV Extra Subscription','DTV HD' )
	GROUP 	BY	csh.account_number
	HAVING 	TV = 1

    commit
    create unique index maybe_fake_pk on #box_type(account_number)
    commit

    update	#box_type
    set 	box_type =  CASE    WHEN HD =1 AND MR = 1 AND HD2TBstb = 1      THEN 'A) HD Combi 2TB'
								WHEN HD =1 AND HD2TBstb = 1                 THEN 'B) HD 2TB'
								WHEN HD =1 AND MR = 1 AND HD1TBstb = 1      THEN 'A) HD Combi 1TB'
								WHEN HD =1 AND HD1TBstb = 1                 THEN 'B) HD 1TB'
								WHEN HD =1 AND MR = 1 AND HDstb = 1         THEN 'A) HD Combi'
								WHEN HD =1 AND HDstb = 1                    THEN 'B) HD'
								WHEN SP =1 AND MR = 1 AND HD2TBstb = 1      THEN 'C) HDx Combi 2TB'
								WHEN SP =1 AND HD2TBstb = 1                 THEN 'D) HDx 2TB'
								WHEN SP =1 AND MR = 1 AND HD1TBstb = 1      THEN 'C) HDx Combi 1TB'
								WHEN SP =1 AND HD1TBstb = 1                 THEN 'D) HDx 1TB'
								WHEN SP =1 AND MR = 1 AND HDstb = 1         THEN 'C) HDx Combi'
								WHEN SP =1 AND HDstb = 1                    THEN 'D) HDx'
								WHEN SP =1 AND MR = 1                       THEN 'E) SkyPlus Combi'
								WHEN SP =1                                  THEN 'F) SkyPlus '
								WHEN MR =1                                  THEN 'G) Multiroom'
								ELSE                                        'H) FDB'
						END
    
    commit

    UPDATE	m08_t1_account_base_stage0
	SET 	box_type_subs = coalesce(bt.box_type, 'Unknown')
	from 	m08_t1_account_base_stage0 	as m08
			left join #box_type     as bt 
			on	m08.account_number = bt.account_number

	commit
	drop table #hda
	drop table #box_type
	drop table #stb_active
	commit
	MESSAGE cast(now() as timestamp)||' | @ M08.1: Deriving Box Type subs DONE' TO CLIENT
	
	--HD_box_subs
	
	update	m08_t1_account_base_stage0
    set  	HD_box_subs = 1
    where 	account_number in	(
									select	distinct 
											m08.account_number
									from  	m08_t1_account_base_stage0  as m08
											inner join #cshcompact	    as csh
											on	m08.account_number = csh.account_number
									where  	csh.subscription_sub_type = 'DTV HD' 
								)
								
								
	commit
	MESSAGE cast(now() as timestamp)||' | @ M08.1: Deriving HD sub DONE' TO CLIENT
	
	--RTM
	
	select	distinct
			account_number
            ,case 	when	(
								RTM not in (
												 'Direct Internet'
												,'Direct Internet Telephony'
												,'Direct Telephone'
												,'Events'
												,'Existing Customer Sales'
												,'Retail Independent'
												,'Retail Multiple'
												,'Sky Homes'
												,'Sky Retail Stores'
												,'Tesco'
												,'Walkers Cobra'
												,'Walkers North'
											)
								or RTM is null
							) 
					then	'Other' 
					else 	RTM 
			end 	as fix_rtm
	into	#temp_shelf
    from	(
				SELECT	base.account_number
						,RANK() OVER	(
											PARTITION BY	ord.account_number
											ORDER BY 		ord.cb_row_id ASC
										) 	AS rank
						,case	WHEN ord.currency_code = 'EUR' AND ord.route_to_market LIKE '%Direct%'                                    THEN 'ROI Direct'
								WHEN ord.currency_code = 'EUR'                                                                            THEN 'ROI Retail'
								WHEN ( ord.retailer_ASA_GROUP_NUMBER ) IN ('11164','11167') AND ord.retailer_ASA_BRANCH_NUMBER LIKE '8%'  THEN 'Tesco'
								WHEN ( ord.retailer_ASA_GROUP_NUMBER ) IN ('43000')                                                       THEN 'Events'
								WHEN ord.retailer_asa_group_number IN ('42000','48000')                                                   THEN 'Walkers North'
								WHEN ord.route_to_market LIKE '%Walkers%' OR ord.retailer_asa_group_number IN ('45000')                   THEN 'Walkers Cobra'
								WHEN ord.ROUTE_TO_MARKET = 'Direct'                                                                       THEN 'Direct Telephone'
								WHEN ord.route_to_market IN ('Direct Internet','Online')                                                  THEN 'Direct Internet'
								ELSE ord.route_to_market 
						END 	AS RTM
                FROM 	m08_t1_account_base_stage0    		AS base
						LEFT JOIN sk_prod.CUST_ORDER_DETAIL AS ord 
						ON	base.account_number = ord.account_number
                where 	base.cust_active_dt <= @profiling_day
            ) 	as base
    where 	rank = 1

	update	m08_t1_account_base_stage0	as m08
	set		rtm = shelf.fix_rtm
	from	#temp_shelf	as shelf
	where	m08.account_number = shelf.account_number
	
	commit
	drop table #temp_shelf
	commit
	
	MESSAGE cast(now() as timestamp)||' | @ M08.1: Deriving RTMs DONE' TO CLIENT
	
	--prem_sports
	--prem_movies
	
	select	csh.account_number
			,min(cel.prem_sports) prem_sports -- this is a nasty fix
			,min(cel.prem_movies) prem_movies -- need to notify this issue in the CSH table...
    into 	#premiums_lookup 
    from  	#cshcompact 						as csh
			inner join sk_prod.cust_entitlement_lookup 	as cel
			on	csh.current_short_description = cel.short_description
	WHERE	csh.subscription_sub_type ='DTV Primary Viewing'
	AND     csh.subscription_type = 'DTV PACKAGE'
	group 	by	csh.account_number
	
	update	m08_t1_account_base_stage0	as m08
	set		prem_sports		= prems.prem_sports
			,prem_movies	= prems.prem_movies
	from	#premiums_lookup			as prems
	where	m08.account_number = prems.account_number
	
	commit
	drop table #premiums_lookup
	commit
	
	MESSAGE cast(now() as timestamp)||' | @ M08.1: Deriving Premiums for Sports DONE' TO CLIENT
	MESSAGE cast(now() as timestamp)||' | @ M08.1: Deriving Premiums for Movies DONE' TO CLIENT
	
	
------------------------
-- M08.2 - QAing results
------------------------

----------------------------
-- M08.3 - Returning results
----------------------------
	
	drop table #cshcompact
	commit
	
-- m08_t1_account_base_stage0

	MESSAGE cast(now() as timestamp)||' | M08 Finished, table m08_t1_account_base_stage0 BUILT' TO CLIENT

    commit



end;

commit;
grant execute on sig_masvg_m08_account_base to vespa_group_low_security;
commit;