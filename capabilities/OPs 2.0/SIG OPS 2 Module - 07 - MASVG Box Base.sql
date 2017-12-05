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
**Project Name:                                                 OPS 2.0
**Analysts:                             Berwyn Cort (berwyn.cort@skyiq.co.uk) Angel Donnarumma (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              Jose Loureda
**Stakeholder:                          Operational Reports / SIG
**Due Date:                             20/09/2013
**Project Code (Insight Collation):
**Sharepoint Folder:

**Business Brief:

**Modules:
        M07.0 - Initialising environment
        M07.1 - Snapshot of current active UK boxes
        M07.2 - Deriving Features of active UK boxes
                -- Add Box_is_3D
                -- Add Box_has_anytime_plus
                -- Add PVR
        M07.3 - QAing results
        M07.4 - Returning results

**Stats:

	-- running time: 17 min approx...

--------------------------------------------------------------------------------------------------------------
*/

-----------------------------------
-- M07.0 - Initialising environment
-----------------------------------

CREATE OR REPLACE PROCEDURE sig_masvg_m07_box_base
AS BEGIN

	MESSAGE cast(now() as timestamp)||' | Beginig M07.0 - Initialising environment' TO CLIENT

    -- Local Variables...
    DECLARE @profiling_day DATE

    SELECT  @profiling_day = MAX(cb_data_date) FROM sk_prod.cust_single_account_view

	MESSAGE cast(now() as timestamp)||' | @ M07.0: Initialisation DONE' TO CLIENT
	
----------------------------------------------
-- M07.1 - Snapshot of current active UK boxes
----------------------------------------------

	MESSAGE cast(now() as timestamp)||' | Beginig M07.1 - Snapshoting current Active UK Boxes' TO CLIENT

	if object_id ('tlbxTemp') is not null
		drop table tlbxTemp
		
    SELECT  *
    INTO    tlbxTemp
    FROM    (vespa_analysts.sig_toolbox_04_Active_Sky_Box_List())


    COMMIT
    CREATE HG INDEX ac_num_index 			ON tlbxTemp (account_number)
    CREATE unique INDEX serv_inst_id_index 	ON tlbxTemp (service_instance_id)
    COMMIT
	
	if object_id ('stage_1') is not null
		drop table stage_1
	
	select	m08.account_number
			,thetemp.service_instance_id
			,thetemp.x_model_number
			,thetemp.Adsmart_flag
			,thetemp.x_pvr_type
			,thetemp.x_manufacturer
			,thetemp.x_box_type
			,thetemp.currency_code
			,thetemp.x_anytime_plus_enabled
			,thetemp.x_description
			,thetemp.x_personal_storage_capacity
	into	stage_1
	from	m08_t1_account_base_stage0 as m08
			left join tlbxtemp	as thetemp
			on	m08.account_number = thetemp.account_number
    
	COMMIT
	drop table tlbxtemp
    CREATE HG INDEX fake_hg1 		ON stage_1 (account_number)
    CREATE unique INDEX fake_key1	ON stage_1 (service_instance_id)
    COMMIT
	
	MESSAGE cast(now() as timestamp)||' | @ M07.1: Snapshot DONE' TO CLIENT
	
	---------------------
	--	BUG PATCH [BEGIN]
	---------------------
	
	--	1/2: we need here to link back to Sky Base as not all Active Accounts
	--	are matching with CSTB table... hence we need to get what possible from CSTB
	--	and work a way around to get the details for the remaining accounts
	-- 	(if they are active we want the details...)

	/*
		THIS COULD ACTUALLY COME IN HANDY AS A COMPLEMENT FOR THE BUG ... FROM SIMON LEARY
		
		select  count(1) as nrows
				,count(distinct decoder_nds_number) as ndecoders
		from    sk_prod.cust_service_instance
		where   decoder_nds_number in   (
											select  distinct last_decoder_nds_number -- 19664
											from    sk_prod.CUST_STB_CALLBACK_SUMMARY
											where   account_number in   (
																			select  distinct account_number
																			from    stage_1
																			where   service_instance_id is null
																		) -- 18367
										)
			
	*/
	-------------------
	--	BUG PATCH [END]
	-------------------
	truncate table m07_t1_box_base_stage0
	commit
	
	insert  into m07_t1_box_base_stage0	(
											account_number
											,card_Subscriber_ID
											,subscriber_ID
											,service_instance_ID
											,Adsmart_flag
											,box_type_physical
											,HD_box_physical
											,box_storage_capacity
											,Box_model
											,pvr_type
											,description
										)
	SELECT  account_number
			,card_Subscriber_ID
			,cast(card_Subscriber_ID as decimal(10)) subscriber_ID
			,service_instance_ID
			,Adsmart_flag
            ,x_box_type
			,HD_box_physical
			,x_personal_storage_capacity
			,x_model_number
			,x_pvr_type
			,x_description
	from    (
				select  distinct 
						a.account_number
						,b.si_external_identifier as card_subscriber_id
						,a.service_instance_id
						,b.si_start_dt
						,rank() over    (
											partition by    a.account_number
															,card_subscriber_id
											order by        a.service_instance_id
															,b.si_start_dt desc
										)   as ranking
						,A.adsmart_flag
						,A.x_box_type
						,CASE   WHEN A.x_description like '%HD%'    THEN 1
																	ELSE 0
						END     AS   HD_box_physical
						,A.x_personal_storage_capacity
						,A.x_model_number
						,A.x_pvr_type
						,x_description
				from    stage_1 as A
						inner join sk_prod.cust_service_instance as B
						on  A.service_instance_id = B.src_system_id
			)   as base
	where   ranking = 1
	
	commit
	MESSAGE cast(now() as timestamp)||' | @ M07.1: Saving Box list into output table DONE' TO CLIENT
	
	---------------------
	--	BUG PATCH [BEGIN]
	---------------------
	
	/*
		-- 01/11/2013
		
		NOTE1:	This fix is put in place due to the gap existing between CSI and CSTB, at the moment we cannot find the
				service intance id for some accounts and that impact on searching box details (because we don't know the box id)
		NOTE2:	Any drop shown in the m07 table after following insert will be due to discrepancies between
				CSI and SAV... but potentially we could avoid this situation (yet carrying on with accounts that
				will not have box details due the known gap between CSTB and SAV)
				
				The reason why is this taking place is because the intention is to keep consistency with the volume of 
				active accounts in the Sky base...		
	*/
	MESSAGE cast(now() as timestamp)||' | @ M07.1: ### BUG-PATCH BEGIN ###' TO CLIENT
	insert	into m07_t1_box_base_stage0 (
											account_number
											,card_Subscriber_ID
										)
	select  distinct
			account_number
			,'unknown'      as card_Subscriber_ID
	from    stage_1
	where   service_instance_id is null
	
	commit
	
	-------------------
	--	BUG PATCH [END]
	-------------------
	
	commit
	MESSAGE cast(now() as timestamp)||' | @ M07.1: Placing Accounts missing Service Instance IDs DONE' TO CLIENT
	MESSAGE cast(now() as timestamp)||' | @ M07.1: ### BUG-PATCH END ###' TO CLIENT
	
---------------------------------------------------
-- M07.2 - Deriving Features of active UK boxes
---------------------------------------------------

	MESSAGE cast(now() as timestamp)||' | Beginig M07.2 - Deriving Features of active UK boxes' TO CLIENT

-- Add Box_is_3D (not needed, basically if the box can HD then can 3D)...
/*
    SELECT  DISTINCT service_instance_id
    INTO    #accounts_with_3dtv
    FROM    sk_prod.cust_subs_hist
    WHERE   subscription_sub_type = '3DTV'
    AND     status_code IN ('AC','PC','AB')
    AND     effective_from_dt <= @profiling_day
    AND     effective_to_dt   >  @profiling_day


    COMMIT
    CREATE  UNIQUE INDEX fake_pk ON #accounts_with_3dtv (service_instance_id)
    COMMIT

    UPDATE  m07_t1_box_base_stage0
    SET     Box_is_3D = 1
    FROM    m07_t1_box_base_stage0
            INNER JOIN #accounts_with_3dtv AS gw3d 
			ON m07_t1_box_base_stage0.service_instance_id = gw3d.service_instance_id

    COMMIT

    DROP   TABLE #accounts_with_3dtv
*/
-- Add Box_has_anytime_plus

    UPDATE  m07_t1_box_base_stage0  as m07
	SET     Box_has_anytime_plus = 1
	FROM    stage_1 as s1
			inner join sk_prod.cust_card_subscriber_link as link
			ON  s1.service_instance_id = link.service_instance_id
			AND link.current_flag = 'Y'
	WHERE   m07.service_instance_id = s1.service_instance_id
	AND     s1.x_anytime_plus_enabled = 'Y'
	
    COMMIT
	MESSAGE cast(now() as timestamp)||' | @ M07.2: Deriving Anytime Plus Capability DONE' TO CLIENT
	
-- Add PVR

	UPDATE  m07_t1_box_base_stage0 AS m07
	SET     pvr = 1
	FROM    sk_prod.cust_subs_hist              as csh
	WHERE   csh.service_instance_id = m07.service_instance_id
	AND     csh.effective_from_dt <= @profiling_day
	AND     csh.effective_to_dt > @profiling_day
	AND     csh.subscription_sub_type in ('DTV Primary Viewing', 'DTV Extra subscription')
	AND     m07.pvr_type like '%PVR%'

    COMMIT
    DROP TABLE stage_1
    COMMIT
	MESSAGE cast(now() as timestamp)||' | @ M07.2: Deriving PVR flag DONE' TO CLIENT
	
------------------------
-- M07.3 - QAing results
------------------------

----------------------------
-- M07.4 - Returning results
----------------------------

-- m07_t1_box_base_stage0...
	MESSAGE cast(now() as timestamp)||' | M07 Finished, table m07_t1_box_base_stage0 BUILT' TO CLIENT

END;

COMMIT;
grant execute on sig_masvg_m07_box_base to vespa_group_low_security;
commit;