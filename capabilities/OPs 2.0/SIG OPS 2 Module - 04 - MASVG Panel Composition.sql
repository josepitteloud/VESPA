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
	
	This unit is to compile in a single table all relevant derivations/aggregations to the composition of the panel,
	which will be used later on to assemble both views (box and account)...

**Modules:

M04: MASVG Panel Composition
        M04.0 - Initialising environment
        M04.1 - Building Blocks for Panel Composition
		M04.2 - Assembling Panel snapshot
        M04.3 - QAing results
        M04.4 - Returning results

**Stats:

	-- running time: 8 min approx...
	
--------------------------------------------------------------------------------------------------------------
*/


-----------------------------------
-- M04.0 - Initialising environment
-----------------------------------

create or replace procedure sig_masvg_m04_panel_composition
as begin
	
	MESSAGE cast(now() as timestamp)||' | Beginig M04.0 - Initialising environment' TO CLIENT
	
    declare	@profiling_day	date

	select @profiling_day = max(cb_data_date) from sk_prod.cust_single_account_view
	
	MESSAGE cast(now() as timestamp)||' | @ M04.0: Initialisation DONE' TO CLIENT
	
	------------------------------------------------
	-- M04.1 - Building Blocks for Panel Composition
	------------------------------------------------
	
	MESSAGE cast(now() as timestamp)||' | Beginig M04.1 - Building Blocks for Panel Composition' TO CLIENT
	
	-- 0) 	Collecting a list of boxes from the Panel Disablement/enablement Campaigns
	--		meaning boxes that will be off/ are still on the Panel...


	select  base.account_number
			,base.card_subscriber_id
			,base.cell_name
			,base.writeback_datetime
	into	#from_campaigns		
	from    (
				select  account_number      
						,card_subscriber_id
						,cell_name
						,writeback_datetime
						,rank() over (partition by card_subscriber_id order by writeback_datetime desc) as most_recent
				from 	sk_prod.campaign_history_cust  a
						inner join sk_prod.campaign_history_lookup_cust   b
						on	a.cell_id = b.cell_id
				where 	(cell_name like 'Vespa Disablement%' or cell_name like 'Vespa Enablement%')
				and 	writeback_datetime >= cast('2011-10-01' as datetime)
			)   as base
			inner join sk_prod.cust_card_subscriber_link as ccsl
			on  base.card_subscriber_id = ccsl.card_subscriber_id
			and base.account_number = ccsl.account_number
			and ccsl.current_flag = 'Y'
	where   base.most_recent = 1    -- removing duplicates...
	and     cell_name not like 'Vespa_Disablement%'
																

	commit
	
	MESSAGE cast(now() as timestamp)||' | @ M04.1: list of boxes from campaigns DONE' TO CLIENT
	--[ NFQA ]


	-- 1)	Getting any box that is seating in the panel snapshot table...

	select  distinct 
			account_number
			,card_subscriber_id
			,panel_no
			,result
			,request_dt
			,created_dt
	into	#from_vss
	from    sk_prod.VESPA_SUBSCRIBER_STATUS

	commit
	MESSAGE cast(now() as timestamp)||' | @ M04.1: list of boxes from Subscriber Status DONE' TO CLIENT
	
	--[ NFQA ]


	-- 2)	Getting any box in the dialback table (this one shows who dialled back and when)...

	select  account_number
			,card_subscriber_id
	into	#from_summary
	from    (
				select  distinct
						convert(varchar(12),account_number) as account_number
						,right(replicate('0',8) || convert(varchar(20), subscriber_id), 8) as card_subscriber_id
				from    sk_prod.vespa_stb_log_summary
				where   account_number is not null
			)   as base

	commit
	MESSAGE cast(now() as timestamp)||' | @ M04.1: list of boxes from Log Summary DONE' TO CLIENT

	--[ NFQA ]


	-- 3)	Complementing the sample with active boxes that are in the Sky Panel...

	select	csl.account_number
			,csl.card_subscriber_id
			--,csl.cb_change_date
	into	#from_skypanel
	from 	sk_prod.cust_card_subscriber_link as csl
			inner join  (
							select  distinct account_number
							from    sk_prod.vespa_sky_view_panel
						)   as sva
			on	csl.account_number = sva.account_number
	where 	current_flag = 'Y'
	and 	@profiling_day between effective_from_dt and effective_to_dt -- 135229


	commit
	MESSAGE cast(now() as timestamp)||' | @ M04.1: list of boxes from Subscriber Link DONE' TO CLIENT
	
	-- [ NFQA ]


	-- 4)	Complementing the sample with confirmed list of Sky View panel seleted boxes...

	select 	account_number
			,card_subscriber_id
			,load_date
	into	#from_skymembers
	from 	vespa_analysts.verified_Sky_View_members
	where 	account_number is not null

	commit
	MESSAGE cast(now() as timestamp)||' | @ M04.1: list of boxes from SkyView DONE' TO CLIENT

	-- [ NFQA ]


	------------------------------------
	-- M04.2 - Assembling Panel snapshot
	------------------------------------
	
	MESSAGE cast(now() as timestamp)||' | Begining M04.2 - Assembling Panel snapshot' TO CLIENT
	
	-- getting into the table everyone in the panel...
	truncate table m04_t1_panel_sample_stage0
	commit
	
	insert	into m04_t1_panel_sample_stage0	(	
												account_number
												,card_subscriber_id
												,subscriber_id
											)
	select	distinct
			account_number
			,card_subscriber_id
			,convert(decimal(10), card_subscriber_id) as box
	from	#from_campaigns
	where	box is not null
	union
	select	distinct
			account_number
			,card_subscriber_id
			,convert(decimal(10), card_subscriber_id) as box
	from	#from_vss
	where	box is not null
	union
	select	distinct
			account_number
			,card_subscriber_id
			,convert(decimal(10), card_subscriber_id) as box
	from	#from_summary
	where	box is not null
	union
	select	distinct
			account_number
			,card_subscriber_id
			,convert(decimal(10), card_subscriber_id) as box
	from	#from_skypanel
	where	box is not null
	union
	select	distinct
			account_number
			,card_subscriber_id
			,convert(decimal(10), card_subscriber_id) as box
	from	#from_skymembers
	where	box is not null

	commit
	MESSAGE cast(now() as timestamp)||' | @ M04.2: Listing Panel Universe DONE' TO CLIENT

	-- Now deriving the Panel Composition fields we need on this section

	-- Panel [DONE]
		 
	update  m04_t1_panel_sample_stage0
	set     panel               =   case    when vss.panel_no = 12 then    'VESPA'
											when vss.panel_no = 6 then     'ALT6'
											when vss.panel_no = 7 then     'ALT7'
											when vss.panel_no = 5 then     'ALT5'
											when vss.panel_no = 11 then    'VESPA11'
									end
			,Status_Vespa       = vss.result
			,vss_request_dt     = case when vss.result = 'Enabled' then convert(date, vss.request_dt) else null end
			,vss_created_date   = case when vss.result = 'Enabled' then convert(date, vss.created_dt) else null end
	from    m04_t1_panel_sample_stage0  as base
			inner join #from_vss        as vss
			on  base.card_subscriber_id = vss.card_subscriber_id

	commit
	MESSAGE cast(now() as timestamp)||' | @ M04.2: Deriving Panel field DONE' TO CLIENT
	
	---------------------------------------------------------------------------------------------------------------------

	-- sky candidate , sky selected , Sky_View_load_date [DONE]

	update  m04_t1_panel_sample_stage0
	set     Is_Sky_View_candidate   = 1
			,Sky_View_load_date     = vsvp.cb_change_date -- This will get overwritten for verified members, but that's okay & intended
	from    m04_t1_panel_sample_stage0              as base
			inner join sk_prod.vespa_sky_view_panel as vsvp
			on  base.account_number = vsvp.account_number
		 
	update	m04_t1_panel_sample_stage0
	set		Is_Sky_View_Selected = 1
			,Sky_View_load_date  = vsvm.load_date
	from	m04_t1_panel_sample_stage0	as base
			inner join vespa_analysts.verified_Sky_View_members as vsvm
			on	base.subscriber_id = vsvm.subscriber_id

	commit
	MESSAGE cast(now() as timestamp)||' | @ M04.2: Deriving Sky candidate, selected and load date DONE' TO CLIENT
	---------------------------------------------------------------------------------------------------------------------	
	 
	-- historic_result_date [DONE] 

    -- Since sybase does not support RANKING on updates, breaking down the logic into to two steps...
    -- we need to get here the most recent boxes per accounts...
    select	*
    into    #temp_shelf
    from	(
	            select	account_number
				        ,card_subscriber_id
				        ,result
						,coalesce(request_dt, modified_dt) as request_dt
						,rank() over (partition by account_number, card_subscriber_id order by request_dt desc, modified_dt desc, created_dt desc) as most_recent
				from 	sk_prod.vespa_subscriber_status_hist
				where 	result in ('Enabled', 'Disabled')
			)	as base
	where	most_recent = 1
	and		result <> 'Disable'

    commit
    create hg index fake_hg1 on #temp_shelf(account_number)
    create hg index fake_hg2 on #temp_shelf(card_subscriber_id)
    commit

	update  m04_t1_panel_sample_stage0
	set     historic_result_date = convert(date, pe.request_dt)
	from    m04_t1_panel_sample_stage0  as base
			inner join	#temp_shelf 	as pe
			on  base.card_subscriber_id = pe.card_subscriber_id
			and base.account_number = pe.account_number	 
			
	commit
    drop table #temp_shelf
    commit
	MESSAGE cast(now() as timestamp)||' | @ M04.2: Deriving Historic Results DONE' TO CLIENT
	---------------------------------------------------------------------------------------------------------------------

	-- selection_date [DONE] 

	update	m04_t1_panel_sample_stage0
	set 	Panel_ID_4_cells_confirm = 1            
			,Selection_date = tvb.writeback_datetime
	from 	m04_t1_panel_sample_stage0	as base
			inner join #from_campaigns	as tvb
			on	base.card_subscriber_id = tvb.card_subscriber_id

	commit

	 -- So now creating from the subscriber history table a list of boxes whose last status was panel 12 (regardless they are disabled or enabled)
	 -- to get last date of writeback_datetime value (panel_id derivation is done further in the code)...

	select	n.*
			,vssh2.panel_no
	into    #subs_hist_vespa_boxes
	from    (
				select	base.card_subscriber_id
						,max(vssh.writeback_datetime) as writeback_datetime
				from    m04_t1_panel_sample_stage0 as base
						inner join sk_prod.vespa_subscriber_status_hist as vssh
						on	base.card_subscriber_id = vssh.card_subscriber_id
				group   by  base.card_subscriber_id
			) 	as n
			inner join sk_prod.vespa_subscriber_status_hist as vssh2
			on	n.card_subscriber_id = vssh2.card_subscriber_id
			and n.writeback_datetime = vssh2.writeback_datetime
			and	vssh2.panel_no in (12,11)


	update  m04_t1_panel_sample_stage0
	set     Panel_ID_4_cells_confirm = 1
			,Selection_date = shvb.writeback_datetime
	from    m04_t1_panel_sample_stage0 as base
			inner join #subs_hist_vespa_boxes as shvb
			on base.card_subscriber_id = shvb.card_subscriber_id
	where   base.selection_date is null
		
	commit
	MESSAGE cast(now() as timestamp)||' | @ M04.2: Deriving Selection Date DONE' TO CLIENT
	---------------------------------------------------------------------------------------------------------------------
		
	-- ps_olive [DONE]

	select	b.service_instance_id
			,convert(integer,min(si_external_identifier)) as subscriber_id
			,convert(bit, max(case when si_service_instance_type = 'Primary DTV' then 1 else 0 end)) as primary_box
			,convert(bit, max(case when si_service_instance_type = 'Secondary DTV (extra digiboxes)' then 1 else 0 end)) as secondary_box
	into 	#subscriber_details
	from 	sk_prod.CUST_SERVICE_INSTANCE as b
			inner join m04_t1_panel_sample_stage0 as base
			on	base.card_subscriber_id = b.si_external_identifier
	where 	si_service_instance_type in ('Primary DTV','Secondary DTV (extra digiboxes)')
	and 	@profiling_day between effective_from_dt and effective_to_dt
	group 	by	b.service_instance_id

	commit
	create index for_stuff on #subscriber_details (subscriber_id)
	commit

	-- Then push those box types onto the subscriber level summary
	update	m04_t1_panel_sample_stage0
	set		PS_Olive = case	when b.subscriber_id is null then 'U'
								when b.primary_box = 1 and secondary_box = 0 then 'P'
								when b.primary_box = 0 and secondary_box = 1 then 'S'
								else '?' 
						end
	from 	m04_t1_panel_sample_stage0 			as base
			left outer join #subscriber_details as b
			on base.subscriber_id = b.subscriber_id
		 
	commit	
	MESSAGE cast(now() as timestamp)||' | @ M04.2: Deriving PS Olive DONE' TO CLIENT
	---------------------------------------------------------------------------------------------------------------------
		 
	-- ps_vespa [DONE]

    --
    select  distinct
            si_external_identifier
            ,si_service_instance_type
    into   #unique_box_list
    from    (
                select	csi.si_external_identifier
            	        ,rank() over(partition by csi.si_external_identifier order by csi.effective_from_dt,csi.si_decoder_pairing_sk desc) as rank_
            			,csi.si_service_instance_type
            	from 	sk_prod.cust_service_instance           as csi
                        inner join m04_t1_panel_sample_stage0   as m04
                        on	m04.card_subscriber_id = CSI.si_external_identifier
            	where 	csi.si_service_instance_type like '%DTV%'
				and     csi.si_latest_src <> 'LEGMIDAS'
            )   as listing
    where   rank_ = 1 

    commit
    create unique index fake_key on #unique_box_list(si_external_identifier)
    commit

	update	m04_t1_panel_sample_stage0
	set 	PS_Vespa = left(csi.si_service_instance_type,1)
	from 	m04_t1_panel_sample_stage0      as base
			inner join	#unique_box_list    as CSI 
			on	base.card_subscriber_id = CSI.si_external_identifier
			
	commit		
    drop table #unique_box_list
    commit
	MESSAGE cast(now() as timestamp)||' | @ M04.2: Deriving PS Vespa DONE' TO CLIENT
	
	---------------------------------------------------------------------------------------------------------------------
				
	-- inferred flag [DONE]

	select	account_number
			,convert(bit, 0) as has_MR
	into 	#maybe_single_households
	from 	m04_t1_panel_sample_stage0
	group 	by	account_number
	having 	count(1) = 1 
			and sum(case when PS_Vespa = 'U' and PS_Olive = 'U' then 1 else 0 end)=1

	-- So this should get us to a pretty concise population that shouldn't take too long to process...

	commit
	create unique index fake_pk on #maybe_single_households (account_number)
	commit

	-- ok, so now let's figure out which have MR and which have multiple associated boxes...
	update	#maybe_single_households
	set 	has_MR = 1
	from 	#maybe_single_households					as hh
			inner join sk_prod.cust_single_account_view as csh
			on	hh.account_number = csh.account_number
	where 	prod_active_multiroom = 1

	commit

	-- Now we have the marks, put them back on SBV; can just join by account number, by
	-- construction these are households with only one box

	update	m04_t1_panel_sample_stage0
	set 	PS_inferred_primary = 1
	from 	m04_t1_panel_sample_stage0			as base
			inner join #maybe_single_households as msh
			on 	base.account_number = msh.account_number
	where 	msh.has_MR = 0


	commit
	MESSAGE cast(now() as timestamp)||' | @ M04.2: Deriving Infered Flag DONE' TO CLIENT
	---------------------------------------------------------------------------------------------------------------------

	-- ps_flag / ps_source [DONE]

	update	m04_t1_panel_sample_stage0
	set		PS_flag = 	case	when PS_Olive = PS_Vespa and PS_Olive <> 'U' then PS_Olive
								when PS_inferred_primary = 1 then 'P' 						-- Only populated for the questionable boxes
								when PS_Olive = 'U' or PS_Olive is null then PS_Vespa
								when PS_Vespa = 'U' or PS_Vespa is null then PS_Olive
								else '!'    												-- this should only leave the case where one of Olive / Vespa says 'P' and the other says 'S'
						end
			,PS_source =	case	when PS_Olive = PS_Vespa and PS_Olive <> 'U' then 'Both agree'
									when PS_inferred_primary = 1 then 'Inferred'
									when PS_Vespa = 'U' or PS_Vespa is null then 'Olive'
									when PS_Olive = 'U' or PS_Olive is null then 'Vespa'
									else 'Collision!'
							end

	commit
	MESSAGE cast(now() as timestamp)||' | @ M04.2: Deriving PS Flag and Source DONE' TO CLIENT
	
	---------------------------------------------------------------------------------------------------------------------	   
		   
	--  enablement date , enablement source [DONE] 
	update	m04_t1_panel_sample_stage0
	set 	Enablement_date =	case	when vss_request_dt         is not null and Status_Vespa = 'Enabled'    then vss_request_dt         -- If the box doesn't say 'Enabled' then there should be a historic enablement to fall back with
										when Sky_View_load_date     is not null                                 then Sky_View_load_date
										when historic_result_date   is not null                                 then historic_result_date
										when Selection_date         is not null                                 then Selection_date
										when vss_created_date       is not null                                 then vss_created_date
								end
			,Enablement_date_source =	case	when vss_request_dt         is not null and Status_Vespa = 'Enabled'    then 'vss_request_dt'
												when Sky_View_load_date     is not null                                 then 'Sky View'
												when historic_result_date   is not null                                 then 'historic'
												when Selection_date         is not null                                 then 'writeback'
												when vss_created_date       is not null                                 then 'vss_created_dt'
										end
		
	commit
	MESSAGE cast(now() as timestamp)||' | @ M04.2: Deriving enablement Date and Source DONE' TO CLIENT
	
	---------------------------------------------------------------------------------------------------------------------
		
	-----------------------
	-- M04.3 - QAing results
	-----------------------

	-- [ NFQA ]
	/*
	select  card_subscriber_id
			,count(1)
	from    m04_t1_panel_sample_stage0
	group   by  card_subscriber_id
	having  count(1) >1
	*/

	----------------------------
	-- M04.4 - Returning results
	----------------------------

	-- ... m04_t1_panel_sample_stage0
	MESSAGE cast(now() as timestamp)||' | M04 Finished, table m04_t1_panel_sample_stage0 BUILT' TO CLIENT


    commit


end;

commit;
grant execute on sig_masvg_m04_panel_composition to vespa_group_low_security;
commit;

/*
STATS:

RUNNING TIME: c. 8 min
*/