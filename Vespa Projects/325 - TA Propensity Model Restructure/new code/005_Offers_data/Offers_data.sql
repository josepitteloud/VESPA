  create variable @Sample_1_EndString varchar(4);
     set @Sample_1_EndString = '27';
  create variable @Reference integer;
     set @Reference=201308;

					select * 
		  into #offers1
		  from yarlagaddar.View_CUST_OFFER_HIST
   where offer_id not in (75680,75687,75685,75683,75686,75684,75681,75682,75680,75681,75682,75579,75580,75581,75583,75584,75586,75587,75589,75590,75592,75593,75594,75595,75598,75601,75602,75607,75610,75612,75613,75616,75618,75619,75620,75621,75622,75623,75624,75625,75626,75627,75628,75629,75630,75631,75634,75636,75638,75642,75643,75644,75647,75651,75654,75655,75656,75657,75664,75666,75667,75668,75673,75675,75677,75579,75583,75584,75589,75590,75592,75594,75595,75601,75602,75607,75610,75612,75613,75618,75619,75620,75621,75622,75623,75624,75625,75626,75627,75628,75629,75630,75631,75634,75636,75638,75654,75655,75656,75657,75673,75675,75677,75444,75443,75445)

			
			   -- select
  select * 
		  into #offers2
				from #offers1
   where account_number like '%' || @Sample_1_EndString

   			-- supernode
																-- filter
												select *
																		,case when offer_start_dt is null then initial_effective_dt else offer_start_dt end
														into #offers3
														from #offers2
														
																-- aggregate
												select sum(offer_id)
														into #offers4
														from #offers3
														
																-- remove administration charges
												select *
														into #offers5
														from #offers4
													where lower(offer_dim_description) not like '%administration charge%'
																
																-- remove office only
												select *
														into #offers6
														from #offers5
													where lower(offer_dim_description) not like '%staff offer%'
																
																-- remove not relevant
												select *
														into #offers7
														from #offers6
													where lower(offer_dim_description) not like '%not relevant%'         
      -- end supernode
						
						-- reference
  select *
		      ,@reference as [reference]
    into #offers8
    from #offers7		
						
  select *
    into #offers9
    from #offers8 as off
         inner join sourcedates as sou on off.[reference] = sou.[reference]
						
  select *
    into #offers10
    from #offers9
   where offer_start_dt > [2_Years_Prior] and offer_start_dt <= Snapshot_Date		
						
						-- branch 2
		select account_number
      		,status_code
				into #offers11
		  from sharmaa.View_attachments_201501 
			where Status_Code = 'AC'
			
			   -- select
  select * 
		  into #offers12
				from #offers11
   where account_number like '%' || @Sample_1_EndString
			
			   -- merge
		select *
		  into #offers13
		  from #offers10 as o10
				     inner join #offers12 as o12 on o10.account_number = o12.account_number
			
   			-- supernode
																-- filler
												select *
																		,trim(offer_type) as offer_type
														into #offers14
														from #offers13			

																-- type				
												select *
														into #offers15
														from #offers14
														
																-- SetToFlag
												select *
																		,case when offer_type = 'Offer'            then 1 else 0 end as offer_type_box_offer
																		,case when offer_type = 'Broadband & Talk' then 1 else 0 end as offer_type_broadband_and_talk
																		,case when offer_type = 'Install Offer'    then 1 else 0 end as offer_type_install_offer
																		,case when offer_type = 'Others'           then 1 else 0 end as offer_type_others
																		,case when offer_type = 'Others_PPO'       then 1 else 0 end as offer_type_others_ppo
																		,case when offer_type = 'Service Call'     then 1 else 0 end as offer_type_service_call
																		,case when offer_type = 'TV Packs'         then 1 else 0 end as offer_type_tv_packs
														into #offers16
														from #offers15	

																-- filter
												select account_number
																		,Status_Code
																		,Reference	
																		,offer_start_dt	
																		,offer_end_dt	
																		,offer_amount	
																		,offer_status	
																		,offer_dim_description	
																		,initial_effective_dt	
																		,Offer_Duration_Months	
																		,Sky_Product	
																		,Offer_Type	
																		,Attachments_Table	
																		,Account_Numbers_ending_in	
																		,Snapshot_Date	
																		,[2_Years_Prior]	
																		,[1_Year_Prior]	
																		,[10_Months_Prior]	
																		,[9_Months_Prior]	
																		,[6_Months_Prior]	
																		,[3_Months_Prior]	
																		,[1_Month_Prior]	
																		,[1_Month_Future]	
																		,[2_Months_Future]
																		,[3_Months_Future]
																		,[4_Months_Future]
																		,[5_Months_Future]
																		,[6_Months_Future]
																		,Offer_Type_Box_Offer as Box_Offer
																		,Offer_Type_BroadBand_and_Talk as BroadBand_and_Talk
																		,Offer_Type_Install_Offer	as Install_Offer	
																		,Offer_Type_Others	as others
																		,Offer_Type_Others_PPO as others_ppo
																		,Offer_Type_Service_Call	 as service_call
																		,Offer_Type_TV_Packs	as tv_packs
														into #offers17
														from #offers16	

      -- type				
  select *
				into #offers18
				from #offers17	
		
       -- supernode		
			select *
			      ,case offer_category when 'Broadband & Talk' then 'Software_Comms'
               									      when 'TV Packs' then 'Software_DTV'
																														else 'Others' end as Offer_category
 				into #offers19
	 			from #offers18	
			
			select * 
			  into #offers20
			  from #offers19
    where offer_start_dt > [2_Years_Prior] 
			   and offer_start_dt <= Snapshot_Date			
			
			select *
      			,sum(box_offer)          as sum_box_offer
      			,sum(BroadBand_and_Talk) as sum_BroadBand_and_Talk
      			,sum(Install_Offer)      as sum_Install_Offer
      			,sum(others)             as sum_others
      			,sum(others_ppo)         as sum_others_ppo
      			,sum(service_call)       as sum_service_call
      			,sum(tv_packs)           as sum_tv_packs
			  into #offers_output
			  from #offers20
			
			select *
      			,sum_box_offer
      			,sum_BroadBand_and_Talk
      			,sum_Install_Offer
      			,sum_others
      			,sum_others_ppo
      			,sum_service_call
      			,sum_tv_packs
			  into #offers22
			  from #offers21
			
			