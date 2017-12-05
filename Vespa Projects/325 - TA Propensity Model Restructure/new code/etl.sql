      -- merge
  select *
		  into etl_main1
				from #data_mart_output as dmt
				     left join future_calling_churn4 as fcc on dmt.account_number = fcc.account_number
;
				
      -- merge
  select *
		  into etl_main2
				from etl_main1 as et1
						   left join #HistoricalCallingBehaviour_output	as hcc on et1.account_number = hcc.account_number
;
		
      -- merge
  select *
		  into etl_main3
				from etl_main2 as et2
						   left join #HistoricalChurnBehaviour_output	as hcc on et2.account_number = hcc.account_number
;
		
      -- merge
  select *
		  into etl_main4
				from etl_main3 as et3
						   left join #offers_output	as oop on et3.account_number = oop.account_number

						-- filler
  select ,case when IC_Calls_Last_Year is null then 0 else IC_Calls_Last_Year end
									,case when IC_Calls_Last_3_Months is null then 0 else IC_Calls_Last_3_Months end
									,case when IC_Calls_Last_6_Months is null then 0 else IC_Calls_Last_6_Months end
									,case when IC_Calls_Last_9_Months is null then 0 else IC_Calls_Last_9_Months end
									,case when PAT_Calls_Last_Year is null then 0 else PAT_Calls_Last_Year end
									,case when PAT_Calls_Last_3_Months is null then 0 else PAT_Calls_Last_3_Months end
									,case when PAT_Calls_Last_6_Months is null then 0 else PAT_Calls_Last_6_Months end
									,case when PAT_Calls_Last_9_Months is null then 0 else PAT_Calls_Last_9_Months end
									,case when TA_Calls_Last_Year is null then 0 else TA_Calls_Last_Year end
									,case when TA_Calls_Last_3_Months is null then 0 else TA_Calls_Last_3_Months end
									,case when TA_Calls_Last_6_Months is null then 0 else TA_Calls_Last_6_Months end
									,case when TA_Calls_Last_9_Months is null then 0 else TA_Calls_Last_9_Months end
									,case when PO_In_Next_4_Months_Flag is null then 0 else PO_In_Next_4_Months_Flag end
									,case when TA_in_NEXT_3_months_flag is null then 0 else TA_in_NEXT_3_months_flag end
									,case when AB_in_Next_3_Months_Flag is null then 0 else AB_in_Next_3_Months_Flag end
									,case when SC_In_Next_4_Months_Flag is null then 0 else SC_In_Next_4_Months_Flag end
									,case when IC_Calls_Last_2_Years is null then 0 else IC_Calls_Last_2_Years end
									,case when TA_Calls_Last_2_Years is null then 0 else TA_Calls_Last_2_Years end
									,case when PAT_Calls_Last_2_Years is null then 0 else PAT_Calls_Last_2_Years end
									,case when PO_Events_Last_Year is null then 0 else PO_Events_Last_Year end
									,case when PO_Events_Last_3_Months is null then 0 else PO_Events_Last_3_Months end
									,case when PO_Events_Last_6_Months is null then 0 else PO_Events_Last_6_Months end
									,case when PO_Events_Last_9_Months is null then 0 else PO_Events_Last_9_Months end
									,case when PO_Events_Last_2_Years is null then 0 else PO_Events_Last_2_Years end
									,case when AB_Events_Last_Year is null then 0 else AB_Events_Last_Year end
									,case when AB_Events_Last_3_Months is null then 0 else AB_Events_Last_3_Months end
									,case when AB_Events_Last_6_Months is null then 0 else AB_Events_Last_6_Months end
									,case when AB_Events_Last_9_Months is null then 0 else AB_Events_Last_9_Months end
									,case when AB_Events_Last_2_Years is null then 0 else AB_Events_Last_2_Years end
									,case when SC_Events_Last_Year is null then 0 else SC_Events_Last_Year end
									,case when SC_Events_Last_3_Months is null then 0 else SC_Events_Last_3_Months end
									,case when SC_Events_Last_6_Months is null then 0 else SC_Events_Last_6_Months end
									,case when SC_Events_Last_9_Months is null then 0 else SC_Events_Last_9_Months end
									,case when SC_Events_Last_2_Years is null then 0 else SC_Events_Last_2_Years end
									,case when PC_Events_Last_Year is null then 0 else PC_Events_Last_Year end
									,case when PC_Events_Last_3_Months is null then 0 else PC_Events_Last_3_Months end
									,case when PC_Events_Last_6_Months is null then 0 else PC_Events_Last_6_Months end
									,case when PC_Events_Last_9_Months is null then 0 else PC_Events_Last_9_Months end
									,case when PC_Events_Last_2_Years is null then 0 else PC_Events_Last_2_Years end
									,case when status_code_AB _effective_from_dt_Max_flag is null then 0 else status_code_AB _effective_from_dt_Max_flag end
									,case when status_code_PO _effective_from_dt_Max_flag is null then 0 else status_code_PO _effective_from_dt_Max_flag end
									,case when status_code_SC _effective_from_dt_Max_flag is null then 0 else status_code_SC _effective_from_dt_Max_flag end
									,case when TypeOfEvent_TA _event_dt_Max_flag is null then 0 else TypeOfEvent_TA _event_dt_Max_flag end
									,case when TA_in_3-6_Months_Flag is null then 0 else TA_in_3-6_Months_Flag end
									,case when Box Offer_Last_2_Years is null then 0 else Box Offer_Last_2_Years end
									,case when BroadBand & Talk_Last_2_Years is null then 0 else BroadBand & Talk_Last_2_Years end
									,case when Install Offer_Last_2_Years is null then 0 else Install Offer_Last_2_Years end
									,case when Others_Last_2_Years is null then 0 else Others_Last_2_Years end
									,case when Others_PPO_Last_2_Years is null then 0 else Others_PPO_Last_2_Years end
									,case when Service Call_Last_2_Years is null then 0 else Service Call_Last_2_Years end
									,case when TV Packs_Last_2_Years is null then 0 else TV Packs_Last_2_Years end
									,case when Box Offer_Last_Year is null then 0 else Box Offer_Last_Year end
									,case when BroadBand & Talk_Last_Year is null then 0 else BroadBand & Talk_Last_Year end
									,case when Install Offer_Last_Year is null then 0 else Install Offer_Last_Year end
									,case when Others_Last_Year is null then 0 else Others_Last_Year end
									,case when Others_PPO_Last_Year is null then 0 else Others_PPO_Last_Year end
									,case when Service Call_Last_Year is null then 0 else Service Call_Last_Year end
									,case when TV Packs_Last_Year is null then 0 else TV Packs_Last_Year end
									,case when Box Offer_Last_6_Months is null then 0 else Box Offer_Last_6_Months end
									,case when BroadBand & Talk_Last_6_Months is null then 0 else BroadBand & Talk_Last_6_Months end
									,case when Install Offer_Last_6_Months is null then 0 else Install Offer_Last_6_Months end
									,case when Others_Last_6_Months is null then 0 else Others_Last_6_Months end
									,case when Others_PPO_Last_6_Months is null then 0 else Others_PPO_Last_6_Months end
									,case when Service Call_Last_6_Months is null then 0 else Service Call_Last_6_Months end
									,case when TV Packs_Last_6_Months is null then 0 else TV Packs_Last_6_Months end
									,case when Box Offer_Last_3_Months is null then 0 else Box Offer_Last_3_Months end
									,case when BroadBand & Talk_Last_3_Months is null then 0 else BroadBand & Talk_Last_3_Months end
									,case when Install Offer_Last_3_Months is null then 0 else Install Offer_Last_3_Months end
									,case when Others_Last_3_Months is null then 0 else Others_Last_3_Months end
									,case when Others_PPO_Last_3_Months is null then 0 else Others_PPO_Last_3_Months end
									,case when Service Call_Last_3_Months is null then 0 else Service Call_Last_3_Months end
									,case when TV Packs_Last_3_Months is null then 0 else TV Packs_Last_3_Months end
									,case when price_protection_flag is null then 0 else price_protection_flag end
									,case when Total_Expiring_Comms_Offers_Next_4-6_Months is null then 0 else Total_Expiring_Comms_Offers_Next_4-6_Months end
									,case when Total_Expiring_Other_Offers_Next_4-6_Months is null then 0 else Total_Expiring_Other_Offers_Next_4-6_Months end
									,case when Total_Expiring_DTV_Offers_Next_4-6_Months is null then 0 else Total_Expiring_DTV_Offers_Next_4-6_Months end
									,case when Total_Expiring_Offer_Value_Next_4-6_Months is null then 0 else Total_Expiring_Offer_Value_Next_4-6_Months end
									,case when Total_Expiring_Comms_Offers_Next_3_Months is null then 0 else Total_Expiring_Comms_Offers_Next_3_Months end
									,case when Total_Expiring_Other_Offers_Next_3_Months is null then 0 else Total_Expiring_Other_Offers_Next_3_Months end
									,case when Total_Expiring_DTV_Offers_Next_3_Months is null then 0 else Total_Expiring_DTV_Offers_Next_3_Months end
									,case when Total_Expiring_Offer_Value_Next_3_Months is null then 0 else Total_Expiring_Offer_Value_Next_3_Months end
		  into etl_main5
				from etl_main4
;
				
						-- segment derivations
		select * 
		  into segment_derivations1
				from etl_main5
;

						-- sum unstable flags
		select *
      		,AB_in_24m_flag + cuscan_in_24m_flag + syscan_in_24m_flag + TA_in_24m_flag as sum_unstable_flags
		  into segment_derivations2
				from segment_derivations1	
;
				
						-- filter
		select *
		  into segment_derivations3
				from segment_derivations2	
				
      -- segment				
		select *
		      ,case when dtv_first_act_date > [10_Months_Prior] then '<10_Months'
              when dtv_first_act_date <= [10_Months_Prior] and dtv_first_act_date > [2_Years_Prior] then '10-24_Months'
              when dtv_first_act_date <= [2_Years_Prior] and sum_unstable_flags = 0 then '24_Months+' end	as segment
		  into segment_derivations4
				from segment_derivations3	
				
  				-- output
  select *
		  from segment_derivations4
		  into TA_MODELING_RAW_DATA_20140312				
;				
									