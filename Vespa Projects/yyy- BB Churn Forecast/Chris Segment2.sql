SELECT (CASE WHEN (T0.offer = 'BBT Exp +90') THEN (CASE WHEN (T0.product_holding = 'E. SABB') 
														THEN (CASE WHEN ((((((T0.life_stage = '1.') OR (T0.life_stage = '14')) 
																			OR (T0.life_stage = '2.')) OR (T0.life_stage = '4.')) 
																			OR (T0.life_stage = '5.')) OR (T0.life_stage = 'U')) 	THEN 0 
																	ELSE 0 END) 
														ELSE (CASE WHEN (((((T0.fss = 'A') OR (T0.fss = 'C')) OR (T0.fss = 'D')) 
																			OR (T0.fss = 'E')) OR (T0.fss = 'F')) 					THEN 0 
																	WHEN ((((T0.fss = 'B') OR (T0.fss = 'G')) OR (T0.fss = 'H')) 
																			OR (T0.fss = 'K')) 										THEN 0 
																	ELSE 0 END) END)
			WHEN ((T0.offer = 'BBT Exp -30') OR (T0.offer = 'OTHER Live -30')) THEN (CASE WHEN (T0.product_holding = 'E. SABB') 	THEN 18 
																	ELSE (CASE WHEN ((((((((T0.fss = '') OR (T0.fss = 'G')) 
																					OR (T0.fss = 'H')) OR (T0.fss = 'J')) 
																					OR (T0.fss = 'K')) OR (T0.fss = 'L')) 
																					OR (T0.fss = 'M')) OR (T0.fss = 'U')) 			THEN 0 
																				ELSE 0 END) END)
			WHEN ((T0.offer = 'BBT Exp 31-90') OR (T0.offer = 'OTHER Exp -30')) THEN (CASE WHEN ((T0.bb_tenure = '1 year') OR (T0.bb_tenure = 'Less 1 year')) THEN 0 
											ELSE (CASE WHEN ((((T0.age = 'Age 18-25') OR (T0.age = 'Age 26-35')) OR (T0.age = 'Age 36-45')) 
															OR (T0.age = 'Unclassified')) THEN 0 
														ELSE 0 END) END) 
			WHEN (T0.offer = 'BBT Live +90') THEN (CASE WHEN (T0.bb_tenure = '1 year') THEN (CASE WHEN (((T0.product_holding = 'B. DTV + Triple play') 
																							OR (T0.product_holding = 'C. DTV + BB Only')) 
																							OR (T0.product_holding = 'D. DTV + Other Comms')) THEN 7
																						ELSE 1 END) 
														WHEN (T0.bb_tenure = '2 years') THEN 4 
														WHEN (T0.bb_tenure = '3 years') THEN 5 
														WHEN (T0.bb_tenure = '4 years') THEN 8 
														WHEN (T0.bb_tenure = '5+ years') THEN (CASE WHEN (((((((((T0.bb_type = 'Broadband Connect') 
																									OR (T0.bb_type = 'Sky Broadband Everyday')) 
																									OR (T0.bb_type = 'Sky Broadband Lite')) 
																									OR (T0.bb_type = 'Sky Broadband Unlimited Fibre')) 
																									OR (T0.bb_type = 'Sky Broadband Unlimited Pro')) 
																									OR (T0.bb_type = 'Sky Fibre')) 
																									OR (T0.bb_type = 'Sky Fibre Lite')) 
																									OR (T0.bb_type = 'Sky Fibre Max')) 
																									OR (T0.bb_type = 'Sky Fibre Unlimited Pro')) THEN 6 
																								ELSE 10 END) 
													ELSE (CASE WHEN ((((((((((((((((T0.bb_type = 'Broadband Connect') 
																OR (T0.bb_type = 'Sky Broadband 12GB')) OR (T0.bb_type = 'Sky Broadband Lite')) 
																OR (T0.bb_type = 'Sky Broadband Lite (ROI - Legacy)')) OR (T0.bb_type = 'Sky Broadband Lite (ROI)')) 
																OR (T0.bb_type = 'Sky Broadband Unlimited (ROI - Legacy)')) OR (T0.bb_type = 'Sky Broadband Unlimited (ROI)')) 
																OR (T0.bb_type = 'Sky Broadband Unlimited Fibre')) OR (T0.bb_type = 'Sky Broadband Unlimited Pro')) 
																OR (T0.bb_type = 'Sky Connect Unlimited (ROI - Legacy)')) OR (T0.bb_type = 'Sky Connect Unlimited (ROI)')) 
																OR (T0.bb_type = 'Sky Fibre')) OR (T0.bb_type = 'Sky Fibre Max')) OR (T0.bb_type = 'Sky Fibre Unlimited (ROI - Legacy)')) 
																OR (T0.bb_type = 'Sky Fibre Unlimited (ROI)')) OR (T0.bb_type = 'Sky Fibre Unlimited Pro')) 	THEN 25 
															ELSE 0 END) END)
				WHEN ((T0.offer = 'BBT Live -30') OR (T0.offer = 'Multi Exp 31-90')) THEN (CASE WHEN ((((T0.affluence = '2.') OR (T0.affluence = '5.')) 
																									OR (T0.affluence = '6.')) OR (T0.affluence = '8.')) THEN 0 
																								ELSE 0 END) 
				WHEN (T0.offer = 'BBT Live 31-90') THEN (CASE WHEN (T0.bb_tenure = 'Less 1 year') THEN 0 
															ELSE 0 END) 
				WHEN (T0.offer = 'Multi Exp +90') THEN 0 
				WHEN ((T0.offer = 'Multi Exp -30') OR (T0.offer = 'Multi Live -30')) THEN 20 
				WHEN (T0.offer = 'Multi Live +90') THEN (CASE WHEN (((((((T0.bb_type = 'Sky Broadband Everyday') OR (T0.bb_type = 'Sky Broadband Unlimited Fibre')) 
																	OR (T0.bb_type = 'Sky Broadband Unlimited Pro')) OR (T0.bb_type = 'Sky Fibre'))
																	OR (T0.bb_type = 'Sky Fibre Lite')) OR (T0.bb_type = 'Sky Fibre Max'))
																	OR (T0.bb_type = 'Sky Fibre Unlimited Pro')) THEN 2 
																ELSE (CASE 	WHEN (T0.bb_tenure = '1 year') THEN 3 
																			WHEN (T0.bb_tenure = '5+ years') THEN 13 
																			WHEN ((T0.bb_tenure = 'Less 1 year') OR (T0.bb_tenure = 'weird')) THEN 24 
																		ELSE 12 END) END) 
				WHEN (T0.offer = 'Multi Live 31-90') THEN 26
				WHEN (T0.offer = 'OTHER Exp +90') THEN (CASE WHEN (((T0.age = 'Age 18-25') OR (T0.age = 'Age 26-35')) 
																OR (T0.age = 'Age 36-45')) THEN 0 
															ELSE (CASE WHEN ((((((((((T0.fss = 'B') OR (T0.fss = 'C')) 
																				OR (T0.fss = 'D')) OR (T0.fss = 'E')) 
																				OR (T0.fss = 'F')) OR (T0.fss = 'G')) 
																				OR (T0.fss = 'I')) OR (T0.fss = 'J')) 
																				OR (T0.fss = 'K')) OR (T0.fss = 'U')) THEN 0 
																		ELSE 0 END) END) 
				WHEN (T0.offer = 'OTHER Exp 31-90') THEN 0 
				WHEN (T0.offer = 'OTHER Live +90') THEN (CASE WHEN (((((((((((((((((((((T0.bb_type = 'Broadband Connect') OR (T0.bb_type = 'Sky Broadband 12GB')) 
																	OR (T0.bb_type = 'Sky Broadband Lite')) OR (T0.bb_type = 'Sky Broadband Lite (ROI - Legacy)')) 
																	OR (T0.bb_type = 'Sky Broadband Lite (ROI)')) OR (T0.bb_type = 'Sky Broadband Unlimited (ROI - Legacy)')) 
																	OR (T0.bb_type = 'Sky Broadband Unlimited (ROI)')) OR (T0.bb_type = 'Sky Broadband Unlimited Fibre')) 
																	OR (T0.bb_type = 'Sky Broadband Unlimited Pro')) OR (T0.bb_type = 'Sky Connect Lite (ROI - Legacy)')) 
																	OR (T0.bb_type = 'Sky Connect Lite (ROI)')) OR (T0.bb_type = 'Sky Connect Unlimited (ROI - Legacy)')) 
																	OR (T0.bb_type = 'Sky Connect Unlimited (ROI)')) OR (T0.bb_type = 'Sky Fibre')) 
																	OR (T0.bb_type = 'Sky Fibre (ROI - Legacy)')) OR (T0.bb_type = 'Sky Fibre (ROI)')) 
																	OR (T0.bb_type = 'Sky Fibre Lite')) OR (T0.bb_type = 'Sky Fibre Max')) 
																	OR (T0.bb_type = 'Sky Fibre Unlimited (ROI - Legacy)')) OR (T0.bb_type = 'Sky Fibre Unlimited (ROI)')) 
																	OR (T0.bb_type = 'Sky Fibre Unlimited Pro')) 
																			THEN (CASE 	WHEN ((T0.bb_tenure = '4 years') OR (T0.bb_tenure = '5+ years')) THEN 9 
																						WHEN ((T0.bb_tenure = 'Less 1 year') OR (T0.bb_tenure = 'weird')) THEN 21 ELSE 11 END) 
																ELSE (CASE WHEN (T0.bb_tenure = '1 year') 		THEN 14 
																			WHEN (T0.bb_tenure = '2 years') 	THEN 15
																			WHEN ((T0.bb_tenure = '3 years') OR (T0.bb_tenure = '4 years')) 	THEN 16
																			WHEN ((T0.bb_tenure = 'Less 1 year') OR (T0.bb_tenure = 'weird')) THEN 0
																		ELSE 17 END) END) 
				WHEN (T0.offer = 'OTHER Live 31-90') THEN (CASE WHEN ((T0.bb_tenure = '2 years') OR (T0.bb_tenure = '3 years')) 	THEN 23
																WHEN ((T0.bb_tenure = 'Less 1 year') OR (T0.bb_tenure = 'weird')) THEN 0
															ELSE (CASE WHEN (((((((T0.fss = '') OR (T0.fss = 'A')) OR (T0.fss = 'F')) OR (T0.fss = 'G')) 
																			OR (T0.fss = 'H')) OR (T0.fss = 'K')) OR (T0.fss = 'U')) THEN 22
																		ELSE 19 END) END) 
				ELSE (CASE 	WHEN ((T0.product_holding = 'C. DTV + BB Only') OR (T0.product_holding = 'D. DTV + Other Comms')) THEN 0 
							WHEN (T0.product_holding = 'E. SABB') THEN (CASE WHEN ((((T0.life_stage = '1.') OR (T0.life_stage = '2.')) OR (T0.life_stage = '4.')) 
																				OR (T0.life_stage = '8.')) THEN 0 
																			WHEN (((((T0.life_stage = '3.') OR (T0.life_stage = '5.')) OR (T0.life_stage = '6.')) 
																					OR (T0.life_stage = '7.')) OR (T0.life_stage = 'U')) THEN 0 
																			ELSE 0 END) 
							ELSE (CASE WHEN ((((((((((((((((((((((T0.bb_type = 'Broadband Connect') OR (T0.bb_type = 'Sky Broadband 12GB')) 
												OR (T0.bb_type = 'Sky Broadband Everyday')) OR (T0.bb_type = 'Sky Broadband Lite')) 
												OR (T0.bb_type = 'Sky Broadband Lite (ROI - Legacy)')) OR (T0.bb_type = 'Sky Broadband Lite (ROI)')) 
												OR (T0.bb_type = 'Sky Broadband Unlimited (ROI - Legacy)')) OR (T0.bb_type = 'Sky Broadband Unlimited (ROI)')) 
												OR (T0.bb_type = 'Sky Broadband Unlimited Fibre')) OR (T0.bb_type = 'Sky Broadband Unlimited Pro')) 
												OR (T0.bb_type = 'Sky Connect Lite (ROI - Legacy)')) OR (T0.bb_type = 'Sky Connect Lite (ROI)')) 
												OR (T0.bb_type = 'Sky Connect Unlimited (ROI - Legacy)')) OR (T0.bb_type = 'Sky Connect Unlimited (ROI)')) 
												OR (T0.bb_type = 'Sky Fibre')) OR (T0.bb_type = 'Sky Fibre (ROI - Legacy)')) OR (T0.bb_type = 'Sky Fibre (ROI)')) 
												OR (T0.bb_type = 'Sky Fibre Lite')) OR (T0.bb_type = 'Sky Fibre Max')) OR (T0.bb_type = 'Sky Fibre Unlimited (ROI - Legacy)')) 
												OR (T0.bb_type = 'Sky Fibre Unlimited (ROI)')) OR (T0.bb_type = 'Sky Fibre Unlimited Pro')) THEN 0 
										ELSE 0 END) END) END) 					AS C0
FROM {TABLE_NAME} T0
