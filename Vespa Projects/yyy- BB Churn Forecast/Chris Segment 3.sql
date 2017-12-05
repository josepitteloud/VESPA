SELECT (CASE 	WHEN ((T0.product_holding = 'C. DTV + BB Only') OR (T0.product_holding = 'D. DTV + Other Comms')) THEN (CASE WHEN (((((((((((((((((T0.offer = 'BBT Exp +90') OR (T0.offer = 'BBT Exp -30')) OR (T0.offer = 'BBT Exp 31-90')) 
																																OR (T0.offer = 'BBT Live +90')) OR (T0.offer = 'BBT Live 31-90')) OR (T0.offer = 'Multi Exp +90')) 
																																OR (T0.offer = 'Multi Exp -30')) OR (T0.offer = 'Multi Exp 31-90')) OR (T0.offer = 'Multi Live +90')) 
																																OR (T0.offer = 'Multi Live -30')) OR (T0.offer = 'Multi Live 31-90')) OR (T0.offer = 'OTHER Exp +90')) 
																																OR (T0.offer = 'OTHER Exp -30')) OR (T0.offer = 'OTHER Exp 31-90')) OR (T0.offer = 'OTHER Live +90')) 
																																OR (T0.offer = 'OTHER Live -30')) OR (T0.offer = 'OTHER Live 31-90')) THEN 0 ELSE 0 END) 
		WHEN (T0.product_holding = 'E. SABB') THEN (CASE 	WHEN (((((T0.offer = 'BBT Exp +90') OR (T0.offer = 'Multi Exp -30')) OR (T0.offer = 'Multi Live +90')) OR (T0.offer = 'Multi Live -30')) OR (T0.offer = 'Multi Live 31-90')) THEN 10 
															WHEN (((((T0.offer = 'BBT Exp -30') OR (T0.offer = 'Multi Exp +90')) OR (T0.offer = 'OTHER Exp -30')) OR (T0.offer = 'OTHER Exp 31-90')) OR (T0.offer = 'OTHER Live 31-90')) THEN 11 
															WHEN ((((T0.offer = 'BBT Exp 31-90') OR (T0.offer = 'BBT Live -30')) OR (T0.offer = 'OTHER Exp +90')) OR (T0.offer = 'OTHER Live -30')) THEN 0 
															WHEN (T0.offer = 'BBT Live 31-90') THEN 0 
															WHEN (T0.offer = 'No Offer Ever') THEN (CASE WHEN ((T0.age = 'Age 18-25') OR (T0.age = 'Age 26-35')) THEN 0 
																										WHEN (T0.age = 'Age 36-45') THEN 0 
																										WHEN (T0.age = 'Age 46-55') THEN 0 ELSE 0 END) 
																										ELSE (CASE 	WHEN (T0.bb_tenure = '1 year') THEN 1 
																													WHEN ((T0.bb_tenure = '2 years') OR (T0.bb_tenure = '3 years')) THEN 2 
																													WHEN ((T0.bb_tenure = '4 years') OR (T0.bb_tenure = '5+ years')) THEN 3 ELSE 99 END) END)
															ELSE (CASE WHEN ((T0.offer = 'BBT Exp +90') OR (T0.offer = 'OTHER Exp +90')) THEN (CASE WHEN (((((((T0.life_stage = '1.') OR (T0.life_stage = '14')) OR (T0.life_stage = '3.')) 
																																							OR (T0.life_stage = '4.')) OR (T0.life_stage = '5.')) OR (T0.life_stage = '6.')) OR (T0.life_stage = '8.')) THEN 0 
																																					WHEN (((T0.life_stage = '11') OR (T0.life_stage = '2.')) OR (T0.life_stage = '7.')) THEN 0 
																																					WHEN ((T0.life_stage = '12') OR (T0.life_stage = 'U')) THEN 0 ELSE 0 END) 
															WHEN ((T0.offer = 'BBT Exp -30') OR (T0.offer = 'Multi Exp 31-90')) THEN (CASE WHEN (((((((T0.life_stage = '2.') OR (T0.life_stage = '3.')) OR (T0.life_stage = '4.')) OR (T0.life_stage = '6.')) OR (T0.life_stage = '7.')) 
																																					OR (T0.life_stage = '8.')) OR (T0.life_stage = '9.')) THEN 0 ELSE 0 END) 
															WHEN (T0.offer = 'BBT Exp 31-90') THEN (CASE WHEN (((((T0.life_stage = '10') OR (T0.life_stage = '12')) OR (T0.life_stage = '13')) OR (T0.life_stage = '9.')) OR (T0.life_stage = 'U')) THEN 0 ELSE 0 END) 
															WHEN (T0.offer = 'BBT Live +90') THEN (CASE WHEN (T0.bb_tenure = '1 year') THEN 5 
																										WHEN (((T0.bb_tenure = '2 years') OR (T0.bb_tenure = '3 years')) OR (T0.bb_tenure = '4 years')) THEN 8 
																										WHEN (T0.bb_tenure = '5+ years') THEN 7 ELSE 98 END) 
															WHEN ((T0.offer = 'BBT Live -30') OR (T0.offer = 'Multi Exp -30')) THEN 0 
															WHEN (T0.offer = 'BBT Live 31-90') THEN (CASE WHEN (T0.bb_tenure = 'Less 1 year') THEN 97 ELSE 96 END) 
															WHEN (T0.offer = 'Multi Exp +90') THEN 0 
															WHEN (T0.offer = 'Multi Live +90') THEN (CASE WHEN (((((((((((((((((((T0.bb_type = 'Broadband Connect') OR (T0.bb_type = 'Sky Broadband 12GB')) OR (T0.bb_type = 'Sky Broadband Everyday')) 
																											OR (T0.bb_type = 'Sky Broadband Lite')) OR (T0.bb_type = 'Sky Broadband Lite (ROI - Legacy)')) OR (T0.bb_type = 'Sky Broadband Unlimited (ROI - Legacy)')) 
																											OR (T0.bb_type = 'Sky Broadband Unlimited (ROI)')) OR (T0.bb_type = 'Sky Broadband Unlimited Fibre')) 
																											OR (T0.bb_type = 'Sky Broadband Unlimited Pro')) OR (T0.bb_type = 'Sky Connect Lite (ROI - Legacy)')) 
																											OR (T0.bb_type = 'Sky Connect Unlimited (ROI - Legacy)')) OR (T0.bb_type = 'Sky Connect Unlimited (ROI)')) 
																											OR (T0.bb_type = 'Sky Fibre')) OR (T0.bb_type = 'Sky Fibre (ROI - Legacy)')) 
																											OR (T0.bb_type = 'Sky Fibre Lite')) OR (T0.bb_type = 'Sky Fibre Max')) OR (T0.bb_type = 'Sky Fibre Unlimited (ROI - Legacy)')) 
																											OR (T0.bb_type = 'Sky Fibre Unlimited (ROI)')) OR (T0.bb_type = 'Sky Fibre Unlimited Pro')) THEN 4 ELSE 9 END) 
															WHEN ((T0.offer = 'Multi Live -30') OR (T0.offer = 'OTHER Live 31-90')) THEN (CASE WHEN (((((((((((((((((((((T0.bb_type = 'Broadband Connect') OR (T0.bb_type = 'Sky Broadband 12GB')) OR (T0.bb_type = 'Sky Broadband Everyday')) 
																																				OR (T0.bb_type = 'Sky Broadband Lite')) OR (T0.bb_type = 'Sky Broadband Lite (ROI - Legacy)')) OR (T0.bb_type = 'Sky Broadband Lite (ROI)')) 
																																				OR (T0.bb_type = 'Sky Broadband Unlimited (ROI - Legacy)')) OR (T0.bb_type = 'Sky Broadband Unlimited (ROI)')) OR (T0.bb_type = 'Sky Broadband Unlimited Fibre')) 
																																				OR (T0.bb_type = 'Sky Broadband Unlimited Pro')) OR (T0.bb_type = 'Sky Connect Lite (ROI - Legacy)')) OR (T0.bb_type = 'Sky Connect Unlimited (ROI - Legacy)')) 
																																				OR (T0.bb_type = 'Sky Connect Unlimited (ROI)')) OR (T0.bb_type = 'Sky Fibre')) OR (T0.bb_type = 'Sky Fibre (ROI - Legacy)')) OR (T0.bb_type = 'Sky Fibre (ROI)')) 
																																				OR (T0.bb_type = 'Sky Fibre Lite')) OR (T0.bb_type = 'Sky Fibre Max')) OR (T0.bb_type = 'Sky Fibre Unlimited (ROI - Legacy)')) 
																																				OR (T0.bb_type = 'Sky Fibre Unlimited (ROI)')) OR (T0.bb_type = 'Sky Fibre Unlimited Pro')) THEN 13 ELSE 0 END) 
															WHEN ((T0.offer = 'Multi Live 31-90') OR (T0.offer = 'OTHER Live -30')) THEN (CASE WHEN (((T0.age = 'Age 46-55') OR (T0.age = 'Age 56-65')) OR (T0.age = 'Age 66+')) THEN 0 ELSE 0 END) 
															WHEN (T0.offer = 'OTHER Exp -30') THEN 0 
															WHEN (T0.offer = 'OTHER Exp 31-90') THEN 0 
															WHEN (T0.offer = 'OTHER Live +90') THEN (CASE WHEN ((((((T0.bb_type = 'Sky Broadband Everyday') OR (T0.bb_type = 'Sky Broadband Unlimited Fibre')) OR (T0.bb_type = 'Sky Fibre')) OR (T0.bb_type = 'Sky Fibre Lite')) 
																												OR (T0.bb_type = 'Sky Fibre Max')) OR (T0.bb_type = 'Sky Fibre Unlimited Pro')) THEN 6 ELSE 12 END) 
															ELSE (CASE WHEN ((((((((((T0.bb_type = 'Sky Broadband 12GB') OR (T0.bb_type = 'Sky Broadband Lite')) OR (T0.bb_type = 'Sky Broadband Lite (ROI)')) OR (T0.bb_type = 'Sky Broadband Unlimited Fibre')) 
																				OR (T0.bb_type = 'Sky Broadband Unlimited Pro')) OR (T0.bb_type = 'Sky Fibre')) OR (T0.bb_type = 'Sky Fibre (ROI)')) OR (T0.bb_type = 'Sky Fibre Lite')) 
																				OR (T0.bb_type = 'Sky Fibre Max')) OR (T0.bb_type = 'Sky Fibre Unlimited Pro')) THEN 0 ELSE 0 END) END) END) AS C0
FROM {TABLE_NAME} T0
