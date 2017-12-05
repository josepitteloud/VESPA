UPDATE BB_CHURN_calls_details_raw_3yr_final
SET segment = (CASE WHEN (((((T0.Offer = 'BBT Exp +90') OR (T0.Offer = 'BBT Live -30')) OR (T0.Offer = 'BBT Live 31-90')) OR (T0.Offer = 'Multi Exp +90')) OR (T0.Offer = 'OTHER Live 31-90')) 
									THEN CASE 	WHEN (((T0.product_holding = 'C. DTV + BB Only') OR (T0.product_holding = 'D. DTV + Other Comms')) OR (T0.product_holding = 'E. SABB')) 
													THEN (CASE 	WHEN (((T0.age = 'Age 46-55') OR (T0.age = 'Age 56-65')) OR (T0.age = 'Age 66+')) 			THEN 12	--0.071969341173376403 
																ELSE  7 /*0.090568659162695797 */END) 
												ELSE (CASE 	WHEN ((((T0.bb_tenure = '2 years') OR (T0.bb_tenure = '3 years')) OR (T0.bb_tenure = '4 years')) OR (T0.bb_tenure = 'Less 1 year')) 		THEN  23	--0.0407354305473559 
															WHEN (T0.bb_tenure = '5+ years') 			THEN	0 -- 0.029488344673429801 
															ELSE 0 /*0.0200276053711642 END)*/ END) END
				WHEN (T0.Offer = 'BBT Exp -30') THEN (CASE 	WHEN ((T0.product_holding = 'D. DTV + Other Comms') OR (T0.product_holding = 'E. SABB')) THEN 1 	--0.46219288445177897 
															ELSE (CASE 	WHEN (T0.bb_type = 'Sky Broadband Unlimited Fibre') THEN 2		-- 0.15791238890234599 
																		ELSE (CASE 	WHEN ((T0.bb_tenure = '1 year') OR (T0.bb_tenure = 'Less 1 year')) THEN 5 	--0.121322560814964 
																					WHEN (((T0.bb_tenure = '2 years') OR (T0.bb_tenure = '3 years')) OR (T0.bb_tenure = '4 years')) 		THEN	14	-- 0.067305186343497594 
																					ELSE 17 	/*0.057837933291090597 */ END) END) END) 
				WHEN (T0.Offer = 'BBT Exp 31-90') THEN (CASE WHEN (((T0.product_holding = 'C. DTV + BB Only') OR (T0.product_holding = 'D. DTV + Other Comms')) OR (T0.product_holding = 'E. SABB')) 
																THEN (CASE 	WHEN (((((T0.bb_tenure = '1 year') OR (T0.bb_tenure = '2 years')) OR (T0.bb_tenure = '3 years')) OR (T0.bb_tenure = '4 years')) OR (T0.bb_tenure = '5+ years')) 		THEN 6 	--0.095081252573199895 
																			ELSE 0 /*0.0366479127164216 */ END) 
															ELSE (CASE 	WHEN (((T0.bb_tenure = '2 years') OR (T0.bb_tenure = '3 years')) OR (T0.bb_tenure = '4 years')) 		THEN 0 		--0.033736153071500602 
																			WHEN (T0.bb_tenure = '5+ years') 		THEN 0 		--0.027494042725157101 
																			ELSE 0 /*0.018916031018905401 */ END) END) 
				WHEN (T0.Offer = 'BBT Live +90') THEN (CASE WHEN (T0.bb_tenure = '1 year') THEN  0	--0.0251699380306298 
															WHEN (T0.bb_tenure = '2 years') THEN 0	--0.022279173771429201 
															WHEN (T0.bb_tenure = '3 years') THEN 0	--0.024865823774373401 
															WHEN (T0.bb_tenure = '4 years') THEN 0	--0.023571669098664299 
															WHEN (T0.bb_tenure = '5+ years') THEN (CASE WHEN ((((((((T0.FSS = '') OR (T0.FSS = 'A')) OR (T0.FSS = 'D')) OR (T0.FSS = 'F')) OR (T0.FSS = 'G')) OR (T0.FSS = 'H')) OR (T0.FSS = 'L')) OR (T0.FSS = 'U')) THEN 0 --0.020821778004570399 
																										ELSE 0 /* 0.0227527199746488 */END) 
															ELSE (CASE WHEN (T0.product_holding = 'E. SABB') THEN (CASE 	WHEN (((T0.age = 'Age 18-25') OR (T0.age = 'Age 56-65')) OR (T0.age = 'Age 66+')) 		THEN	0  --0.015411170016027599 
																															WHEN ((T0.age = 'Age 46-55') OR (T0.age = 'Unclassified')) 		THEN 0 		--0.012902989090553299 
																															ELSE 0 /*0.011767332396907901 */ END) 
																							ELSE (CASE 	WHEN ((((T0.FSS = '') OR (T0.FSS = 'A')) OR (T0.FSS = 'H')) OR (T0.FSS = 'U')) 		THEN 0 		--0.0084012909901697505 
																										WHEN (((((T0.FSS = 'B') OR (T0.FSS = 'D')) OR (T0.FSS = 'E')) OR (T0.FSS = 'F')) OR (T0.FSS = 'G')) 		THEN 0	--0.0089692958059850898 
																										ELSE 0  /*0.010710271256177 */ END) END) END) 
				WHEN (T0.Offer = 'Multi Exp -30') THEN (CASE	WHEN ((((((T0.FSS = 'C') OR (T0.FSS = 'D')) OR (T0.FSS = 'F')) OR (T0.FSS = 'G')) OR (T0.FSS = 'H')) OR (T0.FSS = 'I')) THEN 4		--0.12218879816645201 
																ELSE 3	/*0.13822765150383801 */ END) 
				WHEN (T0.Offer = 'Multi Exp 31-90') THEN 22		--0.040746737095359198 
				WHEN (T0.Offer = 'Multi Live +90') THEN (CASE 	WHEN ((((T0.FSS = '') OR (T0.FSS = 'B')) OR (T0.FSS = 'C')) OR (T0.FSS = 'H')) THEN 0 		--0.022611353170751899 
																WHEN (((T0.FSS = 'A') OR (T0.FSS = 'E')) OR (T0.FSS = 'I')) THEN 0		--0.024297113646281399 
																ELSE 0	/*0.028232410106399399 */ END) 
				WHEN (T0.Offer = 'OTHER Exp +90') THEN (CASE 	WHEN ((((T0.bb_tenure = '1 year') OR (T0.bb_tenure = '2 years')) OR (T0.bb_tenure = '4 years')) OR (T0.bb_tenure = 'Less 1 year')) THEN 21	--0.041045047667564297 
																ELSE 0 /*0.029204183440377798 */ END) 
				WHEN (T0.Offer = 'OTHER Exp -30') THEN (CASE 	WHEN (((((T0.FSS = '') OR (T0.FSS = 'B')) OR (T0.FSS = 'C')) OR (T0.FSS = 'E')) OR (T0.FSS = 'F')) THEN 9 --0.081942630484206497 
																WHEN (((T0.FSS = 'G') OR (T0.FSS = 'H')) OR (T0.FSS = 'I')) THEN 11 --0.076030169242089995 
																ELSE 8 		--0.086027772515991696
																END) 
				WHEN (T0.Offer = 'OTHER Exp 31-90') THEN (CASE 	WHEN ((T0.bb_tenure = '1 year') OR (T0.bb_tenure = 'Less 1 year')) THEN 0 		--0.023675916673072499 
																ELSE (CASE 	WHEN (((T0.age = 'Age 18-25') OR (T0.age = 'Age 26-35')) OR (T0.age = 'Age 36-45')) THEN 0 		--0.030293689048313002 
																			ELSE 0 	/* 0.025213527737037399 */  END) END) 
				WHEN (T0.Offer = 'OTHER Live +90') THEN (CASE 	WHEN ((((((((T0.bb_type = 'Broadband Connect') OR (T0.bb_type = 'Sky Broadband 12GB')) OR (T0.bb_type = 'Sky Broadband Everyday')) OR (T0.bb_type = 'Sky Broadband Lite')) 
																		OR (T0.bb_type = 'Sky Broadband Unlimited Fibre')) OR (T0.bb_type = 'Sky Broadband Unlimited Pro')) OR (T0.bb_type = 'Sky Fibre')) OR (T0.bb_type = 'Sky Fibre Unlimited Pro')) 
																					THEN (CASE 	WHEN ((((((((T0.FSS = 'A') OR (T0.FSS = 'I')) OR (T0.FSS = 'J')) OR (T0.FSS = 'K')) OR (T0.FSS = 'L')) OR (T0.FSS = 'M')) OR (T0.FSS = 'N')) OR (T0.FSS = 'U')) THEN 13 	--0.0678449588497195 
																								ELSE 16  /*0.0581719402384425 */ END) 
																ELSE (CASE 	WHEN ((T0.bb_tenure = '1 year') OR (T0.bb_tenure = '4 years')) THEN (CASE 	WHEN (((((((T0.FSS = '') OR (T0.FSS = 'B')) OR (T0.FSS = 'C')) OR (T0.FSS = 'D')) 
																																							OR (T0.FSS = 'E')) OR (T0.FSS = 'N')) OR (T0.FSS = 'U')) THEN 19 	--0.043658449446230797 
																																						ELSE 0  /* 0.0371177474158077 */ END) 	
																			WHEN (T0.bb_tenure = '2 years') THEN 24 	--0.039010349684610202 
																			WHEN (T0.bb_tenure = '3 years') THEN 0		--0.038385269121812997 
																			WHEN (T0.bb_tenure = 'Less 1 year') THEN (CASE 	WHEN (((((T0.FSS = '') OR (T0.FSS = 'E')) OR (T0.FSS = 'H')) OR (T0.FSS = 'I')) OR (T0.FSS = 'U')) THEN 0 --0.026595120315251099 
																															ELSE 0 /*0.030118734218019701 */ END) 
																			ELSE (CASE 	WHEN (((T0.age = 'Age 18-25') OR (T0.age = 'Age 26-35')) OR (T0.age = 'Age 36-45')) THEN 20 	--0.0419366322534711 
																						ELSE 0 	/*0.034859421625617101*/  END) END) END) 
				ELSE (CASE 	WHEN ((T0.product_holding = 'C. DTV + BB Only') OR (T0.product_holding = 'D. DTV + Other Comms')) THEN  0		--0.013732760940004 
							WHEN (T0.product_holding = 'E. SABB') THEN (CASE 	WHEN ((T0.age = 'Age 18-25') OR (T0.age = 'Age 26-35')) THEN 10 -- 0.078304239401496195 
																				WHEN (T0.age = 'Age 46-55') THEN 18 	--0.048419009220890798 
																				WHEN ((T0.age = 'Age 56-65') OR (T0.age = 'Age 66+')) THEN 25 		--0.038948570242358101 
																				ELSE 15 	/* 0.059921425111673199 */ END) 
							ELSE (CASE 	WHEN ((((((((T0.bb_type = 'Broadband Connect') OR (T0.bb_type = 'Sky Broadband 12GB')) OR (T0.bb_type = 'Sky Broadband Everyday')) OR (T0.bb_type = 'Sky Broadband Lite')) OR (T0.bb_type = 'Sky Broadband Unlimited Fibre')) 
												OR (T0.bb_type = 'Sky Broadband Unlimited Pro')) OR (T0.bb_type = 'Sky Fibre')) OR (T0.bb_type = 'Sky Fibre Unlimited Pro')) THEN 0 	--0.032349027311323802 
										ELSE (CASE 	WHEN (((T0.bb_tenure = '1 year') OR (T0.bb_tenure = '2 years')) OR (T0.bb_tenure = 'Less 1 year')) THEN (CASE	WHEN ((((((T0.FSS = '') OR (T0.FSS = 'A')) OR (T0.FSS = 'B')) OR (T0.FSS = 'C')) OR (T0.FSS = 'I')) OR (T0.FSS = 'N')) THEN 0 --0.0231400806920046 
																																									ELSE 0 /*0.017947272512749302 */ END) 
													WHEN (T0.bb_tenure = '3 years') THEN (CASE 	WHEN (((((((T0.FSS = 'F') OR (T0.FSS = 'G')) OR (T0.FSS = 'H')) OR (T0.FSS = 'J')) OR (T0.FSS = 'L')) OR (T0.FSS = 'M')) OR (T0.FSS = 'N')) THEN 0	--0.0111224605606628 
																								ELSE 0 /*0.0138119597812665 */ END) 
													WHEN (T0.bb_tenure = '4 years') THEN (CASE 	WHEN (((((((T0.FSS = 'F') OR (T0.FSS = 'G')) OR (T0.FSS = 'H')) OR (T0.FSS = 'J')) OR (T0.FSS = 'L')) OR (T0.FSS = 'M')) OR (T0.FSS = 'N')) THEN 0	--0.0106805526520838 
																								ELSE 0 /*0.014627152786599601 */ END) 
													ELSE (CASE 	WHEN (((T0.age = 'Age 18-25') OR (T0.age = 'Age 26-35')) OR (T0.age = 'Age 36-45')) THEN 0		--0.016050568259763798 
																WHEN (T0.age = 'Age 56-65') THEN 0		--0.012175184993976899 
																WHEN (T0.age = 'Age 66+') THEN 0		--0.010625694187338001 
																ELSE 0	/*0.0135210262972947 */ END) END) END) END) END) 

FROM  BB_CHURN_calls_details_raw_3yr_final AS T0 
