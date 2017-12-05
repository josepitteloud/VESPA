         -- Daily Standalone Report 
		 --This SQL gets Accounts that are Standalone for an Opening Date SnapShot and then gets Accounts
         -- that are Standalone for a Closing Date SnapShot and then analyses all the Accounts for their
         -- latest TV, BroadBand, SkyTalk and Line Rental statuses and Activation dates.
         --
		 
		 
         -- Daily Standalone Report 
		 --This SQL gets Accounts that are Standalone for an Opening Date SnapShot and then gets Accounts
         -- that are Standalone for a Closing Date SnapShot and then analyses all the Accounts for their
         -- latest TV, BroadBand, SkyTalk and Line Rental statuses and Activation dates.
         --
		 
		 
WITH 		SQ AS 		( SELECT 	SH.OWNING_CUST_ACCOUNT_ID,
									Cast(To_Char(SnapShotDate,'YYYY-MM-DD') || ' 00:00:00' As Timestamp) AS  SnapShotDateTime,
									MAX(CASE WHEN (SH.SUBSCRIPTION_SUB_TYPE IN ('DTV Primary Viewing','DTV Sky+') OR SH.SUBSCRIPTION_TYPE = 'NOW TV')  AND SH.STATUS_CODE In ('AB','AC','PC')  Then PD.DESCRIPTION End) TVSub,
									MAX(CASE WHEN (SH.SUBSCRIPTION_TYPE = 'NOW TV')  AND SH.STATUS_CODE In ('AB','AC','PC')  Then PD.DESCRIPTION End) now_tv,
									Max(Case When SH.SUBSCRIPTION_SUB_TYPE='DTV Extra Subscription' Then PD.DESCRIPTION End) MRSub,
									Max(Case When SH.SUBSCRIPTION_SUB_TYPE='DTV HD' Then PD.DESCRIPTION End) HDSub,
									Max(Case When SH.SUBSCRIPTION_SUB_TYPE IN ('Broadband DSL Line','NOW_TV_2.0_BROADBAND_LINE') Then PD.DESCRIPTION End) BBSub,
									
									Max(Case When SH.SUBSCRIPTION_SUB_TYPE IN ('SKY TALK SELECT','NOW_TV_2.0_TALK') Then PD.DESCRIPTION End) STSub,
									Max(Case When SH.SUBSCRIPTION_SUB_TYPE IN ('SKY TALK LINE RENTAL','NOW_TV_2.0_LINE_RENTAL') Then PD.DESCRIPTION End) LRSub,
									
									Max(Case When (SH.SUBSCRIPTION_SUB_TYPE IN ('DTV Primary Viewing','DTV Sky+') OR SH.SUBSCRIPTION_TYPE = 'NOW TV') Then Cast(SH.STATUS_START_DT As Date) End) TVStatusDate,
									MAX(Case When (SubStr(SH.SUBSCRIPTION_SUB_TYPE,1,3)<>'DTV' OR SH.SUBSCRIPTION_TYPE <> 'NOW TV') Then Cast(SH.STATUS_START_DT As Date) End) CommsLastActiveDate,
									Max(Case When SH.SUBSCRIPTION_SUB_TYPE IN ('Broadband DSL Line','NOW_TV_2.0_BROADBAND_LINE') Then SH.TECHNOLOGY_CODE End) BBTechCode,
									Max(Case When SH.SUBSCRIPTION_SUB_TYPE IN ('SKY TALK SELECT','NOW_TV_2.0_TALK') Then SH.TECHNOLOGY_CODE End) STTechCode,
									Max(Case When SH.SUBSCRIPTION_SUB_TYPE IN ('SKY TALK LINE RENTAL','NOW_TV_2.0_LINE_RENTAL')Then SH.TECHNOLOGY_CODE End) LRTechCode
							FROM ( 	SELECT CAST(CALENDAR_DATE as DATE) SnapShotDate FROM WH_CALENDAR_DIM WHERE CALENDAR_DATE = '2017-03-23' ) DateParameter -- Period From
							JOIN WH_PH_SUBS_HIST SH ON DateParameter.SnapShotDate Is Not Null
							JOIN WH_PRODUCT_DIM PD ON PRODUCT_SK=ENT_CAT_PROD_SK
							WHERE 	SI_LATEST_SRC='CHORD'
									AND SnapShotDateTime BETWEEN SH.EFFECTIVE_FROM_DT AND SH.EFFECTIVE_TO_DT
									AND (   (SH.SUBSCRIPTION_SUB_TYPE IN ('DTV Primary Viewing','DTV Sky+') OR SH.SUBSCRIPTION_TYPE = 'NOW TV')
											OR (SH.SUBSCRIPTION_SUB_TYPE In ('DTV Extra Subscription','DTV HD') AND SH.STATUS_CODE In ('AB','AC','PC'))
											OR (SH.SUBSCRIPTION_SUB_TYPE IN ('Broadband DSL Line','NOW_TV_2.0_BROADBAND_LINE')
												AND (   (SH.STATUS_CODE In ('AC','AB','PC','PT','CF','BCRQ') AND Cast(SH.FIRST_ACTIVATION_DT As Date)<'9999-09-09')
												OR (SH.STATUS_CODE='AP' AND SH.SALE_TYPE='SNS Bulk Migration' AND Cast(SH.FIRST_ACTIVATION_DT As Date)<'9999-09-09')))
											OR (SH.SUBSCRIPTION_SUB_TYPE IN ('SKY TALK SELECT','NOW_TV_2.0_TALK')
												AND (   (SH.STATUS_CODE='A' AND Cast(SH.FIRST_ACTIVATION_DT As Date)<'9999-09-09')
												OR (SH.STATUS_CODE In ('PC','FBP','BCRQ') AND Cast(SH.STATUS_START_DT As Date)>Cast(SH.FIRST_ACTIVATION_DT As Date))
												OR (SH.STATUS_CODE='FBI' AND SH.PREV_STATUS_CODE='A' AND Cast(SH.FIRST_ACTIVATION_DT As Date)<'9999-09-09' AND (SnapShotDate-Cast(SH.STATUS_START_DT As Date))<60)
												OR (SH.STATUS_CODE='RI' AND SH.PREV_STATUS_CODE IN ('FBP','BCRQ') AND Cast(SH.STATUS_START_DT As Date)>Cast(SH.FIRST_ACTIVATION_DT As Date) 
														AND Cast(SH.FIRST_ACTIVATION_DT As Date)<'9999-09-09')
												OR (SH.STATUS_CODE='PR' AND SH.PREV_STATUS_CODE='A' AND Cast(SH.FIRST_ACTIVATION_DT As Date)<'9999-09-09')))
												OR (SH.SUBSCRIPTION_SUB_TYPE IN ('SKY TALK LINE RENTAL','NOW_TV_2.0_LINE_RENTAL') AND SH.STATUS_CODE In ('A','R','CRQ','BCRQ'))
										)
							GROUP BY SnapShotDate
									,SH.OWNING_CUST_ACCOUNT_ID
							HAVING (BBSub Is Not Null OR now_tv Is Not Null)
							LIMIT 100
							) 		
							
							
			, OC AS ( 	SELECT *,
								Case When TVSub Is Null And (BBSub Is Not Null OR STSub Is Not Null OR LRSub Is Not Null) Then 'Y' Else 'N' End StandAloneFlag,
								Case When TVSub Is Null And (MRSub Is Not Null OR HDSub Is Not Null) Then 'Y' Else 'N' End FreeSatPlusFlag,
								Case When TVSub Is Null And CommsLastActiveDate Is Not Null Then Case When NVL(CommsLastActiveDate,'9999-09-09')<'2010-01-15' AND NVL(TVStatusDate,'9999-09-09')<'2010-01-15' Then 'Y' Else 'N' End Else 'N' End LegacyFlag
						FROM   SQ
						WHERE StandAloneFlag='Y' AND FreeSatPlusFlag='N' /*AND LegacyFlag='N'*/)	
						
			, SQ2 AS ( 			SELECT 	SH.OWNING_CUST_ACCOUNT_ID,
										Cast(To_Char(SnapShotDate,'YYYY-MM-DD') || ' 00:00:00' As Timestamp) SnapShotDateTime,
										MAX(CASE WHEN (SH.SUBSCRIPTION_SUB_TYPE IN ('DTV Primary Viewing','DTV Sky+') OR SH.SUBSCRIPTION_TYPE = 'NOW TV')  AND SH.STATUS_CODE In ('AB','AC','PC')  Then PD.DESCRIPTION End) TVSub,
										MAX(CASE WHEN (SH.SUBSCRIPTION_TYPE = 'NOW TV')  AND SH.STATUS_CODE In ('AB','AC','PC')  Then PD.DESCRIPTION End) now_tv,
										Max(Case When SH.SUBSCRIPTION_SUB_TYPE='DTV Extra Subscription' Then PD.DESCRIPTION End) MRSub,
										Max(Case When SH.SUBSCRIPTION_SUB_TYPE='DTV HD' Then PD.DESCRIPTION End) HDSub,
										Max(Case When SH.SUBSCRIPTION_SUB_TYPE IN ('Broadband DSL Line','NOW_TV_2.0_BROADBAND_LINE') Then PD.DESCRIPTION End) BBSub,
										Max(Case When SH.SUBSCRIPTION_SUB_TYPE IN ('SKY TALK SELECT','NOW_TV_2.0_TALK') Then PD.DESCRIPTION End) STSub,
										Max(Case When SH.SUBSCRIPTION_SUB_TYPE IN ('SKY TALK LINE RENTAL','NOW_TV_2.0_LINE_RENTAL') Then PD.DESCRIPTION End) LRSub,
										MAX(Case When (SH.SUBSCRIPTION_SUB_TYPE in ('DTV Primary Viewing','DTV Sky+')OR SH.SUBSCRIPTION_TYPE = 'NOW TV') Then Cast(SH.STATUS_START_DT As Date) End) TVStatusDate,
										Max(Case When (SubStr(SH.SUBSCRIPTION_SUB_TYPE,1,3)<>'DTV' OR SH.SUBSCRIPTION_TYPE <> 'NOW TV') Then Cast(SH.STATUS_START_DT As Date) End) CommsLastActiveDate,
										Max(Case When SH.SUBSCRIPTION_SUB_TYPE IN ('Broadband DSL Line','NOW_TV_2.0_BROADBAND_LINE') Then SH.TECHNOLOGY_CODE End) BBTechCode,
										Max(Case When SH.SUBSCRIPTION_SUB_TYPE IN ('SKY TALK SELECT','NOW_TV_2.0_TALK') Then SH.TECHNOLOGY_CODE End) STTechCode,
										Max(Case When SH.SUBSCRIPTION_SUB_TYPE IN ('SKY TALK LINE RENTAL','NOW_TV_2.0_LINE_RENTAL') Then SH.TECHNOLOGY_CODE End) LRTechCode
								FROM (  	SELECT CAST(CALENDAR_DATE as DATE) + 1 SnapShotDate  FROM WH_CALENDAR_DIM WHERE CALENDAR_DATE = '2017-03-23') AS a 
								JOIN WH_PH_SUBS_HIST SH  ON SnapShotDate Is Not Null
								JOIN WH_PRODUCT_DIM PD ON PRODUCT_SK=ENT_CAT_PROD_SK
								WHERE 				SI_LATEST_SRC='CHORD'
													AND SnapShotDateTime  BETWEEN SH.EFFECTIVE_FROM_DT AND SH.EFFECTIVE_TO_DT
													AND (   (SH.SUBSCRIPTION_SUB_TYPE in ('DTV Primary Viewing','DTV Sky+'))
														 OR (SH.SUBSCRIPTION_TYPE = 'NOW TV')
														 OR (SH.SUBSCRIPTION_SUB_TYPE In ('DTV Extra Subscription','DTV HD') AND SH.STATUS_CODE In ('AB','AC','PC'))
														 OR (SH.SUBSCRIPTION_SUB_TYPE IN ('Broadband DSL Line','NOW_TV_2.0_BROADBAND_LINE')
																AND ((SH.STATUS_CODE In ('AC','AB','PC','PT','CF','BCRQ') AND Cast(SH.FIRST_ACTIVATION_DT As Date)<'9999-09-09')
																 OR (SH.STATUS_CODE='AP' AND SH.SALE_TYPE='SNS Bulk Migration' AND Cast(SH.FIRST_ACTIVATION_DT As Date)<'9999-09-09')))OR (SH.SUBSCRIPTION_SUB_TYPE IN ('SKY TALK SELECT','NOW_TV_2.0_TALK')
															AND ((SH.STATUS_CODE='A' AND Cast(SH.FIRST_ACTIVATION_DT As Date)<'9999-09-09')
																 OR (SH.STATUS_CODE In ('PC','FBP','BCRQ') AND Cast(SH.STATUS_START_DT As Date)>Cast(SH.FIRST_ACTIVATION_DT As Date))
																 OR (SH.STATUS_CODE='FBI' AND SH.PREV_STATUS_CODE='A' AND Cast(SH.FIRST_ACTIVATION_DT As Date)<'9999-09-09' AND (SnapShotDate-Cast(SH.STATUS_START_DT As Date))<60)
																 OR (SH.STATUS_CODE='RI' AND SH.PREV_STATUS_CODE IN ('FBP','BCRQ') AND Cast(SH.STATUS_START_DT As Date)>Cast(SH.FIRST_ACTIVATION_DT As Date) AND Cast(SH.FIRST_ACTIVATION_DT As Date)<'9999-09-09')
																 OR (SH.STATUS_CODE='PR' AND SH.PREV_STATUS_CODE='A' AND Cast(SH.FIRST_ACTIVATION_DT As Date)<'9999-09-09')))
															OR (SH.SUBSCRIPTION_SUB_TYPE IN ('SKY TALK LINE RENTAL','NOW_TV_2.0_TALK') AND SH.STATUS_CODE In ('A','R','CRQ','BCRQ')))
								GROUP BY SnapShotDate
										,SH.OWNING_CUST_ACCOUNT_ID
								HAVING (BBSub Is Not Null OR now_tv Is Not Null)) 			

			, CC AS ( SELECT *,
							Case When TVSub Is Null And (BBSub Is Not Null OR STSub Is Not Null OR LRSub Is Not Null) Then 'Y' Else 'N' End StandaloneFlag,
							Case When TVSub Is Null And (MRSub Is Not Null OR HDSub Is Not Null) Then 'Y' Else 'N' End FreeSatPlusFlag,
							Case When TVSub Is Null And CommsLastActiveDate Is Not Null Then Case When NVL(CommsLastActiveDate,'9999-09-09')<'2010-01-15' AND NVL(TVStatusDate,'9999-09-09')<'2010-01-15' Then 'Y' Else 'N' End Else 'N' End LegacyFlag
						FROM  SQ2
						WHERE StandAloneFlag='Y' AND FreeSatPlusFlag='N' /*AND LegacyFlag='N'*/)
			, TQ AS (	SELECT OC.OWNING_CUST_ACCOUNT_ID OC_CAID, OC.SnapShotDateTime OC_SnapShotDateTime,OC.StandAloneFlag OC_StandAloneFlag,OC.FreeSatPlusFlag OC_FreeSatPlusFlag,OC.LegacyFlag OC_LegacyFlag,
								  OC.TVSub OC_TVSub,OC.MRSub OC_MRSub,OC.HDSub OC_HDSub,OC.BBSub OC_BBSub,OC.STSub OC_STSub,OC.LRSub OC_LRSub,
								  CC.OWNING_CUST_ACCOUNT_ID CC_CAID, CC.SnapShotDateTime CC_SnapShotDateTime,CC.StandAloneFlag CC_StandAloneFlag,CC.FreeSatPlusFlag CC_FreeSatPlusFlag,CC.LegacyFlag CC_LegacyFlag,
								  CC.TVSub CC_TVSub,CC.MRSub CC_MRSub,CC.HDSub CC_HDSub,CC.BBSub CC_BBSub,CC.STSub CC_STSub,CC.LRSub CC_LRSub,
								  CC.BBTechCode CC_BBTechCode,CC.STTechCode CC_STTechCode,CC.LRTechCode CC_LRTechCode , OC.now_tv oc_now_tv, CC.now_tv cc_now_tv
						FROM 		  	OC 				
						FULL JOIN		CC ON CC.OWNING_CUST_ACCOUNT_ID=OC.OWNING_CUST_ACCOUNT_ID
						WHERE  (OC.TVSub Is Null AND OC.OWNING_CUST_ACCOUNT_ID Is Not Null) 
							OR (CC.TVSub Is Null AND CC.OWNING_CUST_ACCOUNT_ID Is Not Null))
															
SELECT   Max(CU.ACCOUNT_NUMBER) ACCNO
		,Max(CU.SRC_SYSTEM_ID) SYSTEM_ID
		,DECODE(CU.ORGANISATION_UNIT,10,'SKY',20,'NOW TV') ORGUNIT
		,Max(Case When OC_TVSub Is Null Then 0 Else 1 End) OC_TVActive
		,Max(Case When OC_BBSub Is Null Then 0 Else 1 End) OC_BBActive
		,Max(Case When OC_now_tv Is Null Then 0 Else 1 End) OC_Now_tv_active 
		,Max(Case When CC_now_tv Is Null Then 0 Else 1 End) CC_Now_tv_active 
        ,Max(Case When OC_STSub Is Null Then 0 Else 1 End) OC_STActive
		,Max(Case When OC_LRSub Is Null Then 0 Else 1 End) OC_LRActive
		,Max(Case When CC_TVSub Is Null Then Case When (SUBSCRIPTION_SUB_TYPE ='DTV Primary Viewing' or SUBSCRIPTION_TYPE ='NOW TV') AND SH.STATUS_CODE In ('AB','AC','PC') Then 1 Else 0 End Else 1 End) CC_TVActive
		,Max(Case When CC_BBSub Is Null Then 0 Else 1 End) CC_BBActive,         Max(Case When CC_STSub Is Null Then 0 Else 1 End) CC_STActive,
         Max(Case When CC_LRSub Is Null Then 0 Else 1 End) CC_LRActive,
         Max(Case When SUBSCRIPTION_SUB_TYPE IN ('Broadband DSL Line','NOW_TV_2.0_BROADBAND_LINE') Then PD.DESCRIPTION End) BBSub,
         Max(Case When SUBSCRIPTION_SUB_TYPE IN ('Broadband DSL Line','NOW_TV_2.0_BROADBAND_LINE') Then SH.STATUS_CODE End) BBStatusCode,
         Max(Case When SUBSCRIPTION_SUB_TYPE IN ('Broadband DSL Line','NOW_TV_2.0_BROADBAND_LINE') Then STATUS_START_DT End) BBStatusDate,
         Max(Case When SUBSCRIPTION_SUB_TYPE IN ('Broadband DSL Line','NOW_TV_2.0_BROADBAND_LINE') Then FIRST_ACTIVATION_DT End) BBActiveDate,
         Max(CC_BBTechCode) BBTC,          Max(CC_STTechCode) STTC,          Max(CC_LRTechCode) LRTC,         Max(1) CustCount,         Max(0) ForDummyBOJoin
FROM (SELECT Cast(To_Char(CALENDAR_DATE + 1, 'YYYY-MM-DD') || ' 00:00:00' AS TIMESTAMP) SnapShotDateTime   FROM WH_CALENDAR_DIM WHERE CALENDAR_DATE = '2017-03-23' ) 		AS d
JOIN  TQ ON d.SnapShotDateTime IS Not Null   
LEFT JOIN WH_CUST_ACCOUNT_FO CU ON CU.SRC_SYSTEM_ID=NVL(OC_CAID,CC_CAID)
LEFT JOIN WH_PH_SUBS_HIST SH ON EFFECTIVE_FROM_DT	<=	d.SnapShotDateTime
							AND EFFECTIVE_TO_DT		>	d.SnapShotDateTime
							AND SH.OWNING_CUST_ACCOUNT_ID=NVL(OC_CAID,CC_CAID)
							AND (SUBSCRIPTION_SUB_TYPE In ('DTV Primary Viewing','DTV Sky+','Broadband DSL Line','SKY TALK SELECT','SKY TALK LINE RENTAL','NOW_TV_2.0_BROADBAND_LINE','NOW_TV_2.0_TALK','NOW_TV_2.0_LINE_RENTAL') OR (SUBSCRIPTION_TYPE = 'NOW TV'))
LEFT JOIN WH_PRODUCT_DIM PD ON PRODUCT_SK	=	ENT_CAT_PROD_SK
GROUP BY OC_CAID,CC_CAID
	,DECODE(CU.ORGANISATION_UNIT,10,'SKY',20,'NOW TV')




