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
**Project Name:                   		RSMB CHANNEL MAPPING CHECKS
**Analysts:                             Angel Donnarumma (angel.donnarumma_mirabel@skyiq.co.uk)
**Lead(s):                              
**Stakeholder:                          
**Due Date:                             29/10/2013
**Project Code (Insight Collation):     
**Sharepoint Folder:                    
                                                                        
**Business Brief:

QAing RSMB logic for deriving/extracting Weights, Service Keys and Vosdal Definitions/aggregations.

This has been compared for 1 day worth of data (2013-03-01)

*/

------------------
-- ASSEMBLING RSMB
------------------

--insert	into	RSMB_CM_20130301
SELECT 	CASE 	WHEN FCP.EVENT_TYPE='Live' THEN 1
			 	WHEN FCP.EVENT_TYPE='Playback' AND TRIM(TO_CHAR(FCP.TX_EVENT_START_DATETIME,'YYYYMMDD'))=TRIM(TO_CHAR(FCP.EVENT_START_DATETIME,'YYYYMMDD')) 	THEN 2
			 	WHEN FCP.EVENT_TYPE='Playback' AND TRIM(TO_CHAR(FCP.TX_EVENT_START_DATETIME,'YYYYMMDD'))<>TRIM(TO_CHAR(FCP.EVENT_START_DATETIME,'YYYYMMDD'))  	THEN 3 
		END VIEWING_TYPE
		,FSE.ACCOUNT_NUMBER
		,SKS.DB2_STATION_CODE 
		,CASE 	WHEN SKA.CHANNEL_GENRE LIKE 'Movies%' THEN 'Movies'
				WHEN SKA.CHANNEL_GENRE LIKE 'Sports%' THEN 'Sports'
				WHEN SKA.CHANNEL_GENRE LIKE 'Music%' THEN 'Music'
				ELSE SKA.CHANNEL_GENRE 
		END CHANNEL_GENRE
		,TO_DATE	(	CASE	WHEN SUBSTR(FCP.TX_EVENT_START_DATETIME,12,2) in ('00','01','02','03','04','05') THEN TRIM(TO_CHAR(FCP.TX_EVENT_START_DATETIME-1,'YYYYMMDD'))
								ELSE TRIM(TO_CHAR(FCP.TX_EVENT_START_DATETIME,'YYYYMMDD')) 
						END
						,'YYYYMMDD'
					) as BARB_DATE
		,CASE	WHEN EXTRACT(DOW FROM TO_DATE	(
													CASE	WHEN SUBSTR(FCP.TX_EVENT_START_DATETIME,12,2) in ('00','01','02','03','04','05') THEN TRIM(TO_CHAR(FCP.TX_EVENT_START_DATETIME-1,'YYYYMMDD'))
															ELSE TRIM(TO_CHAR(FCP.TX_EVENT_START_DATETIME,'YYYYMMDD')) 
													END
													,'YYYYMMDD'
												)
							)=1 THEN 7 ELSE EXTRACT(DOW FROM TO_DATE	(
																			CASE	WHEN SUBSTR(FCP.TX_EVENT_START_DATETIME,12,2) in ('00','01','02','03','04','05') 
																						THEN TRIM(TO_CHAR(FCP.TX_EVENT_START_DATETIME-1,'YYYYMMDD'))
																					ELSE TRIM(TO_CHAR(FCP.TX_EVENT_START_DATETIME,'YYYYMMDD')) 
																			END
																			,'YYYYMMDD'
																		)
													)-1 
		END DAY_OF_WEEK
		,CASE	WHEN SUBSTR(FCP.TX_EVENT_START_DATETIME,12,2) in ('00','01','02','03','04','05') THEN 240000 
				ELSE 0 
		END + 
		TO_NUMBER	(
						(
							TRIM(SUBSTR(FCP.TX_EVENT_START_DATETIME,12,2)) 
							|| TRIM(SUBSTR(FCP.TX_EVENT_START_DATETIME,15,2))
							|| TRIM(SUBSTR(FCP.TX_EVENT_START_DATETIME,18,2))
						)
						,'999999'
					) as START_TIME
		,CASE	WHEN TO_NUMBER(SUBSTR(FCP.TX_EVENT_START_DATETIME,12,2),'99')<6 AND TO_NUMBER(SUBSTR(FCP.TX_EVENT_START_DATETIME+FCP.EVENT_END_CAPPED_TIME,12,2),'99')>=6 THEN 295959
				ELSE	CASE	WHEN SUBSTR(FCP.TX_EVENT_START_DATETIME+FCP.EVENT_END_CAPPED_TIME,12,2) in ('00','01','02','03','04','05')  THEN 240000 
								ELSE 0 
						END +
						TO_NUMBER	(
										(
											TRIM(SUBSTR(FCP.TX_EVENT_START_DATETIME+FCP.EVENT_END_CAPPED_TIME,12,2)) 
											|| TRIM(SUBSTR(FCP.TX_EVENT_START_DATETIME+FCP.EVENT_END_CAPPED_TIME,15,2))
											|| TRIM(SUBSTR(FCP.TX_EVENT_START_DATETIME+FCP.EVENT_END_CAPPED_TIME,18,2))
										)
										,'999999'
									) 
		END as END_TIME
		,case 	when cast(FCP.TX_EVENT_START_DATETIME as time) > '06:00:00' 
					then cast((cast(cast(FCP.TX_EVENT_START_DATETIME+1 as date) as varchar(20)) || ' 06:00:00')as timestamp)
					else cast((cast(cast(FCP.TX_EVENT_START_DATETIME as date) as varchar(20)) || ' 06:00:00')as timestamp)
		end		as vosdal_cutoff
		,case when event_start_datetime <= vosdal_cutoff and FCP.EVENT_TYPE = 'Playback' then 1 else 0 end as VESPA_VOSDAL
		,fhh.WEIGHT_SCALED_VALUE as RSMB_derived_weights
FROM	(
			select	EVENT_TYPE
					,TX_EVENT_START_DATETIME
					,EVENT_START_DATETIME
					,EVENT_END_CAPPED_TIME
					,DTH_VIEWING_EVENT_ID
					,SERVICE_KEY
					,CAPPED_SHORT_DURATION_FLAG
					,cast(TX_EVENT_START_DATETIME as date) 	as vespa_scaling_date
					,cast(
							(
								CASE 	WHEN SUBSTR(TX_EVENT_START_DATETIME,12,2) in ('00','01','02','03','04','05') 
										THEN TRIM(TO_CHAR(TX_EVENT_START_DATETIME-1,'YYYYMMDD'))
										ELSE TRIM(TO_CHAR(TX_EVENT_START_DATETIME,'YYYYMMDD')) 
								END
							)
							as date
						)	as rsmb_scaling_date
			from 	DIS_REFERENCE..FINAL_CAPPED_EVENTS_HISTORY
			where 	TX_EVENT_START_DATETIME between '2013-03-01 00:00:00' and '2013-03-01 23:59:59' 
			limit 90000000
		) 	as FCP -- > Sample
		inner join DIS_REFERENCE..FINAL_SCALING_EVENT_HISTORY 		as FSE
		on	FSE.DTH_VIEWING_EVENT_ID=FCP.DTH_VIEWING_EVENT_ID
		inner join DIS_REFERENCE..FINAL_SCALING_HOUSEHOLD_HISTORY	as FHH
		on	FSE.ACCOUNT_NUMBER = FHH.ACCOUNT_NUMBER
		and FCP.rsmb_scaling_date = FHH.EVENT_START_DATE -- TOGGLE HERE BETWEEN RSMB / VESPA DATE
		inner join system..VESPA_SERVICE_KEY_DB2_STATION_copy 		as SKS
		on	FCP.SERVICE_KEY = SKS.SERVICE_KEY
		inner join DIS_REFERENCE..SERVICE_KEY_ATTRIBUTES 			as SKA
		on	FCP.SERVICE_KEY = SKA.SERVICE_KEY
		and	FCP.TX_EVENT_START_DATETIME BETWEEN SKA.EFFECTIVE_FROM AND SKA.EFFECTIVE_TO
where	FCP.CAPPED_SHORT_DURATION_FLAG<>1
limit	90000000


-------------------
-- ASSEMBLING VESPA
-------------------

--insert	into	VESPA_CM_20130301
SELECT 	CASE 	WHEN FCP.EVENT_TYPE='Live' THEN 1
			 	WHEN FCP.EVENT_TYPE='Playback' AND TRIM(TO_CHAR(FCP.TX_EVENT_START_DATETIME,'YYYYMMDD'))=TRIM(TO_CHAR(FCP.EVENT_START_DATETIME,'YYYYMMDD')) 	THEN 2
			 	WHEN FCP.EVENT_TYPE='Playback' AND TRIM(TO_CHAR(FCP.TX_EVENT_START_DATETIME,'YYYYMMDD'))<>TRIM(TO_CHAR(FCP.EVENT_START_DATETIME,'YYYYMMDD'))  	THEN 3 
		END VIEWING_TYPE
		,FSE.ACCOUNT_NUMBER
		,SKS.DB2_STATION_CODE 
		,CASE 	WHEN SKA.CHANNEL_GENRE LIKE 'Movies%' THEN 'Movies'
				WHEN SKA.CHANNEL_GENRE LIKE 'Sports%' THEN 'Sports'
				WHEN SKA.CHANNEL_GENRE LIKE 'Music%' THEN 'Music'
				ELSE SKA.CHANNEL_GENRE 
		END CHANNEL_GENRE
		,TO_DATE	(	CASE	WHEN SUBSTR(FCP.TX_EVENT_START_DATETIME,12,2) in ('00','01','02','03','04','05') THEN TRIM(TO_CHAR(FCP.TX_EVENT_START_DATETIME-1,'YYYYMMDD'))
								ELSE TRIM(TO_CHAR(FCP.TX_EVENT_START_DATETIME,'YYYYMMDD')) 
						END
						,'YYYYMMDD'
					) as BARB_DATE
		,CASE	WHEN EXTRACT(DOW FROM TO_DATE	(
													CASE	WHEN SUBSTR(FCP.TX_EVENT_START_DATETIME,12,2) in ('00','01','02','03','04','05') THEN TRIM(TO_CHAR(FCP.TX_EVENT_START_DATETIME-1,'YYYYMMDD'))
															ELSE TRIM(TO_CHAR(FCP.TX_EVENT_START_DATETIME,'YYYYMMDD')) 
													END
													,'YYYYMMDD'
												)
							)=1 THEN 7 ELSE EXTRACT(DOW FROM TO_DATE	(
																			CASE	WHEN SUBSTR(FCP.TX_EVENT_START_DATETIME,12,2) in ('00','01','02','03','04','05') 
																						THEN TRIM(TO_CHAR(FCP.TX_EVENT_START_DATETIME-1,'YYYYMMDD'))
																					ELSE TRIM(TO_CHAR(FCP.TX_EVENT_START_DATETIME,'YYYYMMDD')) 
																			END
																			,'YYYYMMDD'
																		)
													)-1 
		END DAY_OF_WEEK
		,CASE	WHEN SUBSTR(FCP.TX_EVENT_START_DATETIME,12,2) in ('00','01','02','03','04','05') THEN 240000 
				ELSE 0 
		END + 
		TO_NUMBER	(
						(
							TRIM(SUBSTR(FCP.TX_EVENT_START_DATETIME,12,2)) 
							|| TRIM(SUBSTR(FCP.TX_EVENT_START_DATETIME,15,2))
							|| TRIM(SUBSTR(FCP.TX_EVENT_START_DATETIME,18,2))
						)
						,'999999'
					) as START_TIME
		,CASE	WHEN TO_NUMBER(SUBSTR(FCP.TX_EVENT_START_DATETIME,12,2),'99')<6 AND TO_NUMBER(SUBSTR(FCP.TX_EVENT_START_DATETIME+FCP.EVENT_END_CAPPED_TIME,12,2),'99')>=6 THEN 295959
				ELSE	CASE	WHEN SUBSTR(FCP.TX_EVENT_START_DATETIME+FCP.EVENT_END_CAPPED_TIME,12,2) in ('00','01','02','03','04','05')  THEN 240000 
								ELSE 0 
						END +
						TO_NUMBER	(
										(
											TRIM(SUBSTR(FCP.TX_EVENT_START_DATETIME+FCP.EVENT_END_CAPPED_TIME,12,2)) 
											|| TRIM(SUBSTR(FCP.TX_EVENT_START_DATETIME+FCP.EVENT_END_CAPPED_TIME,15,2))
											|| TRIM(SUBSTR(FCP.TX_EVENT_START_DATETIME+FCP.EVENT_END_CAPPED_TIME,18,2))
										)
										,'999999'
									) 
		END as END_TIME
		,case 	when cast(FCP.TX_EVENT_START_DATETIME as time) > '06:00:00' 
					then cast((cast(cast(FCP.TX_EVENT_START_DATETIME+1 as date) as varchar(20)) || ' 06:00:00')as timestamp)
					else cast((cast(cast(FCP.TX_EVENT_START_DATETIME as date) as varchar(20)) || ' 06:00:00')as timestamp)
		end		as vosdal_cutoff
		,case when event_start_datetime <= vosdal_cutoff and FCP.EVENT_TYPE = 'Playback' then 1 else 0 end as VESPA_VOSDAL
		,fhh.WEIGHT_SCALED_VALUE as VESPA_derived_weights
FROM	(
			select	EVENT_TYPE
					,TX_EVENT_START_DATETIME
					,EVENT_START_DATETIME
					,EVENT_END_CAPPED_TIME
					,DTH_VIEWING_EVENT_ID
					,SERVICE_KEY
					,CAPPED_SHORT_DURATION_FLAG
					,cast(TX_EVENT_START_DATETIME as date) 	as vespa_scaling_date
					,cast(
							(
								CASE 	WHEN SUBSTR(TX_EVENT_START_DATETIME,12,2) in ('00','01','02','03','04','05') 
										THEN TRIM(TO_CHAR(TX_EVENT_START_DATETIME-1,'YYYYMMDD'))
										ELSE TRIM(TO_CHAR(TX_EVENT_START_DATETIME,'YYYYMMDD')) 
								END
							)
							as date
						)	as rsmb_scaling_date
			from 	DIS_REFERENCE..FINAL_CAPPED_EVENTS_HISTORY
			where 	TX_EVENT_START_DATETIME between '2013-03-01 00:00:00' and '2013-03-01 23:59:59' 
			limit 90000000
		) 	as FCP -- > Sample
		inner join DIS_REFERENCE..FINAL_SCALING_EVENT_HISTORY 		as FSE
		on	FSE.DTH_VIEWING_EVENT_ID=FCP.DTH_VIEWING_EVENT_ID
		inner join DIS_REFERENCE..FINAL_SCALING_HOUSEHOLD_HISTORY	as FHH
		on	FSE.ACCOUNT_NUMBER = FHH.ACCOUNT_NUMBER
		and FCP.vespa_scaling_date = FHH.EVENT_START_DATE -- TOGGLE HERE BETWEEN RSMB / VESPA DATE
		inner join system..VESPA_SERVICE_KEY_DB2_STATION_copy 		as SKS
		on	FCP.SERVICE_KEY = SKS.SERVICE_KEY
		inner join DIS_REFERENCE..SERVICE_KEY_ATTRIBUTES 			as SKA
		on	FCP.SERVICE_KEY = SKA.SERVICE_KEY
		and	FCP.TX_EVENT_START_DATETIME BETWEEN SKA.EFFECTIVE_FROM AND SKA.EFFECTIVE_TO
where	FCP.CAPPED_SHORT_DURATION_FLAG<>1
limit	90000000


------------------------
-- RSMB Vs. Vespa CHECKS
------------------------

--  Scaled Sky Base derivation
select	'vespa' as index_,sum(vespa_derived_weights)
from	(
			select	distinct
					account_number
					,vespa_derived_weights
			from 	VESPA_CM_20130301
		) as m
union	all
select	'rsmb' as index_,sum(vespa_derived_weights)
from	(
			select	account_number
					,min(vespa_derived_weights) as vespa_derived_weights
			from 	RSMB_CM_20130301
			group	by	account_number
		) as m
		
-- volumen of Vosdal

select	'vespa' as index_, count(1) as hits
from	VESPA_CM_20130301
where	vespa_vosdal = 1
union	all
select	'rsmb' as index_, count(1) as hits
from	RSMB_CM_20130301
where	viewing_type = 2

