/* *****************************


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
**Project Name:                                                 Adsmart - Drop 3 - External Attributes
		
		Description:
			New Attributes to update the Adsmart table
				
		Lead: 	Jose Pitteloud		
		Coded by: Paolo Menna
	Sections: 	26	Variety Pack
				27	All TV
				28	3D Pack
				29	HD Pack
				30	Recorded Viewing
				31	Average Daily Viewing
				32	Movies Premium - Action & Adventure
				33	Movies Premium - Comedy
				34	Movies Premium - Drama & Romance
				35	Movies Premium - Family
				36	Movies Premium - Horror & Thriller
				37	Movies Premium - Sci-Fi & Fantasy
				38	Sports Premium - American
				39	Sports Premium - Boxing & Wrestling
				40	Sports Premium - Cricket
				41	Sports Premium - Golf
				42	Sports Premium - Motor & Extreme
				43	Sports Premium - Niche Sports
				44	Sports Premium - Tennis
				45	Variety Pack (FTA)
				46	Variety Pack (Pay)
				47	Variety Pack - Action & Sci-Fi
				48	Variety Pack - Arts & Lifestyle
				49	Variety Pack - Children
				50	Variety Pack - Comedy & Game Shows
				51	Variety Pack - Drama & Crime
				52	Variety Pack - Movies
				53	Variety Pack - Sports
				54	Family Pack (Pay)
				55	Family Pack - Action & Sci-Fi
				56	Family Pack - Arts & Lifestyle
				57	Family Pack - Drama & Crime
				58	Family Pack - Movies
				59	Family Pack - News & Documentaries
				60	Family Pack - Sports
				61	Original Pack
				62	Original Pack (FTA)
				63	Original Pack (Pay)
				64	Original Pack - Action & Sci-Fi
				65	Original Pack - Arts & Lifestyle
				66	Original Pack - Children
				67	Original Pack - Comedy & Game Shows
				68	Original Pack - Drama & Crime
				69	Original Pack - Movies
				70	Original Pack - News & Documentaries
				71	Original Pack - Sports
				72	Movies Premium
				73	Sports Premium
				74	All TV (Pay)
				75	Third Party Premium
				76	Family Pack - Comedy & Game Shows
				77	Family Pack
				78	A la Carte Premium
				79	Sports Premium - Football
				80	Family Pack - Children
				81	Sports Premium - Rugby
				82	Family Pack (FTA)
				83	Variety Pack - News & Documentaries 

		
*********************************/


/* TESTING TABLE

SELECT account_number
	, samprofileid 					
	, cb_key_household					
	, user_type
		,cast(null as varchar(40)) as "Original Pack - Action & Sci-Fi"
		,cast(null as varchar(40)) as "Variety Pack"
		,cast(null as varchar(40)) as "Variety Pack (FTA)"
		,cast(null as varchar(40)) as "Sports Premium - Football"
		,cast(null as varchar(40)) as "All TV (Pay)"
		,cast(null as varchar(40)) as "All TV"
		,cast(null as varchar(40)) as "HD Pack"
		,cast(null as varchar(40)) as "Movies Premium - Sci-Fi & Fantasy"
		,cast(null as varchar(40)) as "Variety Pack - News & Documentaries"
		,cast(null as varchar(40)) as "Movies Premium - Action & Adventure"
		,cast(null as varchar(40)) as "Variety Pack - Movies"
		,cast(null as varchar(40)) as "Sports Premium - Golf"
		,cast(null as varchar(40)) as "Sports Premium - Rugby"
		,cast(null as varchar(40)) as "Sports Premium - Niche Sports"
		,cast(null as varchar(40)) as "Movies Premium - Comedy"
		,cast(null as varchar(40)) as "Sports Premium - Boxing & Wrestling"
		,cast(null as varchar(40)) as "Original Pack - Comedy & Game Shows"
		,cast(null as varchar(40)) as "Sports Premium - Motor & Extreme"
		,cast(null as varchar(40)) as "Variety Pack - Arts & Lifestyle"
		,cast(null as varchar(40)) as "Family Pack - Children"
		,cast(null as varchar(40)) as "Movies Premium - Drama & Romance"
		,cast(null as varchar(40)) as "Variety Pack - Drama & Crime"
		,cast(null as varchar(40)) as "Original Pack - Sports"
		,cast(null as varchar(40)) as "Sports Premium - Cricket"
		,cast(null as varchar(40)) as "Original Pack - News & Documentaries"
		,cast(null as varchar(40)) as "Recorded Viewing"
		,cast(null as varchar(40)) as "Sports Premium - Tennis"
		,cast(null as varchar(40)) as "Movies Premium - Horror & Thriller"
		,cast(null as varchar(40)) as "Original Pack - Arts & Lifestyle"
		,cast(null as varchar(40)) as "Variety Pack (Pay)"
		,cast(null as varchar(40)) as "Original Pack (FTA)"
		,cast(null as varchar(40)) as "Movies Premium - Family"
		,cast(null as varchar(40)) as "Variety Pack - Action & Sci-Fi"
		,cast(null as varchar(40)) as "Variety Pack - Comedy & Game Shows"
		,cast(null as varchar(40)) as "Family Pack - Arts & Lifestyle"
		,cast(null as varchar(40)) as "Original Pack - Children"
		,cast(null as varchar(40)) as "Family Pack (FTA)"
		,cast(null as varchar(40)) as "Sports Premium"
		,cast(null as varchar(40)) as "Movies Premium"
		,cast(null as varchar(40)) as "Third Party Premium"
		,cast(null as varchar(40)) as "Variety Pack - Children"
		,cast(null as varchar(40)) as "Original Pack (Pay)"
		,cast(null as varchar(40)) as "Variety Pack - Sports"
		,cast(null as varchar(40)) as "Original Pack"
		,cast(null as varchar(40)) as "Original Pack - Drama & Crime"
		,cast(null as varchar(40)) as "Sports Premium - American"
		,cast(null as varchar(40)) as "Family Pack (Pay)"
		,cast(null as varchar(40)) as "Family Pack - Drama & Crime"
		,cast(null as varchar(40)) as "Family Pack - Action & Sci-Fi"
		,cast(null as varchar(40)) as "Family Pack - Sports"
		,cast(null as varchar(40)) as "Family Pack - News & Documentaries"
		,cast(null as varchar(40)) as "Family Pack"
		,cast(null as varchar(40)) as "Original Pack - Movies"
		,cast(null as varchar(40)) as "Family Pack - Movies"
		,cast(null as varchar(40)) as "Family Pack - Comedy & Game Shows"
		,cast(null as varchar(40)) as "A La Carte Premium"
		,cast(null as varchar(40)) as "3D Pack"
INTO pm_dmp2
FROM SAM_REGISTRANT 
WHERE x_user_type in ('Primary', 'Secondary','primary','secondary') 
	AND marked_as_deleted = 'N'

CREATE HG INDEX id1 ON pm_dmp2(account_number)
CREATE HG INDEX id2 ON pm_dmp2(row_id)
CREATE HG INDEX id3 ON pm_dmp2(cb_key_household)
CREATE HG INDEX id4 ON pm_dmp2(cb_address_postcode) 
COMMIT 

*/


declare @period datetime
set @period = (select max(aggregation_period_end_datetime) from sk_prod.VESPA_AGGR_AGGREGATION_DIM)


UPDATE ###DMP_TABLE### a
		SET a."Original Pack - Action & Sci-Fi" = 	CASE WHEN b.aggregation_name ='Original Pack - Action & Sci-Fi' THEN b.aggregation_high_level_banding END
		,a."Variety Pack" = 						CASE WHEN b.aggregation_name ='Variety Pack' THEN b.aggregation_high_level_banding END
		,a."Variety Pack (FTA)" = 					CASE WHEN b.aggregation_name ='Variety Pack (FTA)' THEN b.aggregation_high_level_banding END
		,a."Sports Premium - Football" = 			CASE WHEN b.aggregation_name ='Sports Premium - Football' THEN b.aggregation_high_level_banding END
		,a."All TV (Pay)" = 						CASE WHEN b.aggregation_name ='All TV (Pay)' THEN b.aggregation_high_level_banding END
		,a."All TV" = 								CASE WHEN b.aggregation_name ='All TV' THEN b.aggregation_high_level_banding END
		,a."HD Pack" = 								CASE WHEN b.aggregation_name ='HD Pack' THEN b.aggregation_high_level_banding END
		,a."Movies Premium - Sci-Fi & Fantasy" = 	CASE WHEN b.aggregation_name ='Movies Premium - Sci-Fi & Fantasy' THEN b.aggregation_high_level_banding END
		,a."Variety Pack - News & Documentaries" = 	CASE WHEN b.aggregation_name ='Variety Pack - News & Documentaries' THEN b.aggregation_high_level_banding END
		,a."Movies Premium - Action & Adventure" = 	CASE WHEN b.aggregation_name ='Movies Premium - Action & Adventure' THEN b.aggregation_high_level_banding END
		,a."Variety Pack - Movies" = 				CASE WHEN b.aggregation_name ='Variety Pack - Movies' THEN b.aggregation_high_level_banding END
		,a."Sports Premium - Golf" = 				CASE WHEN b.aggregation_name ='Sports Premium - Golf' THEN b.aggregation_high_level_banding END
		,a."Sports Premium - Rugby" = 				CASE WHEN b.aggregation_name ='Sports Premium - Rugby' THEN b.aggregation_high_level_banding END
		,a."Sports Premium - Niche Sports" = 		CASE WHEN b.aggregation_name ='Sports Premium - Niche Sports' THEN b.aggregation_high_level_banding END
		,a."Movies Premium - Comedy" = 				CASE WHEN b.aggregation_name ='Movies Premium - Comedy' THEN b.aggregation_high_level_banding END
		,a."Sports Premium - Boxing & Wrestling" = 	CASE WHEN b.aggregation_name ='Sports Premium - Boxing & Wrestling' THEN b.aggregation_high_level_banding END
		,a."Original Pack - Comedy & Game Shows" = 	CASE WHEN b.aggregation_name ='Original Pack - Comedy & Game Shows' THEN b.aggregation_high_level_banding END
		,a."Sports Premium - Motor & Extreme" = 	CASE WHEN b.aggregation_name ='Sports Premium - Motor & Extreme' THEN b.aggregation_high_level_banding END
		,a."Variety Pack - Arts & Lifestyle" = 		CASE WHEN b.aggregation_name ='Variety Pack - Arts & Lifestyle' THEN b.aggregation_high_level_banding END
		,a."Family Pack - Children" = 				CASE WHEN b.aggregation_name ='Family Pack - Children' THEN b.aggregation_high_level_banding END
		,a."Movies Premium - Drama & Romance" = 	CASE WHEN b.aggregation_name ='Movies Premium - Drama & Romance' THEN b.aggregation_high_level_banding END
		,a."Variety Pack - Drama & Crime" = 		CASE WHEN b.aggregation_name ='Variety Pack - Drama & Crime' THEN b.aggregation_high_level_banding END
		,a."Original Pack - Sports" = 				CASE WHEN b.aggregation_name ='Original Pack - Sports' THEN b.aggregation_high_level_banding END
		,a."Sports Premium - Cricket" = 			CASE WHEN b.aggregation_name ='Sports Premium - Cricket' THEN b.aggregation_high_level_banding END
		,a."Original Pack - News & Documentaries" = CASE WHEN b.aggregation_name ='Original Pack - News & Documentaries' THEN b.aggregation_high_level_banding END
		,a."Recorded Viewing" = 					CASE WHEN b.aggregation_name ='Recorded Viewing' THEN b.aggregation_high_level_banding END
		,a."Sports Premium - Tennis" = 				CASE WHEN b.aggregation_name ='Sports Premium - Tennis' THEN b.aggregation_high_level_banding END
		,a."Movies Premium - Horror & Thriller" = 	CASE WHEN b.aggregation_name ='Movies Premium - Horror & Thriller' THEN b.aggregation_high_level_banding END
		,a."Original Pack - Arts & Lifestyle" = 	CASE WHEN b.aggregation_name ='Original Pack - Arts & Lifestyle' THEN b.aggregation_high_level_banding END
		,a."Variety Pack (Pay)" = 					CASE WHEN b.aggregation_name ='Variety Pack (Pay)' THEN b.aggregation_high_level_banding END
		,a."Original Pack (FTA)" = 					CASE WHEN b.aggregation_name ='Original Pack (FTA)' THEN b.aggregation_high_level_banding END
		,a."Movies Premium - Family" = 				CASE WHEN b.aggregation_name ='Movies Premium - Family' THEN b.aggregation_high_level_banding END
		,a."Variety Pack - Action & Sci-Fi" = 		CASE WHEN b.aggregation_name ='Variety Pack - Action & Sci-Fi' THEN b.aggregation_high_level_banding END
		,a."Variety Pack - Comedy & Game Shows" = 	CASE WHEN b.aggregation_name ='Variety Pack - Comedy & Game Shows' THEN b.aggregation_high_level_banding END
		,a."Family Pack - Arts & Lifestyle" = 		CASE WHEN b.aggregation_name ='Family Pack - Arts & Lifestyle' THEN b.aggregation_high_level_banding END
		,a."Original Pack - Children" = 			CASE WHEN b.aggregation_name ='Original Pack - Children' THEN b.aggregation_high_level_banding END
		,a."Family Pack (FTA)" = 					CASE WHEN b.aggregation_name ='Family Pack (FTA)' THEN b.aggregation_high_level_banding END
		,a."Sports Premium" = 						CASE WHEN b.aggregation_name ='Sports Premium' THEN b.aggregation_high_level_banding END
		,a."Movies Premium" = 						CASE WHEN b.aggregation_name ='Movies Premium' THEN b.aggregation_high_level_banding END
		,a."Third Party Premium" = 					CASE WHEN b.aggregation_name ='Third Party Premium' THEN b.aggregation_high_level_banding END
		,a."Variety Pack - Children" = 				CASE WHEN b.aggregation_name ='Variety Pack - Children' THEN b.aggregation_high_level_banding END
		,a."Original Pack (Pay)" = 					CASE WHEN b.aggregation_name ='Original Pack (Pay)' THEN b.aggregation_high_level_banding END
		,a."Variety Pack - Sports" = 				CASE WHEN b.aggregation_name ='Variety Pack - Sports' THEN b.aggregation_high_level_banding END
		,a."Original Pack" = 						CASE WHEN b.aggregation_name ='Original Pack' THEN b.aggregation_high_level_banding END
		,a."Original Pack - Drama & Crime" = 		CASE WHEN b.aggregation_name ='Original Pack - Drama & Crime' THEN b.aggregation_high_level_banding END
		,a."Sports Premium - American" = 			CASE WHEN b.aggregation_name ='Sports Premium - American' THEN b.aggregation_high_level_banding END
		,a."Family Pack (Pay)" = 					CASE WHEN b.aggregation_name ='Family Pack (Pay)' THEN b.aggregation_high_level_banding END
		,a."Family Pack - Drama & Crime" = 			CASE WHEN b.aggregation_name ='Family Pack - Drama & Crime' THEN b.aggregation_high_level_banding END
		,a."Family Pack - Action & Sci-Fi" = 		CASE WHEN b.aggregation_name ='Family Pack - Action & Sci-Fi' THEN b.aggregation_high_level_banding END
		,a."Family Pack - Sports" = 				CASE WHEN b.aggregation_name ='Family Pack - Sports' THEN b.aggregation_high_level_banding END
		,a."Family Pack - News & Documentaries" = 	CASE WHEN b.aggregation_name ='Family Pack - News & Documentaries' THEN b.aggregation_high_level_banding END
		,a."Family Pack" = 							CASE WHEN b.aggregation_name ='Family Pack' THEN b.aggregation_high_level_banding END
		,a."Original Pack - Movies" = 				CASE WHEN b.aggregation_name ='Original Pack - Movies' THEN b.aggregation_high_level_banding END
		,a."Family Pack - Movies" = 				CASE WHEN b.aggregation_name ='Family Pack - Movies' THEN b.aggregation_high_level_banding END
		,a."Family Pack - Comedy & Game Shows" = 	CASE WHEN b.aggregation_name ='Family Pack - Comedy & Game Shows' THEN b.aggregation_high_level_banding END
		,a."A La Carte Premium" = 					CASE WHEN b.aggregation_name ='A La Carte Premium' THEN b.aggregation_high_level_banding END
		,a."3D Pack" = 								CASE WHEN b.aggregation_name ='3D Pack' THEN b.aggregation_high_level_banding END
FROM (
	SELECT a.account_number
		, b.aggregation_high_level_banding
		, aggregation_name
	FROM sk_prod.VESPA_AGGR_FACT a
		, sk_prod.VESPA_AGGR_AGGREGATION_DIM b
	WHERE a.Dk_aggregation_dim = b.Pk_aggregation_dim
		AND aggregation_period_end_datetime = @period
	) b
WHERE a.account_number = b.account_number








