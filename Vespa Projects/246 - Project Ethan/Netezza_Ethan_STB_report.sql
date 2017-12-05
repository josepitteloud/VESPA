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
**Project Name:                                                 Project Ethan
**Analysts:                             Jon Green            	(Jonathan.Green@skyiq.co.uk)
**Lead(s):                              Hoi Yu Tang          	(HoiYu.Tang@skyiq.co.uk)
**Stakeholder:                          SkyIQ
										Jose Loureda			(Jose.Loureda@skyiq.co.uk)
**Due Date:                             
**Project Code (Insight Collation):     v246
**Sharepoint Folder:

        http://sp-department.bskyb.com/sites/IQSKY/SIG/Insight%20Collation%20Documents/01%20Analysis%20Requests/V239%20-%20Project%20Ethan

**Business Brief:

        This file contains Netezza queries to be run in support of STB testing on Ethan Vespa panels

--------------------------------------------------------------------------------------------------------------
*/


 SELECT DAY_DATE
       ,PANEL_ID_REPORTED
       ,STB_GROUP
       ,AVG(RQ) AS ARQ
       ,SUM(LOG_RECEIVED_TODAY) AS RETURNING_STBS
   FROM (  SELECT DD.DAY_DATE
                 ,DTH.SCMS_SUBSCRIBER_ID
                 ,DTH.PANEL_ID_REPORTED
                 ,CASE WHEN DTH.SCMS_SUBSCRIBER_ID IN ( -- region Ethan field trial subscribers
                                                       35554907
                                                      ,35554840
                                                      ,35554841
                                                      ,35554842
                                                      ,35554843
                                                      ,35554844
                                                      ,35554845
                                                      ,35554846
                                                      ,35554847
                                                      ,35554848
                                                      ,35554849
                                                      ,35554850
                                                      ,35554851
                                                      ,35554852
                                                      ,35554853
                                                      ,35554854
                                                      ,35554855
                                                      ,35554856
                                                      ,35554857
                                                      ,35554861
                                                      ,35554862
                                                      ,35554869
                                                      ,35554870
                                                      ,35554871
                                                      ,35554872
                                                      ,35554873
                                                      ,35554874
                                                      ,35554875
                                                      ,35554876
                                                      ,35554877
                                                      ,35554878
                                                      ,35554879
                                                      ,35554880
                                                      ,35554881
                                                      ,35554882
                                                      ,35554883
                                                      ,35554884
                                                      ,35554885
                                                      ,35554886
                                                      ,35554887
                                                      ,35554888
                                                      ,35554889
                                                      ,35554890
                                                      ,35554891
                                                      ,35554892
                                                      ,35554892
                                                      ,35554893
                                                      ,35554894
                                                      ,35554895
                                                      ,35554896
                                                      ,35554897
                                                      ,35554898
                                                      ,35554899
                                                      ,35554900
                                                      ,35554902
                                                      ,35554903
                                                      ,35554904
                                                      ,35554905
                                                      ,35554906
                                                      ,35554908
                                                      ,35554909
                                                      ,35554910
                                                      ,35554911
                                                      ,35554912
                                                      ,35554913
                                                      ,35554914
                                                      ,35554915
                                                      ,35554916
                                                      ,35554917
                                                      ,35554918
                                                      ,35554919
                                                      ,35554920
                                                      ,35554921
                                                      ,35554922
                                                      ,35554923
                                                      ,35554925
                                                      ,35554926
--                                                      ,35554927
                                                      ,35554928
                                                      ,35554929
                                                      ,35554930
                                                      ,35554931
                                                      ,35554932
                                                      ,35554933
                                                      ,35554934
                                                      ,35554935
                                                      ,35619596
                                                      ,35619597
                                                      ,35619598
--                                                      ,35619599
                                                      ,35619600
                                                      ,35619601
--                                                      ,35619731
                                                      ,35641456
                                                      ,35710879
--                                                      ,35853854
                                                      ,35853855
                                                      ,35853856
--                                                      ,35853857
                                                      ,36308671 -- from here added 04/08/2015
                                                      ,36308715
                                                      ,36308673
                                                      ,36308716
                                                      ,36308718
                                                      ,36308674
                                                      ,36308721
                                                      ,36308675
                                                      ,216682
                                                      ,36309483
                                                      ,36308726
                                                      ,36308725
                                                      ,36308724
                                                      ,36308723
                                                      ,36308676
                                                      ,36309484
                                                      ,36308720
                                                      ,36308719
--                                                      ,36308677
                                                      ,36308680
                                                      ,36308681
                                                      ,36308682
                                                      ,36308717
                                                      ,36308714
                                                      ,36308712
                                                      ,36308709
                                                      ,36309481
                                                      ,36308708
                                                      ,36308706
                                                      ,36308704
                                                      ,36308702
                                                      ,28791603
                                                      ,36308696
                                                      ,36308683
                                                      ,36308684
                                                      ,36308693
                                                      ,36308695
                                                      ,36308697
                                                      ,36308698
--                                                      ,36308700
                                                      ,36308701
                                                      ,36309482
                                                      ,36308703
                                                      ,36308705
                                                      ,36308707
                                                      ,36308710
                                                      ,36308711
--                                                      ,36308713
                                                      ,36308694
                                                      ,35963127
                                                      ,35963023
                                                      ,35963155
                                                      ,35963017
                                                      ,35963146
                                                      ,35963144
                                                      ,35963142
                                                      ,35963063
                                                      ,35963161
--                                                      ,35963134
                                                      ,35963133
                                                      ,35963387
                                                      ,35963116
                                                      ,35963148
                                                      ,35963151
                                                      ,35963058
                                                      ,35963095
                                                      ,35963033
                                                      ,35963145
                                                      ,35963110
                                                      ,35963119
                                                      ,35963018
                                                      ,35963057
                                                      ,35963123
                                                      ,35963150
                                                      ,35963061
                                                      ,35963093
                                                      ,35967396
                                                      ,35963049
                                                      ,35963386
                                                      ,35963029
                                                      ,35963015
                                                      ,35963045
                                                      ,35963147
--                                                      ,35963156
                                                      ,35963024
                                                      ,35963098
                                                      ,35967399
                                                      ,35963103
                                                      ,35967398
                                                      ,35963046
                                                      ,35963054
                                                      ,35963107
                                                      ,35967401
                                                      ,35963415
                                                      ,35963036
                                                      ,35963025
                                                      ,35963022
--                                                      ,35963044
                                                      ,35963120
                                                      ,35967392
                                                      ,35963038
                                                      ,35963097
                                                      ,35963099
                                                      ,35967394
                                                      ,35963152
                                                      ,35963142
                                                      ,35963388
                                                      ,35963094
                                                      ,35963101
                                                      ,35963043
                                                      ,35963066
                                                      ,35963068
--                                                      ,35963047
                                                      ,35963040
                                                      ,35963016
                                                      ,35963153
                                                      ,35963111
                                                      ,35963124
                                                      ,35967421
                                                      ,35963037
                                                      ,35963109
                                                      ,35963050
                                                      ,35963031
--                                                      ,35963138
--                                                      ,35963158
--                                                      ,35963160
                                                      ,35963053
                                                      ,35963162
                                                      ,35963065
                                                      ,35963091
                                                      ,35963096
                                                      ,35963027
                                                      ,35963062
                                                      ,35963055
                                                      ,35963128
                                                      ,35963113
                                                      ,35963389
                                                      ,35963393
                                                      ,35963115
                                                      ,35963092
                                                      ,35963064
                                                      ,35963067
                                                      ,35963459
                                                      ,35963478
                                                      ,35963102
                                                      ,35963039
                                                      ,35963516
                                                      ,35963541
                                                      ,35963129
                                                      ,35963132
                                                      ,35963135
                                                      ,35963052
                                                      ,35963059
                                                      ,35963149
                                                      ,35967402
                                                      ,35967403
                                                      ,35963089
                                                      ,35963157
                                                      ,35963137
                                                      ,36130829
                                                      ,35963130
                                                      ,36130862
                                                      ,35963020
                                                      ,35963114
                                                      ,35963117
                                                      ,35963125
                                                      ,36130827
                                                      ,35963139
                                                      ,35963100
                                                      ,36130828
                                                      ,36130865
                                                      ,36130864
                                                      ,35963131
                                                      ,35963108
                                                      ,35963141
                                                      ,35963034
                                                      ,35963035
                                                      ,35963121
                                                      ,35963439
                                                      ,35963154
                                                      ,35963140
                                                      ,35963159
                                                      ,35963056
                                                      ,35963028
                                                      ,35963106
                                                      ,35963048
                                                      ,35963042
                                                      ,35963136
                                                      ,36130863
                                                      ,35963026
                                                      ,35963112
                                                      ,36130861
                                                      ,35724049
                                                      ,37530319
                                                      ,35710878 
													  												-- from here added 30/10/2015
												,25518889
												,35963031
												,24920812
												,35554907
												,22385310
												,25296732
												,10577502
												,35963153
												,36308713
												,35963044
												,35963034
												,10026413
												,28686006
												,36308700
												,20542698
												,35963047
												,18687436
												,7390909
												,35619599
												,22225713
												,35619731
												,36308722
												,22597426
												,35853854
												,35554927
												,35963154
												,36308677
												,36308720
												,33551211
												,35963156
												,9654090
												,35963138
												,35531682
												,35963158
												,35963134
												,35963160
												,35554843
												,3308533
												,35853857
												,35873849
--endregion
                                                      )  THEN 'ETHAN TRIAL'
                       WHEN DTH.SCMS_SUBSCRIBER_ID IN ( -- region Ethan test lab STBs
                                                       34307704
                                                      ,34519166 --Priya
                                                      ,34955934 --Noby
                                                      ,34939007 --Katie
                                                      ,34939009 --Sohrab
                                                      ,34307532 --Silvia
                                                      ,34939347 --Martyna
                                                      ,34955055 --Alex
                                                      ,33593292
                                                      ,36021978 -- below added 4th June
                                                      ,36021977
                                                      ,36021972
                                                      ,36021969
                                                      ,36021968
                                                      ,36021967
                                                      ,36021966
                                                      ,36021976
                                                      ,36021975
                                                      ,36021974
                                                      ,36021973
                                                      ,36021971
                                                      ,36022026
                                                      ,36021965 -- endregion
                                                      )  THEN 'ETHAN LAB'
                       WHEN DTH.SCMS_SUBSCRIBER_ID IN ( -- region D12 triallists
25518793
--,34307704 --Hywel
,35396756
,35500462
,35500463
,35500464
,35525696
,35525697
,35525699
,35525700
,35531678
,35531680
,35531681
,35531683
,35531684
,35531685
,35531686
,35531687
,35541811
,35541815
,35593291
,35619741
,35619841
,35619843
 --endregion
                                                      ) THEN 'D12'
                       ELSE 'OTHERS'
                   END AS STB_GROUP
                 ,SUM(CASE WHEN DTH.LOG_RECEIVED_DATE IS NOT NULL THEN 1
                                                                  ELSE 0
                       END
                      ) AS LOGS_RECEIVED_IN_RQ_WINDOW
                 ,MIN(DTH.LOG_RECEIVED_DATE) AS FIRST_LOG_DATE
                 ,(EXTRACT(EPOCH FROM DD.DAY_DATE - FIRST_LOG_DATE) / 86400) + 1  AS PANEL_DAYS
                 ,LOGS_RECEIVED_IN_RQ_WINDOW / PANEL_DAYS AS RQ
                 ,MAX(CASE WHEN DD.DAY_DATE = DTH.LOG_RECEIVED_DATE THEN 1
                           ELSE 0
                       END
                     ) AS LOG_RECEIVED_TODAY
             FROM SMI_DW..DATE_DIM AS DD  -- Get time base
                  LEFT JOIN ( -- Data for RQ
                             SELECT SCMS_SUBSCRIBER_ID
                                   ,PANEL_ID_REPORTED
                                   ,DATE ( -- BST correction
                                          CASE WHEN LOG_RECEIVED_DATETIME BETWEEN '2015-03-29 01:00:00' 
                                                                              AND '2015-10-25 01:00:00' THEN LOG_RECEIVED_DATETIME + CAST('1 HOUR' AS INTERVAL)
                                               ELSE LOG_RECEIVED_DATETIME
                                           END 
                                         ) AS LOG_RECEIVED_DATE
                               FROM DIS_REFERENCE..FINAL_DTH_VIEWING_EVENT_HISTORY
                              WHERE PANEL_ID_REPORTED in (8, 14, 15) -- Limit to Ethan panels                             
                                AND LOG_RECEIVED_DATETIME BETWEEN DATE(NOW()) - CAST('60 DAYS' AS INTERVAL) --'2015-01-01' 
                                                              AND DATE(NOW())
                           GROUP BY SCMS_SUBSCRIBER_ID
                                   ,PANEL_ID_REPORTED
                                   ,LOG_RECEIVED_DATE
                            ) AS DTH ON DTH.LOG_RECEIVED_DATE BETWEEN DD.DAY_DATE - CAST('14 DAYS' AS INTERVAL) 
                                                                  AND DD.DAY_DATE
            WHERE DD.DAY_DATE BETWEEN DATE(NOW()) - CAST('60 DAYS' AS INTERVAL)
                                  AND DATE(NOW())
         GROUP BY DD.DAY_DATE
                 ,DTH.SCMS_SUBSCRIBER_ID
                 ,DTH.PANEL_ID_REPORTED
                 ,STB_GROUP
        ) AS AGG
GROUP BY DAY_DATE
       ,PANEL_ID_REPORTED
       ,STB_GROUP
ORDER BY DAY_DATE
       ,PANEL_ID_REPORTED
       ,STB_GROUP
;





--------------------------------------------------------
-- 2. Simply query to show the last log received date per subscriber
--------------------------------------------------------

  SELECT	TRI.subID
	       ,DTH.PANEL_ID_REPORTED
	       ,DTH.LAST_LOG_RECEIVED_DATE
    FROM (	-- REGION Ethan field triallists & D12
          SELECT 216682 as subid
UNION ALL SELECT 3308533
UNION ALL SELECT 7390909
UNION ALL SELECT 9654090
UNION ALL SELECT 10026413
UNION ALL SELECT 10577502
UNION ALL SELECT 18687436
UNION ALL SELECT 20542698
UNION ALL SELECT 22225713
UNION ALL SELECT 22385310
UNION ALL SELECT 22597426
UNION ALL SELECT 24920812
UNION ALL SELECT 25296732
UNION ALL SELECT 25518793
UNION ALL SELECT 25518889
UNION ALL SELECT 28686006
UNION ALL SELECT 28791603
UNION ALL SELECT 33551211
--UNION ALL SELECT 34307704
UNION ALL SELECT 35396756
UNION ALL SELECT 35500462
UNION ALL SELECT 35500463
UNION ALL SELECT 35500464
UNION ALL SELECT 35525696
UNION ALL SELECT 35525697
UNION ALL SELECT 35525699
UNION ALL SELECT 35525700
UNION ALL SELECT 35531678
UNION ALL SELECT 35531680
UNION ALL SELECT 35531681
UNION ALL SELECT 35531682
UNION ALL SELECT 35531683
UNION ALL SELECT 35531684
UNION ALL SELECT 35531685
UNION ALL SELECT 35531686
UNION ALL SELECT 35531687
UNION ALL SELECT 35541811
UNION ALL SELECT 35541815
UNION ALL SELECT 35554840
UNION ALL SELECT 35554841
UNION ALL SELECT 35554842
UNION ALL SELECT 35554843
UNION ALL SELECT 35554844
UNION ALL SELECT 35554845
UNION ALL SELECT 35554846
UNION ALL SELECT 35554847
UNION ALL SELECT 35554848
UNION ALL SELECT 35554849
UNION ALL SELECT 35554850
UNION ALL SELECT 35554851
UNION ALL SELECT 35554852
UNION ALL SELECT 35554853
UNION ALL SELECT 35554854
UNION ALL SELECT 35554855
UNION ALL SELECT 35554856
UNION ALL SELECT 35554857
UNION ALL SELECT 35554861
UNION ALL SELECT 35554862
UNION ALL SELECT 35554869
UNION ALL SELECT 35554870
UNION ALL SELECT 35554871
UNION ALL SELECT 35554872
UNION ALL SELECT 35554873
UNION ALL SELECT 35554874
UNION ALL SELECT 35554875
UNION ALL SELECT 35554876
UNION ALL SELECT 35554877
UNION ALL SELECT 35554878
UNION ALL SELECT 35554879
UNION ALL SELECT 35554880
UNION ALL SELECT 35554881
UNION ALL SELECT 35554882
UNION ALL SELECT 35554883
UNION ALL SELECT 35554884
UNION ALL SELECT 35554885
UNION ALL SELECT 35554886
UNION ALL SELECT 35554887
UNION ALL SELECT 35554888
UNION ALL SELECT 35554889
UNION ALL SELECT 35554890
UNION ALL SELECT 35554891
UNION ALL SELECT 35554892
UNION ALL SELECT 35554893
UNION ALL SELECT 35554894
UNION ALL SELECT 35554895
UNION ALL SELECT 35554896
UNION ALL SELECT 35554897
UNION ALL SELECT 35554898
UNION ALL SELECT 35554899
UNION ALL SELECT 35554900
UNION ALL SELECT 35554902
UNION ALL SELECT 35554903
UNION ALL SELECT 35554904
UNION ALL SELECT 35554905
UNION ALL SELECT 35554906
UNION ALL SELECT 35554907
UNION ALL SELECT 35554908
UNION ALL SELECT 35554909
UNION ALL SELECT 35554910
UNION ALL SELECT 35554911
UNION ALL SELECT 35554912
UNION ALL SELECT 35554913
UNION ALL SELECT 35554914
UNION ALL SELECT 35554915
UNION ALL SELECT 35554916
UNION ALL SELECT 35554917
UNION ALL SELECT 35554918
UNION ALL SELECT 35554919
UNION ALL SELECT 35554920
UNION ALL SELECT 35554921
UNION ALL SELECT 35554922
UNION ALL SELECT 35554923
UNION ALL SELECT 35554925
UNION ALL SELECT 35554926
UNION ALL SELECT 35554927
UNION ALL SELECT 35554928
UNION ALL SELECT 35554929
UNION ALL SELECT 35554930
UNION ALL SELECT 35554931
UNION ALL SELECT 35554932
UNION ALL SELECT 35554933
UNION ALL SELECT 35554934
UNION ALL SELECT 35554935
UNION ALL SELECT 35593291
UNION ALL SELECT 35619596
UNION ALL SELECT 35619597
UNION ALL SELECT 35619598
UNION ALL SELECT 35619599
UNION ALL SELECT 35619600
UNION ALL SELECT 35619601
UNION ALL SELECT 35619731
UNION ALL SELECT 35619741
UNION ALL SELECT 35619841
UNION ALL SELECT 35619843
UNION ALL SELECT 35641456
UNION ALL SELECT 35710878
UNION ALL SELECT 35710879
UNION ALL SELECT 35724049
UNION ALL SELECT 35853854
UNION ALL SELECT 35853855
UNION ALL SELECT 35853856
UNION ALL SELECT 35853857
UNION ALL SELECT 35873849
UNION ALL SELECT 35963015
UNION ALL SELECT 35963016
UNION ALL SELECT 35963017
UNION ALL SELECT 35963018
UNION ALL SELECT 35963020
UNION ALL SELECT 35963022
UNION ALL SELECT 35963023
UNION ALL SELECT 35963024
UNION ALL SELECT 35963025
UNION ALL SELECT 35963026
UNION ALL SELECT 35963027
UNION ALL SELECT 35963028
UNION ALL SELECT 35963029
UNION ALL SELECT 35963031
UNION ALL SELECT 35963033
UNION ALL SELECT 35963034
UNION ALL SELECT 35963035
UNION ALL SELECT 35963036
UNION ALL SELECT 35963037
UNION ALL SELECT 35963038
UNION ALL SELECT 35963039
UNION ALL SELECT 35963040
UNION ALL SELECT 35963042
UNION ALL SELECT 35963043
UNION ALL SELECT 35963044
UNION ALL SELECT 35963045
UNION ALL SELECT 35963046
UNION ALL SELECT 35963047
UNION ALL SELECT 35963048
UNION ALL SELECT 35963049
UNION ALL SELECT 35963050
UNION ALL SELECT 35963052
UNION ALL SELECT 35963053
UNION ALL SELECT 35963054
UNION ALL SELECT 35963055
UNION ALL SELECT 35963056
UNION ALL SELECT 35963057
UNION ALL SELECT 35963058
UNION ALL SELECT 35963059
UNION ALL SELECT 35963061
UNION ALL SELECT 35963062
UNION ALL SELECT 35963063
UNION ALL SELECT 35963064
UNION ALL SELECT 35963065
UNION ALL SELECT 35963066
UNION ALL SELECT 35963067
UNION ALL SELECT 35963068
UNION ALL SELECT 35963089
UNION ALL SELECT 35963091
UNION ALL SELECT 35963092
UNION ALL SELECT 35963093
UNION ALL SELECT 35963094
UNION ALL SELECT 35963095
UNION ALL SELECT 35963096
UNION ALL SELECT 35963097
UNION ALL SELECT 35963098
UNION ALL SELECT 35963099
UNION ALL SELECT 35963100
UNION ALL SELECT 35963101
UNION ALL SELECT 35963102
UNION ALL SELECT 35963103
UNION ALL SELECT 35963106
UNION ALL SELECT 35963107
UNION ALL SELECT 35963108
UNION ALL SELECT 35963109
UNION ALL SELECT 35963110
UNION ALL SELECT 35963111
UNION ALL SELECT 35963112
UNION ALL SELECT 35963113
UNION ALL SELECT 35963114
UNION ALL SELECT 35963115
UNION ALL SELECT 35963116
UNION ALL SELECT 35963117
UNION ALL SELECT 35963119
UNION ALL SELECT 35963120
UNION ALL SELECT 35963121
UNION ALL SELECT 35963123
UNION ALL SELECT 35963124
UNION ALL SELECT 35963125
UNION ALL SELECT 35963127
UNION ALL SELECT 35963128
UNION ALL SELECT 35963129
UNION ALL SELECT 35963130
UNION ALL SELECT 35963131
UNION ALL SELECT 35963132
UNION ALL SELECT 35963133
UNION ALL SELECT 35963134
UNION ALL SELECT 35963135
UNION ALL SELECT 35963136
UNION ALL SELECT 35963137
UNION ALL SELECT 35963138
UNION ALL SELECT 35963139
UNION ALL SELECT 35963140
UNION ALL SELECT 35963141
UNION ALL SELECT 35963142
UNION ALL SELECT 35963144
UNION ALL SELECT 35963145
UNION ALL SELECT 35963146
UNION ALL SELECT 35963147
UNION ALL SELECT 35963148
UNION ALL SELECT 35963149
UNION ALL SELECT 35963150
UNION ALL SELECT 35963151
UNION ALL SELECT 35963152
UNION ALL SELECT 35963153
UNION ALL SELECT 35963154
UNION ALL SELECT 35963155
UNION ALL SELECT 35963156
UNION ALL SELECT 35963157
UNION ALL SELECT 35963158
UNION ALL SELECT 35963159
UNION ALL SELECT 35963160
UNION ALL SELECT 35963161
UNION ALL SELECT 35963162
UNION ALL SELECT 35963386
UNION ALL SELECT 35963387
UNION ALL SELECT 35963388
UNION ALL SELECT 35963389
UNION ALL SELECT 35963393
UNION ALL SELECT 35963415
UNION ALL SELECT 35963439
UNION ALL SELECT 35963459
UNION ALL SELECT 35963478
UNION ALL SELECT 35963516
UNION ALL SELECT 35963541
UNION ALL SELECT 35967392
UNION ALL SELECT 35967394
UNION ALL SELECT 35967396
UNION ALL SELECT 35967398
UNION ALL SELECT 35967399
UNION ALL SELECT 35967401
UNION ALL SELECT 35967402
UNION ALL SELECT 35967403
UNION ALL SELECT 35967421
UNION ALL SELECT 36130827
UNION ALL SELECT 36130828
UNION ALL SELECT 36130829
UNION ALL SELECT 36130861
UNION ALL SELECT 36130862
UNION ALL SELECT 36130863
UNION ALL SELECT 36130864
UNION ALL SELECT 36130865
UNION ALL SELECT 36308671
UNION ALL SELECT 36308673
UNION ALL SELECT 36308674
UNION ALL SELECT 36308675
UNION ALL SELECT 36308676
UNION ALL SELECT 36308677
UNION ALL SELECT 36308680
UNION ALL SELECT 36308681
UNION ALL SELECT 36308682
UNION ALL SELECT 36308683
UNION ALL SELECT 36308684
UNION ALL SELECT 36308693
UNION ALL SELECT 36308694
UNION ALL SELECT 36308695
UNION ALL SELECT 36308696
UNION ALL SELECT 36308697
UNION ALL SELECT 36308698
UNION ALL SELECT 36308700
UNION ALL SELECT 36308701
UNION ALL SELECT 36308702
UNION ALL SELECT 36308703
UNION ALL SELECT 36308704
UNION ALL SELECT 36308705
UNION ALL SELECT 36308706
UNION ALL SELECT 36308707
UNION ALL SELECT 36308708
UNION ALL SELECT 36308709
UNION ALL SELECT 36308710
UNION ALL SELECT 36308711
UNION ALL SELECT 36308712
UNION ALL SELECT 36308713
UNION ALL SELECT 36308714
UNION ALL SELECT 36308715
UNION ALL SELECT 36308716
UNION ALL SELECT 36308717
UNION ALL SELECT 36308718
UNION ALL SELECT 36308719
UNION ALL SELECT 36308720
UNION ALL SELECT 36308721
UNION ALL SELECT 36308722
UNION ALL SELECT 36308723
UNION ALL SELECT 36308724
UNION ALL SELECT 36308725
UNION ALL SELECT 36308726
UNION ALL SELECT 36309481
UNION ALL SELECT 36309482
UNION ALL SELECT 36309483
UNION ALL SELECT 36309484
UNION ALL SELECT 37530319
--endregion
  				)	AS	TRI
         LEFT JOIN	(	-- DTH data
					       SELECT SCMS_SUBSCRIBER_ID
            						,PANEL_ID_REPORTED
            						,MAX(DATE(CASE WHEN	LOG_RECEIVED_DATETIME BETWEEN '2015-03-29 01:00:00' 
                                                                AND '2015-10-25 01:00:00'	THEN	LOG_RECEIVED_DATETIME + CAST('1 HOUR' AS INTERVAL)
                     												ELSE	LOG_RECEIVED_DATETIME	-- CORRECT FOR BST
                  											END	-- endregion
                 										)
								          )	AS	LAST_LOG_RECEIVED_DATE
        					FROM	DIS_REFERENCE..FINAL_DTH_VIEWING_EVENT_HISTORY
       					WHERE		--	Limit to Ethan panels
							           PANEL_ID_REPORTED		in		(8,14,15)							
        						AND	LOG_RECEIVED_DATETIME BETWEEN	DATE(NOW()) - CAST('60 DAYS' AS INTERVAL)	-- Global analysis time period
                                												AND	DATE(NOW())
  						      AND SCMS_SUBSCRIBER_ID IN	(	-- REGION Ethan field triallists & D12
		 												35500464
														,35541813
														,35541809
														,35619847
														,35500461
														,35541807
														,35500460
														,35554905
														,35554907
														,35554913
														,35554910
														,35554890
														,35554900
														,35554895
														,35554923
														,35554899
														,35554915
														,35554919
														,35554891
														,35554904
														,35554851
														,35554922
														,35554906
														,35619599
														,35554897
														,35554903
														,35554927
														,35554898
														,35554912
														,35554928
														,35554908
														,35554914
														,35554911
														,35554888
														,35554894
														,35554925
														,35554929
														,35554892
														,35554916
														,35554902
														,35554879
														,35554854
														,35554872
														,35554861
														,35554855
														,35554882
														,35554871
														,35554874
														,35554841
														,35554884
														,35554881
														,35554845
														,35554875
														,35554862
														,35554856
														,35619597
														,35619598
														,35619731
														,35554926
														,35554933
														,35710879
														,35554930
														,35554909
														,35554931
														,35554920
														,35554921
														,35853855
														,35554934
														,35554935
														,35641456
														,35554870
														,35554885
														,35554850
														,35554846
														,35554887
														,35554886
														,35554883
														,35619601
														,35554847
														,35554876
														,35554857
														,35619600
														,35724049
														,35554889
														,35710878
														,35619596
														,35554844
														,35853857
														,35554893
														,35853856
														,35554852
														,35853854
														,35554840
														,35554873
														,35554848
														,35554877
														,35554842
														,35554843
														,35963116
														,35963127
														,35963151
														,35963095
														,35963119
														,35963123
														,35963150
														,35963103
														,35963097
														,35967394
														,35963152
														,35963094
														,35963128
														,35963113
														,35963092
														,35963149
														,35967402
														,35963157
														,35963130
														,35963131
														,35963108
														,36130863
														,35963017
														,35963063
														,35963134
														,35963148
														,35963058
														,35963145
														,35963110
														,35963018
														,35963057
														,35963061
														,35963093
														,35963051
														,35963015
														,35963024
														,35963098
														,35963054
														,35963036
														,35963025
														,35963022
														,35963044
														,35963038
														,35963142
														,35963101
														,35963066
														,35963068
														,35963047
														,35963040
														,35963124
														,35963031
														,35963138
														,35963053
														,35963065
														,35963091
														,35963096
														,35963027
														,35963062
														,35963055
														,35963039
														,35963020
														,35963114
														,35963139
														,35963100
														,35963141
														,35963034
														,35963035
														,35963136
														,35963099
														,35963155
														,35963146
														,35963144
														,35963142
														,35963161
														,35963133
														,35963387
														,35967396
														,35963386
														,35963029
														,35963147
														,35963156
														,35967399
														,35967398
														,35963107
														,35967401
														,35963415
														,35963120
														,35967392
														,35963388
														,35963153
														,35967421
														,35963158
														,35963160
														,35963162
														,35963389
														,35963393
														,35963115
														,35963459
														,35963478
														,35963516
														,35963541
														,35963129
														,35963132
														,35963135
														,35963137
														,35963117
														,35963125
														,35963121
														,35963439
														,35963154
														,35963140
														,35963159
														,35963112
														,35531678
														,35554924
														,35500464
														,35531680
														,35531675
														,35593290
														,35531683
														,35531686
														,35539105
														,35531685
														,35500459
														,35525696
														,35541808
														,35471481
														,35593289
														,35531676
														,35531684
														,35531687
														,35531681
														,35531682
														,35593292
														,35593291
														,25518793	-- after here added 04/08/2015
              ,36308671
              ,36308715
              ,36308673
              ,36308716
              ,36308718
              ,36308674
              ,36308721
              ,36308675
              ,216682
              ,36309483
              ,36308726
              ,36308725
              ,36308724
              ,36308723
              ,36308676
              ,36309484
              ,36308720
              ,36308719
              ,36308677
              ,36308680
              ,36308681
              ,36308682
              ,36308717
              ,36308714
              ,36308712
              ,36308709
              ,36309481
              ,36308708
              ,36308706
              ,36308704
              ,36308702
              ,28791603
              ,36308696
              ,36308683
              ,36308684
              ,36308693
              ,36308695
              ,36308697
              ,36308698
              ,36308700
              ,36308701
              ,36309482
              ,36308703
              ,36308705
              ,36308707
              ,36308710
              ,36308711
              ,36308713
              ,36308694
              ,35963127
              ,35963023
              ,35963155
              ,35963017
              ,35963146
              ,35963144
              ,35963142
              ,35963063
              ,35963161
              ,35963134
              ,35963133
              ,35963387
              ,35963116
              ,35963148
              ,35963151
              ,35963058
              ,35963095
              ,35963033
              ,35963145
              ,35963110
              ,35963119
              ,35963018
              ,35963057
              ,35963123
              ,35963150
              ,35963061
              ,35963093
              ,35967396
              ,35963049
              ,35963386
              ,35963029
              ,35963015
              ,35963045
              ,35963147
              ,35963156
              ,35963024
              ,35963098
              ,35967399
              ,35963103
              ,35967398
              ,35963046
              ,35963054
              ,35963107
              ,35967401
              ,35963415
              ,35963036
              ,35963025
              ,35963022
              ,35963044
              ,35963120
              ,35967392
              ,35963038
              ,35963097
              ,35963099
              ,35967394
              ,35963152
              ,35963142
              ,35963388
              ,35963094
              ,35963101
              ,35963043
              ,35963066
              ,35963068
              ,35963047
              ,35963040
              ,35963016
              ,35963153
              ,35963111
              ,35963124
              ,35967421
              ,35963037
              ,35963109
              ,35963050
              ,35963031
              ,35963138
              ,35963158
              ,35963160
              ,35963053
              ,35963162
              ,35963065
              ,35963091
              ,35963096
              ,35963027
              ,35963062
              ,35963055
              ,35963128
              ,35963113
              ,35963389
              ,35963393
              ,35963115
              ,35963092
              ,35963064
              ,35963067
              ,35963459
              ,35963478
              ,35963102
              ,35963039
              ,35963516
              ,35963541
              ,35963129
              ,35963132
              ,35963135
              ,35963052
              ,35963059
              ,35963149
              ,35967402
              ,35967403
              ,35963089
              ,35963157
              ,35963137
              ,36130829
              ,35963130
              ,36130862
              ,35963020
              ,35963114
              ,35963117
              ,35963125
              ,36130827
              ,35963139
              ,35963100
              ,36130828
              ,36130865
              ,36130864
              ,35963131
              ,35963108
              ,35963141
              ,35963034
              ,35963035
              ,35963121
              ,35963439
              ,35963154
              ,35963140
              ,35963159
              ,35963056
              ,35963028
              ,35963106
              ,35963048
              ,35963042
              ,35963136
              ,36130863
              ,35963026
              ,35963112
              ,36130861
              ,35724049
              ,37530319
              ,35710878
			  
			  													  ,35541815
													  ,35619736
													  ,35541811
													  ,35396756
													  ,35500462
													  ,35619843
													  ,35500463
													  ,35619741
													  ,35619733
													  ,34307704
													  ,36021973
													  ,35999146
													  ,36021976
													  ,36021975
													  ,36022026
													  ,36021967
													  ,36021978
													  ,34939046
													  ,36021972 --endregion
													)
					GROUP BY SCMS_SUBSCRIBER_ID
						       ,PANEL_ID_REPORTED
				)	AS	DTH	ON	TRI.subID	=	DTH.SCMS_SUBSCRIBER_ID
GROUP BY TRI.subID
	       ,DTH.PANEL_ID_REPORTED
	       ,DTH.LAST_LOG_RECEIVED_DATE
ORDER BY	DTH.LAST_LOG_RECEIVED_DATE
	       ,DTH.PANEL_ID_REPORTED
	       ,DTH.LAST_LOG_RECEIVED_DATE
;




----------------------------------------------------------------------
-- 3. Even simpler query to show the last log received date per device
----------------------------------------------------------------------
  SELECT scms_subscriber_id
        ,DEVICE_ID
        ,max(CASE WHEN	LOG_RECEIVED_DATETIME BETWEEN '2015-03-29 01:00:00' 
                                                  AND '2015-10-25 01:00:00'	THEN	LOG_RECEIVED_DATETIME + CAST('1 HOUR' AS INTERVAL)
                            												ELSE	LOG_RECEIVED_DATETIME	-- CORRECT FOR BST
              END
             )	 as dttm
    FROM DIS_REFERENCE..FINAL_DTH_VIEWING_EVENT_HISTORY
   WHERE PANEL_ID_REPORTED = 15
/*   
     and CASE WHEN LOG_RECEIVED_DATETIME BETWEEN '2015-03-29 01:00:00' 
                                             AND '2015-10-25 01:00:00'	THEN	LOG_RECEIVED_DATETIME + CAST('1 HOUR' AS INTERVAL)
              ELSE LOG_RECEIVED_DATETIME
--          END < date(now())-1
          END < '2015-09-12 01:00:00'
*/		
group by DEVICE_ID   
        ,scms_subscriber_id
;

----------------------------------------------------------------------
-- 4. All Events for D12
----------------------------------------------------------------------
  SELECT *
    FROM DIS_REFERENCE..FINAL_DTH_VIEWING_EVENT_HISTORY
   WHERE event_start_reported_datetime >= date(now())-1
     AND SCMS_SUBSCRIBER_ID IN (      --region
	                                                    25518793
                                                       ,35500461
                                                       ,35541807
                                                       ,35541809
                                                       ,35541810
                                                       ,35541813
                                                       ,35593298
                                                       ,35619733
                                                       ,35619735
                                                       ,35619841
                                                       ,35619847
                                                       ,35531686
                                                       ,25518793
                                                       ,35500459
                                                       ,35500464
                                                       ,35525696
                                                       ,35525697
                                                       ,35525699
                                                       ,35525700
                                                       ,35531678
                                                       ,35531680
                                                       ,35531681
                                                       ,35531683
                                                       ,35531684
                                                       ,35531685
                                                       ,35531687
                                                       ,35539105
                                                       ,35541808
                                                       ,35593290
                                                       ,35593291
                                                       ,35593292
													  ,35541815
													  ,35619736
													  ,35541811
													  ,35396756
													  ,35500462
													  ,35619843
													  ,35500463
													  ,35619741
													  ,35619733
													  ,34307704
													  ,36021973
													  ,35999146
													  ,36021976
													  ,36021975
													  ,36022026
													  ,36021967
													  ,36021978
													  ,34939046
													  ,36021972 --endregion
                                                       )


----------------------------------------------------------------------
-- 4. Latest event for specific devices
----------------------------------------------------------------------
  SELECT DEVICE_ID
         ,max(event_start_reported_datetime)
    FROM DIS_REFERENCE..FINAL_DTH_VIEWING_EVENT_HISTORY
   WHERE DEVICE_ID IN ('32C0000480006378'
                      ,'32D0000480009017'                                                               
                      ,'32B0550480007100'     
					  ,'32D0000480005911'
                      )
group by DEVICE_ID
;

----------------------------------------------------------------------
-- 5. All events since yesterday for specific devices
----------------------------------------------------------------------
  SELECT full_channel_name
        ,event_start_datetime
        ,event_end_datetime
		,event_action
		,*
    FROM DIS_REFERENCE..FINAL_DTH_VIEWING_EVENT_HISTORY
   WHERE event_start_reported_datetime >= date(now())-3
     AND DEVICE_ID = '32B0550480007100'
order by event_start_datetime
;



----------------------------------------------------------------------
--6. last log received date from ODS database
----------------------------------------------------------------------
  SELECT subscriberid as scms_subscriber_id
        ,src_DEVICE_ID as DEVICE_ID
        ,max(upper(CASE WHEN documentcreationdate BETWEEN '2015-03-29 01:00:00' 
                                                AND '2015-10-25 01:00:00'	THEN documentcreationdate + CAST('1 HOUR' AS INTERVAL)
                            												ELSE documentcreationdate	-- CORRECT FOR BST
             END
             ))	 as dttm
    FROM ODS_LOAD..ETHAN_AMS_VIEWING_EVENTS
   WHERE panelid = 15
--     and CASE WHEN documentcreationdate BETWEEN '2015-03-29 01:00:00' 
--                                             AND '2015-10-25 01:00:00'	THEN	documentcreationdate + CAST('1 HOUR' AS INTERVAL)
--              ELSE documentcreationdate
--          END < '2015-09-13 01:00:00'
group by src_DEVICE_ID   
        ,subscriberid
;



---------------------
----8. last event log
---------------------

  SELECT scms_subscriber_id
        ,DEVICE_ID
        ,max(CASE WHEN	LOG_RECEIVED_DATETIME BETWEEN '2015-03-29 01:00:00' 
                                                  AND '2015-10-25 01:00:00'	THEN	LOG_RECEIVED_DATETIME + CAST('1 HOUR' AS INTERVAL)
                            												ELSE	LOG_RECEIVED_DATETIME	-- CORRECT FOR BST
              END
             )	 as dttm
    FROM DIS_PREPARE..TD_DTH_VIEWING_LAST_event
   WHERE PANEL_ID_REPORTED = 15
group by DEVICE_ID   
        ,scms_subscriber_id



------------------------------
-- 9. D12 data received by day
------------------------------

  select device_id
        ,date(CASE WHEN LOG_RECEIVED_DATETIME BETWEEN '2015-03-29 01:00:00' 
                                             AND '2015-10-25 01:00:00' THEN	LOG_RECEIVED_DATETIME + CAST('1 HOUR' AS INTERVAL)                          											   
																	   ELSE LOG_RECEIVED_DATETIME
          END)as dt
        ,scms_subscriber_id
    FROM DIS_REFERENCE..FINAL_DTH_VIEWING_EVENT_HISTORY		  
   where dt >= '2015-11-01'
     and scms_subscriber_id in (25518793
,34307704
,35396756
,35500462
,35500463
,35500464
,35525696
,35525697
,35525699
,35525700
,35531678
,35531680
,35531681
,35531683
,35531684
,35531685
,35531686
,35531687
,35541811
,35541815
,35593291
,35619741
,35619841
,35619843
					)
group by device_id
        ,dt
        ,scms_subscriber_id
order by dt
        ,device_id
		
		
			  