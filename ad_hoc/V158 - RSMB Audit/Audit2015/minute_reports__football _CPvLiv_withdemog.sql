SELECT UTC_TIME.UTC_BARB_MINUTE,
fshh.scaling_universe_key,
fshh.scaling_attribute_01, 
fshh.scaling_attribute_02,
        count(distinct a.account_number) as no_accounts,
        SUM(CASE WHEN A.ACCOUNT_NUMBER IS NULL THEN 0 ELSE 1 END) AS NO_RECORDS ,
        SUM(CASE WHEN A.EVENT_WEIGHT IS NULL THEN 0 ELSE
                                        A.EVENT_WEIGHT END) AS TOTAL_LIVE_WEIGHT_EVENT, -- used in Viewer 360
        sum(CASE WHEN FSHH.WEIGHT_SCALED_VALUE IS NULL THEN 0
                                 ELSE FSHH.WEIGHT_SCALED_VALUE  END ) AS TOTAL_LIVE_WEIGHT_TE -- used in TE
FROM (
                SELECT CAST('2014-11-23' AS DATE) + CAST(UTC_TIME AS TIME) AS UTC_BARB_MINUTE
                FROM SMI_ACCESS..V_TIME_DIM
                WHERE CLOCK_OFFSET_TYPE_ID = 1 AND DATE_PART('Second',CAST(UTC_TIME AS TIME)) = 0
                                AND UTC_BARB_MINUTE > '2014-11-23 09:59:59'
								AND UTC_BARB_MINUTE < '2014-11-23 20:00:00'
--                UNION ALL
--                SELECT CAST('2015-02-09' AS DATE) + CAST(UTC_TIME AS TIME) AS UTC_BARB_MINUTE
--                FROM SMI_ACCESS..V_TIME_DIM
--                WHERE CLOCK_OFFSET_TYPE_ID = 1 AND DATE_PART('Second',CAST(UTC_TIME AS TIME)) = 0
                                
        ) UTC_TIME
LEFT JOIN
        (
        SELECT  DH1.UTC_DAY_DATE + CAST(TD1.UTC_TIME AS TIME) AS BARB_START_DATEHOUR,
                        DH2.UTC_DAY_DATE + CAST(TD2.UTC_TIME AS TIME) AS BARB_END_DATEHOUR,
                        WEIGHT_SCALED AS EVENT_WEIGHT,
                        BACD.ACCOUNT_NUMBER
        FROM (select * from SMI_ACCESS..V_VIEWING_PROGRAMME_INSTANCE_FACT where DK_PROGRAMME_INSTANCE_DIM=1210010401)  VPIF
        -- BARB START DATE AND TIME
        JOIN SMI_ACCESS..V_DATEHOUR_DIM DH1
                ON VPIF.DK_BARB_MIN_START_DATEHOUR_DIM = DH1.PK_DATEHOUR_DIM
        JOIN SMI_ACCESS..V_TIME_DIM TD1
                ON VPIF.DK_BARB_MIN_START_TIME_DIM = TD1.PK_TIME_DIM
        -- BARB END DATE AND TIME
        JOIN SMI_ACCESS..V_DATEHOUR_DIM DH2
                ON VPIF.DK_BARB_MIN_END_DATEHOUR_DIM = DH2.PK_DATEHOUR_DIM
        JOIN SMI_ACCESS..V_TIME_DIM TD2
                ON VPIF.DK_BARB_MIN_END_TIME_DIM = TD2.PK_TIME_DIM
--            JOIN SMI_ACCESS..V_VIEWING_EVENT_DIM VED
--                         ON VPIF.dk_viewing_event_dim = VED.PK_VIEWING_EVENT_DIM
        -- PICK UP THE ACCOUNT NUMBER
        --inner JOIN MDS..BILLING_CUSTOMER_ACCOUNT_DIM BACD --dont use
              inner JOIN SMI_ACCESS..V_BILLING_CUSTOMER_ACCOUNT_DIM BACD --modified 2015-03-04 [use this]
--                ON VPIF.DK_BILLING_CUSTOMER_ACCOUNT_DIM = BACD.NK_BILLING_CUSTOMER_ACCOUNT_DIM  --option old
                ON VPIF.DK_BILLING_CUSTOMER_ACCOUNT_DIM = BACD.PK_BILLING_CUSTOMER_ACCOUNT_DIM --on prod current
                           
        -- note the use of the playback field to determine whether the viewing is live
-- JOIN SMI_DW..PLAYBACK_DIM PD --original
         JOIN SMI_ACCESS..V_PLAYBACK_DIM PD --use this one
                ON VPIF.DK_PLAYBACK_DIM = PD.PK_PLAYBACK_DIM
        WHERE (DK_BARB_MIN_START_DATEHOUR_DIM BETWEEN 2014112310 AND 2014112319
                OR DK_BARB_MIN_END_DATEHOUR_DIM BETWEEN 2014112310 AND 2014112319)
                AND WEIGHT_SCALED IS NOT NULL
                           AND PD.LIVE_OR_RECORDED = 'LIVE'
--                         AND VED.PANEL_ID in (11,12)  --new condition to restrict to daily panel
              -- group-by required for deduping duplicate DTH_VIEWING_EVENT_ID records in FINAL_DTH_VIEWING_HISTORY
         Group by    BARB_START_DATEHOUR,
                                  BARB_END_DATEHOUR,
                                  EVENT_WEIGHT,
                                  ACCOUNT_NUMBER,
                                  VPIF.DTH_VIEWING_EVENT_ID  
--                                limit 10000;
        ) A
ON UTC_TIME.UTC_BARB_MINUTE BETWEEN A.BARB_START_DATEHOUR AND A.BARB_END_DATEHOUR
LEFT JOIN DIS_REFERENCE..FINAL_SCALING_HOUSEHOLD_HISTORY FSHH
        ON A.ACCOUNT_NUMBER = FSHH.ACCOUNT_NUMBER
        AND CAST(UTC_TIME.UTC_BARB_MINUTE AS DATE) = FSHH.EVENT_START_DATE
GROUP BY UTC_BARB_MINUTE,fshh.scaling_universe_key,fshh.scaling_attribute_01, fshh.scaling_attribute_02

ORDER BY 1
