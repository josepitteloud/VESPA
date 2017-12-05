
/* ***********************************
 *                                  *
 *         SKY_GO_REG               *
 *                                  *
 ************************************/
MESSAGE 'Populate field SKY_GO_REG - START' type status to client
go     
IF EXISTS( SELECT tname FROM syscatalog
            WHERE creator='${CBAF_DB_LIVE_SCHEMA}'
              AND UPPER(tname)='TEMP_SKYGO_USAGE'
              AND UPPER(tabletype)='TABLE')
  BEGIN
    MESSAGE 'WARN: Temp Table TEMP_SKYGO_USAGE already exists - Drop and recreate' type status to client
    drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_SKYGO_USAGE
  END
MESSAGE 'Create Table TEMP_SKYGO_USAGE' type status to client
SELECT sky.account_number,
       1 AS sky_go_reg
INTO ${CBAF_DB_LIVE_SCHEMA}.TEMP_SKYGO_USAGE
FROM ${CBAF_DB_LIVE_SCHEMA}.SKY_PLAYER_USAGE_DETAIL AS sky
INNER JOIN ${CBAF_DB_DATA_SCHEMA}.ADSMART as base
    ON sky.account_number = base.account_number
WHERE sky.cb_data_date >= dateadd(month, -12, now())
  AND sky.cb_data_date < now()
GROUP BY sky.account_number
go
-- Create Index
CREATE  HG INDEX idx04 ON ${CBAF_DB_LIVE_SCHEMA}.TEMP_SKYGO_USAGE(account_number)
go

MESSAGE 'Update field SKY_GO_REG to ADSMART Table' type status to client
go
-- Update ADSMART Table
UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART a
    SET Sky_Go_Reg = case when sky_go.sky_go_reg = 1 then 'Yes' else 'No' end
    FROM ${CBAF_DB_LIVE_SCHEMA}.TEMP_SKYGO_USAGE AS sky_go
    WHERE a.account_number = sky_go.account_number                                                                                    
go
MESSAGE 'Drop Table TEMP_SKYGO_USAGE' type status to client
go
drop table ${CBAF_DB_LIVE_SCHEMA}.TEMP_SKYGO_USAGE 
go
MESSAGE 'Populate field SKY_GO_REG - COMPLETE' type status to client
go


/* ****************************************
 *                                       *
 *        SKY GO USAGE			 *
 *                                       *
 **************************************** --REPLACE BY A PRODUCTIONIZED TABLE -REWRITE THE DEFINITION ACCORDING TO THE DEFINITION */

MESSAGE 'POPULATE SKY GO USAGE - STARTS' type status to client
GO

SELECT   	  ACCOUNT_NUMBER 
		, SKYGO_USAGE_SEGMENT = CASE WHEN SKYGO_LATEST_USAGE_DATE >= DATEADD(MM,-3,GETDATE()) THEN 'Active'  -- ACTIVE USER: HAS USED SKYGO IN THE PAST 3 MONTHS
                                	WHEN SKYGO_LATEST_USAGE_DATE < DATEADD(MM,-3,GETDATE()) THEN 'Lapsed'        -- LAPSED > 1 YR: HAS USED SKYGO BETWEEN THE PAST YEAR AND 3 MONTHS AGO
                                	WHEN SKYGO_LATEST_USAGE_DATE IS NULL THEN 'Registered but never used'
                                        ELSE 'Non registered' END
    , RANK () OVER (PARTITION BY ACCOUNT_NUMBER ORDER BY SKYGO_LATEST_USAGE_DATE DESC, SKYGO_FIRST_STREAM_DATE DESC, CB_ROW_ID DESC) TMP_RANK
INTO ${CBAF_DB_DATA_SCHEMA}.TEMP_SKYGO_USAGE
FROM ${CBAF_DB_LIVE_SCHEMA}.SKY_OTT_USAGE_SUMMARY_ACCOUNT
GO

DELETE FROM ${CBAF_DB_DATA_SCHEMA}.TEMP_SKYGO_USAGE
WHERE TMP_RANK > 1
GO

CREATE HG INDEX SKYGO1 ON ${CBAF_DB_DATA_SCHEMA}.TEMP_SKYGO_USAGE(ACCOUNT_NUMBER)
GO

UPDATE ${CBAF_DB_DATA_SCHEMA}.ADSMART
SET BASE.VIEWING_OF_SKY_GO = COALESCE(TMP_SKYGO_USG.SKYGO_USAGE_SEGMENT, 'Unknown')
FROM ${CBAF_DB_DATA_SCHEMA}.ADSMART AS BASE
JOIN ${CBAF_DB_DATA_SCHEMA}.TEMP_SKYGO_USAGE AS TMP_SKYGO_USG ON BASE.ACCOUNT_NUMBER = TMP_SKYGO_USG.ACCOUNT_NUMBER  
GO

DROP TABLE ${CBAF_DB_DATA_SCHEMA}.TEMP_SKYGO_USAGE
GO

MESSAGE 'POPULATE SKY GO USAGE - COMPLETED' type status to client
GO