-- Main select statement to extract the slot isntance level attributes using campaign number

SELECT
  VIF_Viewing_Slot_Instance_Fact.PK_VIEWING_SLOT_INSTANCE_FACT,
  VSIF_Adsmart_Campaign.SALES_BUYER_NAME,
  VSIF_Adsmart_Campaign.SALES_ADVERTISER_NAME,
  VSIF_Adsmart_Campaign.SALES_PRODUCT_NAME,
  cast(VSIF_Adsmart_Campaign.CAMP_CODE as varchar(6)) as camp_code_6_chars,
  VSIF_Adsmart_Campaign.CAMP_CODE as campaign_code,
  VIF_Viewing_Slot_Instance_Fact.SLOT_PERCENTAGE_VIEWED,
  VIF_Viewing_Slot_Instance_Fact.ACTUAL_IMPRESSION,
  VSIF_Channel.CHANNEL_NAME,
  VSIF_Broadcast_Start_Datehour.BROADCAST_DAY_DATE,
  VSIF_Adsmart_Campaign_Fact.CAMPAIGN_TARGET_ACHIEVED_PERCENTAGE,
  VSIF_Slot.SLOT_DURATION,
  substr(VSIF_Broadcast_Start_Time.BROADCAST_TIME,1,2)||':'||substr(VSIF_Broadcast_Start_Time.BROADCAST_TIME,3,2)||':'||substr(VSIF_Broadcast_Start_Time.BROADCAST_TIME,5,2) as broadcast_time,
  VSIF_UTC_START_DATEHOUR.UTC_DAY_DATE,
  substr(VSIF_UTC_Start_Time.UTC_TIME,1,2)||':'||substr(VSIF_UTC_Start_Time.UTC_TIME,3,2)||':'||substr(VSIF_UTC_Start_Time.UTC_TIME,5,2) as utc_time,
  VSIF_Slot.CLEARCAST_COMMERCIAL_NO,
  VIF_Viewing_Slot_Instance_Fact.DURATION,
  VIF_Viewing_Slot_Instance_Fact.DK_SLOT_COPY_DIM,
  VIF_Viewing_Slot_Instance_Fact.DK_SLOT_REFERENCE_DIM,
   VIF_Viewing_Slot_Instance_Fact.DURATION,
  VSIF_DTH_Active_Viewing_Card.SCMS_SUBSCRIBER_ID,
  VSIF_MDS_Billing_Customer_acccount.ACCOUNT_NUMBER,
  BRA.ACCOUNT_ID,
  VIF_Viewing_Slot_Instance_Fact.ACTUAL_IMPRESSIONS_DAY_1_WEIGHTED
FROM
  ADMIN.V_SLOT_DIM  VSIF_Slot 
   INNER JOIN ADMIN.V_VIEWING_SLOT_INSTANCE_FACT  VIF_Viewing_Slot_Instance_Fact ON (VSIF_Slot.PK_SLOT_DIM=VIF_Viewing_Slot_Instance_Fact.DK_SLOT_DIM)
   INNER JOIN MDS..DTH_ACTIVE_VIEWING_CARD_DIM  VSIF_DTH_Active_Viewing_Card ON (VIF_Viewing_Slot_Instance_Fact.DK_DTH_ACTIVE_VIEWING_CARD_DIM=VSIF_DTH_Active_Viewing_Card.PK_DTH_ACTIVE_VIEWING_CARD_DIM  AND  VSIF_DTH_Active_Viewing_Card.CURRENT_FLAG=1)
   INNER JOIN MDS.ADMIN.DATEHOUR_DIM  VSIF_Broadcast_Start_Datehour ON (VIF_Viewing_Slot_Instance_Fact.DK_BROADCAST_START_DATEHOUR_DIM=VSIF_Broadcast_Start_Datehour.PK_DATEHOUR_DIM)
   INNER JOIN ADMIN.V_TIME_DIM  VSIF_Broadcast_Start_Time ON (VSIF_Broadcast_Start_Time.PK_TIME_DIM=VIF_Viewing_Slot_Instance_Fact.DK_BROADCAST_START_TIME_DIM)
   INNER JOIN MDS.ADMIN.DATEHOUR_DIM  VSIF_UTC_START_DATEHOUR ON (VIF_Viewing_Slot_Instance_Fact.DK_EVENT_START_DATEHOUR_DIM = VSIF_UTC_START_DATEHOUR.PK_DATEHOUR_DIM)
   INNER JOIN ADMIN.V_TIME_DIM  VSIF_UTC_START_TIME ON (VIF_Viewing_Slot_Instance_Fact.DK_EVENT_START_TIME_DIM = VSIF_UTC_START_TIME.PK_TIME_DIM)
   INNER JOIN ADMIN.V_CHANNEL_DIM  VSIF_Channel ON (VSIF_Channel.PK_CHANNEL_DIM=VIF_Viewing_Slot_Instance_Fact.DK_CHANNEL_DIM)
   INNER JOIN ADMIN.V_CAMPAIGN_DIM  VSIF_Adsmart_Campaign ON (VIF_Viewing_Slot_Instance_Fact.DK_ADSMART_MEDIA_CAMPAIGN_DIM=VSIF_Adsmart_Campaign.CAMP_PK)
   INNER JOIN ADMIN.V_ADSMART_CAMPAIGN_FACT  VSIF_Adsmart_Campaign_Fact ON (VSIF_Adsmart_Campaign.CAMP_PK=VSIF_Adsmart_Campaign_Fact.DK_CAMPAIGN_DIM)
   INNER JOIN MDS.ADMIN.BILLING_CUSTOMER_ACCOUNT_DIM  VSIF_MDS_Billing_Customer_acccount ON (VIF_Viewing_Slot_Instance_Fact.DK_BILLING_CUSTOMER_ACCOUNT_DIM=VSIF_MDS_Billing_Customer_acccount.PK_BILLING_CUSTOMER_ACCOUNT_DIM)
   INNER JOIN SMI_DW..BR_CUSTOMER_OBFUSCATION BRA ON VSIF_DTH_Active_Viewing_Card.NK_DTH_ACTIVE_VIEWING_CARD_DIM = BRA.NK_DTH_ACTIVE_VIEWING_CARD_DIM
WHERE
  (
   (  VIF_Viewing_Slot_Instance_Fact.DK_ADSMART_MEDIA_CAMPAIGN_DIM not in (-1, -99)
  )
  AND
  ( ( cast(VSIF_Adsmart_Campaign.CAMP_CODE as varchar(6)) ) IN ('259078','259080','262506') OR '%' IN ('259078','259080','262506'))
   AND
   ( ( VSIF_Adsmart_Campaign.SALES_ADVERTISER_NAME ) IN ('%') OR '%' IN ('%')  )
   AND
   ( ( VSIF_Adsmart_Campaign.SALES_PRODUCT_NAME ) IN ('%') or '%' IN  ('%')  )
   AND
   ( ( VSIF_Adsmart_Campaign.SALES_BUYER_NAME ) IN ('%') OR '%' IN ('%')  )
--  AND
 -- VSIF_Slot.CLEARCAST_COMMERCIAL_NO  IN  ('MNDFCIM001030')
  )


--- campaign summary data
-- campaign summary info tab

select  VCD.PK_CAMPAIGN_DIM,
                VCD.CAMPAIGN_CODE, 
                VCD.CAMPAIGN_START_DATE,
                VCD.CAMPAIGN_END_DATE,
                VACF.actual_impressions,
                VACF.CAMPAIGN_TARGET_IMPRESSIONS,
                VACF.CAMPAIGN_UNIVERSE_SIZE_DAY_ONE_WEIGHTED,
                vacf.universe_size,
                VACF.CAMPAIGN_ACTUAL_IMPRESSIONS_DAY_ONE_WEIGHTED,
                VCD.TECHNICAL_TOTAL_PVR_CAP,
                VCD.BUSINESS_TOTAL_PVR_CAP,
                VACF.DK_ADSMART_SEGMENT_DIM,
                ASD.SEGMENT_RULE                
from smi_dw..v_adsmart_campaign_fact VACF
        JOIN smi_dw..V_CAMPAIGN_DIM VCD
                ON VACF.DK_CAMPAIGN_DIM = VCD.PK_CAMPAIGN_DIM
                AND VCD.CAMPAIGN_CODE in (259078,259080,262506)
        JOIN SMI_DW..ADSMART_SEGMENT_DIM ASD
                ON VACF.DK_ADSMART_SEGMENT_DIM = ASD.PK_ADSMART_SEGMENT_DIM
order by campaign_code



-- VSIF data rec
-- VSIF summary tab
SELECT VCD.CAMPAIGN_CODE,
                SUM(ACTUAL_IMPRESSION) AS TOTAL_PUBLISHED_IMPRESSIONS,
                SUM(ACTUAL_IMPRESSIONS_DAY_1_WEIGHTED) AS TOTAL_RAW_IMPRESSIONS
FROM SMI_ACCESS.ADMIN.V_VIEWING_SLOT_INSTANCE_FACT VSIF
JOIN SMI_DW..CAMPAIGN_DIM VCD
        ON VSIF.DK_ADSMART_MEDIA_CAMPAIGN_DIM = VCD.PK_CAMPAIGN_DIM
        AND VCD.CAMPAIGN_CODE in (259078,259080,262506)
GROUP BY VCD.CAMPAIGN_CODE
ORDER BY 1


-- extract universe sizes               
                
SELECT  A.CAMPAIGN_CODE,
                FHH.EVENT_START_DATE,
                SUM(WEIGHT_SCALED_ADSMART_VALUE) AS UNIVERSE_SIZE,
                COUNT(1) AS PANEL_SIZE
FROM (SELECT VCD.CAMPAIGN_CODE,
                         VCD.CAMPAIGN_START_DATE,
                         VCD.CAMPAIGN_END_DATE,
                         HSF.ACCOUNT_NUMBER
          FROM SMI_DW..HOUSEHOLD_SEGMENT_FACT HSF
                JOIN smi_dw..adsmart_campaign_fact VACF
                        ON HSF.DK_ADSMART_SEGMENT_DIM = VACF.DK_ADSMART_SEGMENT_DIM
                JOIN smi_dw..CAMPAIGN_DIM VCD
                        ON VACF.DK_CAMPAIGN_DIM = VCD.PK_CAMPAIGN_DIM
                        AND VCD.CAMPAIGN_CODE in (259078,259080,262506)
                        AND HSF.DK_SEGMENT_DATE_DIM = VCD.CAMPAIGN_START_DATE
                GROUP BY VCD.CAMPAIGN_CODE,
                         VCD.CAMPAIGN_START_DATE,
                         VCD.CAMPAIGN_END_DATE,
                         HSF.ACCOUNT_NUMBER) A
JOIN DIS_REFERENCE..FINAL_SCALING_HOUSEHOLD_HISTORY FHH
        ON A.ACCOUNT_NUMBER = FHH.ACCOUNT_NUMBER
        AND -1*TO_NUMBER(FHH.EVENT_START_DATE,'999999999') between A.CAMPAIGN_START_DATE and A.campaign_end_date
GROUP BY        FHH.EVENT_START_DATE,
                        A.CAMPAIGN_CODE
order by 1,2
                
                                          


