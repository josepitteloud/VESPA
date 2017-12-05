/*

        HISTORIC CUSTOMER VALUE SEGMENTS
        --------------------------------

        Author  : Nick Leech
        Date    : 21st July 2011


        Use this script to identify value segments for accounts at previous dates
        To identify current value segments then use the data in table sk_prod.VALUE_SEGMENTS_DATA

        Instructions
        ~~~~~~~~~~~~
        Update Section 02 to populate the #Value_Segments table with the accounts and dates you want to target
        Note that the same account may appear multiple times for different dates

        Once complete use the #value segments table to update the working tables for your projects.

        This script can either be incorporated into your own script or run as a stand alone routine.
        It takes around 10 minutes to run for 100K records.


          SECTIONS
          --------
          01 - Create table
          02 - Populate table
          03 - SAV Updates
          04 - Long term events
          05 - TA Events
          06 - Min Max Premiums
          07 - Make Value Segments


*/



-------------------------------------------------  01 - Create table

CREATE TABLE #value_segments(
        id                      BIGINT          IDENTITY PRIMARY KEY
       ,account_number          VARCHAR(20)     NOT NULL
       ,subscription_id         VARCHAR(50)     NULL
       ,target_date             DATE            NOT NULL
       ,segment                 VARCHAR(20)     NULL

       ,first_activation_dt     DATE            NULL
       ,active_days             INTEGER         NULL
       ,CUSCAN_ever             INTEGER         DEFAULT 0
       ,CUSCAN_2Yrs             INTEGER         DEFAULT 0
       ,SYSCAN_ever             INTEGER         DEFAULT 0
       ,SYSCAN_2Yrs             INTEGER         DEFAULT 0
       ,AB_ever                 INTEGER         DEFAULT 0
       ,AB_2Yrs                 INTEGER         DEFAULT 0
       ,PC_ever                 INTEGER         DEFAULT 0
       ,PC_2Yrs                 INTEGER         DEFAULT 0
       ,TA_2yrs                 INTEGER         DEFAULT 0
       ,min_prem_2yrs           INTEGER         DEFAULT 0
       ,max_prem_2yrs           INTEGER         DEFAULT 0
       ,segment_cuscan_only                 VARCHAR(20)     NULL
);

CREATE   HG INDEX idx01 ON #value_segments(account_number);
CREATE DATE INDEX idx02 ON #value_segments(target_date);
CREATE   LF INDEX idx03 ON #value_segments(segment);
CREATE   HG INDEX idx04 ON #value_segments(subscription_id);


-------------------------------------------------  02 - Populate table

-- Alter this query to append the accounts and specific dates you want to identify the segments for.

  INSERT INTO #value_segments (account_number, target_date)
  SELECT account_number, cast('2012-10-15' as date)
    FROM sk_prod.CUST_SINGLE_ACCOUNT_VIEW
 ;



------------------------------------------------ 03 - SAV = First Activation Date & Subscription ID


UPDATE  #value_segments
   SET  first_activation_dt   = sav.ph_subs_first_activation_dt
       ,subscription_id       = sav.prod_ph_subs_subscription_id
  FROM #value_segments AS acc
       INNER JOIN sk_prod.cust_single_account_view AS sav ON acc.account_number = sav.account_number;


UPDATE #value_segments
   SET active_days = DATEDIFF(day,first_activation_dt,target_date);


----------------------------------------------- 04 - Long term events

--unique list of accounts
  SELECT account_number, subscription_id, MAX(target_date) as maxDate, MIN(target_date) as minDate
    INTO #account_list
    FROM #value_segments
GROUP BY account_number, subscription_id;

CREATE  HG INDEX idx01 ON #account_list(account_number);
CREATE HG INDEX idx02 ON #account_list(subscription_id);

--historic status event changes
CREATE TABLE #status_events (
        id                      BIGINT          IDENTITY     PRIMARY KEY
       ,account_number          VARCHAR(20)     NOT NULL
       ,effective_from_dt       DATE            NOT NULL
       ,status_code             VARCHAR(2)      NOT NULL
       ,event_type              VARCHAR(20)     NOT NULL
);

CREATE   HG INDEX idx01 ON #status_events(account_number);
CREATE   LF INDEX idx02 ON #status_events(event_type);
CREATE DATE INDEX idx03 ON #status_events(effective_from_dt);


INSERT INTO #status_events (account_number, effective_from_dt, status_code, event_type)
SELECT  csh.account_number
       ,csh.effective_from_dt
       ,csh.status_code
       ,CASE WHEN status_code = 'PO'              THEN 'CUSCAN'
             WHEN status_code = 'SC'              THEN 'SYSCAN'
             WHEN status_code = 'AB'              THEN 'ACTIVE BLOCK'
             WHEN status_code = 'PC'              THEN 'PENDING CANCEL'
         END AS event_type
  FROM sk_prod.cust_subs_hist AS csh
       INNER JOIN #account_list AS al ON csh.account_number = al.account_number
 WHERE csh.subscription_sub_type = 'DTV Primary Viewing'
   AND csh.status_code_changed = 'Y'
   AND csh.effective_from_dt <= al.maxDate
   AND (    (csh.status_code IN ('AB','PC') AND csh.prev_status_code = 'AC')
         OR (csh.status_code IN ('PO','SC') AND csh.prev_status_code IN ('AC','AB','PC'))
       );


-- Update value Segments

UPDATE  #value_segments
   SET  CUSCAN_ever             = tgt.CUSCAN_ever
       ,CUSCAN_2Yrs             = tgt.CUSCAN_2Yrs
       ,SYSCAN_ever             = tgt.SYSCAN_ever
       ,SYSCAN_2Yrs             = tgt.SYSCAN_2Yrs
       ,AB_ever                 = tgt.AB_ever
       ,AB_2Yrs                 = tgt.AB_2Yrs
       ,PC_ever                 = tgt.PC_ever
       ,PC_2Yrs                 = tgt.PC_2Yrs
  FROM #value_segments AS base
       INNER JOIN (
                    SELECT vs.id

                           --CUSCAN
                           ,SUM(CASE WHEN se.status_code = 'PO'
                                      AND  se.effective_from_dt <= vs.target_date
                                     THEN 1 ELSE 0 END) AS CUSCAN_ever
                           ,SUM(CASE WHEN se.status_code = 'PO'
                                      AND se.effective_from_dt BETWEEN DATEADD(year,-2,vs.target_date) AND vs.target_date
                                     THEN 1 ELSE 0 END) AS CUSCAN_2Yrs

                           --SYSCAN
                           ,SUM(CASE WHEN se.status_code = 'SC'
                                      AND  se.effective_from_dt <= vs.target_date
                                     THEN 1 ELSE 0 END) AS SYSCAN_ever
                           ,SUM(CASE WHEN se.status_code = 'SC'
                                      AND se.effective_from_dt BETWEEN DATEADD(year,-2,vs.target_date) AND vs.target_date
                                     THEN 1 ELSE 0 END) AS SYSCAN_2Yrs

                           --Active Block
                           ,SUM(CASE WHEN se.status_code = 'AB'
                                      AND se.effective_from_dt <= vs.target_date
                                     THEN 1 ELSE 0 END) AS AB_ever
                           ,SUM(CASE WHEN se.status_code = 'AB'
                                      AND se.effective_from_dt BETWEEN DATEADD(year,-2,vs.target_date) AND vs.target_date
                                     THEN 1 ELSE 0 END) AS AB_2Yrs

                           --Pending Cancel
                           ,SUM(CASE WHEN se.status_code = 'PC'
                                      AND se.effective_from_dt <= vs.target_date
                                     THEN 1 ELSE 0 END) AS PC_ever
                           ,SUM(CASE WHEN se.status_code = 'PC'
                                      AND se.effective_from_dt BETWEEN DATEADD(year,-2,vs.target_date) AND vs.target_date
                                     THEN 1 ELSE 0 END) AS PC_2Yrs
                      FROM #value_segments AS vs
                           INNER JOIN #status_events AS se ON vs.account_number = se.account_number
                  GROUP BY vs.id
       )AS tgt on base.id = tgt.id;


------------------------------------------------------------ 05 - TA Events


--List all unique days with TA event
SELECT  DISTINCT
        cca.account_number
       ,cca.attempt_date
  INTO #ta
  FROM sk_prod.cust_change_attempt AS cca
       INNER JOIN #account_list AS al  on cca.account_number = al.account_number
                                      AND cca.subscription_id = al.subscription_id
 WHERE change_attempt_type = 'CANCELLATION ATTEMPT'
   AND created_by_id NOT IN ('dpsbtprd', 'batchuser')
   AND Wh_Attempt_Outcome_Description_1 in ( 'Turnaround Saved'
                                            ,'Legacy Save'
                                            ,'Turnaround Not Saved'
                                            ,'Legacy Fail'
                                            ,'Home Move Saved'
                                            ,'Home Move Not Saved'
                                            ,'Home Move Accept Saved')
   AND cca.attempt_date BETWEEN DATEADD(day,-729,al.minDate) AND al.maxDate ;


CREATE HG INDEX idx01 ON #ta(account_number);

-- Update TA flags
UPDATE  #value_segments
   SET  TA_2Yrs = tgt.ta_2Yrs
  FROM #value_segments AS base
       INNER JOIN (
                    SELECT vs.id
                          ,SUM(CASE WHEN ta.attempt_date BETWEEN DATEADD(day,-729,vs.target_date) AND vs.target_date
                                     THEN 1 ELSE 0 END) AS ta_2Yrs
                      FROM #value_segments AS vs
                           INNER JOIN #ta AS ta ON vs.account_number = ta.account_number
                  GROUP BY vs.id
       )AS tgt on base.id = tgt.id;

------------------------------------------------------ 06 - Min Max Premiums

UPDATE  #value_segments
   SET  min_prem_2Yrs = tgt.min_prem_lst_2_yrs
       ,max_prem_2Yrs = tgt.max_prem_lst_2_yrs
  FROM  #value_segments AS acc
        INNER JOIN (
                   SELECT  base.id
                          ,MAX(cel.prem_movies + cel.prem_sports ) as max_prem_lst_2_yrs
                          ,MIN(cel.prem_movies + cel.prem_sports ) as min_prem_lst_2_yrs
                     FROM sk_prod.cust_subs_hist as csh
                          INNER JOIN #value_segments as base on csh.account_number = base.account_number
                          INNER JOIN sk_prod.cust_entitlement_lookup as cel on csh.current_short_description = cel.short_description
                    WHERE csh.subscription_type      =  'DTV PACKAGE'
                      AND csh.subscription_sub_type  =  'DTV Primary Viewing'
                      AND status_code in ('AC','AB','PC')
                      AND ( -- During 2 year Period
                            (    csh.effective_from_dt BETWEEN DATEADD(day,-729,base.target_date) AND base.target_date
                             AND csh.effective_to_dt >= csh.effective_from_dt
                             )
                            OR -- at start of 2 yr period
                            (    csh.effective_from_dt <= DATEADD(day,-729,base.target_date)
                             AND csh.effective_to_dt   > DATEADD(day,-729,base.target_date)  -- limit to report period
                             )
                          )
                  GROUP BY base.id
        )AS tgt ON acc.id = tgt.id;



------------------------------------------------------ 07 - Make Value Segments


UPDATE #value_segments
   SET       segment =     CASE WHEN active_days < 729                            -- All accounts in first 2 Years
                                THEN 'BEDDING IN'

                                WHEN active_days >= 1825                          -- 5 Years
                                 AND CUSCAN_ever + SYSCAN_ever = 0                -- Never Churned
                                 AND AB_ever + PC_ever = 0                        -- Never AB/PC ed
                                 AND ta_2Yrs = 0                                  -- No TA's in last 2 years
                                 AND min_prem_2yrs = 4                            -- Always top tier for last 2 years
                                THEN 'PLATINUM'

                                WHEN active_days >= 1825                          -- 5 Years
                                 AND CUSCAN_ever + SYSCAN_ever = 0                -- Never Churned
                                 AND AB_ever + PC_ever = 0                        -- Never AB/PC ed
                                 AND min_prem_2yrs > 0                            -- Always had Prems in last 2 Years
                                THEN 'GOLD'

                                WHEN CUSCAN_2Yrs + SYSCAN_2Yrs = 0                -- No Churn in last 2 years
                                 AND AB_2Yrs + PC_2Yrs = 0                        -- No AB/PC 's In last 2 Years
                                 AND min_prem_2yrs > 0                            -- Always had Prems in last 2 Years
                                THEN 'SILVER'

                                WHEN CUSCAN_ever + SYSCAN_ever > 0                -- All Churners
                                  OR AB_2Yrs + PC_2Yrs + ta_2Yrs >= 3             -- Blocks , cancels in last 2 years + ta in last 2 years >= 3
                                THEN 'UNSTABLE'

                                WHEN max_prem_2Yrs > 0                            -- Has Had prems in last 2 years
                                THEN 'BRONZE'

                                ELSE 'COPPER'                                        -- everyone else
                            END;
---Added code for Project 134
UPDATE #value_segments
   SET       segment_cuscan_only =     CASE WHEN active_days < 729                            -- All accounts in first 2 Years
                                THEN 'BEDDING IN'

                                WHEN active_days >= 1825                          -- 5 Years
                                 AND CUSCAN_ever + SYSCAN_ever = 0                -- Never Churned
                                 AND AB_ever + PC_ever = 0                        -- Never AB/PC ed
                                 AND ta_2Yrs = 0                                  -- No TA's in last 2 years
                                 AND min_prem_2yrs = 4                            -- Always top tier for last 2 years
                                THEN 'PLATINUM'

                                WHEN active_days >= 1825                          -- 5 Years
                                 AND CUSCAN_ever + SYSCAN_ever = 0                -- Never Churned
                                 AND AB_ever + PC_ever = 0                        -- Never AB/PC ed
                                 AND min_prem_2yrs > 0                            -- Always had Prems in last 2 Years
                                THEN 'GOLD'

                                WHEN CUSCAN_2Yrs + SYSCAN_2Yrs = 0                -- No Churn in last 2 years
                                 AND AB_2Yrs + PC_2Yrs = 0                        -- No AB/PC 's In last 2 Years
                                 AND min_prem_2yrs > 0                            -- Always had Prems in last 2 Years
                                THEN 'SILVER'

                                WHEN CUSCAN_ever  > 0                -- All Cuscan Churners
                                  OR  PC_2Yrs + ta_2Yrs >= 3             -- Pending cancels in last 2 years + ta in last 2 years >= 3
                                THEN 'UNSTABLE CUSCAN'

                                WHEN CUSCAN_ever + SYSCAN_ever > 0                -- All Churners
                                  OR AB_2Yrs + PC_2Yrs + ta_2Yrs >= 3             -- Blocks , cancels in last 2 years + ta in last 2 years >= 3
                                THEN 'UNSTABLE NON CUSCAN'

                                WHEN max_prem_2Yrs > 0 and  AB_2Yrs=0           -- Has Had prems in last 2 years and no AB
                                THEN 'BRONZE CUSCAN'

                                WHEN max_prem_2Yrs > 0                            -- Has Had prems in last 2 years
                                THEN 'BRONZE NON CUSCAN'

                                WHEN  AB_2Yrs=0           -- No AB in last 2 years
                                THEN 'COPPER CUSCAN'

                                ELSE 'COPPER NON CUSCAN'                                        -- everyone else
                            END;

/*
select segment
,segment_cuscan_only
,count(*) as records
from #value_segments
group by segment
,segment_cuscan_only;
*/