/*###############################################################################
# Created on:   21/09/2012
# Created by:   Tony Kinnaird (TKD)
# Description:  Gives a user a process for those customers who have On-Net
#		Broadband
#
#################################################################################
# Dependencies
# ------------------------------------------------------------------------------
# (none)
#
#################################################################################
# Change log
# ------------------------------------------------------------------------------
# 21/09/2012  TKD   v01 - initial version
#
###############################################################################*/

-- ##############################################################################################################
-- ##### STEP 1.0 - Process									    #####
-- ##############################################################################################################

---------------------------------------------------------------------------------
--  SKY NET Onnet flag   [HH_BROADBAND_ON_NET]
---------------------------------------------------------------------------------

-- Use Easynet Rollout data for fuller ONNET identification
--==========================================================
-- 1) Get BROADBAND_POSTCODE_EXCHANGE postcodes
SELECT cb_address_postcode as postcode, MAX(mdfcode) as exchID
INTO #bpe
FROM sk_prod.BROADBAND_POSTCODE_EXCHANGE
GROUP BY postcode;
commit;

UPDATE #bpe SET postcode = REPLACE(postcode,' ',''); -- Remove spaces for matching
commit;

-- 2) Get BB_POSTCODE_TO_EXCHANGE postcodes
SELECT postcode, MAX(exchange_id) as exchID
INTO #p2e
FROM sk_prod.BB_POSTCODE_TO_EXCHANGE
GROUP BY postcode;
commit;

UPDATE #p2e SET postcode = REPLACE(postcode,' ',''); -- Remove spaces for matching
commit;

-- 3) Combine postcode lists taking BB_POSTCODE_TO_EXCHANGE exchange_id's where possible
SELECT COALESCE(#p2e.postcode, #bpe.postcode) AS postcode
,COALESCE(#p2e.exchID, #bpe.exchID) as exchange_id
,'OFFNET' as exchange
INTO #onnet_lookup
FROM #bpe FULL JOIN #p2e ON #bpe.postcode = #p2e.postcode;
commit;

-- 4) Update with latest Easynet exchange information
UPDATE #onnet_lookup
SET exchange = 'ONNET'
FROM #onnet_lookup AS base
INNER JOIN sk_prod.easynet_rollout_data as easy on base.exchange_id = easy.exchange_id
WHERE easy.exchange_status = 'ONNET';
commit;


-- 5) Flag your base table with onnet exchange data. Note that this uses a postcode field with
-- spaces removed so your table will either need to have a similar filed or use a REPLACE
-- function in the join
UPDATE VIQ_HH_ACCOUNT_TMP
SET HH_BROADBAND_ON_NET = CASE WHEN tgt.exchange = 'ONNET'
        THEN 1
        ELSE 0
        END
FROM VIQ_HH_ACCOUNT_TMP AS base
INNER JOIN #onnet_lookup AS tgt on base.postcode_no_space = tgt.postcode;
commit;

-- ##############################################################################################################
-- ##### STEP 1.0 Ended									    #####
-- ##############################################################################################################


