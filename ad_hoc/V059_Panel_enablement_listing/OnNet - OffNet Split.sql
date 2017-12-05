-- We 

-- 1) Get BROADBAND_POSTCODE_EXCHANGE postcodes
    SELECT cb_address_postcode as postcode, MAX(mdfcode) as exchID
      INTO #bpe
      FROM sk_prod.BROADBAND_POSTCODE_EXCHANGE
  GROUP BY postcode;

  UPDATE #bpe SET postcode = REPLACE(postcode,' ',''); -- Remove spaces for matching

-- 2) Get BB_POSTCODE_TO_EXCHANGE postcodes
    SELECT postcode, MAX(exchange_id) as exchID
      INTO #p2e
      FROM sk_prod.BB_POSTCODE_TO_EXCHANGE
  GROUP BY postcode;

  UPDATE #p2e SET postcode = REPLACE(postcode,' ','');  -- Remove spaces for matching

-- 3) Combine postcode lists taking BB_POSTCODE_TO_EXCHANGE exchange_id's where possible
SELECT COALESCE(#p2e.postcode, #bpe.postcode) AS postcode
      ,COALESCE(#p2e.exchID, #bpe.exchID) as exchange_id
      ,'OFFNET' as exchange
  INTO #onnet_lookup
  FROM #bpe FULL JOIN #p2e ON #bpe.postcode = #p2e.postcode;
 
-- 4) Update with latest Easynet exchange information
UPDATE #onnet_lookup
   SET exchange = 'ONNET'
  FROM #onnet_lookup AS base
       INNER JOIN sk_prod.easynet_rollout_data as easy on base.exchange_id = easy.exchange_id
WHERE easy.exchange_status = 'ONNET'; 

-- 5) Now with the base table, assemble the account lookup
select elap.account_number
    ,elap.panel_id
    ,min(cb_address_postcode) as postcode -- it's arbitrary, if there are duplicates then SAV is bad...
    ,convert(bit, 0) as onnet
into #onnet_patch
from V059_enablement_listing_all_panels as elap
inner join sk_prod.cust_single_account_view as sav
on sav.account_number = elap.account_number
where sav.cust_active_dtv = 1 and panel_id is not null -- OK, so we're getting account number duplicates, that's annoying...
group by elap.account_number, elap.panel_id -- account_number is a PK on this table
;

-- Need upercase and space-free postcodes
update #onnet_patch
set postcode = upper(REPLACE(postcode,' ',''));

commit;
create unique index fake_pk on #onnet_patch (account_number);
create index joinsy on #onnet_patch (postcode);
commit;

-- Make the OnNet flags:
UPDATE #onnet_patch
   SET onnet = CASE WHEN tgt.exchange = 'ONNET'
                    THEN 1
                    ELSE 0
               END
  FROM #onnet_patch AS base
       INNER JOIN #onnet_lookup AS tgt on base.postcode = tgt.postcode;
commit;

-- Pull out the numbers!
select panel_id, onnet, count(1)
from #onnet_patch
group by panel_id, onnet
order by panel_id, onnet;