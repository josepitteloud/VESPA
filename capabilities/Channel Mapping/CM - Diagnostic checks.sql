  CREATE VARIABLE @version int;
SET @version = 36



DELETE FROM CHANNEL_MAP_UPDATES
where version = @version

-- check validity of mandatory fields
INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version, 1,'Service key attributes','Check no null service_keys' as test, count(1) as result
FROM cortb.channel_map_dev_service_key_attributes
where service_key is null

INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version, 2,'Service key attributes','effective_to before effective_from' as test, count(1) as result
FROM cortb.channel_map_dev_service_key_attributes
where effective_to < effective_from --if SK 65535 need to check the SK under notes

INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version,3,'Service key attributes', 'BARB reported YES/NO' as test, count(1) as result
FROM cortb.channel_map_dev_service_key_attributes
where BARB_REPORTED not in ('YES','NO')

INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version,4,'Service key attributes', 'Timeshift_status Principal, Timeshift, NVOD' as test,  count(1) as result
FROM cortb.channel_map_dev_service_key_attributes
where timeshift_status not in ('Principal','Timeshift','NVOD')

INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version,5,'Service key attributes', 'timeshift_minutes 0,60,120' as test, count(1) as result
FROM cortb.channel_map_dev_service_key_attributes
where timeshift_minutes not in (0,60,120)

INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version,6,'Service key attributes', 'Sensitive channel 1,0' as test, count(1) as result
FROM cortb.channel_map_dev_service_key_attributes
where sensitive_channel not in (1,0)

INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version,7,'Service key attributes', 'spot_source BARB, Landmark, None' as test, count(1) as result
FROM cortb.channel_map_dev_service_key_attributes
where spot_source not in ('BARB','Landmark','None')

INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version,8,'Service key attributes', 'Inconsistent BARB reported and spot_source/promo source - some are ok' as test, count(1) as result
FROM cortb.channel_map_dev_service_key_attributes
 where BARB_reported = 'YES' and (spot_source ='None' or promo_source = 'None')

INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version,9,'Service key attributes', 'promo_source BARB, BSS, None' as test, count(1) as result
FROM cortb.channel_map_dev_service_key_attributes
where promo_source not in ('BARB','BSS','None')

INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version,10,'Service key attributes', 'channel_pack Diginets,Diginets non-commercial,Other,Other non-commercial,Terrestrial,Terrestrial non-commercial' as test, count(1) as result
FROM cortb.channel_map_dev_service_key_attributes
where channel_pack not in
(SELECT name FROM channel_pack)
-- check for duplicates
INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version,11,'Service key attributes', 'Duplicate service_key, effective_from - not On Demand records' as test, count(1) as result
FROM (
        SELECT service_key, effective_from, count(1) as no_dupes
        FROM cortb.channel_map_dev_service_key_attributes
        where service_key <> 65535
        GROUP by service_key, effective_from
        having count(1) >1
        ) a

INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version,12,'Service key attributes', 'Duplicate service_key, effective_from - On Demand only records' as test, count(1) as result
FROM (
        SELECT notes, effective_from, count(1) as no_dupes
        FROM cortb.channel_map_dev_service_key_attributes
        where service_key = 65535
        GROUP by notes, effective_from
        having count(1) >1
        ) a

-- check for overlaps
INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version,13,'Service key attributes', 'effective_from overlaps with another record - not On Demand records' as test, count(1) as result
FROM cortb.channel_map_dev_service_key_attributes sk1
        JOIN cortb.channel_map_dev_service_key_attributes sk2
        ON sk1.service_key = sk2.service_key and sk1.effective_from between sk2.effective_from and sk2.effective_to
        and sk1.effective_from <> sk2.effective_from
where sk1.service_key <> 65535

INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version,14,'Service key attributes', 'effective_from overlaps with another record - On Demand only records' as test, count(1) as result
FROM cortb.channel_map_dev_service_key_attributes sk1
        JOIN cortb.channel_map_dev_service_key_attributes sk2
        ON sk1.notes = sk2.notes and sk1.effective_from between sk2.effective_from and sk2.effective_to
        and sk1.effective_from <> sk2.effective_from
where sk1.service_key = 65535

INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version,15,'Service key attributes', 'Effective_to overlaps with another record - not On Demand records' as test, count(1) as result
FROM cortb.channel_map_dev_service_key_attributes sk1
        JOIN cortb.channel_map_dev_service_key_attributes sk2
        ON sk1.service_key = sk2.service_key and sk1.effective_to between sk2.effective_from and sk2.effective_to
        and sk1.effective_from <> sk2.effective_from
where sk1.service_key <> 65535
-- check effective_from > effective_to

INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version,16,'Service key attributes', 'Effective_to overlaps with another record - On Demand only records' as test, count(1) as result
FROM cortb.channel_map_dev_service_key_attributes sk1
        JOIN cortb.channel_map_dev_service_key_attributes sk2
        ON sk1.notes = sk2.notes and sk1.effective_to between sk2.effective_from and sk2.effective_to
        and sk1.effective_from <> sk2.effective_from
where sk1.service_key = 65535

INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version,17,'Service key BARB', 'Effective_from > effective_to' as test, count(1) as result
FROM  cortb.CHANNEL_MAP_DEV_SERVICE_KEY_BARB
where effective_from > effective_to
-- check for duplicates
INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version, 18,'Service key BARB','Duplicate service_key, log_station_code, sti_code, effective_from combination' as test, count(1) as result
FROM (
        SELECT service_key, log_station_code, sti_code, effective_from, count(1) as no_dupes
        FROM  cortb.CHANNEL_MAP_DEV_SERVICE_KEY_BARB
        GROUP BY service_key, log_station_code, sti_code, effective_from
        having count(1) >1
     ) a
-- check for overlap
INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version,19,'Service key BARB', 'Overlap of effective from with another record' as test, count(1) as result
FROM  cortb.CHANNEL_MAP_DEV_SERVICE_KEY_BARB b1
        JOIN  cortb.CHANNEL_MAP_DEV_SERVICE_KEY_BARB b2
        ON b1.service_key = b2.service_key and b1.log_station_code = b2.log_station_code and b1.sti_code = b2.sti_code
        AND b1.effective_from between b2.effective_from and b2.effective_to
        AND b1.effective_from <> b2.effective_from

INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version,20,'Service key BARB', 'Overlap of effective to with another record' as test, count(1) as result
FROM  cortb.CHANNEL_MAP_DEV_SERVICE_KEY_BARB b1
        JOIN  cortb.CHANNEL_MAP_DEV_SERVICE_KEY_BARB b2
        ON b1.service_key = b2.service_key and b1.log_station_code = b2.log_station_code and b1.sti_code = b2.sti_code
        AND b1.effective_to between b2.effective_from and b2.effective_to
        AND b1.effective_from <> b2.effective_from
-- check effective_from > effective_to
INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version,21,'Service key Landmark', 'Effective_from > effective_to' as test, count(1) as result
FROM  cortb.CHANNEL_MAP_DEV_SERVICE_KEY_LANDMARK
where effective_from > effective_to
-- check for duplicates
INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version,22,'Service key landmark', 'Duplicate SARE_no, effective_from' as test, count(1) as result
FROM (
        SELECT service_key, SARE_NO, effective_from, count(1) as no_dupes
        FROM  cortb.CHANNEL_MAP_DEV_SERVICE_KEY_LANDMARK
        GROUP BY service_key, SARE_NO, effective_from
        HAVING count(1) > 1
        ) a
--check for overlap
INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version,23,'Service key landmark', 'Overlapping effective_from with another record' as test, count(1) as result
FROM  cortb.CHANNEL_MAP_DEV_SERVICE_KEY_LANDMARK lm1
        JOIN  cortb.CHANNEL_MAP_DEV_SERVICE_KEY_LANDMARK lm2
        ON lm1.service_key = lm2.service_key and lm1.sare_no = lm2.sare_no
        AND lm1.effective_from between lm2.effective_from and lm2.effective_to
        AND lm1.effective_from <> lm2.effective_from

INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version,24,'Service key landmark', 'Overlapping effective_to with another record' as test, count(1) as result
FROM  cortb.CHANNEL_MAP_DEV_SERVICE_KEY_LANDMARK lm1
        JOIN  cortb.CHANNEL_MAP_DEV_SERVICE_KEY_LANDMARK lm2
        ON lm1.service_key = lm2.service_key and lm1.sare_no = lm2.sare_no
        AND lm1.effective_to between lm2.effective_from and lm2.effective_to
        AND lm1.effective_from <> lm2.effective_from
-- check cross reference
INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version,25,'Service key landmark', 'Service key in landmark, not in attributes' as test, count(1) as result
FROM  cortb.CHANNEL_MAP_DEV_SERVICE_KEY_LANDMARK skl
        LEFT JOIN cortb.channel_map_dev_service_key_attributes ska
        ON skl.service_key = ska.service_key
WHERE ska.service_key is null and skl.service_key is not null

INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version,26,'Service key BARB', 'Service key in BARB, not in attributes' as test, count(1) as result
FROM  cortb.CHANNEL_MAP_DEV_SERVICE_KEY_BARB skb
        LEFT JOIN cortb.channel_map_dev_service_key_attributes ska
        ON skb.service_key = ska.service_key
WHERE ska.service_key is null and skb.service_key is not null
--- BARB reported
INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version,27,'Service key attributes', 'BARB reported record in service_key_attributes not in BARB' as test, count(1) as result
FROM cortb.channel_map_dev_service_key_attributes ska
        LEFT JOIN  cortb.CHANNEL_MAP_DEV_SERVICE_KEY_BARB skb
        ON ska.service_key = skb.service_key and
                ska.effective_from between skb.effective_from and
                skb.effective_to AND ska.effective_to between skb.effective_from and skb.effective_to
WHERE (ska.BARB_reported <> 'YES' and skb.service_key is not null and skb.dummy_BARB_code <> 'YES') or (ska.BARB_reported = 'YES' and skb.service_key is null)
-- spot source barb
INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version,28,'Service key attributes', 'BARB spot_source service_key in service_key_attributes not in BARB' as test, count(1) as result
FROM cortb.channel_map_dev_service_key_attributes ska
        LEFT JOIN  cortb.CHANNEL_MAP_DEV_SERVICE_KEY_BARB skb
        ON ska.service_key = skb.service_key and ska.effective_from between skb.effective_from and skb.effective_to
        AND ska.effective_to between skb.effective_from and skb.effective_to
WHERE (ska.spot_source = 'BARB' AND skb.service_key is null)

-- promo source barb
INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version,29,'Service key attributes', 'BARB promo_source service_key in service_key_attributes not in BARB' as test, count(1) as result
FROM cortb.channel_map_dev_service_key_attributes ska
        LEFT JOIN  cortb.CHANNEL_MAP_DEV_SERVICE_KEY_BARB skb
        ON ska.service_key = skb.service_key and ((ska.effective_from between skb.effective_from and skb.effective_to)
                                                OR (ska.effective_from < skb.effective_from and ska.effective_to between skb.effective_from and skb.effective_to)
                                                OR (ska.effective_from < skb.effective_from and ska.effective_to > skb.effective_to))
WHERE (ska.promo_source = 'BARB' AND skb.service_key is null)

-- spot source landmark
INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version, 30,'Service key attributes','Landmark spot_source service_key in service_key_attributes not in Landmark' as test, count(1) as result
FROM cortb.channel_map_dev_service_key_attributes ska
        LEFT JOIN  cortb.CHANNEL_MAP_DEV_SERVICE_KEY_LANDMARK skl
        ON ska.service_key = skl.service_key and ((ska.effective_from between skl.effective_from and skl.effective_to)
                                                OR (ska.effective_from < skl.effective_from and ska.effective_to between skl.effective_from and skl.effective_to)
                                                OR (ska.effective_from < skl.effective_from and ska.effective_to > skl.effective_to))
WHERE (ska.spot_source = 'Landmark' AND skl.service_key is null)
-- valid values
INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version, 31,'Service key attributes','Primary sales house not valid' as test, count(1) as result
FROM cortb.channel_map_dev_service_key_attributes
where primary_sales_house not in ('ARY','Multicultural/Ethnic Media Sls','Sunrise TV','Disney','Eurosport sales','ITV sales','Sky sales','Media Icon','Axiom Media','Dolphin TV','Sky Kids','Sky - not BARB reported','BBC','Media 15','Channel 4 sales','Turner sales','Channel 5 sales','Exodus Media Sales','Sony Pictures Television International','Alliance Advert','Ethnic Media Sales','Sky Sales - S4C','ESI Media','Evolution Media Network','None','')

INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version, 32,'Service key attributes','Channel group not valid' as test, count(1) as result
FROM cortb.channel_map_dev_service_key_attributes
where channel_group not in
( SELECT name FROM channel_group)

INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version, 33,'Service key attributes','Sales house channel group not valid' as test, count(1) as result
FROM cortb.channel_map_dev_service_key_attributes
WHERE  (primary_sales_house = 'Channel 4 sales' and channel_group not in ('C4','UKTV','C4 Digital','Music', 'BT')) OR
        (primary_sales_house = 'Channel 5 sales' and channel_group not in ('FIVE','FIVE Digital')) OR
        (primary_sales_house = 'ITV sales' and channel_group not in ('ITV','ITV Digital')) OR
        (primary_sales_house in ('ARY','Multicultural/Ethnic Media Sls','Sunrise TV', 'Disney','Eurosport sales','Media Icon','Axiom Media','Dolphin TV','Media 15', 'Turner sales','Alliance Advert','ESI Media','Evolution Media Network') and channel_group <> 'Other') OR
        (primary_sales_house in ( 'Sky sales', 'Sky Kids')  and channel_group not in ('Kids','Ireland Kids','Ireland News','Sky-Active Ads','Documentaries','Ireland Documentaries','Entertainment','Music','Box Office','Lifestyle & Culture','Sports','3D','Ireland Sport','News','Ireland Entertainment','Movies','Other wholly-owned','Ethnic')) OR
        (primary_sales_house = 'BBC' and channel_group <> 'BBC') OR
        (primary_sales_house ='' and channel_group <> '') OR
        (primary_sales_house <> '' and channel_group = '')

INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version, 34,'Service key attributes','Old packaging not valid' as test, count(1) as result
FROM cortb.channel_map_dev_service_key_attributes
where old_packaging not in ('Childrens',
'Knowledge',
'Music',
'N/a',
'News & Events',
'Pay HD Channel with FTA Parent',
'Premium Movies',
'Premium Sports',
'FTA',
'Style & Culture',
'Variety',
'')

INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version, 35,'Service key attributes','New packaging not valid' as test, count(1) as result
FROM cortb.channel_map_dev_service_key_attributes
where new_packaging not in ('Bonus Pack',
'Entertainment',
'Entertainment Extra',
'Entertainment Extra +',
'Extra Channels',
'FTA',
'Kids',
'Premium Movies',
'Premium Sports',
'',
'Original',
'Variety',
'Family'
)

INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version, 36,'Service key attributes','Channel genre not valid' as test, count(1) as result
FROM cortb.channel_map_dev_service_key_attributes
where channel_genre not in
(SELECT name FROM channel_genre)

INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version, 37,'Service key attributes','Format not valid' as test, count(1) as result
FROM cortb.channel_map_dev_service_key_attributes
where format not in ('3D',
'HD',
'Interactive',
'SD',
'Radio',
'')

INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version, 38,'Service key attributes','Retail not valid' as test, count(1) as result
FROM cortb.channel_map_dev_service_key_attributes
where retail not in ('Commercial',
'N/a',
'Non-retail',
'Retail',
'')

INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version, 39,'Service key attributes','Channel_reach not valid' as test, count(1) as result
FROM cortb.channel_map_dev_service_key_attributes
where channel_reach not in ('All',
'ROI',
'UK',
'London',
'Bristol Area',
'Wales',
'Leeds Area',
'N/a',
'')

INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version, 40,'Service key attributes','Pay/FTA not valid' as test, count(1) as result
FROM cortb.channel_map_dev_service_key_attributes
where pay_free_indicator not in ('FTA', 'PAY', 'N/a', '')

INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version, 41,'Service key attributes','Sensitive channel named' as test, count(1) as result
FROM cortb.channel_map_dev_service_key_attributes
where sensitive_channel = 1
and (EPG_Name <> 'Other TV' or Full_Name <> 'Other TV' or VESPA_Name <> 'Other TV')

INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version, 42, 'Service key attributes','Channel owner not valid' as test, count(1) as result
FROM cortb.channel_map_dev_service_key_attributes
where channel_owner not in
(select name from channel_owner)
and channel_owner <> ''

INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version, 43, 'Service key Landmark', 'Service Key (' || service_key || ') has more than 1 SARE number' as test, count(1) as result
FROM cortb.CHANNEL_MAP_DEV_SERVICE_KEY_LANDMARK
WHERE service_key is not null
GROUP BY service_key
HAVING COUNT(sare_no) > 1

INSERT INTO CHANNEL_MAP_UPDATES
SELECT now() as audit_date, @version as version, 44, 'Service key attributes', 'Duplicate EPG Numbers on active records (101 is tolerance)' as test, count(1) as result
from    (
        select  epg_number
                ,count(1)   as hits
        from    cortb.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES
        where   activex = 'Y'
        and     service_key < 65535
        group   by  epg_number
        having  hits > 1
        ) as base

-- display results
 select *
from CHANNEL_MAP_UPDATES
WHERE VERSION = @VERSION

/*

delete from CHANNEL_MAP_UPDATES
where version = 32

*/


