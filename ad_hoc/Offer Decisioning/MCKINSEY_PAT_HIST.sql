	/**************************************************************************************************** */
	  -- MCKINSEY_PAT_HIST
	   
	 /******************************************************************************** */

SELECT  account_number
		, CAST(NULL as  varchar(50)) 	AS cb_key_household 
		, CAST (call_date AS DATE) 		AS event_dt
		,'PAT' 							AS event_type
		,cast(null as integer) as DTH
		,cast(null as integer) as BB
		,cast(null as integer) as Skytalk
		,cast(null as integer) as WLR
		,cast(null as integer) as HD
		,cast(null as integer) as MS
		,cast(null as integer) as Sports
		,cast(null as integer) as Movies
		,cast(null as integer) as SKY_Q
		,cast(null as integer) as HD_PACK
		,cast(null as integer) as SGE
		,cast(null as varchar(50)) as Bundle
		,cast(null as varchar(50)) bb_package
		,cast(null as varchar(50)) bb_package_group
		,cast(null as varchar(50)) Skytalk_package
		,cast(null as varchar(50)) skytalk_package_group

		,cast(null as date) as  DTH_act_date
		,cast(null as date) as dth_fisrt_act_date
		,cast(null as date) as dth_last_act_date
		,cast(null as integer) as tenure_days

		,cast(null as varchar(50)) as DTH_subscription_id
		,cast(null as varchar(50)) as BB_subscription_id
		,cast(null as varchar(50)) as LR_subscription_id
		,cast(null as varchar(50)) as TALK_subscription_id
		,cast(null as varchar(50)) as SGE_subscription_id
		,cast(null as varchar(50)) as HD_subscription_id
		,cast(null as varchar(50)) as MS_subscription_id
		,cast(null as varchar(50)) as HD_PACK_subscription_id
		,cast(null as varchar(50)) as standalonesurcharge_subscription_id

		,cast(null as varchar(50)) as ORDER_CREATED_BY
		,cast(null as varchar(50)) as ORDER_COMMUNICATION_TYPE
		,cast(null as varchar(50)) as ORDER_SALE_TYPE
		,cast(null as varchar(50)) as ORDER_STATUS
		,cast(null as varchar(50)) as RTM_LEVEL_1
		,cast(null as varchar(50)) as RTM_LEVEL_2
		,cast(null as varchar(50)) as RTM_LEVEL_3
		,cast(null as varchar(50)) as SERVICE_CALL_TYPE
		,cast(null as varchar(50)) as SKILL_GROUP
		,cast(null as varchar(50)) as ORDER_TYPE
		,cast(0 as integer) as  Sports_ANY_ADDED
		,cast(0 as integer) as  MOVIES_ANY_ADDED
		,cast(0 as integer) as SPORTS_ADDED
		,cast(0 as integer) as MOVIES_ADDED
		,cast(0 as integer) as SINGLE_SPORTS_ADDED
		,cast(0 as integer) as DUAL_SPORTS_ADDED
		,cast(0 as integer) as SINGLE_MOVIES_ADDED
		,cast(0 as integer) as DUAL_MOVIES_ADDED
		,cast(0 as integer) as FAMILY_ADDED
		,cast(0 as integer) as VARIETY_ADDED
		,cast(0 as integer) as ORIGINAL_ADDED
		,cast(0 as integer) as SKYQ_ADDED
		,cast(0 as integer) as HD_LEGACY_ADDED
		,cast(0 as integer) as HD_BASIC_ADDED
		,cast(0 as integer) as HD_PREMIUM_ADDED
		,cast(0 as integer) as MULTISCREEN_ADDED
		,cast(0 as integer) as MULTISCREEN_PLUS_ADDED
		,cast(0 as integer) as SKY_PLUS_ADDED
		,cast(0 as integer) as SKY_GO_EXTRA_ADDED
		,cast(0 as integer) as NOW_TV_ADDED
		,cast(0 as integer) as  BB_ANY_ADD
		,cast(0 as integer) as BB_UNLIMITED_ADDED
		,cast(0 as integer) as BB_LITE_ADDED
		,cast(0 as integer) as BB_FIBRE_CAP_ADDED
		,cast(0 as integer) as BB_FIBRE_UNLIMITED_ADDED
		,cast(0 as integer) as BB_FIBRE_UNLIMITED_PRO_ADDED
		,cast(0 as integer) as  TALK_ANY_ADD
		,cast(0 as integer) as TALKU_ADDED
		,cast(0 as integer) as TALKW_ADDED
		,cast(0 as integer) as TALKF_ADDED
		,cast(0 as integer) as TALKA_ADDED
		,cast(0 as integer) as TALKP_ADDED
		,cast(0 as integer) as TALKO_ADDED
		,cast(0 as integer) as  Sports_ANY_REMOVED
		,cast(0 as integer) as  MOVIES_ANY_REMOVED
		,cast(0 as integer) as SPORTS_REMOVED
		,cast(0 as integer) as MOVIES_REMOVED
		,cast(0 as integer) as SINGLE_SPORTS_REMOVED
		,cast(0 as integer) as DUAL_SPORTS_REMOVED
		,cast(0 as integer) as SINGLE_MOVIES_REMOVED
		,cast(0 as integer) as DUAL_MOVIES_REMOVED
		,cast(0 as integer) as FAMILY_REMOVED
		,cast(0 as integer) as VARIETY_REMOVED
		,cast(0 as integer) as ORIGINAL_REMOVED
		,cast(0 as integer) as SKYQ_REMOVED
		,cast(0 as integer) as HD_LEGACY_REMOVED
		,cast(0 as integer) as HD_BASIC_REMOVED
		,cast(0 as integer) as HD_PREMIUM_REMOVED
		,cast(0 as integer) as MULTISCREEN_REMOVED
		,cast(0 as integer) as MULTISCREEN_PLUS_REMOVED
		,cast(0 as integer) as SKY_PLUS_REMOVED
		,cast(0 as integer) as SKY_GO_EXTRA_REMOVED
		,cast(0 as integer) as NOW_TV_REMOVED
		,cast(0 as integer) as  BB_ANY_REMOVED
		,cast(0 as integer) as BB_UNLIMITED_REMOVED
		,cast(0 as integer) as BB_LITE_REMOVED
		,cast(0 as integer) as BB_FIBRE_CAP_REMOVED
		,cast(0 as integer) as BB_FIBRE_UNLIMITED_REMOVED
		,cast(0 as integer) as BB_FIBRE_UNLIMITED_PRO_REMOVED
		,cast(0 as integer) as  TALK_ANY_REMOVED
		,cast(0 as integer) as TALKU_REMOVED
		,cast(0 as integer) as TALKW_REMOVED
		,cast(0 as integer) as TALKF_REMOVED
		,cast(0 as integer) as TALKA_REMOVED
		,cast(0 as integer) as TALKP_REMOVED
		,cast(0 as integer) as TALKO_REMOVED
		,cast(0 as integer) as PRE_ORDER_TOTAL_PREMIUMS
		,cast(0 as integer) as PRE_ORDER_TOTAL_SPORTS
		,cast(0 as integer) as PRE_ORDER_TOTAL_MOVIES
		,cast(0 as integer) as PRE_ORDER_DUAL_SPORTS
		,cast(0 as integer) as PRE_ORDER_SINGLE_SPORTS
		,cast(0 as integer) as PRE_ORDER_DUAL_MOVIES
		,cast(0 as integer) as PRE_ORDER_SINGLE_MOVIES
		,cast(null as integer) as POST_ORDER_TOTAL_PREMIUMS
		,cast(null as integer) as POST_ORDER_TOTAL_SPORTS
		,cast(null as integer) as POST_ORDER_TOTAL_MOVIES
		,cast(null as integer) as POST_ORDER_DUAL_SPORTS
		,cast(null as integer) as POST_ORDER_SINGLE_SPORTS
		,cast(null as integer) as POST_ORDER_DUAL_MOVIES
		,cast(null as integer) as POST_ORDER_SINGLE_MOVIES
		,cast(0 as integer) as DTH_cancellation
INTO MCKINSEY_PAT_HIST
FROM calls_details AS a
WHERE CAST (call_date AS DATE) BETWEEN  '2014-02-15' AND '2017-03-16' 
	AND account_number IS NOT NULL 
AND (UPPER(initial_sct) LIKE '%SALRET%' OR UPPER(final_sct) = '%SALRET%' OR  UPPER(initial_sct) LIKE '%SALVAL%' OR UPPER(final_sct) = '%SALVAL%')--PAT Call
GROUP BY
		account_number
		,event_dt, event_type
		,DTH
		, BB
		, Skytalk
		, WLR
		,HD
		,MS
		,Sports
		, Movies
		, SKY_Q
		,HD_PACK
		,SGE
		, Bundle
		, bb_package
		, bb_package_group
		,Skytalk_package
		,skytalk_package_group
		,  DTH_act_date
		,dth_fisrt_act_date
		, dth_last_act_date
		, tenure_days
		,DTH_subscription_id
		, BB_subscription_id
		, LR_subscription_id
		, TALK_subscription_id
		, SGE_subscription_id
		, HD_subscription_id
		, MS_subscription_id
		, HD_PACK_subscription_id
		, standalonesurcharge_subscription_id
		, ORDER_CREATED_BY
		, ORDER_COMMUNICATION_TYPE
		, ORDER_SALE_TYPE
		, ORDER_STATUS
		, RTM_LEVEL_1
		,RTM_LEVEL_2
		, RTM_LEVEL_3
		, SERVICE_CALL_TYPE
		, SKILL_GROUP
		,  ORDER_TYPE
		,   Sports_ANY_ADDED
		,   MOVIES_ANY_ADDED
		,  SPORTS_ADDED
		,  MOVIES_ADDED
		,  SINGLE_SPORTS_ADDED
		,  DUAL_SPORTS_ADDED
		,  SINGLE_MOVIES_ADDED
		,  DUAL_MOVIES_ADDED
		,  FAMILY_ADDED
		,  VARIETY_ADDED
		,  ORIGINAL_ADDED
		,  SKYQ_ADDED
		,  HD_LEGACY_ADDED
		,  HD_BASIC_ADDED
		,  HD_PREMIUM_ADDED
		,  MULTISCREEN_ADDED
		,  MULTISCREEN_PLUS_ADDED
		,  SKY_PLUS_ADDED
		,  SKY_GO_EXTRA_ADDED
		,  NOW_TV_ADDED
		,   BB_ANY_ADD
		,  BB_UNLIMITED_ADDED
		,  BB_LITE_ADDED
		,  BB_FIBRE_CAP_ADDED
		,  BB_FIBRE_UNLIMITED_ADDED
		,  BB_FIBRE_UNLIMITED_PRO_ADDED
		,   TALK_ANY_ADD
		,  TALKU_ADDED
		,  TALKW_ADDED
		,  TALKF_ADDED
		,  TALKA_ADDED
		,  TALKP_ADDED
		,  TALKO_ADDED
		,   Sports_ANY_REMOVED
		,   MOVIES_ANY_REMOVED
		,  SPORTS_REMOVED
		,  MOVIES_REMOVED
		,  SINGLE_SPORTS_REMOVED
		,  DUAL_SPORTS_REMOVED
		,  SINGLE_MOVIES_REMOVED
		,  DUAL_MOVIES_REMOVED
		,  FAMILY_REMOVED
		,  VARIETY_REMOVED
		,  ORIGINAL_REMOVED
		,  SKYQ_REMOVED
		,  HD_LEGACY_REMOVED
		,  HD_BASIC_REMOVED
		,  HD_PREMIUM_REMOVED
		,  MULTISCREEN_REMOVED
		,  MULTISCREEN_PLUS_REMOVED
		,  SKY_PLUS_REMOVED
		,  SKY_GO_EXTRA_REMOVED
		,  NOW_TV_REMOVED
		,   BB_ANY_REMOVED
		,  BB_UNLIMITED_REMOVED
		,  BB_LITE_REMOVED
		,  BB_FIBRE_CAP_REMOVED
		,  BB_FIBRE_UNLIMITED_REMOVED
		,  BB_FIBRE_UNLIMITED_PRO_REMOVED
		,   TALK_ANY_REMOVED
		,  TALKU_REMOVED
		,  TALKW_REMOVED
		,  TALKF_REMOVED
		,  TALKA_REMOVED
		,  TALKP_REMOVED
		,  TALKO_REMOVED
		,  PRE_ORDER_TOTAL_PREMIUMS
		,  PRE_ORDER_TOTAL_SPORTS
		,  PRE_ORDER_TOTAL_MOVIES
		,  PRE_ORDER_DUAL_SPORTS
		,  PRE_ORDER_SINGLE_SPORTS
		,  PRE_ORDER_DUAL_MOVIES
		,  PRE_ORDER_SINGLE_MOVIES
		, POST_ORDER_TOTAL_PREMIUMS
		, POST_ORDER_TOTAL_SPORTS
		, POST_ORDER_TOTAL_MOVIES
		, POST_ORDER_DUAL_SPORTS
		, POST_ORDER_SINGLE_SPORTS
		, POST_ORDER_DUAL_MOVIES
		, POST_ORDER_SINGLE_MOVIES
		,  DTH_cancellation;
		-- 5096456 Row(s) affected

CREATE HG INDEX id1 ON MCKINSEY_PAT_HIST (account_number)
CREATE DATE INDEX id2 ON MCKINSEY_PAT_HIST (event_dt)
CREATE HG   INDEX id3 ON MCKINSEY_PAT_HIST (cb_key_household)
grant all on  MCKINSEY_PAT_HIST to noryd,vespa_group_low_security, rka07, citeam ;
COMMIT;
--------------------------------------------------------

update MCKINSEY_PAT_HIST
set a.cb_key_household= cast( b.cb_key_household as varchar(50))
from MCKINSEY_PAT_HIST a 
JOIN cust_single_account_view b on a.account_number = b.account_number ;

--------------------------------------------------------
GO 

update MCKINSEY_PAT_HIST
set

		a.ORDER_CREATED_BY=b.ORDER_CREATED_BY
		,a.ORDER_COMMUNICATION_TYPE=b.ORDER_COMMUNICATION_TYPE
		,a.ORDER_SALE_TYPE=b.ORDER_SALE_TYPE
		,a.ORDER_STATUS=b.ORDER_STATUS
		,a.RTM_LEVEL_1=b.RTM_LEVEL_1
		,a.RTM_LEVEL_2=b.RTM_LEVEL_2
		,a.RTM_LEVEL_3=b.RTM_LEVEL_3
		,a.SERVICE_CALL_TYPE=b.SERVICE_CALL_TYPE
		,a.SKILL_GROUP=b.SKILL_GROUP
		,a.ORDER_TYPE=b.ORDER_TYPE

		,a.SPORTS_ADDED=b.SPORTS_ADDED
		,a.MOVIES_ADDED=b.MOVIES_ADDED
		,a.SINGLE_SPORTS_ADDED=b.SINGLE_SPORTS_ADDED
		,a.DUAL_SPORTS_ADDED=b.DUAL_SPORTS_ADDED
		,a.SINGLE_MOVIES_ADDED=b.SINGLE_MOVIES_ADDED
		,a.DUAL_MOVIES_ADDED=b.DUAL_MOVIES_ADDED
		,a.FAMILY_ADDED=b.FAMILY_ADDED
		,a.VARIETY_ADDED=b.VARIETY_ADDED
		,a.ORIGINAL_ADDED=b.ORIGINAL_ADDED
		,a.SKYQ_ADDED=b.SKYQ_ADDED
		,a.HD_LEGACY_ADDED=b.HD_LEGACY_ADDED
		,a.HD_BASIC_ADDED=b.HD_BASIC_ADDED
		,a.HD_PREMIUM_ADDED=b.HD_PREMIUM_ADDED
		,a.MULTISCREEN_ADDED=b.MULTISCREEN_ADDED
		,a.MULTISCREEN_PLUS_ADDED=b.MULTISCREEN_PLUS_ADDED
		,a.SKY_PLUS_ADDED=b.SKY_PLUS_ADDED
		,a.SKY_GO_EXTRA_ADDED=b.SKY_GO_EXTRA_ADDED
		,a.NOW_TV_ADDED=b.NOW_TV_ADDED
		,a.BB_ANY_ADD=case when ( b.BB_UNLIMITED_ADDED=1 or b.BB_LITE_ADDED =1 or b.BB_FIBRE_CAP_ADDED =1 or b.BB_FIBRE_UNLIMITED_ADDED =1 or b.BB_FIBRE_UNLIMITED_PRO_ADDED  =1 ) then  1 else 0 end
		,a.BB_UNLIMITED_ADDED=b.BB_UNLIMITED_ADDED
		,a.BB_LITE_ADDED=b.BB_LITE_ADDED
		,a.BB_FIBRE_CAP_ADDED=b.BB_FIBRE_CAP_ADDED
		,a.BB_FIBRE_UNLIMITED_ADDED=b.BB_FIBRE_UNLIMITED_ADDED
		,a.BB_FIBRE_UNLIMITED_PRO_ADDED=b.BB_FIBRE_UNLIMITED_PRO_ADDED
		,a.TALK_ANY_ADD=case when (b.TALKU_ADDED=1 or b.TALKW_ADDED =1 or b.TALKF_ADDED =1 or b.TALKA_ADDED =1 or b.TALKP_ADDED =1 or b.TALKO_ADDED =1 ) then  1 else 0 end
		,a.TALKU_ADDED=b.TALKU_ADDED
		,a.TALKW_ADDED=b.TALKW_ADDED
		,a.TALKF_ADDED=b.TALKF_ADDED
		,a.TALKA_ADDED=b.TALKA_ADDED
		,a.TALKP_ADDED=b.TALKP_ADDED
		,a.TALKO_ADDED=b.TALKO_ADDED

		,a.SPORTS_REMOVED=b.SPORTS__REMOVED
		,a.MOVIES_REMOVED=b.MOVIES_REMOVED
		,a.SINGLE_SPORTS_REMOVED=b.SINGLE_SPORTS_REMOVED
		,a.DUAL_SPORTS_REMOVED=b.DUAL_SPORTS_REMOVED
		,a.SINGLE_MOVIES_REMOVED=b.SINGLE_MOVIES_REMOVED
		,a.DUAL_MOVIES_REMOVED=b.DUAL_MOVIES_REMOVED
		,a.FAMILY_REMOVED=b.FAMILY_REMOVED
		,a.VARIETY_REMOVED=b.VARIETY_REMOVED
		,a.ORIGINAL_REMOVED=b.ORIGINAL_REMOVED
		,a.SKYQ_REMOVED=b.SKYQ_REMOVED
		,a.HD_LEGACY_REMOVED=b.HD_LEGACY_REMOVED
		,a.HD_BASIC_REMOVED=b.HD_BASIC_REMOVED
		,a.HD_PREMIUM_REMOVED=b.HD_PREMIUM_REMOVED
		,a.MULTISCREEN_REMOVED=b.MULTISCREEN_REMOVED
		,a.MULTISCREEN_PLUS_REMOVED=b.MULTISCREEN_PLUS_REMOVED
		,a.SKY_PLUS_REMOVED=b.SKY_PLUS_REMOVED
		,a.SKY_GO_EXTRA_REMOVED=b.SKY_GO_EXTRA_REMOVED
		,a.NOW_TV_REMOVED=b.NOW_TV_REMOVED

		,a.BB_ANY_REMOVED= case when ( b.BB_UNLIMITED_REMOVED=1 or b.BB_LITE_REMOVED =1 or b.BB_FIBRE_CAP_REMOVED =1 or b.BB_FIBRE_UNLIMITED_REMOVED =1 or b.BB_FIBRE_UNLIMITED_PRO_REMOVED  =1 ) then  1 else 0 end
		,a.BB_UNLIMITED_REMOVED=b.BB_UNLIMITED_REMOVED
		,a.BB_LITE_REMOVED=b.BB_LITE_REMOVED
		,a.BB_FIBRE_CAP_REMOVED=b.BB_FIBRE_CAP_REMOVED
		,a.BB_FIBRE_UNLIMITED_REMOVED=b.BB_FIBRE_UNLIMITED_REMOVED
		,a.BB_FIBRE_UNLIMITED_PRO_REMOVED=b.BB_FIBRE_UNLIMITED_PRO_REMOVED
		,a.TALK_ANY_REMOVED= case when (b.TALKU_REMOVED=1 or b.TALKW_REMOVED =1 or b.TALKF_REMOVED =1 or b.TALKA_REMOVED =1 or b.TALKP_REMOVED =1 or b.TALKO_REMOVED =1 ) then  1 else 0 end
		,a.TALKU_REMOVED=b.TALKU_REMOVED
		,a.TALKW_REMOVED=b.TALKW_REMOVED
		,a.TALKF_REMOVED=b.TALKF_REMOVED
		,a.TALKA_REMOVED=b.TALKA_REMOVED
		,a.TALKP_REMOVED=b.TALKP_REMOVED
		,a.TALKO_REMOVED=b.TALKO_REMOVED
		,a.PRE_ORDER_TOTAL_PREMIUMS=b.PRE_ORDER_TOTAL_PREMIUMS
		,a.PRE_ORDER_TOTAL_SPORTS=b.PRE_ORDER_TOTAL_SPORTS
		,a.PRE_ORDER_TOTAL_MOVIES=b.PRE_ORDER_TOTAL_MOVIES
		,a.PRE_ORDER_DUAL_SPORTS=b.PRE_ORDER_DUAL_SPORTS
		,a.PRE_ORDER_SINGLE_SPORTS=b.PRE_ORDER_SINGLE_SPORTS
		,a.PRE_ORDER_DUAL_MOVIES=b.PRE_ORDER_DUAL_MOVIES
		,a.PRE_ORDER_SINGLE_MOVIES=b.PRE_ORDER_SINGLE_MOVIES
		,a.POST_ORDER_TOTAL_PREMIUMS=b.POST_ORDER_TOTAL_PREMIUMS
		,a.POST_ORDER_TOTAL_SPORTS=b.POST_ORDER_TOTAL_SPORTS
		,a.POST_ORDER_TOTAL_MOVIES=b.POST_ORDER_TOTAL_MOVIES
		,a.POST_ORDER_DUAL_SPORTS=b.POST_ORDER_DUAL_SPORTS
		,a.POST_ORDER_SINGLE_SPORTS=b.POST_ORDER_SINGLE_SPORTS
		,a.POST_ORDER_DUAL_MOVIES=b.POST_ORDER_DUAL_MOVIES
		,a.POST_ORDER_SINGLE_MOVIES=b.POST_ORDER_SINGLE_MOVIES

FROM MCKINSEY_PAT_HIST A  
inner join CITEAM.DM_ORDERS b on a.account_number = b.account_number and  a.event_dt = b.ORDER_DT;
-- 4290587 Row(s) affected 4290587 Row(s) affected


GO 
ALTER TABLE MCKINSEY_PAT_HIST add (postcode varchar(100) default null,fibre integer,Cable integer);

update MCKINSEY_PAT_HIST
set a.postcode = upper(b.cb_address_postcode)
from MCKINSEY_PAT_HIST a
JOIN cust_single_account_view b on a.account_number = b.account_number;


--###FIBRE
UPDATE MCKINSEY_PAT_HIST
   SET a.fibre = 1
  FROM MCKINSEY_PAT_HIST a
       INNER JOIN (select a.event_dt,
                          a.postcode
                          from MCKINSEY_PAT_HIST a
                          inner join BT_FIBRE_POSTCODE AS BFP
                          ON REPLACE(a.postcode,' ','') = upper(REPLACE(BFP.cb_address_postcode,' ',''))
                          AND BFP.fibre_enabled_perc >= 75
                          and BFP.first_fibre_enabled_date <= a.event_dt
                          group by a.event_dt, a.postcode) as b
                          on a.event_dt = b.event_dt
                          and a.postcode = b.postcode;
--Cable Area
UPDATE MCKINSEY_PAT_HIST
   SET BASE.Cable = CASE WHEN COALESCE(lower(bb.cable_postcode),'n') = 'y' THEN 1 ELSE 0 END
  FROM MCKINSEY_PAT_HIST AS BASE
      LEFT OUTER JOIN broadband_postcode_exchange  as bb
      ON REPLACE(ISNULL(BASE.postcode,''),' ','') = upper(replace(bb.cb_address_postcode,' ',''));
GO
--tenure

UPDATE MCKINSEY_PAT_HIST
set dth_fisrt_act_date = b.dt,
    dth_last_act_date = b.dt1
    from MCKINSEY_PAT_HIST a
    inner join (select a.account_number,
                       a.event_dt,
                       min(effective_from_dt) as dt,
                       max(case when prev_status_code in ('PO','SC') then effective_from_dt else null end) as dt1
                       from MCKINSEY_PAT_HIST a
                       inner join  cust_subs_hist b
                       on a.account_number = b.account_number
                       and b.subscription_sub_type = 'DTV Primary Viewing'
                       and b.effective_from_dt <= a.event_dt
                       and b.status_code = 'AC' and  b.status_code_changed = 'Y'
                       group by a.account_number, a.event_dt) as b
                       on a.account_number = b.account_number
                       and a.event_dt = b.event_dt;

update MCKINSEY_PAT_HIST
set DTH_act_date = case when dth_last_act_date is null then dth_fisrt_act_date else dth_last_act_date end;
update MCKINSEY_PAT_HIST
set tenure_days=  (event_dt - DTH_act_date)


--bundle

UPDATE  MCKINSEY_PAT_HIST
     SET Bundle =  CASE  WHEN UPPER(b.current_short_description) LIKE '%1M1024%'   THEN 'SKY Q'
                               WHEN UPPER(b.current_product_description) LIKE 'VARIETY%'  THEN 'Variety'
                              WHEN UPPER(b.current_product_description) LIKE 'ORIGINAL%' THEN 'Original'
                              WHEN UPPER(b.current_product_description) LIKE 'FAMILY%'   THEN 'Family'
                              WHEN UPPER(b.current_product_description) LIKE '%KID%' or
                                   UPPER(b.current_product_description) LIKE '%SKY WORLD%' or  UPPER(b.current_product_description) LIKE '%MIX%'     THEN 'Kids,Mix,World'
                              ELSE 'Other' END
    FROM  MCKINSEY_PAT_HIST as a
         INNER JOIN
(select csh.current_product_description, csh.account_number, base.event_dt ,csh.current_short_description FROM  MCKINSEY_PAT_HIST as base inner join
          cust_subs_hist as csh on csh.account_number  = base.account_number
     AND   csh.effective_from_dt <= event_dt-1
         AND csh.effective_to_dt    >= event_dt-1
     AND csh.effective_to_dt    > csh.effective_from_dt
     AND csh.subscription_sub_type = 'DTV Primary Viewing'
     AND csh.status_code in  ('AC','AB','PC')) as b on a.account_number  = b.account_number and a.event_dt=b.event_dt;

GO
--subscripionid and product holding


UPDATE MCKINSEY_PAT_HIST
   SET HD_PACK= 1
  , HD_PACK_subscription_id=subscription_id
     FROM MCKINSEY_PAT_HIST AS BASE INNER JOIN
                  ( SELECT CSH.account_number
                 ,event_dt
                 ,CSH.subscription_id
                    FROM   cust_subs_hist AS CSH
                          INNER JOIN MCKINSEY_PAT_HIST AS BASE  ON BASE.account_number = CSH.account_number
                   where     csh.subscription_sub_type = 'HD Pack'
                        and csh.status_code in ('AC','AB','PC')
                        and CSH.status_code_changed = 'Y'
                        AND   csh.effective_from_dt <= event_dt-1
                       and  csh.effective_to_dt    >= event_dt-1
                       and csh.effective_from_dt <effective_to_dt
                   GROUP BY  CSH.account_number,event_dt,CSH.subscription_id
                 )AS MR ON MR.account_number = BASE.account_number  and MR.event_dt=BASE.event_dt
                 ;



UPDATE MCKINSEY_PAT_HIST
   SET SGE= 1
  , SGE_subscription_id=subscription_id
     FROM MCKINSEY_PAT_HIST AS BASE INNER JOIN
                  ( SELECT CSH.account_number
                 ,event_dt
                 ,CSH.subscription_id
                    FROM   cust_subs_hist AS CSH
                          INNER JOIN MCKINSEY_PAT_HIST AS BASE  ON BASE.account_number = CSH.account_number
                   where     csh.subscription_sub_type = 'Sky Go Extra'
                    AND CSH.subscription_type = 'A-LA-CARTE'
                        and csh.status_code in ('AC','AB','PC')
                        and CSH.status_code_changed = 'Y'
                        AND   csh.effective_from_dt <= event_dt-1
                       and  csh.effective_to_dt    >= event_dt-1
                       and csh.effective_from_dt <effective_to_dt
                   GROUP BY  CSH.account_number,event_dt,CSH.subscription_id
                 )AS MR ON MR.account_number = BASE.account_number  and MR.event_dt=BASE.event_dt
                 ;


GO


UPDATE MCKINSEY_PAT_HIST
   SET DTH_subscription_id=subscription_id
   ,DTH=1
       FROM MCKINSEY_PAT_HIST AS BASE INNER JOIN
                  ( SELECT CSH.account_number
                 ,event_dt
                 ,CSH.subscription_id
                    FROM   cust_subs_hist AS CSH
                          INNER JOIN MCKINSEY_PAT_HIST AS BASE  ON BASE.account_number = CSH.account_number
                   where     csh.subscription_sub_type = 'DTV Extra Subscription'
                        and csh.status_code in ('AC','AB','PC')
                        and CSH.status_code_changed = 'Y'
                        AND   csh.effective_from_dt <= event_dt-1
                       and  csh.effective_to_dt    >= event_dt-1
                       and csh.effective_from_dt <effective_to_dt
                   GROUP BY  CSH.account_number,event_dt,CSH.subscription_id
                 )AS MR ON MR.account_number = BASE.account_number  and MR.event_dt=BASE.event_dt
                 ;




UPDATE MCKINSEY_PAT_HIST
   SET a.sports=b.sports
   ,a.movies=b.movies
   from MCKINSEY_PAT_HIST a inner join (
SELECT b.account_number
                 ,b.event_dt
                 ,a.sports
                 ,a.Movies
                    FROM   citeam.DM_HOLDINGS_HISTORY AS a
                          INNER JOIN MCKINSEY_PAT_HIST AS b  ON b.account_number = a.account_number
                   where a.Holding_set_effective_from_dt <=b.event_dt   and a.Holding_set_effective_to_dt>= b.event_dt and a.DTV=1
                 )AS b ON a.account_number = b.account_number  and a.event_dt=b.event_dt;

GO

UPDATE MCKINSEY_PAT_HIST
   SET BB_subscription_id=subscription_id
   ,BB=1
   ,bb_package=mr.current_product_description
     FROM MCKINSEY_PAT_HIST AS BASE INNER JOIN
                  ( SELECT CSH.account_number
                 ,event_dt
                 ,CSH.subscription_id
                 ,CSH.current_product_description
                    FROM   cust_subs_hist AS CSH
                               INNER JOIN MCKINSEY_PAT_HIST AS BASE  ON BASE.account_number = CSH.account_number
                               where     csh.subscription_sub_type = 'Broadband DSL Line'
                        and csh.status_code in ('AC','AB','PC','PT','CF','BCRQ')
                        and CSH.status_code_changed = 'Y'
                        AND   csh.effective_from_dt <= event_dt-1
                       and  csh.effective_to_dt    >= event_dt-1
                       and csh.effective_from_dt <effective_to_dt
                   GROUP BY  CSH.account_number,event_dt,CSH.subscription_id,CSH.current_product_description
                 )AS MR ON MR.account_number = BASE.account_number  and MR.event_dt=BASE.event_dt
                 ;

GO 

UPDATE MCKINSEY_PAT_HIST
   SET   bb_package_group = CASE WHEN  BB_Package IN ('Sky Broadband Everyday','Sky Broadband Lite','Sky Broadband Lite (ROI)','WiFi Hotspots from The Cloud') THEN 'BBEL'
                                        WHEN  BB_Package IN ('Sky Anytime+','Sky Broadband Unlimited','Sky Broadband Unlimited (ROI)') THEN 'BBUL'
                                        WHEN  BB_Package IN ('Sky Broadband Unlimited Fibre','Sky Fibre Unlimited Pro') THEN  'Fibre'
                                        WHEN  BB_Package IN ('Broadband Connect','Sky Connect Lite (ROI)','Sky Connect Unlimited (ROI)') THEN 'Connect'
                                        ELSE  BB_Package END;

UPDATE MCKINSEY_PAT_HIST
   SET LR_subscription_id=subscription_id
   ,WLR=1
     FROM MCKINSEY_PAT_HIST AS BASE INNER JOIN
                  ( SELECT CSH.account_number
                 ,event_dt
                 ,CSH.subscription_id
                    FROM   cust_subs_hist AS CSH
                          INNER JOIN MCKINSEY_PAT_HIST AS BASE  ON BASE.account_number = CSH.account_number
                   where     csh.subscription_sub_type = 'SKY TALK LINE RENTAL'
                        and csh.status_code in ('A','CRQ','R','BCRQ')
                        and CSH.status_code_changed = 'Y'
                        AND   csh.effective_from_dt <= event_dt-1
                       and  csh.effective_to_dt    >= event_dt-1
                       and csh.effective_from_dt <effective_to_dt
                   GROUP BY  CSH.account_number,event_dt,CSH.subscription_id
                 )AS MR ON MR.account_number = BASE.account_number  and MR.event_dt=BASE.event_dt
                 ;

UPDATE MCKINSEY_PAT_HIST
   SET TALK_subscription_id=subscription_id
   ,Skytalk=1
  ,Skytalk_package=mr.current_product_description
     FROM MCKINSEY_PAT_HIST AS BASE INNER JOIN
                  ( SELECT CSH.account_number
                 ,event_dt
                 ,CSH.subscription_id
                 ,CSH.current_product_description
                    FROM   cust_subs_hist AS CSH
                               INNER JOIN MCKINSEY_PAT_HIST AS BASE  ON BASE.account_number = CSH.account_number
                                             where     csh.subscription_sub_type = 'SKY TALK SELECT'
                        and csh.status_code in ('A','PC','FBP','RI','FBI','BCRQ')
                        and CSH.status_code_changed = 'Y'
                        AND   csh.effective_from_dt <= event_dt-1
                       and  csh.effective_to_dt    >= event_dt-1
                       and csh.effective_from_dt <effective_to_dt
                   GROUP BY  CSH.account_number,event_dt,CSH.subscription_id,CSH.current_product_description
                 )AS MR ON MR.account_number = BASE.account_number  and MR.event_dt=BASE.event_dt
                 ;
GO 

UPDATE MCKINSEY_PAT_HIST
   SET skytalk_package_group= CASE WHEN  SkyTalk_Package  IN ('Anytime','Anytime (No Monthly Fee)','Anytime (Offer)','Anytime with Mobil','Sky Talk Anytime','Sky Talk Anytime (ROI)','Sky Talk Anytime Guiding and Rating Product','Sky Talk Anytime UK','Sky Talk Super Unlimited','Sky Talk Unlimited','Sky Talk Unlimited 12 Month Offer','Sky Talk Unlimited 3 Month Offer','Sky Talk Unlimited 6 Month Offer','Sky Talk Unlimited Staff Tariff') THEN 'Anytime'
                                             WHEN SkyTalk_Package   IN ('Sky Talk Freetime','Sky Talk Freetime (ROI)','Sky Talk Weekends','Weekends with Mobile') THEN 'Freetime/Weekends'
                                          ELSE 'Other' END;

UPDATE MCKINSEY_PAT_HIST
   SET HD_subscription_id=subscription_id
   ,HD=1
     FROM MCKINSEY_PAT_HIST AS BASE INNER JOIN
                  ( SELECT CSH.account_number
                 ,event_dt
                 ,CSH.subscription_id
                    FROM   cust_subs_hist AS CSH
                          INNER JOIN MCKINSEY_PAT_HIST AS BASE  ON BASE.account_number = CSH.account_number
                   where     csh.subscription_sub_type = 'DTV HD'
                        and csh.status_code in ('AC','AB','PC')
                        and CSH.status_code_changed = 'Y'
                        AND   csh.effective_from_dt <= event_dt-1
                       and  csh.effective_to_dt    >= event_dt-1
                       and csh.effective_from_dt <effective_to_dt
                   GROUP BY  CSH.account_number,event_dt,CSH.subscription_id
                 )AS MR ON MR.account_number = BASE.account_number  and MR.event_dt=BASE.event_dt
                 ;


UPDATE MCKINSEY_PAT_HIST
   SET MS_subscription_id=subscription_id
   ,MS=1
     FROM MCKINSEY_PAT_HIST AS BASE INNER JOIN
                  ( SELECT CSH.account_number
                 ,event_dt
                 ,CSH.subscription_id
                    FROM   cust_subs_hist AS CSH
                          INNER JOIN MCKINSEY_PAT_HIST AS BASE  ON BASE.account_number = CSH.account_number
                   where     csh.subscription_sub_type = 'DTV Extra Subscription'
                        and csh.status_code in ('AC','AB','PC')
                        and CSH.status_code_changed = 'Y'
                        AND   csh.effective_from_dt <= event_dt-1
                       and  csh.effective_to_dt    >= event_dt-1
                       and csh.effective_from_dt <effective_to_dt
                   GROUP BY  CSH.account_number,event_dt,CSH.subscription_id
                 )AS MR ON MR.account_number = BASE.account_number  and MR.event_dt=BASE.event_dt
                 ;

GO
UPDATE MCKINSEY_PAT_HIST
   SET standalonesurcharge_subscription_id=subscription_id
     FROM MCKINSEY_PAT_HIST AS BASE INNER JOIN
                  ( SELECT CSH.account_number
                 ,event_dt
                 ,CSH.subscription_id
                    FROM   cust_subs_hist AS CSH
                          INNER JOIN MCKINSEY_PAT_HIST AS BASE  ON BASE.account_number = CSH.account_number
                   where     csh.subscription_sub_type = 'STANDALONESURCHARGE'
                        and csh.status_code in ('AC','AB','PC')
                        and CSH.status_code_changed = 'Y'
                        AND   csh.effective_from_dt <= event_dt-1
                       and  csh.effective_to_dt    >= event_dt-1
                       and csh.effective_from_dt <effective_to_dt
                   GROUP BY  CSH.account_number,event_dt,CSH.subscription_id
                 )AS MR ON MR.account_number = BASE.account_number  and MR.event_dt=BASE.event_dt
                 ;

GO

UPDATE MCKINSEY_PAT_HIST
   SET DTH_cancellation=1
     FROM MCKINSEY_PAT_HIST AS BASE INNER JOIN
                  ( SELECT CSH.account_number
                 ,event_dt
                 ,CSH.subscription_id
                    FROM   cust_subs_hist AS CSH
                          INNER JOIN MCKINSEY_PAT_HIST AS BASE  ON BASE.account_number = CSH.account_number
                   where     csh.subscription_sub_type = 'DTV Extra Subscription'
                        and csh.status_code in ('PC')
                        and csh.prev_status_code in ('AC','AB')
                        and CSH.status_code_changed = 'Y'
                        AND   csh.effective_from_dt = event_dt
                       and  csh.effective_to_dt    >  event_dt

                   GROUP BY  CSH.account_number,event_dt,CSH.subscription_id
                 )AS MR ON MR.account_number = BASE.account_number  and MR.event_dt=BASE.event_dt
                 ;
GO


-----------------------------------------------------------------------------------------------------
--product lapse tenure
alter table MCKINSEY_PAT_HIST

add
(sports_UPgrade_date           date
,movies_UPgrade_date           date

,movies_latest_act_date                 date
,sports_latest_act_date                 date
,sports_downgrade_date           date
,movies_downgrade_date           date
,MS_first_act_date               date
,MS_latest_act_date              date
,MS_churn_dt                     date
,HD_PACK_first_act_date          date
,HD_PACK_churn_dt                date
,HD_PACK_latest_act_date         date

,SKYGOE_first_act_date           date
,SKYGOE_latest_act_date          date

,SKYGOE_churn_dt                 date
,HD_legacy_first_act_date        date
,HD_legacy_latest_act_date       date

,HD_legacy_churn_dt              date
,HD_base_first_act_date          date
,HD_base_latest_act_date         date
,HD_base_churn_dt                date
,BB_latest_act_date       date
,BB_first_act_date               date
,BB_churn_dt              date
,HD_act_date                     date
,HD_churn_date                   date
,HD_Prems_latest_act_date        date
,HD_prems_churn_dt               date

,LR_first_act_date               date
,LR_latest_act_date              date
,LR_churn_dt                     date
,talk_first_act_date               date
,talk_latest_act_date              date
,talk_churn_dt                     date



);


GO

update MCKINSEY_PAT_HIST
set sports_downgrade_date = b.dt1,
    movies_downgrade_date = b.dt2
    from MCKINSEY_PAT_HIST a
    inner join (select a.account_number,
                        a.event_dt,
                       max(case when b.typeofevent = 'SD' then a.event_dt else null end) as dt1,
                       max(case when b.typeofevent = 'MD' then a.event_dt else null end) as dt2
                  from MCKINSEY_PAT_HIST a
                  inner join citeam.view_cust_package_movements_hist b
                  on a.account_number = b.account_number
                  and b.event_dt between a.dth_act_date and a.event_dt
                  and b.typeofevent in ('MD','SD')
                  group by a.account_number,a.event_dt) as b
                  on a.account_number = b.account_number
                  and a.event_dt= b.event_dt;



update MCKINSEY_PAT_HIST
set sports_UPgrade_date = b.dt1,
    movies_UPgrade_date = b.dt2
    from MCKINSEY_PAT_HIST a
    inner join (select a.account_number,
                        a.event_dt,
                       max(case when b.typeofevent = 'SU' then a.event_dt else null end) as dt1,
                       max(case when b.typeofevent = 'MU' then a.event_dt else null end) as dt2
                  from MCKINSEY_PAT_HIST a
                  inner join citeam.view_cust_package_movements_hist b
                  on a.account_number = b.account_number
                  and b.event_dt between a.dth_act_date and a.event_dt
                  and b.typeofevent in ('MU','SU')
                  group by a.account_number,a.event_dt) as b
                  on a.account_number = b.account_number
                  and a.event_dt= b.event_dt;




update MCKINSEY_PAT_HIST
set sports_latest_act_date             =case when sports=1 and sports_UPgrade_date is not null then sports_UPgrade_date else dth_act_date end;

update MCKINSEY_PAT_HIST
set movies_latest_act_date             =case when movies=1 and movies_UPgrade_date is not null then movies_UPgrade_date else dth_act_date end;



UPDATE MCKINSEY_PAT_HIST
   SET BASE.MS_first_act_date   = MR.MS_first_act_date
       ,BASE.MS_latest_act_date = MR.MS_latest_act_date
  FROM MCKINSEY_PAT_HIST AS BASE INNER JOIN
                   ( SELECT  CSH.account_number
                    , BASE.event_dt
                      ,MAX(CASE WHEN CSH.prev_status_code IN ('PO','SC') THEN CSH.effective_from_dt ELSE NULL END) AS MS_latest_act_date -- the most recent activation date post a reinstate
                      ,MIN(CSH.effective_from_dt) AS MS_first_act_date      -- first ever activation date
                    FROM   cust_subs_hist AS CSH
                          INNER JOIN MCKINSEY_PAT_HIST AS BASE  ON BASE.account_number = CSH.account_number
                   where csh.subscription_sub_type = 'DTV Extra Subscription'
                        and csh.status_code = 'AC'
                        and CSH.status_code_changed = 'Y'
                        AND CSH.effective_from_dT<  CSH.effective_to_dt
                          AND CSH.effective_from_dt between BASE.dth_act_date and BASE.event_dt
                   GROUP BY  CSH.account_number,BASE.event_dt
                 )AS MR
                 ON MR.account_number = BASE.account_number
                 and MR.event_dt = BASE.event_dt;

GO


UPDATE MCKINSEY_PAT_HIST
   SET MS_latest_act_date =  CASE WHEN MS_latest_act_date IS NULL THEN MS_first_act_date ELSE MS_latest_act_date END;


   UPDATE MCKINSEY_PAT_HIST
   SET MS_churn_dt=  MS_churn_act_date
  FROM MCKINSEY_PAT_HIST AS BASE INNER JOIN
                 ( SELECT CSH.account_number
                 ,BASE.event_dt
                        , MAX(CSH.effective_from_dt) AS MS_churn_act_date      -- first ever activation date
                    FROM   cust_subs_hist AS CSH
                          INNER JOIN MCKINSEY_PAT_HIST AS BASE  ON BASE.account_number = CSH.account_number
                   where   csh.subscription_sub_type = 'DTV Extra Subscription'
                          and csh.status_code IN ('PO','SC')
                        and csh.prev_status_code in ('AC','AB','PC')
                        and CSH.status_code_changed = 'Y'
                                                AND CSH.effective_from_dt between MS_latest_act_date and event_dt
                    GROUP BY  CSH.account_number,BASE.event_dt
                 )AS MR
                 ON MR.account_number = BASE.account_number
                 and MR.event_dt = BASE.event_dt ;



-------------------------------------------C04  - SGE Start Date
UPDATE MCKINSEY_PAT_HIST
   SET BASE.SKYGOE_first_act_date   = MR.SKYGOE_first_act_date
       ,BASE.SKYGOE_latest_act_date = MR.SKYGOE_latest_act_date
  FROM MCKINSEY_PAT_HIST AS BASE INNER JOIN
                  ( SELECT CSH.account_number
                  ,BASE.event_dt
                         ,MAX(CASE WHEN CSH.prev_status_code IN ('PO','SC') THEN CSH.effective_from_dt ELSE NULL END) AS SKYGOE_latest_act_date -- the most recent activation date post a reinstate
                         ,MIN(CSH.effective_from_dt) AS SKYGOE_first_act_date      -- first ever activation date
                    FROM   cust_subs_hist AS CSH
                          INNER JOIN MCKINSEY_PAT_HIST AS BASE  ON BASE.account_number = CSH.account_number
                   where csh.subscription_sub_type = 'Sky Go Extra'
                    AND CSH.subscription_type = 'A-LA-CARTE'
                        and csh.status_code = 'AC'
                        AND CSH.effective_from_dT<  CSH.effective_to_dt
                        and CSH.status_code_changed = 'Y'
                                                AND CSH.effective_from_dt between dth_act_date and event_dt
                    GROUP BY  CSH.account_number,BASE.event_dt
                 )AS MR
                 ON MR.account_number = BASE.account_number
                 and MR.event_dt = BASE.event_dt ;

UPDATE MCKINSEY_PAT_HIST
   SET SKYGOE_latest_act_date = CASE WHEN SKYGOE_latest_act_date IS NULL THEN SKYGOE_first_act_date ELSE SKYGOE_latest_act_date END;

-------------------------------------------C05  - SGE Churn Date
UPDATE MCKINSEY_PAT_HIST
   SET SKYGOE_churn_dt= SKYGOE_churn_act_date
  FROM MCKINSEY_PAT_HIST AS BASE INNER JOIN
                 ( SELECT CSH.account_number
                 ,BASE.event_dt
                          ,MAX(CSH.effective_from_dt) AS SKYGOE_churn_act_date      -- first ever activation date
                    FROM   cust_subs_hist AS CSH
                          INNER JOIN MCKINSEY_PAT_HIST AS BASE  ON BASE.account_number = CSH.account_number
                   where csh.subscription_sub_type = 'Sky Go Extra'
                    AND CSH.subscription_type = 'A-LA-CARTE'
                        and csh.status_code IN ('PO','SC')
                        and csh.prev_status_code in ('AC','AB','PC')
                        and CSH.status_code_changed = 'Y'
                                                AND CSH.effective_from_dt between SKYGOE_latest_act_date and event_dt
                    GROUP BY  CSH.account_number,BASE.event_dt
                 )AS MR
                 ON MR.account_number = BASE.account_number
                 and MR.event_dt = BASE.event_dt ;


-------------------------------------------C06  - HD Legacy Start Date
UPDATE MCKINSEY_PAT_HIST
   SET BASE.HD_legacy_first_act_date   = MR.HD_legacy_first_act_date
       ,BASE.HD_legacy_latest_act_date = MR.HD_legacy_latest_act_date
  FROM MCKINSEY_PAT_HIST AS BASE INNER JOIN
                  ( SELECT CSH.account_number
                  ,BASE.event_dt
                         ,MAX(CASE WHEN CSH.prev_status_code IN ('PO','SC') THEN CSH.effective_from_dt ELSE NULL END) AS HD_legacy_latest_act_date -- the most recent activation date post a reinstate
                         ,MIN(CSH.effective_from_dt) AS HD_legacy_first_act_date      -- first ever activation date
                    FROM   cust_subs_hist AS CSH
                          INNER JOIN MCKINSEY_PAT_HIST AS BASE  ON BASE.account_number = CSH.account_number
                   where    csh.subscription_sub_type = 'DTV HD'
                            and csh.current_product_sk = 687
                        and csh.status_code = 'AC'
                        AND CSH.effective_from_dT<  CSH.effective_to_dt
                        and CSH.status_code_changed = 'Y'
                                                AND CSH.effective_from_dt between dth_act_date and event_dt
                GROUP BY  CSH.account_number,BASE.event_dt
                 )AS MR
                 ON MR.account_number = BASE.account_number
                 and MR.event_dt = BASE.event_dt ;

GO

UPDATE MCKINSEY_PAT_HIST
   SET HD_legacy_latest_act_date = CASE WHEN HD_legacy_latest_act_date IS NULL THEN HD_legacy_first_act_date ELSE HD_legacy_latest_act_date END;

-------------------------------------------C07  - HD Legacy Churn Date
UPDATE MCKINSEY_PAT_HIST
   SET HD_legacy_churn_dt= HD_legacy_churn_act_date
  FROM MCKINSEY_PAT_HIST AS BASE INNER JOIN
                 ( SELECT CSH.account_number
                 ,BASE.event_dt
                     ,    MAX(CSH.effective_from_dt) AS HD_legacy_churn_act_date      -- first ever activation date
                    FROM   cust_subs_hist AS CSH
                          INNER JOIN MCKINSEY_PAT_HIST AS BASE  ON BASE.account_number = CSH.account_number
                   where csh.subscription_sub_type = 'DTV HD'
                            and csh.current_product_sk = 687
                        and csh.status_code IN ('PO','SC')
                        and csh.prev_status_code in ('AC','AB','PC')
                        and CSH.status_code_changed = 'Y'
                                                AND CSH.effective_from_dt between HD_legacy_latest_act_date and event_dt
                  GROUP BY  CSH.account_number,BASE.event_dt
                 )AS MR
                 ON MR.account_number = BASE.account_number
                 and MR.event_dt = BASE.event_dt ;

------------------------------------------C08  - HD Basic Start Date
UPDATE MCKINSEY_PAT_HIST
   SET BASE.HD_base_first_act_date   = MR.HD_base_first_act_date
       ,BASE.HD_base_latest_act_date = MR.HD_base_latest_act_date
  FROM MCKINSEY_PAT_HIST AS BASE INNER JOIN
                  ( SELECT CSH.account_number
                  ,BASE.event_dt
                         ,MAX(CASE WHEN CSH.prev_status_code IN ('PO','SC') THEN CSH.effective_from_dt ELSE NULL END) AS HD_base_latest_act_date -- the most recent activation date post a reinstate
                         ,MIN(CSH.effective_from_dt) AS HD_base_first_act_date      -- first ever activation date
                    FROM   cust_subs_hist AS CSH
                          INNER JOIN MCKINSEY_PAT_HIST AS BASE  ON BASE.account_number = CSH.account_number
                   where    csh.subscription_sub_type = 'DTV HD'
                            and csh.current_product_sk = 43678
                        and csh.status_code = 'AC'
                        AND CSH.effective_from_dT<  CSH.effective_to_dt
                        and CSH.status_code_changed = 'Y'
                                                AND CSH.effective_from_dt between dth_act_date and event_dt
                  GROUP BY  CSH.account_number,BASE.event_dt
                 )AS MR
                 ON MR.account_number = BASE.account_number
                 and MR.event_dt = BASE.event_dt ;



UPDATE MCKINSEY_PAT_HIST
   SET HD_base_latest_act_date =   CASE WHEN HD_base_latest_act_date IS NULL THEN HD_base_first_act_date ELSE HD_base_latest_act_date END;

GO

------------------------------------------C09  - HD Basic Churn Date
UPDATE MCKINSEY_PAT_HIST
   SET HD_base_churn_dt=  HD_base_churn_act_date
  FROM MCKINSEY_PAT_HIST AS BASE INNER JOIN
                 ( SELECT CSH.account_number
                 ,BASE.event_dt
                    ,     MAX(CSH.effective_from_dt) AS HD_base_churn_act_date      -- first ever activation date
                    FROM   cust_subs_hist AS CSH
                          INNER JOIN MCKINSEY_PAT_HIST AS BASE  ON BASE.account_number = CSH.account_number
                   where csh.subscription_sub_type = 'DTV HD'
                            and csh.current_product_sk = 43678
                        and csh.status_code IN ('PO','SC')
                        and csh.prev_status_code in ('AC','AB','PC')
                        and CSH.status_code_changed = 'Y'
                                                AND CSH.effective_from_dt between HD_base_latest_act_date and event_dt
                  GROUP BY  CSH.account_number,BASE.event_dt
                 )AS MR
                 ON MR.account_number = BASE.account_number
                 and MR.event_dt = BASE.event_dt ;



------------------------------------------C10  - HD Pack Start Date
UPDATE MCKINSEY_PAT_HIST
   SET BASE.HD_PACK_first_act_date   = MR.HD_prems_first_act_date
       ,BASE.HD_PACK_latest_act_date = MR.HD_prems_latest_act_date
  FROM MCKINSEY_PAT_HIST AS BASE INNER JOIN
                  ( SELECT CSH.account_number
                  ,BASE.event_dt
                         ,MAX(CASE WHEN CSH.prev_status_code IN ('PO','SC') THEN CSH.effective_from_dt ELSE NULL END) AS HD_prems_latest_act_date -- the most recent activation date post a reinstate
                         ,MIN(CSH.effective_from_dt) AS HD_prems_first_act_date      -- first ever activation date
                    FROM   cust_subs_hist AS CSH
                          INNER JOIN MCKINSEY_PAT_HIST AS BASE  ON BASE.account_number = CSH.account_number
                   where     csh.subscription_sub_type = 'HD Pack'
                        and csh.status_code = 'AC'
                        AND CSH.effective_from_dT<  CSH.effective_to_dt
                        and CSH.status_code_changed = 'Y'
                                                AND CSH.effective_from_dt between dth_act_date and event_dt
                   GROUP BY  CSH.account_number,BASE.event_dt
                 )AS MR
                 ON MR.account_number = BASE.account_number
                 and MR.event_dt = BASE.event_dt ;


UPDATE MCKINSEY_PAT_HIST
   SET HD_PACK_latest_act_date = CASE WHEN HD_PACK_latest_act_date IS NULL THEN HD_PACK_first_act_date ELSE HD_PACK_latest_act_date END;

   GO
   
------------------------------------------C11  - HD Pack Churn Date
UPDATE MCKINSEY_PAT_HIST
   SET HD_PACK_churn_dt=  dt
  FROM MCKINSEY_PAT_HIST AS BASE INNER JOIN
                 ( SELECT CSH.account_number
                 ,BASE.event_dt
                        , MAX(CSH.effective_from_dt) AS dt      -- first ever activation date
                    FROM   cust_subs_hist AS CSH
                          INNER JOIN MCKINSEY_PAT_HIST AS BASE  ON BASE.account_number = CSH.account_number
                   where  csh.subscription_sub_type = 'HD Pack'
                        and csh.status_code IN ('PO','SC')
                        and csh.prev_status_code in ('AC','AB','PC')
                        and CSH.status_code_changed = 'Y'
                                                AND CSH.effective_from_dt between dth_act_date and event_dt
                   GROUP BY  CSH.account_number,BASE.event_dt
                 )AS MR
                 ON MR.account_number = BASE.account_number
                 and MR.event_dt = BASE.event_dt ;

GO

------------------------------------------C12  - HD Date Corrects
UPDATE MCKINSEY_PAT_HIST
   SET HD_act_date =  CASE WHEN HD_base_latest_act_date is null and HD_legacy_latest_act_date is not null then HD_legacy_latest_act_date
                         WHEN   HD_base_latest_act_date is not null  and HD_legacy_latest_act_date is null    then HD_base_latest_act_date
                         WHEN   HD_base_latest_act_date is not null and HD_legacy_latest_act_date is not null and HD_base_churn_dt<HD_base_latest_act_date then HD_base_latest_act_date
                           WHEN   HD_base_latest_act_date is not null  and HD_legacy_latest_act_date  is not null and HD_base_churn_dt=HD_base_latest_act_date  then HD_legacy_latest_act_date
                         ELSE NULL END;



UPDATE MCKINSEY_PAT_HIST
   SET HD_churn_date=  CASE WHEN HD_base_churn_dt is null  and HD_legacy_churn_dt is not null then HD_legacy_churn_dt
                                WHEN HD_base_churn_dt is not null  and HD_legacy_churn_dt is  null then HD_base_churn_dt
                                WHEN HD_base_churn_dt >= HD_legacy_churn_dt then HD_base_churn_dt
                                  WHEN HD_base_churn_dt < HD_legacy_churn_dt then HD_legacy_churn_dt
                                 ELSE NULL END;

UPDATE MCKINSEY_PAT_HIST
   SET HD_Prems_latest_act_date = CASE WHEN HD_legacy_latest_act_date >= HD_pack_latest_act_date THEN HD_legacy_latest_act_date
                                 WHEN HD_legacy_latest_act_date < HD_pack_latest_act_date  THEN HD_pack_latest_act_date
                                 WHEN HD_legacy_latest_act_date IS NOT NULL THEN  HD_legacy_latest_act_date
                                 WHEN HD_pack_latest_act_date IS NOT NULL THEN  HD_pack_latest_act_date
                                 ELSE null END;


UPDATE MCKINSEY_PAT_HIST
   SET HD_prems_churn_dt=  CASE WHEN HD_Pack_latest_act_date = HD_pack_latest_act_date then HD_pack_churn_dt
                                  WHEN HD_Pack_latest_act_date =HD_legacy_latest_act_date then HD_legacy_churn_dt
                                  ELSE NULL END;


GO

------------------------------------------C14  - BB Start Date

UPDATE MCKINSEY_PAT_HIST
SET BASE.BB_first_act_date   = MR.BB_first_act_date
       ,BASE.bb_latest_act_date = MR.BB_latest_act_date
FROM MCKINSEY_PAT_HIST AS BASE
INNER JOIN(SELECT CSH.account_number
,BASE.event_dt
          ,MAX(CASE WHEN CSH.prev_status_code IN ('PO','SC') THEN CSH.effective_from_dt ELSE NULL END) AS BB_latest_act_date -- the most recent activation date post a reinstate
          ,MIN(CSH.effective_from_dt) AS BB_first_act_date      -- first ever activation date
           FROM   cust_subs_hist AS CSH
           INNER JOIN MCKINSEY_PAT_HIST  AS BASE
           ON BASE.account_number = CSH.account_number
            where csh.subscription_sub_type = 'Broadband DSL Line'
                        AND (csh.status_code in ('AC','AB')
                             OR (csh.status_code='PC' AND prev_status_code not in ('?','RQ','AP','UB','BE','PA') )
                             OR (csh.status_code='CF' AND prev_status_code='PC'                                  )
                             OR (csh.status_code='AP' AND sale_type='SNS Bulk Migration'                         )
                            )
                        and CSH.status_code_changed = 'Y'
                        AND CSH.effective_from_dt <= event_dt
                       AND CSH.effective_from_dT<  CSH.effective_to_dt
                    GROUP BY  CSH.account_number,BASE.event_dt
                 )AS MR
                 ON MR.account_number = BASE.account_number
                 and MR.event_dt = BASE.event_dt ;

UPDATE MCKINSEY_PAT_HIST SET bb_latest_act_date =
CASE WHEN bb_latest_act_date IS NULL THEN BB_first_act_date ELSE bb_latest_act_date END;

------------------------------------------C15  - BB Churn Date

UPDATE MCKINSEY_PAT_HIST SET bb_churn_dt= broadband_churn_act_date
FROM MCKINSEY_PAT_HIST AS BASE
INNER JOIN(SELECT CSH.account_number
,d.event_dt
                   ,MAX(CSH.effective_from_dt) AS broadband_churn_act_date      -- first ever activation date
                        from cust_subs_hist AS csh
                        left join MCKINSEY_PAT_HIST d
                        on csh.account_number=d.account_number
                        where subscription_sub_type = 'Broadband DSL Line'
                        and status_code_changed = 'Y'
                        and prev_status_code not in ('PO','SC','CN')
                        and status_code in  ('PO','SC','CN')
                        AND Status_reason <> 'Moving Home'
                        AND CSH.effective_from_dt between bb_latest_act_date and event_dt
                         group by CSH.account_number,d.event_dt
                    )AS MR
                 ON MR.account_number = BASE.account_number
                 and MR.event_dt = BASE.event_dt ;





------------------------------------------C14  - BB Start Date

UPDATE MCKINSEY_PAT_HIST
SET BASE.LR_first_act_date   = MR.BB_first_act_date
       ,BASE.LR_latest_act_date = MR.BB_latest_act_date
FROM MCKINSEY_PAT_HIST AS BASE
INNER JOIN(SELECT CSH.account_number
                ,BASE.event_dt
          ,MAX(CASE WHEN CSH.prev_status_code IN ('CN') THEN CSH.effective_from_dt ELSE NULL END) AS BB_latest_act_date -- the most recent activation date post a reinstate
          ,MIN(CSH.effective_from_dt) AS BB_first_act_date      -- first ever activation date
           FROM   cust_subs_hist AS CSH
           INNER JOIN MCKINSEY_PAT_HIST  AS BASE
           ON BASE.account_number = CSH.account_number
            where csh.subscription_sub_type = 'SKY TALK LINE RENTAL'
                        AND csh.status_code in ('A')
                        and CSH.status_code_changed = 'Y'
                        AND CSH.effective_from_dt <= event_dt
                        AND CSH.effective_from_dT<  CSH.effective_to_dt
                    GROUP BY  CSH.account_number,BASE.event_dt
                 )AS MR
                 ON MR.account_number = BASE.account_number
                 and MR.event_dt = BASE.event_dt ;

UPDATE MCKINSEY_PAT_HIST SET LR_latest_act_date =
CASE WHEN LR_latest_act_date IS NULL THEN LR_first_act_date ELSE LR_latest_act_date END;

------------------------------------------C15  - BB Churn Date

UPDATE MCKINSEY_PAT_HIST SET LR_churn_dt= broadband_churn_act_date
FROM MCKINSEY_PAT_HIST AS BASE
INNER JOIN(SELECT CSH.account_number
,d.event_dt
                   ,MAX(CSH.effective_from_dt) AS broadband_churn_act_date      -- first ever activation date
                        from cust_subs_hist AS csh
                        left join MCKINSEY_PAT_HIST d
                        on csh.account_number=d.account_number
                        where subscription_sub_type = 'SKY TALK LINE RENTAL'
                        and status_code_changed = 'Y'
                        and prev_status_code not in ('CN')
                        and status_code in  ('CN')
                        AND Status_reason <> 'Moving Home'
                        AND CSH.effective_from_dt between LR_latest_act_date and event_dt
                        group by CSH.account_number,d.event_dt
                    )AS MR
                 ON MR.account_number = BASE.account_number
                 and MR.event_dt = BASE.event_dt ;
GO



------------------------------------------C14  - BB Start Date

UPDATE MCKINSEY_PAT_HIST
SET BASE.talk_first_act_date   = MR.BB_first_act_date
       ,BASE.talk_latest_act_date = MR.BB_latest_act_date
FROM MCKINSEY_PAT_HIST AS BASE
INNER JOIN(SELECT CSH.account_number
,BASE.event_dt
          ,MAX(CASE WHEN CSH.prev_status_code IN ('CN') THEN CSH.effective_from_dt ELSE NULL END) AS BB_latest_act_date -- the most recent activation date post a reinstate
          ,MIN(CSH.effective_from_dt) AS BB_first_act_date      -- first ever activation date
           FROM   cust_subs_hist AS CSH
           INNER JOIN MCKINSEY_PAT_HIST  AS BASE
           ON BASE.account_number = CSH.account_number
            where csh.subscription_sub_type = 'SKY TALK SELECT'
                        AND csh.status_code in ('A')

                        and CSH.status_code_changed = 'Y'
                        AND CSH.effective_from_dt <= event_dt
                         AND CSH.effective_from_dT<  CSH.effective_to_dt
                    GROUP BY  CSH.account_number,BASE.event_dt
                 )AS MR
                 ON MR.account_number = BASE.account_number
                 and MR.event_dt = BASE.event_dt ;

UPDATE MCKINSEY_PAT_HIST SET talk_latest_act_date =
CASE WHEN talk_latest_act_date IS NULL THEN Talk_first_act_date ELSE talk_latest_act_date END;

------------------------------------------C15  - BB Churn Date

UPDATE MCKINSEY_PAT_HIST SET talk_churn_dt= churn_act_date
FROM MCKINSEY_PAT_HIST AS BASE
INNER JOIN(SELECT CSH.account_number
,d.event_dt
                   ,MAX(CSH.effective_from_dt) AS churn_act_date      -- first ever activation date
                        from cust_subs_hist AS csh
                        left join MCKINSEY_PAT_HIST d
                        on csh.account_number=d.account_number
                        where subscription_sub_type = 'SKY TALK SELECT'
                        and status_code_changed = 'Y'
                        and prev_status_code not in ('CN')
                        and status_code in  ('CN')
                        AND Status_reason <> 'Moving Home'
                        AND CSH.effective_from_dt between talk_latest_act_date and event_dt
                        group by CSH.account_number,d.event_dt
                    )AS MR
                 ON MR.account_number = BASE.account_number
                 and MR.event_dt = BASE.event_dt ;


COMMIT 
