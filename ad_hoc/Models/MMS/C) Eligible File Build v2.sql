------------------------------------
--     PART iib - Create Base     --
------------------------------------
 SET TEMPORARY OPTION Query_Temp_Space_Limit = 0;
-- 1. CHANGE dm_em_base_YYYYMM TO RELEVANT TABLE
-- 2. CHANGE sharmaa.view_attachments_YYYYMM to RELEVANT TABLE
-- 3. RUN CODE
If Object_ID('dm_em_base_201706') is not NUll
 then DROP TABLE dm_em_base_201706
end if;

select account_number,
       observation_dt,
       sports,
       movies,
       cast(1 as tinyint) as offers_dtv,
       cast(1 as tinyint) as offers_comms,
       toptier,
       sabb,
       case when broadband=1 and upper(BB_type) not like '%FIBRE%' then 1 else 0 end as bb_unl,
       case when broadband=1 and upper(BB_type)     like '%FIBRE%' then 1 else 0 end as bb_fib,
       bb_type,
       skytalk,
       wlr,
       onnet,
       fibrearea,
       package_desc,
       case when upper(package_desc) like '%FAMILY%' then 1 else 0 end as family,
       cast(null as varchar(20)) as x_curr_box_type,
       mr as ms,
       skygoextra,
       cast(0   as     tinyint) as sports_eli,
       cast(0   as     tinyint) as movies_eli,
       cast(0   as     tinyint) as tt_eli,
       cast(0   as     tinyint) as bb_eli,
       cast(0   as     tinyint) as f_up_eli,
       cast(0   as     tinyint) as f_re_eli,
       cast(0   as     tinyint) as family_eli,
       cast(0   as     tinyint) as ms_eli,
       cast(0   as     tinyint) as sge_eli,
       cast(0   as     tinyint) as hdx,
       cast(null as    varchar(20)) as channel,
       case when hdtv_premium=1 and hdtv=1                      then 0
            when upper(package_desc) like '%FAMILY%' and hdtv=1 then 0
            when hdtv=1                                         then 1 else 0 end as hd_leg

into --drop table
        dm_em_base_201706
 from slo13.view_attachments_201705

-- Offers

SELECT DISTINCT account_number
INTO #offers
FROM CUST_SINGLE_ACCOUNT_VIEW
WHERE ((ACCOUNT_NUMBER NOT IN (SELECT  ACCOUNT_NUMBER
								FROM CUST_PRODUCT_OFFERS
								WHERE (( X_SUBSCRIPTION_TYPE IN ('BROADBAND', 'SKY TALK')) 
										AND (DATEDIFF(DAY,  [OFFER_END_DT], '2017-05-29') > 0) 
										AND (DATEDIFF(day, offer_end_dt, '2017-05-29') <= 30) AND ( ACCOUNT_NUMBER IS NOT NULL)) 
										AND ( product_offer_sk NOT IN (SELECT product_offer_sk	FROM CUST_PRODUCT_OFFERS WHERE lower( [OFFER_DIM_DESCRIPTION]) LIKE '%protection%')))) 
		AND (ACCOUNT_NUMBER NOT IN (SELECT  ACCOUNT_NUMBER 
									FROM [CUST_PRODUCT_OFFERS]
									WHERE (( [X_SUBSCRIPTION_TYPE] IN ('BROADBAND', 'SKY TALK')) 
										AND ( [OFFER_STATUS] = 'Active') 
										AND ( [OFFER_END_DT] >= '2017-05-29') 
										AND ( [ACCOUNT_NUMBER] IS NOT NULL)) 
										AND ( [product_offer_sk] NOT IN (SELECT product_offer_sk
																								FROM CUST_PRODUCT_OFFERS
																								WHERE lower([CUST_PRODUCT_OFFERS].[OFFER_DIM_DESCRIPTION]) LIKE '%protection%')))));

--2
SELECT DISTINCT account_number
INTO #offers2
FROM CUST_SINGLE_ACCOUNT_VIEW
WHERE ((ACCOUNT_NUMBER NOT IN (
				SELECT  ACCOUNT_NUMBER
				FROM CUST_PRODUCT_OFFERS
				WHERE (( X_SUBSCRIPTION_TYPE = 'DTV PACKAGE') 
						AND (DATEDIFF(DAY,  OFFER_END_DT, '2017-05-29') > 0) 
						AND (DATEDIFF(day, offer_end_dt, '2017-05-29') <= 30) 
						AND ( ACCOUNT_NUMBER IS NOT NULL)) 
						AND ( product_offer_sk NOT IN (SELECT product_offer_sk FROM CUST_PRODUCT_OFFERS WHERE lower( [OFFER_DIM_DESCRIPTION]) LIKE '%protection%')))) 
	AND (ACCOUNT_NUMBER NOT IN (
				SELECT  ACCOUNT_NUMBER
				FROM CUST_PRODUCT_OFFERS
				WHERE (( X_SUBSCRIPTION_TYPE = 'DTV PACKAGE') 
					AND ( OFFER_STATUS = 'Active') 
					AND ( OFFER_END_DT >= '2017-05-29') 
					AND ( ACCOUNT_NUMBER IS NOT NULL)) 
					AND ( product_offer_sk NOT IN (SELECT product_offer_sk FROM CUST_PRODUCT_OFFERS WHERE lower( [OFFER_DIM_DESCRIPTION]) LIKE '%protection%')))));


update dm_em_base_201706 a
 set offers_dtv = case when b.account_number is not null then 0 else 1 end
 from #offers2 b
  where a.account_number=b.account_number;

update dm_em_base_201706 a
 set offers_comms = case when b.account_number is not null then 0 else 1 end
 from #offers b
  where a.account_number=b.account_number;

  
  -- Box Type
SELECT su.account_number
INTO #hda
FROM CUST_SET_TOP_BOX stb
INNER JOIN dm_em_base_201706 su ON su.account_number = stb.account_number
WHERE box_installed_dt <= su.observation_dt AND box_replaced_dt > su.observation_dt AND current_product_description LIKE '%HD%';



SELECT su.account_number
	, max(CASE WHEN csh.subscription_sub_type = 'DTV Primary Viewing' THEN 1 ELSE 0 END) AS dtv
	, max(CASE WHEN csh.subscription_sub_type = 'DTV HD' THEN 1 ELSE 0 END) AS hd
	, max(CASE WHEN csh.subscription_sub_type = 'DTV Extra Subscription' THEN 1 ELSE 0 END) AS mr
	, max(CASE WHEN csh.subscription_sub_type = 'DTV Sky+' THEN 1 ELSE 0 END) AS sp
	, max(CASE WHEN #hda.account_number IS NOT NULL THEN 1 ELSE 0 END) AS HDstb
INTO #flags
FROM dm_em_base_201706 AS su
LEFT JOIN cust_subs_hist AS csh ON su.account_number = csh.account_number
LEFT JOIN #hda ON #hda.account_number = su.account_number
WHERE csh.status_code IN ('AC', 'AB', 'PC') AND csh.effective_from_dt <= su.observation_dt AND csh.effective_to_dt > su.observation_dt AND csh.effective_from_dt <> csh.effective_to_dt AND csh.first_activation_dt < '9999-09-09'
GROUP BY su.account_number;

update dm_em_base_201706 as su
set x_Curr_Box_Type = case
    when flags.dtv=1 and flags.hd=1 and flags.mr=1                   then '8) HD Combi'
    when flags.dtv=1 and flags.hd=1                                  then '7) HD Only'
    when flags.dtv=1 and flags.sp=1 and flags.HDstb=1 and flags.mr=1 then '6) HDx Combi'
    when flags.dtv=1 and flags.sp=1 and flags.HDstb=1                then '5) HDx Only'
    when flags.dtv=1 and flags.sp=1 and flags.mr=1                   then '4) Skyplus Combi'
    when flags.dtv=1 and flags.mr=1                                  then '3) Multiroom Only'
    when flags.dtv=1 and flags.sp=1                                  then '2) Skyplus Only'
    when flags.dtv=1                                                 then '1) FDB Only'
    else '7) Inactive' end
from #flags as flags
where su.account_number=flags.account_number;


-- Eligibility
update dm_em_base_201706
 set sports_eli   = case when sabb=0 and sports=0 and movies=0  then 1 else 0 end,
     movies_eli   = case when sabb=0 and sports=0 and movies=0  then 1 else 0 end,
     tt_eli       = case when sabb=0 and sports=0 and movies=0  then 1 else 0 end,
     bb_eli       = case when bb_unl=0 and bb_fib=0 and skytalk=0 and wlr=0 and onnet=1 then 1 else 0 end,
     F_up_eli     = case when bb_unl=0 and bb_fib=0 and skytalk=0 and wlr=0 and onnet=1 and fibrearea=1 then 1 else 0 end,
     f_re_eli     = case when bb_unl=1 and bb_fib=0 and onnet=1 and fibrearea=1 and
                              bb_type not in('Connect') then 1 else 0 end,
     family_eli   = case when sabb=0 and family=0 and hd_leg=0 and x_Curr_Box_Type like '%HD%' then 1 else 0 end,
     MS_eli       = case when sabb=0 and ms=0 and skygoextra=0 then 1 else 0 end,
     SGE_eli      = case when sabb=0 and ms=0 and skygoextra=0 then 1 else 0 end;


