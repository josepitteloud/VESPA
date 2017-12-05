
select case when a.account_number is not null then a.account_number else b.account_number end as account_number,
       b.country
       ,a.decile_FP_basic_to_TT
		,a.decile_FP_DM_to_TT
		,a.decile_FP_DS_to_TT
		,a.decile_FP_Family
		,a.decile_FP_Movies
		,a.decile_FP_Multiscreen
		,a.decile_FP_Sports
		,a.decile_OP_basic_to_TT
		,a.decile_OP_DM_to_TT
		,a.decile_OP_DS_to_TT
		,a.decile_OP_Family
		,a.decile_OP_Movies
		,a.decile_OP_Multiscreen
		,a.decile_OP_Sports
		,a.decile_resp_basic_to_TT
		,a.decile_resp_DM_to_TT
		,a.decile_resp_DS_to_TT
		,a.decile_resp_Family
		,a.decile_resp_Movies
		,a.decile_resp_Multiscreen
		,a.decile_resp_sports
		,a.decile_Uplift_basic_to_TT
		,a.decile_Uplift_DM_to_TT
		,a.decile_Uplift_DS_to_TT
		,a.decile_Uplift_Family
		,a.decile_Uplift_Movies
		,a.decile_Uplift_Multiscreen
		,a.decile_Uplift_Sports
			, a.prob_FP_Sports = b.prob_FP_Sports
			, a.Prob_OP_Sports = b.Prob_OP_Sports
			, a.Prob_Uplift_Sports = b.Prob_Uplift_Sports
			, a.prob_resp_sports = b.prob_resp_sports
			, a.prob_FP_Movies = b.prob_FP_Movies
			, a.Prob_OP_Movies = b.Prob_OP_Movies
			, a.Prob_Uplift_Movies = b.Prob_Uplift_Movies
			, a.Prob_resp_Movies = b.Prob_resp_Movies
			, a.prob_FP_basic_to_TT = b.prob_FP_basic_to_TT
			, a.Prob_OP_basic_to_TT = b.Prob_OP_basic_to_TT
			, a.Prob_Uplift_basic_to_TT = b.Prob_Uplift_basic_to_TT
			, a.Prob_resp_basic_to_TT = b.Prob_resp_basic_to_TT
			, a.prob_FP_DM_to_TT = b.prob_FP_DM_to_TT
			, a.Prob_OP_DM_to_TT = b.Prob_OP_DM_to_TT
			, a.Prob_Uplift_DM_to_TT = b.Prob_Uplift_DM_to_TT
			, a.Prob_resp_DM_to_TT = b.Prob_resp_DM_to_TT
			, a.prob_FP_DS_to_TT = b.prob_FP_DS_to_TT
			, a.Prob_OP_DS_to_TT = b.Prob_OP_DS_to_TT
			, a.Prob_Uplift_DS_to_TT = b.Prob_Uplift_DS_to_TT
			, a.Prob_resp_DS_to_TT = b.Prob_resp_DS_to_TT
			, a.prob_FP_Family = b.prob_FP_Family
			, a.Prob_OP_Family = b.Prob_OP_Family
			, a.Prob_Uplift_Family = b.Prob_Uplift_Family
			, a.prob_resp_Family = b.prob_resp_Family
			, a.prob_FP_Multiscreen = b.prob_FP_Multiscreen
			, a.Prob_OP_Multiscreen = b.Prob_OP_Multiscreen
			, a.Prob_Uplift_Multiscreen = b.Prob_Uplift_Multiscreen
			, a.Prob_resp_Multiscreen = b.Prob_resp_Multiscreen
			, a.bb_offer_prob = b.bb_offer_prob
			, a.bb_full_prob = b.bb_full_prob
			, a.bb_uplift_prob = b.bb_uplift_prob
			, a.bb_resp_prob = b.bb_resp_prob
			, a.f_up_offer_prob = b.f_up_offer_prob
			, a.f_up_full_prob = b.f_up_full_prob
			, a.f_up_uplift_prob = b.f_up_uplift_prob
			, a.f_up_resp_prob = b.f_up_resp_prob
			, a.f_re_offer_prob = b.f_re_offer_prob
			, a.f_re_full_prob = b.f_re_full_prob
			, a.f_re_uplift_prob = b.f_re_uplift_prob
			, a.f_re_resp_prob = b.f_re_resp_prob
			, a.sge_offer_prob = b.sge_offer_prob
			, a.sge_full_prob = b.sge_full_prob
			, a.sge_uplift_prob = b.sge_uplift_prob
			, a.sge_resp_prob = b.sge_resp_prob
				, a.sports_eligible = b.sports_eligible
				, a.movies_eligible = b.movies_eligible
				, a.tt_eligible = b.tt_eligible
				, a.bb_eligible = b.bb_eligible
				, a.F_up_eligible = b.F_up_eligible
				, a.f_re_eligible = b.f_re_eligible
				, a.family_eligible = b.family_eligible
				, a.MS_eligible = b.MS_eligible
				, a.SGE_eligible = b.SGE_eligible
		,cast(null as varchar(30)) as Segment_master,
       0 as sports_fp,
       0 as sports_op_target,
       0 as sports_op_other,
       cast(null as date) as sports_date,
       cast(null as varchar(100)) as sports_desc,
       cast(null as integer) as sports_offer_id,
       0 as movies_fp,
       0 as movies_op_target,
       0 as movies_op_other,
       cast(null as date) as movies_date,
       cast(null as varchar(100)) as movies_desc,
       cast(null as integer) as movies_offer_id,
       0 as tt_fp,
       0 as tt_op_target,
       0 as tt_op_other,
       cast(null as date) as tt_date,
       cast(null as varchar(100)) as tt_desc,
       cast(null as integer) as tt_offer_id,
       0 as family_fp,
       0 as family_op_target,
       0 as family_op_other,
       cast(null as date) as family_date,
       cast(null as varchar(100)) as family_desc,
       cast(null as integer) as family_offer_id,
       0 as bb_fp,
       0 as bb_op_target,
       0 as bb_op_other,
       cast(null as date) as bb_date,
       cast(null as varchar(100)) as bb_desc,
       cast(null as integer) as bb_offer_id,
       0 as f_up_fp,
       0 as f_up_op_target,
       0 as f_up_op_other,
       cast(null as date) as f_up_date,
       cast(null as varchar(100)) as f_up_desc,
       cast(null as integer) as f_up_offer_id,
       0 as f_re_fp,
       0 as f_re_op_target,
       0 as f_re_op_other,
       cast(null as date) as f_re_date,
       cast(null as varchar(100)) as f_re_desc,
       cast(null as integer) as f_re_offer_id,
       0 as sge_fp,
       0 as sge_op_target,
       0 as sge_op_other,
       cast(null as date) as sge_date,
       cast(null as varchar(100)) as sge_desc,
       cast(null as integer) as sge_offer_id,
       0 as ms_fp,
       0 as ms_op_target,
       0 as ms_op_other,
       cast(null as date) as ms_date,
       cast(null as varchar(100)) as ms_desc,
       cast(null as integer) as ms_offer_id,
       cast(null as date) as start_date,
       cast(null as date) as end_date,
       cast(null as date) as camp_start_date,
       cast(null as date) as camp_end_date

 into --drop table
        measure4
from simmonsr.planning_201706 a
full outer join slo13.view_attachments_201705 b on a.account_number=b.account_number

COMMIT 

CREATE HG INDEX id1 ON measure4(account_number) 


delete from measure4 where country <> 'UK'



update measure4
   set start_date = '2017-06-01',
         end_date = '2017-06-30' 
         
         
---------------------------         
UPDATE measure4 AS b
SET b.bb_fp = 1
	, b.bb_date = s.event_dt
FROM (SELECT b.account_number
		, min(effective_from_dt) AS event_dt
	FROM measure4 AS a
	INNER JOIN slo13.bb_dashboard_bookings AS b ON a.account_number = b.account_number 
												AND b.effective_from_dt >= start_date 
												AND b.effective_from_dt <= end_date 
												AND current_product_description NOT LIKE '%Fibre%'
	GROUP BY b.account_number
		, b.effective_from_dt
	) AS s
WHERE b.account_number = s.account_number

UPDATE measure4 AS base
SET base.bb_op_target = flag
	, base.bb_desc = offer_dim_description
	, base.bb_offer_id = offer_id
FROM (
	SELECT a.account_Number
		, b.offer_id
		, b.offer_dim_description
		, 1 AS flag
	FROM measure4 AS a
	INNER JOIN cust_product_offers AS b ON a.account_number = b.account_number 
									AND b.initial_effective_dt >= a.bb_date 
									AND b.initial_effective_dt <= (a.bb_date + 21) 
									AND b.offer_end_dt > b.offer_start_dt 
									AND b.offer_amount < 0 
									AND (upper(b.offer_dim_description) LIKE '%BROADBAND%' 
										OR upper(b.offer_dim_description) LIKE '%BB%' 
										OR upper(b.offer_dim_description) LIKE '%LINE RENTAL%')
	WHERE a.bb_fp = 1
	GROUP BY a.account_Number
		, b.offer_id
		, b.offer_dim_description
	) AS source
WHERE base.account_number = source.account_number AND base.bb_fp = 1

UPDATE measure4
SET bb_fp = CASE WHEN bb_op_target = 1 THEN 0 ELSE bb_fp END
         
-------------------------------


Update measure4 as b
Set b.f_up_fp     = 1,
    b.f_up_date   = s.event_dt

From (Select b.account_number,
             min(effective_from_dt) as event_dt
      from measure4 as a inner join slo13.bb_dashboard_bookings as b	on a.account_number=b.account_number
																	and b.effective_from_dt >=  start_date
																	and b.effective_from_dt <=  end_date
																	and current_product_description like '%Fibre%'
																	--and homemove=0
      Group by b.account_number, b.effective_from_dt) as s
Where b.account_number=s.account_number


Update measure4 as base
Set base.f_up_op_target   = flag,
    base.f_up_desc        = offer_dim_description,
    base.f_up_offer_id    = offer_id
From (Select a.account_Number, b.offer_id, b.offer_dim_description, 1 as flag
      from measure4 as a inner join cust_product_offers as b
         on a.account_number=b.account_number
       and b.initial_effective_dt  >=  a.f_up_date
       and b.initial_effective_dt  <= (a.f_up_date+21)
       and b.offer_end_dt > b.offer_start_dt
       and b.offer_amount < 0
       and (upper(b.offer_dim_description) like '%BROADBAND%'
        or upper(b.offer_dim_description) like '%BB%'
        or upper(b.offer_dim_description) like '%FIBRE%'
        or upper(b.offer_dim_description) like '%LINE RENTAL%'
        )
       Where a.f_up_fp=1
      Group by a.account_Number, b.offer_id, b.offer_dim_description) as source
 Where base.account_number=source.account_number and base.f_up_fp=1

update measure4
 set f_up_fp = case when f_up_op_target=1 then 0 else f_up_fp end

--------------------------------------------------------------

--3 Fibre (Regrade)
Update measure4 as b
Set   b.f_re_Date       = s.event_dt,
      b.f_re_fp         = 1
From (Select b.account_number, min(b.effective_from_dt) as event_dt
       from measure4 as a inner join cust_subs_hist as b
         on b.account_number = a.account_number
        and b.effective_from_dt >=  start_date
        and b.effective_from_dt <=  end_date
       where subscription_sub_type = 'Broadband DSL Line'
       and status_code = 'AC'
       and status_code_changed = 'N'
       and previous_description <> 'Not relevant'
       and upper(previous_description) not like '%FIBRE%'
       and upper(previous_description) like '%BROADBAND%'
       and upper(current_product_description) like '%FIBRE%'
      Group by b.account_number, b.effective_from_dt) as s
Where b.account_number=s.account_number



Update measure4 as base
Set  base.f_re_op_target   = flag,
     base.f_re_desc        = offer_dim_description,
     base.f_re_offer_id    = offer_id
From (Select a.Account_Number,  b.offer_id, b.offer_dim_description, 1 as flag
      from measure4 as a inner join cust_product_offers as b
         on a.account_number=b.account_number
       and a.f_re_Date  >= (b.initial_effective_dt)
       and a.f_re_Date  <= (b.initial_effective_dt+21)
       and b.offer_end_dt > b.offer_start_dt
       and b.offer_amount < 0
       and (upper(b.offer_dim_description) like '%BROADBAND%'
        or upper(b.offer_dim_description) like '%BB%'
        or upper(b.offer_dim_description) like '%FIBRE%'
        or upper(b.offer_dim_description) like '%LINE RENTAL%'
        )
       Where a.f_re_fp=1
      Group by a.Account_Number,  b.offer_id, b.offer_dim_description) as source
 Where base.account_number=source.account_number and base.f_re_fp=1

update measure4
 set f_re_fp = case when f_re_op_target=1 then 0 else f_re_fp end


--------------------------------------------------------------
--4 Sky Go Extra
Update measure4 as b
Set b.SGE_Date=s.event_dt
     ,SGE_fp = 1
From (Select b.account_number, min(effective_from_dt) as event_dt
       from measure4 as a inner join cust_subs_hist as b
         on b.account_number = a.account_number
        and b.effective_from_dt >= start_date
        and b.effective_from_dt <= end_date
                WHERE  subscription_sub_type in( 'Sky Go Extra')
                and   subscription_type = 'A-LA-CARTE'
                and   status_code in ('AC')--,'AB','PC')
                and   prev_status_code not in ('AC','AB','PC')
                and   status_code_changed = 'Y'
      Group by b.account_number, b.effective_from_dt) as s
Where b.account_number=s.account_number


Update measure4 as base
Set  base.sge_op_target   = flag,
     base.sge_desc        = offer_dim_description,
     base.sge_offer_id    = offer_id
From (Select a.Account_Number,  b.offer_id, b.offer_dim_description, 1 as flag
      from measure4 as a inner join cust_product_offers as b
         on a.account_number=b.account_number
       and a.SGE_Date  = b.offer_start_dt --b.initial_effective_dt
       and b.offer_end_dt > b.offer_start_dt
       and b.offer_amount < 0
       and upper(b.offer_dim_description) like '%SKY GO EXTRA%'
       and upper(b.offer_dim_description) not like '%MULTISCREEN%'
       Where a.SGE_fp=1
      Group by a.Account_Number,  b.offer_id, b.offer_dim_description) as source
 Where base.account_number=source.account_number and base.SGE_fp=1

update measure4
 set sge_fp = case when sge_op_target=1 then 0 else sge_fp end




         

--5 MS
Update measure4 as b
Set b.MS_Date=s.event_dt
     ,MS_fp = 1
From (Select b.account_number, min(effective_from_dt) as event_dt
       from measure4 as a inner join cust_subs_hist as b
         on b.account_number = a.account_number
        and b.effective_from_dt >= start_date
        and b.effective_from_dt <= end_date
                WHERE  subscription_sub_type in( 'DTV Extra Subscription')
                and    status_code in ('AC') -- ,'AB','PC')
                and    prev_status_code not in ('AC','AB','PC')
      Group by b.account_number, b.effective_from_dt) as s
Where b.account_number=s.account_number



Update measure4 as base
Set  base.MS_op_target   = flag,
     base.MS_desc        = offer_dim_description,
     base.MS_offer_id    = offer_id
From (Select a.Account_Number,  b.offer_id, b.offer_dim_description, 1 as flag
      from measure4 as a inner join cust_product_offers as b
         on a.account_number=b.account_number
       and a.MS_Date      = b.offer_start_dt
       and b.offer_end_dt > b.offer_start_dt
       and b.offer_amount < 0
       and upper(b.offer_dim_description) like '%MULTI%'
       Where a.MS_fp=1
      Group by a.Account_Number,  b.offer_id, b.offer_dim_description) as source
 Where base.account_number=source.account_number and base.MS_fp=1

update measure4
 set MS_fp = case when MS_op_target=1 then 0 else MS_fp end
         
         
         
         
         
--6 Sports
Select source.*, case when source.MU = 0 and source.SU = 1 then 1 else 0 end as SPORTS_UP
into #sports
from (Select
         a.Account_Number
        ,event_dt
        ,count(distinct TypeOfEvent) as n_Events
        ,max(case when TypeOfEvent = 'MU' then 1 else 0 end)  as MU
        ,max(case when TypeOfEvent = 'SU' then 1 else 0 end)  as SU
      From citeam.View_CUST_PACKAGE_MOVEMENTS_HIST a
       inner join measure4 b
       on a.account_number=b.account_number
       and a.event_dt>= start_date
       and a.event_dt<= end_date
      Group by
        a.Account_Number
        ,event_dt) as source
where sports_up=1

Update measure4 as b
Set b.SPORTS_Date=s.event_dt2
     ,b.SPORTS_fp = 1
From (Select b.account_number, min(b.event_dt) as event_dt2
       from #sports b
      Group by b.account_number) as s
Where b.account_number=s.account_number


Update measure4 as base
Set  base.SPORTS_op_target   = flag,
     base.SPORTS_desc        = offer_dim_description,
     base.SPORTS_offer_id    = offer_id
From (Select a.Account_Number,  b.offer_id, b.offer_dim_description, 1 as flag
      from measure4 as a inner join cust_product_offers as b
         on a.account_number=b.account_number
       and a.SPORTS_Date  = b.offer_start_dt
       and b.offer_end_dt > b.offer_start_dt
       and b.offer_amount < 0
       and (upper(b.offer_dim_description) like '%SPORTS%')
       and upper(b.offer_dim_description) not like '%PRICE PROTECTION%'
       and upper(b.offer_dim_description) not like 'BROADBAND%'
       and upper(b.offer_dim_description) not like 'SKY TALK%'
       and upper(b.offer_dim_description) not like 'SKY BROADBAND%'
       and upper(b.offer_dim_description) not like 'LINE RENTAL%'
       and upper(b.offer_dim_description) not Like '%EXCLUDING TOP TIER%'
       and upper(b.offer_dim_description) not like 'FIBRE%'
       and upper(b.offer_dim_description) not like 'HD%'
       and upper(b.offer_dim_description) not like '%FREE ESPN%'
       and upper(b.offer_dim_description) not like '%FREE FIBRE%'
       and upper(b.offer_dim_description) not like '%FREE BROADBAND%'
       and upper(b.offer_dim_description) not like '%FREE LINE RENTAL%'
       and upper(b.offer_dim_description) not like 'MUTV %'
       and upper(b.offer_dim_description) not like 'CHELSEA TV %'

       Where a.SPORTS_fp=1
      Group by a.Account_Number,  b.offer_id, b.offer_dim_description) as source
 Where base.account_number=source.account_number and base.SPORTS_fp=1

update measure4
 set SPORTS_fp = case when SPORTS_op_target=1 then 0 else SPORTS_fp end

         
         
         
         

--7 Movies

Select source.*, case when source.MU = 1 and source.SU = 0 then 1 else 0 end as MOVIES_UP
into #movies
from (Select
         a.Account_Number
        ,event_dt
        ,count(distinct TypeOfEvent) as n_Events
        ,max(case when TypeOfEvent = 'MU' then 1 else 0 end)  as MU
        ,max(case when TypeOfEvent = 'SU' then 1 else 0 end)  as SU
      From citeam.View_CUST_PACKAGE_MOVEMENTS_HIST a
       inner join measure4 b
       on a.account_number=b.account_number
       and a.event_dt>= start_date
       and a.event_dt<= end_date
      Group by
        a.Account_Number
        ,event_dt) as source
where MOVIES_up=1

Update measure4 as b
Set b.MOVIES_Date=s.event_dt2
     ,b.MOVIES_fp = 1
From (Select b.account_number, min(b.event_dt) as event_dt2
       from #MOVIES b
      Group by b.account_number) as s
Where b.account_number=s.account_number


Update measure4 as base
Set  base.MOVIES_op_target   = flag,
     base.MOVIES_desc        = offer_dim_description,
     base.MOVIES_offer_id    = offer_id
From (Select a.Account_Number,  b.offer_id, b.offer_dim_description, 1 as flag
      from measure4 as a inner join cust_product_offers as b
         on a.account_number=b.account_number
       and a.MOVIES_Date  = b.offer_start_dt
       and b.offer_end_dt > b.offer_start_dt
       and b.offer_amount < 0
       and (upper(b.offer_dim_description) like '%MOVIES%'
            OR upper(b.offer_dim_description) like '%CINEMA%')
       and upper(b.offer_dim_description) not like '%PRICE PROTECTION%'
       and upper(b.offer_dim_description) not like 'BROADBAND%'
       and upper(b.offer_dim_description) not like 'SKY TALK%'
       and upper(b.offer_dim_description) not like 'SKY BROADBAND%'
       and upper(b.offer_dim_description) not like 'LINE RENTAL%'
       and upper(b.offer_dim_description) not Like '%EXCLUDING TOP TIER%'
       and upper(b.offer_dim_description) not like 'FIBRE%'
       and upper(b.offer_dim_description) not like 'HD%'
       and upper(b.offer_dim_description) not like '%FREE ESPN%'
       and upper(b.offer_dim_description) not like '%FREE FIBRE%'
       and upper(b.offer_dim_description) not like '%FREE BROADBAND%'
       and upper(b.offer_dim_description) not like '%FREE LINE RENTAL%'
       and upper(b.offer_dim_description) not like 'MUTV %'
       and upper(b.offer_dim_description) not like 'CHELSEA TV %'

       Where a.MOVIES_fp=1
      Group by a.Account_Number,  b.offer_id, b.offer_dim_description) as source
 Where base.account_number=source.account_number and base.MOVIES_fp=1

update measure4
 set MOVIES_fp = case when MOVIES_op_target=1 then 0 else MOVIES_fp end

         


--8 Top Tier

Select source.*, case when source.MU = 1 and source.SU = 1 then 1 else 0 end as TT_UP
into #TT
from (Select
         a.Account_Number
        ,event_dt
        ,count(distinct TypeOfEvent) as n_Events
        ,max(case when TypeOfEvent = 'MU' then 1 else 0 end)  as MU
        ,max(case when TypeOfEvent = 'SU' then 1 else 0 end)  as SU
      From citeam.View_CUST_PACKAGE_MOVEMENTS_HIST a
       inner join measure4 b
       on a.account_number=b.account_number
       and a.event_dt>= start_date
       and a.event_dt<= end_date
      Group by
        a.Account_Number
        ,event_dt) as source
where TT_up=1

Update measure4 as b
Set   b.TT_Date=s.event_dt2
     ,b.TT_fp = 1
From (Select b.account_number, min(b.event_dt) as event_dt2
       from #TT b
      Group by b.account_number) as s
Where b.account_number=s.account_number


Update measure4 as base
Set  base.TT_op_target   = flag,
     base.TT_desc        = offer_dim_description,
     base.TT_offer_id    = offer_id
From (Select a.Account_Number,  b.offer_id, b.offer_dim_description, 1 as flag
      from measure4 as a inner join cust_product_offers as b
         on a.account_number=b.account_number
       and a.TT_Date      = b.offer_start_dt
       and b.offer_end_dt > b.offer_start_dt
       and b.offer_amount < 0
      and     (
              (upper(b.offer_dim_description) like '%MOVIES AND SPORTS%')
                        OR    (upper(b.offer_dim_description) like '%CINEMA AND SPORTS%')
           or (upper(b.offer_dim_description) like '%SPORTS AND CINEMA%')
                      or (upper(b.offer_dim_description) like '%SPORTS AND MOVIES%')
           or (upper(b.offer_dim_description) like '%TOP TIER%')
           or (upper(b.offer_dim_description) like '%TOPTIER%')
              )
      and     (
              (upper(b.offer_dim_description) not like '%EXCLUDING TOP TIER%')
              )
   Where a.TT_fp=1
      Group by a.Account_Number,  b.offer_id, b.offer_dim_description) as source
 Where base.account_number=source.account_number and base.TT_fp=1
       
         
         
         
UPDATE measure4
SET TT_fp = CASE WHEN TT_op_target = 1 THEN 0 ELSE TT_fp END

         
         
		 
		 
		 
		 
		 
		 

ALTER TABLE measure4
ADD (sports_eligible	tinyint DEFAULT NULL ,
        movies_eligible	tinyint DEFAULT NULL ,
        tt_eligible	tinyint DEFAULT NULL ,
        bb_eligible	tinyint DEFAULT NULL ,
        F_up_eligible	tinyint DEFAULT NULL ,
        f_re_eligible	tinyint DEFAULT NULL ,
        family_eligible	tinyint DEFAULT NULL ,
        MS_eligible	tinyint DEFAULT NULL ,
        SGE_eligible	tinyint DEFAULT NULL ,
            prob_FP_Sports float DEFAULT NULL , 
            Prob_OP_Sports float DEFAULT NULL , 
            Prob_Uplift_Sports float DEFAULT NULL , 
            prob_resp_sports float DEFAULT NULL , 
            prob_FP_Movies float DEFAULT NULL , 
            Prob_OP_Movies float DEFAULT NULL , 
            Prob_Uplift_Movies float DEFAULT NULL , 
            Prob_resp_Movies float DEFAULT NULL , 
            prob_FP_basic_to_TT float DEFAULT NULL , 
            Prob_OP_basic_to_TT float DEFAULT NULL , 
            Prob_Uplift_basic_to_TT float DEFAULT NULL , 
            Prob_resp_basic_to_TT float DEFAULT NULL , 
            prob_FP_DM_to_TT float DEFAULT NULL , 
            Prob_OP_DM_to_TT float DEFAULT NULL , 
            Prob_Uplift_DM_to_TT float DEFAULT NULL , 
            Prob_resp_DM_to_TT float DEFAULT NULL , 
            prob_FP_DS_to_TT float DEFAULT NULL , 
            Prob_OP_DS_to_TT float DEFAULT NULL , 
            Prob_Uplift_DS_to_TT float DEFAULT NULL , 
            Prob_resp_DS_to_TT float DEFAULT NULL , 
            prob_FP_Family float DEFAULT NULL , 
            Prob_OP_Family float DEFAULT NULL , 
            Prob_Uplift_Family float DEFAULT NULL , 
            prob_resp_Family float DEFAULT NULL , 
            prob_FP_Multiscreen float DEFAULT NULL , 
            Prob_OP_Multiscreen float DEFAULT NULL , 
            Prob_Uplift_Multiscreen float DEFAULT NULL , 
            Prob_resp_Multiscreen float DEFAULT NULL , 
            bb_offer_prob float DEFAULT NULL , 
            bb_full_prob float DEFAULT NULL , 
            bb_uplift_prob float DEFAULT NULL , 
            bb_resp_prob float DEFAULT NULL , 
            f_up_offer_prob float DEFAULT NULL , 
            
            
            
            f_up_full_prob float DEFAULT NULL , 
            f_up_uplift_prob float DEFAULT NULL , 
            f_up_resp_prob float DEFAULT NULL , 
            f_re_offer_prob float DEFAULT NULL , 
            f_re_full_prob float DEFAULT NULL , 
            f_re_uplift_prob float DEFAULT NULL , 
            f_re_resp_prob float DEFAULT NULL , 
            sge_offer_prob float DEFAULT NULL , 
            sge_full_prob float DEFAULT NULL , 
            sge_uplift_prob float DEFAULT NULL , 
            sge_resp_prob float DEFAULT NULL 
            )
            
UPDATE             measure4
SET  
FROM measure4 AS a 
JOIN simmonsr.planning_201706 AS b ON a.account_number = b.account_number            
            
UPDATE             measure4
SET
FROM measure4 AS a 
JOIN simmonsr.planning_201706 AS b ON a.account_number = b.account_number 

		 
		 



select account_number
,NTILE(10) OVER ( partition by sports_eligible ORDER BY prob_FP_Sports DESC )        as elig_dec_FP_sports
,NTILE(10) OVER ( partition by sports_eligible ORDER BY prob_OP_Sports DESC )        as elig_dec_OP_sports

,NTILE(10) OVER ( partition by movies_eligible ORDER BY prob_FP_movies DESC )           as elig_dec_FP_movies
,NTILE(10) OVER ( partition by movies_eligible ORDER BY prob_OP_movies DESC )          as elig_dec_OP_movies

,NTILE(10) OVER ( partition by bb_eligible ORDER BY bb_full_prob DESC )                 as elig_dec_FP_bb
,NTILE(10) OVER ( partition by bb_eligible ORDER BY bb_offer_prob DESC )             as elig_dec_OP_bb

,NTILE(10) OVER ( partition by f_up_eligible ORDER BY f_up_full_prob DESC )           as elig_dec_FP_f_up
,NTILE(10) OVER ( partition by f_up_eligible ORDER BY f_up_offer_prob DESC )          as elig_dec_OP_f_up

,NTILE(10) OVER ( partition by f_re_eligible ORDER BY f_re_full_prob DESC )           as elig_dec_FP_f_re
,NTILE(10) OVER ( partition by f_re_eligible ORDER BY f_re_offer_prob DESC )          as elig_dec_OP_f_re

,NTILE(10) OVER ( partition by family_eligible ORDER BY prob_FP_family DESC )         as elig_dec_FP_family
,NTILE(10) OVER ( partition by family_eligible ORDER BY prob_OP_family DESC )         as elig_dec_OP_family

,NTILE(10) OVER ( partition by tt_eligible ORDER BY prob_FP_basic_to_tt DESC )          as elig_dec_FP_tt  --??? check this is the right prob - there are a few for tt
,NTILE(10) OVER ( partition by tt_eligible ORDER BY prob_OP_basic_to_tt DESC )        as elig_dec_OP_tt  --??? check this is the right prob - there are a few for tt

,NTILE(10) OVER ( partition by sge_eligible ORDER BY sge_full_prob DESC )              as elig_dec_FP_sge
,NTILE(10) OVER ( partition by sge_eligible ORDER BY sge_offer_prob DESC )              as elig_dec_OP_sge

,NTILE(10) OVER ( partition by ms_eligible ORDER BY prob_FP_multiscreen DESC )          as elig_dec_FP_multiscreen
,NTILE(10) OVER ( partition by ms_eligible ORDER BY prob_OP_multiscreen DESC )          as elig_dec_OP_multiscreen
into #t2
from measure4

COMMIT
CREATE HG INDEX ID1 ON #t2 (account_number )
COMMIT 
SELECT a.* , elig_dec_FP_sports            ,elig_dec_OP_sports            ,elig_dec_FP_movies            ,elig_dec_OP_movies            ,elig_dec_FP_bb            ,elig_dec_OP_bb
            ,elig_dec_FP_f_up            ,elig_dec_OP_f_up            ,elig_dec_FP_f_re            ,elig_dec_OP_f_re            ,elig_dec_FP_family            ,elig_dec_OP_family
            ,elig_dec_FP_tt            ,elig_dec_OP_tt            ,elig_dec_FP_sge            ,elig_dec_OP_sge            ,elig_dec_FP_multiscreen            ,elig_dec_OP_multiscreen
 INTO measure5
 FROM measure4 AS a 
 JOIN #t2 AS b ON a.account_number = b.account_number 
 		 
		 
		 
		 
---------------------------------------------------------------------------------------------------
------------ Outputs
---------------------------------------------------------------------------------------------------
SELECT 'Sports FP' model, elig_dec_FP_sports, sports_fp, count(*) hits, avg(prob_FP_Sports) avg_conv, expected = avg_conv *  hits FROM measure5 WHERE sports_eligible = 1 Group by elig_dec_FP_sports, sports_fp UNION 
 SELECT 'Sports OP' model, elig_dec_OP_sports, sports_op_target, count(*) hits, avg(prob_OP_Sports) avg_conv, expected = avg_conv *  hits  FROM measure5 WHERE sports_eligible = 1 Group by elig_dec_oP_sports, sports_op_target UNION
 SELECT 'Movies FP' model, elig_dec_FP_movies, movies_fp, count(*) hits, avg(prob_FP_movies) avg_conv, expected = avg_conv *  hits  FROM measure5 WHERE movies_eligible = 1 Group by elig_dec_FP_movies, movies_fp UNION
 SELECT 'Movies OP' model, elig_dec_OP_movies, movies_op_target, count(*) hits, avg(prob_oP_movies) avg_conv, expected = avg_conv *  hits  FROM measure5 WHERE movies_eligible = 1 Group by elig_dec_oP_movies, movies_op_target 		 