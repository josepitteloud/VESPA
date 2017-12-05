
----------------------------------
--                              --
--        METRICS BUILD         --
--                              --
----------------------------------
grant all on simmonsr.CMO_BAU_TEST_MEASUREMENT_1415Q4 to public
grant all on simmonsr.ccdm_q4_master to public

--Check DB Update Dates
select max(effective_from_dt) from cust_subs_hist -- Usually once a week on a Thursday
--2015-04-30 (Refreshed on 2015-05-03)
--2015-05-07 (Refreshed on 2015-05-11)
--2015-05-14 (Refreshed on 2015-05-18)
--2015-05-21 (Refreshed on 2015-05-25)
--2015-05-28 (Refreshed on 2015-06-01)
--2015-06-04 (Refreshed on 2015-06-07)

select max(effective_from_dt) from simmonsr.bb_dashboard_bookings -- Usually once a week on a Monday
select max(initial_effective_dt), max(offer_start_dt), max(status_change_date), max(dw_created_dt) from cust_product_offers
select max(event_dt) from citeam.View_CUST_PACKAGE_MOVEMENTS_HIST


-- SET START AND END DATES
DECLARE @START  DATE
DECLARE @END    DATE
SET     @START = '2015-04-24'
SET     @END   = '2015-06-02'

-- CONTROL DATES
DECLARE @START_control  DATE
DECLARE @END_control    DATE
SET     @START_control = '2015-04-24'
SET     @END_control   = '2015-05-22'

DECLARE  @startdate_SMT DATE
DECLARE  @enddate_SMT   DATE
SET      @startdate_SMT = '2015-04-24'
SET      @enddate_SMT   = '2015-05-22'

DECLARE  @startdate_BB DATE
DECLARE  @enddate_BB   DATE
SET      @startdate_BB = '2015-05-05'
SET      @enddate_BB   = '2015-06-02'

DECLARE  @startdate_OTHER DATE
DECLARE  @enddate_OTHER   DATE
SET      @startdate_OTHER = '2015-04-28'
SET      @enddate_OTHER   = '2015-05-25'

IF object_id ('measure2') IS NOT NULL
BEGIN
    DROP TABLE measure2
END
select case when a.account_number is not null then a.account_number else b.account_number end as account_number,
       b.country,
       a.decile_fp_sports ,
       a.decile_op_sports ,
       a.decile_uplift_sports ,
       a.decile_fp_movies ,
       a.decile_op_movies ,
       a.decile_uplift_movies ,
       a.decile_fp_toptier ,
       a.decile_op_toptier ,
       a.decile_uplift_toptier ,
       a.decile_fp_family ,
       a.decile_op_family ,
       a.decile_uplift_family ,
       a.decile_fp_bb ,
       a.decile_op_bb ,
       a.decile_uplift_bb ,
       a.decile_fp_f_up ,
       a.decile_op_f_up ,
       a.decile_uplift_f_up ,
       a.decile_fp_f_re ,
       a.decile_op_f_re ,
       a.decile_uplift_f_re ,
       a.decile_fp_sge,
       a.decile_op_sge,
       a.decile_uplift_sge,
       a.decile_fp_multiscreen,
       a.decile_op_multiscreen,
       a.decile_uplift_multiscreen,
       a.decile_movies_uplift_old,
       a.decile_sports_uplift_old,
       a.decile_tt_uplift_old,
       a.decile_bb_old as decile_bb_uplift_old,
       a.decile_f_up_old,
       a.segment,
       c.segment as overall_segment,
       cast(null as varchar(30)) as Segment_master,
       a.campaign_type,
       a.campaign_cell,
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
        measure2
 from simmonsr.CMO_BAU_TEST_MEASUREMENT_1415Q4_v2 a
  full outer join sharmaa.view_attachments_201504 b
   on a.account_number=b.account_number
   left join simmonsr.ccdm_q4_master c
   on b.account_number=c.account_number

update measure2
 set segment_master = case when segment is not null then segment
                           when segment is null and overall_segment is not null then 'C - Other'
                           else 'A - No DM' end

delete from measure2 where country <> 'UK'
delete from measure2 where segment is not null and overall_segment is null

--Dates select segment, segment_master, count(*) from measure2 group by  segment, segment_master

update measure2
   set start_date = @start,
         end_date = @end,
  camp_start_date = case when segment in('B -  BAU vs CMO - BAU','B -  BAU vs CMO - CMO','B - 720K Offer Test')
                      and campaign_type in('1. Sports          ','2. Movies          ','3. Top Tier        ')
                          then @startdate_SMT
                          when segment in('B -  BAU vs CMO - BAU','B -  BAU vs CMO - CMO','B - 720K Offer Test')
                      and campaign_type in('4. Family          ','6. Multiscreen     ','5. SkyGoExtra      ')
                          then @startdate_OTHER
                          when segment in('B -  BAU vs CMO - BAU','B -  BAU vs CMO - CMO','B - 720K Offer Test')
                      and campaign_type in('7. Broadband Upsell','8. Fibre - Upsell  ','9. Fibre - Regrade ')
                          then @startdate_BB
                          when segment in('B -  BAU vs CMO - Control') then @startdate_SMT
                       else @START end,
    camp_end_date = case when segment in('B -  BAU vs CMO - BAU','B -  BAU vs CMO - CMO','B - 720K Offer Test')
                      and campaign_type in('1. Sports          ','2. Movies          ','3. Top Tier        ')
                          then @enddate_SMT
                          when segment in('B -  BAU vs CMO - BAU','B -  BAU vs CMO - CMO','B - 720K Offer Test')
                      and campaign_type in('4. Family          ','6. Multiscreen     ','5. SkyGoExtra      ')
                          then @enddate_OTHER
                          when segment in('B -  BAU vs CMO - BAU','B -  BAU vs CMO - CMO','B - 720K Offer Test')
                      and campaign_type in('7. Broadband Upsell','8. Fibre - Upsell  ','9. Fibre - Regrade ')
                          then @enddate_BB
                          when segment in('B -  BAU vs CMO - Control') then @enddate_SMT
                      else @END end


--1  BB
-- Requests
Update measure2 as b
Set b.bb_fp     = 1,
    b.bb_date   = s.event_dt

From (Select b.account_number,
             min(effective_from_dt) as event_dt
      from measure2 as a inner join simmonsr.bb_dashboard_bookings as b
         on a.account_number=b.account_number
        and b.effective_from_dt >=  start_date
        and b.effective_from_dt <=  end_date
        and current_product_description not like '%Fibre%'
        --and homemove=0
      Group by b.account_number, b.effective_from_dt) as s
Where b.account_number=s.account_number

Update measure2 as base
Set base.bb_op_target   = flag,
    base.bb_desc        = offer_dim_description,
    base.bb_offer_id    = offer_id
From (Select a.account_Number, b.offer_id, b.offer_dim_description, 1 as flag
      from measure2 as a inner join cust_product_offers as b
         on a.account_number=b.account_number
       and b.initial_effective_dt  >=  a.bb_date
       and b.initial_effective_dt  <= (a.bb_date+21)
       and b.offer_end_dt > b.offer_start_dt
       and b.offer_amount < 0
       and (upper(b.offer_dim_description) like '%BROADBAND%'
        or upper(b.offer_dim_description) like '%BB%'
        or upper(b.offer_dim_description) like '%LINE RENTAL%'
        )
       Where a.bb_fp=1
      Group by a.account_Number, b.offer_id, b.offer_dim_description) as source
 Where base.account_number=source.account_number and base.bb_fp=1

update measure2
 set bb_fp = case when bb_op_target=1 then 0 else bb_fp end


-- Activations
/*
Update measure2 as b
Set b.bb_fp     = 1,
    b.bb_date   = s.event_dt

From (Select b.account_number,
             min(activation_date) as event_dt
      from measure2 as a inner join simmonsr.bb_dashboard_bookings as b
         on a.account_number=b.account_number
        and b.effective_from_dt >=  start_date
        and b.effective_from_dt <=  end_date
        and current_product_description not like '%Fibre%'
        and b.activation_date is not null
        --and homemove=0
      Group by b.account_number, b.effective_from_dt) as s
Where b.account_number=s.account_number

Update measure2 as base
Set base.bb_op_target   = flag,
    base.bb_desc        = offer_dim_description,
    base.bb_offer_id    = offer_id
From (Select a.account_Number, b.offer_id, b.offer_dim_description, 1 as flag
      from measure2 as a inner join cust_product_offers as b
         on a.account_number=b.account_number
       and b.offer_start_dt  >=  a.bb_date
       and b.offer_end_dt > b.offer_start_dt
       and b.offer_amount < 0
       and (upper(b.offer_dim_description) like '%BROADBAND%'
        or upper(b.offer_dim_description) like '%BB%'
        or upper(b.offer_dim_description) like '%LINE RENTAL%'
        )
       Where a.bb_fp=1
      Group by a.account_Number, b.offer_id, b.offer_dim_description) as source
 Where base.account_number=source.account_number and base.bb_fp=1

update measure2
 set bb_fp = case when bb_op_target=1 then 0 else bb_fp end
*/



--2 BB (Fibre Upgrade)
--Requests
Update measure2 as b
Set b.f_up_fp     = 1,
    b.f_up_date   = s.event_dt

From (Select b.account_number,
             min(effective_from_dt) as event_dt
      from measure2 as a inner join simmonsr.bb_dashboard_bookings as b
         on a.account_number=b.account_number
        and b.effective_from_dt >=  start_date
        and b.effective_from_dt <=  end_date
        and current_product_description like '%Fibre%'
        --and homemove=0
      Group by b.account_number, b.effective_from_dt) as s
Where b.account_number=s.account_number


Update measure2 as base
Set base.f_up_op_target   = flag,
    base.f_up_desc        = offer_dim_description,
    base.f_up_offer_id    = offer_id
From (Select a.account_Number, b.offer_id, b.offer_dim_description, 1 as flag
      from measure2 as a inner join cust_product_offers as b
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

update measure2
 set f_up_fp = case when f_up_op_target=1 then 0 else f_up_fp end



-- Activations
/*
Update measure2 as b
Set b.f_up_fp     = 1,
    b.f_up_date   = s.event_dt

From (Select b.account_number,
             min(activation_date) as event_dt
      from measure2 as a inner join simmonsr.bb_dashboard_bookings as b
         on a.account_number=b.account_number
        and b.effective_from_dt >=  start_date
        and b.effective_from_dt <=  end_date
        and current_product_description like '%Fibre%'
        and b.activation_date is not null
        --and homemove=0
      Group by b.account_number, b.effective_from_dt) as s
Where b.account_number=s.account_number

Update measure2 as base
Set base.f_up_op_target   = flag,
    base.f_up_desc        = offer_dim_description,
    base.f_up_offer_id    = offer_id
From (Select a.account_Number, b.offer_id, b.offer_dim_description, 1 as flag
      from measure2 as a inner join cust_product_offers as b
         on a.account_number=b.account_number
       and b.offer_start_dt  >=  a.bb_date
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

update measure2
 set f_up_fp = case when f_up_op_target=1 then 0 else f_up_fp end
*/

--3 Fibre (Regrade)
Update measure2 as b
Set   b.f_re_Date       = s.event_dt,
      b.f_re_fp         = 1
From (Select b.account_number, min(b.effective_from_dt) as event_dt
       from measure2 as a inner join cust_subs_hist as b
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



Update measure2 as base
Set  base.f_re_op_target   = flag,
     base.f_re_desc        = offer_dim_description,
     base.f_re_offer_id    = offer_id
From (Select a.Account_Number,  b.offer_id, b.offer_dim_description, 1 as flag
      from measure2 as a inner join cust_product_offers as b
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

update measure2
 set f_re_fp = case when f_re_op_target=1 then 0 else f_re_fp end




--4 Sky Go Extra
Update measure2 as b
Set b.SGE_Date=s.event_dt
     ,SGE_fp = 1
From (Select b.account_number, min(effective_from_dt) as event_dt
       from measure2 as a inner join cust_subs_hist as b
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


Update measure2 as base
Set  base.sge_op_target   = flag,
     base.sge_desc        = offer_dim_description,
     base.sge_offer_id    = offer_id
From (Select a.Account_Number,  b.offer_id, b.offer_dim_description, 1 as flag
      from measure2 as a inner join cust_product_offers as b
         on a.account_number=b.account_number
       and a.SGE_Date  = b.offer_start_dt --b.initial_effective_dt
       and b.offer_end_dt > b.offer_start_dt
       and b.offer_amount < 0
       and upper(b.offer_dim_description) like '%SKY GO EXTRA%'
       and upper(b.offer_dim_description) not like '%MULTISCREEN%'
       Where a.SGE_fp=1
      Group by a.Account_Number,  b.offer_id, b.offer_dim_description) as source
 Where base.account_number=source.account_number and base.SGE_fp=1

update measure2
 set sge_fp = case when sge_op_target=1 then 0 else sge_fp end




--5 MS
Update measure2 as b
Set b.MS_Date=s.event_dt
     ,MS_fp = 1
From (Select b.account_number, min(effective_from_dt) as event_dt
       from measure2 as a inner join cust_subs_hist as b
         on b.account_number = a.account_number
        and b.effective_from_dt >= start_date
        and b.effective_from_dt <= end_date
                WHERE  subscription_sub_type in( 'DTV Extra Subscription')
                and    status_code in ('AC') -- ,'AB','PC')
                and    prev_status_code not in ('AC','AB','PC')
      Group by b.account_number, b.effective_from_dt) as s
Where b.account_number=s.account_number



Update measure2 as base
Set  base.MS_op_target   = flag,
     base.MS_desc        = offer_dim_description,
     base.MS_offer_id    = offer_id
From (Select a.Account_Number,  b.offer_id, b.offer_dim_description, 1 as flag
      from measure2 as a inner join cust_product_offers as b
         on a.account_number=b.account_number
       and a.MS_Date      = b.offer_start_dt
       and b.offer_end_dt > b.offer_start_dt
       and b.offer_amount < 0
       and upper(b.offer_dim_description) like '%MULTI%'
       Where a.MS_fp=1
      Group by a.Account_Number,  b.offer_id, b.offer_dim_description) as source
 Where base.account_number=source.account_number and base.MS_fp=1

update measure2
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
       inner join measure2 b
       on a.account_number=b.account_number
       and a.event_dt>= start_date
       and a.event_dt<= end_date
      Group by
        a.Account_Number
        ,event_dt) as source
where sports_up=1

Update measure2 as b
Set b.SPORTS_Date=s.event_dt2
     ,b.SPORTS_fp = 1
From (Select b.account_number, min(b.event_dt) as event_dt2
       from #sports b
      Group by b.account_number) as s
Where b.account_number=s.account_number


Update measure2 as base
Set  base.SPORTS_op_target   = flag,
     base.SPORTS_desc        = offer_dim_description,
     base.SPORTS_offer_id    = offer_id
From (Select a.Account_Number,  b.offer_id, b.offer_dim_description, 1 as flag
      from measure2 as a inner join cust_product_offers as b
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

update measure2
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
       inner join measure2 b
       on a.account_number=b.account_number
       and a.event_dt>= start_date
       and a.event_dt<= end_date
      Group by
        a.Account_Number
        ,event_dt) as source
where MOVIES_up=1

Update measure2 as b
Set b.MOVIES_Date=s.event_dt2
     ,b.MOVIES_fp = 1
From (Select b.account_number, min(b.event_dt) as event_dt2
       from #MOVIES b
      Group by b.account_number) as s
Where b.account_number=s.account_number


Update measure2 as base
Set  base.MOVIES_op_target   = flag,
     base.MOVIES_desc        = offer_dim_description,
     base.MOVIES_offer_id    = offer_id
From (Select a.Account_Number,  b.offer_id, b.offer_dim_description, 1 as flag
      from measure2 as a inner join cust_product_offers as b
         on a.account_number=b.account_number
       and a.MOVIES_Date  = b.offer_start_dt
       and b.offer_end_dt > b.offer_start_dt
       and b.offer_amount < 0
       and (upper(b.offer_dim_description) like '%MOVIES%')
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

update measure2
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
       inner join measure2 b
       on a.account_number=b.account_number
       and a.event_dt>= start_date
       and a.event_dt<= end_date
      Group by
        a.Account_Number
        ,event_dt) as source
where TT_up=1

Update measure2 as b
Set   b.TT_Date=s.event_dt2
     ,b.TT_fp = 1
From (Select b.account_number, min(b.event_dt) as event_dt2
       from #TT b
      Group by b.account_number) as s
Where b.account_number=s.account_number


Update measure2 as base
Set  base.TT_op_target   = flag,
     base.TT_desc        = offer_dim_description,
     base.TT_offer_id    = offer_id
From (Select a.Account_Number,  b.offer_id, b.offer_dim_description, 1 as flag
      from measure2 as a inner join cust_product_offers as b
         on a.account_number=b.account_number
       and a.TT_Date      = b.offer_start_dt
       and b.offer_end_dt > b.offer_start_dt
       and b.offer_amount < 0
      and     (
              (upper(b.offer_dim_description) like '%MOVIES AND SPORTS%')
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

/*
-- 2 - Test Cells
Update measure2 as base
Set  base.TT_op_target   = flag,
     base.TT_desc        = offer_dim_description,
     base.TT_offer_id    = offer_id
From (Select a.Account_Number,  b.offer_id, b.offer_dim_description, 1 as flag
      from measure2 as a inner join cust_product_offers as b
         on a.account_number=b.account_number
       and a.TT_Date      = b.offer_start_dt
       and b.offer_end_dt > b.offer_start_dt
       and b.offer_amount < 0
      and     (
              (upper(b.offer_dim_description) like '%MOVIES%')
           or (upper(b.offer_dim_description) like '%SPORTS%')
           or (upper(b.offer_dim_description) like '%TOP TIER%')
           or (upper(b.offer_dim_description) like '%TOPTIER%')
              )
      and     (
              (upper(b.offer_dim_description) not like '%EXCLUDING TOP TIER%')
              )


      Group by a.Account_Number,  b.offer_id, b.offer_dim_description) as source
 Where base.account_number=source.account_number and base.TT_fp=1
         and base.campaign_type='3. Top Tier        '
         and base.campaign_cell not in('BAU                        ',
                                'CMO                        ',
                                'Control - Basic Customers  ')
*/

update measure2
 set TT_fp = case when TT_op_target=1 then 0 else TT_fp end



--9 Family (1)
/*
select distinct
   a.Account_number,
   effective_from_dt,
   current_product_description,
   ent_cat_prod_changed,
   b.genre as new_package,
   c.genre as prev_package
into #family
from cust_subs_hist a
   left join cust_entitlement_lookup b on a.current_short_description = b.short_description
   left join cust_entitlement_lookup c on a.previous_short_description = c.short_description
   inner join measure2 d on a.account_number=d.account_number
where   effective_from_dt   >= start_date
    and effective_from_dt   <= end_date
    and subscription_sub_type = 'DTV Primary Viewing'
    and ent_cat_prod_changed = 'Y'
    and upper(prev_package) not like '%FAMILY%'
    and upper(new_package)      like '%FAMILY%'


Update measure2 as b
Set   b.FAMILY_Date=s.event_dt2
     ,b.FAMILY_fp = 1
From (Select b.account_number, min(b.effective_from_dt) as event_dt2
       from #FAMILY b
      Group by b.account_number) as s
Where b.account_number=s.account_number


Update measure2 as base
Set  base.FAMILY_op_target   = flag,
     base.FAMILY_desc        = offer_dim_description,
     base.FAMILY_offer_id    = offer_id
From (Select a.Account_Number,  b.offer_id, b.offer_dim_description, 1 as flag
      from measure2 as a inner join cust_product_offers as b
         on a.account_number=b.account_number
       and a.FAMILY_Date  = b.offer_start_dt
       and b.offer_end_dt > b.offer_start_dt
       and b.offer_amount < 0
       and ((upper(b.offer_dim_description) like '%FAMILY%') OR (upper(b.offer_dim_description) like '%HD%'))

       Where a.FAMILY_fp=1
      Group by a.Account_Number,  b.offer_id, b.offer_dim_description) as source
 Where base.account_number=source.account_number and base.FAMILY_fp=1


update measure2
 set FAMILY_fp = case when FAMILY_op_target=1 then 0 else FAMILY_fp end
*/


--9 Family - ii
/*
alter table measure2
add family2_fp           tinyint default 0,
add family2_op_target    tinyint default 0,
add family2_op_other     tinyint default 0,
add family2_date         date default null,
add family2_desc         varchar(100) default null,
add family2_offer_id     integer default 0 */


select ch13w.account_number,
       effective_from_dt
into --drop table
        #family_sales
from measure2 as ch13w
     inner join
     cust_subs_hist as csh
     on csh.account_number = ch13w.account_number
     where effective_from_dt    >= start_date
        and effective_from_dt   <= end_date
        and csh.subscription_sub_type = 'DTV HD'
        and csh.current_product_sk = 43678
        and csh.status_code in ('AC')--,'AB','PC')
        and csh.prev_status_code not in ('AC','AB','PC')
        and csh.status_end_dt > csh.status_start_dt



Update measure2 as b
Set   b.FAMILY_Date=s.event_dt2
     ,b.FAMILY_fp = 1
From (Select b.account_number, min(b.effective_from_dt) as event_dt2
       from #FAMILY_sales b
      Group by b.account_number) as s
Where b.account_number=s.account_number


Update measure2 as base
Set  base.FAMILY_op_target   = flag,
     base.FAMILY_desc        = offer_dim_description,
     base.FAMILY_offer_id    = offer_id
From (Select a.Account_Number,  b.offer_id, b.offer_dim_description, 1 as flag
      from measure2 as a inner join cust_product_offers as b
         on a.account_number=b.account_number
       and a.FAMILY_Date  = b.offer_start_dt
       and b.offer_end_dt > b.offer_start_dt
       and b.offer_amount < 0
       and ((upper(b.offer_dim_description) like '%FAMILY%') OR (upper(b.offer_dim_description) like '%HD%'))

       Where a.FAMILY_fp=1
      Group by a.Account_Number,  b.offer_id, b.offer_dim_description) as source
 Where base.account_number=source.account_number and base.FAMILY_fp=1


update measure2
 set FAMILY_fp = case when FAMILY_op_target=1 then 0 else FAMILY_fp end

/*
select segment_master, campaign_cell, count(*), sum(family_fp), sum(family_op_target), sum(family2_fp), sum(family2_op_target)
from measure2
group by segment_Master, campaign_cell

select family_date, count(*) from measure2 group by family_date
select family2_date, count(*) from measure2 group by family2_date
*/



---- Delete SGE Campaign
delete from measure2 where campaign_cell in('BAU','CMO') and campaign_type='5. SkyGoExtra      '


---- Delete Response which are too early

delete from measure2 where segment_master in('B -  BAU vs CMO - BAU','B -  BAU vs CMO - CMO','B - 720K Offer Test')
   and campaign_type in ('1. Sports          ') and sports_date<camp_start_date
delete from measure2 where segment_master in('B -  BAU vs CMO - BAU','B -  BAU vs CMO - CMO','B - 720K Offer Test')
   and campaign_type in ('2. Movies          ') and movies_date<camp_start_date
delete from measure2 where segment_master in('B -  BAU vs CMO - BAU','B -  BAU vs CMO - CMO','B - 720K Offer Test')
   and campaign_type in ('3. Top Tier        ') and tt_date<camp_start_date
delete from measure2 where segment_master in('B -  BAU vs CMO - BAU','B -  BAU vs CMO - CMO','B - 720K Offer Test')
   and campaign_type in ('7. Broadband Upsell') and bb_date<camp_start_date
delete from measure2 where segment_master in('B -  BAU vs CMO - BAU','B -  BAU vs CMO - CMO','B - 720K Offer Test')
   and campaign_type in ('8. Fibre - Upsell  ') and f_up_date<camp_start_date
delete from measure2 where segment_master in('B -  BAU vs CMO - BAU','B -  BAU vs CMO - CMO','B - 720K Offer Test')
   and campaign_type in ('9. Fibre - Regrade ') and f_re_date<camp_start_date
delete from measure2 where segment_master in('B -  BAU vs CMO - BAU','B -  BAU vs CMO - CMO','B - 720K Offer Test')
   and campaign_type in ('4. Family          ') and family_date<camp_start_date
delete from measure2 where segment_master in('B -  BAU vs CMO - BAU','B -  BAU vs CMO - CMO','B - 720K Offer Test')
   and campaign_type in ('6. Multiscreen     ') and ms_date<camp_start_date
delete from measure2 where segment_master in('B -  BAU vs CMO - BAU','B -  BAU vs CMO - CMO','B - 720K Offer Test')
   and campaign_type in ('5. SkyGoExtra      ') and sge_date<camp_start_date



--Set responses to 0 if they occur before or after the specific campaign start and end dates
update measure2
 set sports_fp          = case when sports_date<camp_start_date or sports_date>camp_end_date then 0 else sports_fp end,
     sports_op_target   = case when sports_date<camp_start_date or sports_date>camp_end_date then 0 else sports_op_target end,
     sports_op_other    = case when sports_date<camp_start_date or sports_date>camp_end_date then 0 else sports_op_other end,
     sports_desc        = case when sports_date<camp_start_date or sports_date>camp_end_date then null else sports_desc end,
     sports_offer_id    = case when sports_date<camp_start_date or sports_date>camp_end_date then null else sports_offer_id end,
     sports_date        = case when sports_date<camp_start_date or sports_date>camp_end_date then null else sports_date end,

     movies_fp          = case when movies_date<camp_start_date or movies_date>camp_end_date then 0 else movies_fp end,
     movies_op_target   = case when movies_date<camp_start_date or movies_date>camp_end_date then 0 else movies_op_target end,
     movies_op_other    = case when movies_date<camp_start_date or movies_date>camp_end_date then 0 else movies_op_other end,
     movies_desc        = case when movies_date<camp_start_date or movies_date>camp_end_date then null else movies_desc end,
     movies_offer_id    = case when movies_date<camp_start_date or movies_date>camp_end_date then null else movies_offer_id end,
     movies_date        = case when movies_date<camp_start_date or movies_date>camp_end_date then null else movies_date end,

     tt_fp          = case when tt_date<camp_start_date or tt_date>camp_end_date then 0 else tt_fp end,
     tt_op_target   = case when tt_date<camp_start_date or tt_date>camp_end_date then 0 else tt_op_target end,
     tt_op_other    = case when tt_date<camp_start_date or tt_date>camp_end_date then 0 else tt_op_other end,
     tt_desc        = case when tt_date<camp_start_date or tt_date>camp_end_date then null else tt_desc end,
     tt_offer_id    = case when tt_date<camp_start_date or tt_date>camp_end_date then null else tt_offer_id end,
     tt_date        = case when tt_date<camp_start_date or tt_date>camp_end_date then null else tt_date end,

     family_fp          = case when family_date<camp_start_date or family_date>camp_end_date then 0 else family_fp end,
     family_op_target   = case when family_date<camp_start_date or family_date>camp_end_date then 0 else family_op_target end,
     family_op_other    = case when family_date<camp_start_date or family_date>camp_end_date then 0 else family_op_other end,
     family_desc        = case when family_date<camp_start_date or family_date>camp_end_date then null else family_desc end,
     family_offer_id    = case when family_date<camp_start_date or family_date>camp_end_date then null else family_offer_id end,
     family_date        = case when family_date<camp_start_date or family_date>camp_end_date then null else family_date end,

     ms_fp          = case when ms_date<camp_start_date or ms_date>camp_end_date then 0 else ms_fp end,
     ms_op_target   = case when ms_date<camp_start_date or ms_date>camp_end_date then 0 else ms_op_target end,
     ms_op_other    = case when ms_date<camp_start_date or ms_date>camp_end_date then 0 else ms_op_other end,
     ms_desc        = case when ms_date<camp_start_date or ms_date>camp_end_date then null else ms_desc end,
     ms_offer_id    = case when ms_date<camp_start_date or ms_date>camp_end_date then null else ms_offer_id end,
     ms_date        = case when ms_date<camp_start_date or ms_date>camp_end_date then null else ms_date end,

     sge_fp          = case when sge_date<camp_start_date or sge_date>camp_end_date then 0 else sge_fp end,
     sge_op_target   = case when sge_date<camp_start_date or sge_date>camp_end_date then 0 else sge_op_target end,
     sge_op_other    = case when sge_date<camp_start_date or sge_date>camp_end_date then 0 else sge_op_other end,
     sge_desc        = case when sge_date<camp_start_date or sge_date>camp_end_date then null else sge_desc end,
     sge_offer_id    = case when sge_date<camp_start_date or sge_date>camp_end_date then null else sge_offer_id end,
     sge_date        = case when sge_date<camp_start_date or sge_date>camp_end_date then null else sge_date end,

     f_up_fp          = case when f_up_date<camp_start_date or f_up_date>camp_end_date then 0 else f_up_fp end,
     f_up_op_target   = case when f_up_date<camp_start_date or f_up_date>camp_end_date then 0 else f_up_op_target end,
     f_up_op_other    = case when f_up_date<camp_start_date or f_up_date>camp_end_date then 0 else f_up_op_other end,
     f_up_desc        = case when f_up_date<camp_start_date or f_up_date>camp_end_date then null else f_up_desc end,
     f_up_offer_id    = case when f_up_date<camp_start_date or f_up_date>camp_end_date then null else f_up_offer_id end,
     f_up_date        = case when f_up_date<camp_start_date or f_up_date>camp_end_date then null else f_up_date end,

     bb_fp          = case when bb_date<camp_start_date or bb_date>camp_end_date then 0 else bb_fp end,
     bb_op_target   = case when bb_date<camp_start_date or bb_date>camp_end_date then 0 else bb_op_target end,
     bb_op_other    = case when bb_date<camp_start_date or bb_date>camp_end_date then 0 else bb_op_other end,
     bb_desc        = case when bb_date<camp_start_date or bb_date>camp_end_date then null else bb_desc end,
     bb_offer_id    = case when bb_date<camp_start_date or bb_date>camp_end_date then null else bb_offer_id end,
     bb_date        = case when bb_date<camp_start_date or bb_date>camp_end_date then null else bb_date end,

     f_re_fp          = case when f_re_date<camp_start_date or f_re_date>camp_end_date then 0 else f_re_fp end,
     f_re_op_target   = case when f_re_date<camp_start_date or f_re_date>camp_end_date then 0 else f_re_op_target end,
     f_re_op_other    = case when f_re_date<camp_start_date or f_re_date>camp_end_date then 0 else f_re_op_other end,
     f_re_desc        = case when f_re_date<camp_start_date or f_re_date>camp_end_date then null else f_re_desc end,
     f_re_offer_id    = case when f_re_date<camp_start_date or f_re_date>camp_end_date then null else f_re_offer_id end,
     f_re_date        = case when f_re_date<camp_start_date or f_re_date>camp_end_date then null else f_re_date end

 where segment_master in('B -  BAU vs CMO - BAU','B -  BAU vs CMO - CMO','B - 720K Offer Test','B -  BAU vs CMO - Control')
   and campaign_type is not null

--Set responses to 0 if they occur before or after the start and end dates for the control groups
update measure2
 set sports_fp          = case when sports_date<@start_control or sports_date>@end_control then 0 else sports_fp end,
     sports_op_target   = case when sports_date<@start_control or sports_date>@end_control then 0 else sports_op_target end,
     sports_op_other    = case when sports_date<@start_control or sports_date>@end_control then 0 else sports_op_other end,
     sports_desc        = case when sports_date<@start_control or sports_date>@end_control then null else sports_desc end,
     sports_offer_id    = case when sports_date<@start_control or sports_date>@end_control then null else sports_offer_id end,
     sports_date        = case when sports_date<@start_control or sports_date>@end_control then null else sports_date end,

     movies_fp          = case when movies_date<@start_control or movies_date>@end_control then 0 else movies_fp end,
     movies_op_target   = case when movies_date<@start_control or movies_date>@end_control then 0 else movies_op_target end,
     movies_op_other    = case when movies_date<@start_control or movies_date>@end_control then 0 else movies_op_other end,
     movies_desc        = case when movies_date<@start_control or movies_date>@end_control then null else movies_desc end,
     movies_offer_id    = case when movies_date<@start_control or movies_date>@end_control then null else movies_offer_id end,
     movies_date        = case when movies_date<@start_control or movies_date>@end_control then null else movies_date end,

     tt_fp          = case when tt_date<@start_control or tt_date>@end_control then 0 else tt_fp end,
     tt_op_target   = case when tt_date<@start_control or tt_date>@end_control then 0 else tt_op_target end,
     tt_op_other    = case when tt_date<@start_control or tt_date>@end_control then 0 else tt_op_other end,
     tt_desc        = case when tt_date<@start_control or tt_date>@end_control then null else tt_desc end,
     tt_offer_id    = case when tt_date<@start_control or tt_date>@end_control then null else tt_offer_id end,
     tt_date        = case when tt_date<@start_control or tt_date>@end_control then null else tt_date end,

     family_fp          = case when family_date<@start_control or family_date>@end_control then 0 else family_fp end,
     family_op_target   = case when family_date<@start_control or family_date>@end_control then 0 else family_op_target end,
     family_op_other    = case when family_date<@start_control or family_date>@end_control then 0 else family_op_other end,
     family_desc        = case when family_date<@start_control or family_date>@end_control then null else family_desc end,
     family_offer_id    = case when family_date<@start_control or family_date>@end_control then null else family_offer_id end,
     family_date        = case when family_date<@start_control or family_date>@end_control then null else family_date end,

     ms_fp          = case when ms_date<@start_control or ms_date>@end_control then 0 else ms_fp end,
     ms_op_target   = case when ms_date<@start_control or ms_date>@end_control then 0 else ms_op_target end,
     ms_op_other    = case when ms_date<@start_control or ms_date>@end_control then 0 else ms_op_other end,
     ms_desc        = case when ms_date<@start_control or ms_date>@end_control then null else ms_desc end,
     ms_offer_id    = case when ms_date<@start_control or ms_date>@end_control then null else ms_offer_id end,
     ms_date        = case when ms_date<@start_control or ms_date>@end_control then null else ms_date end,

     sge_fp          = case when sge_date<@start_control or sge_date>@end_control then 0 else sge_fp end,
     sge_op_target   = case when sge_date<@start_control or sge_date>@end_control then 0 else sge_op_target end,
     sge_op_other    = case when sge_date<@start_control or sge_date>@end_control then 0 else sge_op_other end,
     sge_desc        = case when sge_date<@start_control or sge_date>@end_control then null else sge_desc end,
     sge_offer_id    = case when sge_date<@start_control or sge_date>@end_control then null else sge_offer_id end,
     sge_date        = case when sge_date<@start_control or sge_date>@end_control then null else sge_date end,

     f_up_fp          = case when f_up_date<@start_control or f_up_date>@end_control then 0 else f_up_fp end,
     f_up_op_target   = case when f_up_date<@start_control or f_up_date>@end_control then 0 else f_up_op_target end,
     f_up_op_other    = case when f_up_date<@start_control or f_up_date>@end_control then 0 else f_up_op_other end,
     f_up_desc        = case when f_up_date<@start_control or f_up_date>@end_control then null else f_up_desc end,
     f_up_offer_id    = case when f_up_date<@start_control or f_up_date>@end_control then null else f_up_offer_id end,
     f_up_date        = case when f_up_date<@start_control or f_up_date>@end_control then null else f_up_date end,

     bb_fp          = case when bb_date<@start_control or bb_date>@end_control then 0 else bb_fp end,
     bb_op_target   = case when bb_date<@start_control or bb_date>@end_control then 0 else bb_op_target end,
     bb_op_other    = case when bb_date<@start_control or bb_date>@end_control then 0 else bb_op_other end,
     bb_desc        = case when bb_date<@start_control or bb_date>@end_control then null else bb_desc end,
     bb_offer_id    = case when bb_date<@start_control or bb_date>@end_control then null else bb_offer_id end,
     bb_date        = case when bb_date<@start_control or bb_date>@end_control then null else bb_date end,

     f_re_fp          = case when f_re_date<@start_control or f_re_date>@end_control then 0 else f_re_fp end,
     f_re_op_target   = case when f_re_date<@start_control or f_re_date>@end_control then 0 else f_re_op_target end,
     f_re_op_other    = case when f_re_date<@start_control or f_re_date>@end_control then 0 else f_re_op_other end,
     f_re_desc        = case when f_re_date<@start_control or f_re_date>@end_control then null else f_re_desc end,
     f_re_offer_id    = case when f_re_date<@start_control or f_re_date>@end_control then null else f_re_offer_id end,
     f_re_date        = case when f_re_date<@start_control or f_re_date>@end_control then null else f_re_date end

 where segment_master in('B -  BAU vs CMO - BAU','B -  BAU vs CMO - CMO','B - 720K Offer Test','B -  BAU vs CMO - Control')
   and campaign_type is null


-- RESULTS 1
IF object_id ('cmo_results1') IS NOT NULL
BEGIN
    DROP TABLE cmo_results1
END
select segment_master, campaign_type, campaign_cell,
       sum(family_fp)   as family_fp,           sum(family_op_target) as family_op_target,
       sum(sports_fp)   as sports_fp,           sum(sports_op_target) as sports_op_target,
       sum(movies_fp)   as movies_fp,           sum(movies_op_target) as movies_op_target,
       sum(TT_fp)       as TT_fp,               sum(TT_op_target)     as TT_op_target,
       sum(bb_fp)       as bb_fp,               sum(bb_op_target)     as bb_op_target,
       sum(f_up_fp)     as f_up_fp,             sum(f_up_op_target)   as f_up_op_target,
       sum(f_re_fp)     as fre_fp,              sum(f_re_op_target)   as f_re_op_target,
       sum(sge_fp)      as sge_fp,              sum(sge_op_target)    as sge_op_target,
       sum(ms_fp)       as ms_fp,               sum(ms_op_target)     as ms_op_target,
       count(*) as volume
into --drop table
        cmo_results1
from measure2
 group by segment_master, campaign_type, campaign_cell

grant all on cmo_results1 to public


-- RESULTS 2
IF object_id ('cmo_results2') IS NOT NULL
BEGIN
    DROP TABLE cmo_results2
END
select segment_master, campaign_type, campaign_cell,
       family_desc, sports_Desc, movies_desc, tt_desc, bb_desc, f_up_desc, f_re_desc, sge_desc, ms_desc,
              count(*) as volume
into --drop table
        cmo_results2
from measure2 where segment_master in('B -  BAU vs CMO - BAU','B -  BAU vs CMO - CMO','B - 720K Offer Test')
 group by segment_master, campaign_type, campaign_cell,
       family_desc, sports_Desc, movies_desc, tt_desc, bb_desc, f_up_desc, f_re_desc, sge_desc, ms_desc

grant all on cmo_results2 to public



-- RESULTS 3
IF object_id ('cmo_results3') IS NOT NULL
BEGIN
    DROP TABLE cmo_results3
END
select segment_master, campaign_type, campaign_cell,
        decile_fp_sge, decile_op_sge, decile_uplift_sge,
        decile_fp_multiscreen, decile_op_multiscreen,  decile_uplift_multiscreen,
        decile_fp_sports , decile_op_sports , decile_uplift_sports ,
        decile_fp_movies , decile_op_movies , decile_uplift_movies ,
        decile_fp_toptier , decile_op_toptier , decile_uplift_toptier ,
        decile_fp_family , decile_op_family , decile_uplift_family ,
        decile_fp_bb , decile_op_bb , decile_uplift_bb ,
        decile_fp_f_up , decile_op_f_up , decile_uplift_f_up ,
        decile_fp_f_re , decile_op_f_re , decile_uplift_f_re ,
       sum(family_fp)   as family_fp,           sum(family_op_target) as family_op_target,
       sum(sports_fp)   as sports_fp,           sum(sports_op_target) as sports_op_target,
       sum(movies_fp)   as movies_fp,           sum(movies_op_target) as movies_op_target,
       sum(TT_fp)       as TT_fp,               sum(TT_op_target)     as TT_op_target,
       sum(bb_fp)       as bb_fp,               sum(bb_op_target)     as bb_op_target,
       sum(f_up_fp)     as f_up_fp,             sum(f_up_op_target)   as f_up_op_target,
       sum(f_re_fp)     as fre_fp,              sum(f_re_op_target)   as f_re_op_target,
       sum(sge_fp)      as sge_fp,              sum(sge_op_target)    as sge_op_target,
       sum(ms_fp)       as ms_fp,               sum(ms_op_target)     as ms_op_target,
       count(*) as volume
into --drop table
        cmo_results3
from measure2 where segment_master in('B -  BAU vs CMO - CMO')
 group by segment_master, campaign_type, campaign_cell,
        decile_fp_sge, decile_op_sge, decile_uplift_sge,
        decile_fp_multiscreen, decile_op_multiscreen,  decile_uplift_multiscreen,
        decile_fp_sports , decile_op_sports , decile_uplift_sports ,
        decile_fp_movies , decile_op_movies , decile_uplift_movies ,
        decile_fp_toptier , decile_op_toptier , decile_uplift_toptier ,
        decile_fp_family , decile_op_family , decile_uplift_family ,
        decile_fp_bb , decile_op_bb , decile_uplift_bb ,
        decile_fp_f_up , decile_op_f_up , decile_uplift_f_up ,
        decile_fp_f_re , decile_op_f_re , decile_uplift_f_re

grant all on cmo_results3 to public


-- RESULTS 4
IF object_id ('cmo_results4') IS NOT NULL
BEGIN
    DROP TABLE cmo_results4
END
select segment_master, campaign_type, campaign_cell,
        decile_movies_uplift_old, decile_sports_uplift_old, decile_tt_uplift_old, decile_bb_uplift_old, decile_f_up_old,
       sum(sports_fp)   as sports_fp,           sum(sports_op_target) as sports_op_target,
       sum(movies_fp)   as movies_fp,           sum(movies_op_target) as movies_op_target,
       sum(TT_fp)       as TT_fp,               sum(TT_op_target)     as TT_op_target,
       sum(bb_fp)       as bb_fp,               sum(bb_op_target)     as bb_op_target,
       sum(f_up_fp)     as f_up_fp,             sum(f_up_op_target)   as f_up_op_target,
       sum(f_re_fp)     as fre_fp,              sum(f_re_op_target)   as f_re_op_target,
       count(*) as volume
into --drop table
        cmo_results4
from measure2 where segment_master in('B -  BAU vs CMO - BAU')
 group by segment_master, campaign_type, campaign_cell,
        decile_movies_uplift_old, decile_sports_uplift_old, decile_tt_uplift_old, decile_bb_uplift_old, decile_f_up_old

grant all on cmo_results4 to public



-- RESULTS QA
IF object_id ('cmo_results_QA') IS NOT NULL
BEGIN
    DROP TABLE cmo_results_QA
END
select segment, campaign_type, camp_start_date, camp_end_date, count(*),
       sports_date, movies_date, tt_date, family_date, ms_date, sge_date, bb_date, f_up_date, f_re_date
into --drop table
        cmo_results_QA
from measure2 where segment_master in('B -  BAU vs CMO - BAU','B -  BAU vs CMO - CMO','B - 720K Offer Test','B -  BAU vs CMO - Control')
 group by segment, campaign_type, camp_start_date, camp_end_date,
     sports_date, movies_date, tt_date, family_date, ms_date, sge_date, bb_date, f_up_date, f_re_date

grant all on cmo_results_QA to public

select * from cmo_results_QA

--------------------------------------
-- Classify Offers as True / Other  --
--------------------------------------

update measure2
 set sports_op_other = case
  when campaign_Cell in('BAU','CMO') and sports_desc ='Sports Half Price for 6 Months when Upgrading from Base - Existing UK Customers' then 1
  when campaign_Cell='Offer 1 - 6MHP             ' and sports_desc ='Sports Half Price for 6 Months when Upgrading from Base - Existing UK Customers' then 1
  when campaign_Cell='Offer 2 - ELP�12           ' and sports_desc ='Sports at 12 GBP for 12 Months upgrading from Base Pack - Existing UK Customers' then 1
  when campaign_Cell='Offer 3 - ELP�15           ' and sports_desc ='Sports at 15 GBP for 12 Months upgrading from Base Pack - Existing UK Customers' then 1
  else 0 end, --when sports_desc is not null then 1 else 0 end,

     movies_op_other = case
  when campaign_Cell in('BAU','CMO') and movies_desc in('Movies 50% Off for 6 Months When Upgrading from Base Pack - Existing UK Customers','Movies Half Price Upgrade for 6 Months - UK Customer') then 1
  when campaign_cell ='Offer 1 - 6MHP             ' and movies_desc in('Movies 50% Off for 6 Months When Upgrading from Base Pack - Existing UK Customers','Movies Half Price Upgrade for 6 Months - UK Customer') then 1
  when campaign_cell ='Offer 2 - ELP�8            ' and movies_desc ='Movies 8 GBP for 12 Months when Upgrading from Base - Existing UK Customers' then 1
  when campaign_cell ='Offer 3 - ELP�10           ' and movies_desc ='Sky Movies 10GBP for 12 months - Existing UK Customer' then 1
  else 0 end, --when movies_desc is not null then 1 else 0 end,

     tt_op_other = case
  when campaign_Cell in('BAU','CMO') and tt_desc
  in('Sports and Movies 50% Off for 6 Months when Upgrading from Base - Existing UK Customers',
     'Sports and Movies Half Price for 6 Months when Upgrading from Base - Existing UK Customers',
     'Sports and Movies for 12.25 GBP for 6 Months when Upgrading from Base - Existing UK Customers') then 1
  when campaign_cell ='Offer 1 - 6MHP (Basic)     ' and tt_desc
  in('Sports and Movies 50% Off for 6 Months when Upgrading from Base - Existing UK Customers',
     'Sports and Movies Half Price for 6 Months when Upgrading from Base - Existing UK Customers',
     'Sports and Movies for 12.25 GBP for 6 Months when Upgrading from Base - Existing UK Customers') then 1
 -- when campaign_cell ='Offer 2 - �9.25 for DS (DM)' and tt_desc ='' then 1
 -- when campaign_cell ='Offer 3 - �4.50 for DM (DS)' and tt_desc ='' then 1
  else 0 end, --when tt_desc is not null then 1 else 0 end,

 family_op_other = case
  when campaign_Cell in('BAU','CMO') and family_desc
  in('Upgrade Cost from Variety to Family 50% Off for 6 Months - Existing UK Customers') then 1
  when campaign_cell='Offer 1 - 6MHP             ' and family_desc='Upgrade Cost from Variety to Family 50% Off for 6 Months - Existing UK Customers' then 1
  when campaign_cell='Offer 2 - 12MHP            ' and family_desc='Upgrade Cost from Variety to Family 50% Off for 12 Months - Existing UK Customers' then 1
  else 0 end, --when family_desc is not null then 1 else 0 end,

 ms_op_other = case
   when campaign_Cell in('BAU','CMO') and ms_desc
    in ('Sky Multiscreen 50% Off for 6 Months - Existing UK Customers') then 1
   when campaign_cell ='Offer 1 - 6MHP             ' and ms_desc='Sky Multiscreen 50% Off for 6 Months - Existing UK Customers' then 1
   when campaign_cell ='Offer 2 - 12MHP            ' and ms_desc='Sky Multiscreen 50% Off for 12 Months - Existing UK Customers' then 1
   else 0 end, --when ms_desc is not null then 1 else 0 end,

 bb_op_other = case
   when campaign_Cell in('BAU','CMO') and bb_desc
    in('Broadband Unlimited Free for 12 Months When Ordering Line Rental - Existing Customers UK (Auto)',
       'Broadband Unlimited Free for 12 Months - UK Customer',
       'Broadband Unlimited Free For 12 Months - Existing UK Customers(Recontract)') then 1
    when campaign_cell='Offer 1 - 12MFREE          ' and bb_desc
    in('Broadband Unlimited Free for 12 Months When Ordering Line Rental - Existing Customers UK (Auto)',
       'Broadband Unlimited Free for 12 Months - UK Customer',
       'Broadband Unlimited Free For 12 Months - Existing UK Customers(Recontract)') then 1
    when campaign_cell='Offer 2 - 12MHP            ' and bb_desc
    in('Broadband Unlimited Half Price for 12 Months with Sky TV and Line Rental - Existing UK Customers',
       'Broadband Unlimited 50% Off for 12 Months with Sky TV and Line Rental - Existing UK Customers') then 1
    else 0 end, --when bb_desc is not null then 1 else 0 end,

 f_up_op_other = case
   when campaign_Cell in('BAU','CMO') and f_up_desc
    in('Sky Fibre Free for 12 Months with Line Rental - Existing UK Customers') then 1
    when campaign_cell='Offer 1 - 12MFREE CAP      ' and f_up_desc
    in('Sky Fibre Free for 12 Months with Line Rental - Existing UK Customers') then 1
    when campaign_cell='Offer 2 - 6MFREE UNLIM     ' and f_up_desc
    in('Fibre Unlimited Free for 6 Months with Sky TV and Line Rental - Existing UK Customers') then 1
    else 0 end, --when f_up_desc is not null then 1 else 0 end,

 f_re_op_other = case
   when campaign_Cell in('BAU','CMO') and f_re_desc
    in('Sky Fibre Free for 12 Months with Line Rental - Existing UK Customers') then 1
    when campaign_cell='Offer 1 - 12MFREE CAP      ' and f_re_desc
    in('Sky Fibre Free for 12 Months with Line Rental - Existing UK Customers') then 1
    when campaign_cell='Offer 2 - 6MFREE UNLIM     ' and f_re_desc
    in('Fibre Unlimited Free for 6 Months with Sky TV and Line Rental - Existing UK Customers') then 1
    else 0 end  --when f_re_desc is not null then 1 else 0 end


update measure2
 set sports_op_target = case when sports_op_other=1 then 0 else sports_op_target end,
     movies_op_target = case when movies_op_other=1 then 0 else movies_op_target end,
     tt_op_target     = case when tt_op_other=1 then 0 else tt_op_target end,
     family_op_target = case when family_op_other=1 then 0 else family_op_target end,
     ms_op_target     = case when ms_op_other=1 then 0 else ms_op_target end,
     bb_op_target     = case when bb_op_other=1 then 0 else bb_op_target end,
     f_up_op_target   = case when f_up_op_other=1 then 0 else f_up_op_target end,
     f_re_op_target   = case when f_re_op_other=1 then 0 else f_re_op_target end



-- RESULTS 5
IF object_id ('cmo_results5') IS NOT NULL
BEGIN
    DROP TABLE cmo_results5
END
select segment_master, campaign_type, campaign_cell,
       sum(sports_fp)   as sports_fp,           sum(sports_op_target) as sports_op_target,   sum(sports_op_other) as sports_op_other,
       sum(movies_fp)   as movies_fp,           sum(movies_op_target) as movies_op_target,   sum(movies_op_other) as movies_op_other,
       sum(TT_fp)       as TT_fp,               sum(TT_op_target)     as TT_op_target,       sum(tt_op_other) as tt_op_other,
       sum(family_fp)   as family_fp,           sum(family_op_target) as family_op_target,   sum(family_op_other) as family_op_other,
       sum(ms_fp)       as ms_fp,               sum(ms_op_target)     as ms_op_target,       sum(ms_op_other) as ms_op_other,
       sum(bb_fp)       as bb_fp,               sum(bb_op_target)     as bb_op_target,       sum(bb_op_other) as bb_op_other,
       sum(f_up_fp)     as f_up_fp,             sum(f_up_op_target)   as f_up_op_target,     sum(f_up_op_other) as f_up_op_other,
       sum(f_re_fp)     as fre_fp,              sum(f_re_op_target)   as f_re_op_target,     sum(f_re_op_other) as f_re_op_other,
       count(*) as volume
into --drop table
        cmo_results5
from measure2 where segment_master in('B -  BAU vs CMO - BAU','B -  BAU vs CMO - CMO','B - 720K Offer Test')
 group by segment_master, campaign_type, campaign_cell

grant all on cmo_results5 to public
grant all on measure2 to public





--
/*
select top 10 * from measure2

select count(*) from measure2 where segment_master = 'B -  BAU vs CMO - CMO' and campaign_type is null

select segment_master, campaign_type, count(*)
from measure2 where segment_master = 'B -  BAU vs CMO - CMO'

select top 10 * from simmonsr.CMO_BAU_TEST_MEASUREMENT_1415Q4_v2

select a.account_number, a.segment_master, a.campaign_type, sports, movies, bb, bb_fibre, pack_family,
        skytalk, lr, sge, ms, onnet, fibrearea_99, offers_dtv, offers_bb
into --drop table
                #bingo
from measure2 a
 left join simmonsr.CMO_BAU_TEST_MEASUREMENT_1415Q4_v2 b
  on a.account_number=b.account_number
   where segment_master in('B -  BAU vs CMO - BAU','B -  BAU vs CMO - CMO','B -  BAU vs CMO - Control')

select segment_master, campaign_type, sports, movies, bb, bb_fibre, pack_family,
        skytalk, lr, sge, ms, onnet, fibrearea_99, offers_dtv, offers_bb, count(*)
from #bingo
group by segment_master, campaign_type, sports, movies, bb, bb_fibre, pack_family,
        skytalk, lr, sge, ms, onnet, fibrearea_99, offers_dtv, offers_bb

*/
