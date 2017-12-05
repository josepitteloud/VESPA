text
create procedure pitteloudj.SABB_my_sky_login_prob as
begin
  message convert(timestamp,now()) || ' | SABB_my_sky_login_prob - Initialization begin ' to client
  declare @mx_dt date
  set @mx_dt = (select max(end_date) from cust_fcast_weekly_base_2)
  if exists(select tname from syscatalog where creator = USER_NAME() and UPPER(tname) = UPPER('SABB_my_sky_login_prob_TABLE') and tabletype = 'TABLE')
    drop table pitteloudj.SABB_my_sky_login_prob_TABLE
  select account_number,
    'segment_sa'=convert(varchar(20),null),
    'curr_count'=MAX(case when end_date = @mx_dt then my_sky_login_3m_raw else 0 end), -- my_sky_login_3m_raw on the current week
    'count_1w'=MAX(case when end_date = DATEADD(day,-7,@mx_dt) then my_sky_login_3m_raw else 0 end), -- -- my_sky_login_3m_raw on the Previous week
    'Calls_LW'=case when curr_count < count_1w then 0 else curr_count-count_1w end, -- Logins made last week
    'L_12'=MAX(case when end_date between DATEADD(month,-12,@mx_dt) and DATEADD(week,-1,@mx_dt) then my_sky_login_3m_raw else 0 end), -- Max Login in the past 12 month
    'L_9'=MAX(case when end_date between DATEADD(month,-9,@mx_dt) and DATEADD(week,-1,@mx_dt) then my_sky_login_3m_raw else 0 end), -- Max Login in the past 9 month
    'L_6'=MAX(case when end_date between DATEADD(month,-6,@mx_dt) and DATEADD(week,-1,@mx_dt) then my_sky_login_3m_raw else 0 end), -- Max Login in the past 6 month
    'L_3'=MAX(case when end_date between DATEADD(month,-3,@mx_dt) and DATEADD(week,-1,@mx_dt) then my_sky_login_3m_raw else 0 end), -- Max Login in the past 3 month
    'Prob_Group'=case when L_12 = 0 then 1
    when L_9 = 0 then 2
    when L_6 = 0 then 3
    when L_3 = 0 then 4
    else 5 -- Picking the longest Group
    end into #mysky_prob_1
    from cust_fcast_weekly_base_2
    where bb_active = 1 and dtv_active = 0
    and account_number = any(select distinct account_number from FORECAST_Base_Sample)
    and BB_latest_act_dt is not null
    group by account_number
  commit work
  create hg index id1 on #mysky_prob_1(account_number)
  commit work
  update #mysky_prob_1 as a
    set a.segment_sa = b.segment_sa from
    #mysky_prob_1 as a
    join DTV_fcast_weekly_base_2 as b on a.account_number = b.account_number and b.end_Date = @mx_dt
  commit work
  select 'Prob_Group'=1,
    Calls_LW,
    segment_sa,
    'hits'=COUNT(1),
    't_segment'=SUM(hits) over(partition by segment_sa),
    'prob'=case when t_segment > 0 then convert(real,hits)/convert(real,t_segment) else 0 end,
    'rank_id'=RANK() over(partition by segment_sa order by Calls_LW asc)
    into #mysky_prob_2
    from #mysky_prob_1
    where L_12 = 0
    group by Calls_LW,
    segment_sa union
  select 'Prob_Group'=2,
    Calls_LW,
    segment_sa,
    'hits'=COUNT(1),
    't_segment'=SUM(hits) over(partition by segment_sa),
    'prob'=case when t_segment > 0 then convert(real,hits)/convert(real,t_segment) else 0 end,
    'rank_id'=RANK() over(partition by segment_sa order by Calls_LW asc)
    from #mysky_prob_1
    where L_9 = 0
    group by Calls_LW,
    segment_sa union
  select 'Prob_Group'=3,
    Calls_LW,
    segment_sa,
    'hits'=COUNT(1),
    't_segment'=SUM(hits) over(partition by segment_sa),
    'prob'=case when t_segment > 0 then convert(real,hits)/convert(real,t_segment) else 0 end,
    'rank_id'=RANK() over(partition by segment_sa order by Calls_LW asc)
    from #mysky_prob_1
    where L_6 = 0
    group by Calls_LW,
    segment_sa union
  select 'Prob_Group'=4,
    Calls_LW,
    segment_sa,
    'hits'=COUNT(1),
    't_segment'=SUM(hits) over(partition by segment_sa),
    'prob'=case when t_segment > 0 then convert(real,hits)/convert(real,t_segment) else 0 end,
    'rank_id'=RANK() over(partition by segment_sa order by Calls_LW asc)
    from #mysky_prob_1
    where L_3 = 0
    group by Calls_LW,
    segment_sa union
  select 'Prob_Group'=5,
    Calls_LW,
    segment_sa,
    'hits'=COUNT(1),
    't_segment'=SUM(hits) over(partition by segment_sa),
    'prob'=case when t_segment > 0 then convert(real,hits)/convert(real,t_segment) else 0 end,
    'rank_id'=RANK() over(partition by segment_sa order by Calls_LW asc)
    from #mysky_prob_1
    where Prob_Group = 5
    group by Calls_LW,
    segment_sa
  select *,
    'UPPER_LIMIT'=SUM(prob) over(partition by Prob_Group,segment_sa order by RANK_ID asc)
    into #mysky_prob_3
    from #mysky_prob_2
  select a.Prob_Group,
    a.Calls_LW,
    a.segment_sa,
    'Lower_limit'=COALESCE(b.UPPER_LIMIT,0),
    a.UPPER_LIMIT
    into SABB_my_sky_login_prob_TABLE
    from #mysky_prob_3 as a
      left outer join #mysky_prob_3 as b on a.segment_sa = b.segment_sa
      and a.Prob_Group = b.Prob_Group
      and a.rank_id-1 = b.rank_id
  message convert(timestamp,now()) || ' | SABB_my_sky_login_prob_TABLE - COMPLETED: ' || @@rowcount to client
  commit work
  create lf index id1 on pitteloudj.SABB_my_sky_login_prob_TABLE(Prob_Group)
  create lf index id2 on pitteloudj.SABB_my_sky_login_prob_TABLE(Calls_LW)
  create hg index id3 on pitteloudj.SABB_my_sky_login_prob_TABLE(Lower_limit)
  create hg index id4 on pitteloudj.SABB_my_sky_login_prob_TABLE(UPPER_LIMIT)
  grant select on pitteloudj.SABB_my_sky_login_prob_TABLE to citeam,vespa_group_low_security
  commit work
  message convert(timestamp,now()) || ' | SABB_my_sky_login_prob - COMPLETED ' to client
end