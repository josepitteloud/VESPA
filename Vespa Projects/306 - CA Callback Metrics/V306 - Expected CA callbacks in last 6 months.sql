-------------------------------------------------------------------------------------------------------------------------------------------------
-- Get callback day per account/subscriber
-------------------------------------------------------------------------------------------------------------------------------------------------
drop table #csi;
select	--top 20
		account_number
	,	si_external_identifier
    ,	si_callback_day
into	#csi
from	CUST_SERVICE_INSTANCE
where	effective_to_dt	=	'9999-09-09'	-- Isolate to active records
group by
		account_number
	,	si_external_identifier
    ,	si_callback_day
;
create hg index idx1 on #csi(account_number);
create hg index idx2 on #csi(si_external_identifier);
create lf index idx3 on #csi(si_callback_day);

select top 20 * from #csi;


-------------------------------------------------------------------------------------------------------------------------------------------------
-- Get tenure
-------------------------------------------------------------------------------------------------------------------------------------------------
drop table #acc;
select
		account_number
	,	max(acct_first_account_activation_dt)	max_acct_first_account_activation_dt	-- Most recent activation datetime per account
    ,   case								-- First-pass logic to estimate number of expected callbacks per account
            when datediff(month,acct_first_account_activation_dt,getdate()) <= 0 then 0
            when datediff(month,acct_first_account_activation_dt,getdate()) <= 1 then 1
            when datediff(month,acct_first_account_activation_dt,getdate()) <= 2 then 2
            when datediff(month,acct_first_account_activation_dt,getdate()) <= 3 then 3
            when datediff(month,acct_first_account_activation_dt,getdate()) <= 4 then 4
            when datediff(month,acct_first_account_activation_dt,getdate()) <= 5 then 5
            else 6
        end as  ExpectedCbcks
into	#acc
from	CUST_SINGLE_ACCOUNT_VIEW
where	cust_active_dtv	=	1		-- Filter for active customer accounts (maybe remove this in final implementation?)
group by
		account_number
	,	ExpectedCbcks
;

create unique hg index idx1 on #acc(account_number);
create date index idx2 on #acc(max_acct_first_account_activation_dt);
create lf index idx3 on #acc(ExpectedCbcks);

select top 20 * from #acc;


-------------------------------------------------------------------------------------------------------------------------------------------------
-- Generate date vector
-------------------------------------------------------------------------------------------------------------------------------------------------
select	calendar_date	as	dt
into	#cal
from	SKY_CALENDAR
where	dt	between	getdate() - 180
			and		getdate()
;
create date index idx1 on #cal(dt);

select top 20 * from #cal;


-------------------------------------------------------------------------------------------------------------------------------------------------
-- Join and calculate the true expected number of monthly callbacks within the last 180 days-------------------------------------------------------------------------------------------------------------------------------------------------
select	-- top 100
		a.account_number
	,	a.si_external_identifier
    ,	a.si_callback_day
	,	b.max_acct_first_account_activation_dt
	,	b.ExpectedCbcks									-- Initial estimate (no need to keep this)
    ,	count(distinct c.dt)	as	x_ExpectedCbck		-- True number of expected callbacks
from
				#csi	a
	inner join	#acc	b	on	a.account_number	=	b.account_number
    cross join	#cal	c
where
		datepart(day,c.dt)						=	a.si_callback_day
    and	c.dt									>=	b.max_acct_first_account_activation_dt
	-- and	b.max_acct_first_account_activation_dt	>=	today() - 180
group by
		a.account_number
	,	a.si_external_identifier
    ,	a.si_callback_day
	,	b.max_acct_first_account_activation_dt
	,	b.ExpectedCbcks
;
