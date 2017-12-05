---XBOX Sky Go Usage on vespa and non-vespa panels

select  account_number ,max(case when site_name = 'XBOX' then 1 else 0 end) as xbox_user 
into #xbox_accounts from sk_prod.SKY_PLAYER_USAGE_DETAIL 
where  activity_dt between '2012-08-12' and '2012-11-11' group by account_number;
--select sum(xbox_user),count(*) from #xbox_accounts
select account_number 
into #active_accounts
from sk_prod.cust_single_account_view as a
where acct_type='Standard' and account_number <>'?' and pty_country_code is not null
and ph_subs_status_code in ('AC','AB','PC')
;

--select site_name ,count(*)  from sk_prod.SKY_PLAYER_USAGE_DETAIL where  activity_dt between '2012-08-12' and '2012-11-11' group by site_name ;
select account_number 
into #vespa_accounts 
from sk_prod.vespa_events_all 
where event_start_date_time_utc 
 between '2012-08-12' and '2012-11-11' group by account_number;
commit;
---Create Index---

create  hg index idx1 on #xbox_accounts(account_number);

create  hg index idx1 on #active_accounts(account_number);

create  hg index idx1 on #vespa_accounts (account_number);
commit;

commit;

---Create Overall Counts
select count(a.account_number) as active_accounts
,case when b.account_number is not null then 1 else 0 end as vespa_accounts
,case when c.account_number is not null then 1 else 0 end as sky_go_user
,case when c.xbox_user =1 then 1 else 0 end as sky_go_xbox_user
from #active_accounts as a
left outer join #vespa_accounts as b
on a.account_number = b.account_number
left outer join #xbox_accounts as c
on a.account_number = c.account_number
group by vespa_accounts
,sky_go_user
,sky_go_xbox_user
;


