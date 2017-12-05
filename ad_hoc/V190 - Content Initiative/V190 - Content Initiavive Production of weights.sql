/*------------------------------------------------------------------------------------------------------------------
        Project:    V190-CONTENT INITIATIVE
        Program:    Generate Weight Values from Vespa to UK Base
        Version:    1
        Created:    20130621
        Lead:       SUSANNE CHAN
        Analyst:    Dan Barnett
        SK Prod:    5
        QA:         
------------------------------------------------------------------------------------------------------------------*/


---Part 1--Get details of all DTV Accounts Active as at start 1st March 2013----
----Get Account status at 1st Feb 2013  and 1st May 2013
select
                account_number
into            #v190_all_active_accounts
FROM            sk_prod.cust_subs_hist
WHERE          subscription_sub_type = 'DTV Primary Viewing'
and  effective_from_dt< '2013-03-01' and effective_to_dt>='2013-03-01' and status_code = 'AC'
group by account_number
;
Commit;

select * into v190_all_active_accounts from #v190_all_active_accounts; commit;

create hg index idx1 on v190_all_active_accounts(account_number);
commit;

---Create List of Eligible Vespa Accounts----
--drop table #nov_2012_viewing;
select      account_number
            ,count(distinct(cast(viewing_starts as date))) as no_days_returned   
into        #nov_2012_viewing
from        mawbya.V190_viewing_data_table_201211
group by    account_number
;


select      account_number
            ,count(distinct(cast(viewing_starts as date))) as no_days_returned   
into        #dec_2012_viewing
from        mawbya.V190_viewing_data_table_201212
group by    account_number
;


select      account_number
            ,count(distinct(cast(viewing_starts as date))) as no_days_returned   
into        #Jan_2013_viewing
from        mawbya.V190_viewing_data_table_201301
group by    account_number
;


select      account_number
            ,count(distinct(cast(viewing_starts as date))) as no_days_returned   
into        #Feb_2013_viewing
from        mawbya.V190_viewing_data_table_201302
group by    account_number
;

commit;


create  hg index idx1 on #Nov_2012_viewing(account_number);
create  hg index idx1 on #Dec_2012_viewing(account_number);
create  hg index idx1 on #Jan_2013_viewing(account_number);
create  hg index idx1 on #Feb_2013_viewing(account_number);
commit;

--drop table v190_all_active_accounts;
----Add Days viewing per month on to Base Table---


Commit;
alter table v190_all_active_accounts add nov_2012_days_returned integer default 0;
alter table v190_all_active_accounts add dec_2012_days_returned integer default 0;
alter table v190_all_active_accounts add jan_2013_days_returned integer default 0;
alter table v190_all_active_accounts add feb_2013_days_returned integer default 0;

commit;

update v190_all_active_accounts
set nov_2012_days_returned = case when b.no_days_returned is null then 0 else no_days_returned end
from v190_all_active_accounts as a
left outer join  #nov_2012_viewing as b
on a.account_number = b.account_number
;


update v190_all_active_accounts
set dec_2012_days_returned = case when b.no_days_returned is null then 0 else no_days_returned end
from v190_all_active_accounts as a
left outer join  #dec_2012_viewing as b
on a.account_number = b.account_number
;


update v190_all_active_accounts
set jan_2013_days_returned = case when b.no_days_returned is null then 0 else no_days_returned end
from v190_all_active_accounts as a
left outer join  #jan_2013_viewing as b
on a.account_number = b.account_number
;


update v190_all_active_accounts
set feb_2013_days_returned = case when b.no_days_returned is null then 0 else no_days_returned end
from v190_all_active_accounts as a
left outer join  #feb_2013_viewing as b
on a.account_number = b.account_number
;
commit;


alter table v190_all_active_accounts add eligible_vespa_analysis_account integer default 0;

update v190_all_active_accounts
set eligible_vespa_analysis_account = case when nov_2012_days_returned >=14 and dec_2012_days_returned >=14 and jan_2013_days_returned >=14 and feb_2013_days_returned >=14
then 1 else 0 end
from v190_all_active_accounts as a
;
commit;


alter table  v190_all_active_accounts add activation_date date;
update v190_all_active_accounts
set  activation_date =b.ph_subs_first_activation_dt
from v190_all_active_accounts  as a
left outer join sk_prod.cust_single_account_view as b
on a.account_number = b.account_number
;
commit;

alter table v190_all_active_accounts add full_months_tenure integer;

update v190_all_active_accounts
set full_months_tenure=  case when cast(dateformat(activation_date,'DD') as integer)>1 then 
     datediff(mm,activation_date,cast('2013-03-01' as date))-1 else datediff(mm,activation_date,cast('2013-03-01' as date)) end 
from v190_all_active_accounts
;
commit;


alter table  v190_all_active_accounts add country_code varchar(3);
update v190_all_active_accounts
set  country_code =b.pty_country_code
from v190_all_active_accounts  as a
left outer join sk_prod.cust_single_account_view as b
on a.account_number = b.account_number
;
commit;

alter table  v190_all_active_accounts add acct_type varchar(10);
update v190_all_active_accounts
set  acct_type =b.acct_type
from v190_all_active_accounts  as a
left outer join sk_prod.cust_single_account_view as b
on a.account_number = b.account_number
;
commit;

Delete from  v190_all_active_accounts where acct_type<>'Standard'; commit;
Delete from  v190_all_active_accounts where country_code not in ('GBR'); commit;
Delete from  v190_all_active_accounts where country_code is null; commit;

commit;






/*
select full_months_tenure
,count(*) as accounts
,sum(eligible_vespa_analysis_account) as vespa_analysis_accounts
,sum(case when feb_2013_days_returned>0 then 1 else 0 end) as any_feb_viewing
from v190_all_active_accounts
group by full_months_tenure
order by full_months_tenure
;

commit;
*

--select top 5000 * from v190_all_active_accounts where activation_date<='2013-02-02' order by activation_date desc;


