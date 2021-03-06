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

----Start of Part 1----
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

----End of Part 1----

----Part 2 Generate Viewing Summary Pivots---
select channel_name
,Channel_Name_Inc_Hd 
,channel_name_inc_hd_staggercast
,max(pay_channel) as pay
into #channel_lookup
from dbarnett.epg_data_phase_2
group by channel_name
,Channel_Name_Inc_Hd 
,channel_name_inc_hd_staggercast
;
commit;

create  hg index idx1 on #channel_lookup(channel_name);
commit;

select channel_name
,Channel_Name_Inc_Hd 
,channel_name_inc_hd_staggercast
, case when channel_name_inc_hd_staggercast in ('Nick Jr','Nick Jr 2') then  'Nick Jr'
when channel_name_inc_hd_staggercast in ('Sky Arts 1','Sky Arts 2') then  'Sky Arts 1 or 2'
when channel_name_inc_hd_staggercast in ('Sky 1','Sky 2') then  'Sky 1 or 2'
when channel_name_inc_hd_staggercast in ('Sky Sports 1'
,'Sky Sports 2'
,'Sky Sports 3'
,'Sky Sports 4'
,'Sky Sports F1'
,'Football First') then  'Sky Sports Channels'
when channel_name_inc_hd_staggercast in 
('Sky DramaRom'
,'Sky Movies 007'
,'Sky Movies Action'
,'Sky Movies Classics'
,'Sky Movies Comedy'
,'Sky Movies Family'
,'Sky Movies Indie'
,'Sky Movies Mdn Greats'
,'Sky Movies Sci-Fi/Horror'
,'Sky Movies Showcase'
,'Sky Movies Thriller'
,'Sky Premiere'
) then  'Sky Movies Channels'
 else  channel_name_inc_hd_staggercast end as channel_name_inc_hd_staggercast_channel_families
,pay as pay_channel
into v190_channel_lookup_with_channel_family
from #channel_lookup
;
commit;

create  hg index idx1 on v190_channel_lookup_with_channel_family(channel_name);

--drop table v190_top_x_channels_summary_for_powerpivot-Add on Groupings Created by Anthony--drop table v190_top_x_channels_summary_for_powerpivot--drop table v190_top_x_channels_summary_for_powerpivot
--drop table v190_top_x_channels_summary_for_powerpivotalter table v190_channel_lookup_with_channel_family delete channel_category ;
alter table v190_channel_lookup_with_channel_family add channel_category varchar(23);

update v190_channel_lookup_with_channel_family
set channel_category= case when b.channel_category is null then '99)UNALLOCATED' else b.channel_category end
from v190_channel_lookup_with_channel_family as a
left outer join mawbya.v190_channels_lu_am as b
on a.channel_name =b.channel_name
;


commit;
--drop table v190_top_x_channels_summary_for_powerpivotselect top 500 * from mawbya.v190_channels_lu_am;
--drop table v190_top_x_channels_summary_for_powerpivotselect top 500 * from v190_channel_lookup_with_channel_family;
--drop table v190_top_x_channels_summary_for_powerpivotselect top 500 * from mawbya.V190_viewing_data_table_201211;

select account_number
,a.channel_name
,channel_name_inc_hd_staggercast
,channel_name_inc_hd_staggercast_channel_families
,sum(viewing_duration) as total_viewing_duration
,sum(case when daypart_1 = 'd)late_peak' then viewing_duration else 0 end) as total_viewing_duration_broadcast_start_21_23_hours
,sum(case when daypart_2 = 'd)late_peak' then viewing_duration else 0 end) as total_viewing_duration_broadcast_start_20_23_hours
into #nov_2012_viewing_by_channel
from  mawbya.V190_viewing_data_table_201211 as a
left outer join v190_channel_lookup_with_channel_family as b
on a.channel_name = b.channel_name
group by account_number
,a.channel_name
,channel_name_inc_hd_staggercast
,channel_name_inc_hd_staggercast_channel_families
;
commit;


create  hg index idx1 on #nov_2012_viewing_by_channel(account_number);


--drop table v190_top_x_channels_summary_for_powerpivotDec 2012
select account_number
,a.channel_name
,channel_name_inc_hd_staggercast
,channel_name_inc_hd_staggercast_channel_families
,sum(viewing_duration) as total_viewing_duration
,sum(case when daypart_1 = 'd)late_peak' then viewing_duration else 0 end) as total_viewing_duration_broadcast_start_21_23_hours
,sum(case when daypart_2 = 'd)late_peak' then viewing_duration else 0 end) as total_viewing_duration_broadcast_start_20_23_hours
into #dec_2012_viewing_by_channel
from  mawbya.V190_viewing_data_table_201212 as a
left outer join v190_channel_lookup_with_channel_family as b
on a.channel_name = b.channel_name
group by account_number
,a.channel_name
,channel_name_inc_hd_staggercast
,channel_name_inc_hd_staggercast_channel_families
;
commit;


create  hg index idx1 on #dec_2012_viewing_by_channel(account_number);


--drop table v190_top_x_channels_summary_for_powerpivot-Jan 2013
select account_number
,a.channel_name
,channel_name_inc_hd_staggercast
,channel_name_inc_hd_staggercast_channel_families
,sum(viewing_duration) as total_viewing_duration
,sum(case when daypart_1 = 'd)late_peak' then viewing_duration else 0 end) as total_viewing_duration_broadcast_start_21_23_hours
,sum(case when daypart_2 = 'd)late_peak' then viewing_duration else 0 end) as total_viewing_duration_broadcast_start_20_23_hours
into #jan_2013_viewing_by_channel
from  mawbya.V190_viewing_data_table_201301 as a
left outer join v190_channel_lookup_with_channel_family as b
on a.channel_name = b.channel_name
group by account_number
,a.channel_name
,channel_name_inc_hd_staggercast
,channel_name_inc_hd_staggercast_channel_families
;
commit;


create  hg index idx1 on #jan_2013_viewing_by_channel(account_number);

--drop table v190_top_x_channels_summary_for_powerpivotFeb 2013
select account_number
,a.channel_name
,channel_name_inc_hd_staggercast
,channel_name_inc_hd_staggercast_channel_families
,sum(viewing_duration) as total_viewing_duration
,sum(case when daypart_1 = 'd)late_peak' then viewing_duration else 0 end) as total_viewing_duration_broadcast_start_21_23_hours
,sum(case when daypart_2 = 'd)late_peak' then viewing_duration else 0 end) as total_viewing_duration_broadcast_start_20_23_hours
into #feb_2013_viewing_by_channel
from  mawbya.V190_viewing_data_table_201302 as a
left outer join v190_channel_lookup_with_channel_family as b
on a.channel_name = b.channel_name
group by account_number
,a.channel_name
,channel_name_inc_hd_staggercast
,channel_name_inc_hd_staggercast_channel_families
;
commit;


create  hg index idx1 on #feb_2013_viewing_by_channel(account_number);

commit;
--drop table v190_top_x_channels_summary_for_powerpivotselect top 100 * from #feb_2013_viewing_by_channel;
--drop table v190_top_x_channels_summary_for_powerpivot--drop table v190_top_x_channels_summary_for_powerpivotAppend all tables into a single table to enable totals to be created--drop table v190_top_x_channels_summary_for_powerpivot-

create table v190_monthly_viewing_appends
( account_number varchar(20)
,channel_name varchar(40)
,channel_name_inc_hd_staggercast varchar(90)
,channel_name_inc_hd_staggercast_channel_families varchar(90)
,total_viewing_duration integer
,total_viewing_duration_broadcast_start_21_23_hours integer
,total_viewing_duration_broadcast_start_20_23_hours integer
);

--drop table v190_top_x_channels_summary_for_powerpivotAppend 4 monthly summaries into a single table--drop table v190_top_x_channels_summary_for_powerpivot
insert into v190_monthly_viewing_appends
select * from #dec_2012_viewing_by_channel
;
commit;

insert into v190_monthly_viewing_appends
select * from #nov_2012_viewing_by_channel
;
commit;

insert into v190_monthly_viewing_appends
select * from #jan_2013_viewing_by_channel
;
commit;

insert into v190_monthly_viewing_appends
select * from #feb_2013_viewing_by_channel
;
commit;


create  hg index idx1 on v190_monthly_viewing_appends(account_number);
create  hg index idx2 on v190_monthly_viewing_appends(channel_name_inc_hd_staggercast);
create  hg index idx3 on v190_monthly_viewing_appends(channel_name_inc_hd_staggercast_channel_families);
--drop table v190_top_x_channels_summary_for_powerpivot
--drop table v190_top_x_channels_summary_for_powerpivotdrop table v190_total_viewing_by_channel; drop table v190_total_viewing_by_channel_family;
select account_number
,channel_name_inc_hd_staggercast
,sum(total_viewing_duration) as total_duration
,sum(total_viewing_duration_broadcast_start_21_23_hours) as total_duration_broadcast_start_21_22_hours
,sum(total_viewing_duration_broadcast_start_20_23_hours) as total_duration_broadcast_start_20_22_hours
,rank() over (partition by account_number order by total_duration desc  ,channel_name_inc_hd_staggercast desc) as rank_total_duration
,rank() over (partition by account_number order by total_duration_broadcast_start_21_22_hours desc ,channel_name_inc_hd_staggercast desc) as rank_total_duration_broadcast_21_22_hours
,rank() over (partition by account_number order by total_duration_broadcast_start_20_22_hours desc ,channel_name_inc_hd_staggercast desc) as rank_total_duration_broadcast_20_22_hours
into v190_total_viewing_by_channel
from v190_monthly_viewing_appends
group by account_number
,channel_name_inc_hd_staggercast
;

select account_number
,channel_name_inc_hd_staggercast_channel_families
,sum(total_viewing_duration) as total_duration
,sum(total_viewing_duration_broadcast_start_21_23_hours) as total_duration_broadcast_start_21_22_hours
,sum(total_viewing_duration_broadcast_start_20_23_hours) as total_duration_broadcast_start_20_22_hours
,rank() over (partition by account_number order by total_duration desc ,channel_name_inc_hd_staggercast_channel_families desc) as rank_total_duration
,rank() over (partition by account_number order by total_duration_broadcast_start_21_22_hours desc ,channel_name_inc_hd_staggercast_channel_families desc) as rank_total_duration_broadcast_21_22_hours
,rank() over (partition by account_number order by total_duration_broadcast_start_20_22_hours desc ,channel_name_inc_hd_staggercast_channel_families desc) as rank_total_duration_broadcast_20_22_hours
into v190_total_viewing_by_channel_family
from v190_monthly_viewing_appends
group by account_number
,channel_name_inc_hd_staggercast_channel_families
;

commit;


create  hg index idx1 on v190_total_viewing_by_channel(account_number);

create  hg index idx1 on v190_total_viewing_by_channel_family(account_number);
--drop table v190_top_x_channels_summary_for_powerpivotselect top 500 * from v190_total_viewing_by_channel;

--drop table v190_top_x_channels_summary_for_powerpivot Add Pay Channel Info on to Rankings--drop table v190_top_x_channels_summary_for_powerpivot-
--drop table v190_top_x_channels_summary_for_powerpivotdrop table #pay_channel_info_channel;
--drop table v190_top_x_channels_summary_for_powerpivotdrop table #pay_channel_info_channel_family;
select channel_name_inc_hd_staggercast
,min(channel_category) as channel_category_type
,max(pay_channel) as pay
into #pay_channel_info_channel
from v190_channel_lookup_with_channel_family
group by channel_name_inc_hd_staggercast
;


select channel_name_inc_hd_staggercast_channel_families
,min(channel_category) as channel_category_type
,max(pay_channel) as pay
into #pay_channel_info_channel_family
from v190_channel_lookup_with_channel_family
group by channel_name_inc_hd_staggercast_channel_families
;


Alter table v190_total_viewing_by_channel add pay_channel integer;
Alter table v190_total_viewing_by_channel add channel_category varchar(23);

Update v190_total_viewing_by_channel
Set pay_channel= case when b.pay=1 then 1 else 0 end
,channel_category = b.channel_category_type
From v190_total_viewing_by_channel as a
Left outer join #pay_channel_info_channel as b
On a.channel_name_inc_hd_staggercast=b.channel_name_inc_hd_staggercast
;

Alter table v190_total_viewing_by_channel_family add pay_channel integer;
Alter table v190_total_viewing_by_channel_family add channel_category varchar(23);

Update v190_total_viewing_by_channel_family
Set pay_channel= case when b.pay=1 then 1 else 0 end
,channel_category = b.channel_category_type
From v190_total_viewing_by_channel_family as a
Left outer join #pay_channel_info_channel_family as b
On a.channel_name_inc_hd_staggercast_channel_families=b.channel_name_inc_hd_staggercast_channel_families
;
commit;

--drop table v190_top_x_channels_summary_for_powerpivotselect channel_name_inc_hd_staggercast_channel_families , count(*) as records from v190_total_viewing_by_channel_family where rank_total_duration = 1  group by channel_name_inc_hd_staggercast_channel_families order by records desc
--drop table v190_top_x_channels_summary_for_powerpivotselect channel_category , count(*) from mawbya.v190_channels_lu_am group by channel_category order by channel_category;
--drop table v190_top_x_channels_summary_for_powerpivotselect top 100 * from v190_total_viewing_by_channel_family;

--drop table v190_top_x_channels_summary_for_powerpivot--drop table v190_top_x_channels_summary_for_powerpivotCalculate number of Pay/Channel Category in Top 10
--drop table v190_top_x_channels_summary_for_powerpivotdrop table #top_x_channels_summary;
select a.account_number
--drop table v190_top_x_channels_summary_for_powerpivotPay Channels
,sum(case when channel_category not in('1)TRUE_FREE_TO_AIR','2)SKY_FREE_TO_AIR') and rank_total_duration<=5  then 1 else 0 end) as pay_channels_top_05
,sum(case when channel_category not in('1)TRUE_FREE_TO_AIR','2)SKY_FREE_TO_AIR') and rank_total_duration<=10  then 1 else 0 end) as pay_channels_top_10
,sum(case when channel_category not in('1)TRUE_FREE_TO_AIR','2)SKY_FREE_TO_AIR') and rank_total_duration<=20  then 1 else 0 end) as pay_channels_top_20

,sum(case when channel_category not in('1)TRUE_FREE_TO_AIR','2)SKY_FREE_TO_AIR') and rank_total_duration_broadcast_20_22_hours<=5  then 1 else 0 end) as pay_channels_top_05_broadcast_2000_to_2259
,sum(case when channel_category not in('1)TRUE_FREE_TO_AIR','2)SKY_FREE_TO_AIR') and rank_total_duration_broadcast_20_22_hours<=10  then 1 else 0 end) as pay_channels_top_10_broadcast_2000_to_2259
,sum(case when channel_category not in('1)TRUE_FREE_TO_AIR','2)SKY_FREE_TO_AIR') and rank_total_duration_broadcast_20_22_hours<=20  then 1 else 0 end) as pay_channels_top_20_broadcast_2000_to_2259

,sum(case when channel_category not in('1)TRUE_FREE_TO_AIR','2)SKY_FREE_TO_AIR') and rank_total_duration_broadcast_21_22_hours<=5  then 1 else 0 end) as pay_channels_top_05_broadcast_2100_to_2259
,sum(case when channel_category not in('1)TRUE_FREE_TO_AIR','2)SKY_FREE_TO_AIR') and rank_total_duration_broadcast_21_22_hours<=10  then 1 else 0 end) as pay_channels_top_10_broadcast_2100_to_2259
,sum(case when channel_category not in('1)TRUE_FREE_TO_AIR','2)SKY_FREE_TO_AIR') and rank_total_duration_broadcast_21_22_hours<=20  then 1 else 0 end) as pay_channels_top_20_broadcast_2100_to_2259


--drop table v190_top_x_channels_summary_for_powerpivotTrue Free to Air

,sum(case when channel_category in('1)TRUE_FREE_TO_AIR') and rank_total_duration<=5  then 1 else 0 end) as true_FTA_top_05
,sum(case when channel_category in('1)TRUE_FREE_TO_AIR') and rank_total_duration<=10  then 1 else 0 end) as true_FTA_top_10
,sum(case when channel_category in('1)TRUE_FREE_TO_AIR') and rank_total_duration<=20  then 1 else 0 end) as true_FTA_top_20

,sum(case when channel_category in('1)TRUE_FREE_TO_AIR') and rank_total_duration_broadcast_20_22_hours<=5  then 1 else 0 end) as true_FTA_top_05_broadcast_2000_to_2259
,sum(case when channel_category in('1)TRUE_FREE_TO_AIR') and rank_total_duration_broadcast_20_22_hours<=10  then 1 else 0 end) as true_FTA_top_10_broadcast_2000_to_2259
,sum(case when channel_category in('1)TRUE_FREE_TO_AIR') and rank_total_duration_broadcast_20_22_hours<=20  then 1 else 0 end) as true_FTA_top_20_broadcast_2000_to_2259

,sum(case when channel_category in('1)TRUE_FREE_TO_AIR') and rank_total_duration_broadcast_21_22_hours<=5  then 1 else 0 end) as true_FTA_top_05_broadcast_2100_to_2259
,sum(case when channel_category in('1)TRUE_FREE_TO_AIR') and rank_total_duration_broadcast_21_22_hours<=10  then 1 else 0 end) as true_FTA_top_10_broadcast_2100_to_2259
,sum(case when channel_category in('1)TRUE_FREE_TO_AIR') and rank_total_duration_broadcast_21_22_hours<=20  then 1 else 0 end) as true_FTA_top_20_broadcast_2100_to_2259


--drop table v190_top_x_channels_summary_for_powerpivotSky Free to Air

,sum(case when channel_category in('2)SKY_FREE_TO_AIR') and rank_total_duration<=5  then 1 else 0 end) as Sky_FTA_top_05
,sum(case when channel_category in('2)SKY_FREE_TO_AIR') and rank_total_duration<=10  then 1 else 0 end) as Sky_FTA_top_10
,sum(case when channel_category in('2)SKY_FREE_TO_AIR') and rank_total_duration<=20  then 1 else 0 end) as Sky_FTA_top_20

,sum(case when channel_category in('2)SKY_FREE_TO_AIR') and rank_total_duration_broadcast_20_22_hours<=5  then 1 else 0 end) as Sky_FTA_top_05_broadcast_2000_to_2259
,sum(case when channel_category in('2)SKY_FREE_TO_AIR') and rank_total_duration_broadcast_20_22_hours<=10  then 1 else 0 end) as Sky_FTA_top_10_broadcast_2000_to_2259
,sum(case when channel_category in('2)SKY_FREE_TO_AIR') and rank_total_duration_broadcast_20_22_hours<=20  then 1 else 0 end) as Sky_FTA_top_20_broadcast_2000_to_2259

,sum(case when channel_category in('2)SKY_FREE_TO_AIR') and rank_total_duration_broadcast_21_22_hours<=5  then 1 else 0 end) as Sky_FTA_top_05_broadcast_2100_to_2259
,sum(case when channel_category in('2)SKY_FREE_TO_AIR') and rank_total_duration_broadcast_21_22_hours<=10  then 1 else 0 end) as Sky_FTA_top_10_broadcast_2100_to_2259
,sum(case when channel_category in('2)SKY_FREE_TO_AIR') and rank_total_duration_broadcast_21_22_hours<=20  then 1 else 0 end) as Sky_FTA_top_20_broadcast_2100_to_2259



--drop table v190_top_x_channels_summary_for_powerpivotSky Pay Basic

,sum(case when channel_category in('3)SKY_PAY_BASIC') and rank_total_duration<=5  then 1 else 0 end) as Sky_pay_basic_top_05
,sum(case when channel_category in('3)SKY_PAY_BASIC') and rank_total_duration<=10  then 1 else 0 end) as Sky_pay_basic_top_10
,sum(case when channel_category in('3)SKY_PAY_BASIC') and rank_total_duration<=20  then 1 else 0 end) as Sky_pay_basic_top_20

,sum(case when channel_category in('3)SKY_PAY_BASIC') and rank_total_duration_broadcast_20_22_hours<=5  then 1 else 0 end) as Sky_pay_basic_top_05_broadcast_2000_to_2259
,sum(case when channel_category in('3)SKY_PAY_BASIC') and rank_total_duration_broadcast_20_22_hours<=10  then 1 else 0 end) as Sky_pay_basic_top_10_broadcast_2000_to_2259
,sum(case when channel_category in('3)SKY_PAY_BASIC') and rank_total_duration_broadcast_20_22_hours<=20  then 1 else 0 end) as Sky_pay_basic_top_20_broadcast_2000_to_2259

,sum(case when channel_category in('3)SKY_PAY_BASIC') and rank_total_duration_broadcast_21_22_hours<=5  then 1 else 0 end) as Sky_pay_basic_top_05_broadcast_2100_to_2259
,sum(case when channel_category in('3)SKY_PAY_BASIC') and rank_total_duration_broadcast_21_22_hours<=10  then 1 else 0 end) as Sky_pay_basic_top_10_broadcast_2100_to_2259
,sum(case when channel_category in('3)SKY_PAY_BASIC') and rank_total_duration_broadcast_21_22_hours<=20  then 1 else 0 end) as Sky_pay_basic_top_20_broadcast_2100_to_2259

--drop table v190_top_x_channels_summary_for_powerpivotSky Pay Exclusive
,sum(case when channel_category in('4)SKY_PAY_EXCLUSIVE') and rank_total_duration<=5  then 1 else 0 end) as Sky_pay_exclusive_top_05
,sum(case when channel_category in('4)SKY_PAY_EXCLUSIVE') and rank_total_duration<=10  then 1 else 0 end) as Sky_pay_exclusive_top_10
,sum(case when channel_category in('4)SKY_PAY_EXCLUSIVE') and rank_total_duration<=20  then 1 else 0 end) as Sky_pay_exclusive_top_20

,sum(case when channel_category in('4)SKY_PAY_EXCLUSIVE') and rank_total_duration_broadcast_20_22_hours<=5  then 1 else 0 end) as Sky_pay_exclusive_top_05_broadcast_2000_to_2259
,sum(case when channel_category in('4)SKY_PAY_EXCLUSIVE') and rank_total_duration_broadcast_20_22_hours<=10  then 1 else 0 end) as Sky_pay_exclusive_top_10_broadcast_2000_to_2259
,sum(case when channel_category in('4)SKY_PAY_EXCLUSIVE') and rank_total_duration_broadcast_20_22_hours<=20  then 1 else 0 end) as Sky_pay_exclusive_top_20_broadcast_2000_to_2259

,sum(case when channel_category in('4)SKY_PAY_EXCLUSIVE') and rank_total_duration_broadcast_21_22_hours<=5  then 1 else 0 end) as Sky_pay_exclusive_top_05_broadcast_2100_to_2259
,sum(case when channel_category in('4)SKY_PAY_EXCLUSIVE') and rank_total_duration_broadcast_21_22_hours<=10  then 1 else 0 end) as Sky_pay_exclusive_top_10_broadcast_2100_to_2259
,sum(case when channel_category in('4)SKY_PAY_EXCLUSIVE') and rank_total_duration_broadcast_21_22_hours<=20  then 1 else 0 end) as Sky_pay_exclusive_top_20_broadcast_2100_to_2259

--drop table v190_top_x_channels_summary_for_powerpivot-Sky Premiums
,sum(case when channel_category in('6)SKY_PREMIUMS') and rank_total_duration<=5  then 1 else 0 end) as Sky_premiums_top_05
,sum(case when channel_category in('6)SKY_PREMIUMS') and rank_total_duration<=10  then 1 else 0 end) as Sky_premiums_top_10
,sum(case when channel_category in('6)SKY_PREMIUMS') and rank_total_duration<=20  then 1 else 0 end) as Sky_premiums_top_20

,sum(case when channel_category in('6)SKY_PREMIUMS') and rank_total_duration_broadcast_20_22_hours<=5  then 1 else 0 end) as Sky_premiums_top_05_broadcast_2000_to_2259
,sum(case when channel_category in('6)SKY_PREMIUMS') and rank_total_duration_broadcast_20_22_hours<=10  then 1 else 0 end) as Sky_premiums_top_10_broadcast_2000_to_2259
,sum(case when channel_category in('6)SKY_PREMIUMS') and rank_total_duration_broadcast_20_22_hours<=20  then 1 else 0 end) as Sky_premiums_top_20_broadcast_2000_to_2259

,sum(case when channel_category in('6)SKY_PREMIUMS') and rank_total_duration_broadcast_21_22_hours<=5  then 1 else 0 end) as Sky_premiums_top_05_broadcast_2100_to_2259
,sum(case when channel_category in('6)SKY_PREMIUMS') and rank_total_duration_broadcast_21_22_hours<=10  then 1 else 0 end) as Sky_premiums_top_10_broadcast_2100_to_2259
,sum(case when channel_category in('6)SKY_PREMIUMS') and rank_total_duration_broadcast_21_22_hours<=20  then 1 else 0 end) as Sky_premiums_top_20_broadcast_2100_to_2259

--drop table v190_top_x_channels_summary_for_powerpivotThird Party Basic
,sum(case when channel_category in('5)THIRD_PARTY_BASIC') and rank_total_duration<=5  then 1 else 0 end) as third_party_basic_top_05
,sum(case when channel_category in('5)THIRD_PARTY_BASIC') and rank_total_duration<=10  then 1 else 0 end) as third_party_basic_top_10
,sum(case when channel_category in('5)THIRD_PARTY_BASIC') and rank_total_duration<=20  then 1 else 0 end) as third_party_basic_top_20

,sum(case when channel_category in('5)THIRD_PARTY_BASIC') and rank_total_duration_broadcast_20_22_hours<=5  then 1 else 0 end) as third_party_basic_top_05_broadcast_2000_to_2259
,sum(case when channel_category in('5)THIRD_PARTY_BASIC') and rank_total_duration_broadcast_20_22_hours<=10  then 1 else 0 end) as third_party_basic_top_10_broadcast_2000_to_2259
,sum(case when channel_category in('5)THIRD_PARTY_BASIC') and rank_total_duration_broadcast_20_22_hours<=20  then 1 else 0 end) as third_party_basic_top_20_broadcast_2000_to_2259

,sum(case when channel_category in('5)THIRD_PARTY_BASIC') and rank_total_duration_broadcast_21_22_hours<=5  then 1 else 0 end) as third_party_basic_top_05_broadcast_2100_to_2259
,sum(case when channel_category in('5)THIRD_PARTY_BASIC') and rank_total_duration_broadcast_21_22_hours<=10  then 1 else 0 end) as third_party_basic_top_10_broadcast_2100_to_2259
,sum(case when channel_category in('5)THIRD_PARTY_BASIC') and rank_total_duration_broadcast_21_22_hours<=20  then 1 else 0 end) as third_party_basic_top_20_broadcast_2100_to_2259

--drop table v190_top_x_channels_summary_for_powerpivotThird Non Basic
,sum(case when channel_category in('7)THIRD_PARTY_NON_BASIC') and rank_total_duration<=5  then 1 else 0 end) as third_party_non_basic_top_05
,sum(case when channel_category in('7)THIRD_PARTY_NON_BASIC') and rank_total_duration<=10  then 1 else 0 end) as third_party_non_basic_top_10
,sum(case when channel_category in('7)THIRD_PARTY_NON_BASIC') and rank_total_duration<=20  then 1 else 0 end) as third_party_non_basic_top_20

,sum(case when channel_category in('7)THIRD_PARTY_NON_BASIC') and rank_total_duration_broadcast_20_22_hours<=5  then 1 else 0 end) as third_party_non_basic_top_05_broadcast_2000_to_2259
,sum(case when channel_category in('7)THIRD_PARTY_NON_BASIC') and rank_total_duration_broadcast_20_22_hours<=10  then 1 else 0 end) as third_party_non_basic_top_10_broadcast_2000_to_2259
,sum(case when channel_category in('7)THIRD_PARTY_NON_BASIC') and rank_total_duration_broadcast_20_22_hours<=20  then 1 else 0 end) as third_party_non_basic_top_20_broadcast_2000_to_2259

,sum(case when channel_category in('7)THIRD_PARTY_NON_BASIC') and rank_total_duration_broadcast_21_22_hours<=5  then 1 else 0 end) as third_party_non_basic_top_05_broadcast_2100_to_2259
,sum(case when channel_category in('7)THIRD_PARTY_NON_BASIC') and rank_total_duration_broadcast_21_22_hours<=10  then 1 else 0 end) as third_party_non_basic_top_10_broadcast_2100_to_2259
,sum(case when channel_category in('7)THIRD_PARTY_NON_BASIC') and rank_total_duration_broadcast_21_22_hours<=20  then 1 else 0 end) as third_party_non_basic_top_20_broadcast_2100_to_2259

into v190_top_x_channels_summary
from v190_total_viewing_by_channel as a
left outer join v190_all_active_accounts as b
on a.account_number = b.account_number
where b.eligible_vespa_analysis_account =1
group by a.account_number
;
commit;
--drop table v190_top_x_channels_summary_for_powerpivotselect top 500 * from #top_x_channels_summary;

--drop table v190_top_x_channels_summary_for_powerpivot-Repeat for Channel Family--drop table v190_top_x_channels_summary_for_powerpivot--drop table v190_top_x_channels_summary_for_powerpivot
--drop table v190_top_x_channels_summary_for_powerpivot--drop table v190_top_x_channels_summary_for_powerpivotCalculate number of Pay/Channel Category in Top 10
--drop table v190_top_x_channels_summary_for_powerpivotdrop table #top_x_channels_summary;
select a.account_number
--drop table v190_top_x_channels_summary_for_powerpivotPay Channels
,sum(case when channel_category not in('1)TRUE_FREE_TO_AIR','2)SKY_FREE_TO_AIR') and rank_total_duration<=5  then 1 else 0 end) as pay_channels_top_05
,sum(case when channel_category not in('1)TRUE_FREE_TO_AIR','2)SKY_FREE_TO_AIR') and rank_total_duration<=10  then 1 else 0 end) as pay_channels_top_10
,sum(case when channel_category not in('1)TRUE_FREE_TO_AIR','2)SKY_FREE_TO_AIR') and rank_total_duration<=20  then 1 else 0 end) as pay_channels_top_20

,sum(case when channel_category not in('1)TRUE_FREE_TO_AIR','2)SKY_FREE_TO_AIR') and rank_total_duration_broadcast_20_22_hours<=5  then 1 else 0 end) as pay_channels_top_05_broadcast_2000_to_2259
,sum(case when channel_category not in('1)TRUE_FREE_TO_AIR','2)SKY_FREE_TO_AIR') and rank_total_duration_broadcast_20_22_hours<=10  then 1 else 0 end) as pay_channels_top_10_broadcast_2000_to_2259
,sum(case when channel_category not in('1)TRUE_FREE_TO_AIR','2)SKY_FREE_TO_AIR') and rank_total_duration_broadcast_20_22_hours<=20  then 1 else 0 end) as pay_channels_top_20_broadcast_2000_to_2259

,sum(case when channel_category not in('1)TRUE_FREE_TO_AIR','2)SKY_FREE_TO_AIR') and rank_total_duration_broadcast_21_22_hours<=5  then 1 else 0 end) as pay_channels_top_05_broadcast_2100_to_2259
,sum(case when channel_category not in('1)TRUE_FREE_TO_AIR','2)SKY_FREE_TO_AIR') and rank_total_duration_broadcast_21_22_hours<=10  then 1 else 0 end) as pay_channels_top_10_broadcast_2100_to_2259
,sum(case when channel_category not in('1)TRUE_FREE_TO_AIR','2)SKY_FREE_TO_AIR') and rank_total_duration_broadcast_21_22_hours<=20  then 1 else 0 end) as pay_channels_top_20_broadcast_2100_to_2259


--drop table v190_top_x_channels_summary_for_powerpivotTrue Free to Air

,sum(case when channel_category in('1)TRUE_FREE_TO_AIR') and rank_total_duration<=5  then 1 else 0 end) as true_FTA_top_05
,sum(case when channel_category in('1)TRUE_FREE_TO_AIR') and rank_total_duration<=10  then 1 else 0 end) as true_FTA_top_10
,sum(case when channel_category in('1)TRUE_FREE_TO_AIR') and rank_total_duration<=20  then 1 else 0 end) as true_FTA_top_20

,sum(case when channel_category in('1)TRUE_FREE_TO_AIR') and rank_total_duration_broadcast_20_22_hours<=5  then 1 else 0 end) as true_FTA_top_05_broadcast_2000_to_2259
,sum(case when channel_category in('1)TRUE_FREE_TO_AIR') and rank_total_duration_broadcast_20_22_hours<=10  then 1 else 0 end) as true_FTA_top_10_broadcast_2000_to_2259
,sum(case when channel_category in('1)TRUE_FREE_TO_AIR') and rank_total_duration_broadcast_20_22_hours<=20  then 1 else 0 end) as true_FTA_top_20_broadcast_2000_to_2259

,sum(case when channel_category in('1)TRUE_FREE_TO_AIR') and rank_total_duration_broadcast_21_22_hours<=5  then 1 else 0 end) as true_FTA_top_05_broadcast_2100_to_2259
,sum(case when channel_category in('1)TRUE_FREE_TO_AIR') and rank_total_duration_broadcast_21_22_hours<=10  then 1 else 0 end) as true_FTA_top_10_broadcast_2100_to_2259
,sum(case when channel_category in('1)TRUE_FREE_TO_AIR') and rank_total_duration_broadcast_21_22_hours<=20  then 1 else 0 end) as true_FTA_top_20_broadcast_2100_to_2259


--drop table v190_top_x_channels_summary_for_powerpivotSky Free to Air

,sum(case when channel_category in('2)SKY_FREE_TO_AIR') and rank_total_duration<=5  then 1 else 0 end) as Sky_FTA_top_05
,sum(case when channel_category in('2)SKY_FREE_TO_AIR') and rank_total_duration<=10  then 1 else 0 end) as Sky_FTA_top_10
,sum(case when channel_category in('2)SKY_FREE_TO_AIR') and rank_total_duration<=20  then 1 else 0 end) as Sky_FTA_top_20

,sum(case when channel_category in('2)SKY_FREE_TO_AIR') and rank_total_duration_broadcast_20_22_hours<=5  then 1 else 0 end) as Sky_FTA_top_05_broadcast_2000_to_2259
,sum(case when channel_category in('2)SKY_FREE_TO_AIR') and rank_total_duration_broadcast_20_22_hours<=10  then 1 else 0 end) as Sky_FTA_top_10_broadcast_2000_to_2259
,sum(case when channel_category in('2)SKY_FREE_TO_AIR') and rank_total_duration_broadcast_20_22_hours<=20  then 1 else 0 end) as Sky_FTA_top_20_broadcast_2000_to_2259

,sum(case when channel_category in('2)SKY_FREE_TO_AIR') and rank_total_duration_broadcast_21_22_hours<=5  then 1 else 0 end) as Sky_FTA_top_05_broadcast_2100_to_2259
,sum(case when channel_category in('2)SKY_FREE_TO_AIR') and rank_total_duration_broadcast_21_22_hours<=10  then 1 else 0 end) as Sky_FTA_top_10_broadcast_2100_to_2259
,sum(case when channel_category in('2)SKY_FREE_TO_AIR') and rank_total_duration_broadcast_21_22_hours<=20  then 1 else 0 end) as Sky_FTA_top_20_broadcast_2100_to_2259



--drop table v190_top_x_channels_summary_for_powerpivotSky Pay Basic

,sum(case when channel_category in('3)SKY_PAY_BASIC') and rank_total_duration<=5  then 1 else 0 end) as Sky_pay_basic_top_05
,sum(case when channel_category in('3)SKY_PAY_BASIC') and rank_total_duration<=10  then 1 else 0 end) as Sky_pay_basic_top_10
,sum(case when channel_category in('3)SKY_PAY_BASIC') and rank_total_duration<=20  then 1 else 0 end) as Sky_pay_basic_top_20

,sum(case when channel_category in('3)SKY_PAY_BASIC') and rank_total_duration_broadcast_20_22_hours<=5  then 1 else 0 end) as Sky_pay_basic_top_05_broadcast_2000_to_2259
,sum(case when channel_category in('3)SKY_PAY_BASIC') and rank_total_duration_broadcast_20_22_hours<=10  then 1 else 0 end) as Sky_pay_basic_top_10_broadcast_2000_to_2259
,sum(case when channel_category in('3)SKY_PAY_BASIC') and rank_total_duration_broadcast_20_22_hours<=20  then 1 else 0 end) as Sky_pay_basic_top_20_broadcast_2000_to_2259

,sum(case when channel_category in('3)SKY_PAY_BASIC') and rank_total_duration_broadcast_21_22_hours<=5  then 1 else 0 end) as Sky_pay_basic_top_05_broadcast_2100_to_2259
,sum(case when channel_category in('3)SKY_PAY_BASIC') and rank_total_duration_broadcast_21_22_hours<=10  then 1 else 0 end) as Sky_pay_basic_top_10_broadcast_2100_to_2259
,sum(case when channel_category in('3)SKY_PAY_BASIC') and rank_total_duration_broadcast_21_22_hours<=20  then 1 else 0 end) as Sky_pay_basic_top_20_broadcast_2100_to_2259

--drop table v190_top_x_channels_summary_for_powerpivotSky Pay Exclusive
,sum(case when channel_category in('4)SKY_PAY_EXCLUSIVE') and rank_total_duration<=5  then 1 else 0 end) as Sky_pay_exclusive_top_05
,sum(case when channel_category in('4)SKY_PAY_EXCLUSIVE') and rank_total_duration<=10  then 1 else 0 end) as Sky_pay_exclusive_top_10
,sum(case when channel_category in('4)SKY_PAY_EXCLUSIVE') and rank_total_duration<=20  then 1 else 0 end) as Sky_pay_exclusive_top_20

,sum(case when channel_category in('4)SKY_PAY_EXCLUSIVE') and rank_total_duration_broadcast_20_22_hours<=5  then 1 else 0 end) as Sky_pay_exclusive_top_05_broadcast_2000_to_2259
,sum(case when channel_category in('4)SKY_PAY_EXCLUSIVE') and rank_total_duration_broadcast_20_22_hours<=10  then 1 else 0 end) as Sky_pay_exclusive_top_10_broadcast_2000_to_2259
,sum(case when channel_category in('4)SKY_PAY_EXCLUSIVE') and rank_total_duration_broadcast_20_22_hours<=20  then 1 else 0 end) as Sky_pay_exclusive_top_20_broadcast_2000_to_2259

,sum(case when channel_category in('4)SKY_PAY_EXCLUSIVE') and rank_total_duration_broadcast_21_22_hours<=5  then 1 else 0 end) as Sky_pay_exclusive_top_05_broadcast_2100_to_2259
,sum(case when channel_category in('4)SKY_PAY_EXCLUSIVE') and rank_total_duration_broadcast_21_22_hours<=10  then 1 else 0 end) as Sky_pay_exclusive_top_10_broadcast_2100_to_2259
,sum(case when channel_category in('4)SKY_PAY_EXCLUSIVE') and rank_total_duration_broadcast_21_22_hours<=20  then 1 else 0 end) as Sky_pay_exclusive_top_20_broadcast_2100_to_2259

--drop table v190_top_x_channels_summary_for_powerpivot-Sky Premiums
,sum(case when channel_category in('6)SKY_PREMIUMS') and rank_total_duration<=5  then 1 else 0 end) as Sky_premiums_top_05
,sum(case when channel_category in('6)SKY_PREMIUMS') and rank_total_duration<=10  then 1 else 0 end) as Sky_premiums_top_10
,sum(case when channel_category in('6)SKY_PREMIUMS') and rank_total_duration<=20  then 1 else 0 end) as Sky_premiums_top_20

,sum(case when channel_category in('6)SKY_PREMIUMS') and rank_total_duration_broadcast_20_22_hours<=5  then 1 else 0 end) as Sky_premiums_top_05_broadcast_2000_to_2259
,sum(case when channel_category in('6)SKY_PREMIUMS') and rank_total_duration_broadcast_20_22_hours<=10  then 1 else 0 end) as Sky_premiums_top_10_broadcast_2000_to_2259
,sum(case when channel_category in('6)SKY_PREMIUMS') and rank_total_duration_broadcast_20_22_hours<=20  then 1 else 0 end) as Sky_premiums_top_20_broadcast_2000_to_2259

,sum(case when channel_category in('6)SKY_PREMIUMS') and rank_total_duration_broadcast_21_22_hours<=5  then 1 else 0 end) as Sky_premiums_top_05_broadcast_2100_to_2259
,sum(case when channel_category in('6)SKY_PREMIUMS') and rank_total_duration_broadcast_21_22_hours<=10  then 1 else 0 end) as Sky_premiums_top_10_broadcast_2100_to_2259
,sum(case when channel_category in('6)SKY_PREMIUMS') and rank_total_duration_broadcast_21_22_hours<=20  then 1 else 0 end) as Sky_premiums_top_20_broadcast_2100_to_2259

--drop table v190_top_x_channels_summary_for_powerpivotThird Party Basic
,sum(case when channel_category in('5)THIRD_PARTY_BASIC') and rank_total_duration<=5  then 1 else 0 end) as third_party_basic_top_05
,sum(case when channel_category in('5)THIRD_PARTY_BASIC') and rank_total_duration<=10  then 1 else 0 end) as third_party_basic_top_10
,sum(case when channel_category in('5)THIRD_PARTY_BASIC') and rank_total_duration<=20  then 1 else 0 end) as third_party_basic_top_20

,sum(case when channel_category in('5)THIRD_PARTY_BASIC') and rank_total_duration_broadcast_20_22_hours<=5  then 1 else 0 end) as third_party_basic_top_05_broadcast_2000_to_2259
,sum(case when channel_category in('5)THIRD_PARTY_BASIC') and rank_total_duration_broadcast_20_22_hours<=10  then 1 else 0 end) as third_party_basic_top_10_broadcast_2000_to_2259
,sum(case when channel_category in('5)THIRD_PARTY_BASIC') and rank_total_duration_broadcast_20_22_hours<=20  then 1 else 0 end) as third_party_basic_top_20_broadcast_2000_to_2259

,sum(case when channel_category in('5)THIRD_PARTY_BASIC') and rank_total_duration_broadcast_21_22_hours<=5  then 1 else 0 end) as third_party_basic_top_05_broadcast_2100_to_2259
,sum(case when channel_category in('5)THIRD_PARTY_BASIC') and rank_total_duration_broadcast_21_22_hours<=10  then 1 else 0 end) as third_party_basic_top_10_broadcast_2100_to_2259
,sum(case when channel_category in('5)THIRD_PARTY_BASIC') and rank_total_duration_broadcast_21_22_hours<=20  then 1 else 0 end) as third_party_basic_top_20_broadcast_2100_to_2259

--drop table v190_top_x_channels_summary_for_powerpivotThird Non Basic
,sum(case when channel_category in('7)THIRD_PARTY_NON_BASIC') and rank_total_duration<=5  then 1 else 0 end) as third_party_non_basic_top_05
,sum(case when channel_category in('7)THIRD_PARTY_NON_BASIC') and rank_total_duration<=10  then 1 else 0 end) as third_party_non_basic_top_10
,sum(case when channel_category in('7)THIRD_PARTY_NON_BASIC') and rank_total_duration<=20  then 1 else 0 end) as third_party_non_basic_top_20

,sum(case when channel_category in('7)THIRD_PARTY_NON_BASIC') and rank_total_duration_broadcast_20_22_hours<=5  then 1 else 0 end) as third_party_non_basic_top_05_broadcast_2000_to_2259
,sum(case when channel_category in('7)THIRD_PARTY_NON_BASIC') and rank_total_duration_broadcast_20_22_hours<=10  then 1 else 0 end) as third_party_non_basic_top_10_broadcast_2000_to_2259
,sum(case when channel_category in('7)THIRD_PARTY_NON_BASIC') and rank_total_duration_broadcast_20_22_hours<=20  then 1 else 0 end) as third_party_non_basic_top_20_broadcast_2000_to_2259

,sum(case when channel_category in('7)THIRD_PARTY_NON_BASIC') and rank_total_duration_broadcast_21_22_hours<=5  then 1 else 0 end) as third_party_non_basic_top_05_broadcast_2100_to_2259
,sum(case when channel_category in('7)THIRD_PARTY_NON_BASIC') and rank_total_duration_broadcast_21_22_hours<=10  then 1 else 0 end) as third_party_non_basic_top_10_broadcast_2100_to_2259
,sum(case when channel_category in('7)THIRD_PARTY_NON_BASIC') and rank_total_duration_broadcast_21_22_hours<=20  then 1 else 0 end) as third_party_non_basic_top_20_broadcast_2100_to_2259

into v190_top_x_channel_family_summary
from v190_total_viewing_by_channel_family as a
left outer join v190_all_active_accounts as b
on a.account_number = b.account_number
where b.eligible_vespa_analysis_account =1
group by a.account_number
;
commit;
--drop table v190_top_x_channels_summary_for_powerpivot
--select top 500 * from v190_total_viewing_by_channel_family
--drop table v190_top_x_channels_summary_for_powerpivot--drop table v190_top_x_channels_summary_for_powerpivotAdd March TA/Churn Activity to master list--drop table v190_top_x_channels_summary_for_powerpivot

alter table v190_all_active_accounts add march_event varchar(20);

update v190_all_active_accounts
set march_event=b.march_event
from v190_all_active_accounts as a
left outer join mawbya.V190_churn_ta_status as b
on a.account_number = b.account_number
;

commit;

--drop table v190_top_x_channels_summary_for_powerpivot-Add March Event to Viewing Tables--drop table v190_top_x_channels_summary_for_powerpivot--drop table v190_top_x_channels_summary_for_powerpivot

alter table v190_top_x_channels_summary add march_event varchar(20);

update v190_top_x_channels_summary
set march_event=b.march_event
from v190_top_x_channels_summary as a
left outer join mawbya.V190_churn_ta_status as b
on a.account_number = b.account_number
;

commit;

alter table v190_top_x_channel_family_summary add march_event varchar(20);

update v190_top_x_channel_family_summary
set march_event=b.march_event
from v190_top_x_channel_family_summary as a
left outer join mawbya.V190_churn_ta_status as b
on a.account_number = b.account_number
;

---Add On Summary Details from Tee's table

alter table v190_top_x_channels_summary add product_holding varchar(50);
alter table v190_top_x_channels_summary add tv_package varchar(50);
alter table v190_top_x_channels_summary add new_tv_package varchar(50);
alter table v190_top_x_channels_summary add box_type varchar(50);

---Extra Variables----
alter table v190_top_x_channels_summary add discontinuous_tenure_desc varchar(50);
alter table v190_top_x_channels_summary add rtm varchar(50);
alter table v190_top_x_channels_summary add talk_package varchar(50);
alter table v190_top_x_channels_summary add bb_package varchar(50);
alter table v190_top_x_channels_summary add contribution_desc varchar(50);
alter table v190_top_x_channels_summary add value_segment varchar(50);
alter table v190_top_x_channels_summary add no_of_previous_ab integer;
alter table v190_top_x_channels_summary add no_of_previous_pc integer;
alter table v190_top_x_channels_summary add no_of_previous_po integer;
alter table v190_top_x_channels_summary add no_of_previous_sc integer;
alter table v190_top_x_channels_summary add no_of_previous_ta integer;
alter table v190_top_x_channels_summary add no_of_previous_ta_12m integer;
alter table v190_top_x_channels_summary add no_of_previous_pat integer;
alter table v190_top_x_channels_summary add no_of_previous_pat_12m integer;
alter table v190_top_x_channels_summary add no_of_previous_ta_24m integer;
alter table v190_top_x_channels_summary add no_of_previous_ta_2m integer;
alter table v190_top_x_channels_summary add no_of_previous_ta_saved_2m integer;
alter table v190_top_x_channels_summary add end_of_offer_flag integer;
alter table v190_top_x_channels_summary add home_move_6m integer;
alter table v190_top_x_channels_summary add customer_management_segment varchar(50);
alter table v190_top_x_channels_summary add at_risk_segment varchar(50);
alter table v190_top_x_channels_summary add no_of_premium_upgrades integer;
alter table v190_top_x_channels_summary add no_of_premium_downgrades integer;
alter table v190_top_x_channels_summary add premiums_upgrade_flag integer;
alter table v190_top_x_channels_summary add premiums_downgrade_flag integer;
alter table v190_top_x_channels_summary add sky_go_usage integer;
alter table v190_top_x_channels_summary add on_demand_usage integer;
alter table v190_top_x_channels_summary add emails_sent integer;
alter table v190_top_x_channels_summary add emails_opened integer;
alter table v190_top_x_channels_summary add emails_clicked integer;
alter table v190_top_x_channels_summary add sports_days_available integer;
alter table v190_top_x_channels_summary add movies_days_available integer;
alter table v190_top_x_channels_summary add hd_days_available integer;
alter table v190_top_x_channels_summary add espn_days_available integer;
alter table v190_top_x_channels_summary add mutv_days_available integer;
alter table v190_top_x_channels_summary add cfctv_days_available integer;
alter table v190_top_x_channels_summary add in_viewing_panel integer;
alter table v190_top_x_channels_summary add aggregated_at_risk_segment varchar(50);


--

update v190_top_x_channels_summary
set product_holding=b.product_holding
, tv_package=b.tv_package
,new_tv_package=b.new_tv_package
,box_type=b.box_type

---
,discontinuous_tenure_desc	=b.discontinuous_tenure_desc
,rtm	=b.rtm
,talk_package	=b.talk_package
,bb_package	=b.bb_package
,contribution_desc	=b.contribution_desc
,value_segment	=b.value_segment
,no_of_previous_ab	=b.no_of_previous_ab
,no_of_previous_pc	=b.no_of_previous_pc
,no_of_previous_po	=b.no_of_previous_po
,no_of_previous_sc	=b.no_of_previous_sc
,no_of_previous_ta	=b.no_of_previous_ta
,no_of_previous_ta_12m	=b.no_of_previous_ta_12m
,no_of_previous_pat	=b.no_of_previous_pat
,no_of_previous_pat_12m	=b.no_of_previous_pat_12m
,no_of_previous_ta_24m	=b.no_of_previous_ta_24m
,no_of_previous_ta_2m	=b.no_of_previous_ta_2m
,no_of_previous_ta_saved_2m	=b.no_of_previous_ta_saved_2m
,end_of_offer_flag	=b.end_of_offer_flag
,home_move_6m	=b.home_move_6m
,customer_management_segment	=b.customer_management_segment
,at_risk_segment	=b.at_risk_segment
,no_of_premium_upgrades	=b.no_of_premium_upgrades
,no_of_premium_downgrades	=b.no_of_premium_downgrades
,premiums_upgrade_flag	=b.premiums_upgrade_flag
,premiums_downgrade_flag	=b.premiums_downgrade_flag
,sky_go_usage	=b.sky_go_usage
,on_demand_usage	=b.on_demand_usage
,emails_sent	=b.emails_sent
,emails_opened	=b.emails_opened
,emails_clicked	=b.emails_clicked
,sports_days_available	=b.sports_days_available
,movies_days_available	=b.movies_days_available
,hd_days_available	=b.hd_days_available
,espn_days_available	=b.espn_days_available
,mutv_days_available	=b.mutv_days_available
,cfctv_days_available	=b.cfctv_days_available
,in_viewing_panel	=b.in_viewing_panel
,aggregated_at_risk_segment	=b.aggregated_at_risk_segment




from v190_top_x_channels_summary as a
left outer join hensirir.SK4637_Content_Initiative_Event_Base as b
on a.account_number = b.account_number
;
commit;

----Add in Household Level Metrics---
alter table v190_top_x_channels_summary add cqm_score_desc varchar(50);
alter table v190_top_x_channels_summary add affluence_desc varchar(50);
alter table v190_top_x_channels_summary add lifestage_desc varchar(50);
alter table v190_top_x_channels_summary add h_mosaic_uk_group_desc varchar(50);
alter table v190_top_x_channels_summary add h_income_band_desc varchar(50);
alter table v190_top_x_channels_summary add h_length_of_residency_desc varchar(50);
alter table v190_top_x_channels_summary add h_fss_group_desc varchar(50);
alter table v190_top_x_channels_summary add h_household_composition_desc varchar(50);
alter table v190_top_x_channels_summary add cable_area integer;
alter table v190_top_x_channels_summary add fibre_area integer;
alter table v190_top_x_channels_summary add onnet_area integer;
alter table v190_top_x_channels_summary add active_hh integer;

alter table v190_top_x_channels_summary add cb_key_household bigint;

update v190_top_x_channels_summary
set cb_key_household = b.cb_key_household
from v190_top_x_channels_summary as a
left outer join sk_prod.cust_single_account_view as b
on a.account_number =b.account_number
;
commit;
--select top 10 cb_key_household from  sk_prod.cust_single_account_view;


update v190_top_x_channels_summary
set cqm_score_desc	=b.cqm_score_desc
,affluence_desc	=b.affluence_desc
,lifestage_desc	=b.lifestage_desc
,h_mosaic_uk_group_desc	=b.h_mosaic_uk_group_desc
,h_income_band_desc	=b.h_income_band_desc
,h_length_of_residency_desc	=b.h_length_of_residency_desc
,h_fss_group_desc	=b.h_fss_group_desc
,h_household_composition_desc	=b.h_household_composition_desc
,cable_area	=b.cable_area
,fibre_area	=b.fibre_area
,onnet_area	=b.onnet_area
,active_hh	=b.active_hh

from v190_top_x_channels_summary as a
left outer join 	hensirir.SK4637_Content_Initiative_UK_Households  as b
on a.cb_key_household = b.cb_key_household
;
commit;

----Repeat Adding Variables in for grouped channel Pivot


---Add On Summary Details from Tee's table

alter table v190_top_x_channel_family_summary add product_holding varchar(50);
alter table v190_top_x_channel_family_summary add tv_package varchar(50);
alter table v190_top_x_channel_family_summary add new_tv_package varchar(50);
alter table v190_top_x_channel_family_summary add box_type varchar(50);

---Extra Variables----
alter table v190_top_x_channel_family_summary add discontinuous_tenure_desc varchar(50);
alter table v190_top_x_channel_family_summary add rtm varchar(50);
alter table v190_top_x_channel_family_summary add talk_package varchar(50);
alter table v190_top_x_channel_family_summary add bb_package varchar(50);
alter table v190_top_x_channel_family_summary add contribution_desc varchar(50);
alter table v190_top_x_channel_family_summary add value_segment varchar(50);
alter table v190_top_x_channel_family_summary add no_of_previous_ab integer;
alter table v190_top_x_channel_family_summary add no_of_previous_pc integer;
alter table v190_top_x_channel_family_summary add no_of_previous_po integer;
alter table v190_top_x_channel_family_summary add no_of_previous_sc integer;
alter table v190_top_x_channel_family_summary add no_of_previous_ta integer;
alter table v190_top_x_channel_family_summary add no_of_previous_ta_12m integer;
alter table v190_top_x_channel_family_summary add no_of_previous_pat integer;
alter table v190_top_x_channel_family_summary add no_of_previous_pat_12m integer;
alter table v190_top_x_channel_family_summary add no_of_previous_ta_24m integer;
alter table v190_top_x_channel_family_summary add no_of_previous_ta_2m integer;
alter table v190_top_x_channel_family_summary add no_of_previous_ta_saved_2m integer;
alter table v190_top_x_channel_family_summary add end_of_offer_flag integer;
alter table v190_top_x_channel_family_summary add home_move_6m integer;
alter table v190_top_x_channel_family_summary add customer_management_segment varchar(50);
alter table v190_top_x_channel_family_summary add at_risk_segment varchar(50);
alter table v190_top_x_channel_family_summary add no_of_premium_upgrades integer;
alter table v190_top_x_channel_family_summary add no_of_premium_downgrades integer;
alter table v190_top_x_channel_family_summary add premiums_upgrade_flag integer;
alter table v190_top_x_channel_family_summary add premiums_downgrade_flag integer;
alter table v190_top_x_channel_family_summary add sky_go_usage integer;
alter table v190_top_x_channel_family_summary add on_demand_usage integer;
alter table v190_top_x_channel_family_summary add emails_sent integer;
alter table v190_top_x_channel_family_summary add emails_opened integer;
alter table v190_top_x_channel_family_summary add emails_clicked integer;
alter table v190_top_x_channel_family_summary add sports_days_available integer;
alter table v190_top_x_channel_family_summary add movies_days_available integer;
alter table v190_top_x_channel_family_summary add hd_days_available integer;
alter table v190_top_x_channel_family_summary add espn_days_available integer;
alter table v190_top_x_channel_family_summary add mutv_days_available integer;
alter table v190_top_x_channel_family_summary add cfctv_days_available integer;
alter table v190_top_x_channel_family_summary add in_viewing_panel integer;
alter table v190_top_x_channel_family_summary add aggregated_at_risk_segment varchar(50);


--

update v190_top_x_channel_family_summary
set product_holding=b.product_holding
, tv_package=b.tv_package
,new_tv_package=b.new_tv_package
,box_type=b.box_type

---
,discontinuous_tenure_desc	=b.discontinuous_tenure_desc
,rtm	=b.rtm
,talk_package	=b.talk_package
,bb_package	=b.bb_package
,contribution_desc	=b.contribution_desc
,value_segment	=b.value_segment
,no_of_previous_ab	=b.no_of_previous_ab
,no_of_previous_pc	=b.no_of_previous_pc
,no_of_previous_po	=b.no_of_previous_po
,no_of_previous_sc	=b.no_of_previous_sc
,no_of_previous_ta	=b.no_of_previous_ta
,no_of_previous_ta_12m	=b.no_of_previous_ta_12m
,no_of_previous_pat	=b.no_of_previous_pat
,no_of_previous_pat_12m	=b.no_of_previous_pat_12m
,no_of_previous_ta_24m	=b.no_of_previous_ta_24m
,no_of_previous_ta_2m	=b.no_of_previous_ta_2m
,no_of_previous_ta_saved_2m	=b.no_of_previous_ta_saved_2m
,end_of_offer_flag	=b.end_of_offer_flag
,home_move_6m	=b.home_move_6m
,customer_management_segment	=b.customer_management_segment
,at_risk_segment	=b.at_risk_segment
,no_of_premium_upgrades	=b.no_of_premium_upgrades
,no_of_premium_downgrades	=b.no_of_premium_downgrades
,premiums_upgrade_flag	=b.premiums_upgrade_flag
,premiums_downgrade_flag	=b.premiums_downgrade_flag
,sky_go_usage	=b.sky_go_usage
,on_demand_usage	=b.on_demand_usage
,emails_sent	=b.emails_sent
,emails_opened	=b.emails_opened
,emails_clicked	=b.emails_clicked
,sports_days_available	=b.sports_days_available
,movies_days_available	=b.movies_days_available
,hd_days_available	=b.hd_days_available
,espn_days_available	=b.espn_days_available
,mutv_days_available	=b.mutv_days_available
,cfctv_days_available	=b.cfctv_days_available
,in_viewing_panel	=b.in_viewing_panel
,aggregated_at_risk_segment	=b.aggregated_at_risk_segment




from v190_top_x_channel_family_summary as a
left outer join hensirir.SK4637_Content_Initiative_Event_Base as b
on a.account_number = b.account_number
;
commit;

----Add in Household Level Metrics---
alter table v190_top_x_channel_family_summary add cqm_score_desc varchar(50);
alter table v190_top_x_channel_family_summary add affluence_desc varchar(50);
alter table v190_top_x_channel_family_summary add lifestage_desc varchar(50);
alter table v190_top_x_channel_family_summary add h_mosaic_uk_group_desc varchar(50);
alter table v190_top_x_channel_family_summary add h_income_band_desc varchar(50);
alter table v190_top_x_channel_family_summary add h_length_of_residency_desc varchar(50);
alter table v190_top_x_channel_family_summary add h_fss_group_desc varchar(50);
alter table v190_top_x_channel_family_summary add h_household_composition_desc varchar(50);
alter table v190_top_x_channel_family_summary add cable_area integer;
alter table v190_top_x_channel_family_summary add fibre_area integer;
alter table v190_top_x_channel_family_summary add onnet_area integer;
alter table v190_top_x_channel_family_summary add active_hh integer;

alter table v190_top_x_channel_family_summary add cb_key_household bigint;

update v190_top_x_channel_family_summary
set cb_key_household = b.cb_key_household
from v190_top_x_channel_family_summary as a
left outer join sk_prod.cust_single_account_view as b
on a.account_number =b.account_number
;
commit;
--select top 10 cb_key_household from  sk_prod.cust_single_account_view;


update v190_top_x_channel_family_summary
set cqm_score_desc	=b.cqm_score_desc
,affluence_desc	=b.affluence_desc
,lifestage_desc	=b.lifestage_desc
,h_mosaic_uk_group_desc	=b.h_mosaic_uk_group_desc
,h_income_band_desc	=b.h_income_band_desc
,h_length_of_residency_desc	=b.h_length_of_residency_desc
,h_fss_group_desc	=b.h_fss_group_desc
,h_household_composition_desc	=b.h_household_composition_desc
,cable_area	=b.cable_area
,fibre_area	=b.fibre_area
,onnet_area	=b.onnet_area
,active_hh	=b.active_hh

from v190_top_x_channel_family_summary as a
left outer join 	hensirir.SK4637_Content_Initiative_UK_Households  as b
on a.cb_key_household = b.cb_key_household
;
commit;










--select count(*) , count(distinct cb_key_household) from hensirir.SK4637_Content_Initiative_UK_Households 

--select top 100 * from hensirir.SK4637_Content_Initiative_Event_Base 

---Add Mar - May Metric


alter table v190_top_x_channels_summary add march_may_event varchar(20);

update v190_top_x_channels_summary
set march_may_event=b.march_may_event
from v190_top_x_channels_summary as a
left outer join dbarnett.V190_churn_ta_status_3months as b
on a.account_number = b.account_number
;

commit;

alter table v190_top_x_channel_family_summary add march_may_event varchar(20);

update v190_top_x_channel_family_summary
set march_may_event=b.march_may_event
from v190_top_x_channel_family_summary as a
left outer join dbarnett.V190_churn_ta_status_3months as b
on a.account_number = b.account_number
;

commit;

grant all on v190_top_x_channel_family_summary to public;
grant all on v190_top_x_channels_summary to public;
commit;
--drop table v190_top_x_channels_summary_for_powerpivot


select * into v190_top_x_channels_summary_for_powerpivot from v190_top_x_channels_summary;

alter table v190_top_x_channels_summary_for_powerpivot delete account_number; commit;

grant all on v190_top_x_channels_summary_for_powerpivot to public; commit;

----End of Part 2----

----Part 3 Generate Account Weights---
--Add on TA/AB Activity, Package Type, Box Type, Affluence Grouping (Tenure Already Present)
alter table v190_all_active_accounts add march_may_event varchar(20);

update v190_all_active_accounts
set march_may_event=b.march_may_event
from v190_all_active_accounts as a
left outer join dbarnett.V190_churn_ta_status_3months as b
on a.account_number = b.account_number
;

---Add in Account Variables

alter table v190_all_active_accounts  add product_holding_group varchar(50);
alter table v190_all_active_accounts  add tv_package_group varchar(50);
alter table v190_all_active_accounts  add new_tv_package varchar(50);
alter table v190_all_active_accounts  add box_type_group varchar(50);
alter table v190_all_active_accounts  add discontinuous_tenure_desc varchar(50);
alter table v190_all_active_accounts  add value_segment_group varchar(50);
commit;
update v190_all_active_accounts
set product_holding_group=case when  b.product_holding = 'TV Only' then 'a) TV Only' else 'b) TV with BB and or Talk' end
, tv_package_group=case when b.tv_package = 'Basic' then 'a) Basic' 
                        when b.tv_package = 'Dual Movies' then 'b) Dual Movies' 
                        when b.tv_package = 'Dual Sports' then 'c) Dual Sports'
                        when b.tv_package = 'Dual Sports' then 'd) Top Tier' else 'd) Other' end
,new_tv_package=b.new_tv_package
,box_type_group=case          when b.box_type in ('A) HD Combi','C) HDx Combi','E) Sky+ Combi','G) Multiroom') then 'a) Multiple Boxes'  
                        when b.box_type in ('B) HD','D) HDx','E) Sky+ Combi','F) Sky+') then 'b) Single Non-FDB Box' 
                        when b.box_type in ('H) FDB') then 'c) FDB Box' else 'b) Single Non-FDB Box'  end ---Set non-Matches to Most popular group

,value_segment_group= case when b.value_segment = 'UNSTABLE' then 'UNSTABLE' else 'STABLE' end
,discontinuous_tenure_desc=b.discontinuous_tenure_desc
from v190_all_active_accounts as a
left outer join hensirir.SK4637_Content_Initiative_Event_Base as b
on a.account_number = b.account_number
;

---Add On Affluence Grouping---
alter table v190_all_active_accounts add cb_key_household bigint;

update v190_all_active_accounts
set cb_key_household = b.cb_key_household
from v190_all_active_accounts as a
left outer join sk_prod.cust_single_account_view as b
on a.account_number =b.account_number
;


alter table v190_all_active_accounts  add affluence_group varchar(50);
commit;


commit;
update v190_all_active_accounts
set affluence_group = case when b.affluence_desc in ('Very High','High') then 'a) High'
                        when b.affluence_desc in ('Mid','Mid High','Mid Low') then 'b) Med'
                        when b.affluence_desc in ('Low','Very Low','Unknown') then 'c) Low' else 'b) Med' end
from v190_all_active_accounts as a
left outer join hensirir.SK4637_Content_Initiative_UK_Households as b
on a.cb_key_household = b.cb_key_household
;
/*

select product_holding , count(*) from hensirir.SK4637_Content_Initiative_Event_Base group by product_holding order by product_holding
select tv_package , count(*) from hensirir.SK4637_Content_Initiative_Event_Base group by tv_package order by tv_package
select new_tv_package , count(*) from hensirir.SK4637_Content_Initiative_Event_Base group by new_tv_package order by new_tv_package
select box_type , count(*) from hensirir.SK4637_Content_Initiative_Event_Base group by box_type order by box_type
select value_segment , count(*) from hensirir.SK4637_Content_Initiative_Event_Base group by value_segment order by value_segment
select product_holding , count(*) from hensirir.SK4637_Content_Initiative_Event_Base group by product_holding order by product_holding
select affluence_desc , count(*) from hensirir.SK4637_Content_Initiative_UK_Households  group by affluence_desc order by affluence_desc




*/

----End of Part 3----

----Part 4 Add Weights to Output Pivot---

select march_may_event 
, product_holding_group
, tv_package_group
,new_tv_package
,box_type_group
,value_segment_group
,discontinuous_tenure_desc
,affluence_group
, count(*) as accounts 
, sum(eligible_vespa_analysis_account) as vespa_accounts
into #uk_and_vespa_accounts
from v190_all_active_accounts 
where full_months_tenure>=15
group by march_may_event 
, product_holding_group
, tv_package_group
,new_tv_package
,box_type_group
,value_segment_group
,discontinuous_tenure_desc
,affluence_group 
order by vespa_accounts

select a.*
, case when vespa_accounts=0 then 0 else cast(accounts as real)/cast(vespa_accounts as real) end  as weight_value
into v190_eligible_account_weight_groups
from #uk_and_vespa_accounts as a
;
commit;
--select * from v190_eligible_account_weight_groups;
--Match Accounts to Weight to get weighted values---


alter table v190_all_active_accounts  add weight_value real;
commit;

commit;
update v190_all_active_accounts
set weight_value = b.weight_value
from v190_all_active_accounts as a
left outer join v190_eligible_account_weight_groups as b
on a.march_may_event=b.march_may_event  
and  a.product_holding_group=b.product_holding_group
and a.tv_package_group=b.tv_package_group
and a.new_tv_package=b.new_tv_package
and a.box_type_group=b.box_type_group
and a.value_segment_group=b.value_segment_group
and a.discontinuous_tenure_desc=b.discontinuous_tenure_desc
and a.affluence_group=b.affluence_group 
where full_months_tenure>=15 and eligible_vespa_analysis_account=1
;
commit;


--select sum(accounts) from #uk_and_vespa_accounts where vespa_accounts=0
--select * from #uk_and_vespa_accounts;

---Test
--select march_may_event  , count(*) , sum(weight_value) from v190_all_active_accounts where full_months_tenure>=15 group by march_may_event   order by march_may_event  

----End of Part 4----


---Part 5 ---

---Add Weights Back on to Pivot for Output
alter table v190_top_x_channels_summary add weight_value real;

update v190_top_x_channels_summary
set weight_value=b.weight_value
from v190_top_x_channels_summary as a
left outer join v190_all_active_accounts as b
on a.account_number = b.account_number
;

commit;

alter table v190_top_x_channel_family_summary  add weight_value real;

update v190_top_x_channel_family_summary
set weight_value=b.weight_value
from v190_top_x_channel_family_summary as a
left outer join v190_all_active_accounts as b
on a.account_number = b.account_number
;

commit;

grant all on v190_top_x_channel_family_summary to public;
grant all on v190_top_x_channels_summary to public;
commit;
drop table v190_top_x_channels_summary_for_powerpivot


select * into v190_top_x_channels_summary_for_powerpivot from v190_top_x_channels_summary;

alter table v190_top_x_channels_summary_for_powerpivot delete account_number; commit;

grant all on v190_top_x_channels_summary_for_powerpivot to public; commit;
commit;


----Repeat Output Pivot Table but with Channel family i.e., Sky Sports 1/2 etc., group---
select * into v190_top_x_channel_family_summary_for_powerpivot from v190_top_x_channel_family_summary;

alter table v190_top_x_channel_family_summary_for_powerpivot delete account_number; commit;

grant all on v190_top_x_channel_family_summary_for_powerpivot to public; commit;
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

--lect  case when full_months_tenure<15 then 0 else 1 end as tenure ,count(*) as accounts,sum(eligible_vespa_analysis_account) as vespa_analysis_accounts
from v190_all_active_accounts
group by tenure
order by tenure
;
commit;

---Add On Package/Affluence/Stability Segment/Box Details---

select top 100 * from v190_top_x_channels_summary;
update v190_top_x_channels_summary
set product_holding=b.product_holding
, tv_package=b.tv_package
,new_tv_package=b.new_tv_package
,box_type=b.box_type

---
,discontinuous_tenure_desc	=b.discontinuous_tenure_desc
,rtm	=b.rtm
,talk_package	=b.talk_package
,bb_package	=b.bb_package
,contribution_desc	=b.contribution_desc
,value_segment	=b.value_segment
,no_of_previous_ab	=b.no_of_previous_ab
,no_of_previous_pc	=b.no_of_previous_pc
,no_of_previous_po	=b.no_of_previous_po
,no_of_previous_sc	=b.no_of_previous_sc
,no_of_previous_ta	=b.no_of_previous_ta
,no_of_previous_ta_12m	=b.no_of_previous_ta_12m
,no_of_previous_pat	=b.no_of_previous_pat
,no_of_previous_pat_12m	=b.no_of_previous_pat_12m
,no_of_previous_ta_24m	=b.no_of_previous_ta_24m
,no_of_previous_ta_2m	=b.no_of_previous_ta_2m
,no_of_previous_ta_saved_2m	=b.no_of_previous_ta_saved_2m
,end_of_offer_flag	=b.end_of_offer_flag
,home_move_6m	=b.home_move_6m
,customer_management_segment	=b.customer_management_segment
,at_risk_segment	=b.at_risk_segment
,no_of_premium_upgrades	=b.no_of_premium_upgrades
,no_of_premium_downgrades	=b.no_of_premium_downgrades
,premiums_upgrade_flag	=b.premiums_upgrade_flag
,premiums_downgrade_flag	=b.premiums_downgrade_flag
,sky_go_usage	=b.sky_go_usage
,on_demand_usage	=b.on_demand_usage
,emails_sent	=b.emails_sent
,emails_opened	=b.emails_opened
,emails_clicked	=b.emails_clicked
,sports_days_available	=b.sports_days_available
,movies_days_available	=b.movies_days_available
,hd_days_available	=b.hd_days_available
,espn_days_available	=b.espn_days_available
,mutv_days_available	=b.mutv_days_available
,cfctv_days_available	=b.cfctv_days_available
,in_viewing_panel	=b.in_viewing_panel
,aggregated_at_risk_segment	=b.aggregated_at_risk_segment




from v190_top_x_channels_summary as a
left outer join hensirir.SK4637_Content_Initiative_Event_Base as b
on a.account_number = b.account_number
;
commit;


*/