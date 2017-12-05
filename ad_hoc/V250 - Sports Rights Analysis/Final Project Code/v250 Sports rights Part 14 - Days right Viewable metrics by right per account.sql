/*-----------------------------------------------------------------------------------------------------------------
        Project:V250 - Sports right Analysis Profiling
        Part - Part 14 Viewing summary
        
        Analyst: Dan Barnett
        SK Prod: 5
        Create Summary viewable per right for each account also with Live/Non Live version as well as overall
        Due to size issues accounts split into several tables, code run and results joined back together

*/------------------------------------------------------------------------------------------------------------------

alter table dbarnett.v250_days_viewing_by_account add account_weight real;
update dbarnett.v250_days_viewing_by_account
set account_weight=case when b.account_weight  is null then 0 else b.account_weight end
from dbarnett.v250_days_viewing_by_account as a
left outer join dbarnett.v250_annualised_activity_table_for_workshop as b
on a.account_number = b.account_number
;
commit;

--select count (distinct account_number) from dbarnett.v250_days_viewing_by_account where account_weight>0
--select count (*) from dbarnett.v250_days_viewing_by_account 
delete from dbarnett.v250_days_viewing_by_account where account_weight = 0; commit;
--drop table dbarnett.v250_days_viewing_by_account_00_to_04;
select * into dbarnett.v250_days_viewing_by_account_00_to_02
from dbarnett.v250_days_viewing_by_account
where right(account_number,1) in ('0','1','2')
;

commit;

commit;
CREATE HG INDEX idx1 ON dbarnett.v250_days_viewing_by_account_00_to_02 (account_number);
CREATE LF INDEX idx3 ON dbarnett.v250_days_viewing_by_account_00_to_02 (viewing_date);
commit;

select a.account_number
,b.analysis_right
,sum(days_with_viewing) as days_right_viewable
into dbarnett.v250_days_right_viewable_by_account_00_to_02
from dbarnett.v250_rights_broadcast_overall as b 
left outer join dbarnett.v250_days_viewing_by_account_00_to_02 as a
on b.broadcast_date=a.viewing_date
group by  a.account_number
,b.analysis_right
;
commit;

--drop table dbarnett.v250_days_viewing_by_account_03_to_06;

select * into dbarnett.v250_days_viewing_by_account_03_to_06
from dbarnett.v250_days_viewing_by_account
where right(account_number,1) in ('3','4','5')
;

commit;

commit;
CREATE HG INDEX idx1 ON dbarnett.v250_days_viewing_by_account_03_to_06 (account_number);
CREATE LF INDEX idx3 ON dbarnett.v250_days_viewing_by_account_03_to_06 (viewing_date);
commit;
delete from dbarnett.v250_days_viewing_by_account_03_to_06 where right(account_number,1) in ('6')
commit;
drop table dbarnett.v250_days_right_viewable_by_account_03_to_06;
select a.account_number
,b.analysis_right
,sum(days_with_viewing) as days_right_viewable
into dbarnett.v250_days_right_viewable_by_account_03_to_06
from dbarnett.v250_rights_broadcast_overall as b 
left outer join dbarnett.v250_days_viewing_by_account_03_to_06 as a
on b.broadcast_date=a.viewing_date
group by  a.account_number
,b.analysis_right
;
commit;

drop table dbarnett.v250_days_viewing_by_account_03_to_06;

--select count(*) from  dbarnett.v250_days_viewing_by_account_03_to_06
select a.account_number
,b.analysis_right
,sum(days_with_viewing) as days_right_viewable
into dbarnett.v250_days_right_viewable_by_account_v09
from dbarnett.v250_rights_broadcast_overall as b 
left outer join dbarnett.v250_days_viewing_by_account as a
on b.broadcast_date=a.viewing_date
where right(account_number,1)='9'
group by  a.account_number
,b.analysis_right
;

commit;

select a.account_number
,b.analysis_right
,sum(days_with_viewing) as days_right_viewable
into dbarnett.v250_days_right_viewable_by_account_v08
from dbarnett.v250_rights_broadcast_overall as b 
left outer join dbarnett.v250_days_viewing_by_account as a
on b.broadcast_date=a.viewing_date
where right(account_number,1)='8'
group by  a.account_number
,b.analysis_right
;

commit;

--drop table dbarnett.v250_days_viewing_by_account_04_to_09;
select * into dbarnett.v250_days_viewing_by_account_06_to_07
from dbarnett.v250_days_viewing_by_account
where right(account_number,1) in ('6','7')
;

commit;

commit;
CREATE HG INDEX idx1 ON dbarnett.v250_days_viewing_by_account_06_to_07 (account_number);
CREATE LF INDEX idx3 ON dbarnett.v250_days_viewing_by_account_06_to_07 (viewing_date);
commit;

select a.account_number
,b.analysis_right
,sum(days_with_viewing) as days_right_viewable
into dbarnett.v250_days_right_viewable_by_account_06_to_07
from dbarnett.v250_rights_broadcast_overall as b 
left outer join dbarnett.v250_days_viewing_by_account_06_to_07 as a
on b.broadcast_date=a.viewing_date
group by  a.account_number
,b.analysis_right
;
commit;

drop table dbarnett.v250_days_viewing_by_account_06_to_07;
commit;

---Append together---

insert into dbarnett.v250_days_right_viewable_by_account_v09
(select * from dbarnett.v250_days_right_viewable_by_account_v08)
;
commit;


insert into dbarnett.v250_days_right_viewable_by_account_v09
(select * from dbarnett.v250_days_right_viewable_by_account_06_to_07)
;
commit;

insert into dbarnett.v250_days_right_viewable_by_account_v09
(select * from dbarnett.v250_days_right_viewable_by_account_00_to_02)
;
commit;

--select right(account_number,1) as acno ,count(*) from dbarnett.v250_days_right_viewable_by_account_v09 group by acno order by acno;


drop table dbarnett.v250_days_right_viewable_by_account_03_to_06;
select a.account_number
,b.analysis_right
,sum(days_with_viewing) as days_right_viewable
into dbarnett.v250_days_right_viewable_by_account_03_to_06
from dbarnett.v250_rights_broadcast_overall as b 
left outer join dbarnett.v250_days_viewing_by_account_03_to_06 as a
on b.broadcast_date=a.viewing_date
group by  a.account_number
,b.analysis_right
;
commit;

insert into dbarnett.v250_days_right_viewable_by_account_v09
(select * from dbarnett.v250_days_right_viewable_by_account_03_to_06)
;
commit;


commit;
CREATE HG INDEX idx1 ON dbarnett.v250_days_right_viewable_by_account_v09 (account_number);
CREATE LF INDEX idx2 ON dbarnett.v250_days_right_viewable_by_account_v09 (analysis_right);
commit;

--select days_right_viewable,count(*) from  dbarnett.v250_days_right_viewable_by_account_v09 where analysis_right = 'Premier League Football - Sky Sports (MNF)' group by days_right_viewable order by days_right_viewable;
--select count(*) from dbarnett.v250_rights_broadcast_overall;
---Calculate Number of Days each right (and Live non/Live split broadcast)---

commit;
CREATE LF INDEX idx1 ON dbarnett.v250_rights_broadcast_by_live_status (broadcast_date);
CREATE HG INDEX idx2 ON dbarnett.v250_rights_broadcast_by_live_status (analysis_right);
CREATE LF INDEX idx3 ON dbarnett.v250_rights_broadcast_by_live_status (live);
commit;
--drop table dbarnett.v250_days_right_viewable_by_account_by_live_status;
select a.account_number
,b.analysis_right
,b.live
,count(*) as days_right_viewable
into dbarnett.v250_days_right_viewable_by_account_by_live_status
from dbarnett.v250_days_viewing_by_account as a
left outer join dbarnett.v250_rights_broadcast_by_live_status as b
on a.viewing_date=b.broadcast_date
group by  a.account_number
,b.analysis_right
,b.live
;

commit;

--select top 100 * from dbarnett.v250_days_viewing_by_account;
--select count(*) from dbarnett.v250_rights_broadcast_overall;
---Calculate Number of Days each right (and Live non/Live split broadcast)---


--select distinct analysis_right from dbarnett.v250_days_right_viewable_by_account_by_live_status order by analysis_right
--select * from dbarnett.v250_sports_rights_epg_data_for_analysis  where analysis_right='NFL - BBC' order by live and live=1 
--select top 100 * from dbarnett.v250_days_right_viewable_by_account_by_live_status;

---Match Days Viewable to Days broadcast to get % of Content Accounting returning data for each right/account

--select top 100 * from dbarnett.v250_rights_broadcast_overall;
--drop table #summary_by_analysis_right;
select analysis_right
,count(distinct broadcast_date) as days_broadcast
,sum(total_broadcast) as right_broadcast_duration
,sum(programmes_broadcast) as total_programmes_broadcast
into #summary_by_analysis_right
from dbarnett.v250_rights_broadcast_overall
group by analysis_right
;

commit;
--select * from #summary_by_analysis_right;
CREATE HG INDEX idx1 ON #summary_by_analysis_right (analysis_right);
commit;
--drop table #summary_by_analysis_right_by_live_status;
select analysis_right
,live
,count(distinct broadcast_date) as days_broadcast
,sum(total_broadcast) as right_broadcast_duration
,sum(programmes_broadcast) as total_programmes_broadcast
into #summary_by_analysis_right_by_live_status
from dbarnett.v250_rights_broadcast_by_live_status
group by analysis_right
,live
;
commit;
--select * from  dbarnett.v250_rights_broadcast_by_live_status where analysis_right='NFL - BBC'
--select * from #summary_by_analysis_right_by_live_status;
--select top 100 * from dbarnett.v250_days_right_viewable_by_account
commit;
--select count(*) from dbarnett.v250_days_right_viewable_by_account_v09;
alter table dbarnett.v250_days_right_viewable_by_account_v09 add days_right_broadcast integer;
alter table dbarnett.v250_days_right_viewable_by_account_v09 add right_broadcast_duration integer;
alter table dbarnett.v250_days_right_viewable_by_account_v09 add right_broadcast_programmes integer;

update dbarnett.v250_days_right_viewable_by_account_v09
set days_right_broadcast=b.days_broadcast
,right_broadcast_duration=b.right_broadcast_duration
,right_broadcast_programmes=b.total_programmes_broadcast
from dbarnett.v250_days_right_viewable_by_account_v09 as a
left outer join #summary_by_analysis_right as b
on a.analysis_right=b.analysis_right;
--select top 100 * from dbarnett.v250_days_right_viewable_by_account_by_live_status;
commit;
--select * from dbarnett.v250_days_right_viewable_by_account where analysis_right = 'World Club Championship - BBC';
---Edited 2nd Feb 2014

alter table dbarnett.v250_days_right_viewable_by_account_by_live_status add days_right_broadcast integer;
alter table dbarnett.v250_days_right_viewable_by_account_by_live_status add right_broadcast_duration integer;
alter table dbarnett.v250_days_right_viewable_by_account_by_live_status add right_broadcast_programmes integer;

update dbarnett.v250_days_right_viewable_by_account_by_live_status
set days_right_broadcast=b.days_broadcast
,right_broadcast_duration=b.right_broadcast_duration
,right_broadcast_programmes=b.total_programmes_broadcast
from dbarnett.v250_days_right_viewable_by_account_by_live_status as a
left outer join #summary_by_analysis_right_by_live_status as b
on a.analysis_right=b.analysis_right and a.live=b.live;

commit;


--select top 100 * from  dbarnett.v250_master_account_list as a
--select top 100 * from  dbarnett.v250_unannualised_right_activity as a
--select top 100 * from  dbarnett.v250_days_right_viewable_by_account  as a
--,cast(total_viewing_duration_all as real) * 365 / cast(total_days_with_viewing as real) as annualised_total_viewing_duration_seconds
--pt4
--dbarnett.v250_days_right_viewable_by_account dbarnett.v250_days_right_viewable_by_account_by_live_status

commit;
CREATE HG INDEX idx1 ON dbarnett.v250_days_right_viewable_by_account (account_number);
commit;

--select top 100 * from dbarnett.v250_days_right_viewable_by_account ;
----Convert dbarnett.v250_days_right_viewable_by_account  to one record per account----
drop table dbarnett.v250_right_viewable_account_summary;
select account_number
,sum(case when analysis_right ='Africa Cup of Nations - Eurosport' then days_right_viewable else 0 end) 
as AFCEUR_days_right_viewable
,sum(case when analysis_right ='Africa Cup of Nations - ITV' then days_right_viewable else 0 end) 
as AFCITV_days_right_viewable
,sum(case when analysis_right ='Americas Cup - BBC' then days_right_viewable else 0 end) 
as AMCBBC_days_right_viewable
,sum(case when analysis_right ='Asian Tour Golf 2012-2013 Sky Sports' then days_right_viewable else 0 end) 
as ATGSS_days_right_viewable
,sum(case when analysis_right ='ATP Tennis Masters Series  2011-2013 Sky Sports' then days_right_viewable else 0 end) 
as ATPSS_days_right_viewable
,sum(case when analysis_right ='Australia Home Cricket  2012-2016 Sky Sports' then days_right_viewable else 0 end) 
as AHCSS_days_right_viewable
,sum(case when analysis_right ='Australian Football - BT Sport' then days_right_viewable else 0 end) 
as AUFBTS_days_right_viewable
,sum(case when analysis_right ='Australian Open Tennis - BBC' then days_right_viewable else 0 end) 
as AOTBBC_days_right_viewable
,sum(case when analysis_right ='Australian Open Tennis - Eurosport' then days_right_viewable else 0 end) 
as AOTEUR_days_right_viewable
,sum(case when analysis_right ='Aviva Premiership - ESPN' then days_right_viewable else 0 end) 
as AVPSS_days_right_viewable
,sum(case when analysis_right ='BBC American Football' then days_right_viewable else 0 end) 
as AFBBC_days_right_viewable
,sum(case when analysis_right ='BBC Athletics' then days_right_viewable else 0 end) 
as ATHBBC_days_right_viewable
,sum(case when analysis_right ='BBC Boxing' then days_right_viewable else 0 end) 
as BOXBBC_days_right_viewable
,sum(case when analysis_right ='BBC Darts' then days_right_viewable else 0 end) 
as DRTBBC_days_right_viewable
,sum(case when analysis_right ='BBC Equestrian' then days_right_viewable else 0 end) 
as EQUBBC_days_right_viewable
,sum(case when analysis_right ='BBC Football' then days_right_viewable else 0 end) 
as FOOTBBC_days_right_viewable
,sum(case when analysis_right ='BBC Golf' then days_right_viewable else 0 end) 
as GOLFBBC_days_right_viewable
,sum(case when analysis_right ='BBC Motor Sport' then days_right_viewable else 0 end) 
as MSPBBC_days_right_viewable
,sum(case when analysis_right ='BBC Rugby' then days_right_viewable else 0 end) 
as RUGBBC_days_right_viewable
,sum(case when analysis_right ='BBC Snooker/Pool' then days_right_viewable else 0 end) 
as SNPBBC_days_right_viewable
,sum(case when analysis_right ='BBC Tennis' then days_right_viewable else 0 end) 
as TENBBC_days_right_viewable
,sum(case when analysis_right ='BBC Unknown' then days_right_viewable else 0 end) 
as UNKBBC_days_right_viewable
,sum(case when analysis_right ='BBC Watersports' then days_right_viewable else 0 end) 
as WATBBC_days_right_viewable
,sum(case when analysis_right ='BBC Wintersports' then days_right_viewable else 0 end) 
as WINBBC_days_right_viewable
,sum(case when analysis_right ='Boxing  - Channel 5' then days_right_viewable else 0 end) 
as BOXCH5_days_right_viewable
,sum(case when analysis_right ='Boxing - Matchroom 2012-2014 Sky Sports' then days_right_viewable else 0 end) 
as BOXMSS_days_right_viewable
,sum(case when analysis_right ='Brazil Football - BT Sport' then days_right_viewable else 0 end) 
as BFTBTS_days_right_viewable
,sum(case when analysis_right ='British & Irish Lions 2013 - Sky Sports' then days_right_viewable else 0 end) 
as BILSS_days_right_viewable
,sum(case when analysis_right ='British Open Golf - BBC' then days_right_viewable else 0 end) 
as BOGSS_days_right_viewable
,sum(case when analysis_right ='BT Sport American Football' then days_right_viewable else 0 end) 
as AFBTS_days_right_viewable
,sum(case when analysis_right ='BT Sport Athletics' then days_right_viewable else 0 end) 
as ATHBTS_days_right_viewable
,sum(case when analysis_right ='BT Sport Baseball' then days_right_viewable else 0 end) 
as BASEBTS_days_right_viewable
,sum(case when analysis_right ='BT Sport Basketball' then days_right_viewable else 0 end) 
as BASKBTS_days_right_viewable
,sum(case when analysis_right ='BT Sport Boxing' then days_right_viewable else 0 end) 
as BOXBTS_days_right_viewable
,sum(case when analysis_right ='BT Sport Cricket' then days_right_viewable else 0 end) 
as CRIBTS_days_right_viewable
,sum(case when analysis_right ='BT Sport Equestrian' then days_right_viewable else 0 end) 
as EQUBTS_days_right_viewable
,sum(case when analysis_right ='BT Sport Extreme' then days_right_viewable else 0 end) 
as EXTBTS_days_right_viewable
,sum(case when analysis_right ='BT Sport Football' then days_right_viewable else 0 end) 
as FOOTBTS_days_right_viewable
,sum(case when analysis_right ='BT Sport Motor Sport' then days_right_viewable else 0 end) 
as MSPBTS_days_right_viewable
,sum(case when analysis_right ='BT Sport Rugby' then days_right_viewable else 0 end) 
as RUGBTS_days_right_viewable
,sum(case when analysis_right ='BT Sport Tennis' then days_right_viewable else 0 end) 
as TENBTS_days_right_viewable
,sum(case when analysis_right ='BT Sport Unknown' then days_right_viewable else 0 end) 
as UNKBTS_days_right_viewable
,sum(case when analysis_right ='BT Sport Wintersports' then days_right_viewable else 0 end) 
as WINBTS_days_right_viewable
,sum(case when analysis_right ='Bundesliga - BT Sport' then days_right_viewable else 0 end) 
as BUNBTS_days_right_viewable
,sum(case when analysis_right ='Bundesliga- ESPN' then days_right_viewable else 0 end) 
as BUNESPN_days_right_viewable
,sum(case when analysis_right ='Challenge Darts' then days_right_viewable else 0 end) 
as DRTCHA_days_right_viewable
,sum(case when analysis_right ='Challenge Extreme' then days_right_viewable else 0 end) 
as EXTCHA_days_right_viewable
,sum(case when analysis_right ='Challenge Unknown' then days_right_viewable else 0 end) 
as UNKCHA_days_right_viewable
,sum(case when analysis_right ='Challenge Wrestling' then days_right_viewable else 0 end) 
as WRECHA_days_right_viewable
,sum(case when analysis_right ='Champions League - ITV' then days_right_viewable else 0 end) 
as CHLITV_days_right_viewable
,sum(case when analysis_right ='Champions Trophy - I C C Cricket Deal  2012-2015 Sky Sports' then days_right_viewable else 0 end) 
as ICCSS_days_right_viewable
,sum(case when analysis_right ='Channel 4 American Football' then days_right_viewable else 0 end) 
as AMCH4_days_right_viewable
,sum(case when analysis_right ='Channel 4 Athletics' then days_right_viewable else 0 end) 
as ATHCH4_days_right_viewable
,sum(case when analysis_right ='Channel 4 Boxing' then days_right_viewable else 0 end) 
as BOXCH4_days_right_viewable
,sum(case when analysis_right ='Channel 4 Equestrian' then days_right_viewable else 0 end) 
as EQUCH4_days_right_viewable
,sum(case when analysis_right ='Channel 4 Extreme' then days_right_viewable else 0 end) 
as EXTCH4_days_right_viewable
,sum(case when analysis_right ='Channel 4 Football' then days_right_viewable else 0 end) 
as FOOTCH4_days_right_viewable
,sum(case when analysis_right ='Channel 4 Motor Sport' then days_right_viewable else 0 end) 
as MSPCH4_days_right_viewable
,sum(case when analysis_right ='Channel 4 Racing' then days_right_viewable else 0 end) 
as RACCH4_days_right_viewable
,sum(case when analysis_right ='Channel 4 Unknown' then days_right_viewable else 0 end) 
as UNKCH4_days_right_viewable
,sum(case when analysis_right ='Channel 4 Watersports' then days_right_viewable else 0 end) 
as WATCH4_days_right_viewable
,sum(case when analysis_right ='Channel 4 Wintersports' then days_right_viewable else 0 end) 
as WINCH4_days_right_viewable
,sum(case when analysis_right ='Channel 5 Athletics' then days_right_viewable else 0 end) 
as ATHCH5_days_right_viewable
,sum(case when analysis_right ='Channel 5 Boxing' then days_right_viewable else 0 end) 
as BOXOCH5_days_right_viewable
,sum(case when analysis_right ='Channel 5 Cricket' then days_right_viewable else 0 end) 
as CRICH5_days_right_viewable
,sum(case when analysis_right ='Channel 5 Motor Sport' then days_right_viewable else 0 end) 
as MSPCH5_days_right_viewable
,sum(case when analysis_right ='Channel 5 Unknown' then days_right_viewable else 0 end) 
as UNKCH5_days_right_viewable
,sum(case when analysis_right ='Channel 5 Wrestling' then days_right_viewable else 0 end) 
as WRECH5_days_right_viewable
,sum(case when analysis_right ='Cheltenham Festival - Channel 4' then days_right_viewable else 0 end) 
as CHELCH4_days_right_viewable
,sum(case when analysis_right ='Community Shield - ITV' then days_right_viewable else 0 end) 
as CMSITV_days_right_viewable
,sum(case when analysis_right ='Confederations Cup - BBC' then days_right_viewable else 0 end) 
as CONCBBC_days_right_viewable
,sum(case when analysis_right ='Conference - BT Sport' then days_right_viewable else 0 end) 
as CONFBTS_days_right_viewable
,sum(case when analysis_right ='Cycling - La Vuelta ITV' then days_right_viewable else 0 end) 
as CLVITV_days_right_viewable
,sum(case when analysis_right ='Cycling - U C I World Tour Sky Sports' then days_right_viewable else 0 end) 
as CUCISS_days_right_viewable
,sum(case when analysis_right ='Cycling Tour of Britain - Eurosport' then days_right_viewable else 0 end) 
as CTBEUR_days_right_viewable
,sum(case when analysis_right ='Cycling: tour of britain ITV4' then days_right_viewable else 0 end) 
as CTCITV_days_right_viewable
,sum(case when analysis_right ='Derby - Channel 4' then days_right_viewable else 0 end) 
as DERCH4_days_right_viewable
,sum(case when analysis_right ='ECB (highlights) - Channel 5' then days_right_viewable else 0 end) 
as ECBHCH5_days_right_viewable
,sum(case when analysis_right ='ECB Cricket Sky Sports' then days_right_viewable else 0 end) 
as GECRSS_days_right_viewable
,sum(case when analysis_right ='ECB non-Test Cricket Sky Sports' then days_right_viewable else 0 end) 
as ECBNSS_days_right_viewable
,sum(case when analysis_right ='ECB Test Cricket Sky Sports' then days_right_viewable else 0 end) 
as ECBTSS_days_right_viewable
,sum(case when analysis_right ='England Football Internationals - ITV' then days_right_viewable else 0 end) 
as GENGITV_days_right_viewable
,sum(case when analysis_right ='England Friendlies (Football) - ITV' then days_right_viewable else 0 end) 
as EFRITV_days_right_viewable
,sum(case when analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then days_right_viewable else 0 end) 
as ENRSS_days_right_viewable
,sum(case when analysis_right ='England World Cup Qualifying (Away) - ITV' then days_right_viewable else 0 end) 
as EWQAITV_days_right_viewable
,sum(case when analysis_right ='England World Cup Qualifying (Home) - ITV' then days_right_viewable else 0 end) 
as EWQHITV_days_right_viewable
,sum(case when analysis_right ='ESPN American Football' then days_right_viewable else 0 end) 
as AMESPN_days_right_viewable
,sum(case when analysis_right ='ESPN Athletics' then days_right_viewable else 0 end) 
as ATHESPN_days_right_viewable
,sum(case when analysis_right ='ESPN Baseball' then days_right_viewable else 0 end) 
as BASEESPN_days_right_viewable
,sum(case when analysis_right ='ESPN Basketball' then days_right_viewable else 0 end) 
as BASKESPN_days_right_viewable
,sum(case when analysis_right ='ESPN Boxing' then days_right_viewable else 0 end) 
as BOXESPN_days_right_viewable
,sum(case when analysis_right ='ESPN Cricket' then days_right_viewable else 0 end) 
as CRIESPN_days_right_viewable
,sum(case when analysis_right ='ESPN Darts' then days_right_viewable else 0 end) 
as DARTESPN_days_right_viewable
,sum(case when analysis_right ='ESPN Equestrian' then days_right_viewable else 0 end) 
as EQUESPN_days_right_viewable
,sum(case when analysis_right ='ESPN Extreme' then days_right_viewable else 0 end) 
as EXTESPN_days_right_viewable
,sum(case when analysis_right ='ESPN Football' then days_right_viewable else 0 end) 
as FOOTESPN_days_right_viewable
,sum(case when analysis_right ='ESPN Golf' then days_right_viewable else 0 end) 
as GOLFESPN_days_right_viewable
,sum(case when analysis_right ='ESPN Ice Hockey' then days_right_viewable else 0 end) 
as IHESPN_days_right_viewable
,sum(case when analysis_right ='ESPN Motor Sport' then days_right_viewable else 0 end) 
as MSPESPN_days_right_viewable
,sum(case when analysis_right ='ESPN Racing' then days_right_viewable else 0 end) 
as RACESPN_days_right_viewable
,sum(case when analysis_right ='ESPN Rugby' then days_right_viewable else 0 end) 
as RUGESPN_days_right_viewable
,sum(case when analysis_right ='ESPN Snooker/Pool' then days_right_viewable else 0 end) 
as SNPESPN_days_right_viewable
,sum(case when analysis_right ='ESPN Tennis' then days_right_viewable else 0 end) 
as TENESPN_days_right_viewable
,sum(case when analysis_right ='ESPN Unknown' then days_right_viewable else 0 end) 
as UNKESPN_days_right_viewable
,sum(case when analysis_right ='ESPN Watersports' then days_right_viewable else 0 end) 
as WATESPN_days_right_viewable
,sum(case when analysis_right ='ESPN Wintersports' then days_right_viewable else 0 end) 
as WINESPN_days_right_viewable
,sum(case when analysis_right ='ESPN Wrestling' then days_right_viewable else 0 end) 
as WREESPN_days_right_viewable
,sum(case when analysis_right ='Europa League - BT Sport' then days_right_viewable else 0 end) 
as ELBTSP_days_right_viewable
,sum(case when analysis_right ='Europa League - ESPN' then days_right_viewable else 0 end) 
as ELESPN_days_right_viewable
,sum(case when analysis_right ='Europa League - ITV' then days_right_viewable else 0 end) 
as ELITV_days_right_viewable
,sum(case when analysis_right ='European Tour Golf - Sky Sports' then days_right_viewable else 0 end) 
as ETGSS_days_right_viewable
,sum(case when analysis_right ='Eurosport American Football' then days_right_viewable else 0 end) 
as AMEUR_days_right_viewable
,sum(case when analysis_right ='Eurosport Athletics' then days_right_viewable else 0 end) 
as ATHEUR_days_right_viewable
,sum(case when analysis_right ='Eurosport Baseball' then days_right_viewable else 0 end) 
as BASEEUR_days_right_viewable
,sum(case when analysis_right ='Eurosport Basketball' then days_right_viewable else 0 end) 
as BASKEUR_days_right_viewable
,sum(case when analysis_right ='Eurosport Boxing' then days_right_viewable else 0 end) 
as BOXEUR_days_right_viewable
,sum(case when analysis_right ='Eurosport Cricket' then days_right_viewable else 0 end) 
as CRIEUR_days_right_viewable
,sum(case when analysis_right ='Eurosport Darts' then days_right_viewable else 0 end) 
as DARTEUR_days_right_viewable
,sum(case when analysis_right ='Eurosport Equestrian' then days_right_viewable else 0 end) 
as EQUEUR_days_right_viewable
,sum(case when analysis_right ='Eurosport Extreme' then days_right_viewable else 0 end) 
as EXTEUR_days_right_viewable
,sum(case when analysis_right ='Eurosport Football' then days_right_viewable else 0 end) 
as FOOTEUR_days_right_viewable
,sum(case when analysis_right ='Eurosport Golf' then days_right_viewable else 0 end) 
as GOLFEUR_days_right_viewable
,sum(case when analysis_right ='Eurosport Ice Hockey' then days_right_viewable else 0 end) 
as IHEUR_days_right_viewable
,sum(case when analysis_right ='Eurosport Motor Sport' then days_right_viewable else 0 end) 
as MSPEUR_days_right_viewable
,sum(case when analysis_right ='Eurosport Racing' then days_right_viewable else 0 end) 
as RACEUR_days_right_viewable
,sum(case when analysis_right ='Eurosport Snooker/Pool' then days_right_viewable else 0 end) 
as SNPEUR_days_right_viewable
,sum(case when analysis_right ='Eurosport Tennis' then days_right_viewable else 0 end) 
as TENEUR_days_right_viewable
,sum(case when analysis_right ='Eurosport Unknown' then days_right_viewable else 0 end) 
as UNKEUR_days_right_viewable
,sum(case when analysis_right ='Eurosport Watersports' then days_right_viewable else 0 end) 
as WATEUR_days_right_viewable
,sum(case when analysis_right ='Eurosport Wintersports' then days_right_viewable else 0 end) 
as WINEUR_days_right_viewable
,sum(case when analysis_right ='F1 - BBC' then days_right_viewable else 0 end) 
as GF1BBC_days_right_viewable
,sum(case when analysis_right ='F1 - Sky Sports' then days_right_viewable else 0 end) 
as GF1SS_days_right_viewable
,sum(case when analysis_right ='F1 (non-Live)- BBC' then days_right_viewable else 0 end) 
as F1NBBC_days_right_viewable
,sum(case when analysis_right ='F1 (Practice Live)- BBC' then days_right_viewable else 0 end) 
as F1PBBC_days_right_viewable
,sum(case when analysis_right ='F1 (Qualifying Live)- BBC' then days_right_viewable else 0 end) 
as F1QBBC_days_right_viewable
,sum(case when analysis_right ='F1 (Race Live)- BBC' then days_right_viewable else 0 end) 
as F1RBBC_days_right_viewable
,sum(case when analysis_right ='FA Cup - ESPN' then days_right_viewable else 0 end) 
as FACESPN_days_right_viewable
,sum(case when analysis_right ='FA Cup - ITV' then days_right_viewable else 0 end) 
as FACITV_days_right_viewable
,sum(case when analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then days_right_viewable else 0 end) 
as FLCCSS_days_right_viewable
,sum(case when analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - others' then days_right_viewable else 0 end) 
as FLOTSS_days_right_viewable
,sum(case when analysis_right ='Formula One 2012-2018 - (non-Live) Sky Sports' then days_right_viewable else 0 end) 
as F1NSS_days_right_viewable
,sum(case when analysis_right ='Formula One 2012-2018 - (Practice Live) Sky Sports' then days_right_viewable else 0 end) 
as F1PSS_days_right_viewable
,sum(case when analysis_right ='Formula One 2012-2018 - (Qualifying Live) Sky Sports' then days_right_viewable else 0 end) 
as F1QSS_days_right_viewable
,sum(case when analysis_right ='Formula One 2012-2018 - (Race Live) Sky Sports' then days_right_viewable else 0 end) 
as F1RSS_days_right_viewable
,sum(case when analysis_right ='French Open Tennis - Eurosport' then days_right_viewable else 0 end) 
as FOTEUR_days_right_viewable
,sum(case when analysis_right ='French Open Tennis - ITV' then days_right_viewable else 0 end) 
as FOTITV_days_right_viewable
,sum(case when analysis_right ='Grand National - Channel 4' then days_right_viewable else 0 end) 
as GDNCH4_days_right_viewable
,sum(case when analysis_right ='Heineken Cup 2010/11 To 2013/14- Sky Sports' then days_right_viewable else 0 end) 
as HECSS_days_right_viewable
,sum(case when analysis_right ='I R B Sevens 2008/09 -2014/15 - Sky Sports' then days_right_viewable else 0 end) 
as IRBSS_days_right_viewable
,sum(case when analysis_right ='IAAF World Athletics Championship - Eurosport' then days_right_viewable else 0 end) 
as WACEUR_days_right_viewable
,sum(case when analysis_right ='India Home Cricket 2012-2018 Sky Sports' then days_right_viewable else 0 end) 
as IHCSS_days_right_viewable
,sum(case when analysis_right ='India Premier League - ITV' then days_right_viewable else 0 end) 
as IPLITV_days_right_viewable
,sum(case when analysis_right ='International Freindlies - ESPN' then days_right_viewable else 0 end) 
as IFESPN_days_right_viewable
,sum(case when analysis_right ='International Friendlies - BT Sport' then days_right_viewable else 0 end) 
as IFBTS_days_right_viewable
,sum(case when analysis_right ='ITV1 Boxing' then days_right_viewable else 0 end) 
as BOXITV1_days_right_viewable
,sum(case when analysis_right ='ITV1 Football' then days_right_viewable else 0 end) 
as FOOTITV1_days_right_viewable
,sum(case when analysis_right ='ITV1 Motor Sport' then days_right_viewable else 0 end) 
as MOTSITV1_days_right_viewable
,sum(case when analysis_right ='ITV1 Rugby' then days_right_viewable else 0 end) 
as RUGITV1_days_right_viewable
,sum(case when analysis_right ='ITV1 Snooker/Pool' then days_right_viewable else 0 end) 
as SNPITV1_days_right_viewable
,sum(case when analysis_right ='ITV1 Unknown' then days_right_viewable else 0 end) 
as UNKITV1_days_right_viewable
,sum(case when analysis_right ='ITV4 Boxing' then days_right_viewable else 0 end) 
as BOXITV4_days_right_viewable
,sum(case when analysis_right ='ITV4 Cricket' then days_right_viewable else 0 end) 
as CRIITV4_days_right_viewable
,sum(case when analysis_right ='ITV4 Darts' then days_right_viewable else 0 end) 
as DARTITV4_days_right_viewable
,sum(case when analysis_right ='ITV4 Extreme' then days_right_viewable else 0 end) 
as EXTITV4_days_right_viewable
,sum(case when analysis_right ='ITV4 Football' then days_right_viewable else 0 end) 
as FOOTITV4_days_right_viewable
,sum(case when analysis_right ='ITV4 Motor Sport' then days_right_viewable else 0 end) 
as MSPITV4_days_right_viewable
,sum(case when analysis_right ='ITV4 Rugby' then days_right_viewable else 0 end) 
as RUGITV4_days_right_viewable
,sum(case when analysis_right ='ITV4 Snooker/Pool' then days_right_viewable else 0 end) 
as SNPITV4_days_right_viewable
,sum(case when analysis_right ='ITV4 Tennis' then days_right_viewable else 0 end) 
as TENITV4_days_right_viewable
,sum(case when analysis_right ='ITV4 Unknown' then days_right_viewable else 0 end) 
as UNKITV4_days_right_viewable
,sum(case when analysis_right ='Ligue 1 - BT Sport' then days_right_viewable else 0 end) 
as L1BTS_days_right_viewable
,sum(case when analysis_right ='Ligue 1 - ESPN' then days_right_viewable else 0 end) 
as L1ESPN_days_right_viewable
,sum(case when analysis_right ='Match of the day - BBC' then days_right_viewable else 0 end) 
as MOTDBBC_days_right_viewable
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Other)- Sky Sports' then days_right_viewable else 0 end) 
as MROSS_days_right_viewable
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Pool)- Sky Sports' then days_right_viewable else 0 end) 
as MRPSS_days_right_viewable
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Snooker)- Sky Sports' then days_right_viewable else 0 end) 
as MRSSS_days_right_viewable
,sum(case when analysis_right ='Moto GP BBC' then days_right_viewable else 0 end) 
as MGPBBC_days_right_viewable
,sum(case when analysis_right ='NBA - Sky Sports' then days_right_viewable else 0 end) 
as NBASS_days_right_viewable
,sum(case when analysis_right ='NFL - BBC' then days_right_viewable else 0 end) 
as NFLBBC_days_right_viewable
,sum(case when analysis_right ='NFL - Channel 4' then days_right_viewable else 0 end) 
as NFLCH4_days_right_viewable
,sum(case when analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then days_right_viewable else 0 end) 
as NFLSS_days_right_viewable
,sum(case when analysis_right ='Northern Ireland Football Association 2012-2014 - Sky Sports' then days_right_viewable else 0 end) 
as NIFSS_days_right_viewable
,sum(case when analysis_right ='Oaks - Channel 4' then days_right_viewable else 0 end) 
as OAKCH4_days_right_viewable
,sum(case when analysis_right ='Other American Football' then days_right_viewable else 0 end) 
as AMOTH_days_right_viewable
,sum(case when analysis_right ='Other Athletics' then days_right_viewable else 0 end) 
as ATHOTH_days_right_viewable
,sum(case when analysis_right ='Other Baseball' then days_right_viewable else 0 end) 
as BASEOTH_days_right_viewable
,sum(case when analysis_right ='Other Basketball' then days_right_viewable else 0 end) 
as BASKOTH_days_right_viewable
,sum(case when analysis_right ='Other Boxing' then days_right_viewable else 0 end) 
as BOXOTH_days_right_viewable
,sum(case when analysis_right ='Other Cricket' then days_right_viewable else 0 end) 
as CRIOTH_days_right_viewable
,sum(case when analysis_right ='Other Darts' then days_right_viewable else 0 end) 
as DARTOTH_days_right_viewable
,sum(case when analysis_right ='Other Equestrian' then days_right_viewable else 0 end) 
as EQUOTH_days_right_viewable
,sum(case when analysis_right ='Other Extreme' then days_right_viewable else 0 end) 
as EXTOTH_days_right_viewable
,sum(case when analysis_right ='Other Fishing' then days_right_viewable else 0 end) 
as FSHOTH_days_right_viewable
,sum(case when analysis_right ='Other Football' then days_right_viewable else 0 end) 
as FOOTOTH_days_right_viewable
,sum(case when analysis_right ='Other Golf' then days_right_viewable else 0 end) 
as GOLFOTH_days_right_viewable
,sum(case when analysis_right ='Other Ice Hockey' then days_right_viewable else 0 end) 
as IHOTH_days_right_viewable
,sum(case when analysis_right ='Other Motor Sport' then days_right_viewable else 0 end) 
as MSPOTH_days_right_viewable
,sum(case when analysis_right ='Other Racing' then days_right_viewable else 0 end) 
as RACOTH_days_right_viewable
,sum(case when analysis_right ='Other Rugby' then days_right_viewable else 0 end) 
as RUGOTH_days_right_viewable
,sum(case when analysis_right ='Other Rugby Internationals - ESPN' then days_right_viewable else 0 end) 
as ORUGESPN_days_right_viewable
,sum(case when analysis_right ='Other Snooker/Pool' then days_right_viewable else 0 end) 
as OTHSNP_days_right_viewable
,sum(case when analysis_right ='Other Tennis' then days_right_viewable else 0 end) 
as OTHTEN_days_right_viewable
,sum(case when analysis_right ='Other Unknown' then days_right_viewable else 0 end) 
as OTHUNK_days_right_viewable
,sum(case when analysis_right ='Other Watersports' then days_right_viewable else 0 end) 
as OTHWAT_days_right_viewable
,sum(case when analysis_right ='Other Wintersports' then days_right_viewable else 0 end) 
as OTHWIN_days_right_viewable
,sum(case when analysis_right ='Other Wrestling' then days_right_viewable else 0 end) 
as OTHWRE_days_right_viewable
,sum(case when analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then days_right_viewable else 0 end) 
as PGASS_days_right_viewable
,sum(case when analysis_right ='Premier League - BT Sport' then days_right_viewable else 0 end) 
as PLBTS_days_right_viewable
,sum(case when analysis_right ='Premier League - ESPN' then days_right_viewable else 0 end) 
as PLESPN_days_right_viewable
,sum(case when analysis_right ='Premier League Darts 2010-2013 - Sky Sports' then days_right_viewable else 0 end) 
as PLDSS_days_right_viewable
,sum(case when analysis_right ='Premier League Football - Sky Sports' then days_right_viewable else 0 end) 
as GPLSS_days_right_viewable
,sum(case when analysis_right ='Premier League Football - Sky Sports (Match Choice)' then days_right_viewable else 0 end) 
as PLMCSS_days_right_viewable
,sum(case when analysis_right ='Premier League Football - Sky Sports (MNF)' then days_right_viewable else 0 end) 
as PLMNFSS_days_right_viewable
,sum(case when analysis_right ='Premier League Football - Sky Sports (non Live)' then days_right_viewable else 0 end) 
as PLNLSS_days_right_viewable
,sum(case when analysis_right ='Premier League Football - Sky Sports (other Live)' then days_right_viewable else 0 end) 
as PLOLSS_days_right_viewable
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sat Lunchtime)' then days_right_viewable else 0 end) 
as PLSLSS_days_right_viewable
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sat Night Live)' then days_right_viewable else 0 end) 
as PLSNSS_days_right_viewable
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sun 4pm)' then days_right_viewable else 0 end) 
as PLS4SS_days_right_viewable
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sun Lunchtime)' then days_right_viewable else 0 end) 
as PLSULSS_days_right_viewable
,sum(case when analysis_right ='Premiership Rugby - Sky Sports' then days_right_viewable else 0 end) 
as PRUSS_days_right_viewable
,sum(case when analysis_right ='Republic Of Ireland FA - 2010/11 To 2013/14 - Sky Sports' then days_right_viewable else 0 end) 
as ROISS_days_right_viewable
,sum(case when analysis_right ='Royal Ascot - Channel 4' then days_right_viewable else 0 end) 
as RASCH4_days_right_viewable
,sum(case when analysis_right ='Rugby Internationals (England) - BBC' then days_right_viewable else 0 end) 
as RIEBBC_days_right_viewable
,sum(case when analysis_right ='Rugby Internationals (Ireland) - BBC' then days_right_viewable else 0 end) 
as RIIBBC_days_right_viewable
,sum(case when analysis_right ='Rugby Internationals (Scotland) - BBC' then days_right_viewable else 0 end) 
as RISBBC_days_right_viewable
,sum(case when analysis_right ='Rugby Internationals (Wales) - BBC' then days_right_viewable else 0 end) 
as RIWBBC_days_right_viewable
,sum(case when analysis_right ='Rugby League  Challenge Cup- BBC' then days_right_viewable else 0 end) 
as RLCCBBC_days_right_viewable
,sum(case when analysis_right ='Rugby League - Sky Sports' then days_right_viewable else 0 end) 
as RLGSS_days_right_viewable
,sum(case when analysis_right ='Rugby League  World Cup- BBC' then days_right_viewable else 0 end) 
as RLWCBBC_days_right_viewable
,sum(case when analysis_right ='Sanzar Rugby Union 2011 - 2015 - Sky Sports' then days_right_viewable else 0 end) 
as SARUSS_days_right_viewable
,sum(case when analysis_right ='Scottish FA Deal - 2010/11 To 2013/14 - Sky Sports' then days_right_viewable else 0 end) 
as SFASS_days_right_viewable
,sum(case when analysis_right ='Serie A - BT Sport' then days_right_viewable else 0 end) 
as SABTS_days_right_viewable
,sum(case when analysis_right ='Serie A - ESPN' then days_right_viewable else 0 end) 
as SAESPN_days_right_viewable
,sum(case when analysis_right ='SFL - ESPN' then days_right_viewable else 0 end) 
as SFLESPN_days_right_viewable
,sum(case when analysis_right ='Six Nations - BBC' then days_right_viewable else 0 end) 
as SNRBBC_days_right_viewable
,sum(case when analysis_right ='Sky 1 and Sky 2 Boxing' then days_right_viewable else 0 end) 
as BOXS12_days_right_viewable
,sum(case when analysis_right ='Sky 1 and Sky 2 Football' then days_right_viewable else 0 end) 
as FOOTS12_days_right_viewable
,sum(case when analysis_right ='Sky 1 and Sky 2 Motor Sport' then days_right_viewable else 0 end) 
as MSPS12_days_right_viewable
,sum(case when analysis_right ='Sky 1 and Sky 2 Unknown' then days_right_viewable else 0 end) 
as UNKS12_days_right_viewable
,sum(case when analysis_right ='Sky 1 and Sky 2 Wrestling' then days_right_viewable else 0 end) 
as WRES12_days_right_viewable
,sum(case when analysis_right ='Sky Sports American Football' then days_right_viewable else 0 end) 
as AMSS_days_right_viewable
,sum(case when analysis_right ='Sky Sports Athletics' then days_right_viewable else 0 end) 
as ATHSS_days_right_viewable
,sum(case when analysis_right ='Sky Sports Baseball' then days_right_viewable else 0 end) 
as BASESS_days_right_viewable
,sum(case when analysis_right ='Sky Sports Basketball' then days_right_viewable else 0 end) 
as BASKSS_days_right_viewable
,sum(case when analysis_right ='Sky Sports Boxing' then days_right_viewable else 0 end) 
as BOXSS_days_right_viewable
,sum(case when analysis_right ='Sky Sports Cricket' then days_right_viewable else 0 end) 
as CRISS_days_right_viewable
,sum(case when analysis_right ='Sky Sports Darts' then days_right_viewable else 0 end) 
as DARTSS_days_right_viewable
,sum(case when analysis_right ='Sky Sports Equestrian' then days_right_viewable else 0 end) 
as EQUSS_days_right_viewable
,sum(case when analysis_right ='Sky Sports Extreme' then days_right_viewable else 0 end) 
as EXTSS_days_right_viewable
,sum(case when analysis_right ='Sky Sports Fishing' then days_right_viewable else 0 end) 
as FISHSS_days_right_viewable
,sum(case when analysis_right ='Sky Sports Football' then days_right_viewable else 0 end) 
as FOOTSS_days_right_viewable
,sum(case when analysis_right ='Sky Sports Golf' then days_right_viewable else 0 end) 
as GOLFSS_days_right_viewable
,sum(case when analysis_right ='Sky Sports Ice Hockey' then days_right_viewable else 0 end) 
as IHSS_days_right_viewable
,sum(case when analysis_right ='Sky Sports Motor Sport' then days_right_viewable else 0 end) 
as MSPSS_days_right_viewable
,sum(case when analysis_right ='Sky Sports Racing' then days_right_viewable else 0 end) 
as RACSS_days_right_viewable
,sum(case when analysis_right ='Sky Sports Rugby' then days_right_viewable else 0 end) 
as RUGSS_days_right_viewable
,sum(case when analysis_right ='Sky Sports Snooker/Pool' then days_right_viewable else 0 end) 
as SNPSS_days_right_viewable
,sum(case when analysis_right ='Sky Sports Tennis' then days_right_viewable else 0 end) 
as TENSS_days_right_viewable
,sum(case when analysis_right ='Sky Sports Unknown' then days_right_viewable else 0 end) 
as UNKSS_days_right_viewable
,sum(case when analysis_right ='Sky Sports Watersports' then days_right_viewable else 0 end) 
as WATSS_days_right_viewable
,sum(case when analysis_right ='Sky Sports Wintersports' then days_right_viewable else 0 end) 
as WINSS_days_right_viewable
,sum(case when analysis_right ='Sky Sports Wrestling' then days_right_viewable else 0 end) 
as WRESS_days_right_viewable
,sum(case when analysis_right ='Solheim Cup 2013 Ladies Golf - Sky Sports' then days_right_viewable else 0 end) 
as SOLSS_days_right_viewable
,sum(case when analysis_right ='South African Home Cricket  2012-2020 Sky Sports' then days_right_viewable else 0 end) 
as SACSS_days_right_viewable
,sum(case when analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then days_right_viewable else 0 end) 
as SPFSS_days_right_viewable
,sum(case when analysis_right ='SPFL - BT Sport' then days_right_viewable else 0 end) 
as SPFLBTS_days_right_viewable
,sum(case when analysis_right ='SPL - ESPN' then days_right_viewable else 0 end) 
as SPLESPN_days_right_viewable
,sum(case when analysis_right ='SPL - Sky Sports' then days_right_viewable else 0 end) 
as SPLSS_days_right_viewable
,sum(case when analysis_right ='Sportfive World Cup 2014 Qualifers - Sky Sports' then days_right_viewable else 0 end) 
as SP5SS_days_right_viewable
,sum(case when analysis_right ='The boat race - BBC' then days_right_viewable else 0 end) 
as BTRBBC_days_right_viewable
,sum(case when analysis_right ='The football league show - BBC' then days_right_viewable else 0 end) 
as FLSBBC_days_right_viewable
,sum(case when analysis_right ='The Masters Golf - BBC' then days_right_viewable else 0 end) 
as MGBBC_days_right_viewable
,sum(case when analysis_right ='TNA Wrestling Challenge' then days_right_viewable else 0 end) 
as TNACHA_days_right_viewable
,sum(case when analysis_right ='Tour de France - Eurosport' then days_right_viewable else 0 end) 
as TDFEUR_days_right_viewable
,sum(case when analysis_right ='Tour de France - ITV' then days_right_viewable else 0 end) 
as TDFITV_days_right_viewable
,sum(case when analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then days_right_viewable else 0 end) 
as USMGSS_days_right_viewable
,sum(case when analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then days_right_viewable else 0 end) 
as USOTSS_days_right_viewable
,sum(case when analysis_right ='U.S Open/Womens/Seniors/Amateur Golf Sky Sports' then days_right_viewable else 0 end) 
as USOGSS_days_right_viewable
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports' then days_right_viewable else 0 end) 
as CLASS_days_right_viewable
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (non Live)' then days_right_viewable else 0 end) 
as CLNSS_days_right_viewable
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (other Live)' then days_right_viewable else 0 end) 
as CLOSS_days_right_viewable
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (Tue)' then days_right_viewable else 0 end) 
as CLTSS_days_right_viewable
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (Wed)' then days_right_viewable else 0 end) 
as CLWSS_days_right_viewable
,sum(case when analysis_right ='US Open Tennis - Eurosport' then days_right_viewable else 0 end) 
as USOTEUR_days_right_viewable
,sum(case when analysis_right ='USA Football - BT Sport' then days_right_viewable else 0 end) 
as USFBTS_days_right_viewable
,sum(case when analysis_right ='USPGA Championship (2007-2016) Sky Sports' then days_right_viewable else 0 end) 
as USPGASS_days_right_viewable
,sum(case when analysis_right ='WCQ - ESPN' then days_right_viewable else 0 end) 
as WCQESPN_days_right_viewable
,sum(case when analysis_right ='Welsh Home International Football 2012-2014 Sky Sports' then days_right_viewable else 0 end) 
as WIFSS_days_right_viewable
,sum(case when analysis_right ='West Indies Home Cricket 2013-2020 Sky Sports' then days_right_viewable else 0 end) 
as WICSS_days_right_viewable
,sum(case when analysis_right ='Wimbledon - BBC' then days_right_viewable else 0 end) 
as WIMBBC_days_right_viewable
,sum(case when analysis_right ='Womens Cricket - I C C Cricket Deal  2012-2015 Sky Sports' then days_right_viewable else 0 end) 
as WICCSS_days_right_viewable
,sum(case when analysis_right ='World Athletics Championship - More 4' then days_right_viewable else 0 end) 
as WACMR4_days_right_viewable
,sum(case when analysis_right ='World Club Championship - BBC' then days_right_viewable else 0 end) 
as WCLBBBC_days_right_viewable
,sum(case when analysis_right ='World Cup Qualifiers - BT Sport' then days_right_viewable else 0 end) 
as WCQBTS_days_right_viewable
,sum(case when analysis_right ='World Darts Championship 2009-2012 Sky Sports' then days_right_viewable else 0 end) 
as WDCSS_days_right_viewable
,sum(case when analysis_right ='World snooker championship - BBC' then days_right_viewable else 0 end) 
as WSCBBC_days_right_viewable
,sum(case when analysis_right ='WWE Sky 1 and 2' then days_right_viewable else 0 end) 
as WWES12_days_right_viewable
,sum(case when analysis_right ='WWE Sky Sports' then days_right_viewable else 0 end) 
as WWESS_days_right_viewable
,sum(case when analysis_right ='Africa Cup of Nations - Eurosport' then days_right_broadcast else 0 end) 
as AFCEUR_days_right_broadcast
,sum(case when analysis_right ='Africa Cup of Nations - ITV' then days_right_broadcast else 0 end) 
as AFCITV_days_right_broadcast
,sum(case when analysis_right ='Americas Cup - BBC' then days_right_broadcast else 0 end) 
as AMCBBC_days_right_broadcast
,sum(case when analysis_right ='Asian Tour Golf 2012-2013 Sky Sports' then days_right_broadcast else 0 end) 
as ATGSS_days_right_broadcast
,sum(case when analysis_right ='ATP Tennis Masters Series  2011-2013 Sky Sports' then days_right_broadcast else 0 end) 
as ATPSS_days_right_broadcast
,sum(case when analysis_right ='Australia Home Cricket  2012-2016 Sky Sports' then days_right_broadcast else 0 end) 
as AHCSS_days_right_broadcast
,sum(case when analysis_right ='Australian Football - BT Sport' then days_right_broadcast else 0 end) 
as AUFBTS_days_right_broadcast
,sum(case when analysis_right ='Australian Open Tennis - BBC' then days_right_broadcast else 0 end) 
as AOTBBC_days_right_broadcast
,sum(case when analysis_right ='Australian Open Tennis - Eurosport' then days_right_broadcast else 0 end) 
as AOTEUR_days_right_broadcast
,sum(case when analysis_right ='Aviva Premiership - ESPN' then days_right_broadcast else 0 end) 
as AVPSS_days_right_broadcast
,sum(case when analysis_right ='BBC American Football' then days_right_broadcast else 0 end) 
as AFBBC_days_right_broadcast
,sum(case when analysis_right ='BBC Athletics' then days_right_broadcast else 0 end) 
as ATHBBC_days_right_broadcast
,sum(case when analysis_right ='BBC Boxing' then days_right_broadcast else 0 end) 
as BOXBBC_days_right_broadcast
,sum(case when analysis_right ='BBC Darts' then days_right_broadcast else 0 end) 
as DRTBBC_days_right_broadcast
,sum(case when analysis_right ='BBC Equestrian' then days_right_broadcast else 0 end) 
as EQUBBC_days_right_broadcast
,sum(case when analysis_right ='BBC Football' then days_right_broadcast else 0 end) 
as FOOTBBC_days_right_broadcast
,sum(case when analysis_right ='BBC Golf' then days_right_broadcast else 0 end) 
as GOLFBBC_days_right_broadcast
,sum(case when analysis_right ='BBC Motor Sport' then days_right_broadcast else 0 end) 
as MSPBBC_days_right_broadcast
,sum(case when analysis_right ='BBC Rugby' then days_right_broadcast else 0 end) 
as RUGBBC_days_right_broadcast
,sum(case when analysis_right ='BBC Snooker/Pool' then days_right_broadcast else 0 end) 
as SNPBBC_days_right_broadcast
,sum(case when analysis_right ='BBC Tennis' then days_right_broadcast else 0 end) 
as TENBBC_days_right_broadcast
,sum(case when analysis_right ='BBC Unknown' then days_right_broadcast else 0 end) 
as UNKBBC_days_right_broadcast
,sum(case when analysis_right ='BBC Watersports' then days_right_broadcast else 0 end) 
as WATBBC_days_right_broadcast
,sum(case when analysis_right ='BBC Wintersports' then days_right_broadcast else 0 end) 
as WINBBC_days_right_broadcast
,sum(case when analysis_right ='Boxing  - Channel 5' then days_right_broadcast else 0 end) 
as BOXCH5_days_right_broadcast
,sum(case when analysis_right ='Boxing - Matchroom 2012-2014 Sky Sports' then days_right_broadcast else 0 end) 
as BOXMSS_days_right_broadcast
,sum(case when analysis_right ='Brazil Football - BT Sport' then days_right_broadcast else 0 end) 
as BFTBTS_days_right_broadcast
,sum(case when analysis_right ='British & Irish Lions 2013 - Sky Sports' then days_right_broadcast else 0 end) 
as BILSS_days_right_broadcast
,sum(case when analysis_right ='British Open Golf - BBC' then days_right_broadcast else 0 end) 
as BOGSS_days_right_broadcast
,sum(case when analysis_right ='BT Sport American Football' then days_right_broadcast else 0 end) 
as AFBTS_days_right_broadcast
,sum(case when analysis_right ='BT Sport Athletics' then days_right_broadcast else 0 end) 
as ATHBTS_days_right_broadcast
,sum(case when analysis_right ='BT Sport Baseball' then days_right_broadcast else 0 end) 
as BASEBTS_days_right_broadcast
,sum(case when analysis_right ='BT Sport Basketball' then days_right_broadcast else 0 end) 
as BASKBTS_days_right_broadcast
,sum(case when analysis_right ='BT Sport Boxing' then days_right_broadcast else 0 end) 
as BOXBTS_days_right_broadcast
,sum(case when analysis_right ='BT Sport Cricket' then days_right_broadcast else 0 end) 
as CRIBTS_days_right_broadcast
,sum(case when analysis_right ='BT Sport Equestrian' then days_right_broadcast else 0 end) 
as EQUBTS_days_right_broadcast
,sum(case when analysis_right ='BT Sport Extreme' then days_right_broadcast else 0 end) 
as EXTBTS_days_right_broadcast
,sum(case when analysis_right ='BT Sport Football' then days_right_broadcast else 0 end) 
as FOOTBTS_days_right_broadcast
,sum(case when analysis_right ='BT Sport Motor Sport' then days_right_broadcast else 0 end) 
as MSPBTS_days_right_broadcast
,sum(case when analysis_right ='BT Sport Rugby' then days_right_broadcast else 0 end) 
as RUGBTS_days_right_broadcast
,sum(case when analysis_right ='BT Sport Tennis' then days_right_broadcast else 0 end) 
as TENBTS_days_right_broadcast
,sum(case when analysis_right ='BT Sport Unknown' then days_right_broadcast else 0 end) 
as UNKBTS_days_right_broadcast
,sum(case when analysis_right ='BT Sport Wintersports' then days_right_broadcast else 0 end) 
as WINBTS_days_right_broadcast
,sum(case when analysis_right ='Bundesliga - BT Sport' then days_right_broadcast else 0 end) 
as BUNBTS_days_right_broadcast
,sum(case when analysis_right ='Bundesliga- ESPN' then days_right_broadcast else 0 end) 
as BUNESPN_days_right_broadcast
,sum(case when analysis_right ='Challenge Darts' then days_right_broadcast else 0 end) 
as DRTCHA_days_right_broadcast
,sum(case when analysis_right ='Challenge Extreme' then days_right_broadcast else 0 end) 
as EXTCHA_days_right_broadcast
,sum(case when analysis_right ='Challenge Unknown' then days_right_broadcast else 0 end) 
as UNKCHA_days_right_broadcast
,sum(case when analysis_right ='Challenge Wrestling' then days_right_broadcast else 0 end) 
as WRECHA_days_right_broadcast
,sum(case when analysis_right ='Champions League - ITV' then days_right_broadcast else 0 end) 
as CHLITV_days_right_broadcast
,sum(case when analysis_right ='Champions Trophy - I C C Cricket Deal  2012-2015 Sky Sports' then days_right_broadcast else 0 end) 
as ICCSS_days_right_broadcast
,sum(case when analysis_right ='Channel 4 American Football' then days_right_broadcast else 0 end) 
as AMCH4_days_right_broadcast
,sum(case when analysis_right ='Channel 4 Athletics' then days_right_broadcast else 0 end) 
as ATHCH4_days_right_broadcast
,sum(case when analysis_right ='Channel 4 Boxing' then days_right_broadcast else 0 end) 
as BOXCH4_days_right_broadcast
,sum(case when analysis_right ='Channel 4 Equestrian' then days_right_broadcast else 0 end) 
as EQUCH4_days_right_broadcast
,sum(case when analysis_right ='Channel 4 Extreme' then days_right_broadcast else 0 end) 
as EXTCH4_days_right_broadcast
,sum(case when analysis_right ='Channel 4 Football' then days_right_broadcast else 0 end) 
as FOOTCH4_days_right_broadcast
,sum(case when analysis_right ='Channel 4 Motor Sport' then days_right_broadcast else 0 end) 
as MSPCH4_days_right_broadcast
,sum(case when analysis_right ='Channel 4 Racing' then days_right_broadcast else 0 end) 
as RACCH4_days_right_broadcast
,sum(case when analysis_right ='Channel 4 Unknown' then days_right_broadcast else 0 end) 
as UNKCH4_days_right_broadcast
,sum(case when analysis_right ='Channel 4 Watersports' then days_right_broadcast else 0 end) 
as WATCH4_days_right_broadcast
,sum(case when analysis_right ='Channel 4 Wintersports' then days_right_broadcast else 0 end) 
as WINCH4_days_right_broadcast
,sum(case when analysis_right ='Channel 5 Athletics' then days_right_broadcast else 0 end) 
as ATHCH5_days_right_broadcast
,sum(case when analysis_right ='Channel 5 Boxing' then days_right_broadcast else 0 end) 
as BOXOCH5_days_right_broadcast
,sum(case when analysis_right ='Channel 5 Cricket' then days_right_broadcast else 0 end) 
as CRICH5_days_right_broadcast
,sum(case when analysis_right ='Channel 5 Motor Sport' then days_right_broadcast else 0 end) 
as MSPCH5_days_right_broadcast
,sum(case when analysis_right ='Channel 5 Unknown' then days_right_broadcast else 0 end) 
as UNKCH5_days_right_broadcast
,sum(case when analysis_right ='Channel 5 Wrestling' then days_right_broadcast else 0 end) 
as WRECH5_days_right_broadcast
,sum(case when analysis_right ='Cheltenham Festival - Channel 4' then days_right_broadcast else 0 end) 
as CHELCH4_days_right_broadcast
,sum(case when analysis_right ='Community Shield - ITV' then days_right_broadcast else 0 end) 
as CMSITV_days_right_broadcast
,sum(case when analysis_right ='Confederations Cup - BBC' then days_right_broadcast else 0 end) 
as CONCBBC_days_right_broadcast
,sum(case when analysis_right ='Conference - BT Sport' then days_right_broadcast else 0 end) 
as CONFBTS_days_right_broadcast
,sum(case when analysis_right ='Cycling - La Vuelta ITV' then days_right_broadcast else 0 end) 
as CLVITV_days_right_broadcast
,sum(case when analysis_right ='Cycling - U C I World Tour Sky Sports' then days_right_broadcast else 0 end) 
as CUCISS_days_right_broadcast
,sum(case when analysis_right ='Cycling Tour of Britain - Eurosport' then days_right_broadcast else 0 end) 
as CTBEUR_days_right_broadcast
,sum(case when analysis_right ='Cycling: tour of britain ITV4' then days_right_broadcast else 0 end) 
as CTCITV_days_right_broadcast
,sum(case when analysis_right ='Derby - Channel 4' then days_right_broadcast else 0 end) 
as DERCH4_days_right_broadcast
,sum(case when analysis_right ='ECB (highlights) - Channel 5' then days_right_broadcast else 0 end) 
as ECBHCH5_days_right_broadcast
,sum(case when analysis_right ='ECB Cricket Sky Sports' then days_right_broadcast else 0 end) 
as GECRSS_days_right_broadcast
,sum(case when analysis_right ='ECB non-Test Cricket Sky Sports' then days_right_broadcast else 0 end) 
as ECBNSS_days_right_broadcast
,sum(case when analysis_right ='ECB Test Cricket Sky Sports' then days_right_broadcast else 0 end) 
as ECBTSS_days_right_broadcast
,sum(case when analysis_right ='England Football Internationals - ITV' then days_right_broadcast else 0 end) 
as GENGITV_days_right_broadcast
,sum(case when analysis_right ='England Friendlies (Football) - ITV' then days_right_broadcast else 0 end) 
as EFRITV_days_right_broadcast
,sum(case when analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then days_right_broadcast else 0 end) 
as ENRSS_days_right_broadcast
,sum(case when analysis_right ='England World Cup Qualifying (Away) - ITV' then days_right_broadcast else 0 end) 
as EWQAITV_days_right_broadcast
,sum(case when analysis_right ='England World Cup Qualifying (Home) - ITV' then days_right_broadcast else 0 end) 
as EWQHITV_days_right_broadcast
,sum(case when analysis_right ='ESPN American Football' then days_right_broadcast else 0 end) 
as AMESPN_days_right_broadcast
,sum(case when analysis_right ='ESPN Athletics' then days_right_broadcast else 0 end) 
as ATHESPN_days_right_broadcast
,sum(case when analysis_right ='ESPN Baseball' then days_right_broadcast else 0 end) 
as BASEESPN_days_right_broadcast
,sum(case when analysis_right ='ESPN Basketball' then days_right_broadcast else 0 end) 
as BASKESPN_days_right_broadcast
,sum(case when analysis_right ='ESPN Boxing' then days_right_broadcast else 0 end) 
as BOXESPN_days_right_broadcast
,sum(case when analysis_right ='ESPN Cricket' then days_right_broadcast else 0 end) 
as CRIESPN_days_right_broadcast
,sum(case when analysis_right ='ESPN Darts' then days_right_broadcast else 0 end) 
as DARTESPN_days_right_broadcast
,sum(case when analysis_right ='ESPN Equestrian' then days_right_broadcast else 0 end) 
as EQUESPN_days_right_broadcast
,sum(case when analysis_right ='ESPN Extreme' then days_right_broadcast else 0 end) 
as EXTESPN_days_right_broadcast
,sum(case when analysis_right ='ESPN Football' then days_right_broadcast else 0 end) 
as FOOTESPN_days_right_broadcast
,sum(case when analysis_right ='ESPN Golf' then days_right_broadcast else 0 end) 
as GOLFESPN_days_right_broadcast
,sum(case when analysis_right ='ESPN Ice Hockey' then days_right_broadcast else 0 end) 
as IHESPN_days_right_broadcast
,sum(case when analysis_right ='ESPN Motor Sport' then days_right_broadcast else 0 end) 
as MSPESPN_days_right_broadcast
,sum(case when analysis_right ='ESPN Racing' then days_right_broadcast else 0 end) 
as RACESPN_days_right_broadcast
,sum(case when analysis_right ='ESPN Rugby' then days_right_broadcast else 0 end) 
as RUGESPN_days_right_broadcast
,sum(case when analysis_right ='ESPN Snooker/Pool' then days_right_broadcast else 0 end) 
as SNPESPN_days_right_broadcast
,sum(case when analysis_right ='ESPN Tennis' then days_right_broadcast else 0 end) 
as TENESPN_days_right_broadcast
,sum(case when analysis_right ='ESPN Unknown' then days_right_broadcast else 0 end) 
as UNKESPN_days_right_broadcast
,sum(case when analysis_right ='ESPN Watersports' then days_right_broadcast else 0 end) 
as WATESPN_days_right_broadcast
,sum(case when analysis_right ='ESPN Wintersports' then days_right_broadcast else 0 end) 
as WINESPN_days_right_broadcast
,sum(case when analysis_right ='ESPN Wrestling' then days_right_broadcast else 0 end) 
as WREESPN_days_right_broadcast
,sum(case when analysis_right ='Europa League - BT Sport' then days_right_broadcast else 0 end) 
as ELBTSP_days_right_broadcast
,sum(case when analysis_right ='Europa League - ESPN' then days_right_broadcast else 0 end) 
as ELESPN_days_right_broadcast
,sum(case when analysis_right ='Europa League - ITV' then days_right_broadcast else 0 end) 
as ELITV_days_right_broadcast
,sum(case when analysis_right ='European Tour Golf - Sky Sports' then days_right_broadcast else 0 end) 
as ETGSS_days_right_broadcast
,sum(case when analysis_right ='Eurosport American Football' then days_right_broadcast else 0 end) 
as AMEUR_days_right_broadcast
,sum(case when analysis_right ='Eurosport Athletics' then days_right_broadcast else 0 end) 
as ATHEUR_days_right_broadcast
,sum(case when analysis_right ='Eurosport Baseball' then days_right_broadcast else 0 end) 
as BASEEUR_days_right_broadcast
,sum(case when analysis_right ='Eurosport Basketball' then days_right_broadcast else 0 end) 
as BASKEUR_days_right_broadcast
,sum(case when analysis_right ='Eurosport Boxing' then days_right_broadcast else 0 end) 
as BOXEUR_days_right_broadcast
,sum(case when analysis_right ='Eurosport Cricket' then days_right_broadcast else 0 end) 
as CRIEUR_days_right_broadcast
,sum(case when analysis_right ='Eurosport Darts' then days_right_broadcast else 0 end) 
as DARTEUR_days_right_broadcast
,sum(case when analysis_right ='Eurosport Equestrian' then days_right_broadcast else 0 end) 
as EQUEUR_days_right_broadcast
,sum(case when analysis_right ='Eurosport Extreme' then days_right_broadcast else 0 end) 
as EXTEUR_days_right_broadcast
,sum(case when analysis_right ='Eurosport Football' then days_right_broadcast else 0 end) 
as FOOTEUR_days_right_broadcast
,sum(case when analysis_right ='Eurosport Golf' then days_right_broadcast else 0 end) 
as GOLFEUR_days_right_broadcast
,sum(case when analysis_right ='Eurosport Ice Hockey' then days_right_broadcast else 0 end) 
as IHEUR_days_right_broadcast
,sum(case when analysis_right ='Eurosport Motor Sport' then days_right_broadcast else 0 end) 
as MSPEUR_days_right_broadcast
,sum(case when analysis_right ='Eurosport Racing' then days_right_broadcast else 0 end) 
as RACEUR_days_right_broadcast
,sum(case when analysis_right ='Eurosport Snooker/Pool' then days_right_broadcast else 0 end) 
as SNPEUR_days_right_broadcast
,sum(case when analysis_right ='Eurosport Tennis' then days_right_broadcast else 0 end) 
as TENEUR_days_right_broadcast
,sum(case when analysis_right ='Eurosport Unknown' then days_right_broadcast else 0 end) 
as UNKEUR_days_right_broadcast
,sum(case when analysis_right ='Eurosport Watersports' then days_right_broadcast else 0 end) 
as WATEUR_days_right_broadcast
,sum(case when analysis_right ='Eurosport Wintersports' then days_right_broadcast else 0 end) 
as WINEUR_days_right_broadcast
,sum(case when analysis_right ='F1 - BBC' then days_right_broadcast else 0 end) 
as GF1BBC_days_right_broadcast
,sum(case when analysis_right ='F1 - Sky Sports' then days_right_broadcast else 0 end) 
as GF1SS_days_right_broadcast
,sum(case when analysis_right ='F1 (non-Live)- BBC' then days_right_broadcast else 0 end) 
as F1NBBC_days_right_broadcast
,sum(case when analysis_right ='F1 (Practice Live)- BBC' then days_right_broadcast else 0 end) 
as F1PBBC_days_right_broadcast
,sum(case when analysis_right ='F1 (Qualifying Live)- BBC' then days_right_broadcast else 0 end) 
as F1QBBC_days_right_broadcast
,sum(case when analysis_right ='F1 (Race Live)- BBC' then days_right_broadcast else 0 end) 
as F1RBBC_days_right_broadcast
,sum(case when analysis_right ='FA Cup - ESPN' then days_right_broadcast else 0 end) 
as FACESPN_days_right_broadcast
,sum(case when analysis_right ='FA Cup - ITV' then days_right_broadcast else 0 end) 
as FACITV_days_right_broadcast
,sum(case when analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then days_right_broadcast else 0 end) 
as FLCCSS_days_right_broadcast
,sum(case when analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - others' then days_right_broadcast else 0 end) 
as FLOTSS_days_right_broadcast
,sum(case when analysis_right ='Formula One 2012-2018 - (non-Live) Sky Sports' then days_right_broadcast else 0 end) 
as F1NSS_days_right_broadcast
,sum(case when analysis_right ='Formula One 2012-2018 - (Practice Live) Sky Sports' then days_right_broadcast else 0 end) 
as F1PSS_days_right_broadcast
,sum(case when analysis_right ='Formula One 2012-2018 - (Qualifying Live) Sky Sports' then days_right_broadcast else 0 end) 
as F1QSS_days_right_broadcast
,sum(case when analysis_right ='Formula One 2012-2018 - (Race Live) Sky Sports' then days_right_broadcast else 0 end) 
as F1RSS_days_right_broadcast
,sum(case when analysis_right ='French Open Tennis - Eurosport' then days_right_broadcast else 0 end) 
as FOTEUR_days_right_broadcast
,sum(case when analysis_right ='French Open Tennis - ITV' then days_right_broadcast else 0 end) 
as FOTITV_days_right_broadcast
,sum(case when analysis_right ='Grand National - Channel 4' then days_right_broadcast else 0 end) 
as GDNCH4_days_right_broadcast
,sum(case when analysis_right ='Heineken Cup 2010/11 To 2013/14- Sky Sports' then days_right_broadcast else 0 end) 
as HECSS_days_right_broadcast
,sum(case when analysis_right ='I R B Sevens 2008/09 -2014/15 - Sky Sports' then days_right_broadcast else 0 end) 
as IRBSS_days_right_broadcast
,sum(case when analysis_right ='IAAF World Athletics Championship - Eurosport' then days_right_broadcast else 0 end) 
as WACEUR_days_right_broadcast
,sum(case when analysis_right ='India Home Cricket 2012-2018 Sky Sports' then days_right_broadcast else 0 end) 
as IHCSS_days_right_broadcast
,sum(case when analysis_right ='India Premier League - ITV' then days_right_broadcast else 0 end) 
as IPLITV_days_right_broadcast
,sum(case when analysis_right ='International Freindlies - ESPN' then days_right_broadcast else 0 end) 
as IFESPN_days_right_broadcast
,sum(case when analysis_right ='International Friendlies - BT Sport' then days_right_broadcast else 0 end) 
as IFBTS_days_right_broadcast
,sum(case when analysis_right ='ITV1 Boxing' then days_right_broadcast else 0 end) 
as BOXITV1_days_right_broadcast
,sum(case when analysis_right ='ITV1 Football' then days_right_broadcast else 0 end) 
as FOOTITV1_days_right_broadcast
,sum(case when analysis_right ='ITV1 Motor Sport' then days_right_broadcast else 0 end) 
as MOTSITV1_days_right_broadcast
,sum(case when analysis_right ='ITV1 Rugby' then days_right_broadcast else 0 end) 
as RUGITV1_days_right_broadcast
,sum(case when analysis_right ='ITV1 Snooker/Pool' then days_right_broadcast else 0 end) 
as SNPITV1_days_right_broadcast
,sum(case when analysis_right ='ITV1 Unknown' then days_right_broadcast else 0 end) 
as UNKITV1_days_right_broadcast
,sum(case when analysis_right ='ITV4 Boxing' then days_right_broadcast else 0 end) 
as BOXITV4_days_right_broadcast
,sum(case when analysis_right ='ITV4 Cricket' then days_right_broadcast else 0 end) 
as CRIITV4_days_right_broadcast
,sum(case when analysis_right ='ITV4 Darts' then days_right_broadcast else 0 end) 
as DARTITV4_days_right_broadcast
,sum(case when analysis_right ='ITV4 Extreme' then days_right_broadcast else 0 end) 
as EXTITV4_days_right_broadcast
,sum(case when analysis_right ='ITV4 Football' then days_right_broadcast else 0 end) 
as FOOTITV4_days_right_broadcast
,sum(case when analysis_right ='ITV4 Motor Sport' then days_right_broadcast else 0 end) 
as MSPITV4_days_right_broadcast
,sum(case when analysis_right ='ITV4 Rugby' then days_right_broadcast else 0 end) 
as RUGITV4_days_right_broadcast
,sum(case when analysis_right ='ITV4 Snooker/Pool' then days_right_broadcast else 0 end) 
as SNPITV4_days_right_broadcast
,sum(case when analysis_right ='ITV4 Tennis' then days_right_broadcast else 0 end) 
as TENITV4_days_right_broadcast
,sum(case when analysis_right ='ITV4 Unknown' then days_right_broadcast else 0 end) 
as UNKITV4_days_right_broadcast
,sum(case when analysis_right ='Ligue 1 - BT Sport' then days_right_broadcast else 0 end) 
as L1BTS_days_right_broadcast
,sum(case when analysis_right ='Ligue 1 - ESPN' then days_right_broadcast else 0 end) 
as L1ESPN_days_right_broadcast
,sum(case when analysis_right ='Match of the day - BBC' then days_right_broadcast else 0 end) 
as MOTDBBC_days_right_broadcast
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Other)- Sky Sports' then days_right_broadcast else 0 end) 
as MROSS_days_right_broadcast
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Pool)- Sky Sports' then days_right_broadcast else 0 end) 
as MRPSS_days_right_broadcast
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Snooker)- Sky Sports' then days_right_broadcast else 0 end) 
as MRSSS_days_right_broadcast
,sum(case when analysis_right ='Moto GP BBC' then days_right_broadcast else 0 end) 
as MGPBBC_days_right_broadcast
,sum(case when analysis_right ='NBA - Sky Sports' then days_right_broadcast else 0 end) 
as NBASS_days_right_broadcast
,sum(case when analysis_right ='NFL - BBC' then days_right_broadcast else 0 end) 
as NFLBBC_days_right_broadcast
,sum(case when analysis_right ='NFL - Channel 4' then days_right_broadcast else 0 end) 
as NFLCH4_days_right_broadcast
,sum(case when analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then days_right_broadcast else 0 end) 
as NFLSS_days_right_broadcast
,sum(case when analysis_right ='Northern Ireland Football Association 2012-2014 - Sky Sports' then days_right_broadcast else 0 end) 
as NIFSS_days_right_broadcast
,sum(case when analysis_right ='Oaks - Channel 4' then days_right_broadcast else 0 end) 
as OAKCH4_days_right_broadcast
,sum(case when analysis_right ='Other American Football' then days_right_broadcast else 0 end) 
as AMOTH_days_right_broadcast
,sum(case when analysis_right ='Other Athletics' then days_right_broadcast else 0 end) 
as ATHOTH_days_right_broadcast
,sum(case when analysis_right ='Other Baseball' then days_right_broadcast else 0 end) 
as BASEOTH_days_right_broadcast
,sum(case when analysis_right ='Other Basketball' then days_right_broadcast else 0 end) 
as BASKOTH_days_right_broadcast
,sum(case when analysis_right ='Other Boxing' then days_right_broadcast else 0 end) 
as BOXOTH_days_right_broadcast
,sum(case when analysis_right ='Other Cricket' then days_right_broadcast else 0 end) 
as CRIOTH_days_right_broadcast
,sum(case when analysis_right ='Other Darts' then days_right_broadcast else 0 end) 
as DARTOTH_days_right_broadcast
,sum(case when analysis_right ='Other Equestrian' then days_right_broadcast else 0 end) 
as EQUOTH_days_right_broadcast
,sum(case when analysis_right ='Other Extreme' then days_right_broadcast else 0 end) 
as EXTOTH_days_right_broadcast
,sum(case when analysis_right ='Other Fishing' then days_right_broadcast else 0 end) 
as FSHOTH_days_right_broadcast
,sum(case when analysis_right ='Other Football' then days_right_broadcast else 0 end) 
as FOOTOTH_days_right_broadcast
,sum(case when analysis_right ='Other Golf' then days_right_broadcast else 0 end) 
as GOLFOTH_days_right_broadcast
,sum(case when analysis_right ='Other Ice Hockey' then days_right_broadcast else 0 end) 
as IHOTH_days_right_broadcast
,sum(case when analysis_right ='Other Motor Sport' then days_right_broadcast else 0 end) 
as MSPOTH_days_right_broadcast
,sum(case when analysis_right ='Other Racing' then days_right_broadcast else 0 end) 
as RACOTH_days_right_broadcast
,sum(case when analysis_right ='Other Rugby' then days_right_broadcast else 0 end) 
as RUGOTH_days_right_broadcast
,sum(case when analysis_right ='Other Rugby Internationals - ESPN' then days_right_broadcast else 0 end) 
as ORUGESPN_days_right_broadcast
,sum(case when analysis_right ='Other Snooker/Pool' then days_right_broadcast else 0 end) 
as OTHSNP_days_right_broadcast
,sum(case when analysis_right ='Other Tennis' then days_right_broadcast else 0 end) 
as OTHTEN_days_right_broadcast
,sum(case when analysis_right ='Other Unknown' then days_right_broadcast else 0 end) 
as OTHUNK_days_right_broadcast
,sum(case when analysis_right ='Other Watersports' then days_right_broadcast else 0 end) 
as OTHWAT_days_right_broadcast
,sum(case when analysis_right ='Other Wintersports' then days_right_broadcast else 0 end) 
as OTHWIN_days_right_broadcast
,sum(case when analysis_right ='Other Wrestling' then days_right_broadcast else 0 end) 
as OTHWRE_days_right_broadcast
,sum(case when analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then days_right_broadcast else 0 end) 
as PGASS_days_right_broadcast
,sum(case when analysis_right ='Premier League - BT Sport' then days_right_broadcast else 0 end) 
as PLBTS_days_right_broadcast
,sum(case when analysis_right ='Premier League - ESPN' then days_right_broadcast else 0 end) 
as PLESPN_days_right_broadcast
,sum(case when analysis_right ='Premier League Darts 2010-2013 - Sky Sports' then days_right_broadcast else 0 end) 
as PLDSS_days_right_broadcast
,sum(case when analysis_right ='Premier League Football - Sky Sports' then days_right_broadcast else 0 end) 
as GPLSS_days_right_broadcast
,sum(case when analysis_right ='Premier League Football - Sky Sports (Match Choice)' then days_right_broadcast else 0 end) 
as PLMCSS_days_right_broadcast
,sum(case when analysis_right ='Premier League Football - Sky Sports (MNF)' then days_right_broadcast else 0 end) 
as PLMNFSS_days_right_broadcast
,sum(case when analysis_right ='Premier League Football - Sky Sports (non Live)' then days_right_broadcast else 0 end) 
as PLNLSS_days_right_broadcast
,sum(case when analysis_right ='Premier League Football - Sky Sports (other Live)' then days_right_broadcast else 0 end) 
as PLOLSS_days_right_broadcast
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sat Lunchtime)' then days_right_broadcast else 0 end) 
as PLSLSS_days_right_broadcast
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sat Night Live)' then days_right_broadcast else 0 end) 
as PLSNSS_days_right_broadcast
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sun 4pm)' then days_right_broadcast else 0 end) 
as PLS4SS_days_right_broadcast
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sun Lunchtime)' then days_right_broadcast else 0 end) 
as PLSULSS_days_right_broadcast
,sum(case when analysis_right ='Premiership Rugby - Sky Sports' then days_right_broadcast else 0 end) 
as PRUSS_days_right_broadcast
,sum(case when analysis_right ='Republic Of Ireland FA - 2010/11 To 2013/14 - Sky Sports' then days_right_broadcast else 0 end) 
as ROISS_days_right_broadcast
,sum(case when analysis_right ='Royal Ascot - Channel 4' then days_right_broadcast else 0 end) 
as RASCH4_days_right_broadcast
,sum(case when analysis_right ='Rugby Internationals (England) - BBC' then days_right_broadcast else 0 end) 
as RIEBBC_days_right_broadcast
,sum(case when analysis_right ='Rugby Internationals (Ireland) - BBC' then days_right_broadcast else 0 end) 
as RIIBBC_days_right_broadcast
,sum(case when analysis_right ='Rugby Internationals (Scotland) - BBC' then days_right_broadcast else 0 end) 
as RISBBC_days_right_broadcast
,sum(case when analysis_right ='Rugby Internationals (Wales) - BBC' then days_right_broadcast else 0 end) 
as RIWBBC_days_right_broadcast
,sum(case when analysis_right ='Rugby League  Challenge Cup- BBC' then days_right_broadcast else 0 end) 
as RLCCBBC_days_right_broadcast
,sum(case when analysis_right ='Rugby League - Sky Sports' then days_right_broadcast else 0 end) 
as RLGSS_days_right_broadcast
,sum(case when analysis_right ='Rugby League  World Cup- BBC' then days_right_broadcast else 0 end) 
as RLWCBBC_days_right_broadcast
,sum(case when analysis_right ='Sanzar Rugby Union 2011 - 2015 - Sky Sports' then days_right_broadcast else 0 end) 
as SARUSS_days_right_broadcast
,sum(case when analysis_right ='Scottish FA Deal - 2010/11 To 2013/14 - Sky Sports' then days_right_broadcast else 0 end) 
as SFASS_days_right_broadcast
,sum(case when analysis_right ='Serie A - BT Sport' then days_right_broadcast else 0 end) 
as SABTS_days_right_broadcast
,sum(case when analysis_right ='Serie A - ESPN' then days_right_broadcast else 0 end) 
as SAESPN_days_right_broadcast
,sum(case when analysis_right ='SFL - ESPN' then days_right_broadcast else 0 end) 
as SFLESPN_days_right_broadcast
,sum(case when analysis_right ='Six Nations - BBC' then days_right_broadcast else 0 end) 
as SNRBBC_days_right_broadcast
,sum(case when analysis_right ='Sky 1 and Sky 2 Boxing' then days_right_broadcast else 0 end) 
as BOXS12_days_right_broadcast
,sum(case when analysis_right ='Sky 1 and Sky 2 Football' then days_right_broadcast else 0 end) 
as FOOTS12_days_right_broadcast
,sum(case when analysis_right ='Sky 1 and Sky 2 Motor Sport' then days_right_broadcast else 0 end) 
as MSPS12_days_right_broadcast
,sum(case when analysis_right ='Sky 1 and Sky 2 Unknown' then days_right_broadcast else 0 end) 
as UNKS12_days_right_broadcast
,sum(case when analysis_right ='Sky 1 and Sky 2 Wrestling' then days_right_broadcast else 0 end) 
as WRES12_days_right_broadcast
,sum(case when analysis_right ='Sky Sports American Football' then days_right_broadcast else 0 end) 
as AMSS_days_right_broadcast
,sum(case when analysis_right ='Sky Sports Athletics' then days_right_broadcast else 0 end) 
as ATHSS_days_right_broadcast
,sum(case when analysis_right ='Sky Sports Baseball' then days_right_broadcast else 0 end) 
as BASESS_days_right_broadcast
,sum(case when analysis_right ='Sky Sports Basketball' then days_right_broadcast else 0 end) 
as BASKSS_days_right_broadcast
,sum(case when analysis_right ='Sky Sports Boxing' then days_right_broadcast else 0 end) 
as BOXSS_days_right_broadcast
,sum(case when analysis_right ='Sky Sports Cricket' then days_right_broadcast else 0 end) 
as CRISS_days_right_broadcast
,sum(case when analysis_right ='Sky Sports Darts' then days_right_broadcast else 0 end) 
as DARTSS_days_right_broadcast
,sum(case when analysis_right ='Sky Sports Equestrian' then days_right_broadcast else 0 end) 
as EQUSS_days_right_broadcast
,sum(case when analysis_right ='Sky Sports Extreme' then days_right_broadcast else 0 end) 
as EXTSS_days_right_broadcast
,sum(case when analysis_right ='Sky Sports Fishing' then days_right_broadcast else 0 end) 
as FISHSS_days_right_broadcast
,sum(case when analysis_right ='Sky Sports Football' then days_right_broadcast else 0 end) 
as FOOTSS_days_right_broadcast
,sum(case when analysis_right ='Sky Sports Golf' then days_right_broadcast else 0 end) 
as GOLFSS_days_right_broadcast
,sum(case when analysis_right ='Sky Sports Ice Hockey' then days_right_broadcast else 0 end) 
as IHSS_days_right_broadcast
,sum(case when analysis_right ='Sky Sports Motor Sport' then days_right_broadcast else 0 end) 
as MSPSS_days_right_broadcast
,sum(case when analysis_right ='Sky Sports Racing' then days_right_broadcast else 0 end) 
as RACSS_days_right_broadcast
,sum(case when analysis_right ='Sky Sports Rugby' then days_right_broadcast else 0 end) 
as RUGSS_days_right_broadcast
,sum(case when analysis_right ='Sky Sports Snooker/Pool' then days_right_broadcast else 0 end) 
as SNPSS_days_right_broadcast
,sum(case when analysis_right ='Sky Sports Tennis' then days_right_broadcast else 0 end) 
as TENSS_days_right_broadcast
,sum(case when analysis_right ='Sky Sports Unknown' then days_right_broadcast else 0 end) 
as UNKSS_days_right_broadcast
,sum(case when analysis_right ='Sky Sports Watersports' then days_right_broadcast else 0 end) 
as WATSS_days_right_broadcast
,sum(case when analysis_right ='Sky Sports Wintersports' then days_right_broadcast else 0 end) 
as WINSS_days_right_broadcast
,sum(case when analysis_right ='Sky Sports Wrestling' then days_right_broadcast else 0 end) 
as WRESS_days_right_broadcast
,sum(case when analysis_right ='Solheim Cup 2013 Ladies Golf - Sky Sports' then days_right_broadcast else 0 end) 
as SOLSS_days_right_broadcast
,sum(case when analysis_right ='South African Home Cricket  2012-2020 Sky Sports' then days_right_broadcast else 0 end) 
as SACSS_days_right_broadcast
,sum(case when analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then days_right_broadcast else 0 end) 
as SPFSS_days_right_broadcast
,sum(case when analysis_right ='SPFL - BT Sport' then days_right_broadcast else 0 end) 
as SPFLBTS_days_right_broadcast
,sum(case when analysis_right ='SPL - ESPN' then days_right_broadcast else 0 end) 
as SPLESPN_days_right_broadcast
,sum(case when analysis_right ='SPL - Sky Sports' then days_right_broadcast else 0 end) 
as SPLSS_days_right_broadcast
,sum(case when analysis_right ='Sportfive World Cup 2014 Qualifers - Sky Sports' then days_right_broadcast else 0 end) 
as SP5SS_days_right_broadcast
,sum(case when analysis_right ='The boat race - BBC' then days_right_broadcast else 0 end) 
as BTRBBC_days_right_broadcast
,sum(case when analysis_right ='The football league show - BBC' then days_right_broadcast else 0 end) 
as FLSBBC_days_right_broadcast
,sum(case when analysis_right ='The Masters Golf - BBC' then days_right_broadcast else 0 end) 
as MGBBC_days_right_broadcast
,sum(case when analysis_right ='TNA Wrestling Challenge' then days_right_broadcast else 0 end) 
as TNACHA_days_right_broadcast
,sum(case when analysis_right ='Tour de France - Eurosport' then days_right_broadcast else 0 end) 
as TDFEUR_days_right_broadcast
,sum(case when analysis_right ='Tour de France - ITV' then days_right_broadcast else 0 end) 
as TDFITV_days_right_broadcast
,sum(case when analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then days_right_broadcast else 0 end) 
as USMGSS_days_right_broadcast
,sum(case when analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then days_right_broadcast else 0 end) 
as USOTSS_days_right_broadcast
,sum(case when analysis_right ='U.S Open/Womens/Seniors/Amateur Golf Sky Sports' then days_right_broadcast else 0 end) 
as USOGSS_days_right_broadcast
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports' then days_right_broadcast else 0 end) 
as CLASS_days_right_broadcast
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (non Live)' then days_right_broadcast else 0 end) 
as CLNSS_days_right_broadcast
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (other Live)' then days_right_broadcast else 0 end) 
as CLOSS_days_right_broadcast
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (Tue)' then days_right_broadcast else 0 end) 
as CLTSS_days_right_broadcast
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (Wed)' then days_right_broadcast else 0 end) 
as CLWSS_days_right_broadcast
,sum(case when analysis_right ='US Open Tennis - Eurosport' then days_right_broadcast else 0 end) 
as USOTEUR_days_right_broadcast
,sum(case when analysis_right ='USA Football - BT Sport' then days_right_broadcast else 0 end) 
as USFBTS_days_right_broadcast
,sum(case when analysis_right ='USPGA Championship (2007-2016) Sky Sports' then days_right_broadcast else 0 end) 
as USPGASS_days_right_broadcast
,sum(case when analysis_right ='WCQ - ESPN' then days_right_broadcast else 0 end) 
as WCQESPN_days_right_broadcast
,sum(case when analysis_right ='Welsh Home International Football 2012-2014 Sky Sports' then days_right_broadcast else 0 end) 
as WIFSS_days_right_broadcast
,sum(case when analysis_right ='West Indies Home Cricket 2013-2020 Sky Sports' then days_right_broadcast else 0 end) 
as WICSS_days_right_broadcast
,sum(case when analysis_right ='Wimbledon - BBC' then days_right_broadcast else 0 end) 
as WIMBBC_days_right_broadcast
,sum(case when analysis_right ='Womens Cricket - I C C Cricket Deal  2012-2015 Sky Sports' then days_right_broadcast else 0 end) 
as WICCSS_days_right_broadcast
,sum(case when analysis_right ='World Athletics Championship - More 4' then days_right_broadcast else 0 end) 
as WACMR4_days_right_broadcast
,sum(case when analysis_right ='World Club Championship - BBC' then days_right_broadcast else 0 end) 
as WCLBBBC_days_right_broadcast
,sum(case when analysis_right ='World Cup Qualifiers - BT Sport' then days_right_broadcast else 0 end) 
as WCQBTS_days_right_broadcast
,sum(case when analysis_right ='World Darts Championship 2009-2012 Sky Sports' then days_right_broadcast else 0 end) 
as WDCSS_days_right_broadcast
,sum(case when analysis_right ='World snooker championship - BBC' then days_right_broadcast else 0 end) 
as WSCBBC_days_right_broadcast
,sum(case when analysis_right ='WWE Sky 1 and 2' then days_right_broadcast else 0 end) 
as WWES12_days_right_broadcast
,sum(case when analysis_right ='WWE Sky Sports' then days_right_broadcast else 0 end) 
as WWESS_days_right_broadcast


,sum(case when analysis_right ='Africa Cup of Nations - Eurosport' then right_broadcast_duration else 0 end) 
as AFCEUR_right_broadcast_duration
,sum(case when analysis_right ='Africa Cup of Nations - ITV' then right_broadcast_duration else 0 end) 
as AFCITV_right_broadcast_duration
,sum(case when analysis_right ='Americas Cup - BBC' then right_broadcast_duration else 0 end) 
as AMCBBC_right_broadcast_duration
,sum(case when analysis_right ='Asian Tour Golf 2012-2013 Sky Sports' then right_broadcast_duration else 0 end) 
as ATGSS_right_broadcast_duration
,sum(case when analysis_right ='ATP Tennis Masters Series  2011-2013 Sky Sports' then right_broadcast_duration else 0 end) 
as ATPSS_right_broadcast_duration
,sum(case when analysis_right ='Australia Home Cricket  2012-2016 Sky Sports' then right_broadcast_duration else 0 end) 
as AHCSS_right_broadcast_duration
,sum(case when analysis_right ='Australian Football - BT Sport' then right_broadcast_duration else 0 end) 
as AUFBTS_right_broadcast_duration
,sum(case when analysis_right ='Australian Open Tennis - BBC' then right_broadcast_duration else 0 end) 
as AOTBBC_right_broadcast_duration
,sum(case when analysis_right ='Australian Open Tennis - Eurosport' then right_broadcast_duration else 0 end) 
as AOTEUR_right_broadcast_duration
,sum(case when analysis_right ='Aviva Premiership - ESPN' then right_broadcast_duration else 0 end) 
as AVPSS_right_broadcast_duration
,sum(case when analysis_right ='BBC American Football' then right_broadcast_duration else 0 end) 
as AFBBC_right_broadcast_duration
,sum(case when analysis_right ='BBC Athletics' then right_broadcast_duration else 0 end) 
as ATHBBC_right_broadcast_duration
,sum(case when analysis_right ='BBC Boxing' then right_broadcast_duration else 0 end) 
as BOXBBC_right_broadcast_duration
,sum(case when analysis_right ='BBC Darts' then right_broadcast_duration else 0 end) 
as DRTBBC_right_broadcast_duration
,sum(case when analysis_right ='BBC Equestrian' then right_broadcast_duration else 0 end) 
as EQUBBC_right_broadcast_duration
,sum(case when analysis_right ='BBC Football' then right_broadcast_duration else 0 end) 
as FOOTBBC_right_broadcast_duration
,sum(case when analysis_right ='BBC Golf' then right_broadcast_duration else 0 end) 
as GOLFBBC_right_broadcast_duration
,sum(case when analysis_right ='BBC Motor Sport' then right_broadcast_duration else 0 end) 
as MSPBBC_right_broadcast_duration
,sum(case when analysis_right ='BBC Rugby' then right_broadcast_duration else 0 end) 
as RUGBBC_right_broadcast_duration
,sum(case when analysis_right ='BBC Snooker/Pool' then right_broadcast_duration else 0 end) 
as SNPBBC_right_broadcast_duration
,sum(case when analysis_right ='BBC Tennis' then right_broadcast_duration else 0 end) 
as TENBBC_right_broadcast_duration
,sum(case when analysis_right ='BBC Unknown' then right_broadcast_duration else 0 end) 
as UNKBBC_right_broadcast_duration
,sum(case when analysis_right ='BBC Watersports' then right_broadcast_duration else 0 end) 
as WATBBC_right_broadcast_duration
,sum(case when analysis_right ='BBC Wintersports' then right_broadcast_duration else 0 end) 
as WINBBC_right_broadcast_duration
,sum(case when analysis_right ='Boxing  - Channel 5' then right_broadcast_duration else 0 end) 
as BOXCH5_right_broadcast_duration
,sum(case when analysis_right ='Boxing - Matchroom 2012-2014 Sky Sports' then right_broadcast_duration else 0 end) 
as BOXMSS_right_broadcast_duration
,sum(case when analysis_right ='Brazil Football - BT Sport' then right_broadcast_duration else 0 end) 
as BFTBTS_right_broadcast_duration
,sum(case when analysis_right ='British & Irish Lions 2013 - Sky Sports' then right_broadcast_duration else 0 end) 
as BILSS_right_broadcast_duration
,sum(case when analysis_right ='British Open Golf - BBC' then right_broadcast_duration else 0 end) 
as BOGSS_right_broadcast_duration
,sum(case when analysis_right ='BT Sport American Football' then right_broadcast_duration else 0 end) 
as AFBTS_right_broadcast_duration
,sum(case when analysis_right ='BT Sport Athletics' then right_broadcast_duration else 0 end) 
as ATHBTS_right_broadcast_duration
,sum(case when analysis_right ='BT Sport Baseball' then right_broadcast_duration else 0 end) 
as BASEBTS_right_broadcast_duration
,sum(case when analysis_right ='BT Sport Basketball' then right_broadcast_duration else 0 end) 
as BASKBTS_right_broadcast_duration
,sum(case when analysis_right ='BT Sport Boxing' then right_broadcast_duration else 0 end) 
as BOXBTS_right_broadcast_duration
,sum(case when analysis_right ='BT Sport Cricket' then right_broadcast_duration else 0 end) 
as CRIBTS_right_broadcast_duration
,sum(case when analysis_right ='BT Sport Equestrian' then right_broadcast_duration else 0 end) 
as EQUBTS_right_broadcast_duration
,sum(case when analysis_right ='BT Sport Extreme' then right_broadcast_duration else 0 end) 
as EXTBTS_right_broadcast_duration
,sum(case when analysis_right ='BT Sport Football' then right_broadcast_duration else 0 end) 
as FOOTBTS_right_broadcast_duration
,sum(case when analysis_right ='BT Sport Motor Sport' then right_broadcast_duration else 0 end) 
as MSPBTS_right_broadcast_duration
,sum(case when analysis_right ='BT Sport Rugby' then right_broadcast_duration else 0 end) 
as RUGBTS_right_broadcast_duration
,sum(case when analysis_right ='BT Sport Tennis' then right_broadcast_duration else 0 end) 
as TENBTS_right_broadcast_duration
,sum(case when analysis_right ='BT Sport Unknown' then right_broadcast_duration else 0 end) 
as UNKBTS_right_broadcast_duration
,sum(case when analysis_right ='BT Sport Wintersports' then right_broadcast_duration else 0 end) 
as WINBTS_right_broadcast_duration
,sum(case when analysis_right ='Bundesliga - BT Sport' then right_broadcast_duration else 0 end) 
as BUNBTS_right_broadcast_duration
,sum(case when analysis_right ='Bundesliga- ESPN' then right_broadcast_duration else 0 end) 
as BUNESPN_right_broadcast_duration
,sum(case when analysis_right ='Challenge Darts' then right_broadcast_duration else 0 end) 
as DRTCHA_right_broadcast_duration
,sum(case when analysis_right ='Challenge Extreme' then right_broadcast_duration else 0 end) 
as EXTCHA_right_broadcast_duration
,sum(case when analysis_right ='Challenge Unknown' then right_broadcast_duration else 0 end) 
as UNKCHA_right_broadcast_duration
,sum(case when analysis_right ='Challenge Wrestling' then right_broadcast_duration else 0 end) 
as WRECHA_right_broadcast_duration
,sum(case when analysis_right ='Champions League - ITV' then right_broadcast_duration else 0 end) 
as CHLITV_right_broadcast_duration
,sum(case when analysis_right ='Champions Trophy - I C C Cricket Deal  2012-2015 Sky Sports' then right_broadcast_duration else 0 end) 
as ICCSS_right_broadcast_duration
,sum(case when analysis_right ='Channel 4 American Football' then right_broadcast_duration else 0 end) 
as AMCH4_right_broadcast_duration
,sum(case when analysis_right ='Channel 4 Athletics' then right_broadcast_duration else 0 end) 
as ATHCH4_right_broadcast_duration
,sum(case when analysis_right ='Channel 4 Boxing' then right_broadcast_duration else 0 end) 
as BOXCH4_right_broadcast_duration
,sum(case when analysis_right ='Channel 4 Equestrian' then right_broadcast_duration else 0 end) 
as EQUCH4_right_broadcast_duration
,sum(case when analysis_right ='Channel 4 Extreme' then right_broadcast_duration else 0 end) 
as EXTCH4_right_broadcast_duration
,sum(case when analysis_right ='Channel 4 Football' then right_broadcast_duration else 0 end) 
as FOOTCH4_right_broadcast_duration
,sum(case when analysis_right ='Channel 4 Motor Sport' then right_broadcast_duration else 0 end) 
as MSPCH4_right_broadcast_duration
,sum(case when analysis_right ='Channel 4 Racing' then right_broadcast_duration else 0 end) 
as RACCH4_right_broadcast_duration
,sum(case when analysis_right ='Channel 4 Unknown' then right_broadcast_duration else 0 end) 
as UNKCH4_right_broadcast_duration
,sum(case when analysis_right ='Channel 4 Watersports' then right_broadcast_duration else 0 end) 
as WATCH4_right_broadcast_duration
,sum(case when analysis_right ='Channel 4 Wintersports' then right_broadcast_duration else 0 end) 
as WINCH4_right_broadcast_duration
,sum(case when analysis_right ='Channel 5 Athletics' then right_broadcast_duration else 0 end) 
as ATHCH5_right_broadcast_duration
,sum(case when analysis_right ='Channel 5 Boxing' then right_broadcast_duration else 0 end) 
as BOXOCH5_right_broadcast_duration
,sum(case when analysis_right ='Channel 5 Cricket' then right_broadcast_duration else 0 end) 
as CRICH5_right_broadcast_duration
,sum(case when analysis_right ='Channel 5 Motor Sport' then right_broadcast_duration else 0 end) 
as MSPCH5_right_broadcast_duration
,sum(case when analysis_right ='Channel 5 Unknown' then right_broadcast_duration else 0 end) 
as UNKCH5_right_broadcast_duration
,sum(case when analysis_right ='Channel 5 Wrestling' then right_broadcast_duration else 0 end) 
as WRECH5_right_broadcast_duration
,sum(case when analysis_right ='Cheltenham Festival - Channel 4' then right_broadcast_duration else 0 end) 
as CHELCH4_right_broadcast_duration
,sum(case when analysis_right ='Community Shield - ITV' then right_broadcast_duration else 0 end) 
as CMSITV_right_broadcast_duration
,sum(case when analysis_right ='Confederations Cup - BBC' then right_broadcast_duration else 0 end) 
as CONCBBC_right_broadcast_duration
,sum(case when analysis_right ='Conference - BT Sport' then right_broadcast_duration else 0 end) 
as CONFBTS_right_broadcast_duration
,sum(case when analysis_right ='Cycling - La Vuelta ITV' then right_broadcast_duration else 0 end) 
as CLVITV_right_broadcast_duration
,sum(case when analysis_right ='Cycling - U C I World Tour Sky Sports' then right_broadcast_duration else 0 end) 
as CUCISS_right_broadcast_duration
,sum(case when analysis_right ='Cycling Tour of Britain - Eurosport' then right_broadcast_duration else 0 end) 
as CTBEUR_right_broadcast_duration
,sum(case when analysis_right ='Cycling: tour of britain ITV4' then right_broadcast_duration else 0 end) 
as CTCITV_right_broadcast_duration
,sum(case when analysis_right ='Derby - Channel 4' then right_broadcast_duration else 0 end) 
as DERCH4_right_broadcast_duration
,sum(case when analysis_right ='ECB (highlights) - Channel 5' then right_broadcast_duration else 0 end) 
as ECBHCH5_right_broadcast_duration
,sum(case when analysis_right ='ECB Cricket Sky Sports' then right_broadcast_duration else 0 end) 
as GECRSS_right_broadcast_duration
,sum(case when analysis_right ='ECB non-Test Cricket Sky Sports' then right_broadcast_duration else 0 end) 
as ECBNSS_right_broadcast_duration
,sum(case when analysis_right ='ECB Test Cricket Sky Sports' then right_broadcast_duration else 0 end) 
as ECBTSS_right_broadcast_duration
,sum(case when analysis_right ='England Football Internationals - ITV' then right_broadcast_duration else 0 end) 
as GENGITV_right_broadcast_duration
,sum(case when analysis_right ='England Friendlies (Football) - ITV' then right_broadcast_duration else 0 end) 
as EFRITV_right_broadcast_duration
,sum(case when analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then right_broadcast_duration else 0 end) 
as ENRSS_right_broadcast_duration
,sum(case when analysis_right ='England World Cup Qualifying (Away) - ITV' then right_broadcast_duration else 0 end) 
as EWQAITV_right_broadcast_duration
,sum(case when analysis_right ='England World Cup Qualifying (Home) - ITV' then right_broadcast_duration else 0 end) 
as EWQHITV_right_broadcast_duration
,sum(case when analysis_right ='ESPN American Football' then right_broadcast_duration else 0 end) 
as AMESPN_right_broadcast_duration
,sum(case when analysis_right ='ESPN Athletics' then right_broadcast_duration else 0 end) 
as ATHESPN_right_broadcast_duration
,sum(case when analysis_right ='ESPN Baseball' then right_broadcast_duration else 0 end) 
as BASEESPN_right_broadcast_duration
,sum(case when analysis_right ='ESPN Basketball' then right_broadcast_duration else 0 end) 
as BASKESPN_right_broadcast_duration
,sum(case when analysis_right ='ESPN Boxing' then right_broadcast_duration else 0 end) 
as BOXESPN_right_broadcast_duration
,sum(case when analysis_right ='ESPN Cricket' then right_broadcast_duration else 0 end) 
as CRIESPN_right_broadcast_duration
,sum(case when analysis_right ='ESPN Darts' then right_broadcast_duration else 0 end) 
as DARTESPN_right_broadcast_duration
,sum(case when analysis_right ='ESPN Equestrian' then right_broadcast_duration else 0 end) 
as EQUESPN_right_broadcast_duration
,sum(case when analysis_right ='ESPN Extreme' then right_broadcast_duration else 0 end) 
as EXTESPN_right_broadcast_duration
,sum(case when analysis_right ='ESPN Football' then right_broadcast_duration else 0 end) 
as FOOTESPN_right_broadcast_duration
,sum(case when analysis_right ='ESPN Golf' then right_broadcast_duration else 0 end) 
as GOLFESPN_right_broadcast_duration
,sum(case when analysis_right ='ESPN Ice Hockey' then right_broadcast_duration else 0 end) 
as IHESPN_right_broadcast_duration
,sum(case when analysis_right ='ESPN Motor Sport' then right_broadcast_duration else 0 end) 
as MSPESPN_right_broadcast_duration
,sum(case when analysis_right ='ESPN Racing' then right_broadcast_duration else 0 end) 
as RACESPN_right_broadcast_duration
,sum(case when analysis_right ='ESPN Rugby' then right_broadcast_duration else 0 end) 
as RUGESPN_right_broadcast_duration
,sum(case when analysis_right ='ESPN Snooker/Pool' then right_broadcast_duration else 0 end) 
as SNPESPN_right_broadcast_duration
,sum(case when analysis_right ='ESPN Tennis' then right_broadcast_duration else 0 end) 
as TENESPN_right_broadcast_duration
,sum(case when analysis_right ='ESPN Unknown' then right_broadcast_duration else 0 end) 
as UNKESPN_right_broadcast_duration
,sum(case when analysis_right ='ESPN Watersports' then right_broadcast_duration else 0 end) 
as WATESPN_right_broadcast_duration
,sum(case when analysis_right ='ESPN Wintersports' then right_broadcast_duration else 0 end) 
as WINESPN_right_broadcast_duration
,sum(case when analysis_right ='ESPN Wrestling' then right_broadcast_duration else 0 end) 
as WREESPN_right_broadcast_duration
,sum(case when analysis_right ='Europa League - BT Sport' then right_broadcast_duration else 0 end) 
as ELBTSP_right_broadcast_duration
,sum(case when analysis_right ='Europa League - ESPN' then right_broadcast_duration else 0 end) 
as ELESPN_right_broadcast_duration
,sum(case when analysis_right ='Europa League - ITV' then right_broadcast_duration else 0 end) 
as ELITV_right_broadcast_duration
,sum(case when analysis_right ='European Tour Golf - Sky Sports' then right_broadcast_duration else 0 end) 
as ETGSS_right_broadcast_duration
,sum(case when analysis_right ='Eurosport American Football' then right_broadcast_duration else 0 end) 
as AMEUR_right_broadcast_duration
,sum(case when analysis_right ='Eurosport Athletics' then right_broadcast_duration else 0 end) 
as ATHEUR_right_broadcast_duration
,sum(case when analysis_right ='Eurosport Baseball' then right_broadcast_duration else 0 end) 
as BASEEUR_right_broadcast_duration
,sum(case when analysis_right ='Eurosport Basketball' then right_broadcast_duration else 0 end) 
as BASKEUR_right_broadcast_duration
,sum(case when analysis_right ='Eurosport Boxing' then right_broadcast_duration else 0 end) 
as BOXEUR_right_broadcast_duration
,sum(case when analysis_right ='Eurosport Cricket' then right_broadcast_duration else 0 end) 
as CRIEUR_right_broadcast_duration
,sum(case when analysis_right ='Eurosport Darts' then right_broadcast_duration else 0 end) 
as DARTEUR_right_broadcast_duration
,sum(case when analysis_right ='Eurosport Equestrian' then right_broadcast_duration else 0 end) 
as EQUEUR_right_broadcast_duration
,sum(case when analysis_right ='Eurosport Extreme' then right_broadcast_duration else 0 end) 
as EXTEUR_right_broadcast_duration
,sum(case when analysis_right ='Eurosport Football' then right_broadcast_duration else 0 end) 
as FOOTEUR_right_broadcast_duration
,sum(case when analysis_right ='Eurosport Golf' then right_broadcast_duration else 0 end) 
as GOLFEUR_right_broadcast_duration
,sum(case when analysis_right ='Eurosport Ice Hockey' then right_broadcast_duration else 0 end) 
as IHEUR_right_broadcast_duration
,sum(case when analysis_right ='Eurosport Motor Sport' then right_broadcast_duration else 0 end) 
as MSPEUR_right_broadcast_duration
,sum(case when analysis_right ='Eurosport Racing' then right_broadcast_duration else 0 end) 
as RACEUR_right_broadcast_duration
,sum(case when analysis_right ='Eurosport Snooker/Pool' then right_broadcast_duration else 0 end) 
as SNPEUR_right_broadcast_duration
,sum(case when analysis_right ='Eurosport Tennis' then right_broadcast_duration else 0 end) 
as TENEUR_right_broadcast_duration
,sum(case when analysis_right ='Eurosport Unknown' then right_broadcast_duration else 0 end) 
as UNKEUR_right_broadcast_duration
,sum(case when analysis_right ='Eurosport Watersports' then right_broadcast_duration else 0 end) 
as WATEUR_right_broadcast_duration
,sum(case when analysis_right ='Eurosport Wintersports' then right_broadcast_duration else 0 end) 
as WINEUR_right_broadcast_duration
,sum(case when analysis_right ='F1 - BBC' then right_broadcast_duration else 0 end) 
as GF1BBC_right_broadcast_duration
,sum(case when analysis_right ='F1 - Sky Sports' then right_broadcast_duration else 0 end) 
as GF1SS_right_broadcast_duration
,sum(case when analysis_right ='F1 (non-Live)- BBC' then right_broadcast_duration else 0 end) 
as F1NBBC_right_broadcast_duration
,sum(case when analysis_right ='F1 (Practice Live)- BBC' then right_broadcast_duration else 0 end) 
as F1PBBC_right_broadcast_duration
,sum(case when analysis_right ='F1 (Qualifying Live)- BBC' then right_broadcast_duration else 0 end) 
as F1QBBC_right_broadcast_duration
,sum(case when analysis_right ='F1 (Race Live)- BBC' then right_broadcast_duration else 0 end) 
as F1RBBC_right_broadcast_duration
,sum(case when analysis_right ='FA Cup - ESPN' then right_broadcast_duration else 0 end) 
as FACESPN_right_broadcast_duration
,sum(case when analysis_right ='FA Cup - ITV' then right_broadcast_duration else 0 end) 
as FACITV_right_broadcast_duration
,sum(case when analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then right_broadcast_duration else 0 end) 
as FLCCSS_right_broadcast_duration
,sum(case when analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - others' then right_broadcast_duration else 0 end) 
as FLOTSS_right_broadcast_duration
,sum(case when analysis_right ='Formula One 2012-2018 - (non-Live) Sky Sports' then right_broadcast_duration else 0 end) 
as F1NSS_right_broadcast_duration
,sum(case when analysis_right ='Formula One 2012-2018 - (Practice Live) Sky Sports' then right_broadcast_duration else 0 end) 
as F1PSS_right_broadcast_duration
,sum(case when analysis_right ='Formula One 2012-2018 - (Qualifying Live) Sky Sports' then right_broadcast_duration else 0 end) 
as F1QSS_right_broadcast_duration
,sum(case when analysis_right ='Formula One 2012-2018 - (Race Live) Sky Sports' then right_broadcast_duration else 0 end) 
as F1RSS_right_broadcast_duration
,sum(case when analysis_right ='French Open Tennis - Eurosport' then right_broadcast_duration else 0 end) 
as FOTEUR_right_broadcast_duration
,sum(case when analysis_right ='French Open Tennis - ITV' then right_broadcast_duration else 0 end) 
as FOTITV_right_broadcast_duration
,sum(case when analysis_right ='Grand National - Channel 4' then right_broadcast_duration else 0 end) 
as GDNCH4_right_broadcast_duration
,sum(case when analysis_right ='Heineken Cup 2010/11 To 2013/14- Sky Sports' then right_broadcast_duration else 0 end) 
as HECSS_right_broadcast_duration
,sum(case when analysis_right ='I R B Sevens 2008/09 -2014/15 - Sky Sports' then right_broadcast_duration else 0 end) 
as IRBSS_right_broadcast_duration
,sum(case when analysis_right ='IAAF World Athletics Championship - Eurosport' then right_broadcast_duration else 0 end) 
as WACEUR_right_broadcast_duration
,sum(case when analysis_right ='India Home Cricket 2012-2018 Sky Sports' then right_broadcast_duration else 0 end) 
as IHCSS_right_broadcast_duration
,sum(case when analysis_right ='India Premier League - ITV' then right_broadcast_duration else 0 end) 
as IPLITV_right_broadcast_duration
,sum(case when analysis_right ='International Freindlies - ESPN' then right_broadcast_duration else 0 end) 
as IFESPN_right_broadcast_duration
,sum(case when analysis_right ='International Friendlies - BT Sport' then right_broadcast_duration else 0 end) 
as IFBTS_right_broadcast_duration
,sum(case when analysis_right ='ITV1 Boxing' then right_broadcast_duration else 0 end) 
as BOXITV1_right_broadcast_duration
,sum(case when analysis_right ='ITV1 Football' then right_broadcast_duration else 0 end) 
as FOOTITV1_right_broadcast_duration
,sum(case when analysis_right ='ITV1 Motor Sport' then right_broadcast_duration else 0 end) 
as MOTSITV1_right_broadcast_duration
,sum(case when analysis_right ='ITV1 Rugby' then right_broadcast_duration else 0 end) 
as RUGITV1_right_broadcast_duration
,sum(case when analysis_right ='ITV1 Snooker/Pool' then right_broadcast_duration else 0 end) 
as SNPITV1_right_broadcast_duration
,sum(case when analysis_right ='ITV1 Unknown' then right_broadcast_duration else 0 end) 
as UNKITV1_right_broadcast_duration
,sum(case when analysis_right ='ITV4 Boxing' then right_broadcast_duration else 0 end) 
as BOXITV4_right_broadcast_duration
,sum(case when analysis_right ='ITV4 Cricket' then right_broadcast_duration else 0 end) 
as CRIITV4_right_broadcast_duration
,sum(case when analysis_right ='ITV4 Darts' then right_broadcast_duration else 0 end) 
as DARTITV4_right_broadcast_duration
,sum(case when analysis_right ='ITV4 Extreme' then right_broadcast_duration else 0 end) 
as EXTITV4_right_broadcast_duration
,sum(case when analysis_right ='ITV4 Football' then right_broadcast_duration else 0 end) 
as FOOTITV4_right_broadcast_duration
,sum(case when analysis_right ='ITV4 Motor Sport' then right_broadcast_duration else 0 end) 
as MSPITV4_right_broadcast_duration
,sum(case when analysis_right ='ITV4 Rugby' then right_broadcast_duration else 0 end) 
as RUGITV4_right_broadcast_duration
,sum(case when analysis_right ='ITV4 Snooker/Pool' then right_broadcast_duration else 0 end) 
as SNPITV4_right_broadcast_duration
,sum(case when analysis_right ='ITV4 Tennis' then right_broadcast_duration else 0 end) 
as TENITV4_right_broadcast_duration
,sum(case when analysis_right ='ITV4 Unknown' then right_broadcast_duration else 0 end) 
as UNKITV4_right_broadcast_duration
,sum(case when analysis_right ='Ligue 1 - BT Sport' then right_broadcast_duration else 0 end) 
as L1BTS_right_broadcast_duration
,sum(case when analysis_right ='Ligue 1 - ESPN' then right_broadcast_duration else 0 end) 
as L1ESPN_right_broadcast_duration
,sum(case when analysis_right ='Match of the day - BBC' then right_broadcast_duration else 0 end) 
as MOTDBBC_right_broadcast_duration
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Other)- Sky Sports' then right_broadcast_duration else 0 end) 
as MROSS_right_broadcast_duration
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Pool)- Sky Sports' then right_broadcast_duration else 0 end) 
as MRPSS_right_broadcast_duration
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Snooker)- Sky Sports' then right_broadcast_duration else 0 end) 
as MRSSS_right_broadcast_duration
,sum(case when analysis_right ='Moto GP BBC' then right_broadcast_duration else 0 end) 
as MGPBBC_right_broadcast_duration
,sum(case when analysis_right ='NBA - Sky Sports' then right_broadcast_duration else 0 end) 
as NBASS_right_broadcast_duration
,sum(case when analysis_right ='NFL - BBC' then right_broadcast_duration else 0 end) 
as NFLBBC_right_broadcast_duration
,sum(case when analysis_right ='NFL - Channel 4' then right_broadcast_duration else 0 end) 
as NFLCH4_right_broadcast_duration
,sum(case when analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then right_broadcast_duration else 0 end) 
as NFLSS_right_broadcast_duration
,sum(case when analysis_right ='Northern Ireland Football Association 2012-2014 - Sky Sports' then right_broadcast_duration else 0 end) 
as NIFSS_right_broadcast_duration
,sum(case when analysis_right ='Oaks - Channel 4' then right_broadcast_duration else 0 end) 
as OAKCH4_right_broadcast_duration
,sum(case when analysis_right ='Other American Football' then right_broadcast_duration else 0 end) 
as AMOTH_right_broadcast_duration
,sum(case when analysis_right ='Other Athletics' then right_broadcast_duration else 0 end) 
as ATHOTH_right_broadcast_duration
,sum(case when analysis_right ='Other Baseball' then right_broadcast_duration else 0 end) 
as BASEOTH_right_broadcast_duration
,sum(case when analysis_right ='Other Basketball' then right_broadcast_duration else 0 end) 
as BASKOTH_right_broadcast_duration
,sum(case when analysis_right ='Other Boxing' then right_broadcast_duration else 0 end) 
as BOXOTH_right_broadcast_duration
,sum(case when analysis_right ='Other Cricket' then right_broadcast_duration else 0 end) 
as CRIOTH_right_broadcast_duration
,sum(case when analysis_right ='Other Darts' then right_broadcast_duration else 0 end) 
as DARTOTH_right_broadcast_duration
,sum(case when analysis_right ='Other Equestrian' then right_broadcast_duration else 0 end) 
as EQUOTH_right_broadcast_duration
,sum(case when analysis_right ='Other Extreme' then right_broadcast_duration else 0 end) 
as EXTOTH_right_broadcast_duration
,sum(case when analysis_right ='Other Fishing' then right_broadcast_duration else 0 end) 
as FSHOTH_right_broadcast_duration
,sum(case when analysis_right ='Other Football' then right_broadcast_duration else 0 end) 
as FOOTOTH_right_broadcast_duration
,sum(case when analysis_right ='Other Golf' then right_broadcast_duration else 0 end) 
as GOLFOTH_right_broadcast_duration
,sum(case when analysis_right ='Other Ice Hockey' then right_broadcast_duration else 0 end) 
as IHOTH_right_broadcast_duration
,sum(case when analysis_right ='Other Motor Sport' then right_broadcast_duration else 0 end) 
as MSPOTH_right_broadcast_duration
,sum(case when analysis_right ='Other Racing' then right_broadcast_duration else 0 end) 
as RACOTH_right_broadcast_duration
,sum(case when analysis_right ='Other Rugby' then right_broadcast_duration else 0 end) 
as RUGOTH_right_broadcast_duration
,sum(case when analysis_right ='Other Rugby Internationals - ESPN' then right_broadcast_duration else 0 end) 
as ORUGESPN_right_broadcast_duration
,sum(case when analysis_right ='Other Snooker/Pool' then right_broadcast_duration else 0 end) 
as OTHSNP_right_broadcast_duration
,sum(case when analysis_right ='Other Tennis' then right_broadcast_duration else 0 end) 
as OTHTEN_right_broadcast_duration
,sum(case when analysis_right ='Other Unknown' then right_broadcast_duration else 0 end) 
as OTHUNK_right_broadcast_duration
,sum(case when analysis_right ='Other Watersports' then right_broadcast_duration else 0 end) 
as OTHWAT_right_broadcast_duration
,sum(case when analysis_right ='Other Wintersports' then right_broadcast_duration else 0 end) 
as OTHWIN_right_broadcast_duration
,sum(case when analysis_right ='Other Wrestling' then right_broadcast_duration else 0 end) 
as OTHWRE_right_broadcast_duration
,sum(case when analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then right_broadcast_duration else 0 end) 
as PGASS_right_broadcast_duration
,sum(case when analysis_right ='Premier League - BT Sport' then right_broadcast_duration else 0 end) 
as PLBTS_right_broadcast_duration
,sum(case when analysis_right ='Premier League - ESPN' then right_broadcast_duration else 0 end) 
as PLESPN_right_broadcast_duration
,sum(case when analysis_right ='Premier League Darts 2010-2013 - Sky Sports' then right_broadcast_duration else 0 end) 
as PLDSS_right_broadcast_duration
,sum(case when analysis_right ='Premier League Football - Sky Sports' then right_broadcast_duration else 0 end) 
as GPLSS_right_broadcast_duration
,sum(case when analysis_right ='Premier League Football - Sky Sports (Match Choice)' then right_broadcast_duration else 0 end) 
as PLMCSS_right_broadcast_duration
,sum(case when analysis_right ='Premier League Football - Sky Sports (MNF)' then right_broadcast_duration else 0 end) 
as PLMNFSS_right_broadcast_duration
,sum(case when analysis_right ='Premier League Football - Sky Sports (non Live)' then right_broadcast_duration else 0 end) 
as PLNLSS_right_broadcast_duration
,sum(case when analysis_right ='Premier League Football - Sky Sports (other Live)' then right_broadcast_duration else 0 end) 
as PLOLSS_right_broadcast_duration
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sat Lunchtime)' then right_broadcast_duration else 0 end) 
as PLSLSS_right_broadcast_duration
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sat Night Live)' then right_broadcast_duration else 0 end) 
as PLSNSS_right_broadcast_duration
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sun 4pm)' then right_broadcast_duration else 0 end) 
as PLS4SS_right_broadcast_duration
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sun Lunchtime)' then right_broadcast_duration else 0 end) 
as PLSULSS_right_broadcast_duration
,sum(case when analysis_right ='Premiership Rugby - Sky Sports' then right_broadcast_duration else 0 end) 
as PRUSS_right_broadcast_duration
,sum(case when analysis_right ='Republic Of Ireland FA - 2010/11 To 2013/14 - Sky Sports' then right_broadcast_duration else 0 end) 
as ROISS_right_broadcast_duration
,sum(case when analysis_right ='Royal Ascot - Channel 4' then right_broadcast_duration else 0 end) 
as RASCH4_right_broadcast_duration
,sum(case when analysis_right ='Rugby Internationals (England) - BBC' then right_broadcast_duration else 0 end) 
as RIEBBC_right_broadcast_duration
,sum(case when analysis_right ='Rugby Internationals (Ireland) - BBC' then right_broadcast_duration else 0 end) 
as RIIBBC_right_broadcast_duration
,sum(case when analysis_right ='Rugby Internationals (Scotland) - BBC' then right_broadcast_duration else 0 end) 
as RISBBC_right_broadcast_duration
,sum(case when analysis_right ='Rugby Internationals (Wales) - BBC' then right_broadcast_duration else 0 end) 
as RIWBBC_right_broadcast_duration
,sum(case when analysis_right ='Rugby League  Challenge Cup- BBC' then right_broadcast_duration else 0 end) 
as RLCCBBC_right_broadcast_duration
,sum(case when analysis_right ='Rugby League - Sky Sports' then right_broadcast_duration else 0 end) 
as RLGSS_right_broadcast_duration
,sum(case when analysis_right ='Rugby League  World Cup- BBC' then right_broadcast_duration else 0 end) 
as RLWCBBC_right_broadcast_duration
,sum(case when analysis_right ='Sanzar Rugby Union 2011 - 2015 - Sky Sports' then right_broadcast_duration else 0 end) 
as SARUSS_right_broadcast_duration
,sum(case when analysis_right ='Scottish FA Deal - 2010/11 To 2013/14 - Sky Sports' then right_broadcast_duration else 0 end) 
as SFASS_right_broadcast_duration
,sum(case when analysis_right ='Serie A - BT Sport' then right_broadcast_duration else 0 end) 
as SABTS_right_broadcast_duration
,sum(case when analysis_right ='Serie A - ESPN' then right_broadcast_duration else 0 end) 
as SAESPN_right_broadcast_duration
,sum(case when analysis_right ='SFL - ESPN' then right_broadcast_duration else 0 end) 
as SFLESPN_right_broadcast_duration
,sum(case when analysis_right ='Six Nations - BBC' then right_broadcast_duration else 0 end) 
as SNRBBC_right_broadcast_duration
,sum(case when analysis_right ='Sky 1 and Sky 2 Boxing' then right_broadcast_duration else 0 end) 
as BOXS12_right_broadcast_duration
,sum(case when analysis_right ='Sky 1 and Sky 2 Football' then right_broadcast_duration else 0 end) 
as FOOTS12_right_broadcast_duration
,sum(case when analysis_right ='Sky 1 and Sky 2 Motor Sport' then right_broadcast_duration else 0 end) 
as MSPS12_right_broadcast_duration
,sum(case when analysis_right ='Sky 1 and Sky 2 Unknown' then right_broadcast_duration else 0 end) 
as UNKS12_right_broadcast_duration
,sum(case when analysis_right ='Sky 1 and Sky 2 Wrestling' then right_broadcast_duration else 0 end) 
as WRES12_right_broadcast_duration
,sum(case when analysis_right ='Sky Sports American Football' then right_broadcast_duration else 0 end) 
as AMSS_right_broadcast_duration
,sum(case when analysis_right ='Sky Sports Athletics' then right_broadcast_duration else 0 end) 
as ATHSS_right_broadcast_duration
,sum(case when analysis_right ='Sky Sports Baseball' then right_broadcast_duration else 0 end) 
as BASESS_right_broadcast_duration
,sum(case when analysis_right ='Sky Sports Basketball' then right_broadcast_duration else 0 end) 
as BASKSS_right_broadcast_duration
,sum(case when analysis_right ='Sky Sports Boxing' then right_broadcast_duration else 0 end) 
as BOXSS_right_broadcast_duration
,sum(case when analysis_right ='Sky Sports Cricket' then right_broadcast_duration else 0 end) 
as CRISS_right_broadcast_duration
,sum(case when analysis_right ='Sky Sports Darts' then right_broadcast_duration else 0 end) 
as DARTSS_right_broadcast_duration
,sum(case when analysis_right ='Sky Sports Equestrian' then right_broadcast_duration else 0 end) 
as EQUSS_right_broadcast_duration
,sum(case when analysis_right ='Sky Sports Extreme' then right_broadcast_duration else 0 end) 
as EXTSS_right_broadcast_duration
,sum(case when analysis_right ='Sky Sports Fishing' then right_broadcast_duration else 0 end) 
as FISHSS_right_broadcast_duration
,sum(case when analysis_right ='Sky Sports Football' then right_broadcast_duration else 0 end) 
as FOOTSS_right_broadcast_duration
,sum(case when analysis_right ='Sky Sports Golf' then right_broadcast_duration else 0 end) 
as GOLFSS_right_broadcast_duration
,sum(case when analysis_right ='Sky Sports Ice Hockey' then right_broadcast_duration else 0 end) 
as IHSS_right_broadcast_duration
,sum(case when analysis_right ='Sky Sports Motor Sport' then right_broadcast_duration else 0 end) 
as MSPSS_right_broadcast_duration
,sum(case when analysis_right ='Sky Sports Racing' then right_broadcast_duration else 0 end) 
as RACSS_right_broadcast_duration
,sum(case when analysis_right ='Sky Sports Rugby' then right_broadcast_duration else 0 end) 
as RUGSS_right_broadcast_duration
,sum(case when analysis_right ='Sky Sports Snooker/Pool' then right_broadcast_duration else 0 end) 
as SNPSS_right_broadcast_duration
,sum(case when analysis_right ='Sky Sports Tennis' then right_broadcast_duration else 0 end) 
as TENSS_right_broadcast_duration
,sum(case when analysis_right ='Sky Sports Unknown' then right_broadcast_duration else 0 end) 
as UNKSS_right_broadcast_duration
,sum(case when analysis_right ='Sky Sports Watersports' then right_broadcast_duration else 0 end) 
as WATSS_right_broadcast_duration
,sum(case when analysis_right ='Sky Sports Wintersports' then right_broadcast_duration else 0 end) 
as WINSS_right_broadcast_duration
,sum(case when analysis_right ='Sky Sports Wrestling' then right_broadcast_duration else 0 end) 
as WRESS_right_broadcast_duration
,sum(case when analysis_right ='Solheim Cup 2013 Ladies Golf - Sky Sports' then right_broadcast_duration else 0 end) 
as SOLSS_right_broadcast_duration
,sum(case when analysis_right ='South African Home Cricket  2012-2020 Sky Sports' then right_broadcast_duration else 0 end) 
as SACSS_right_broadcast_duration
,sum(case when analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then right_broadcast_duration else 0 end) 
as SPFSS_right_broadcast_duration
,sum(case when analysis_right ='SPFL - BT Sport' then right_broadcast_duration else 0 end) 
as SPFLBTS_right_broadcast_duration
,sum(case when analysis_right ='SPL - ESPN' then right_broadcast_duration else 0 end) 
as SPLESPN_right_broadcast_duration
,sum(case when analysis_right ='SPL - Sky Sports' then right_broadcast_duration else 0 end) 
as SPLSS_right_broadcast_duration
,sum(case when analysis_right ='Sportfive World Cup 2014 Qualifers - Sky Sports' then right_broadcast_duration else 0 end) 
as SP5SS_right_broadcast_duration
,sum(case when analysis_right ='The boat race - BBC' then right_broadcast_duration else 0 end) 
as BTRBBC_right_broadcast_duration
,sum(case when analysis_right ='The football league show - BBC' then right_broadcast_duration else 0 end) 
as FLSBBC_right_broadcast_duration
,sum(case when analysis_right ='The Masters Golf - BBC' then right_broadcast_duration else 0 end) 
as MGBBC_right_broadcast_duration
,sum(case when analysis_right ='TNA Wrestling Challenge' then right_broadcast_duration else 0 end) 
as TNACHA_right_broadcast_duration
,sum(case when analysis_right ='Tour de France - Eurosport' then right_broadcast_duration else 0 end) 
as TDFEUR_right_broadcast_duration
,sum(case when analysis_right ='Tour de France - ITV' then right_broadcast_duration else 0 end) 
as TDFITV_right_broadcast_duration
,sum(case when analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then right_broadcast_duration else 0 end) 
as USMGSS_right_broadcast_duration
,sum(case when analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then right_broadcast_duration else 0 end) 
as USOTSS_right_broadcast_duration
,sum(case when analysis_right ='U.S Open/Womens/Seniors/Amateur Golf Sky Sports' then right_broadcast_duration else 0 end) 
as USOGSS_right_broadcast_duration
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports' then right_broadcast_duration else 0 end) 
as CLASS_right_broadcast_duration
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (non Live)' then right_broadcast_duration else 0 end) 
as CLNSS_right_broadcast_duration
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (other Live)' then right_broadcast_duration else 0 end) 
as CLOSS_right_broadcast_duration
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (Tue)' then right_broadcast_duration else 0 end) 
as CLTSS_right_broadcast_duration
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (Wed)' then right_broadcast_duration else 0 end) 
as CLWSS_right_broadcast_duration
,sum(case when analysis_right ='US Open Tennis - Eurosport' then right_broadcast_duration else 0 end) 
as USOTEUR_right_broadcast_duration
,sum(case when analysis_right ='USA Football - BT Sport' then right_broadcast_duration else 0 end) 
as USFBTS_right_broadcast_duration
,sum(case when analysis_right ='USPGA Championship (2007-2016) Sky Sports' then right_broadcast_duration else 0 end) 
as USPGASS_right_broadcast_duration
,sum(case when analysis_right ='WCQ - ESPN' then right_broadcast_duration else 0 end) 
as WCQESPN_right_broadcast_duration
,sum(case when analysis_right ='Welsh Home International Football 2012-2014 Sky Sports' then right_broadcast_duration else 0 end) 
as WIFSS_right_broadcast_duration
,sum(case when analysis_right ='West Indies Home Cricket 2013-2020 Sky Sports' then right_broadcast_duration else 0 end) 
as WICSS_right_broadcast_duration
,sum(case when analysis_right ='Wimbledon - BBC' then right_broadcast_duration else 0 end) 
as WIMBBC_right_broadcast_duration
,sum(case when analysis_right ='Womens Cricket - I C C Cricket Deal  2012-2015 Sky Sports' then right_broadcast_duration else 0 end) 
as WICCSS_right_broadcast_duration
,sum(case when analysis_right ='World Athletics Championship - More 4' then right_broadcast_duration else 0 end) 
as WACMR4_right_broadcast_duration
,sum(case when analysis_right ='World Club Championship - BBC' then right_broadcast_duration else 0 end) 
as WCLBBBC_right_broadcast_duration
,sum(case when analysis_right ='World Cup Qualifiers - BT Sport' then right_broadcast_duration else 0 end) 
as WCQBTS_right_broadcast_duration
,sum(case when analysis_right ='World Darts Championship 2009-2012 Sky Sports' then right_broadcast_duration else 0 end) 
as WDCSS_right_broadcast_duration
,sum(case when analysis_right ='World snooker championship - BBC' then right_broadcast_duration else 0 end) 
as WSCBBC_right_broadcast_duration
,sum(case when analysis_right ='WWE Sky 1 and 2' then right_broadcast_duration else 0 end) 
as WWES12_right_broadcast_duration
,sum(case when analysis_right ='WWE Sky Sports' then right_broadcast_duration else 0 end) 
as WWESS_right_broadcast_duration

,sum(case when analysis_right ='Africa Cup of Nations - Eurosport' then right_broadcast_programmes else 0 end) 
as AFCEUR_right_broadcast_programmes
,sum(case when analysis_right ='Africa Cup of Nations - ITV' then right_broadcast_programmes else 0 end) 
as AFCITV_right_broadcast_programmes
,sum(case when analysis_right ='Americas Cup - BBC' then right_broadcast_programmes else 0 end) 
as AMCBBC_right_broadcast_programmes
,sum(case when analysis_right ='Asian Tour Golf 2012-2013 Sky Sports' then right_broadcast_programmes else 0 end) 
as ATGSS_right_broadcast_programmes
,sum(case when analysis_right ='ATP Tennis Masters Series  2011-2013 Sky Sports' then right_broadcast_programmes else 0 end) 
as ATPSS_right_broadcast_programmes
,sum(case when analysis_right ='Australia Home Cricket  2012-2016 Sky Sports' then right_broadcast_programmes else 0 end) 
as AHCSS_right_broadcast_programmes
,sum(case when analysis_right ='Australian Football - BT Sport' then right_broadcast_programmes else 0 end) 
as AUFBTS_right_broadcast_programmes
,sum(case when analysis_right ='Australian Open Tennis - BBC' then right_broadcast_programmes else 0 end) 
as AOTBBC_right_broadcast_programmes
,sum(case when analysis_right ='Australian Open Tennis - Eurosport' then right_broadcast_programmes else 0 end) 
as AOTEUR_right_broadcast_programmes
,sum(case when analysis_right ='Aviva Premiership - ESPN' then right_broadcast_programmes else 0 end) 
as AVPSS_right_broadcast_programmes
,sum(case when analysis_right ='BBC American Football' then right_broadcast_programmes else 0 end) 
as AFBBC_right_broadcast_programmes
,sum(case when analysis_right ='BBC Athletics' then right_broadcast_programmes else 0 end) 
as ATHBBC_right_broadcast_programmes
,sum(case when analysis_right ='BBC Boxing' then right_broadcast_programmes else 0 end) 
as BOXBBC_right_broadcast_programmes
,sum(case when analysis_right ='BBC Darts' then right_broadcast_programmes else 0 end) 
as DRTBBC_right_broadcast_programmes
,sum(case when analysis_right ='BBC Equestrian' then right_broadcast_programmes else 0 end) 
as EQUBBC_right_broadcast_programmes
,sum(case when analysis_right ='BBC Football' then right_broadcast_programmes else 0 end) 
as FOOTBBC_right_broadcast_programmes
,sum(case when analysis_right ='BBC Golf' then right_broadcast_programmes else 0 end) 
as GOLFBBC_right_broadcast_programmes
,sum(case when analysis_right ='BBC Motor Sport' then right_broadcast_programmes else 0 end) 
as MSPBBC_right_broadcast_programmes
,sum(case when analysis_right ='BBC Rugby' then right_broadcast_programmes else 0 end) 
as RUGBBC_right_broadcast_programmes
,sum(case when analysis_right ='BBC Snooker/Pool' then right_broadcast_programmes else 0 end) 
as SNPBBC_right_broadcast_programmes
,sum(case when analysis_right ='BBC Tennis' then right_broadcast_programmes else 0 end) 
as TENBBC_right_broadcast_programmes
,sum(case when analysis_right ='BBC Unknown' then right_broadcast_programmes else 0 end) 
as UNKBBC_right_broadcast_programmes
,sum(case when analysis_right ='BBC Watersports' then right_broadcast_programmes else 0 end) 
as WATBBC_right_broadcast_programmes
,sum(case when analysis_right ='BBC Wintersports' then right_broadcast_programmes else 0 end) 
as WINBBC_right_broadcast_programmes
,sum(case when analysis_right ='Boxing  - Channel 5' then right_broadcast_programmes else 0 end) 
as BOXCH5_right_broadcast_programmes
,sum(case when analysis_right ='Boxing - Matchroom 2012-2014 Sky Sports' then right_broadcast_programmes else 0 end) 
as BOXMSS_right_broadcast_programmes
,sum(case when analysis_right ='Brazil Football - BT Sport' then right_broadcast_programmes else 0 end) 
as BFTBTS_right_broadcast_programmes
,sum(case when analysis_right ='British & Irish Lions 2013 - Sky Sports' then right_broadcast_programmes else 0 end) 
as BILSS_right_broadcast_programmes
,sum(case when analysis_right ='British Open Golf - BBC' then right_broadcast_programmes else 0 end) 
as BOGSS_right_broadcast_programmes
,sum(case when analysis_right ='BT Sport American Football' then right_broadcast_programmes else 0 end) 
as AFBTS_right_broadcast_programmes
,sum(case when analysis_right ='BT Sport Athletics' then right_broadcast_programmes else 0 end) 
as ATHBTS_right_broadcast_programmes
,sum(case when analysis_right ='BT Sport Baseball' then right_broadcast_programmes else 0 end) 
as BASEBTS_right_broadcast_programmes
,sum(case when analysis_right ='BT Sport Basketball' then right_broadcast_programmes else 0 end) 
as BASKBTS_right_broadcast_programmes
,sum(case when analysis_right ='BT Sport Boxing' then right_broadcast_programmes else 0 end) 
as BOXBTS_right_broadcast_programmes
,sum(case when analysis_right ='BT Sport Cricket' then right_broadcast_programmes else 0 end) 
as CRIBTS_right_broadcast_programmes
,sum(case when analysis_right ='BT Sport Equestrian' then right_broadcast_programmes else 0 end) 
as EQUBTS_right_broadcast_programmes
,sum(case when analysis_right ='BT Sport Extreme' then right_broadcast_programmes else 0 end) 
as EXTBTS_right_broadcast_programmes
,sum(case when analysis_right ='BT Sport Football' then right_broadcast_programmes else 0 end) 
as FOOTBTS_right_broadcast_programmes
,sum(case when analysis_right ='BT Sport Motor Sport' then right_broadcast_programmes else 0 end) 
as MSPBTS_right_broadcast_programmes
,sum(case when analysis_right ='BT Sport Rugby' then right_broadcast_programmes else 0 end) 
as RUGBTS_right_broadcast_programmes
,sum(case when analysis_right ='BT Sport Tennis' then right_broadcast_programmes else 0 end) 
as TENBTS_right_broadcast_programmes
,sum(case when analysis_right ='BT Sport Unknown' then right_broadcast_programmes else 0 end) 
as UNKBTS_right_broadcast_programmes
,sum(case when analysis_right ='BT Sport Wintersports' then right_broadcast_programmes else 0 end) 
as WINBTS_right_broadcast_programmes
,sum(case when analysis_right ='Bundesliga - BT Sport' then right_broadcast_programmes else 0 end) 
as BUNBTS_right_broadcast_programmes
,sum(case when analysis_right ='Bundesliga- ESPN' then right_broadcast_programmes else 0 end) 
as BUNESPN_right_broadcast_programmes
,sum(case when analysis_right ='Challenge Darts' then right_broadcast_programmes else 0 end) 
as DRTCHA_right_broadcast_programmes
,sum(case when analysis_right ='Challenge Extreme' then right_broadcast_programmes else 0 end) 
as EXTCHA_right_broadcast_programmes
,sum(case when analysis_right ='Challenge Unknown' then right_broadcast_programmes else 0 end) 
as UNKCHA_right_broadcast_programmes
,sum(case when analysis_right ='Challenge Wrestling' then right_broadcast_programmes else 0 end) 
as WRECHA_right_broadcast_programmes
,sum(case when analysis_right ='Champions League - ITV' then right_broadcast_programmes else 0 end) 
as CHLITV_right_broadcast_programmes
,sum(case when analysis_right ='Champions Trophy - I C C Cricket Deal  2012-2015 Sky Sports' then right_broadcast_programmes else 0 end) 
as ICCSS_right_broadcast_programmes
,sum(case when analysis_right ='Channel 4 American Football' then right_broadcast_programmes else 0 end) 
as AMCH4_right_broadcast_programmes
,sum(case when analysis_right ='Channel 4 Athletics' then right_broadcast_programmes else 0 end) 
as ATHCH4_right_broadcast_programmes
,sum(case when analysis_right ='Channel 4 Boxing' then right_broadcast_programmes else 0 end) 
as BOXCH4_right_broadcast_programmes
,sum(case when analysis_right ='Channel 4 Equestrian' then right_broadcast_programmes else 0 end) 
as EQUCH4_right_broadcast_programmes
,sum(case when analysis_right ='Channel 4 Extreme' then right_broadcast_programmes else 0 end) 
as EXTCH4_right_broadcast_programmes
,sum(case when analysis_right ='Channel 4 Football' then right_broadcast_programmes else 0 end) 
as FOOTCH4_right_broadcast_programmes
,sum(case when analysis_right ='Channel 4 Motor Sport' then right_broadcast_programmes else 0 end) 
as MSPCH4_right_broadcast_programmes
,sum(case when analysis_right ='Channel 4 Racing' then right_broadcast_programmes else 0 end) 
as RACCH4_right_broadcast_programmes
,sum(case when analysis_right ='Channel 4 Unknown' then right_broadcast_programmes else 0 end) 
as UNKCH4_right_broadcast_programmes
,sum(case when analysis_right ='Channel 4 Watersports' then right_broadcast_programmes else 0 end) 
as WATCH4_right_broadcast_programmes
,sum(case when analysis_right ='Channel 4 Wintersports' then right_broadcast_programmes else 0 end) 
as WINCH4_right_broadcast_programmes
,sum(case when analysis_right ='Channel 5 Athletics' then right_broadcast_programmes else 0 end) 
as ATHCH5_right_broadcast_programmes
,sum(case when analysis_right ='Channel 5 Boxing' then right_broadcast_programmes else 0 end) 
as BOXOCH5_right_broadcast_programmes
,sum(case when analysis_right ='Channel 5 Cricket' then right_broadcast_programmes else 0 end) 
as CRICH5_right_broadcast_programmes
,sum(case when analysis_right ='Channel 5 Motor Sport' then right_broadcast_programmes else 0 end) 
as MSPCH5_right_broadcast_programmes
,sum(case when analysis_right ='Channel 5 Unknown' then right_broadcast_programmes else 0 end) 
as UNKCH5_right_broadcast_programmes
,sum(case when analysis_right ='Channel 5 Wrestling' then right_broadcast_programmes else 0 end) 
as WRECH5_right_broadcast_programmes
,sum(case when analysis_right ='Cheltenham Festival - Channel 4' then right_broadcast_programmes else 0 end) 
as CHELCH4_right_broadcast_programmes
,sum(case when analysis_right ='Community Shield - ITV' then right_broadcast_programmes else 0 end) 
as CMSITV_right_broadcast_programmes
,sum(case when analysis_right ='Confederations Cup - BBC' then right_broadcast_programmes else 0 end) 
as CONCBBC_right_broadcast_programmes
,sum(case when analysis_right ='Conference - BT Sport' then right_broadcast_programmes else 0 end) 
as CONFBTS_right_broadcast_programmes
,sum(case when analysis_right ='Cycling - La Vuelta ITV' then right_broadcast_programmes else 0 end) 
as CLVITV_right_broadcast_programmes
,sum(case when analysis_right ='Cycling - U C I World Tour Sky Sports' then right_broadcast_programmes else 0 end) 
as CUCISS_right_broadcast_programmes
,sum(case when analysis_right ='Cycling Tour of Britain - Eurosport' then right_broadcast_programmes else 0 end) 
as CTBEUR_right_broadcast_programmes
,sum(case when analysis_right ='Cycling: tour of britain ITV4' then right_broadcast_programmes else 0 end) 
as CTCITV_right_broadcast_programmes
,sum(case when analysis_right ='Derby - Channel 4' then right_broadcast_programmes else 0 end) 
as DERCH4_right_broadcast_programmes
,sum(case when analysis_right ='ECB (highlights) - Channel 5' then right_broadcast_programmes else 0 end) 
as ECBHCH5_right_broadcast_programmes
,sum(case when analysis_right ='ECB Cricket Sky Sports' then right_broadcast_programmes else 0 end) 
as GECRSS_right_broadcast_programmes
,sum(case when analysis_right ='ECB non-Test Cricket Sky Sports' then right_broadcast_programmes else 0 end) 
as ECBNSS_right_broadcast_programmes
,sum(case when analysis_right ='ECB Test Cricket Sky Sports' then right_broadcast_programmes else 0 end) 
as ECBTSS_right_broadcast_programmes
,sum(case when analysis_right ='England Football Internationals - ITV' then right_broadcast_programmes else 0 end) 
as GENGITV_right_broadcast_programmes
,sum(case when analysis_right ='England Friendlies (Football) - ITV' then right_broadcast_programmes else 0 end) 
as EFRITV_right_broadcast_programmes
,sum(case when analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then right_broadcast_programmes else 0 end) 
as ENRSS_right_broadcast_programmes
,sum(case when analysis_right ='England World Cup Qualifying (Away) - ITV' then right_broadcast_programmes else 0 end) 
as EWQAITV_right_broadcast_programmes
,sum(case when analysis_right ='England World Cup Qualifying (Home) - ITV' then right_broadcast_programmes else 0 end) 
as EWQHITV_right_broadcast_programmes
,sum(case when analysis_right ='ESPN American Football' then right_broadcast_programmes else 0 end) 
as AMESPN_right_broadcast_programmes
,sum(case when analysis_right ='ESPN Athletics' then right_broadcast_programmes else 0 end) 
as ATHESPN_right_broadcast_programmes
,sum(case when analysis_right ='ESPN Baseball' then right_broadcast_programmes else 0 end) 
as BASEESPN_right_broadcast_programmes
,sum(case when analysis_right ='ESPN Basketball' then right_broadcast_programmes else 0 end) 
as BASKESPN_right_broadcast_programmes
,sum(case when analysis_right ='ESPN Boxing' then right_broadcast_programmes else 0 end) 
as BOXESPN_right_broadcast_programmes
,sum(case when analysis_right ='ESPN Cricket' then right_broadcast_programmes else 0 end) 
as CRIESPN_right_broadcast_programmes
,sum(case when analysis_right ='ESPN Darts' then right_broadcast_programmes else 0 end) 
as DARTESPN_right_broadcast_programmes
,sum(case when analysis_right ='ESPN Equestrian' then right_broadcast_programmes else 0 end) 
as EQUESPN_right_broadcast_programmes
,sum(case when analysis_right ='ESPN Extreme' then right_broadcast_programmes else 0 end) 
as EXTESPN_right_broadcast_programmes
,sum(case when analysis_right ='ESPN Football' then right_broadcast_programmes else 0 end) 
as FOOTESPN_right_broadcast_programmes
,sum(case when analysis_right ='ESPN Golf' then right_broadcast_programmes else 0 end) 
as GOLFESPN_right_broadcast_programmes
,sum(case when analysis_right ='ESPN Ice Hockey' then right_broadcast_programmes else 0 end) 
as IHESPN_right_broadcast_programmes
,sum(case when analysis_right ='ESPN Motor Sport' then right_broadcast_programmes else 0 end) 
as MSPESPN_right_broadcast_programmes
,sum(case when analysis_right ='ESPN Racing' then right_broadcast_programmes else 0 end) 
as RACESPN_right_broadcast_programmes
,sum(case when analysis_right ='ESPN Rugby' then right_broadcast_programmes else 0 end) 
as RUGESPN_right_broadcast_programmes
,sum(case when analysis_right ='ESPN Snooker/Pool' then right_broadcast_programmes else 0 end) 
as SNPESPN_right_broadcast_programmes
,sum(case when analysis_right ='ESPN Tennis' then right_broadcast_programmes else 0 end) 
as TENESPN_right_broadcast_programmes
,sum(case when analysis_right ='ESPN Unknown' then right_broadcast_programmes else 0 end) 
as UNKESPN_right_broadcast_programmes
,sum(case when analysis_right ='ESPN Watersports' then right_broadcast_programmes else 0 end) 
as WATESPN_right_broadcast_programmes
,sum(case when analysis_right ='ESPN Wintersports' then right_broadcast_programmes else 0 end) 
as WINESPN_right_broadcast_programmes
,sum(case when analysis_right ='ESPN Wrestling' then right_broadcast_programmes else 0 end) 
as WREESPN_right_broadcast_programmes
,sum(case when analysis_right ='Europa League - BT Sport' then right_broadcast_programmes else 0 end) 
as ELBTSP_right_broadcast_programmes
,sum(case when analysis_right ='Europa League - ESPN' then right_broadcast_programmes else 0 end) 
as ELESPN_right_broadcast_programmes
,sum(case when analysis_right ='Europa League - ITV' then right_broadcast_programmes else 0 end) 
as ELITV_right_broadcast_programmes
,sum(case when analysis_right ='European Tour Golf - Sky Sports' then right_broadcast_programmes else 0 end) 
as ETGSS_right_broadcast_programmes
,sum(case when analysis_right ='Eurosport American Football' then right_broadcast_programmes else 0 end) 
as AMEUR_right_broadcast_programmes
,sum(case when analysis_right ='Eurosport Athletics' then right_broadcast_programmes else 0 end) 
as ATHEUR_right_broadcast_programmes
,sum(case when analysis_right ='Eurosport Baseball' then right_broadcast_programmes else 0 end) 
as BASEEUR_right_broadcast_programmes
,sum(case when analysis_right ='Eurosport Basketball' then right_broadcast_programmes else 0 end) 
as BASKEUR_right_broadcast_programmes
,sum(case when analysis_right ='Eurosport Boxing' then right_broadcast_programmes else 0 end) 
as BOXEUR_right_broadcast_programmes
,sum(case when analysis_right ='Eurosport Cricket' then right_broadcast_programmes else 0 end) 
as CRIEUR_right_broadcast_programmes
,sum(case when analysis_right ='Eurosport Darts' then right_broadcast_programmes else 0 end) 
as DARTEUR_right_broadcast_programmes
,sum(case when analysis_right ='Eurosport Equestrian' then right_broadcast_programmes else 0 end) 
as EQUEUR_right_broadcast_programmes
,sum(case when analysis_right ='Eurosport Extreme' then right_broadcast_programmes else 0 end) 
as EXTEUR_right_broadcast_programmes
,sum(case when analysis_right ='Eurosport Football' then right_broadcast_programmes else 0 end) 
as FOOTEUR_right_broadcast_programmes
,sum(case when analysis_right ='Eurosport Golf' then right_broadcast_programmes else 0 end) 
as GOLFEUR_right_broadcast_programmes
,sum(case when analysis_right ='Eurosport Ice Hockey' then right_broadcast_programmes else 0 end) 
as IHEUR_right_broadcast_programmes
,sum(case when analysis_right ='Eurosport Motor Sport' then right_broadcast_programmes else 0 end) 
as MSPEUR_right_broadcast_programmes
,sum(case when analysis_right ='Eurosport Racing' then right_broadcast_programmes else 0 end) 
as RACEUR_right_broadcast_programmes
,sum(case when analysis_right ='Eurosport Snooker/Pool' then right_broadcast_programmes else 0 end) 
as SNPEUR_right_broadcast_programmes
,sum(case when analysis_right ='Eurosport Tennis' then right_broadcast_programmes else 0 end) 
as TENEUR_right_broadcast_programmes
,sum(case when analysis_right ='Eurosport Unknown' then right_broadcast_programmes else 0 end) 
as UNKEUR_right_broadcast_programmes
,sum(case when analysis_right ='Eurosport Watersports' then right_broadcast_programmes else 0 end) 
as WATEUR_right_broadcast_programmes
,sum(case when analysis_right ='Eurosport Wintersports' then right_broadcast_programmes else 0 end) 
as WINEUR_right_broadcast_programmes
,sum(case when analysis_right ='F1 - BBC' then right_broadcast_programmes else 0 end) 
as GF1BBC_right_broadcast_programmes
,sum(case when analysis_right ='F1 - Sky Sports' then right_broadcast_programmes else 0 end) 
as GF1SS_right_broadcast_programmes
,sum(case when analysis_right ='F1 (non-Live)- BBC' then right_broadcast_programmes else 0 end) 
as F1NBBC_right_broadcast_programmes
,sum(case when analysis_right ='F1 (Practice Live)- BBC' then right_broadcast_programmes else 0 end) 
as F1PBBC_right_broadcast_programmes
,sum(case when analysis_right ='F1 (Qualifying Live)- BBC' then right_broadcast_programmes else 0 end) 
as F1QBBC_right_broadcast_programmes
,sum(case when analysis_right ='F1 (Race Live)- BBC' then right_broadcast_programmes else 0 end) 
as F1RBBC_right_broadcast_programmes
,sum(case when analysis_right ='FA Cup - ESPN' then right_broadcast_programmes else 0 end) 
as FACESPN_right_broadcast_programmes
,sum(case when analysis_right ='FA Cup - ITV' then right_broadcast_programmes else 0 end) 
as FACITV_right_broadcast_programmes
,sum(case when analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then right_broadcast_programmes else 0 end) 
as FLCCSS_right_broadcast_programmes
,sum(case when analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - others' then right_broadcast_programmes else 0 end) 
as FLOTSS_right_broadcast_programmes
,sum(case when analysis_right ='Formula One 2012-2018 - (non-Live) Sky Sports' then right_broadcast_programmes else 0 end) 
as F1NSS_right_broadcast_programmes
,sum(case when analysis_right ='Formula One 2012-2018 - (Practice Live) Sky Sports' then right_broadcast_programmes else 0 end) 
as F1PSS_right_broadcast_programmes
,sum(case when analysis_right ='Formula One 2012-2018 - (Qualifying Live) Sky Sports' then right_broadcast_programmes else 0 end) 
as F1QSS_right_broadcast_programmes
,sum(case when analysis_right ='Formula One 2012-2018 - (Race Live) Sky Sports' then right_broadcast_programmes else 0 end) 
as F1RSS_right_broadcast_programmes
,sum(case when analysis_right ='French Open Tennis - Eurosport' then right_broadcast_programmes else 0 end) 
as FOTEUR_right_broadcast_programmes
,sum(case when analysis_right ='French Open Tennis - ITV' then right_broadcast_programmes else 0 end) 
as FOTITV_right_broadcast_programmes
,sum(case when analysis_right ='Grand National - Channel 4' then right_broadcast_programmes else 0 end) 
as GDNCH4_right_broadcast_programmes
,sum(case when analysis_right ='Heineken Cup 2010/11 To 2013/14- Sky Sports' then right_broadcast_programmes else 0 end) 
as HECSS_right_broadcast_programmes
,sum(case when analysis_right ='I R B Sevens 2008/09 -2014/15 - Sky Sports' then right_broadcast_programmes else 0 end) 
as IRBSS_right_broadcast_programmes
,sum(case when analysis_right ='IAAF World Athletics Championship - Eurosport' then right_broadcast_programmes else 0 end) 
as WACEUR_right_broadcast_programmes
,sum(case when analysis_right ='India Home Cricket 2012-2018 Sky Sports' then right_broadcast_programmes else 0 end) 
as IHCSS_right_broadcast_programmes
,sum(case when analysis_right ='India Premier League - ITV' then right_broadcast_programmes else 0 end) 
as IPLITV_right_broadcast_programmes
,sum(case when analysis_right ='International Freindlies - ESPN' then right_broadcast_programmes else 0 end) 
as IFESPN_right_broadcast_programmes
,sum(case when analysis_right ='International Friendlies - BT Sport' then right_broadcast_programmes else 0 end) 
as IFBTS_right_broadcast_programmes
,sum(case when analysis_right ='ITV1 Boxing' then right_broadcast_programmes else 0 end) 
as BOXITV1_right_broadcast_programmes
,sum(case when analysis_right ='ITV1 Football' then right_broadcast_programmes else 0 end) 
as FOOTITV1_right_broadcast_programmes
,sum(case when analysis_right ='ITV1 Motor Sport' then right_broadcast_programmes else 0 end) 
as MOTSITV1_right_broadcast_programmes
,sum(case when analysis_right ='ITV1 Rugby' then right_broadcast_programmes else 0 end) 
as RUGITV1_right_broadcast_programmes
,sum(case when analysis_right ='ITV1 Snooker/Pool' then right_broadcast_programmes else 0 end) 
as SNPITV1_right_broadcast_programmes
,sum(case when analysis_right ='ITV1 Unknown' then right_broadcast_programmes else 0 end) 
as UNKITV1_right_broadcast_programmes
,sum(case when analysis_right ='ITV4 Boxing' then right_broadcast_programmes else 0 end) 
as BOXITV4_right_broadcast_programmes
,sum(case when analysis_right ='ITV4 Cricket' then right_broadcast_programmes else 0 end) 
as CRIITV4_right_broadcast_programmes
,sum(case when analysis_right ='ITV4 Darts' then right_broadcast_programmes else 0 end) 
as DARTITV4_right_broadcast_programmes
,sum(case when analysis_right ='ITV4 Extreme' then right_broadcast_programmes else 0 end) 
as EXTITV4_right_broadcast_programmes
,sum(case when analysis_right ='ITV4 Football' then right_broadcast_programmes else 0 end) 
as FOOTITV4_right_broadcast_programmes
,sum(case when analysis_right ='ITV4 Motor Sport' then right_broadcast_programmes else 0 end) 
as MSPITV4_right_broadcast_programmes
,sum(case when analysis_right ='ITV4 Rugby' then right_broadcast_programmes else 0 end) 
as RUGITV4_right_broadcast_programmes
,sum(case when analysis_right ='ITV4 Snooker/Pool' then right_broadcast_programmes else 0 end) 
as SNPITV4_right_broadcast_programmes
,sum(case when analysis_right ='ITV4 Tennis' then right_broadcast_programmes else 0 end) 
as TENITV4_right_broadcast_programmes
,sum(case when analysis_right ='ITV4 Unknown' then right_broadcast_programmes else 0 end) 
as UNKITV4_right_broadcast_programmes
,sum(case when analysis_right ='Ligue 1 - BT Sport' then right_broadcast_programmes else 0 end) 
as L1BTS_right_broadcast_programmes
,sum(case when analysis_right ='Ligue 1 - ESPN' then right_broadcast_programmes else 0 end) 
as L1ESPN_right_broadcast_programmes
,sum(case when analysis_right ='Match of the day - BBC' then right_broadcast_programmes else 0 end) 
as MOTDBBC_right_broadcast_programmes
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Other)- Sky Sports' then right_broadcast_programmes else 0 end) 
as MROSS_right_broadcast_programmes
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Pool)- Sky Sports' then right_broadcast_programmes else 0 end) 
as MRPSS_right_broadcast_programmes
,sum(case when analysis_right ='Matchroom Output Deal  2010-2012 (Snooker)- Sky Sports' then right_broadcast_programmes else 0 end) 
as MRSSS_right_broadcast_programmes
,sum(case when analysis_right ='Moto GP BBC' then right_broadcast_programmes else 0 end) 
as MGPBBC_right_broadcast_programmes
,sum(case when analysis_right ='NBA - Sky Sports' then right_broadcast_programmes else 0 end) 
as NBASS_right_broadcast_programmes
,sum(case when analysis_right ='NFL - BBC' then right_broadcast_programmes else 0 end) 
as NFLBBC_right_broadcast_programmes
,sum(case when analysis_right ='NFL - Channel 4' then right_broadcast_programmes else 0 end) 
as NFLCH4_right_broadcast_programmes
,sum(case when analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then right_broadcast_programmes else 0 end) 
as NFLSS_right_broadcast_programmes
,sum(case when analysis_right ='Northern Ireland Football Association 2012-2014 - Sky Sports' then right_broadcast_programmes else 0 end) 
as NIFSS_right_broadcast_programmes
,sum(case when analysis_right ='Oaks - Channel 4' then right_broadcast_programmes else 0 end) 
as OAKCH4_right_broadcast_programmes
,sum(case when analysis_right ='Other American Football' then right_broadcast_programmes else 0 end) 
as AMOTH_right_broadcast_programmes
,sum(case when analysis_right ='Other Athletics' then right_broadcast_programmes else 0 end) 
as ATHOTH_right_broadcast_programmes
,sum(case when analysis_right ='Other Baseball' then right_broadcast_programmes else 0 end) 
as BASEOTH_right_broadcast_programmes
,sum(case when analysis_right ='Other Basketball' then right_broadcast_programmes else 0 end) 
as BASKOTH_right_broadcast_programmes
,sum(case when analysis_right ='Other Boxing' then right_broadcast_programmes else 0 end) 
as BOXOTH_right_broadcast_programmes
,sum(case when analysis_right ='Other Cricket' then right_broadcast_programmes else 0 end) 
as CRIOTH_right_broadcast_programmes
,sum(case when analysis_right ='Other Darts' then right_broadcast_programmes else 0 end) 
as DARTOTH_right_broadcast_programmes
,sum(case when analysis_right ='Other Equestrian' then right_broadcast_programmes else 0 end) 
as EQUOTH_right_broadcast_programmes
,sum(case when analysis_right ='Other Extreme' then right_broadcast_programmes else 0 end) 
as EXTOTH_right_broadcast_programmes
,sum(case when analysis_right ='Other Fishing' then right_broadcast_programmes else 0 end) 
as FSHOTH_right_broadcast_programmes
,sum(case when analysis_right ='Other Football' then right_broadcast_programmes else 0 end) 
as FOOTOTH_right_broadcast_programmes
,sum(case when analysis_right ='Other Golf' then right_broadcast_programmes else 0 end) 
as GOLFOTH_right_broadcast_programmes
,sum(case when analysis_right ='Other Ice Hockey' then right_broadcast_programmes else 0 end) 
as IHOTH_right_broadcast_programmes
,sum(case when analysis_right ='Other Motor Sport' then right_broadcast_programmes else 0 end) 
as MSPOTH_right_broadcast_programmes
,sum(case when analysis_right ='Other Racing' then right_broadcast_programmes else 0 end) 
as RACOTH_right_broadcast_programmes
,sum(case when analysis_right ='Other Rugby' then right_broadcast_programmes else 0 end) 
as RUGOTH_right_broadcast_programmes
,sum(case when analysis_right ='Other Rugby Internationals - ESPN' then right_broadcast_programmes else 0 end) 
as ORUGESPN_right_broadcast_programmes
,sum(case when analysis_right ='Other Snooker/Pool' then right_broadcast_programmes else 0 end) 
as OTHSNP_right_broadcast_programmes
,sum(case when analysis_right ='Other Tennis' then right_broadcast_programmes else 0 end) 
as OTHTEN_right_broadcast_programmes
,sum(case when analysis_right ='Other Unknown' then right_broadcast_programmes else 0 end) 
as OTHUNK_right_broadcast_programmes
,sum(case when analysis_right ='Other Watersports' then right_broadcast_programmes else 0 end) 
as OTHWAT_right_broadcast_programmes
,sum(case when analysis_right ='Other Wintersports' then right_broadcast_programmes else 0 end) 
as OTHWIN_right_broadcast_programmes
,sum(case when analysis_right ='Other Wrestling' then right_broadcast_programmes else 0 end) 
as OTHWRE_right_broadcast_programmes
,sum(case when analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then right_broadcast_programmes else 0 end) 
as PGASS_right_broadcast_programmes
,sum(case when analysis_right ='Premier League - BT Sport' then right_broadcast_programmes else 0 end) 
as PLBTS_right_broadcast_programmes
,sum(case when analysis_right ='Premier League - ESPN' then right_broadcast_programmes else 0 end) 
as PLESPN_right_broadcast_programmes
,sum(case when analysis_right ='Premier League Darts 2010-2013 - Sky Sports' then right_broadcast_programmes else 0 end) 
as PLDSS_right_broadcast_programmes
,sum(case when analysis_right ='Premier League Football - Sky Sports' then right_broadcast_programmes else 0 end) 
as GPLSS_right_broadcast_programmes
,sum(case when analysis_right ='Premier League Football - Sky Sports (Match Choice)' then right_broadcast_programmes else 0 end) 
as PLMCSS_right_broadcast_programmes
,sum(case when analysis_right ='Premier League Football - Sky Sports (MNF)' then right_broadcast_programmes else 0 end) 
as PLMNFSS_right_broadcast_programmes
,sum(case when analysis_right ='Premier League Football - Sky Sports (non Live)' then right_broadcast_programmes else 0 end) 
as PLNLSS_right_broadcast_programmes
,sum(case when analysis_right ='Premier League Football - Sky Sports (other Live)' then right_broadcast_programmes else 0 end) 
as PLOLSS_right_broadcast_programmes
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sat Lunchtime)' then right_broadcast_programmes else 0 end) 
as PLSLSS_right_broadcast_programmes
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sat Night Live)' then right_broadcast_programmes else 0 end) 
as PLSNSS_right_broadcast_programmes
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sun 4pm)' then right_broadcast_programmes else 0 end) 
as PLS4SS_right_broadcast_programmes
,sum(case when analysis_right ='Premier League Football - Sky Sports (Sun Lunchtime)' then right_broadcast_programmes else 0 end) 
as PLSULSS_right_broadcast_programmes
,sum(case when analysis_right ='Premiership Rugby - Sky Sports' then right_broadcast_programmes else 0 end) 
as PRUSS_right_broadcast_programmes
,sum(case when analysis_right ='Republic Of Ireland FA - 2010/11 To 2013/14 - Sky Sports' then right_broadcast_programmes else 0 end) 
as ROISS_right_broadcast_programmes
,sum(case when analysis_right ='Royal Ascot - Channel 4' then right_broadcast_programmes else 0 end) 
as RASCH4_right_broadcast_programmes
,sum(case when analysis_right ='Rugby Internationals (England) - BBC' then right_broadcast_programmes else 0 end) 
as RIEBBC_right_broadcast_programmes
,sum(case when analysis_right ='Rugby Internationals (Ireland) - BBC' then right_broadcast_programmes else 0 end) 
as RIIBBC_right_broadcast_programmes
,sum(case when analysis_right ='Rugby Internationals (Scotland) - BBC' then right_broadcast_programmes else 0 end) 
as RISBBC_right_broadcast_programmes
,sum(case when analysis_right ='Rugby Internationals (Wales) - BBC' then right_broadcast_programmes else 0 end) 
as RIWBBC_right_broadcast_programmes
,sum(case when analysis_right ='Rugby League  Challenge Cup- BBC' then right_broadcast_programmes else 0 end) 
as RLCCBBC_right_broadcast_programmes
,sum(case when analysis_right ='Rugby League - Sky Sports' then right_broadcast_programmes else 0 end) 
as RLGSS_right_broadcast_programmes
,sum(case when analysis_right ='Rugby League  World Cup- BBC' then right_broadcast_programmes else 0 end) 
as RLWCBBC_right_broadcast_programmes
,sum(case when analysis_right ='Sanzar Rugby Union 2011 - 2015 - Sky Sports' then right_broadcast_programmes else 0 end) 
as SARUSS_right_broadcast_programmes
,sum(case when analysis_right ='Scottish FA Deal - 2010/11 To 2013/14 - Sky Sports' then right_broadcast_programmes else 0 end) 
as SFASS_right_broadcast_programmes
,sum(case when analysis_right ='Serie A - BT Sport' then right_broadcast_programmes else 0 end) 
as SABTS_right_broadcast_programmes
,sum(case when analysis_right ='Serie A - ESPN' then right_broadcast_programmes else 0 end) 
as SAESPN_right_broadcast_programmes
,sum(case when analysis_right ='SFL - ESPN' then right_broadcast_programmes else 0 end) 
as SFLESPN_right_broadcast_programmes
,sum(case when analysis_right ='Six Nations - BBC' then right_broadcast_programmes else 0 end) 
as SNRBBC_right_broadcast_programmes
,sum(case when analysis_right ='Sky 1 and Sky 2 Boxing' then right_broadcast_programmes else 0 end) 
as BOXS12_right_broadcast_programmes
,sum(case when analysis_right ='Sky 1 and Sky 2 Football' then right_broadcast_programmes else 0 end) 
as FOOTS12_right_broadcast_programmes
,sum(case when analysis_right ='Sky 1 and Sky 2 Motor Sport' then right_broadcast_programmes else 0 end) 
as MSPS12_right_broadcast_programmes
,sum(case when analysis_right ='Sky 1 and Sky 2 Unknown' then right_broadcast_programmes else 0 end) 
as UNKS12_right_broadcast_programmes
,sum(case when analysis_right ='Sky 1 and Sky 2 Wrestling' then right_broadcast_programmes else 0 end) 
as WRES12_right_broadcast_programmes
,sum(case when analysis_right ='Sky Sports American Football' then right_broadcast_programmes else 0 end) 
as AMSS_right_broadcast_programmes
,sum(case when analysis_right ='Sky Sports Athletics' then right_broadcast_programmes else 0 end) 
as ATHSS_right_broadcast_programmes
,sum(case when analysis_right ='Sky Sports Baseball' then right_broadcast_programmes else 0 end) 
as BASESS_right_broadcast_programmes
,sum(case when analysis_right ='Sky Sports Basketball' then right_broadcast_programmes else 0 end) 
as BASKSS_right_broadcast_programmes
,sum(case when analysis_right ='Sky Sports Boxing' then right_broadcast_programmes else 0 end) 
as BOXSS_right_broadcast_programmes
,sum(case when analysis_right ='Sky Sports Cricket' then right_broadcast_programmes else 0 end) 
as CRISS_right_broadcast_programmes
,sum(case when analysis_right ='Sky Sports Darts' then right_broadcast_programmes else 0 end) 
as DARTSS_right_broadcast_programmes
,sum(case when analysis_right ='Sky Sports Equestrian' then right_broadcast_programmes else 0 end) 
as EQUSS_right_broadcast_programmes
,sum(case when analysis_right ='Sky Sports Extreme' then right_broadcast_programmes else 0 end) 
as EXTSS_right_broadcast_programmes
,sum(case when analysis_right ='Sky Sports Fishing' then right_broadcast_programmes else 0 end) 
as FISHSS_right_broadcast_programmes
,sum(case when analysis_right ='Sky Sports Football' then right_broadcast_programmes else 0 end) 
as FOOTSS_right_broadcast_programmes
,sum(case when analysis_right ='Sky Sports Golf' then right_broadcast_programmes else 0 end) 
as GOLFSS_right_broadcast_programmes
,sum(case when analysis_right ='Sky Sports Ice Hockey' then right_broadcast_programmes else 0 end) 
as IHSS_right_broadcast_programmes
,sum(case when analysis_right ='Sky Sports Motor Sport' then right_broadcast_programmes else 0 end) 
as MSPSS_right_broadcast_programmes
,sum(case when analysis_right ='Sky Sports Racing' then right_broadcast_programmes else 0 end) 
as RACSS_right_broadcast_programmes
,sum(case when analysis_right ='Sky Sports Rugby' then right_broadcast_programmes else 0 end) 
as RUGSS_right_broadcast_programmes
,sum(case when analysis_right ='Sky Sports Snooker/Pool' then right_broadcast_programmes else 0 end) 
as SNPSS_right_broadcast_programmes
,sum(case when analysis_right ='Sky Sports Tennis' then right_broadcast_programmes else 0 end) 
as TENSS_right_broadcast_programmes
,sum(case when analysis_right ='Sky Sports Unknown' then right_broadcast_programmes else 0 end) 
as UNKSS_right_broadcast_programmes
,sum(case when analysis_right ='Sky Sports Watersports' then right_broadcast_programmes else 0 end) 
as WATSS_right_broadcast_programmes
,sum(case when analysis_right ='Sky Sports Wintersports' then right_broadcast_programmes else 0 end) 
as WINSS_right_broadcast_programmes
,sum(case when analysis_right ='Sky Sports Wrestling' then right_broadcast_programmes else 0 end) 
as WRESS_right_broadcast_programmes
,sum(case when analysis_right ='Solheim Cup 2013 Ladies Golf - Sky Sports' then right_broadcast_programmes else 0 end) 
as SOLSS_right_broadcast_programmes
,sum(case when analysis_right ='South African Home Cricket  2012-2020 Sky Sports' then right_broadcast_programmes else 0 end) 
as SACSS_right_broadcast_programmes
,sum(case when analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then right_broadcast_programmes else 0 end) 
as SPFSS_right_broadcast_programmes
,sum(case when analysis_right ='SPFL - BT Sport' then right_broadcast_programmes else 0 end) 
as SPFLBTS_right_broadcast_programmes
,sum(case when analysis_right ='SPL - ESPN' then right_broadcast_programmes else 0 end) 
as SPLESPN_right_broadcast_programmes
,sum(case when analysis_right ='SPL - Sky Sports' then right_broadcast_programmes else 0 end) 
as SPLSS_right_broadcast_programmes
,sum(case when analysis_right ='Sportfive World Cup 2014 Qualifers - Sky Sports' then right_broadcast_programmes else 0 end) 
as SP5SS_right_broadcast_programmes
,sum(case when analysis_right ='The boat race - BBC' then right_broadcast_programmes else 0 end) 
as BTRBBC_right_broadcast_programmes
,sum(case when analysis_right ='The football league show - BBC' then right_broadcast_programmes else 0 end) 
as FLSBBC_right_broadcast_programmes
,sum(case when analysis_right ='The Masters Golf - BBC' then right_broadcast_programmes else 0 end) 
as MGBBC_right_broadcast_programmes
,sum(case when analysis_right ='TNA Wrestling Challenge' then right_broadcast_programmes else 0 end) 
as TNACHA_right_broadcast_programmes
,sum(case when analysis_right ='Tour de France - Eurosport' then right_broadcast_programmes else 0 end) 
as TDFEUR_right_broadcast_programmes
,sum(case when analysis_right ='Tour de France - ITV' then right_broadcast_programmes else 0 end) 
as TDFITV_right_broadcast_programmes
,sum(case when analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then right_broadcast_programmes else 0 end) 
as USMGSS_right_broadcast_programmes
,sum(case when analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then right_broadcast_programmes else 0 end) 
as USOTSS_right_broadcast_programmes
,sum(case when analysis_right ='U.S Open/Womens/Seniors/Amateur Golf Sky Sports' then right_broadcast_programmes else 0 end) 
as USOGSS_right_broadcast_programmes
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports' then right_broadcast_programmes else 0 end) 
as CLASS_right_broadcast_programmes
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (non Live)' then right_broadcast_programmes else 0 end) 
as CLNSS_right_broadcast_programmes
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (other Live)' then right_broadcast_programmes else 0 end) 
as CLOSS_right_broadcast_programmes
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (Tue)' then right_broadcast_programmes else 0 end) 
as CLTSS_right_broadcast_programmes
,sum(case when analysis_right ='UEFA Champions League -  Sky Sports (Wed)' then right_broadcast_programmes else 0 end) 
as CLWSS_right_broadcast_programmes
,sum(case when analysis_right ='US Open Tennis - Eurosport' then right_broadcast_programmes else 0 end) 
as USOTEUR_right_broadcast_programmes
,sum(case when analysis_right ='USA Football - BT Sport' then right_broadcast_programmes else 0 end) 
as USFBTS_right_broadcast_programmes
,sum(case when analysis_right ='USPGA Championship (2007-2016) Sky Sports' then right_broadcast_programmes else 0 end) 
as USPGASS_right_broadcast_programmes
,sum(case when analysis_right ='WCQ - ESPN' then right_broadcast_programmes else 0 end) 
as WCQESPN_right_broadcast_programmes
,sum(case when analysis_right ='Welsh Home International Football 2012-2014 Sky Sports' then right_broadcast_programmes else 0 end) 
as WIFSS_right_broadcast_programmes
,sum(case when analysis_right ='West Indies Home Cricket 2013-2020 Sky Sports' then right_broadcast_programmes else 0 end) 
as WICSS_right_broadcast_programmes
,sum(case when analysis_right ='Wimbledon - BBC' then right_broadcast_programmes else 0 end) 
as WIMBBC_right_broadcast_programmes
,sum(case when analysis_right ='Womens Cricket - I C C Cricket Deal  2012-2015 Sky Sports' then right_broadcast_programmes else 0 end) 
as WICCSS_right_broadcast_programmes
,sum(case when analysis_right ='World Athletics Championship - More 4' then right_broadcast_programmes else 0 end) 
as WACMR4_right_broadcast_programmes
,sum(case when analysis_right ='World Club Championship - BBC' then right_broadcast_programmes else 0 end) 
as WCLBBBC_right_broadcast_programmes
,sum(case when analysis_right ='World Cup Qualifiers - BT Sport' then right_broadcast_programmes else 0 end) 
as WCQBTS_right_broadcast_programmes
,sum(case when analysis_right ='World Darts Championship 2009-2012 Sky Sports' then right_broadcast_programmes else 0 end) 
as WDCSS_right_broadcast_programmes
,sum(case when analysis_right ='World snooker championship - BBC' then right_broadcast_programmes else 0 end) 
as WSCBBC_right_broadcast_programmes
,sum(case when analysis_right ='WWE Sky 1 and 2' then right_broadcast_programmes else 0 end) 
as WWES12_right_broadcast_programmes
,sum(case when analysis_right ='WWE Sky Sports' then right_broadcast_programmes else 0 end) 
as WWESS_right_broadcast_programmes





into  dbarnett.v250_right_viewable_account_summary
from  dbarnett.v250_days_right_viewable_by_account_v09
group by account_number
;
commit;
--select count(*) from dbarnett.v250_right_viewable_account_summary;
---run down to here---



CREATE HG INDEX idx1 ON dbarnett.v250_days_right_viewable_by_account_by_live_status (account_number);
commit; 
--select count(*) from  dbarnett.v250_right_viewable_account_summary;
---Aggregate for live non live---dbarnett.v250_right_viewable_account_summary_by_live_status
--select count(*) from dbarnett.v250_right_viewable_account_summary_by_live_status
drop table dbarnett.v250_right_viewable_account_summary_by_live_status;
select account_number
,sum(case when live=1 and analysis_right ='Africa Cup of Nations - Eurosport' then days_right_viewable else 0 end) 
as AFCEUR_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Africa Cup of Nations - ITV' then days_right_viewable else 0 end) 
as AFCITV_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Americas Cup - BBC' then days_right_viewable else 0 end) 
as AMCBBC_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Asian Tour Golf 2012-2013 Sky Sports' then days_right_viewable else 0 end) 
as ATGSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='ATP Tennis Masters Series  2011-2013 Sky Sports' then days_right_viewable else 0 end) 
as ATPSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Australia Home Cricket  2012-2016 Sky Sports' then days_right_viewable else 0 end) 
as AHCSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Australian Football - BT Sport' then days_right_viewable else 0 end) 
as AUFBTS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Australian Open Tennis - BBC' then days_right_viewable else 0 end) 
as AOTBBC_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Australian Open Tennis - Eurosport' then days_right_viewable else 0 end) 
as AOTEUR_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Aviva Premiership - ESPN' then days_right_viewable else 0 end) 
as AVPSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='BBC American Football' then days_right_viewable else 0 end) 
as AFBBC_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='BBC Athletics' then days_right_viewable else 0 end) 
as ATHBBC_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='BBC Boxing' then days_right_viewable else 0 end) 
as BOXBBC_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='BBC Darts' then days_right_viewable else 0 end) 
as DRTBBC_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='BBC Equestrian' then days_right_viewable else 0 end) 
as EQUBBC_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='BBC Football' then days_right_viewable else 0 end) 
as FOOTBBC_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='BBC Golf' then days_right_viewable else 0 end) 
as GOLFBBC_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='BBC Motor Sport' then days_right_viewable else 0 end) 
as MSPBBC_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='BBC Rugby' then days_right_viewable else 0 end) 
as RUGBBC_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='BBC Snooker/Pool' then days_right_viewable else 0 end) 
as SNPBBC_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='BBC Tennis' then days_right_viewable else 0 end) 
as TENBBC_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='BBC Unknown' then days_right_viewable else 0 end) 
as UNKBBC_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='BBC Watersports' then days_right_viewable else 0 end) 
as WATBBC_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='BBC Wintersports' then days_right_viewable else 0 end) 
as WINBBC_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Boxing  - Channel 5' then days_right_viewable else 0 end) 
as BOXCH5_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Boxing - Matchroom 2012-2014 Sky Sports' then days_right_viewable else 0 end) 
as BOXMSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Brazil Football - BT Sport' then days_right_viewable else 0 end) 
as BFTBTS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='British & Irish Lions 2013 - Sky Sports' then days_right_viewable else 0 end) 
as BILSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='British Open Golf - BBC' then days_right_viewable else 0 end) 
as BOGSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='BT Sport American Football' then days_right_viewable else 0 end) 
as AFBTS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Athletics' then days_right_viewable else 0 end) 
as ATHBTS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Baseball' then days_right_viewable else 0 end) 
as BASEBTS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Basketball' then days_right_viewable else 0 end) 
as BASKBTS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Boxing' then days_right_viewable else 0 end) 
as BOXBTS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Cricket' then days_right_viewable else 0 end) 
as CRIBTS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Equestrian' then days_right_viewable else 0 end) 
as EQUBTS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Extreme' then days_right_viewable else 0 end) 
as EXTBTS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Football' then days_right_viewable else 0 end) 
as FOOTBTS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Motor Sport' then days_right_viewable else 0 end) 
as MSPBTS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Rugby' then days_right_viewable else 0 end) 
as RUGBTS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Tennis' then days_right_viewable else 0 end) 
as TENBTS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Unknown' then days_right_viewable else 0 end) 
as UNKBTS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Wintersports' then days_right_viewable else 0 end) 
as WINBTS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Bundesliga - BT Sport' then days_right_viewable else 0 end) 
as BUNBTS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Bundesliga- ESPN' then days_right_viewable else 0 end) 
as BUNESPN_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Challenge Darts' then days_right_viewable else 0 end) 
as DRTCHA_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Challenge Extreme' then days_right_viewable else 0 end) 
as EXTCHA_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Challenge Unknown' then days_right_viewable else 0 end) 
as UNKCHA_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Challenge Wrestling' then days_right_viewable else 0 end) 
as WRECHA_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Champions League - ITV' then days_right_viewable else 0 end) 
as CHLITV_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Champions Trophy - I C C Cricket Deal  2012-2015 Sky Sports' then days_right_viewable else 0 end) 
as ICCSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 American Football' then days_right_viewable else 0 end) 
as AMCH4_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Athletics' then days_right_viewable else 0 end) 
as ATHCH4_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Boxing' then days_right_viewable else 0 end) 
as BOXCH4_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Equestrian' then days_right_viewable else 0 end) 
as EQUCH4_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Extreme' then days_right_viewable else 0 end) 
as EXTCH4_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Football' then days_right_viewable else 0 end) 
as FOOTCH4_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Motor Sport' then days_right_viewable else 0 end) 
as MSPCH4_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Racing' then days_right_viewable else 0 end) 
as RACCH4_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Unknown' then days_right_viewable else 0 end) 
as UNKCH4_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Watersports' then days_right_viewable else 0 end) 
as WATCH4_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Wintersports' then days_right_viewable else 0 end) 
as WINCH4_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Athletics' then days_right_viewable else 0 end) 
as ATHCH5_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Boxing' then days_right_viewable else 0 end) 
as BOXOCH5_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Cricket' then days_right_viewable else 0 end) 
as CRICH5_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Motor Sport' then days_right_viewable else 0 end) 
as MSPCH5_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Unknown' then days_right_viewable else 0 end) 
as UNKCH5_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Wrestling' then days_right_viewable else 0 end) 
as WRECH5_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Cheltenham Festival - Channel 4' then days_right_viewable else 0 end) 
as CHELCH4_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Community Shield - ITV' then days_right_viewable else 0 end) 
as CMSITV_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Confederations Cup - BBC' then days_right_viewable else 0 end) 
as CONCBBC_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Conference - BT Sport' then days_right_viewable else 0 end) 
as CONFBTS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Cycling - La Vuelta ITV' then days_right_viewable else 0 end) 
as CLVITV_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Cycling - U C I World Tour Sky Sports' then days_right_viewable else 0 end) 
as CUCISS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Cycling Tour of Britain - Eurosport' then days_right_viewable else 0 end) 
as CTBEUR_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Cycling: tour of britain ITV4' then days_right_viewable else 0 end) 
as CTCITV_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Derby - Channel 4' then days_right_viewable else 0 end) 
as DERCH4_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='ECB (highlights) - Channel 5' then days_right_viewable else 0 end) 
as ECBHCH5_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='ECB Cricket Sky Sports' then days_right_viewable else 0 end) 
as GECRSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='ECB non-Test Cricket Sky Sports' then days_right_viewable else 0 end) 
as ECBNSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='ECB Test Cricket Sky Sports' then days_right_viewable else 0 end) 
as ECBTSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='England Football Internationals - ITV' then days_right_viewable else 0 end) 
as GENGITV_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='England Friendlies (Football) - ITV' then days_right_viewable else 0 end) 
as EFRITV_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then days_right_viewable else 0 end) 
as ENRSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='England World Cup Qualifying (Away) - ITV' then days_right_viewable else 0 end) 
as EWQAITV_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='England World Cup Qualifying (Home) - ITV' then days_right_viewable else 0 end) 
as EWQHITV_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='ESPN American Football' then days_right_viewable else 0 end) 
as AMESPN_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='ESPN Athletics' then days_right_viewable else 0 end) 
as ATHESPN_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='ESPN Baseball' then days_right_viewable else 0 end) 
as BASEESPN_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='ESPN Basketball' then days_right_viewable else 0 end) 
as BASKESPN_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='ESPN Boxing' then days_right_viewable else 0 end) 
as BOXESPN_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='ESPN Cricket' then days_right_viewable else 0 end) 
as CRIESPN_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='ESPN Darts' then days_right_viewable else 0 end) 
as DARTESPN_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='ESPN Equestrian' then days_right_viewable else 0 end) 
as EQUESPN_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='ESPN Extreme' then days_right_viewable else 0 end) 
as EXTESPN_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='ESPN Football' then days_right_viewable else 0 end) 
as FOOTESPN_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='ESPN Golf' then days_right_viewable else 0 end) 
as GOLFESPN_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='ESPN Ice Hockey' then days_right_viewable else 0 end) 
as IHESPN_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='ESPN Motor Sport' then days_right_viewable else 0 end) 
as MSPESPN_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='ESPN Racing' then days_right_viewable else 0 end) 
as RACESPN_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='ESPN Rugby' then days_right_viewable else 0 end) 
as RUGESPN_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='ESPN Snooker/Pool' then days_right_viewable else 0 end) 
as SNPESPN_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='ESPN Tennis' then days_right_viewable else 0 end) 
as TENESPN_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='ESPN Unknown' then days_right_viewable else 0 end) 
as UNKESPN_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='ESPN Watersports' then days_right_viewable else 0 end) 
as WATESPN_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='ESPN Wintersports' then days_right_viewable else 0 end) 
as WINESPN_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='ESPN Wrestling' then days_right_viewable else 0 end) 
as WREESPN_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Europa League - BT Sport' then days_right_viewable else 0 end) 
as ELBTSP_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Europa League - ESPN' then days_right_viewable else 0 end) 
as ELESPN_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Europa League - ITV' then days_right_viewable else 0 end) 
as ELITV_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='European Tour Golf - Sky Sports' then days_right_viewable else 0 end) 
as ETGSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Eurosport American Football' then days_right_viewable else 0 end) 
as AMEUR_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Athletics' then days_right_viewable else 0 end) 
as ATHEUR_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Baseball' then days_right_viewable else 0 end) 
as BASEEUR_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Basketball' then days_right_viewable else 0 end) 
as BASKEUR_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Boxing' then days_right_viewable else 0 end) 
as BOXEUR_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Cricket' then days_right_viewable else 0 end) 
as CRIEUR_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Darts' then days_right_viewable else 0 end) 
as DARTEUR_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Equestrian' then days_right_viewable else 0 end) 
as EQUEUR_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Extreme' then days_right_viewable else 0 end) 
as EXTEUR_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Football' then days_right_viewable else 0 end) 
as FOOTEUR_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Golf' then days_right_viewable else 0 end) 
as GOLFEUR_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Ice Hockey' then days_right_viewable else 0 end) 
as IHEUR_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Motor Sport' then days_right_viewable else 0 end) 
as MSPEUR_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Racing' then days_right_viewable else 0 end) 
as RACEUR_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Snooker/Pool' then days_right_viewable else 0 end) 
as SNPEUR_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Tennis' then days_right_viewable else 0 end) 
as TENEUR_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Unknown' then days_right_viewable else 0 end) 
as UNKEUR_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Watersports' then days_right_viewable else 0 end) 
as WATEUR_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Wintersports' then days_right_viewable else 0 end) 
as WINEUR_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='F1 - BBC' then days_right_viewable else 0 end) 
as GF1BBC_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='F1 - Sky Sports' then days_right_viewable else 0 end) 
as GF1SS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='F1 (non-Live)- BBC' then days_right_viewable else 0 end) 
as F1NBBC_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='F1 (Practice Live)- BBC' then days_right_viewable else 0 end) 
as F1PBBC_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='F1 (Qualifying Live)- BBC' then days_right_viewable else 0 end) 
as F1QBBC_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='F1 (Race Live)- BBC' then days_right_viewable else 0 end) 
as F1RBBC_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='FA Cup - ESPN' then days_right_viewable else 0 end) 
as FACESPN_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='FA Cup - ITV' then days_right_viewable else 0 end) 
as FACITV_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then days_right_viewable else 0 end) 
as FLCCSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - others' then days_right_viewable else 0 end) 
as FLOTSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Formula One 2012-2018 - (non-Live) Sky Sports' then days_right_viewable else 0 end) 
as F1NSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Formula One 2012-2018 - (Practice Live) Sky Sports' then days_right_viewable else 0 end) 
as F1PSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Formula One 2012-2018 - (Qualifying Live) Sky Sports' then days_right_viewable else 0 end) 
as F1QSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Formula One 2012-2018 - (Race Live) Sky Sports' then days_right_viewable else 0 end) 
as F1RSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='French Open Tennis - Eurosport' then days_right_viewable else 0 end) 
as FOTEUR_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='French Open Tennis - ITV' then days_right_viewable else 0 end) 
as FOTITV_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Grand National - Channel 4' then days_right_viewable else 0 end) 
as GDNCH4_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Heineken Cup 2010/11 To 2013/14- Sky Sports' then days_right_viewable else 0 end) 
as HECSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='I R B Sevens 2008/09 -2014/15 - Sky Sports' then days_right_viewable else 0 end) 
as IRBSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='IAAF World Athletics Championship - Eurosport' then days_right_viewable else 0 end) 
as WACEUR_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='India Home Cricket 2012-2018 Sky Sports' then days_right_viewable else 0 end) 
as IHCSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='India Premier League - ITV' then days_right_viewable else 0 end) 
as IPLITV_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='International Freindlies - ESPN' then days_right_viewable else 0 end) 
as IFESPN_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='International Friendlies - BT Sport' then days_right_viewable else 0 end) 
as IFBTS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Boxing' then days_right_viewable else 0 end) 
as BOXITV1_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Football' then days_right_viewable else 0 end) 
as FOOTITV1_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Motor Sport' then days_right_viewable else 0 end) 
as MOTSITV1_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Rugby' then days_right_viewable else 0 end) 
as RUGITV1_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Snooker/Pool' then days_right_viewable else 0 end) 
as SNPITV1_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Unknown' then days_right_viewable else 0 end) 
as UNKITV1_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Boxing' then days_right_viewable else 0 end) 
as BOXITV4_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Cricket' then days_right_viewable else 0 end) 
as CRIITV4_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Darts' then days_right_viewable else 0 end) 
as DARTITV4_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Extreme' then days_right_viewable else 0 end) 
as EXTITV4_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Football' then days_right_viewable else 0 end) 
as FOOTITV4_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Motor Sport' then days_right_viewable else 0 end) 
as MSPITV4_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Rugby' then days_right_viewable else 0 end) 
as RUGITV4_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Snooker/Pool' then days_right_viewable else 0 end) 
as SNPITV4_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Tennis' then days_right_viewable else 0 end) 
as TENITV4_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Unknown' then days_right_viewable else 0 end) 
as UNKITV4_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Ligue 1 - BT Sport' then days_right_viewable else 0 end) 
as L1BTS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Ligue 1 - ESPN' then days_right_viewable else 0 end) 
as L1ESPN_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Match of the day - BBC' then days_right_viewable else 0 end) 
as MOTDBBC_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Matchroom Output Deal  2010-2012 (Other)- Sky Sports' then days_right_viewable else 0 end) 
as MROSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Matchroom Output Deal  2010-2012 (Pool)- Sky Sports' then days_right_viewable else 0 end) 
as MRPSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Matchroom Output Deal  2010-2012 (Snooker)- Sky Sports' then days_right_viewable else 0 end) 
as MRSSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Moto GP BBC' then days_right_viewable else 0 end) 
as MGPBBC_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='NBA - Sky Sports' then days_right_viewable else 0 end) 
as NBASS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='NFL - BBC' then days_right_viewable else 0 end) 
as NFLBBC_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='NFL - Channel 4' then days_right_viewable else 0 end) 
as NFLCH4_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then days_right_viewable else 0 end) 
as NFLSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Northern Ireland Football Association 2012-2014 - Sky Sports' then days_right_viewable else 0 end) 
as NIFSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Oaks - Channel 4' then days_right_viewable else 0 end) 
as OAKCH4_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Other American Football' then days_right_viewable else 0 end) 
as AMOTH_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Other Athletics' then days_right_viewable else 0 end) 
as ATHOTH_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Other Baseball' then days_right_viewable else 0 end) 
as BASEOTH_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Other Basketball' then days_right_viewable else 0 end) 
as BASKOTH_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Other Boxing' then days_right_viewable else 0 end) 
as BOXOTH_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Other Cricket' then days_right_viewable else 0 end) 
as CRIOTH_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Other Darts' then days_right_viewable else 0 end) 
as DARTOTH_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Other Equestrian' then days_right_viewable else 0 end) 
as EQUOTH_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Other Extreme' then days_right_viewable else 0 end) 
as EXTOTH_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Other Fishing' then days_right_viewable else 0 end) 
as FSHOTH_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Other Football' then days_right_viewable else 0 end) 
as FOOTOTH_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Other Golf' then days_right_viewable else 0 end) 
as GOLFOTH_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Other Ice Hockey' then days_right_viewable else 0 end) 
as IHOTH_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Other Motor Sport' then days_right_viewable else 0 end) 
as MSPOTH_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Other Racing' then days_right_viewable else 0 end) 
as RACOTH_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Other Rugby' then days_right_viewable else 0 end) 
as RUGOTH_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Other Rugby Internationals - ESPN' then days_right_viewable else 0 end) 
as ORUGESPN_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Other Snooker/Pool' then days_right_viewable else 0 end) 
as OTHSNP_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Other Tennis' then days_right_viewable else 0 end) 
as OTHTEN_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Other Unknown' then days_right_viewable else 0 end) 
as OTHUNK_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Other Watersports' then days_right_viewable else 0 end) 
as OTHWAT_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Other Wintersports' then days_right_viewable else 0 end) 
as OTHWIN_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Other Wrestling' then days_right_viewable else 0 end) 
as OTHWRE_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then days_right_viewable else 0 end) 
as PGASS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Premier League - BT Sport' then days_right_viewable else 0 end) 
as PLBTS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Premier League - ESPN' then days_right_viewable else 0 end) 
as PLESPN_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Premier League Darts 2010-2013 - Sky Sports' then days_right_viewable else 0 end) 
as PLDSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports' then days_right_viewable else 0 end) 
as GPLSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (Match Choice)' then days_right_viewable else 0 end) 
as PLMCSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (MNF)' then days_right_viewable else 0 end) 
as PLMNFSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (non Live)' then days_right_viewable else 0 end) 
as PLNLSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (other Live)' then days_right_viewable else 0 end) 
as PLOLSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (Sat Lunchtime)' then days_right_viewable else 0 end) 
as PLSLSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (Sat Night Live)' then days_right_viewable else 0 end) 
as PLSNSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (Sun 4pm)' then days_right_viewable else 0 end) 
as PLS4SS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (Sun Lunchtime)' then days_right_viewable else 0 end) 
as PLSULSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Premiership Rugby - Sky Sports' then days_right_viewable else 0 end) 
as PRUSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Republic Of Ireland FA - 2010/11 To 2013/14 - Sky Sports' then days_right_viewable else 0 end) 
as ROISS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Royal Ascot - Channel 4' then days_right_viewable else 0 end) 
as RASCH4_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Rugby Internationals (England) - BBC' then days_right_viewable else 0 end) 
as RIEBBC_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Rugby Internationals (Ireland) - BBC' then days_right_viewable else 0 end) 
as RIIBBC_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Rugby Internationals (Scotland) - BBC' then days_right_viewable else 0 end) 
as RISBBC_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Rugby Internationals (Wales) - BBC' then days_right_viewable else 0 end) 
as RIWBBC_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Rugby League  Challenge Cup- BBC' then days_right_viewable else 0 end) 
as RLCCBBC_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Rugby League - Sky Sports' then days_right_viewable else 0 end) 
as RLGSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Rugby League  World Cup- BBC' then days_right_viewable else 0 end) 
as RLWCBBC_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Sanzar Rugby Union 2011 - 2015 - Sky Sports' then days_right_viewable else 0 end) 
as SARUSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Scottish FA Deal - 2010/11 To 2013/14 - Sky Sports' then days_right_viewable else 0 end) 
as SFASS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Serie A - BT Sport' then days_right_viewable else 0 end) 
as SABTS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Serie A - ESPN' then days_right_viewable else 0 end) 
as SAESPN_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='SFL - ESPN' then days_right_viewable else 0 end) 
as SFLESPN_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Six Nations - BBC' then days_right_viewable else 0 end) 
as SNRBBC_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Sky 1 and Sky 2 Boxing' then days_right_viewable else 0 end) 
as BOXS12_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Sky 1 and Sky 2 Football' then days_right_viewable else 0 end) 
as FOOTS12_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Sky 1 and Sky 2 Motor Sport' then days_right_viewable else 0 end) 
as MSPS12_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Sky 1 and Sky 2 Unknown' then days_right_viewable else 0 end) 
as UNKS12_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Sky 1 and Sky 2 Wrestling' then days_right_viewable else 0 end) 
as WRES12_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports American Football' then days_right_viewable else 0 end) 
as AMSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Athletics' then days_right_viewable else 0 end) 
as ATHSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Baseball' then days_right_viewable else 0 end) 
as BASESS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Basketball' then days_right_viewable else 0 end) 
as BASKSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Boxing' then days_right_viewable else 0 end) 
as BOXSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Cricket' then days_right_viewable else 0 end) 
as CRISS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Darts' then days_right_viewable else 0 end) 
as DARTSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Equestrian' then days_right_viewable else 0 end) 
as EQUSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Extreme' then days_right_viewable else 0 end) 
as EXTSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Fishing' then days_right_viewable else 0 end) 
as FISHSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Football' then days_right_viewable else 0 end) 
as FOOTSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Golf' then days_right_viewable else 0 end) 
as GOLFSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Ice Hockey' then days_right_viewable else 0 end) 
as IHSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Motor Sport' then days_right_viewable else 0 end) 
as MSPSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Racing' then days_right_viewable else 0 end) 
as RACSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Rugby' then days_right_viewable else 0 end) 
as RUGSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Snooker/Pool' then days_right_viewable else 0 end) 
as SNPSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Tennis' then days_right_viewable else 0 end) 
as TENSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Unknown' then days_right_viewable else 0 end) 
as UNKSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Watersports' then days_right_viewable else 0 end) 
as WATSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Wintersports' then days_right_viewable else 0 end) 
as WINSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Wrestling' then days_right_viewable else 0 end) 
as WRESS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Solheim Cup 2013 Ladies Golf - Sky Sports' then days_right_viewable else 0 end) 
as SOLSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='South African Home Cricket  2012-2020 Sky Sports' then days_right_viewable else 0 end) 
as SACSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then days_right_viewable else 0 end) 
as SPFSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='SPFL - BT Sport' then days_right_viewable else 0 end) 
as SPFLBTS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='SPL - ESPN' then days_right_viewable else 0 end) 
as SPLESPN_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='SPL - Sky Sports' then days_right_viewable else 0 end) 
as SPLSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Sportfive World Cup 2014 Qualifers - Sky Sports' then days_right_viewable else 0 end) 
as SP5SS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='The boat race - BBC' then days_right_viewable else 0 end) 
as BTRBBC_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='The football league show - BBC' then days_right_viewable else 0 end) 
as FLSBBC_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='The Masters Golf - BBC' then days_right_viewable else 0 end) 
as MGBBC_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='TNA Wrestling Challenge' then days_right_viewable else 0 end) 
as TNACHA_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Tour de France - Eurosport' then days_right_viewable else 0 end) 
as TDFEUR_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Tour de France - ITV' then days_right_viewable else 0 end) 
as TDFITV_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then days_right_viewable else 0 end) 
as USMGSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then days_right_viewable else 0 end) 
as USOTSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='U.S Open/Womens/Seniors/Amateur Golf Sky Sports' then days_right_viewable else 0 end) 
as USOGSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='UEFA Champions League -  Sky Sports' then days_right_viewable else 0 end) 
as CLASS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='UEFA Champions League -  Sky Sports (non Live)' then days_right_viewable else 0 end) 
as CLNSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='UEFA Champions League -  Sky Sports (other Live)' then days_right_viewable else 0 end) 
as CLOSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='UEFA Champions League -  Sky Sports (Tue)' then days_right_viewable else 0 end) 
as CLTSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='UEFA Champions League -  Sky Sports (Wed)' then days_right_viewable else 0 end) 
as CLWSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='US Open Tennis - Eurosport' then days_right_viewable else 0 end) 
as USOTEUR_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='USA Football - BT Sport' then days_right_viewable else 0 end) 
as USFBTS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='USPGA Championship (2007-2016) Sky Sports' then days_right_viewable else 0 end) 
as USPGASS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='WCQ - ESPN' then days_right_viewable else 0 end) 
as WCQESPN_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Welsh Home International Football 2012-2014 Sky Sports' then days_right_viewable else 0 end) 
as WIFSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='West Indies Home Cricket 2013-2020 Sky Sports' then days_right_viewable else 0 end) 
as WICSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Wimbledon - BBC' then days_right_viewable else 0 end) 
as WIMBBC_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='Womens Cricket - I C C Cricket Deal  2012-2015 Sky Sports' then days_right_viewable else 0 end) 
as WICCSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='World Athletics Championship - More 4' then days_right_viewable else 0 end) 
as WACMR4_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='World Club Championship - BBC' then days_right_viewable else 0 end) 
as WCLBBBC_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='World Cup Qualifiers - BT Sport' then days_right_viewable else 0 end) 
as WCQBTS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='World Darts Championship 2009-2012 Sky Sports' then days_right_viewable else 0 end) 
as WDCSS_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='World snooker championship - BBC' then days_right_viewable else 0 end) 
as WSCBBC_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='WWE Sky 1 and 2' then days_right_viewable else 0 end) 
as WWES12_days_right_viewable_LIVE
,sum(case when live=1 and analysis_right ='WWE Sky Sports' then days_right_viewable else 0 end) 
as WWESS_days_right_viewable_LIVE
,sum(case when Live=0 and analysis_right ='Africa Cup of Nations - Eurosport' then days_right_viewable else 0 end) 
as AFCEUR_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Africa Cup of Nations - ITV' then days_right_viewable else 0 end) 
as AFCITV_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Americas Cup - BBC' then days_right_viewable else 0 end) 
as AMCBBC_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Asian Tour Golf 2012-2013 Sky Sports' then days_right_viewable else 0 end) 
as ATGSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='ATP Tennis Masters Series  2011-2013 Sky Sports' then days_right_viewable else 0 end) 
as ATPSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Australia Home Cricket  2012-2016 Sky Sports' then days_right_viewable else 0 end) 
as AHCSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Australian Football - BT Sport' then days_right_viewable else 0 end) 
as AUFBTS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Australian Open Tennis - BBC' then days_right_viewable else 0 end) 
as AOTBBC_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Australian Open Tennis - Eurosport' then days_right_viewable else 0 end) 
as AOTEUR_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Aviva Premiership - ESPN' then days_right_viewable else 0 end) 
as AVPSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='BBC American Football' then days_right_viewable else 0 end) 
as AFBBC_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='BBC Athletics' then days_right_viewable else 0 end) 
as ATHBBC_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='BBC Boxing' then days_right_viewable else 0 end) 
as BOXBBC_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='BBC Darts' then days_right_viewable else 0 end) 
as DRTBBC_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='BBC Equestrian' then days_right_viewable else 0 end) 
as EQUBBC_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='BBC Football' then days_right_viewable else 0 end) 
as FOOTBBC_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='BBC Golf' then days_right_viewable else 0 end) 
as GOLFBBC_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='BBC Motor Sport' then days_right_viewable else 0 end) 
as MSPBBC_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='BBC Rugby' then days_right_viewable else 0 end) 
as RUGBBC_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='BBC Snooker/Pool' then days_right_viewable else 0 end) 
as SNPBBC_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='BBC Tennis' then days_right_viewable else 0 end) 
as TENBBC_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='BBC Unknown' then days_right_viewable else 0 end) 
as UNKBBC_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='BBC Watersports' then days_right_viewable else 0 end) 
as WATBBC_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='BBC Wintersports' then days_right_viewable else 0 end) 
as WINBBC_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Boxing  - Channel 5' then days_right_viewable else 0 end) 
as BOXCH5_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Boxing - Matchroom 2012-2014 Sky Sports' then days_right_viewable else 0 end) 
as BOXMSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Brazil Football - BT Sport' then days_right_viewable else 0 end) 
as BFTBTS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='British & Irish Lions 2013 - Sky Sports' then days_right_viewable else 0 end) 
as BILSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='British Open Golf - BBC' then days_right_viewable else 0 end) 
as BOGSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport American Football' then days_right_viewable else 0 end) 
as AFBTS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Athletics' then days_right_viewable else 0 end) 
as ATHBTS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Baseball' then days_right_viewable else 0 end) 
as BASEBTS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Basketball' then days_right_viewable else 0 end) 
as BASKBTS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Boxing' then days_right_viewable else 0 end) 
as BOXBTS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Cricket' then days_right_viewable else 0 end) 
as CRIBTS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Equestrian' then days_right_viewable else 0 end) 
as EQUBTS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Extreme' then days_right_viewable else 0 end) 
as EXTBTS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Football' then days_right_viewable else 0 end) 
as FOOTBTS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Motor Sport' then days_right_viewable else 0 end) 
as MSPBTS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Rugby' then days_right_viewable else 0 end) 
as RUGBTS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Tennis' then days_right_viewable else 0 end) 
as TENBTS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Unknown' then days_right_viewable else 0 end) 
as UNKBTS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Wintersports' then days_right_viewable else 0 end) 
as WINBTS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Bundesliga - BT Sport' then days_right_viewable else 0 end) 
as BUNBTS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Bundesliga- ESPN' then days_right_viewable else 0 end) 
as BUNESPN_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Challenge Darts' then days_right_viewable else 0 end) 
as DRTCHA_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Challenge Extreme' then days_right_viewable else 0 end) 
as EXTCHA_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Challenge Unknown' then days_right_viewable else 0 end) 
as UNKCHA_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Challenge Wrestling' then days_right_viewable else 0 end) 
as WRECHA_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Champions League - ITV' then days_right_viewable else 0 end) 
as CHLITV_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Champions Trophy - I C C Cricket Deal  2012-2015 Sky Sports' then days_right_viewable else 0 end) 
as ICCSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 American Football' then days_right_viewable else 0 end) 
as AMCH4_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 Athletics' then days_right_viewable else 0 end) 
as ATHCH4_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 Boxing' then days_right_viewable else 0 end) 
as BOXCH4_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 Equestrian' then days_right_viewable else 0 end) 
as EQUCH4_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 Extreme' then days_right_viewable else 0 end) 
as EXTCH4_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 Football' then days_right_viewable else 0 end) 
as FOOTCH4_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 Motor Sport' then days_right_viewable else 0 end) 
as MSPCH4_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 Racing' then days_right_viewable else 0 end) 
as RACCH4_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 Unknown' then days_right_viewable else 0 end) 
as UNKCH4_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 Watersports' then days_right_viewable else 0 end) 
as WATCH4_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 Wintersports' then days_right_viewable else 0 end) 
as WINCH4_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Channel 5 Athletics' then days_right_viewable else 0 end) 
as ATHCH5_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Channel 5 Boxing' then days_right_viewable else 0 end) 
as BOXOCH5_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Channel 5 Cricket' then days_right_viewable else 0 end) 
as CRICH5_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Channel 5 Motor Sport' then days_right_viewable else 0 end) 
as MSPCH5_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Channel 5 Unknown' then days_right_viewable else 0 end) 
as UNKCH5_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Channel 5 Wrestling' then days_right_viewable else 0 end) 
as WRECH5_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Cheltenham Festival - Channel 4' then days_right_viewable else 0 end) 
as CHELCH4_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Community Shield - ITV' then days_right_viewable else 0 end) 
as CMSITV_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Confederations Cup - BBC' then days_right_viewable else 0 end) 
as CONCBBC_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Conference - BT Sport' then days_right_viewable else 0 end) 
as CONFBTS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Cycling - La Vuelta ITV' then days_right_viewable else 0 end) 
as CLVITV_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Cycling - U C I World Tour Sky Sports' then days_right_viewable else 0 end) 
as CUCISS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Cycling Tour of Britain - Eurosport' then days_right_viewable else 0 end) 
as CTBEUR_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Cycling: tour of britain ITV4' then days_right_viewable else 0 end) 
as CTCITV_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Derby - Channel 4' then days_right_viewable else 0 end) 
as DERCH4_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='ECB (highlights) - Channel 5' then days_right_viewable else 0 end) 
as ECBHCH5_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='ECB Cricket Sky Sports' then days_right_viewable else 0 end) 
as GECRSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='ECB non-Test Cricket Sky Sports' then days_right_viewable else 0 end) 
as ECBNSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='ECB Test Cricket Sky Sports' then days_right_viewable else 0 end) 
as ECBTSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='England Football Internationals - ITV' then days_right_viewable else 0 end) 
as GENGITV_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='England Friendlies (Football) - ITV' then days_right_viewable else 0 end) 
as EFRITV_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then days_right_viewable else 0 end) 
as ENRSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='England World Cup Qualifying (Away) - ITV' then days_right_viewable else 0 end) 
as EWQAITV_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='England World Cup Qualifying (Home) - ITV' then days_right_viewable else 0 end) 
as EWQHITV_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='ESPN American Football' then days_right_viewable else 0 end) 
as AMESPN_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Athletics' then days_right_viewable else 0 end) 
as ATHESPN_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Baseball' then days_right_viewable else 0 end) 
as BASEESPN_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Basketball' then days_right_viewable else 0 end) 
as BASKESPN_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Boxing' then days_right_viewable else 0 end) 
as BOXESPN_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Cricket' then days_right_viewable else 0 end) 
as CRIESPN_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Darts' then days_right_viewable else 0 end) 
as DARTESPN_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Equestrian' then days_right_viewable else 0 end) 
as EQUESPN_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Extreme' then days_right_viewable else 0 end) 
as EXTESPN_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Football' then days_right_viewable else 0 end) 
as FOOTESPN_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Golf' then days_right_viewable else 0 end) 
as GOLFESPN_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Ice Hockey' then days_right_viewable else 0 end) 
as IHESPN_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Motor Sport' then days_right_viewable else 0 end) 
as MSPESPN_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Racing' then days_right_viewable else 0 end) 
as RACESPN_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Rugby' then days_right_viewable else 0 end) 
as RUGESPN_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Snooker/Pool' then days_right_viewable else 0 end) 
as SNPESPN_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Tennis' then days_right_viewable else 0 end) 
as TENESPN_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Unknown' then days_right_viewable else 0 end) 
as UNKESPN_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Watersports' then days_right_viewable else 0 end) 
as WATESPN_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Wintersports' then days_right_viewable else 0 end) 
as WINESPN_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Wrestling' then days_right_viewable else 0 end) 
as WREESPN_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Europa League - BT Sport' then days_right_viewable else 0 end) 
as ELBTSP_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Europa League - ESPN' then days_right_viewable else 0 end) 
as ELESPN_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Europa League - ITV' then days_right_viewable else 0 end) 
as ELITV_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='European Tour Golf - Sky Sports' then days_right_viewable else 0 end) 
as ETGSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport American Football' then days_right_viewable else 0 end) 
as AMEUR_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Athletics' then days_right_viewable else 0 end) 
as ATHEUR_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Baseball' then days_right_viewable else 0 end) 
as BASEEUR_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Basketball' then days_right_viewable else 0 end) 
as BASKEUR_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Boxing' then days_right_viewable else 0 end) 
as BOXEUR_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Cricket' then days_right_viewable else 0 end) 
as CRIEUR_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Darts' then days_right_viewable else 0 end) 
as DARTEUR_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Equestrian' then days_right_viewable else 0 end) 
as EQUEUR_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Extreme' then days_right_viewable else 0 end) 
as EXTEUR_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Football' then days_right_viewable else 0 end) 
as FOOTEUR_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Golf' then days_right_viewable else 0 end) 
as GOLFEUR_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Ice Hockey' then days_right_viewable else 0 end) 
as IHEUR_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Motor Sport' then days_right_viewable else 0 end) 
as MSPEUR_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Racing' then days_right_viewable else 0 end) 
as RACEUR_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Snooker/Pool' then days_right_viewable else 0 end) 
as SNPEUR_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Tennis' then days_right_viewable else 0 end) 
as TENEUR_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Unknown' then days_right_viewable else 0 end) 
as UNKEUR_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Watersports' then days_right_viewable else 0 end) 
as WATEUR_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Wintersports' then days_right_viewable else 0 end) 
as WINEUR_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='F1 - BBC' then days_right_viewable else 0 end) 
as GF1BBC_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='F1 - Sky Sports' then days_right_viewable else 0 end) 
as GF1SS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='F1 (non-Live)- BBC' then days_right_viewable else 0 end) 
as F1NBBC_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='F1 (Practice Live)- BBC' then days_right_viewable else 0 end) 
as F1PBBC_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='F1 (Qualifying Live)- BBC' then days_right_viewable else 0 end) 
as F1QBBC_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='F1 (Race Live)- BBC' then days_right_viewable else 0 end) 
as F1RBBC_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='FA Cup - ESPN' then days_right_viewable else 0 end) 
as FACESPN_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='FA Cup - ITV' then days_right_viewable else 0 end) 
as FACITV_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then days_right_viewable else 0 end) 
as FLCCSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - others' then days_right_viewable else 0 end) 
as FLOTSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Formula One 2012-2018 - (non-Live) Sky Sports' then days_right_viewable else 0 end) 
as F1NSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Formula One 2012-2018 - (Practice Live) Sky Sports' then days_right_viewable else 0 end) 
as F1PSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Formula One 2012-2018 - (Qualifying Live) Sky Sports' then days_right_viewable else 0 end) 
as F1QSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Formula One 2012-2018 - (Race Live) Sky Sports' then days_right_viewable else 0 end) 
as F1RSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='French Open Tennis - Eurosport' then days_right_viewable else 0 end) 
as FOTEUR_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='French Open Tennis - ITV' then days_right_viewable else 0 end) 
as FOTITV_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Grand National - Channel 4' then days_right_viewable else 0 end) 
as GDNCH4_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Heineken Cup 2010/11 To 2013/14- Sky Sports' then days_right_viewable else 0 end) 
as HECSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='I R B Sevens 2008/09 -2014/15 - Sky Sports' then days_right_viewable else 0 end) 
as IRBSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='IAAF World Athletics Championship - Eurosport' then days_right_viewable else 0 end) 
as WACEUR_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='India Home Cricket 2012-2018 Sky Sports' then days_right_viewable else 0 end) 
as IHCSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='India Premier League - ITV' then days_right_viewable else 0 end) 
as IPLITV_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='International Freindlies - ESPN' then days_right_viewable else 0 end) 
as IFESPN_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='International Friendlies - BT Sport' then days_right_viewable else 0 end) 
as IFBTS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='ITV1 Boxing' then days_right_viewable else 0 end) 
as BOXITV1_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='ITV1 Football' then days_right_viewable else 0 end) 
as FOOTITV1_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='ITV1 Motor Sport' then days_right_viewable else 0 end) 
as MOTSITV1_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='ITV1 Rugby' then days_right_viewable else 0 end) 
as RUGITV1_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='ITV1 Snooker/Pool' then days_right_viewable else 0 end) 
as SNPITV1_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='ITV1 Unknown' then days_right_viewable else 0 end) 
as UNKITV1_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='ITV4 Boxing' then days_right_viewable else 0 end) 
as BOXITV4_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='ITV4 Cricket' then days_right_viewable else 0 end) 
as CRIITV4_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='ITV4 Darts' then days_right_viewable else 0 end) 
as DARTITV4_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='ITV4 Extreme' then days_right_viewable else 0 end) 
as EXTITV4_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='ITV4 Football' then days_right_viewable else 0 end) 
as FOOTITV4_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='ITV4 Motor Sport' then days_right_viewable else 0 end) 
as MSPITV4_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='ITV4 Rugby' then days_right_viewable else 0 end) 
as RUGITV4_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='ITV4 Snooker/Pool' then days_right_viewable else 0 end) 
as SNPITV4_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='ITV4 Tennis' then days_right_viewable else 0 end) 
as TENITV4_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='ITV4 Unknown' then days_right_viewable else 0 end) 
as UNKITV4_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Ligue 1 - BT Sport' then days_right_viewable else 0 end) 
as L1BTS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Ligue 1 - ESPN' then days_right_viewable else 0 end) 
as L1ESPN_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Match of the day - BBC' then days_right_viewable else 0 end) 
as MOTDBBC_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Matchroom Output Deal  2010-2012 (Other)- Sky Sports' then days_right_viewable else 0 end) 
as MROSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Matchroom Output Deal  2010-2012 (Pool)- Sky Sports' then days_right_viewable else 0 end) 
as MRPSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Matchroom Output Deal  2010-2012 (Snooker)- Sky Sports' then days_right_viewable else 0 end) 
as MRSSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Moto GP BBC' then days_right_viewable else 0 end) 
as MGPBBC_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='NBA - Sky Sports' then days_right_viewable else 0 end) 
as NBASS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='NFL - BBC' then days_right_viewable else 0 end) 
as NFLBBC_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='NFL - Channel 4' then days_right_viewable else 0 end) 
as NFLCH4_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then days_right_viewable else 0 end) 
as NFLSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Northern Ireland Football Association 2012-2014 - Sky Sports' then days_right_viewable else 0 end) 
as NIFSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Oaks - Channel 4' then days_right_viewable else 0 end) 
as OAKCH4_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Other American Football' then days_right_viewable else 0 end) 
as AMOTH_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Other Athletics' then days_right_viewable else 0 end) 
as ATHOTH_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Other Baseball' then days_right_viewable else 0 end) 
as BASEOTH_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Other Basketball' then days_right_viewable else 0 end) 
as BASKOTH_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Other Boxing' then days_right_viewable else 0 end) 
as BOXOTH_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Other Cricket' then days_right_viewable else 0 end) 
as CRIOTH_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Other Darts' then days_right_viewable else 0 end) 
as DARTOTH_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Other Equestrian' then days_right_viewable else 0 end) 
as EQUOTH_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Other Extreme' then days_right_viewable else 0 end) 
as EXTOTH_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Other Fishing' then days_right_viewable else 0 end) 
as FSHOTH_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Other Football' then days_right_viewable else 0 end) 
as FOOTOTH_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Other Golf' then days_right_viewable else 0 end) 
as GOLFOTH_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Other Ice Hockey' then days_right_viewable else 0 end) 
as IHOTH_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Other Motor Sport' then days_right_viewable else 0 end) 
as MSPOTH_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Other Racing' then days_right_viewable else 0 end) 
as RACOTH_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Other Rugby' then days_right_viewable else 0 end) 
as RUGOTH_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Other Rugby Internationals - ESPN' then days_right_viewable else 0 end) 
as ORUGESPN_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Other Snooker/Pool' then days_right_viewable else 0 end) 
as OTHSNP_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Other Tennis' then days_right_viewable else 0 end) 
as OTHTEN_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Other Unknown' then days_right_viewable else 0 end) 
as OTHUNK_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Other Watersports' then days_right_viewable else 0 end) 
as OTHWAT_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Other Wintersports' then days_right_viewable else 0 end) 
as OTHWIN_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Other Wrestling' then days_right_viewable else 0 end) 
as OTHWRE_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then days_right_viewable else 0 end) 
as PGASS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Premier League - BT Sport' then days_right_viewable else 0 end) 
as PLBTS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Premier League - ESPN' then days_right_viewable else 0 end) 
as PLESPN_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Premier League Darts 2010-2013 - Sky Sports' then days_right_viewable else 0 end) 
as PLDSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Premier League Football - Sky Sports' then days_right_viewable else 0 end) 
as GPLSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Premier League Football - Sky Sports (Match Choice)' then days_right_viewable else 0 end) 
as PLMCSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Premier League Football - Sky Sports (MNF)' then days_right_viewable else 0 end) 
as PLMNFSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Premier League Football - Sky Sports (non Live)' then days_right_viewable else 0 end) 
as PLNLSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Premier League Football - Sky Sports (other Live)' then days_right_viewable else 0 end) 
as PLOLSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Premier League Football - Sky Sports (Sat Lunchtime)' then days_right_viewable else 0 end) 
as PLSLSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Premier League Football - Sky Sports (Sat Night Live)' then days_right_viewable else 0 end) 
as PLSNSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Premier League Football - Sky Sports (Sun 4pm)' then days_right_viewable else 0 end) 
as PLS4SS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Premier League Football - Sky Sports (Sun Lunchtime)' then days_right_viewable else 0 end) 
as PLSULSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Premiership Rugby - Sky Sports' then days_right_viewable else 0 end) 
as PRUSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Republic Of Ireland FA - 2010/11 To 2013/14 - Sky Sports' then days_right_viewable else 0 end) 
as ROISS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Royal Ascot - Channel 4' then days_right_viewable else 0 end) 
as RASCH4_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Rugby Internationals (England) - BBC' then days_right_viewable else 0 end) 
as RIEBBC_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Rugby Internationals (Ireland) - BBC' then days_right_viewable else 0 end) 
as RIIBBC_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Rugby Internationals (Scotland) - BBC' then days_right_viewable else 0 end) 
as RISBBC_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Rugby Internationals (Wales) - BBC' then days_right_viewable else 0 end) 
as RIWBBC_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Rugby League  Challenge Cup- BBC' then days_right_viewable else 0 end) 
as RLCCBBC_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Rugby League - Sky Sports' then days_right_viewable else 0 end) 
as RLGSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Rugby League  World Cup- BBC' then days_right_viewable else 0 end) 
as RLWCBBC_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Sanzar Rugby Union 2011 - 2015 - Sky Sports' then days_right_viewable else 0 end) 
as SARUSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Scottish FA Deal - 2010/11 To 2013/14 - Sky Sports' then days_right_viewable else 0 end) 
as SFASS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Serie A - BT Sport' then days_right_viewable else 0 end) 
as SABTS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Serie A - ESPN' then days_right_viewable else 0 end) 
as SAESPN_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='SFL - ESPN' then days_right_viewable else 0 end) 
as SFLESPN_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Six Nations - BBC' then days_right_viewable else 0 end) 
as SNRBBC_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Sky 1 and Sky 2 Boxing' then days_right_viewable else 0 end) 
as BOXS12_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Sky 1 and Sky 2 Football' then days_right_viewable else 0 end) 
as FOOTS12_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Sky 1 and Sky 2 Motor Sport' then days_right_viewable else 0 end) 
as MSPS12_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Sky 1 and Sky 2 Unknown' then days_right_viewable else 0 end) 
as UNKS12_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Sky 1 and Sky 2 Wrestling' then days_right_viewable else 0 end) 
as WRES12_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports American Football' then days_right_viewable else 0 end) 
as AMSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Athletics' then days_right_viewable else 0 end) 
as ATHSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Baseball' then days_right_viewable else 0 end) 
as BASESS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Basketball' then days_right_viewable else 0 end) 
as BASKSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Boxing' then days_right_viewable else 0 end) 
as BOXSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Cricket' then days_right_viewable else 0 end) 
as CRISS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Darts' then days_right_viewable else 0 end) 
as DARTSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Equestrian' then days_right_viewable else 0 end) 
as EQUSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Extreme' then days_right_viewable else 0 end) 
as EXTSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Fishing' then days_right_viewable else 0 end) 
as FISHSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Football' then days_right_viewable else 0 end) 
as FOOTSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Golf' then days_right_viewable else 0 end) 
as GOLFSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Ice Hockey' then days_right_viewable else 0 end) 
as IHSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Motor Sport' then days_right_viewable else 0 end) 
as MSPSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Racing' then days_right_viewable else 0 end) 
as RACSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Rugby' then days_right_viewable else 0 end) 
as RUGSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Snooker/Pool' then days_right_viewable else 0 end) 
as SNPSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Tennis' then days_right_viewable else 0 end) 
as TENSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Unknown' then days_right_viewable else 0 end) 
as UNKSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Watersports' then days_right_viewable else 0 end) 
as WATSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Wintersports' then days_right_viewable else 0 end) 
as WINSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Wrestling' then days_right_viewable else 0 end) 
as WRESS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Solheim Cup 2013 Ladies Golf - Sky Sports' then days_right_viewable else 0 end) 
as SOLSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='South African Home Cricket  2012-2020 Sky Sports' then days_right_viewable else 0 end) 
as SACSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then days_right_viewable else 0 end) 
as SPFSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='SPFL - BT Sport' then days_right_viewable else 0 end) 
as SPFLBTS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='SPL - ESPN' then days_right_viewable else 0 end) 
as SPLESPN_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='SPL - Sky Sports' then days_right_viewable else 0 end) 
as SPLSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Sportfive World Cup 2014 Qualifers - Sky Sports' then days_right_viewable else 0 end) 
as SP5SS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='The boat race - BBC' then days_right_viewable else 0 end) 
as BTRBBC_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='The football league show - BBC' then days_right_viewable else 0 end) 
as FLSBBC_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='The Masters Golf - BBC' then days_right_viewable else 0 end) 
as MGBBC_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='TNA Wrestling Challenge' then days_right_viewable else 0 end) 
as TNACHA_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Tour de France - Eurosport' then days_right_viewable else 0 end) 
as TDFEUR_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Tour de France - ITV' then days_right_viewable else 0 end) 
as TDFITV_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then days_right_viewable else 0 end) 
as USMGSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then days_right_viewable else 0 end) 
as USOTSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='U.S Open/Womens/Seniors/Amateur Golf Sky Sports' then days_right_viewable else 0 end) 
as USOGSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='UEFA Champions League -  Sky Sports' then days_right_viewable else 0 end) 
as CLASS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='UEFA Champions League -  Sky Sports (non Live)' then days_right_viewable else 0 end) 
as CLNSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='UEFA Champions League -  Sky Sports (other Live)' then days_right_viewable else 0 end) 
as CLOSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='UEFA Champions League -  Sky Sports (Tue)' then days_right_viewable else 0 end) 
as CLTSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='UEFA Champions League -  Sky Sports (Wed)' then days_right_viewable else 0 end) 
as CLWSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='US Open Tennis - Eurosport' then days_right_viewable else 0 end) 
as USOTEUR_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='USA Football - BT Sport' then days_right_viewable else 0 end) 
as USFBTS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='USPGA Championship (2007-2016) Sky Sports' then days_right_viewable else 0 end) 
as USPGASS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='WCQ - ESPN' then days_right_viewable else 0 end) 
as WCQESPN_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Welsh Home International Football 2012-2014 Sky Sports' then days_right_viewable else 0 end) 
as WIFSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='West Indies Home Cricket 2013-2020 Sky Sports' then days_right_viewable else 0 end) 
as WICSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Wimbledon - BBC' then days_right_viewable else 0 end) 
as WIMBBC_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='Womens Cricket - I C C Cricket Deal  2012-2015 Sky Sports' then days_right_viewable else 0 end) 
as WICCSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='World Athletics Championship - More 4' then days_right_viewable else 0 end) 
as WACMR4_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='World Club Championship - BBC' then days_right_viewable else 0 end) 
as WCLBBBC_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='World Cup Qualifiers - BT Sport' then days_right_viewable else 0 end) 
as WCQBTS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='World Darts Championship 2009-2012 Sky Sports' then days_right_viewable else 0 end) 
as WDCSS_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='World snooker championship - BBC' then days_right_viewable else 0 end) 
as WSCBBC_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='WWE Sky 1 and 2' then days_right_viewable else 0 end) 
as WWES12_days_right_viewableNon_Live
,sum(case when Live=0 and analysis_right ='WWE Sky Sports' then days_right_viewable else 0 end) 
as WWESS_days_right_viewableNon_Live
,sum(case when live=1 and analysis_right ='Africa Cup of Nations - Eurosport' then days_right_broadcast else 0 end) 
as AFCEUR_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Africa Cup of Nations - ITV' then days_right_broadcast else 0 end) 
as AFCITV_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Americas Cup - BBC' then days_right_broadcast else 0 end) 
as AMCBBC_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Asian Tour Golf 2012-2013 Sky Sports' then days_right_broadcast else 0 end) 
as ATGSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='ATP Tennis Masters Series  2011-2013 Sky Sports' then days_right_broadcast else 0 end) 
as ATPSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Australia Home Cricket  2012-2016 Sky Sports' then days_right_broadcast else 0 end) 
as AHCSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Australian Football - BT Sport' then days_right_broadcast else 0 end) 
as AUFBTS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Australian Open Tennis - BBC' then days_right_broadcast else 0 end) 
as AOTBBC_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Australian Open Tennis - Eurosport' then days_right_broadcast else 0 end) 
as AOTEUR_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Aviva Premiership - ESPN' then days_right_broadcast else 0 end) 
as AVPSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='BBC American Football' then days_right_broadcast else 0 end) 
as AFBBC_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='BBC Athletics' then days_right_broadcast else 0 end) 
as ATHBBC_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='BBC Boxing' then days_right_broadcast else 0 end) 
as BOXBBC_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='BBC Darts' then days_right_broadcast else 0 end) 
as DRTBBC_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='BBC Equestrian' then days_right_broadcast else 0 end) 
as EQUBBC_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='BBC Football' then days_right_broadcast else 0 end) 
as FOOTBBC_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='BBC Golf' then days_right_broadcast else 0 end) 
as GOLFBBC_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='BBC Motor Sport' then days_right_broadcast else 0 end) 
as MSPBBC_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='BBC Rugby' then days_right_broadcast else 0 end) 
as RUGBBC_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='BBC Snooker/Pool' then days_right_broadcast else 0 end) 
as SNPBBC_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='BBC Tennis' then days_right_broadcast else 0 end) 
as TENBBC_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='BBC Unknown' then days_right_broadcast else 0 end) 
as UNKBBC_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='BBC Watersports' then days_right_broadcast else 0 end) 
as WATBBC_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='BBC Wintersports' then days_right_broadcast else 0 end) 
as WINBBC_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Boxing  - Channel 5' then days_right_broadcast else 0 end) 
as BOXCH5_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Boxing - Matchroom 2012-2014 Sky Sports' then days_right_broadcast else 0 end) 
as BOXMSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Brazil Football - BT Sport' then days_right_broadcast else 0 end) 
as BFTBTS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='British & Irish Lions 2013 - Sky Sports' then days_right_broadcast else 0 end) 
as BILSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='British Open Golf - BBC' then days_right_broadcast else 0 end) 
as BOGSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='BT Sport American Football' then days_right_broadcast else 0 end) 
as AFBTS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Athletics' then days_right_broadcast else 0 end) 
as ATHBTS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Baseball' then days_right_broadcast else 0 end) 
as BASEBTS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Basketball' then days_right_broadcast else 0 end) 
as BASKBTS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Boxing' then days_right_broadcast else 0 end) 
as BOXBTS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Cricket' then days_right_broadcast else 0 end) 
as CRIBTS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Equestrian' then days_right_broadcast else 0 end) 
as EQUBTS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Extreme' then days_right_broadcast else 0 end) 
as EXTBTS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Football' then days_right_broadcast else 0 end) 
as FOOTBTS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Motor Sport' then days_right_broadcast else 0 end) 
as MSPBTS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Rugby' then days_right_broadcast else 0 end) 
as RUGBTS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Tennis' then days_right_broadcast else 0 end) 
as TENBTS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Unknown' then days_right_broadcast else 0 end) 
as UNKBTS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Wintersports' then days_right_broadcast else 0 end) 
as WINBTS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Bundesliga - BT Sport' then days_right_broadcast else 0 end) 
as BUNBTS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Bundesliga- ESPN' then days_right_broadcast else 0 end) 
as BUNESPN_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Challenge Darts' then days_right_broadcast else 0 end) 
as DRTCHA_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Challenge Extreme' then days_right_broadcast else 0 end) 
as EXTCHA_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Challenge Unknown' then days_right_broadcast else 0 end) 
as UNKCHA_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Challenge Wrestling' then days_right_broadcast else 0 end) 
as WRECHA_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Champions League - ITV' then days_right_broadcast else 0 end) 
as CHLITV_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Champions Trophy - I C C Cricket Deal  2012-2015 Sky Sports' then days_right_broadcast else 0 end) 
as ICCSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 American Football' then days_right_broadcast else 0 end) 
as AMCH4_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Athletics' then days_right_broadcast else 0 end) 
as ATHCH4_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Boxing' then days_right_broadcast else 0 end) 
as BOXCH4_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Equestrian' then days_right_broadcast else 0 end) 
as EQUCH4_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Extreme' then days_right_broadcast else 0 end) 
as EXTCH4_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Football' then days_right_broadcast else 0 end) 
as FOOTCH4_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Motor Sport' then days_right_broadcast else 0 end) 
as MSPCH4_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Racing' then days_right_broadcast else 0 end) 
as RACCH4_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Unknown' then days_right_broadcast else 0 end) 
as UNKCH4_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Watersports' then days_right_broadcast else 0 end) 
as WATCH4_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Wintersports' then days_right_broadcast else 0 end) 
as WINCH4_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Athletics' then days_right_broadcast else 0 end) 
as ATHCH5_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Boxing' then days_right_broadcast else 0 end) 
as BOXOCH5_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Cricket' then days_right_broadcast else 0 end) 
as CRICH5_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Motor Sport' then days_right_broadcast else 0 end) 
as MSPCH5_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Unknown' then days_right_broadcast else 0 end) 
as UNKCH5_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Wrestling' then days_right_broadcast else 0 end) 
as WRECH5_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Cheltenham Festival - Channel 4' then days_right_broadcast else 0 end) 
as CHELCH4_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Community Shield - ITV' then days_right_broadcast else 0 end) 
as CMSITV_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Confederations Cup - BBC' then days_right_broadcast else 0 end) 
as CONCBBC_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Conference - BT Sport' then days_right_broadcast else 0 end) 
as CONFBTS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Cycling - La Vuelta ITV' then days_right_broadcast else 0 end) 
as CLVITV_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Cycling - U C I World Tour Sky Sports' then days_right_broadcast else 0 end) 
as CUCISS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Cycling Tour of Britain - Eurosport' then days_right_broadcast else 0 end) 
as CTBEUR_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Cycling: tour of britain ITV4' then days_right_broadcast else 0 end) 
as CTCITV_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Derby - Channel 4' then days_right_broadcast else 0 end) 
as DERCH4_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='ECB (highlights) - Channel 5' then days_right_broadcast else 0 end) 
as ECBHCH5_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='ECB Cricket Sky Sports' then days_right_broadcast else 0 end) 
as GECRSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='ECB non-Test Cricket Sky Sports' then days_right_broadcast else 0 end) 
as ECBNSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='ECB Test Cricket Sky Sports' then days_right_broadcast else 0 end) 
as ECBTSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='England Football Internationals - ITV' then days_right_broadcast else 0 end) 
as GENGITV_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='England Friendlies (Football) - ITV' then days_right_broadcast else 0 end) 
as EFRITV_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then days_right_broadcast else 0 end) 
as ENRSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='England World Cup Qualifying (Away) - ITV' then days_right_broadcast else 0 end) 
as EWQAITV_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='England World Cup Qualifying (Home) - ITV' then days_right_broadcast else 0 end) 
as EWQHITV_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='ESPN American Football' then days_right_broadcast else 0 end) 
as AMESPN_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='ESPN Athletics' then days_right_broadcast else 0 end) 
as ATHESPN_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='ESPN Baseball' then days_right_broadcast else 0 end) 
as BASEESPN_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='ESPN Basketball' then days_right_broadcast else 0 end) 
as BASKESPN_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='ESPN Boxing' then days_right_broadcast else 0 end) 
as BOXESPN_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='ESPN Cricket' then days_right_broadcast else 0 end) 
as CRIESPN_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='ESPN Darts' then days_right_broadcast else 0 end) 
as DARTESPN_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='ESPN Equestrian' then days_right_broadcast else 0 end) 
as EQUESPN_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='ESPN Extreme' then days_right_broadcast else 0 end) 
as EXTESPN_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='ESPN Football' then days_right_broadcast else 0 end) 
as FOOTESPN_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='ESPN Golf' then days_right_broadcast else 0 end) 
as GOLFESPN_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='ESPN Ice Hockey' then days_right_broadcast else 0 end) 
as IHESPN_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='ESPN Motor Sport' then days_right_broadcast else 0 end) 
as MSPESPN_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='ESPN Racing' then days_right_broadcast else 0 end) 
as RACESPN_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='ESPN Rugby' then days_right_broadcast else 0 end) 
as RUGESPN_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='ESPN Snooker/Pool' then days_right_broadcast else 0 end) 
as SNPESPN_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='ESPN Tennis' then days_right_broadcast else 0 end) 
as TENESPN_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='ESPN Unknown' then days_right_broadcast else 0 end) 
as UNKESPN_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='ESPN Watersports' then days_right_broadcast else 0 end) 
as WATESPN_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='ESPN Wintersports' then days_right_broadcast else 0 end) 
as WINESPN_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='ESPN Wrestling' then days_right_broadcast else 0 end) 
as WREESPN_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Europa League - BT Sport' then days_right_broadcast else 0 end) 
as ELBTSP_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Europa League - ESPN' then days_right_broadcast else 0 end) 
as ELESPN_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Europa League - ITV' then days_right_broadcast else 0 end) 
as ELITV_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='European Tour Golf - Sky Sports' then days_right_broadcast else 0 end) 
as ETGSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Eurosport American Football' then days_right_broadcast else 0 end) 
as AMEUR_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Athletics' then days_right_broadcast else 0 end) 
as ATHEUR_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Baseball' then days_right_broadcast else 0 end) 
as BASEEUR_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Basketball' then days_right_broadcast else 0 end) 
as BASKEUR_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Boxing' then days_right_broadcast else 0 end) 
as BOXEUR_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Cricket' then days_right_broadcast else 0 end) 
as CRIEUR_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Darts' then days_right_broadcast else 0 end) 
as DARTEUR_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Equestrian' then days_right_broadcast else 0 end) 
as EQUEUR_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Extreme' then days_right_broadcast else 0 end) 
as EXTEUR_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Football' then days_right_broadcast else 0 end) 
as FOOTEUR_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Golf' then days_right_broadcast else 0 end) 
as GOLFEUR_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Ice Hockey' then days_right_broadcast else 0 end) 
as IHEUR_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Motor Sport' then days_right_broadcast else 0 end) 
as MSPEUR_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Racing' then days_right_broadcast else 0 end) 
as RACEUR_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Snooker/Pool' then days_right_broadcast else 0 end) 
as SNPEUR_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Tennis' then days_right_broadcast else 0 end) 
as TENEUR_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Unknown' then days_right_broadcast else 0 end) 
as UNKEUR_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Watersports' then days_right_broadcast else 0 end) 
as WATEUR_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Wintersports' then days_right_broadcast else 0 end) 
as WINEUR_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='F1 - BBC' then days_right_broadcast else 0 end) 
as GF1BBC_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='F1 - Sky Sports' then days_right_broadcast else 0 end) 
as GF1SS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='F1 (non-Live)- BBC' then days_right_broadcast else 0 end) 
as F1NBBC_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='F1 (Practice Live)- BBC' then days_right_broadcast else 0 end) 
as F1PBBC_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='F1 (Qualifying Live)- BBC' then days_right_broadcast else 0 end) 
as F1QBBC_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='F1 (Race Live)- BBC' then days_right_broadcast else 0 end) 
as F1RBBC_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='FA Cup - ESPN' then days_right_broadcast else 0 end) 
as FACESPN_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='FA Cup - ITV' then days_right_broadcast else 0 end) 
as FACITV_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then days_right_broadcast else 0 end) 
as FLCCSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - others' then days_right_broadcast else 0 end) 
as FLOTSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Formula One 2012-2018 - (non-Live) Sky Sports' then days_right_broadcast else 0 end) 
as F1NSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Formula One 2012-2018 - (Practice Live) Sky Sports' then days_right_broadcast else 0 end) 
as F1PSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Formula One 2012-2018 - (Qualifying Live) Sky Sports' then days_right_broadcast else 0 end) 
as F1QSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Formula One 2012-2018 - (Race Live) Sky Sports' then days_right_broadcast else 0 end) 
as F1RSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='French Open Tennis - Eurosport' then days_right_broadcast else 0 end) 
as FOTEUR_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='French Open Tennis - ITV' then days_right_broadcast else 0 end) 
as FOTITV_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Grand National - Channel 4' then days_right_broadcast else 0 end) 
as GDNCH4_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Heineken Cup 2010/11 To 2013/14- Sky Sports' then days_right_broadcast else 0 end) 
as HECSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='I R B Sevens 2008/09 -2014/15 - Sky Sports' then days_right_broadcast else 0 end) 
as IRBSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='IAAF World Athletics Championship - Eurosport' then days_right_broadcast else 0 end) 
as WACEUR_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='India Home Cricket 2012-2018 Sky Sports' then days_right_broadcast else 0 end) 
as IHCSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='India Premier League - ITV' then days_right_broadcast else 0 end) 
as IPLITV_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='International Freindlies - ESPN' then days_right_broadcast else 0 end) 
as IFESPN_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='International Friendlies - BT Sport' then days_right_broadcast else 0 end) 
as IFBTS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Boxing' then days_right_broadcast else 0 end) 
as BOXITV1_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Football' then days_right_broadcast else 0 end) 
as FOOTITV1_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Motor Sport' then days_right_broadcast else 0 end) 
as MOTSITV1_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Rugby' then days_right_broadcast else 0 end) 
as RUGITV1_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Snooker/Pool' then days_right_broadcast else 0 end) 
as SNPITV1_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Unknown' then days_right_broadcast else 0 end) 
as UNKITV1_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Boxing' then days_right_broadcast else 0 end) 
as BOXITV4_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Cricket' then days_right_broadcast else 0 end) 
as CRIITV4_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Darts' then days_right_broadcast else 0 end) 
as DARTITV4_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Extreme' then days_right_broadcast else 0 end) 
as EXTITV4_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Football' then days_right_broadcast else 0 end) 
as FOOTITV4_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Motor Sport' then days_right_broadcast else 0 end) 
as MSPITV4_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Rugby' then days_right_broadcast else 0 end) 
as RUGITV4_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Snooker/Pool' then days_right_broadcast else 0 end) 
as SNPITV4_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Tennis' then days_right_broadcast else 0 end) 
as TENITV4_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Unknown' then days_right_broadcast else 0 end) 
as UNKITV4_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Ligue 1 - BT Sport' then days_right_broadcast else 0 end) 
as L1BTS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Ligue 1 - ESPN' then days_right_broadcast else 0 end) 
as L1ESPN_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Match of the day - BBC' then days_right_broadcast else 0 end) 
as MOTDBBC_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Matchroom Output Deal  2010-2012 (Other)- Sky Sports' then days_right_broadcast else 0 end) 
as MROSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Matchroom Output Deal  2010-2012 (Pool)- Sky Sports' then days_right_broadcast else 0 end) 
as MRPSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Matchroom Output Deal  2010-2012 (Snooker)- Sky Sports' then days_right_broadcast else 0 end) 
as MRSSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Moto GP BBC' then days_right_broadcast else 0 end) 
as MGPBBC_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='NBA - Sky Sports' then days_right_broadcast else 0 end) 
as NBASS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='NFL - BBC' then days_right_broadcast else 0 end) 
as NFLBBC_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='NFL - Channel 4' then days_right_broadcast else 0 end) 
as NFLCH4_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then days_right_broadcast else 0 end) 
as NFLSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Northern Ireland Football Association 2012-2014 - Sky Sports' then days_right_broadcast else 0 end) 
as NIFSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Oaks - Channel 4' then days_right_broadcast else 0 end) 
as OAKCH4_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Other American Football' then days_right_broadcast else 0 end) 
as AMOTH_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Other Athletics' then days_right_broadcast else 0 end) 
as ATHOTH_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Other Baseball' then days_right_broadcast else 0 end) 
as BASEOTH_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Other Basketball' then days_right_broadcast else 0 end) 
as BASKOTH_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Other Boxing' then days_right_broadcast else 0 end) 
as BOXOTH_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Other Cricket' then days_right_broadcast else 0 end) 
as CRIOTH_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Other Darts' then days_right_broadcast else 0 end) 
as DARTOTH_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Other Equestrian' then days_right_broadcast else 0 end) 
as EQUOTH_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Other Extreme' then days_right_broadcast else 0 end) 
as EXTOTH_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Other Fishing' then days_right_broadcast else 0 end) 
as FSHOTH_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Other Football' then days_right_broadcast else 0 end) 
as FOOTOTH_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Other Golf' then days_right_broadcast else 0 end) 
as GOLFOTH_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Other Ice Hockey' then days_right_broadcast else 0 end) 
as IHOTH_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Other Motor Sport' then days_right_broadcast else 0 end) 
as MSPOTH_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Other Racing' then days_right_broadcast else 0 end) 
as RACOTH_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Other Rugby' then days_right_broadcast else 0 end) 
as RUGOTH_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Other Rugby Internationals - ESPN' then days_right_broadcast else 0 end) 
as ORUGESPN_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Other Snooker/Pool' then days_right_broadcast else 0 end) 
as OTHSNP_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Other Tennis' then days_right_broadcast else 0 end) 
as OTHTEN_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Other Unknown' then days_right_broadcast else 0 end) 
as OTHUNK_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Other Watersports' then days_right_broadcast else 0 end) 
as OTHWAT_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Other Wintersports' then days_right_broadcast else 0 end) 
as OTHWIN_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Other Wrestling' then days_right_broadcast else 0 end) 
as OTHWRE_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then days_right_broadcast else 0 end) 
as PGASS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Premier League - BT Sport' then days_right_broadcast else 0 end) 
as PLBTS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Premier League - ESPN' then days_right_broadcast else 0 end) 
as PLESPN_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Premier League Darts 2010-2013 - Sky Sports' then days_right_broadcast else 0 end) 
as PLDSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports' then days_right_broadcast else 0 end) 
as GPLSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (Match Choice)' then days_right_broadcast else 0 end) 
as PLMCSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (MNF)' then days_right_broadcast else 0 end) 
as PLMNFSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (non Live)' then days_right_broadcast else 0 end) 
as PLNLSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (other Live)' then days_right_broadcast else 0 end) 
as PLOLSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (Sat Lunchtime)' then days_right_broadcast else 0 end) 
as PLSLSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (Sat Night Live)' then days_right_broadcast else 0 end) 
as PLSNSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (Sun 4pm)' then days_right_broadcast else 0 end) 
as PLS4SS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (Sun Lunchtime)' then days_right_broadcast else 0 end) 
as PLSULSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Premiership Rugby - Sky Sports' then days_right_broadcast else 0 end) 
as PRUSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Republic Of Ireland FA - 2010/11 To 2013/14 - Sky Sports' then days_right_broadcast else 0 end) 
as ROISS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Royal Ascot - Channel 4' then days_right_broadcast else 0 end) 
as RASCH4_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Rugby Internationals (England) - BBC' then days_right_broadcast else 0 end) 
as RIEBBC_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Rugby Internationals (Ireland) - BBC' then days_right_broadcast else 0 end) 
as RIIBBC_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Rugby Internationals (Scotland) - BBC' then days_right_broadcast else 0 end) 
as RISBBC_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Rugby Internationals (Wales) - BBC' then days_right_broadcast else 0 end) 
as RIWBBC_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Rugby League  Challenge Cup- BBC' then days_right_broadcast else 0 end) 
as RLCCBBC_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Rugby League - Sky Sports' then days_right_broadcast else 0 end) 
as RLGSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Rugby League  World Cup- BBC' then days_right_broadcast else 0 end) 
as RLWCBBC_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Sanzar Rugby Union 2011 - 2015 - Sky Sports' then days_right_broadcast else 0 end) 
as SARUSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Scottish FA Deal - 2010/11 To 2013/14 - Sky Sports' then days_right_broadcast else 0 end) 
as SFASS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Serie A - BT Sport' then days_right_broadcast else 0 end) 
as SABTS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Serie A - ESPN' then days_right_broadcast else 0 end) 
as SAESPN_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='SFL - ESPN' then days_right_broadcast else 0 end) 
as SFLESPN_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Six Nations - BBC' then days_right_broadcast else 0 end) 
as SNRBBC_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Sky 1 and Sky 2 Boxing' then days_right_broadcast else 0 end) 
as BOXS12_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Sky 1 and Sky 2 Football' then days_right_broadcast else 0 end) 
as FOOTS12_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Sky 1 and Sky 2 Motor Sport' then days_right_broadcast else 0 end) 
as MSPS12_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Sky 1 and Sky 2 Unknown' then days_right_broadcast else 0 end) 
as UNKS12_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Sky 1 and Sky 2 Wrestling' then days_right_broadcast else 0 end) 
as WRES12_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports American Football' then days_right_broadcast else 0 end) 
as AMSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Athletics' then days_right_broadcast else 0 end) 
as ATHSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Baseball' then days_right_broadcast else 0 end) 
as BASESS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Basketball' then days_right_broadcast else 0 end) 
as BASKSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Boxing' then days_right_broadcast else 0 end) 
as BOXSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Cricket' then days_right_broadcast else 0 end) 
as CRISS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Darts' then days_right_broadcast else 0 end) 
as DARTSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Equestrian' then days_right_broadcast else 0 end) 
as EQUSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Extreme' then days_right_broadcast else 0 end) 
as EXTSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Fishing' then days_right_broadcast else 0 end) 
as FISHSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Football' then days_right_broadcast else 0 end) 
as FOOTSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Golf' then days_right_broadcast else 0 end) 
as GOLFSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Ice Hockey' then days_right_broadcast else 0 end) 
as IHSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Motor Sport' then days_right_broadcast else 0 end) 
as MSPSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Racing' then days_right_broadcast else 0 end) 
as RACSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Rugby' then days_right_broadcast else 0 end) 
as RUGSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Snooker/Pool' then days_right_broadcast else 0 end) 
as SNPSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Tennis' then days_right_broadcast else 0 end) 
as TENSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Unknown' then days_right_broadcast else 0 end) 
as UNKSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Watersports' then days_right_broadcast else 0 end) 
as WATSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Wintersports' then days_right_broadcast else 0 end) 
as WINSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Wrestling' then days_right_broadcast else 0 end) 
as WRESS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Solheim Cup 2013 Ladies Golf - Sky Sports' then days_right_broadcast else 0 end) 
as SOLSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='South African Home Cricket  2012-2020 Sky Sports' then days_right_broadcast else 0 end) 
as SACSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then days_right_broadcast else 0 end) 
as SPFSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='SPFL - BT Sport' then days_right_broadcast else 0 end) 
as SPFLBTS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='SPL - ESPN' then days_right_broadcast else 0 end) 
as SPLESPN_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='SPL - Sky Sports' then days_right_broadcast else 0 end) 
as SPLSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Sportfive World Cup 2014 Qualifers - Sky Sports' then days_right_broadcast else 0 end) 
as SP5SS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='The boat race - BBC' then days_right_broadcast else 0 end) 
as BTRBBC_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='The football league show - BBC' then days_right_broadcast else 0 end) 
as FLSBBC_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='The Masters Golf - BBC' then days_right_broadcast else 0 end) 
as MGBBC_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='TNA Wrestling Challenge' then days_right_broadcast else 0 end) 
as TNACHA_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Tour de France - Eurosport' then days_right_broadcast else 0 end) 
as TDFEUR_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Tour de France - ITV' then days_right_broadcast else 0 end) 
as TDFITV_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then days_right_broadcast else 0 end) 
as USMGSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then days_right_broadcast else 0 end) 
as USOTSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='U.S Open/Womens/Seniors/Amateur Golf Sky Sports' then days_right_broadcast else 0 end) 
as USOGSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='UEFA Champions League -  Sky Sports' then days_right_broadcast else 0 end) 
as CLASS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='UEFA Champions League -  Sky Sports (non Live)' then days_right_broadcast else 0 end) 
as CLNSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='UEFA Champions League -  Sky Sports (other Live)' then days_right_broadcast else 0 end) 
as CLOSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='UEFA Champions League -  Sky Sports (Tue)' then days_right_broadcast else 0 end) 
as CLTSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='UEFA Champions League -  Sky Sports (Wed)' then days_right_broadcast else 0 end) 
as CLWSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='US Open Tennis - Eurosport' then days_right_broadcast else 0 end) 
as USOTEUR_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='USA Football - BT Sport' then days_right_broadcast else 0 end) 
as USFBTS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='USPGA Championship (2007-2016) Sky Sports' then days_right_broadcast else 0 end) 
as USPGASS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='WCQ - ESPN' then days_right_broadcast else 0 end) 
as WCQESPN_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Welsh Home International Football 2012-2014 Sky Sports' then days_right_broadcast else 0 end) 
as WIFSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='West Indies Home Cricket 2013-2020 Sky Sports' then days_right_broadcast else 0 end) 
as WICSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Wimbledon - BBC' then days_right_broadcast else 0 end) 
as WIMBBC_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='Womens Cricket - I C C Cricket Deal  2012-2015 Sky Sports' then days_right_broadcast else 0 end) 
as WICCSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='World Athletics Championship - More 4' then days_right_broadcast else 0 end) 
as WACMR4_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='World Club Championship - BBC' then days_right_broadcast else 0 end) 
as WCLBBBC_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='World Cup Qualifiers - BT Sport' then days_right_broadcast else 0 end) 
as WCQBTS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='World Darts Championship 2009-2012 Sky Sports' then days_right_broadcast else 0 end) 
as WDCSS_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='World snooker championship - BBC' then days_right_broadcast else 0 end) 
as WSCBBC_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='WWE Sky 1 and 2' then days_right_broadcast else 0 end) 
as WWES12_days_right_broadcast_LIVE
,sum(case when live=1 and analysis_right ='WWE Sky Sports' then days_right_broadcast else 0 end) 
as WWESS_days_right_broadcast_LIVE
,sum(case when Live=0 and analysis_right ='Africa Cup of Nations - Eurosport' then days_right_broadcast else 0 end) 
as AFCEUR_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Africa Cup of Nations - ITV' then days_right_broadcast else 0 end) 
as AFCITV_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Americas Cup - BBC' then days_right_broadcast else 0 end) 
as AMCBBC_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Asian Tour Golf 2012-2013 Sky Sports' then days_right_broadcast else 0 end) 
as ATGSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='ATP Tennis Masters Series  2011-2013 Sky Sports' then days_right_broadcast else 0 end) 
as ATPSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Australia Home Cricket  2012-2016 Sky Sports' then days_right_broadcast else 0 end) 
as AHCSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Australian Football - BT Sport' then days_right_broadcast else 0 end) 
as AUFBTS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Australian Open Tennis - BBC' then days_right_broadcast else 0 end) 
as AOTBBC_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Australian Open Tennis - Eurosport' then days_right_broadcast else 0 end) 
as AOTEUR_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Aviva Premiership - ESPN' then days_right_broadcast else 0 end) 
as AVPSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='BBC American Football' then days_right_broadcast else 0 end) 
as AFBBC_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='BBC Athletics' then days_right_broadcast else 0 end) 
as ATHBBC_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='BBC Boxing' then days_right_broadcast else 0 end) 
as BOXBBC_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='BBC Darts' then days_right_broadcast else 0 end) 
as DRTBBC_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='BBC Equestrian' then days_right_broadcast else 0 end) 
as EQUBBC_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='BBC Football' then days_right_broadcast else 0 end) 
as FOOTBBC_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='BBC Golf' then days_right_broadcast else 0 end) 
as GOLFBBC_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='BBC Motor Sport' then days_right_broadcast else 0 end) 
as MSPBBC_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='BBC Rugby' then days_right_broadcast else 0 end) 
as RUGBBC_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='BBC Snooker/Pool' then days_right_broadcast else 0 end) 
as SNPBBC_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='BBC Tennis' then days_right_broadcast else 0 end) 
as TENBBC_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='BBC Unknown' then days_right_broadcast else 0 end) 
as UNKBBC_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='BBC Watersports' then days_right_broadcast else 0 end) 
as WATBBC_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='BBC Wintersports' then days_right_broadcast else 0 end) 
as WINBBC_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Boxing  - Channel 5' then days_right_broadcast else 0 end) 
as BOXCH5_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Boxing - Matchroom 2012-2014 Sky Sports' then days_right_broadcast else 0 end) 
as BOXMSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Brazil Football - BT Sport' then days_right_broadcast else 0 end) 
as BFTBTS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='British & Irish Lions 2013 - Sky Sports' then days_right_broadcast else 0 end) 
as BILSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='British Open Golf - BBC' then days_right_broadcast else 0 end) 
as BOGSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport American Football' then days_right_broadcast else 0 end) 
as AFBTS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Athletics' then days_right_broadcast else 0 end) 
as ATHBTS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Baseball' then days_right_broadcast else 0 end) 
as BASEBTS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Basketball' then days_right_broadcast else 0 end) 
as BASKBTS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Boxing' then days_right_broadcast else 0 end) 
as BOXBTS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Cricket' then days_right_broadcast else 0 end) 
as CRIBTS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Equestrian' then days_right_broadcast else 0 end) 
as EQUBTS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Extreme' then days_right_broadcast else 0 end) 
as EXTBTS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Football' then days_right_broadcast else 0 end) 
as FOOTBTS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Motor Sport' then days_right_broadcast else 0 end) 
as MSPBTS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Rugby' then days_right_broadcast else 0 end) 
as RUGBTS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Tennis' then days_right_broadcast else 0 end) 
as TENBTS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Unknown' then days_right_broadcast else 0 end) 
as UNKBTS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Wintersports' then days_right_broadcast else 0 end) 
as WINBTS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Bundesliga - BT Sport' then days_right_broadcast else 0 end) 
as BUNBTS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Bundesliga- ESPN' then days_right_broadcast else 0 end) 
as BUNESPN_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Challenge Darts' then days_right_broadcast else 0 end) 
as DRTCHA_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Challenge Extreme' then days_right_broadcast else 0 end) 
as EXTCHA_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Challenge Unknown' then days_right_broadcast else 0 end) 
as UNKCHA_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Challenge Wrestling' then days_right_broadcast else 0 end) 
as WRECHA_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Champions League - ITV' then days_right_broadcast else 0 end) 
as CHLITV_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Champions Trophy - I C C Cricket Deal  2012-2015 Sky Sports' then days_right_broadcast else 0 end) 
as ICCSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 American Football' then days_right_broadcast else 0 end) 
as AMCH4_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 Athletics' then days_right_broadcast else 0 end) 
as ATHCH4_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 Boxing' then days_right_broadcast else 0 end) 
as BOXCH4_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 Equestrian' then days_right_broadcast else 0 end) 
as EQUCH4_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 Extreme' then days_right_broadcast else 0 end) 
as EXTCH4_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 Football' then days_right_broadcast else 0 end) 
as FOOTCH4_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 Motor Sport' then days_right_broadcast else 0 end) 
as MSPCH4_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 Racing' then days_right_broadcast else 0 end) 
as RACCH4_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 Unknown' then days_right_broadcast else 0 end) 
as UNKCH4_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 Watersports' then days_right_broadcast else 0 end) 
as WATCH4_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 Wintersports' then days_right_broadcast else 0 end) 
as WINCH4_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Channel 5 Athletics' then days_right_broadcast else 0 end) 
as ATHCH5_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Channel 5 Boxing' then days_right_broadcast else 0 end) 
as BOXOCH5_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Channel 5 Cricket' then days_right_broadcast else 0 end) 
as CRICH5_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Channel 5 Motor Sport' then days_right_broadcast else 0 end) 
as MSPCH5_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Channel 5 Unknown' then days_right_broadcast else 0 end) 
as UNKCH5_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Channel 5 Wrestling' then days_right_broadcast else 0 end) 
as WRECH5_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Cheltenham Festival - Channel 4' then days_right_broadcast else 0 end) 
as CHELCH4_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Community Shield - ITV' then days_right_broadcast else 0 end) 
as CMSITV_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Confederations Cup - BBC' then days_right_broadcast else 0 end) 
as CONCBBC_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Conference - BT Sport' then days_right_broadcast else 0 end) 
as CONFBTS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Cycling - La Vuelta ITV' then days_right_broadcast else 0 end) 
as CLVITV_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Cycling - U C I World Tour Sky Sports' then days_right_broadcast else 0 end) 
as CUCISS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Cycling Tour of Britain - Eurosport' then days_right_broadcast else 0 end) 
as CTBEUR_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Cycling: tour of britain ITV4' then days_right_broadcast else 0 end) 
as CTCITV_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Derby - Channel 4' then days_right_broadcast else 0 end) 
as DERCH4_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='ECB (highlights) - Channel 5' then days_right_broadcast else 0 end) 
as ECBHCH5_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='ECB Cricket Sky Sports' then days_right_broadcast else 0 end) 
as GECRSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='ECB non-Test Cricket Sky Sports' then days_right_broadcast else 0 end) 
as ECBNSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='ECB Test Cricket Sky Sports' then days_right_broadcast else 0 end) 
as ECBTSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='England Football Internationals - ITV' then days_right_broadcast else 0 end) 
as GENGITV_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='England Friendlies (Football) - ITV' then days_right_broadcast else 0 end) 
as EFRITV_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then days_right_broadcast else 0 end) 
as ENRSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='England World Cup Qualifying (Away) - ITV' then days_right_broadcast else 0 end) 
as EWQAITV_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='England World Cup Qualifying (Home) - ITV' then days_right_broadcast else 0 end) 
as EWQHITV_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='ESPN American Football' then days_right_broadcast else 0 end) 
as AMESPN_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Athletics' then days_right_broadcast else 0 end) 
as ATHESPN_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Baseball' then days_right_broadcast else 0 end) 
as BASEESPN_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Basketball' then days_right_broadcast else 0 end) 
as BASKESPN_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Boxing' then days_right_broadcast else 0 end) 
as BOXESPN_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Cricket' then days_right_broadcast else 0 end) 
as CRIESPN_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Darts' then days_right_broadcast else 0 end) 
as DARTESPN_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Equestrian' then days_right_broadcast else 0 end) 
as EQUESPN_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Extreme' then days_right_broadcast else 0 end) 
as EXTESPN_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Football' then days_right_broadcast else 0 end) 
as FOOTESPN_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Golf' then days_right_broadcast else 0 end) 
as GOLFESPN_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Ice Hockey' then days_right_broadcast else 0 end) 
as IHESPN_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Motor Sport' then days_right_broadcast else 0 end) 
as MSPESPN_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Racing' then days_right_broadcast else 0 end) 
as RACESPN_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Rugby' then days_right_broadcast else 0 end) 
as RUGESPN_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Snooker/Pool' then days_right_broadcast else 0 end) 
as SNPESPN_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Tennis' then days_right_broadcast else 0 end) 
as TENESPN_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Unknown' then days_right_broadcast else 0 end) 
as UNKESPN_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Watersports' then days_right_broadcast else 0 end) 
as WATESPN_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Wintersports' then days_right_broadcast else 0 end) 
as WINESPN_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Wrestling' then days_right_broadcast else 0 end) 
as WREESPN_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Europa League - BT Sport' then days_right_broadcast else 0 end) 
as ELBTSP_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Europa League - ESPN' then days_right_broadcast else 0 end) 
as ELESPN_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Europa League - ITV' then days_right_broadcast else 0 end) 
as ELITV_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='European Tour Golf - Sky Sports' then days_right_broadcast else 0 end) 
as ETGSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport American Football' then days_right_broadcast else 0 end) 
as AMEUR_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Athletics' then days_right_broadcast else 0 end) 
as ATHEUR_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Baseball' then days_right_broadcast else 0 end) 
as BASEEUR_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Basketball' then days_right_broadcast else 0 end) 
as BASKEUR_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Boxing' then days_right_broadcast else 0 end) 
as BOXEUR_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Cricket' then days_right_broadcast else 0 end) 
as CRIEUR_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Darts' then days_right_broadcast else 0 end) 
as DARTEUR_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Equestrian' then days_right_broadcast else 0 end) 
as EQUEUR_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Extreme' then days_right_broadcast else 0 end) 
as EXTEUR_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Football' then days_right_broadcast else 0 end) 
as FOOTEUR_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Golf' then days_right_broadcast else 0 end) 
as GOLFEUR_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Ice Hockey' then days_right_broadcast else 0 end) 
as IHEUR_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Motor Sport' then days_right_broadcast else 0 end) 
as MSPEUR_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Racing' then days_right_broadcast else 0 end) 
as RACEUR_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Snooker/Pool' then days_right_broadcast else 0 end) 
as SNPEUR_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Tennis' then days_right_broadcast else 0 end) 
as TENEUR_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Unknown' then days_right_broadcast else 0 end) 
as UNKEUR_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Watersports' then days_right_broadcast else 0 end) 
as WATEUR_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Wintersports' then days_right_broadcast else 0 end) 
as WINEUR_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='F1 - BBC' then days_right_broadcast else 0 end) 
as GF1BBC_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='F1 - Sky Sports' then days_right_broadcast else 0 end) 
as GF1SS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='F1 (non-Live)- BBC' then days_right_broadcast else 0 end) 
as F1NBBC_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='F1 (Practice Live)- BBC' then days_right_broadcast else 0 end) 
as F1PBBC_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='F1 (Qualifying Live)- BBC' then days_right_broadcast else 0 end) 
as F1QBBC_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='F1 (Race Live)- BBC' then days_right_broadcast else 0 end) 
as F1RBBC_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='FA Cup - ESPN' then days_right_broadcast else 0 end) 
as FACESPN_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='FA Cup - ITV' then days_right_broadcast else 0 end) 
as FACITV_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then days_right_broadcast else 0 end) 
as FLCCSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - others' then days_right_broadcast else 0 end) 
as FLOTSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Formula One 2012-2018 - (non-Live) Sky Sports' then days_right_broadcast else 0 end) 
as F1NSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Formula One 2012-2018 - (Practice Live) Sky Sports' then days_right_broadcast else 0 end) 
as F1PSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Formula One 2012-2018 - (Qualifying Live) Sky Sports' then days_right_broadcast else 0 end) 
as F1QSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Formula One 2012-2018 - (Race Live) Sky Sports' then days_right_broadcast else 0 end) 
as F1RSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='French Open Tennis - Eurosport' then days_right_broadcast else 0 end) 
as FOTEUR_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='French Open Tennis - ITV' then days_right_broadcast else 0 end) 
as FOTITV_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Grand National - Channel 4' then days_right_broadcast else 0 end) 
as GDNCH4_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Heineken Cup 2010/11 To 2013/14- Sky Sports' then days_right_broadcast else 0 end) 
as HECSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='I R B Sevens 2008/09 -2014/15 - Sky Sports' then days_right_broadcast else 0 end) 
as IRBSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='IAAF World Athletics Championship - Eurosport' then days_right_broadcast else 0 end) 
as WACEUR_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='India Home Cricket 2012-2018 Sky Sports' then days_right_broadcast else 0 end) 
as IHCSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='India Premier League - ITV' then days_right_broadcast else 0 end) 
as IPLITV_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='International Freindlies - ESPN' then days_right_broadcast else 0 end) 
as IFESPN_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='International Friendlies - BT Sport' then days_right_broadcast else 0 end) 
as IFBTS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='ITV1 Boxing' then days_right_broadcast else 0 end) 
as BOXITV1_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='ITV1 Football' then days_right_broadcast else 0 end) 
as FOOTITV1_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='ITV1 Motor Sport' then days_right_broadcast else 0 end) 
as MOTSITV1_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='ITV1 Rugby' then days_right_broadcast else 0 end) 
as RUGITV1_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='ITV1 Snooker/Pool' then days_right_broadcast else 0 end) 
as SNPITV1_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='ITV1 Unknown' then days_right_broadcast else 0 end) 
as UNKITV1_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='ITV4 Boxing' then days_right_broadcast else 0 end) 
as BOXITV4_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='ITV4 Cricket' then days_right_broadcast else 0 end) 
as CRIITV4_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='ITV4 Darts' then days_right_broadcast else 0 end) 
as DARTITV4_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='ITV4 Extreme' then days_right_broadcast else 0 end) 
as EXTITV4_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='ITV4 Football' then days_right_broadcast else 0 end) 
as FOOTITV4_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='ITV4 Motor Sport' then days_right_broadcast else 0 end) 
as MSPITV4_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='ITV4 Rugby' then days_right_broadcast else 0 end) 
as RUGITV4_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='ITV4 Snooker/Pool' then days_right_broadcast else 0 end) 
as SNPITV4_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='ITV4 Tennis' then days_right_broadcast else 0 end) 
as TENITV4_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='ITV4 Unknown' then days_right_broadcast else 0 end) 
as UNKITV4_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Ligue 1 - BT Sport' then days_right_broadcast else 0 end) 
as L1BTS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Ligue 1 - ESPN' then days_right_broadcast else 0 end) 
as L1ESPN_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Match of the day - BBC' then days_right_broadcast else 0 end) 
as MOTDBBC_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Matchroom Output Deal  2010-2012 (Other)- Sky Sports' then days_right_broadcast else 0 end) 
as MROSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Matchroom Output Deal  2010-2012 (Pool)- Sky Sports' then days_right_broadcast else 0 end) 
as MRPSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Matchroom Output Deal  2010-2012 (Snooker)- Sky Sports' then days_right_broadcast else 0 end) 
as MRSSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Moto GP BBC' then days_right_broadcast else 0 end) 
as MGPBBC_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='NBA - Sky Sports' then days_right_broadcast else 0 end) 
as NBASS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='NFL - BBC' then days_right_broadcast else 0 end) 
as NFLBBC_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='NFL - Channel 4' then days_right_broadcast else 0 end) 
as NFLCH4_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then days_right_broadcast else 0 end) 
as NFLSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Northern Ireland Football Association 2012-2014 - Sky Sports' then days_right_broadcast else 0 end) 
as NIFSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Oaks - Channel 4' then days_right_broadcast else 0 end) 
as OAKCH4_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Other American Football' then days_right_broadcast else 0 end) 
as AMOTH_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Other Athletics' then days_right_broadcast else 0 end) 
as ATHOTH_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Other Baseball' then days_right_broadcast else 0 end) 
as BASEOTH_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Other Basketball' then days_right_broadcast else 0 end) 
as BASKOTH_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Other Boxing' then days_right_broadcast else 0 end) 
as BOXOTH_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Other Cricket' then days_right_broadcast else 0 end) 
as CRIOTH_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Other Darts' then days_right_broadcast else 0 end) 
as DARTOTH_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Other Equestrian' then days_right_broadcast else 0 end) 
as EQUOTH_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Other Extreme' then days_right_broadcast else 0 end) 
as EXTOTH_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Other Fishing' then days_right_broadcast else 0 end) 
as FSHOTH_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Other Football' then days_right_broadcast else 0 end) 
as FOOTOTH_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Other Golf' then days_right_broadcast else 0 end) 
as GOLFOTH_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Other Ice Hockey' then days_right_broadcast else 0 end) 
as IHOTH_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Other Motor Sport' then days_right_broadcast else 0 end) 
as MSPOTH_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Other Racing' then days_right_broadcast else 0 end) 
as RACOTH_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Other Rugby' then days_right_broadcast else 0 end) 
as RUGOTH_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Other Rugby Internationals - ESPN' then days_right_broadcast else 0 end) 
as ORUGESPN_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Other Snooker/Pool' then days_right_broadcast else 0 end) 
as OTHSNP_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Other Tennis' then days_right_broadcast else 0 end) 
as OTHTEN_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Other Unknown' then days_right_broadcast else 0 end) 
as OTHUNK_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Other Watersports' then days_right_broadcast else 0 end) 
as OTHWAT_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Other Wintersports' then days_right_broadcast else 0 end) 
as OTHWIN_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Other Wrestling' then days_right_broadcast else 0 end) 
as OTHWRE_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then days_right_broadcast else 0 end) 
as PGASS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Premier League - BT Sport' then days_right_broadcast else 0 end) 
as PLBTS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Premier League - ESPN' then days_right_broadcast else 0 end) 
as PLESPN_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Premier League Darts 2010-2013 - Sky Sports' then days_right_broadcast else 0 end) 
as PLDSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Premier League Football - Sky Sports' then days_right_broadcast else 0 end) 
as GPLSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Premier League Football - Sky Sports (Match Choice)' then days_right_broadcast else 0 end) 
as PLMCSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Premier League Football - Sky Sports (MNF)' then days_right_broadcast else 0 end) 
as PLMNFSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Premier League Football - Sky Sports (non Live)' then days_right_broadcast else 0 end) 
as PLNLSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Premier League Football - Sky Sports (other Live)' then days_right_broadcast else 0 end) 
as PLOLSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Premier League Football - Sky Sports (Sat Lunchtime)' then days_right_broadcast else 0 end) 
as PLSLSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Premier League Football - Sky Sports (Sat Night Live)' then days_right_broadcast else 0 end) 
as PLSNSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Premier League Football - Sky Sports (Sun 4pm)' then days_right_broadcast else 0 end) 
as PLS4SS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Premier League Football - Sky Sports (Sun Lunchtime)' then days_right_broadcast else 0 end) 
as PLSULSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Premiership Rugby - Sky Sports' then days_right_broadcast else 0 end) 
as PRUSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Republic Of Ireland FA - 2010/11 To 2013/14 - Sky Sports' then days_right_broadcast else 0 end) 
as ROISS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Royal Ascot - Channel 4' then days_right_broadcast else 0 end) 
as RASCH4_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Rugby Internationals (England) - BBC' then days_right_broadcast else 0 end) 
as RIEBBC_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Rugby Internationals (Ireland) - BBC' then days_right_broadcast else 0 end) 
as RIIBBC_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Rugby Internationals (Scotland) - BBC' then days_right_broadcast else 0 end) 
as RISBBC_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Rugby Internationals (Wales) - BBC' then days_right_broadcast else 0 end) 
as RIWBBC_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Rugby League  Challenge Cup- BBC' then days_right_broadcast else 0 end) 
as RLCCBBC_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Rugby League - Sky Sports' then days_right_broadcast else 0 end) 
as RLGSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Rugby League  World Cup- BBC' then days_right_broadcast else 0 end) 
as RLWCBBC_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Sanzar Rugby Union 2011 - 2015 - Sky Sports' then days_right_broadcast else 0 end) 
as SARUSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Scottish FA Deal - 2010/11 To 2013/14 - Sky Sports' then days_right_broadcast else 0 end) 
as SFASS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Serie A - BT Sport' then days_right_broadcast else 0 end) 
as SABTS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Serie A - ESPN' then days_right_broadcast else 0 end) 
as SAESPN_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='SFL - ESPN' then days_right_broadcast else 0 end) 
as SFLESPN_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Six Nations - BBC' then days_right_broadcast else 0 end) 
as SNRBBC_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Sky 1 and Sky 2 Boxing' then days_right_broadcast else 0 end) 
as BOXS12_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Sky 1 and Sky 2 Football' then days_right_broadcast else 0 end) 
as FOOTS12_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Sky 1 and Sky 2 Motor Sport' then days_right_broadcast else 0 end) 
as MSPS12_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Sky 1 and Sky 2 Unknown' then days_right_broadcast else 0 end) 
as UNKS12_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Sky 1 and Sky 2 Wrestling' then days_right_broadcast else 0 end) 
as WRES12_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports American Football' then days_right_broadcast else 0 end) 
as AMSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Athletics' then days_right_broadcast else 0 end) 
as ATHSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Baseball' then days_right_broadcast else 0 end) 
as BASESS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Basketball' then days_right_broadcast else 0 end) 
as BASKSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Boxing' then days_right_broadcast else 0 end) 
as BOXSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Cricket' then days_right_broadcast else 0 end) 
as CRISS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Darts' then days_right_broadcast else 0 end) 
as DARTSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Equestrian' then days_right_broadcast else 0 end) 
as EQUSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Extreme' then days_right_broadcast else 0 end) 
as EXTSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Fishing' then days_right_broadcast else 0 end) 
as FISHSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Football' then days_right_broadcast else 0 end) 
as FOOTSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Golf' then days_right_broadcast else 0 end) 
as GOLFSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Ice Hockey' then days_right_broadcast else 0 end) 
as IHSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Motor Sport' then days_right_broadcast else 0 end) 
as MSPSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Racing' then days_right_broadcast else 0 end) 
as RACSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Rugby' then days_right_broadcast else 0 end) 
as RUGSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Snooker/Pool' then days_right_broadcast else 0 end) 
as SNPSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Tennis' then days_right_broadcast else 0 end) 
as TENSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Unknown' then days_right_broadcast else 0 end) 
as UNKSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Watersports' then days_right_broadcast else 0 end) 
as WATSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Wintersports' then days_right_broadcast else 0 end) 
as WINSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Wrestling' then days_right_broadcast else 0 end) 
as WRESS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Solheim Cup 2013 Ladies Golf - Sky Sports' then days_right_broadcast else 0 end) 
as SOLSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='South African Home Cricket  2012-2020 Sky Sports' then days_right_broadcast else 0 end) 
as SACSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then days_right_broadcast else 0 end) 
as SPFSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='SPFL - BT Sport' then days_right_broadcast else 0 end) 
as SPFLBTS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='SPL - ESPN' then days_right_broadcast else 0 end) 
as SPLESPN_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='SPL - Sky Sports' then days_right_broadcast else 0 end) 
as SPLSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Sportfive World Cup 2014 Qualifers - Sky Sports' then days_right_broadcast else 0 end) 
as SP5SS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='The boat race - BBC' then days_right_broadcast else 0 end) 
as BTRBBC_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='The football league show - BBC' then days_right_broadcast else 0 end) 
as FLSBBC_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='The Masters Golf - BBC' then days_right_broadcast else 0 end) 
as MGBBC_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='TNA Wrestling Challenge' then days_right_broadcast else 0 end) 
as TNACHA_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Tour de France - Eurosport' then days_right_broadcast else 0 end) 
as TDFEUR_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Tour de France - ITV' then days_right_broadcast else 0 end) 
as TDFITV_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then days_right_broadcast else 0 end) 
as USMGSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then days_right_broadcast else 0 end) 
as USOTSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='U.S Open/Womens/Seniors/Amateur Golf Sky Sports' then days_right_broadcast else 0 end) 
as USOGSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='UEFA Champions League -  Sky Sports' then days_right_broadcast else 0 end) 
as CLASS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='UEFA Champions League -  Sky Sports (non Live)' then days_right_broadcast else 0 end) 
as CLNSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='UEFA Champions League -  Sky Sports (other Live)' then days_right_broadcast else 0 end) 
as CLOSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='UEFA Champions League -  Sky Sports (Tue)' then days_right_broadcast else 0 end) 
as CLTSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='UEFA Champions League -  Sky Sports (Wed)' then days_right_broadcast else 0 end) 
as CLWSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='US Open Tennis - Eurosport' then days_right_broadcast else 0 end) 
as USOTEUR_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='USA Football - BT Sport' then days_right_broadcast else 0 end) 
as USFBTS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='USPGA Championship (2007-2016) Sky Sports' then days_right_broadcast else 0 end) 
as USPGASS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='WCQ - ESPN' then days_right_broadcast else 0 end) 
as WCQESPN_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Welsh Home International Football 2012-2014 Sky Sports' then days_right_broadcast else 0 end) 
as WIFSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='West Indies Home Cricket 2013-2020 Sky Sports' then days_right_broadcast else 0 end) 
as WICSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Wimbledon - BBC' then days_right_broadcast else 0 end) 
as WIMBBC_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='Womens Cricket - I C C Cricket Deal  2012-2015 Sky Sports' then days_right_broadcast else 0 end) 
as WICCSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='World Athletics Championship - More 4' then days_right_broadcast else 0 end) 
as WACMR4_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='World Club Championship - BBC' then days_right_broadcast else 0 end) 
as WCLBBBC_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='World Cup Qualifiers - BT Sport' then days_right_broadcast else 0 end) 
as WCQBTS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='World Darts Championship 2009-2012 Sky Sports' then days_right_broadcast else 0 end) 
as WDCSS_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='World snooker championship - BBC' then days_right_broadcast else 0 end) 
as WSCBBC_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='WWE Sky 1 and 2' then days_right_broadcast else 0 end) 
as WWES12_days_right_broadcastNon_Live
,sum(case when Live=0 and analysis_right ='WWE Sky Sports' then days_right_broadcast else 0 end) 
as WWESS_days_right_broadcastNon_Live

,sum(case when live=1 and analysis_right ='Africa Cup of Nations - Eurosport' then right_broadcast_duration else 0 end) 
as AFCEUR_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Africa Cup of Nations - ITV' then right_broadcast_duration else 0 end) 
as AFCITV_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Americas Cup - BBC' then right_broadcast_duration else 0 end) 
as AMCBBC_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Asian Tour Golf 2012-2013 Sky Sports' then right_broadcast_duration else 0 end) 
as ATGSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='ATP Tennis Masters Series  2011-2013 Sky Sports' then right_broadcast_duration else 0 end) 
as ATPSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Australia Home Cricket  2012-2016 Sky Sports' then right_broadcast_duration else 0 end) 
as AHCSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Australian Football - BT Sport' then right_broadcast_duration else 0 end) 
as AUFBTS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Australian Open Tennis - BBC' then right_broadcast_duration else 0 end) 
as AOTBBC_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Australian Open Tennis - Eurosport' then right_broadcast_duration else 0 end) 
as AOTEUR_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Aviva Premiership - ESPN' then right_broadcast_duration else 0 end) 
as AVPSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='BBC American Football' then right_broadcast_duration else 0 end) 
as AFBBC_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='BBC Athletics' then right_broadcast_duration else 0 end) 
as ATHBBC_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='BBC Boxing' then right_broadcast_duration else 0 end) 
as BOXBBC_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='BBC Darts' then right_broadcast_duration else 0 end) 
as DRTBBC_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='BBC Equestrian' then right_broadcast_duration else 0 end) 
as EQUBBC_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='BBC Football' then right_broadcast_duration else 0 end) 
as FOOTBBC_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='BBC Golf' then right_broadcast_duration else 0 end) 
as GOLFBBC_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='BBC Motor Sport' then right_broadcast_duration else 0 end) 
as MSPBBC_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='BBC Rugby' then right_broadcast_duration else 0 end) 
as RUGBBC_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='BBC Snooker/Pool' then right_broadcast_duration else 0 end) 
as SNPBBC_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='BBC Tennis' then right_broadcast_duration else 0 end) 
as TENBBC_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='BBC Unknown' then right_broadcast_duration else 0 end) 
as UNKBBC_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='BBC Watersports' then right_broadcast_duration else 0 end) 
as WATBBC_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='BBC Wintersports' then right_broadcast_duration else 0 end) 
as WINBBC_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Boxing  - Channel 5' then right_broadcast_duration else 0 end) 
as BOXCH5_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Boxing - Matchroom 2012-2014 Sky Sports' then right_broadcast_duration else 0 end) 
as BOXMSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Brazil Football - BT Sport' then right_broadcast_duration else 0 end) 
as BFTBTS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='British & Irish Lions 2013 - Sky Sports' then right_broadcast_duration else 0 end) 
as BILSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='British Open Golf - BBC' then right_broadcast_duration else 0 end) 
as BOGSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='BT Sport American Football' then right_broadcast_duration else 0 end) 
as AFBTS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Athletics' then right_broadcast_duration else 0 end) 
as ATHBTS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Baseball' then right_broadcast_duration else 0 end) 
as BASEBTS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Basketball' then right_broadcast_duration else 0 end) 
as BASKBTS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Boxing' then right_broadcast_duration else 0 end) 
as BOXBTS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Cricket' then right_broadcast_duration else 0 end) 
as CRIBTS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Equestrian' then right_broadcast_duration else 0 end) 
as EQUBTS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Extreme' then right_broadcast_duration else 0 end) 
as EXTBTS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Football' then right_broadcast_duration else 0 end) 
as FOOTBTS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Motor Sport' then right_broadcast_duration else 0 end) 
as MSPBTS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Rugby' then right_broadcast_duration else 0 end) 
as RUGBTS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Tennis' then right_broadcast_duration else 0 end) 
as TENBTS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Unknown' then right_broadcast_duration else 0 end) 
as UNKBTS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Wintersports' then right_broadcast_duration else 0 end) 
as WINBTS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Bundesliga - BT Sport' then right_broadcast_duration else 0 end) 
as BUNBTS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Bundesliga- ESPN' then right_broadcast_duration else 0 end) 
as BUNESPN_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Challenge Darts' then right_broadcast_duration else 0 end) 
as DRTCHA_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Challenge Extreme' then right_broadcast_duration else 0 end) 
as EXTCHA_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Challenge Unknown' then right_broadcast_duration else 0 end) 
as UNKCHA_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Challenge Wrestling' then right_broadcast_duration else 0 end) 
as WRECHA_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Champions League - ITV' then right_broadcast_duration else 0 end) 
as CHLITV_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Champions Trophy - I C C Cricket Deal  2012-2015 Sky Sports' then right_broadcast_duration else 0 end) 
as ICCSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 American Football' then right_broadcast_duration else 0 end) 
as AMCH4_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Athletics' then right_broadcast_duration else 0 end) 
as ATHCH4_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Boxing' then right_broadcast_duration else 0 end) 
as BOXCH4_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Equestrian' then right_broadcast_duration else 0 end) 
as EQUCH4_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Extreme' then right_broadcast_duration else 0 end) 
as EXTCH4_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Football' then right_broadcast_duration else 0 end) 
as FOOTCH4_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Motor Sport' then right_broadcast_duration else 0 end) 
as MSPCH4_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Racing' then right_broadcast_duration else 0 end) 
as RACCH4_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Unknown' then right_broadcast_duration else 0 end) 
as UNKCH4_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Watersports' then right_broadcast_duration else 0 end) 
as WATCH4_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Wintersports' then right_broadcast_duration else 0 end) 
as WINCH4_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Athletics' then right_broadcast_duration else 0 end) 
as ATHCH5_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Boxing' then right_broadcast_duration else 0 end) 
as BOXOCH5_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Cricket' then right_broadcast_duration else 0 end) 
as CRICH5_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Motor Sport' then right_broadcast_duration else 0 end) 
as MSPCH5_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Unknown' then right_broadcast_duration else 0 end) 
as UNKCH5_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Wrestling' then right_broadcast_duration else 0 end) 
as WRECH5_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Cheltenham Festival - Channel 4' then right_broadcast_duration else 0 end) 
as CHELCH4_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Community Shield - ITV' then right_broadcast_duration else 0 end) 
as CMSITV_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Confederations Cup - BBC' then right_broadcast_duration else 0 end) 
as CONCBBC_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Conference - BT Sport' then right_broadcast_duration else 0 end) 
as CONFBTS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Cycling - La Vuelta ITV' then right_broadcast_duration else 0 end) 
as CLVITV_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Cycling - U C I World Tour Sky Sports' then right_broadcast_duration else 0 end) 
as CUCISS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Cycling Tour of Britain - Eurosport' then right_broadcast_duration else 0 end) 
as CTBEUR_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Cycling: tour of britain ITV4' then right_broadcast_duration else 0 end) 
as CTCITV_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Derby - Channel 4' then right_broadcast_duration else 0 end) 
as DERCH4_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='ECB (highlights) - Channel 5' then right_broadcast_duration else 0 end) 
as ECBHCH5_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='ECB Cricket Sky Sports' then right_broadcast_duration else 0 end) 
as GECRSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='ECB non-Test Cricket Sky Sports' then right_broadcast_duration else 0 end) 
as ECBNSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='ECB Test Cricket Sky Sports' then right_broadcast_duration else 0 end) 
as ECBTSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='England Football Internationals - ITV' then right_broadcast_duration else 0 end) 
as GENGITV_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='England Friendlies (Football) - ITV' then right_broadcast_duration else 0 end) 
as EFRITV_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then right_broadcast_duration else 0 end) 
as ENRSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='England World Cup Qualifying (Away) - ITV' then right_broadcast_duration else 0 end) 
as EWQAITV_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='England World Cup Qualifying (Home) - ITV' then right_broadcast_duration else 0 end) 
as EWQHITV_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='ESPN American Football' then right_broadcast_duration else 0 end) 
as AMESPN_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='ESPN Athletics' then right_broadcast_duration else 0 end) 
as ATHESPN_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='ESPN Baseball' then right_broadcast_duration else 0 end) 
as BASEESPN_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='ESPN Basketball' then right_broadcast_duration else 0 end) 
as BASKESPN_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='ESPN Boxing' then right_broadcast_duration else 0 end) 
as BOXESPN_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='ESPN Cricket' then right_broadcast_duration else 0 end) 
as CRIESPN_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='ESPN Darts' then right_broadcast_duration else 0 end) 
as DARTESPN_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='ESPN Equestrian' then right_broadcast_duration else 0 end) 
as EQUESPN_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='ESPN Extreme' then right_broadcast_duration else 0 end) 
as EXTESPN_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='ESPN Football' then right_broadcast_duration else 0 end) 
as FOOTESPN_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='ESPN Golf' then right_broadcast_duration else 0 end) 
as GOLFESPN_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='ESPN Ice Hockey' then right_broadcast_duration else 0 end) 
as IHESPN_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='ESPN Motor Sport' then right_broadcast_duration else 0 end) 
as MSPESPN_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='ESPN Racing' then right_broadcast_duration else 0 end) 
as RACESPN_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='ESPN Rugby' then right_broadcast_duration else 0 end) 
as RUGESPN_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='ESPN Snooker/Pool' then right_broadcast_duration else 0 end) 
as SNPESPN_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='ESPN Tennis' then right_broadcast_duration else 0 end) 
as TENESPN_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='ESPN Unknown' then right_broadcast_duration else 0 end) 
as UNKESPN_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='ESPN Watersports' then right_broadcast_duration else 0 end) 
as WATESPN_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='ESPN Wintersports' then right_broadcast_duration else 0 end) 
as WINESPN_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='ESPN Wrestling' then right_broadcast_duration else 0 end) 
as WREESPN_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Europa League - BT Sport' then right_broadcast_duration else 0 end) 
as ELBTSP_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Europa League - ESPN' then right_broadcast_duration else 0 end) 
as ELESPN_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Europa League - ITV' then right_broadcast_duration else 0 end) 
as ELITV_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='European Tour Golf - Sky Sports' then right_broadcast_duration else 0 end) 
as ETGSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Eurosport American Football' then right_broadcast_duration else 0 end) 
as AMEUR_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Athletics' then right_broadcast_duration else 0 end) 
as ATHEUR_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Baseball' then right_broadcast_duration else 0 end) 
as BASEEUR_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Basketball' then right_broadcast_duration else 0 end) 
as BASKEUR_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Boxing' then right_broadcast_duration else 0 end) 
as BOXEUR_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Cricket' then right_broadcast_duration else 0 end) 
as CRIEUR_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Darts' then right_broadcast_duration else 0 end) 
as DARTEUR_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Equestrian' then right_broadcast_duration else 0 end) 
as EQUEUR_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Extreme' then right_broadcast_duration else 0 end) 
as EXTEUR_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Football' then right_broadcast_duration else 0 end) 
as FOOTEUR_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Golf' then right_broadcast_duration else 0 end) 
as GOLFEUR_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Ice Hockey' then right_broadcast_duration else 0 end) 
as IHEUR_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Motor Sport' then right_broadcast_duration else 0 end) 
as MSPEUR_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Racing' then right_broadcast_duration else 0 end) 
as RACEUR_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Snooker/Pool' then right_broadcast_duration else 0 end) 
as SNPEUR_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Tennis' then right_broadcast_duration else 0 end) 
as TENEUR_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Unknown' then right_broadcast_duration else 0 end) 
as UNKEUR_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Watersports' then right_broadcast_duration else 0 end) 
as WATEUR_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Wintersports' then right_broadcast_duration else 0 end) 
as WINEUR_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='F1 - BBC' then right_broadcast_duration else 0 end) 
as GF1BBC_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='F1 - Sky Sports' then right_broadcast_duration else 0 end) 
as GF1SS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='F1 (non-Live)- BBC' then right_broadcast_duration else 0 end) 
as F1NBBC_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='F1 (Practice Live)- BBC' then right_broadcast_duration else 0 end) 
as F1PBBC_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='F1 (Qualifying Live)- BBC' then right_broadcast_duration else 0 end) 
as F1QBBC_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='F1 (Race Live)- BBC' then right_broadcast_duration else 0 end) 
as F1RBBC_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='FA Cup - ESPN' then right_broadcast_duration else 0 end) 
as FACESPN_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='FA Cup - ITV' then right_broadcast_duration else 0 end) 
as FACITV_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then right_broadcast_duration else 0 end) 
as FLCCSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - others' then right_broadcast_duration else 0 end) 
as FLOTSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Formula One 2012-2018 - (non-Live) Sky Sports' then right_broadcast_duration else 0 end) 
as F1NSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Formula One 2012-2018 - (Practice Live) Sky Sports' then right_broadcast_duration else 0 end) 
as F1PSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Formula One 2012-2018 - (Qualifying Live) Sky Sports' then right_broadcast_duration else 0 end) 
as F1QSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Formula One 2012-2018 - (Race Live) Sky Sports' then right_broadcast_duration else 0 end) 
as F1RSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='French Open Tennis - Eurosport' then right_broadcast_duration else 0 end) 
as FOTEUR_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='French Open Tennis - ITV' then right_broadcast_duration else 0 end) 
as FOTITV_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Grand National - Channel 4' then right_broadcast_duration else 0 end) 
as GDNCH4_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Heineken Cup 2010/11 To 2013/14- Sky Sports' then right_broadcast_duration else 0 end) 
as HECSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='I R B Sevens 2008/09 -2014/15 - Sky Sports' then right_broadcast_duration else 0 end) 
as IRBSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='IAAF World Athletics Championship - Eurosport' then right_broadcast_duration else 0 end) 
as WACEUR_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='India Home Cricket 2012-2018 Sky Sports' then right_broadcast_duration else 0 end) 
as IHCSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='India Premier League - ITV' then right_broadcast_duration else 0 end) 
as IPLITV_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='International Freindlies - ESPN' then right_broadcast_duration else 0 end) 
as IFESPN_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='International Friendlies - BT Sport' then right_broadcast_duration else 0 end) 
as IFBTS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Boxing' then right_broadcast_duration else 0 end) 
as BOXITV1_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Football' then right_broadcast_duration else 0 end) 
as FOOTITV1_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Motor Sport' then right_broadcast_duration else 0 end) 
as MOTSITV1_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Rugby' then right_broadcast_duration else 0 end) 
as RUGITV1_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Snooker/Pool' then right_broadcast_duration else 0 end) 
as SNPITV1_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Unknown' then right_broadcast_duration else 0 end) 
as UNKITV1_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Boxing' then right_broadcast_duration else 0 end) 
as BOXITV4_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Cricket' then right_broadcast_duration else 0 end) 
as CRIITV4_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Darts' then right_broadcast_duration else 0 end) 
as DARTITV4_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Extreme' then right_broadcast_duration else 0 end) 
as EXTITV4_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Football' then right_broadcast_duration else 0 end) 
as FOOTITV4_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Motor Sport' then right_broadcast_duration else 0 end) 
as MSPITV4_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Rugby' then right_broadcast_duration else 0 end) 
as RUGITV4_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Snooker/Pool' then right_broadcast_duration else 0 end) 
as SNPITV4_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Tennis' then right_broadcast_duration else 0 end) 
as TENITV4_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Unknown' then right_broadcast_duration else 0 end) 
as UNKITV4_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Ligue 1 - BT Sport' then right_broadcast_duration else 0 end) 
as L1BTS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Ligue 1 - ESPN' then right_broadcast_duration else 0 end) 
as L1ESPN_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Match of the day - BBC' then right_broadcast_duration else 0 end) 
as MOTDBBC_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Matchroom Output Deal  2010-2012 (Other)- Sky Sports' then right_broadcast_duration else 0 end) 
as MROSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Matchroom Output Deal  2010-2012 (Pool)- Sky Sports' then right_broadcast_duration else 0 end) 
as MRPSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Matchroom Output Deal  2010-2012 (Snooker)- Sky Sports' then right_broadcast_duration else 0 end) 
as MRSSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Moto GP BBC' then right_broadcast_duration else 0 end) 
as MGPBBC_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='NBA - Sky Sports' then right_broadcast_duration else 0 end) 
as NBASS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='NFL - BBC' then right_broadcast_duration else 0 end) 
as NFLBBC_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='NFL - Channel 4' then right_broadcast_duration else 0 end) 
as NFLCH4_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then right_broadcast_duration else 0 end) 
as NFLSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Northern Ireland Football Association 2012-2014 - Sky Sports' then right_broadcast_duration else 0 end) 
as NIFSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Oaks - Channel 4' then right_broadcast_duration else 0 end) 
as OAKCH4_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Other American Football' then right_broadcast_duration else 0 end) 
as AMOTH_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Other Athletics' then right_broadcast_duration else 0 end) 
as ATHOTH_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Other Baseball' then right_broadcast_duration else 0 end) 
as BASEOTH_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Other Basketball' then right_broadcast_duration else 0 end) 
as BASKOTH_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Other Boxing' then right_broadcast_duration else 0 end) 
as BOXOTH_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Other Cricket' then right_broadcast_duration else 0 end) 
as CRIOTH_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Other Darts' then right_broadcast_duration else 0 end) 
as DARTOTH_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Other Equestrian' then right_broadcast_duration else 0 end) 
as EQUOTH_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Other Extreme' then right_broadcast_duration else 0 end) 
as EXTOTH_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Other Fishing' then right_broadcast_duration else 0 end) 
as FSHOTH_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Other Football' then right_broadcast_duration else 0 end) 
as FOOTOTH_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Other Golf' then right_broadcast_duration else 0 end) 
as GOLFOTH_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Other Ice Hockey' then right_broadcast_duration else 0 end) 
as IHOTH_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Other Motor Sport' then right_broadcast_duration else 0 end) 
as MSPOTH_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Other Racing' then right_broadcast_duration else 0 end) 
as RACOTH_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Other Rugby' then right_broadcast_duration else 0 end) 
as RUGOTH_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Other Rugby Internationals - ESPN' then right_broadcast_duration else 0 end) 
as ORUGESPN_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Other Snooker/Pool' then right_broadcast_duration else 0 end) 
as OTHSNP_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Other Tennis' then right_broadcast_duration else 0 end) 
as OTHTEN_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Other Unknown' then right_broadcast_duration else 0 end) 
as OTHUNK_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Other Watersports' then right_broadcast_duration else 0 end) 
as OTHWAT_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Other Wintersports' then right_broadcast_duration else 0 end) 
as OTHWIN_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Other Wrestling' then right_broadcast_duration else 0 end) 
as OTHWRE_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then right_broadcast_duration else 0 end) 
as PGASS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Premier League - BT Sport' then right_broadcast_duration else 0 end) 
as PLBTS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Premier League - ESPN' then right_broadcast_duration else 0 end) 
as PLESPN_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Premier League Darts 2010-2013 - Sky Sports' then right_broadcast_duration else 0 end) 
as PLDSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports' then right_broadcast_duration else 0 end) 
as GPLSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (Match Choice)' then right_broadcast_duration else 0 end) 
as PLMCSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (MNF)' then right_broadcast_duration else 0 end) 
as PLMNFSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (non Live)' then right_broadcast_duration else 0 end) 
as PLNLSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (other Live)' then right_broadcast_duration else 0 end) 
as PLOLSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (Sat Lunchtime)' then right_broadcast_duration else 0 end) 
as PLSLSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (Sat Night Live)' then right_broadcast_duration else 0 end) 
as PLSNSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (Sun 4pm)' then right_broadcast_duration else 0 end) 
as PLS4SS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (Sun Lunchtime)' then right_broadcast_duration else 0 end) 
as PLSULSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Premiership Rugby - Sky Sports' then right_broadcast_duration else 0 end) 
as PRUSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Republic Of Ireland FA - 2010/11 To 2013/14 - Sky Sports' then right_broadcast_duration else 0 end) 
as ROISS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Royal Ascot - Channel 4' then right_broadcast_duration else 0 end) 
as RASCH4_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Rugby Internationals (England) - BBC' then right_broadcast_duration else 0 end) 
as RIEBBC_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Rugby Internationals (Ireland) - BBC' then right_broadcast_duration else 0 end) 
as RIIBBC_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Rugby Internationals (Scotland) - BBC' then right_broadcast_duration else 0 end) 
as RISBBC_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Rugby Internationals (Wales) - BBC' then right_broadcast_duration else 0 end) 
as RIWBBC_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Rugby League  Challenge Cup- BBC' then right_broadcast_duration else 0 end) 
as RLCCBBC_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Rugby League - Sky Sports' then right_broadcast_duration else 0 end) 
as RLGSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Rugby League  World Cup- BBC' then right_broadcast_duration else 0 end) 
as RLWCBBC_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Sanzar Rugby Union 2011 - 2015 - Sky Sports' then right_broadcast_duration else 0 end) 
as SARUSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Scottish FA Deal - 2010/11 To 2013/14 - Sky Sports' then right_broadcast_duration else 0 end) 
as SFASS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Serie A - BT Sport' then right_broadcast_duration else 0 end) 
as SABTS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Serie A - ESPN' then right_broadcast_duration else 0 end) 
as SAESPN_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='SFL - ESPN' then right_broadcast_duration else 0 end) 
as SFLESPN_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Six Nations - BBC' then right_broadcast_duration else 0 end) 
as SNRBBC_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Sky 1 and Sky 2 Boxing' then right_broadcast_duration else 0 end) 
as BOXS12_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Sky 1 and Sky 2 Football' then right_broadcast_duration else 0 end) 
as FOOTS12_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Sky 1 and Sky 2 Motor Sport' then right_broadcast_duration else 0 end) 
as MSPS12_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Sky 1 and Sky 2 Unknown' then right_broadcast_duration else 0 end) 
as UNKS12_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Sky 1 and Sky 2 Wrestling' then right_broadcast_duration else 0 end) 
as WRES12_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports American Football' then right_broadcast_duration else 0 end) 
as AMSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Athletics' then right_broadcast_duration else 0 end) 
as ATHSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Baseball' then right_broadcast_duration else 0 end) 
as BASESS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Basketball' then right_broadcast_duration else 0 end) 
as BASKSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Boxing' then right_broadcast_duration else 0 end) 
as BOXSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Cricket' then right_broadcast_duration else 0 end) 
as CRISS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Darts' then right_broadcast_duration else 0 end) 
as DARTSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Equestrian' then right_broadcast_duration else 0 end) 
as EQUSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Extreme' then right_broadcast_duration else 0 end) 
as EXTSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Fishing' then right_broadcast_duration else 0 end) 
as FISHSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Football' then right_broadcast_duration else 0 end) 
as FOOTSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Golf' then right_broadcast_duration else 0 end) 
as GOLFSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Ice Hockey' then right_broadcast_duration else 0 end) 
as IHSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Motor Sport' then right_broadcast_duration else 0 end) 
as MSPSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Racing' then right_broadcast_duration else 0 end) 
as RACSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Rugby' then right_broadcast_duration else 0 end) 
as RUGSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Snooker/Pool' then right_broadcast_duration else 0 end) 
as SNPSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Tennis' then right_broadcast_duration else 0 end) 
as TENSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Unknown' then right_broadcast_duration else 0 end) 
as UNKSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Watersports' then right_broadcast_duration else 0 end) 
as WATSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Wintersports' then right_broadcast_duration else 0 end) 
as WINSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Wrestling' then right_broadcast_duration else 0 end) 
as WRESS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Solheim Cup 2013 Ladies Golf - Sky Sports' then right_broadcast_duration else 0 end) 
as SOLSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='South African Home Cricket  2012-2020 Sky Sports' then right_broadcast_duration else 0 end) 
as SACSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then right_broadcast_duration else 0 end) 
as SPFSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='SPFL - BT Sport' then right_broadcast_duration else 0 end) 
as SPFLBTS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='SPL - ESPN' then right_broadcast_duration else 0 end) 
as SPLESPN_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='SPL - Sky Sports' then right_broadcast_duration else 0 end) 
as SPLSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Sportfive World Cup 2014 Qualifers - Sky Sports' then right_broadcast_duration else 0 end) 
as SP5SS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='The boat race - BBC' then right_broadcast_duration else 0 end) 
as BTRBBC_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='The football league show - BBC' then right_broadcast_duration else 0 end) 
as FLSBBC_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='The Masters Golf - BBC' then right_broadcast_duration else 0 end) 
as MGBBC_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='TNA Wrestling Challenge' then right_broadcast_duration else 0 end) 
as TNACHA_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Tour de France - Eurosport' then right_broadcast_duration else 0 end) 
as TDFEUR_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Tour de France - ITV' then right_broadcast_duration else 0 end) 
as TDFITV_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then right_broadcast_duration else 0 end) 
as USMGSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then right_broadcast_duration else 0 end) 
as USOTSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='U.S Open/Womens/Seniors/Amateur Golf Sky Sports' then right_broadcast_duration else 0 end) 
as USOGSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='UEFA Champions League -  Sky Sports' then right_broadcast_duration else 0 end) 
as CLASS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='UEFA Champions League -  Sky Sports (non Live)' then right_broadcast_duration else 0 end) 
as CLNSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='UEFA Champions League -  Sky Sports (other Live)' then right_broadcast_duration else 0 end) 
as CLOSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='UEFA Champions League -  Sky Sports (Tue)' then right_broadcast_duration else 0 end) 
as CLTSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='UEFA Champions League -  Sky Sports (Wed)' then right_broadcast_duration else 0 end) 
as CLWSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='US Open Tennis - Eurosport' then right_broadcast_duration else 0 end) 
as USOTEUR_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='USA Football - BT Sport' then right_broadcast_duration else 0 end) 
as USFBTS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='USPGA Championship (2007-2016) Sky Sports' then right_broadcast_duration else 0 end) 
as USPGASS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='WCQ - ESPN' then right_broadcast_duration else 0 end) 
as WCQESPN_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Welsh Home International Football 2012-2014 Sky Sports' then right_broadcast_duration else 0 end) 
as WIFSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='West Indies Home Cricket 2013-2020 Sky Sports' then right_broadcast_duration else 0 end) 
as WICSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Wimbledon - BBC' then right_broadcast_duration else 0 end) 
as WIMBBC_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='Womens Cricket - I C C Cricket Deal  2012-2015 Sky Sports' then right_broadcast_duration else 0 end) 
as WICCSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='World Athletics Championship - More 4' then right_broadcast_duration else 0 end) 
as WACMR4_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='World Club Championship - BBC' then right_broadcast_duration else 0 end) 
as WCLBBBC_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='World Cup Qualifiers - BT Sport' then right_broadcast_duration else 0 end) 
as WCQBTS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='World Darts Championship 2009-2012 Sky Sports' then right_broadcast_duration else 0 end) 
as WDCSS_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='World snooker championship - BBC' then right_broadcast_duration else 0 end) 
as WSCBBC_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='WWE Sky 1 and 2' then right_broadcast_duration else 0 end) 
as WWES12_right_broadcast_duration_LIVE
,sum(case when live=1 and analysis_right ='WWE Sky Sports' then right_broadcast_duration else 0 end) 
as WWESS_right_broadcast_duration_LIVE
,sum(case when Live=0 and analysis_right ='Africa Cup of Nations - Eurosport' then right_broadcast_duration else 0 end) 
as AFCEUR_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Africa Cup of Nations - ITV' then right_broadcast_duration else 0 end) 
as AFCITV_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Americas Cup - BBC' then right_broadcast_duration else 0 end) 
as AMCBBC_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Asian Tour Golf 2012-2013 Sky Sports' then right_broadcast_duration else 0 end) 
as ATGSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='ATP Tennis Masters Series  2011-2013 Sky Sports' then right_broadcast_duration else 0 end) 
as ATPSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Australia Home Cricket  2012-2016 Sky Sports' then right_broadcast_duration else 0 end) 
as AHCSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Australian Football - BT Sport' then right_broadcast_duration else 0 end) 
as AUFBTS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Australian Open Tennis - BBC' then right_broadcast_duration else 0 end) 
as AOTBBC_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Australian Open Tennis - Eurosport' then right_broadcast_duration else 0 end) 
as AOTEUR_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Aviva Premiership - ESPN' then right_broadcast_duration else 0 end) 
as AVPSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='BBC American Football' then right_broadcast_duration else 0 end) 
as AFBBC_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='BBC Athletics' then right_broadcast_duration else 0 end) 
as ATHBBC_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='BBC Boxing' then right_broadcast_duration else 0 end) 
as BOXBBC_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='BBC Darts' then right_broadcast_duration else 0 end) 
as DRTBBC_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='BBC Equestrian' then right_broadcast_duration else 0 end) 
as EQUBBC_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='BBC Football' then right_broadcast_duration else 0 end) 
as FOOTBBC_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='BBC Golf' then right_broadcast_duration else 0 end) 
as GOLFBBC_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='BBC Motor Sport' then right_broadcast_duration else 0 end) 
as MSPBBC_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='BBC Rugby' then right_broadcast_duration else 0 end) 
as RUGBBC_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='BBC Snooker/Pool' then right_broadcast_duration else 0 end) 
as SNPBBC_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='BBC Tennis' then right_broadcast_duration else 0 end) 
as TENBBC_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='BBC Unknown' then right_broadcast_duration else 0 end) 
as UNKBBC_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='BBC Watersports' then right_broadcast_duration else 0 end) 
as WATBBC_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='BBC Wintersports' then right_broadcast_duration else 0 end) 
as WINBBC_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Boxing  - Channel 5' then right_broadcast_duration else 0 end) 
as BOXCH5_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Boxing - Matchroom 2012-2014 Sky Sports' then right_broadcast_duration else 0 end) 
as BOXMSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Brazil Football - BT Sport' then right_broadcast_duration else 0 end) 
as BFTBTS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='British & Irish Lions 2013 - Sky Sports' then right_broadcast_duration else 0 end) 
as BILSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='British Open Golf - BBC' then right_broadcast_duration else 0 end) 
as BOGSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport American Football' then right_broadcast_duration else 0 end) 
as AFBTS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Athletics' then right_broadcast_duration else 0 end) 
as ATHBTS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Baseball' then right_broadcast_duration else 0 end) 
as BASEBTS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Basketball' then right_broadcast_duration else 0 end) 
as BASKBTS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Boxing' then right_broadcast_duration else 0 end) 
as BOXBTS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Cricket' then right_broadcast_duration else 0 end) 
as CRIBTS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Equestrian' then right_broadcast_duration else 0 end) 
as EQUBTS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Extreme' then right_broadcast_duration else 0 end) 
as EXTBTS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Football' then right_broadcast_duration else 0 end) 
as FOOTBTS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Motor Sport' then right_broadcast_duration else 0 end) 
as MSPBTS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Rugby' then right_broadcast_duration else 0 end) 
as RUGBTS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Tennis' then right_broadcast_duration else 0 end) 
as TENBTS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Unknown' then right_broadcast_duration else 0 end) 
as UNKBTS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Wintersports' then right_broadcast_duration else 0 end) 
as WINBTS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Bundesliga - BT Sport' then right_broadcast_duration else 0 end) 
as BUNBTS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Bundesliga- ESPN' then right_broadcast_duration else 0 end) 
as BUNESPN_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Challenge Darts' then right_broadcast_duration else 0 end) 
as DRTCHA_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Challenge Extreme' then right_broadcast_duration else 0 end) 
as EXTCHA_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Challenge Unknown' then right_broadcast_duration else 0 end) 
as UNKCHA_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Challenge Wrestling' then right_broadcast_duration else 0 end) 
as WRECHA_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Champions League - ITV' then right_broadcast_duration else 0 end) 
as CHLITV_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Champions Trophy - I C C Cricket Deal  2012-2015 Sky Sports' then right_broadcast_duration else 0 end) 
as ICCSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 American Football' then right_broadcast_duration else 0 end) 
as AMCH4_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 Athletics' then right_broadcast_duration else 0 end) 
as ATHCH4_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 Boxing' then right_broadcast_duration else 0 end) 
as BOXCH4_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 Equestrian' then right_broadcast_duration else 0 end) 
as EQUCH4_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 Extreme' then right_broadcast_duration else 0 end) 
as EXTCH4_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 Football' then right_broadcast_duration else 0 end) 
as FOOTCH4_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 Motor Sport' then right_broadcast_duration else 0 end) 
as MSPCH4_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 Racing' then right_broadcast_duration else 0 end) 
as RACCH4_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 Unknown' then right_broadcast_duration else 0 end) 
as UNKCH4_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 Watersports' then right_broadcast_duration else 0 end) 
as WATCH4_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 Wintersports' then right_broadcast_duration else 0 end) 
as WINCH4_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Channel 5 Athletics' then right_broadcast_duration else 0 end) 
as ATHCH5_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Channel 5 Boxing' then right_broadcast_duration else 0 end) 
as BOXOCH5_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Channel 5 Cricket' then right_broadcast_duration else 0 end) 
as CRICH5_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Channel 5 Motor Sport' then right_broadcast_duration else 0 end) 
as MSPCH5_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Channel 5 Unknown' then right_broadcast_duration else 0 end) 
as UNKCH5_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Channel 5 Wrestling' then right_broadcast_duration else 0 end) 
as WRECH5_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Cheltenham Festival - Channel 4' then right_broadcast_duration else 0 end) 
as CHELCH4_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Community Shield - ITV' then right_broadcast_duration else 0 end) 
as CMSITV_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Confederations Cup - BBC' then right_broadcast_duration else 0 end) 
as CONCBBC_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Conference - BT Sport' then right_broadcast_duration else 0 end) 
as CONFBTS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Cycling - La Vuelta ITV' then right_broadcast_duration else 0 end) 
as CLVITV_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Cycling - U C I World Tour Sky Sports' then right_broadcast_duration else 0 end) 
as CUCISS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Cycling Tour of Britain - Eurosport' then right_broadcast_duration else 0 end) 
as CTBEUR_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Cycling: tour of britain ITV4' then right_broadcast_duration else 0 end) 
as CTCITV_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Derby - Channel 4' then right_broadcast_duration else 0 end) 
as DERCH4_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='ECB (highlights) - Channel 5' then right_broadcast_duration else 0 end) 
as ECBHCH5_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='ECB Cricket Sky Sports' then right_broadcast_duration else 0 end) 
as GECRSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='ECB non-Test Cricket Sky Sports' then right_broadcast_duration else 0 end) 
as ECBNSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='ECB Test Cricket Sky Sports' then right_broadcast_duration else 0 end) 
as ECBTSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='England Football Internationals - ITV' then right_broadcast_duration else 0 end) 
as GENGITV_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='England Friendlies (Football) - ITV' then right_broadcast_duration else 0 end) 
as EFRITV_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then right_broadcast_duration else 0 end) 
as ENRSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='England World Cup Qualifying (Away) - ITV' then right_broadcast_duration else 0 end) 
as EWQAITV_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='England World Cup Qualifying (Home) - ITV' then right_broadcast_duration else 0 end) 
as EWQHITV_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='ESPN American Football' then right_broadcast_duration else 0 end) 
as AMESPN_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Athletics' then right_broadcast_duration else 0 end) 
as ATHESPN_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Baseball' then right_broadcast_duration else 0 end) 
as BASEESPN_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Basketball' then right_broadcast_duration else 0 end) 
as BASKESPN_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Boxing' then right_broadcast_duration else 0 end) 
as BOXESPN_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Cricket' then right_broadcast_duration else 0 end) 
as CRIESPN_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Darts' then right_broadcast_duration else 0 end) 
as DARTESPN_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Equestrian' then right_broadcast_duration else 0 end) 
as EQUESPN_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Extreme' then right_broadcast_duration else 0 end) 
as EXTESPN_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Football' then right_broadcast_duration else 0 end) 
as FOOTESPN_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Golf' then right_broadcast_duration else 0 end) 
as GOLFESPN_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Ice Hockey' then right_broadcast_duration else 0 end) 
as IHESPN_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Motor Sport' then right_broadcast_duration else 0 end) 
as MSPESPN_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Racing' then right_broadcast_duration else 0 end) 
as RACESPN_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Rugby' then right_broadcast_duration else 0 end) 
as RUGESPN_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Snooker/Pool' then right_broadcast_duration else 0 end) 
as SNPESPN_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Tennis' then right_broadcast_duration else 0 end) 
as TENESPN_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Unknown' then right_broadcast_duration else 0 end) 
as UNKESPN_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Watersports' then right_broadcast_duration else 0 end) 
as WATESPN_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Wintersports' then right_broadcast_duration else 0 end) 
as WINESPN_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Wrestling' then right_broadcast_duration else 0 end) 
as WREESPN_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Europa League - BT Sport' then right_broadcast_duration else 0 end) 
as ELBTSP_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Europa League - ESPN' then right_broadcast_duration else 0 end) 
as ELESPN_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Europa League - ITV' then right_broadcast_duration else 0 end) 
as ELITV_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='European Tour Golf - Sky Sports' then right_broadcast_duration else 0 end) 
as ETGSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport American Football' then right_broadcast_duration else 0 end) 
as AMEUR_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Athletics' then right_broadcast_duration else 0 end) 
as ATHEUR_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Baseball' then right_broadcast_duration else 0 end) 
as BASEEUR_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Basketball' then right_broadcast_duration else 0 end) 
as BASKEUR_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Boxing' then right_broadcast_duration else 0 end) 
as BOXEUR_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Cricket' then right_broadcast_duration else 0 end) 
as CRIEUR_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Darts' then right_broadcast_duration else 0 end) 
as DARTEUR_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Equestrian' then right_broadcast_duration else 0 end) 
as EQUEUR_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Extreme' then right_broadcast_duration else 0 end) 
as EXTEUR_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Football' then right_broadcast_duration else 0 end) 
as FOOTEUR_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Golf' then right_broadcast_duration else 0 end) 
as GOLFEUR_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Ice Hockey' then right_broadcast_duration else 0 end) 
as IHEUR_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Motor Sport' then right_broadcast_duration else 0 end) 
as MSPEUR_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Racing' then right_broadcast_duration else 0 end) 
as RACEUR_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Snooker/Pool' then right_broadcast_duration else 0 end) 
as SNPEUR_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Tennis' then right_broadcast_duration else 0 end) 
as TENEUR_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Unknown' then right_broadcast_duration else 0 end) 
as UNKEUR_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Watersports' then right_broadcast_duration else 0 end) 
as WATEUR_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Wintersports' then right_broadcast_duration else 0 end) 
as WINEUR_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='F1 - BBC' then right_broadcast_duration else 0 end) 
as GF1BBC_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='F1 - Sky Sports' then right_broadcast_duration else 0 end) 
as GF1SS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='F1 (non-Live)- BBC' then right_broadcast_duration else 0 end) 
as F1NBBC_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='F1 (Practice Live)- BBC' then right_broadcast_duration else 0 end) 
as F1PBBC_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='F1 (Qualifying Live)- BBC' then right_broadcast_duration else 0 end) 
as F1QBBC_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='F1 (Race Live)- BBC' then right_broadcast_duration else 0 end) 
as F1RBBC_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='FA Cup - ESPN' then right_broadcast_duration else 0 end) 
as FACESPN_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='FA Cup - ITV' then right_broadcast_duration else 0 end) 
as FACITV_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then right_broadcast_duration else 0 end) 
as FLCCSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - others' then right_broadcast_duration else 0 end) 
as FLOTSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Formula One 2012-2018 - (non-Live) Sky Sports' then right_broadcast_duration else 0 end) 
as F1NSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Formula One 2012-2018 - (Practice Live) Sky Sports' then right_broadcast_duration else 0 end) 
as F1PSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Formula One 2012-2018 - (Qualifying Live) Sky Sports' then right_broadcast_duration else 0 end) 
as F1QSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Formula One 2012-2018 - (Race Live) Sky Sports' then right_broadcast_duration else 0 end) 
as F1RSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='French Open Tennis - Eurosport' then right_broadcast_duration else 0 end) 
as FOTEUR_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='French Open Tennis - ITV' then right_broadcast_duration else 0 end) 
as FOTITV_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Grand National - Channel 4' then right_broadcast_duration else 0 end) 
as GDNCH4_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Heineken Cup 2010/11 To 2013/14- Sky Sports' then right_broadcast_duration else 0 end) 
as HECSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='I R B Sevens 2008/09 -2014/15 - Sky Sports' then right_broadcast_duration else 0 end) 
as IRBSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='IAAF World Athletics Championship - Eurosport' then right_broadcast_duration else 0 end) 
as WACEUR_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='India Home Cricket 2012-2018 Sky Sports' then right_broadcast_duration else 0 end) 
as IHCSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='India Premier League - ITV' then right_broadcast_duration else 0 end) 
as IPLITV_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='International Freindlies - ESPN' then right_broadcast_duration else 0 end) 
as IFESPN_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='International Friendlies - BT Sport' then right_broadcast_duration else 0 end) 
as IFBTS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='ITV1 Boxing' then right_broadcast_duration else 0 end) 
as BOXITV1_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='ITV1 Football' then right_broadcast_duration else 0 end) 
as FOOTITV1_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='ITV1 Motor Sport' then right_broadcast_duration else 0 end) 
as MOTSITV1_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='ITV1 Rugby' then right_broadcast_duration else 0 end) 
as RUGITV1_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='ITV1 Snooker/Pool' then right_broadcast_duration else 0 end) 
as SNPITV1_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='ITV1 Unknown' then right_broadcast_duration else 0 end) 
as UNKITV1_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='ITV4 Boxing' then right_broadcast_duration else 0 end) 
as BOXITV4_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='ITV4 Cricket' then right_broadcast_duration else 0 end) 
as CRIITV4_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='ITV4 Darts' then right_broadcast_duration else 0 end) 
as DARTITV4_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='ITV4 Extreme' then right_broadcast_duration else 0 end) 
as EXTITV4_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='ITV4 Football' then right_broadcast_duration else 0 end) 
as FOOTITV4_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='ITV4 Motor Sport' then right_broadcast_duration else 0 end) 
as MSPITV4_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='ITV4 Rugby' then right_broadcast_duration else 0 end) 
as RUGITV4_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='ITV4 Snooker/Pool' then right_broadcast_duration else 0 end) 
as SNPITV4_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='ITV4 Tennis' then right_broadcast_duration else 0 end) 
as TENITV4_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='ITV4 Unknown' then right_broadcast_duration else 0 end) 
as UNKITV4_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Ligue 1 - BT Sport' then right_broadcast_duration else 0 end) 
as L1BTS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Ligue 1 - ESPN' then right_broadcast_duration else 0 end) 
as L1ESPN_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Match of the day - BBC' then right_broadcast_duration else 0 end) 
as MOTDBBC_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Matchroom Output Deal  2010-2012 (Other)- Sky Sports' then right_broadcast_duration else 0 end) 
as MROSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Matchroom Output Deal  2010-2012 (Pool)- Sky Sports' then right_broadcast_duration else 0 end) 
as MRPSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Matchroom Output Deal  2010-2012 (Snooker)- Sky Sports' then right_broadcast_duration else 0 end) 
as MRSSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Moto GP BBC' then right_broadcast_duration else 0 end) 
as MGPBBC_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='NBA - Sky Sports' then right_broadcast_duration else 0 end) 
as NBASS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='NFL - BBC' then right_broadcast_duration else 0 end) 
as NFLBBC_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='NFL - Channel 4' then right_broadcast_duration else 0 end) 
as NFLCH4_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then right_broadcast_duration else 0 end) 
as NFLSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Northern Ireland Football Association 2012-2014 - Sky Sports' then right_broadcast_duration else 0 end) 
as NIFSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Oaks - Channel 4' then right_broadcast_duration else 0 end) 
as OAKCH4_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Other American Football' then right_broadcast_duration else 0 end) 
as AMOTH_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Other Athletics' then right_broadcast_duration else 0 end) 
as ATHOTH_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Other Baseball' then right_broadcast_duration else 0 end) 
as BASEOTH_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Other Basketball' then right_broadcast_duration else 0 end) 
as BASKOTH_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Other Boxing' then right_broadcast_duration else 0 end) 
as BOXOTH_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Other Cricket' then right_broadcast_duration else 0 end) 
as CRIOTH_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Other Darts' then right_broadcast_duration else 0 end) 
as DARTOTH_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Other Equestrian' then right_broadcast_duration else 0 end) 
as EQUOTH_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Other Extreme' then right_broadcast_duration else 0 end) 
as EXTOTH_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Other Fishing' then right_broadcast_duration else 0 end) 
as FSHOTH_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Other Football' then right_broadcast_duration else 0 end) 
as FOOTOTH_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Other Golf' then right_broadcast_duration else 0 end) 
as GOLFOTH_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Other Ice Hockey' then right_broadcast_duration else 0 end) 
as IHOTH_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Other Motor Sport' then right_broadcast_duration else 0 end) 
as MSPOTH_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Other Racing' then right_broadcast_duration else 0 end) 
as RACOTH_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Other Rugby' then right_broadcast_duration else 0 end) 
as RUGOTH_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Other Rugby Internationals - ESPN' then right_broadcast_duration else 0 end) 
as ORUGESPN_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Other Snooker/Pool' then right_broadcast_duration else 0 end) 
as OTHSNP_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Other Tennis' then right_broadcast_duration else 0 end) 
as OTHTEN_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Other Unknown' then right_broadcast_duration else 0 end) 
as OTHUNK_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Other Watersports' then right_broadcast_duration else 0 end) 
as OTHWAT_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Other Wintersports' then right_broadcast_duration else 0 end) 
as OTHWIN_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Other Wrestling' then right_broadcast_duration else 0 end) 
as OTHWRE_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then right_broadcast_duration else 0 end) 
as PGASS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Premier League - BT Sport' then right_broadcast_duration else 0 end) 
as PLBTS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Premier League - ESPN' then right_broadcast_duration else 0 end) 
as PLESPN_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Premier League Darts 2010-2013 - Sky Sports' then right_broadcast_duration else 0 end) 
as PLDSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Premier League Football - Sky Sports' then right_broadcast_duration else 0 end) 
as GPLSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Premier League Football - Sky Sports (Match Choice)' then right_broadcast_duration else 0 end) 
as PLMCSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Premier League Football - Sky Sports (MNF)' then right_broadcast_duration else 0 end) 
as PLMNFSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Premier League Football - Sky Sports (non Live)' then right_broadcast_duration else 0 end) 
as PLNLSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Premier League Football - Sky Sports (other Live)' then right_broadcast_duration else 0 end) 
as PLOLSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Premier League Football - Sky Sports (Sat Lunchtime)' then right_broadcast_duration else 0 end) 
as PLSLSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Premier League Football - Sky Sports (Sat Night Live)' then right_broadcast_duration else 0 end) 
as PLSNSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Premier League Football - Sky Sports (Sun 4pm)' then right_broadcast_duration else 0 end) 
as PLS4SS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Premier League Football - Sky Sports (Sun Lunchtime)' then right_broadcast_duration else 0 end) 
as PLSULSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Premiership Rugby - Sky Sports' then right_broadcast_duration else 0 end) 
as PRUSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Republic Of Ireland FA - 2010/11 To 2013/14 - Sky Sports' then right_broadcast_duration else 0 end) 
as ROISS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Royal Ascot - Channel 4' then right_broadcast_duration else 0 end) 
as RASCH4_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Rugby Internationals (England) - BBC' then right_broadcast_duration else 0 end) 
as RIEBBC_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Rugby Internationals (Ireland) - BBC' then right_broadcast_duration else 0 end) 
as RIIBBC_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Rugby Internationals (Scotland) - BBC' then right_broadcast_duration else 0 end) 
as RISBBC_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Rugby Internationals (Wales) - BBC' then right_broadcast_duration else 0 end) 
as RIWBBC_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Rugby League  Challenge Cup- BBC' then right_broadcast_duration else 0 end) 
as RLCCBBC_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Rugby League - Sky Sports' then right_broadcast_duration else 0 end) 
as RLGSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Rugby League  World Cup- BBC' then right_broadcast_duration else 0 end) 
as RLWCBBC_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Sanzar Rugby Union 2011 - 2015 - Sky Sports' then right_broadcast_duration else 0 end) 
as SARUSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Scottish FA Deal - 2010/11 To 2013/14 - Sky Sports' then right_broadcast_duration else 0 end) 
as SFASS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Serie A - BT Sport' then right_broadcast_duration else 0 end) 
as SABTS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Serie A - ESPN' then right_broadcast_duration else 0 end) 
as SAESPN_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='SFL - ESPN' then right_broadcast_duration else 0 end) 
as SFLESPN_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Six Nations - BBC' then right_broadcast_duration else 0 end) 
as SNRBBC_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Sky 1 and Sky 2 Boxing' then right_broadcast_duration else 0 end) 
as BOXS12_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Sky 1 and Sky 2 Football' then right_broadcast_duration else 0 end) 
as FOOTS12_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Sky 1 and Sky 2 Motor Sport' then right_broadcast_duration else 0 end) 
as MSPS12_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Sky 1 and Sky 2 Unknown' then right_broadcast_duration else 0 end) 
as UNKS12_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Sky 1 and Sky 2 Wrestling' then right_broadcast_duration else 0 end) 
as WRES12_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports American Football' then right_broadcast_duration else 0 end) 
as AMSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Athletics' then right_broadcast_duration else 0 end) 
as ATHSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Baseball' then right_broadcast_duration else 0 end) 
as BASESS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Basketball' then right_broadcast_duration else 0 end) 
as BASKSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Boxing' then right_broadcast_duration else 0 end) 
as BOXSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Cricket' then right_broadcast_duration else 0 end) 
as CRISS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Darts' then right_broadcast_duration else 0 end) 
as DARTSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Equestrian' then right_broadcast_duration else 0 end) 
as EQUSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Extreme' then right_broadcast_duration else 0 end) 
as EXTSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Fishing' then right_broadcast_duration else 0 end) 
as FISHSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Football' then right_broadcast_duration else 0 end) 
as FOOTSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Golf' then right_broadcast_duration else 0 end) 
as GOLFSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Ice Hockey' then right_broadcast_duration else 0 end) 
as IHSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Motor Sport' then right_broadcast_duration else 0 end) 
as MSPSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Racing' then right_broadcast_duration else 0 end) 
as RACSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Rugby' then right_broadcast_duration else 0 end) 
as RUGSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Snooker/Pool' then right_broadcast_duration else 0 end) 
as SNPSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Tennis' then right_broadcast_duration else 0 end) 
as TENSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Unknown' then right_broadcast_duration else 0 end) 
as UNKSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Watersports' then right_broadcast_duration else 0 end) 
as WATSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Wintersports' then right_broadcast_duration else 0 end) 
as WINSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Wrestling' then right_broadcast_duration else 0 end) 
as WRESS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Solheim Cup 2013 Ladies Golf - Sky Sports' then right_broadcast_duration else 0 end) 
as SOLSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='South African Home Cricket  2012-2020 Sky Sports' then right_broadcast_duration else 0 end) 
as SACSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then right_broadcast_duration else 0 end) 
as SPFSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='SPFL - BT Sport' then right_broadcast_duration else 0 end) 
as SPFLBTS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='SPL - ESPN' then right_broadcast_duration else 0 end) 
as SPLESPN_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='SPL - Sky Sports' then right_broadcast_duration else 0 end) 
as SPLSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Sportfive World Cup 2014 Qualifers - Sky Sports' then right_broadcast_duration else 0 end) 
as SP5SS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='The boat race - BBC' then right_broadcast_duration else 0 end) 
as BTRBBC_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='The football league show - BBC' then right_broadcast_duration else 0 end) 
as FLSBBC_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='The Masters Golf - BBC' then right_broadcast_duration else 0 end) 
as MGBBC_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='TNA Wrestling Challenge' then right_broadcast_duration else 0 end) 
as TNACHA_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Tour de France - Eurosport' then right_broadcast_duration else 0 end) 
as TDFEUR_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Tour de France - ITV' then right_broadcast_duration else 0 end) 
as TDFITV_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then right_broadcast_duration else 0 end) 
as USMGSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then right_broadcast_duration else 0 end) 
as USOTSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='U.S Open/Womens/Seniors/Amateur Golf Sky Sports' then right_broadcast_duration else 0 end) 
as USOGSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='UEFA Champions League -  Sky Sports' then right_broadcast_duration else 0 end) 
as CLASS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='UEFA Champions League -  Sky Sports (non Live)' then right_broadcast_duration else 0 end) 
as CLNSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='UEFA Champions League -  Sky Sports (other Live)' then right_broadcast_duration else 0 end) 
as CLOSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='UEFA Champions League -  Sky Sports (Tue)' then right_broadcast_duration else 0 end) 
as CLTSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='UEFA Champions League -  Sky Sports (Wed)' then right_broadcast_duration else 0 end) 
as CLWSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='US Open Tennis - Eurosport' then right_broadcast_duration else 0 end) 
as USOTEUR_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='USA Football - BT Sport' then right_broadcast_duration else 0 end) 
as USFBTS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='USPGA Championship (2007-2016) Sky Sports' then right_broadcast_duration else 0 end) 
as USPGASS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='WCQ - ESPN' then right_broadcast_duration else 0 end) 
as WCQESPN_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Welsh Home International Football 2012-2014 Sky Sports' then right_broadcast_duration else 0 end) 
as WIFSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='West Indies Home Cricket 2013-2020 Sky Sports' then right_broadcast_duration else 0 end) 
as WICSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Wimbledon - BBC' then right_broadcast_duration else 0 end) 
as WIMBBC_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='Womens Cricket - I C C Cricket Deal  2012-2015 Sky Sports' then right_broadcast_duration else 0 end) 
as WICCSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='World Athletics Championship - More 4' then right_broadcast_duration else 0 end) 
as WACMR4_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='World Club Championship - BBC' then right_broadcast_duration else 0 end) 
as WCLBBBC_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='World Cup Qualifiers - BT Sport' then right_broadcast_duration else 0 end) 
as WCQBTS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='World Darts Championship 2009-2012 Sky Sports' then right_broadcast_duration else 0 end) 
as WDCSS_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='World snooker championship - BBC' then right_broadcast_duration else 0 end) 
as WSCBBC_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='WWE Sky 1 and 2' then right_broadcast_duration else 0 end) 
as WWES12_right_broadcast_durationNon_Live
,sum(case when Live=0 and analysis_right ='WWE Sky Sports' then right_broadcast_duration else 0 end) 
as WWESS_right_broadcast_durationNon_Live


,sum(case when live=1 and analysis_right ='Africa Cup of Nations - Eurosport' then right_broadcast_programmes else 0 end) 
as AFCEUR_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Africa Cup of Nations - ITV' then right_broadcast_programmes else 0 end) 
as AFCITV_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Americas Cup - BBC' then right_broadcast_programmes else 0 end) 
as AMCBBC_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Asian Tour Golf 2012-2013 Sky Sports' then right_broadcast_programmes else 0 end) 
as ATGSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='ATP Tennis Masters Series  2011-2013 Sky Sports' then right_broadcast_programmes else 0 end) 
as ATPSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Australia Home Cricket  2012-2016 Sky Sports' then right_broadcast_programmes else 0 end) 
as AHCSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Australian Football - BT Sport' then right_broadcast_programmes else 0 end) 
as AUFBTS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Australian Open Tennis - BBC' then right_broadcast_programmes else 0 end) 
as AOTBBC_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Australian Open Tennis - Eurosport' then right_broadcast_programmes else 0 end) 
as AOTEUR_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Aviva Premiership - ESPN' then right_broadcast_programmes else 0 end) 
as AVPSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='BBC American Football' then right_broadcast_programmes else 0 end) 
as AFBBC_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='BBC Athletics' then right_broadcast_programmes else 0 end) 
as ATHBBC_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='BBC Boxing' then right_broadcast_programmes else 0 end) 
as BOXBBC_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='BBC Darts' then right_broadcast_programmes else 0 end) 
as DRTBBC_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='BBC Equestrian' then right_broadcast_programmes else 0 end) 
as EQUBBC_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='BBC Football' then right_broadcast_programmes else 0 end) 
as FOOTBBC_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='BBC Golf' then right_broadcast_programmes else 0 end) 
as GOLFBBC_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='BBC Motor Sport' then right_broadcast_programmes else 0 end) 
as MSPBBC_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='BBC Rugby' then right_broadcast_programmes else 0 end) 
as RUGBBC_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='BBC Snooker/Pool' then right_broadcast_programmes else 0 end) 
as SNPBBC_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='BBC Tennis' then right_broadcast_programmes else 0 end) 
as TENBBC_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='BBC Unknown' then right_broadcast_programmes else 0 end) 
as UNKBBC_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='BBC Watersports' then right_broadcast_programmes else 0 end) 
as WATBBC_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='BBC Wintersports' then right_broadcast_programmes else 0 end) 
as WINBBC_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Boxing  - Channel 5' then right_broadcast_programmes else 0 end) 
as BOXCH5_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Boxing - Matchroom 2012-2014 Sky Sports' then right_broadcast_programmes else 0 end) 
as BOXMSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Brazil Football - BT Sport' then right_broadcast_programmes else 0 end) 
as BFTBTS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='British & Irish Lions 2013 - Sky Sports' then right_broadcast_programmes else 0 end) 
as BILSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='British Open Golf - BBC' then right_broadcast_programmes else 0 end) 
as BOGSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='BT Sport American Football' then right_broadcast_programmes else 0 end) 
as AFBTS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Athletics' then right_broadcast_programmes else 0 end) 
as ATHBTS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Baseball' then right_broadcast_programmes else 0 end) 
as BASEBTS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Basketball' then right_broadcast_programmes else 0 end) 
as BASKBTS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Boxing' then right_broadcast_programmes else 0 end) 
as BOXBTS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Cricket' then right_broadcast_programmes else 0 end) 
as CRIBTS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Equestrian' then right_broadcast_programmes else 0 end) 
as EQUBTS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Extreme' then right_broadcast_programmes else 0 end) 
as EXTBTS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Football' then right_broadcast_programmes else 0 end) 
as FOOTBTS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Motor Sport' then right_broadcast_programmes else 0 end) 
as MSPBTS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Rugby' then right_broadcast_programmes else 0 end) 
as RUGBTS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Tennis' then right_broadcast_programmes else 0 end) 
as TENBTS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Unknown' then right_broadcast_programmes else 0 end) 
as UNKBTS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='BT Sport Wintersports' then right_broadcast_programmes else 0 end) 
as WINBTS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Bundesliga - BT Sport' then right_broadcast_programmes else 0 end) 
as BUNBTS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Bundesliga- ESPN' then right_broadcast_programmes else 0 end) 
as BUNESPN_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Challenge Darts' then right_broadcast_programmes else 0 end) 
as DRTCHA_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Challenge Extreme' then right_broadcast_programmes else 0 end) 
as EXTCHA_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Challenge Unknown' then right_broadcast_programmes else 0 end) 
as UNKCHA_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Challenge Wrestling' then right_broadcast_programmes else 0 end) 
as WRECHA_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Champions League - ITV' then right_broadcast_programmes else 0 end) 
as CHLITV_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Champions Trophy - I C C Cricket Deal  2012-2015 Sky Sports' then right_broadcast_programmes else 0 end) 
as ICCSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 American Football' then right_broadcast_programmes else 0 end) 
as AMCH4_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Athletics' then right_broadcast_programmes else 0 end) 
as ATHCH4_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Boxing' then right_broadcast_programmes else 0 end) 
as BOXCH4_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Equestrian' then right_broadcast_programmes else 0 end) 
as EQUCH4_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Extreme' then right_broadcast_programmes else 0 end) 
as EXTCH4_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Football' then right_broadcast_programmes else 0 end) 
as FOOTCH4_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Motor Sport' then right_broadcast_programmes else 0 end) 
as MSPCH4_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Racing' then right_broadcast_programmes else 0 end) 
as RACCH4_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Unknown' then right_broadcast_programmes else 0 end) 
as UNKCH4_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Watersports' then right_broadcast_programmes else 0 end) 
as WATCH4_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Channel 4 Wintersports' then right_broadcast_programmes else 0 end) 
as WINCH4_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Athletics' then right_broadcast_programmes else 0 end) 
as ATHCH5_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Boxing' then right_broadcast_programmes else 0 end) 
as BOXOCH5_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Cricket' then right_broadcast_programmes else 0 end) 
as CRICH5_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Motor Sport' then right_broadcast_programmes else 0 end) 
as MSPCH5_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Unknown' then right_broadcast_programmes else 0 end) 
as UNKCH5_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Channel 5 Wrestling' then right_broadcast_programmes else 0 end) 
as WRECH5_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Cheltenham Festival - Channel 4' then right_broadcast_programmes else 0 end) 
as CHELCH4_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Community Shield - ITV' then right_broadcast_programmes else 0 end) 
as CMSITV_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Confederations Cup - BBC' then right_broadcast_programmes else 0 end) 
as CONCBBC_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Conference - BT Sport' then right_broadcast_programmes else 0 end) 
as CONFBTS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Cycling - La Vuelta ITV' then right_broadcast_programmes else 0 end) 
as CLVITV_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Cycling - U C I World Tour Sky Sports' then right_broadcast_programmes else 0 end) 
as CUCISS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Cycling Tour of Britain - Eurosport' then right_broadcast_programmes else 0 end) 
as CTBEUR_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Cycling: tour of britain ITV4' then right_broadcast_programmes else 0 end) 
as CTCITV_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Derby - Channel 4' then right_broadcast_programmes else 0 end) 
as DERCH4_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='ECB (highlights) - Channel 5' then right_broadcast_programmes else 0 end) 
as ECBHCH5_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='ECB Cricket Sky Sports' then right_broadcast_programmes else 0 end) 
as GECRSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='ECB non-Test Cricket Sky Sports' then right_broadcast_programmes else 0 end) 
as ECBNSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='ECB Test Cricket Sky Sports' then right_broadcast_programmes else 0 end) 
as ECBTSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='England Football Internationals - ITV' then right_broadcast_programmes else 0 end) 
as GENGITV_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='England Friendlies (Football) - ITV' then right_broadcast_programmes else 0 end) 
as EFRITV_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then right_broadcast_programmes else 0 end) 
as ENRSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='England World Cup Qualifying (Away) - ITV' then right_broadcast_programmes else 0 end) 
as EWQAITV_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='England World Cup Qualifying (Home) - ITV' then right_broadcast_programmes else 0 end) 
as EWQHITV_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='ESPN American Football' then right_broadcast_programmes else 0 end) 
as AMESPN_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='ESPN Athletics' then right_broadcast_programmes else 0 end) 
as ATHESPN_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='ESPN Baseball' then right_broadcast_programmes else 0 end) 
as BASEESPN_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='ESPN Basketball' then right_broadcast_programmes else 0 end) 
as BASKESPN_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='ESPN Boxing' then right_broadcast_programmes else 0 end) 
as BOXESPN_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='ESPN Cricket' then right_broadcast_programmes else 0 end) 
as CRIESPN_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='ESPN Darts' then right_broadcast_programmes else 0 end) 
as DARTESPN_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='ESPN Equestrian' then right_broadcast_programmes else 0 end) 
as EQUESPN_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='ESPN Extreme' then right_broadcast_programmes else 0 end) 
as EXTESPN_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='ESPN Football' then right_broadcast_programmes else 0 end) 
as FOOTESPN_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='ESPN Golf' then right_broadcast_programmes else 0 end) 
as GOLFESPN_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='ESPN Ice Hockey' then right_broadcast_programmes else 0 end) 
as IHESPN_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='ESPN Motor Sport' then right_broadcast_programmes else 0 end) 
as MSPESPN_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='ESPN Racing' then right_broadcast_programmes else 0 end) 
as RACESPN_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='ESPN Rugby' then right_broadcast_programmes else 0 end) 
as RUGESPN_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='ESPN Snooker/Pool' then right_broadcast_programmes else 0 end) 
as SNPESPN_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='ESPN Tennis' then right_broadcast_programmes else 0 end) 
as TENESPN_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='ESPN Unknown' then right_broadcast_programmes else 0 end) 
as UNKESPN_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='ESPN Watersports' then right_broadcast_programmes else 0 end) 
as WATESPN_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='ESPN Wintersports' then right_broadcast_programmes else 0 end) 
as WINESPN_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='ESPN Wrestling' then right_broadcast_programmes else 0 end) 
as WREESPN_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Europa League - BT Sport' then right_broadcast_programmes else 0 end) 
as ELBTSP_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Europa League - ESPN' then right_broadcast_programmes else 0 end) 
as ELESPN_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Europa League - ITV' then right_broadcast_programmes else 0 end) 
as ELITV_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='European Tour Golf - Sky Sports' then right_broadcast_programmes else 0 end) 
as ETGSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Eurosport American Football' then right_broadcast_programmes else 0 end) 
as AMEUR_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Athletics' then right_broadcast_programmes else 0 end) 
as ATHEUR_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Baseball' then right_broadcast_programmes else 0 end) 
as BASEEUR_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Basketball' then right_broadcast_programmes else 0 end) 
as BASKEUR_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Boxing' then right_broadcast_programmes else 0 end) 
as BOXEUR_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Cricket' then right_broadcast_programmes else 0 end) 
as CRIEUR_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Darts' then right_broadcast_programmes else 0 end) 
as DARTEUR_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Equestrian' then right_broadcast_programmes else 0 end) 
as EQUEUR_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Extreme' then right_broadcast_programmes else 0 end) 
as EXTEUR_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Football' then right_broadcast_programmes else 0 end) 
as FOOTEUR_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Golf' then right_broadcast_programmes else 0 end) 
as GOLFEUR_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Ice Hockey' then right_broadcast_programmes else 0 end) 
as IHEUR_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Motor Sport' then right_broadcast_programmes else 0 end) 
as MSPEUR_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Racing' then right_broadcast_programmes else 0 end) 
as RACEUR_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Snooker/Pool' then right_broadcast_programmes else 0 end) 
as SNPEUR_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Tennis' then right_broadcast_programmes else 0 end) 
as TENEUR_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Unknown' then right_broadcast_programmes else 0 end) 
as UNKEUR_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Watersports' then right_broadcast_programmes else 0 end) 
as WATEUR_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Eurosport Wintersports' then right_broadcast_programmes else 0 end) 
as WINEUR_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='F1 - BBC' then right_broadcast_programmes else 0 end) 
as GF1BBC_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='F1 - Sky Sports' then right_broadcast_programmes else 0 end) 
as GF1SS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='F1 (non-Live)- BBC' then right_broadcast_programmes else 0 end) 
as F1NBBC_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='F1 (Practice Live)- BBC' then right_broadcast_programmes else 0 end) 
as F1PBBC_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='F1 (Qualifying Live)- BBC' then right_broadcast_programmes else 0 end) 
as F1QBBC_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='F1 (Race Live)- BBC' then right_broadcast_programmes else 0 end) 
as F1RBBC_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='FA Cup - ESPN' then right_broadcast_programmes else 0 end) 
as FACESPN_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='FA Cup - ITV' then right_broadcast_programmes else 0 end) 
as FACITV_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then right_broadcast_programmes else 0 end) 
as FLCCSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - others' then right_broadcast_programmes else 0 end) 
as FLOTSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Formula One 2012-2018 - (non-Live) Sky Sports' then right_broadcast_programmes else 0 end) 
as F1NSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Formula One 2012-2018 - (Practice Live) Sky Sports' then right_broadcast_programmes else 0 end) 
as F1PSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Formula One 2012-2018 - (Qualifying Live) Sky Sports' then right_broadcast_programmes else 0 end) 
as F1QSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Formula One 2012-2018 - (Race Live) Sky Sports' then right_broadcast_programmes else 0 end) 
as F1RSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='French Open Tennis - Eurosport' then right_broadcast_programmes else 0 end) 
as FOTEUR_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='French Open Tennis - ITV' then right_broadcast_programmes else 0 end) 
as FOTITV_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Grand National - Channel 4' then right_broadcast_programmes else 0 end) 
as GDNCH4_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Heineken Cup 2010/11 To 2013/14- Sky Sports' then right_broadcast_programmes else 0 end) 
as HECSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='I R B Sevens 2008/09 -2014/15 - Sky Sports' then right_broadcast_programmes else 0 end) 
as IRBSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='IAAF World Athletics Championship - Eurosport' then right_broadcast_programmes else 0 end) 
as WACEUR_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='India Home Cricket 2012-2018 Sky Sports' then right_broadcast_programmes else 0 end) 
as IHCSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='India Premier League - ITV' then right_broadcast_programmes else 0 end) 
as IPLITV_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='International Freindlies - ESPN' then right_broadcast_programmes else 0 end) 
as IFESPN_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='International Friendlies - BT Sport' then right_broadcast_programmes else 0 end) 
as IFBTS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Boxing' then right_broadcast_programmes else 0 end) 
as BOXITV1_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Football' then right_broadcast_programmes else 0 end) 
as FOOTITV1_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Motor Sport' then right_broadcast_programmes else 0 end) 
as MOTSITV1_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Rugby' then right_broadcast_programmes else 0 end) 
as RUGITV1_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Snooker/Pool' then right_broadcast_programmes else 0 end) 
as SNPITV1_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='ITV1 Unknown' then right_broadcast_programmes else 0 end) 
as UNKITV1_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Boxing' then right_broadcast_programmes else 0 end) 
as BOXITV4_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Cricket' then right_broadcast_programmes else 0 end) 
as CRIITV4_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Darts' then right_broadcast_programmes else 0 end) 
as DARTITV4_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Extreme' then right_broadcast_programmes else 0 end) 
as EXTITV4_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Football' then right_broadcast_programmes else 0 end) 
as FOOTITV4_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Motor Sport' then right_broadcast_programmes else 0 end) 
as MSPITV4_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Rugby' then right_broadcast_programmes else 0 end) 
as RUGITV4_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Snooker/Pool' then right_broadcast_programmes else 0 end) 
as SNPITV4_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Tennis' then right_broadcast_programmes else 0 end) 
as TENITV4_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='ITV4 Unknown' then right_broadcast_programmes else 0 end) 
as UNKITV4_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Ligue 1 - BT Sport' then right_broadcast_programmes else 0 end) 
as L1BTS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Ligue 1 - ESPN' then right_broadcast_programmes else 0 end) 
as L1ESPN_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Match of the day - BBC' then right_broadcast_programmes else 0 end) 
as MOTDBBC_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Matchroom Output Deal  2010-2012 (Other)- Sky Sports' then right_broadcast_programmes else 0 end) 
as MROSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Matchroom Output Deal  2010-2012 (Pool)- Sky Sports' then right_broadcast_programmes else 0 end) 
as MRPSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Matchroom Output Deal  2010-2012 (Snooker)- Sky Sports' then right_broadcast_programmes else 0 end) 
as MRSSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Moto GP BBC' then right_broadcast_programmes else 0 end) 
as MGPBBC_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='NBA - Sky Sports' then right_broadcast_programmes else 0 end) 
as NBASS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='NFL - BBC' then right_broadcast_programmes else 0 end) 
as NFLBBC_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='NFL - Channel 4' then right_broadcast_programmes else 0 end) 
as NFLCH4_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then right_broadcast_programmes else 0 end) 
as NFLSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Northern Ireland Football Association 2012-2014 - Sky Sports' then right_broadcast_programmes else 0 end) 
as NIFSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Oaks - Channel 4' then right_broadcast_programmes else 0 end) 
as OAKCH4_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Other American Football' then right_broadcast_programmes else 0 end) 
as AMOTH_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Other Athletics' then right_broadcast_programmes else 0 end) 
as ATHOTH_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Other Baseball' then right_broadcast_programmes else 0 end) 
as BASEOTH_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Other Basketball' then right_broadcast_programmes else 0 end) 
as BASKOTH_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Other Boxing' then right_broadcast_programmes else 0 end) 
as BOXOTH_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Other Cricket' then right_broadcast_programmes else 0 end) 
as CRIOTH_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Other Darts' then right_broadcast_programmes else 0 end) 
as DARTOTH_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Other Equestrian' then right_broadcast_programmes else 0 end) 
as EQUOTH_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Other Extreme' then right_broadcast_programmes else 0 end) 
as EXTOTH_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Other Fishing' then right_broadcast_programmes else 0 end) 
as FSHOTH_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Other Football' then right_broadcast_programmes else 0 end) 
as FOOTOTH_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Other Golf' then right_broadcast_programmes else 0 end) 
as GOLFOTH_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Other Ice Hockey' then right_broadcast_programmes else 0 end) 
as IHOTH_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Other Motor Sport' then right_broadcast_programmes else 0 end) 
as MSPOTH_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Other Racing' then right_broadcast_programmes else 0 end) 
as RACOTH_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Other Rugby' then right_broadcast_programmes else 0 end) 
as RUGOTH_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Other Rugby Internationals - ESPN' then right_broadcast_programmes else 0 end) 
as ORUGESPN_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Other Snooker/Pool' then right_broadcast_programmes else 0 end) 
as OTHSNP_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Other Tennis' then right_broadcast_programmes else 0 end) 
as OTHTEN_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Other Unknown' then right_broadcast_programmes else 0 end) 
as OTHUNK_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Other Watersports' then right_broadcast_programmes else 0 end) 
as OTHWAT_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Other Wintersports' then right_broadcast_programmes else 0 end) 
as OTHWIN_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Other Wrestling' then right_broadcast_programmes else 0 end) 
as OTHWRE_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then right_broadcast_programmes else 0 end) 
as PGASS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Premier League - BT Sport' then right_broadcast_programmes else 0 end) 
as PLBTS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Premier League - ESPN' then right_broadcast_programmes else 0 end) 
as PLESPN_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Premier League Darts 2010-2013 - Sky Sports' then right_broadcast_programmes else 0 end) 
as PLDSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports' then right_broadcast_programmes else 0 end) 
as GPLSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (Match Choice)' then right_broadcast_programmes else 0 end) 
as PLMCSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (MNF)' then right_broadcast_programmes else 0 end) 
as PLMNFSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (non Live)' then right_broadcast_programmes else 0 end) 
as PLNLSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (other Live)' then right_broadcast_programmes else 0 end) 
as PLOLSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (Sat Lunchtime)' then right_broadcast_programmes else 0 end) 
as PLSLSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (Sat Night Live)' then right_broadcast_programmes else 0 end) 
as PLSNSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (Sun 4pm)' then right_broadcast_programmes else 0 end) 
as PLS4SS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Premier League Football - Sky Sports (Sun Lunchtime)' then right_broadcast_programmes else 0 end) 
as PLSULSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Premiership Rugby - Sky Sports' then right_broadcast_programmes else 0 end) 
as PRUSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Republic Of Ireland FA - 2010/11 To 2013/14 - Sky Sports' then right_broadcast_programmes else 0 end) 
as ROISS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Royal Ascot - Channel 4' then right_broadcast_programmes else 0 end) 
as RASCH4_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Rugby Internationals (England) - BBC' then right_broadcast_programmes else 0 end) 
as RIEBBC_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Rugby Internationals (Ireland) - BBC' then right_broadcast_programmes else 0 end) 
as RIIBBC_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Rugby Internationals (Scotland) - BBC' then right_broadcast_programmes else 0 end) 
as RISBBC_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Rugby Internationals (Wales) - BBC' then right_broadcast_programmes else 0 end) 
as RIWBBC_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Rugby League  Challenge Cup- BBC' then right_broadcast_programmes else 0 end) 
as RLCCBBC_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Rugby League - Sky Sports' then right_broadcast_programmes else 0 end) 
as RLGSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Rugby League  World Cup- BBC' then right_broadcast_programmes else 0 end) 
as RLWCBBC_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Sanzar Rugby Union 2011 - 2015 - Sky Sports' then right_broadcast_programmes else 0 end) 
as SARUSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Scottish FA Deal - 2010/11 To 2013/14 - Sky Sports' then right_broadcast_programmes else 0 end) 
as SFASS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Serie A - BT Sport' then right_broadcast_programmes else 0 end) 
as SABTS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Serie A - ESPN' then right_broadcast_programmes else 0 end) 
as SAESPN_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='SFL - ESPN' then right_broadcast_programmes else 0 end) 
as SFLESPN_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Six Nations - BBC' then right_broadcast_programmes else 0 end) 
as SNRBBC_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Sky 1 and Sky 2 Boxing' then right_broadcast_programmes else 0 end) 
as BOXS12_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Sky 1 and Sky 2 Football' then right_broadcast_programmes else 0 end) 
as FOOTS12_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Sky 1 and Sky 2 Motor Sport' then right_broadcast_programmes else 0 end) 
as MSPS12_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Sky 1 and Sky 2 Unknown' then right_broadcast_programmes else 0 end) 
as UNKS12_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Sky 1 and Sky 2 Wrestling' then right_broadcast_programmes else 0 end) 
as WRES12_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports American Football' then right_broadcast_programmes else 0 end) 
as AMSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Athletics' then right_broadcast_programmes else 0 end) 
as ATHSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Baseball' then right_broadcast_programmes else 0 end) 
as BASESS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Basketball' then right_broadcast_programmes else 0 end) 
as BASKSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Boxing' then right_broadcast_programmes else 0 end) 
as BOXSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Cricket' then right_broadcast_programmes else 0 end) 
as CRISS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Darts' then right_broadcast_programmes else 0 end) 
as DARTSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Equestrian' then right_broadcast_programmes else 0 end) 
as EQUSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Extreme' then right_broadcast_programmes else 0 end) 
as EXTSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Fishing' then right_broadcast_programmes else 0 end) 
as FISHSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Football' then right_broadcast_programmes else 0 end) 
as FOOTSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Golf' then right_broadcast_programmes else 0 end) 
as GOLFSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Ice Hockey' then right_broadcast_programmes else 0 end) 
as IHSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Motor Sport' then right_broadcast_programmes else 0 end) 
as MSPSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Racing' then right_broadcast_programmes else 0 end) 
as RACSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Rugby' then right_broadcast_programmes else 0 end) 
as RUGSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Snooker/Pool' then right_broadcast_programmes else 0 end) 
as SNPSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Tennis' then right_broadcast_programmes else 0 end) 
as TENSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Unknown' then right_broadcast_programmes else 0 end) 
as UNKSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Watersports' then right_broadcast_programmes else 0 end) 
as WATSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Wintersports' then right_broadcast_programmes else 0 end) 
as WINSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Sky Sports Wrestling' then right_broadcast_programmes else 0 end) 
as WRESS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Solheim Cup 2013 Ladies Golf - Sky Sports' then right_broadcast_programmes else 0 end) 
as SOLSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='South African Home Cricket  2012-2020 Sky Sports' then right_broadcast_programmes else 0 end) 
as SACSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then right_broadcast_programmes else 0 end) 
as SPFSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='SPFL - BT Sport' then right_broadcast_programmes else 0 end) 
as SPFLBTS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='SPL - ESPN' then right_broadcast_programmes else 0 end) 
as SPLESPN_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='SPL - Sky Sports' then right_broadcast_programmes else 0 end) 
as SPLSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Sportfive World Cup 2014 Qualifers - Sky Sports' then right_broadcast_programmes else 0 end) 
as SP5SS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='The boat race - BBC' then right_broadcast_programmes else 0 end) 
as BTRBBC_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='The football league show - BBC' then right_broadcast_programmes else 0 end) 
as FLSBBC_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='The Masters Golf - BBC' then right_broadcast_programmes else 0 end) 
as MGBBC_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='TNA Wrestling Challenge' then right_broadcast_programmes else 0 end) 
as TNACHA_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Tour de France - Eurosport' then right_broadcast_programmes else 0 end) 
as TDFEUR_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Tour de France - ITV' then right_broadcast_programmes else 0 end) 
as TDFITV_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then right_broadcast_programmes else 0 end) 
as USMGSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then right_broadcast_programmes else 0 end) 
as USOTSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='U.S Open/Womens/Seniors/Amateur Golf Sky Sports' then right_broadcast_programmes else 0 end) 
as USOGSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='UEFA Champions League -  Sky Sports' then right_broadcast_programmes else 0 end) 
as CLASS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='UEFA Champions League -  Sky Sports (non Live)' then right_broadcast_programmes else 0 end) 
as CLNSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='UEFA Champions League -  Sky Sports (other Live)' then right_broadcast_programmes else 0 end) 
as CLOSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='UEFA Champions League -  Sky Sports (Tue)' then right_broadcast_programmes else 0 end) 
as CLTSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='UEFA Champions League -  Sky Sports (Wed)' then right_broadcast_programmes else 0 end) 
as CLWSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='US Open Tennis - Eurosport' then right_broadcast_programmes else 0 end) 
as USOTEUR_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='USA Football - BT Sport' then right_broadcast_programmes else 0 end) 
as USFBTS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='USPGA Championship (2007-2016) Sky Sports' then right_broadcast_programmes else 0 end) 
as USPGASS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='WCQ - ESPN' then right_broadcast_programmes else 0 end) 
as WCQESPN_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Welsh Home International Football 2012-2014 Sky Sports' then right_broadcast_programmes else 0 end) 
as WIFSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='West Indies Home Cricket 2013-2020 Sky Sports' then right_broadcast_programmes else 0 end) 
as WICSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Wimbledon - BBC' then right_broadcast_programmes else 0 end) 
as WIMBBC_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='Womens Cricket - I C C Cricket Deal  2012-2015 Sky Sports' then right_broadcast_programmes else 0 end) 
as WICCSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='World Athletics Championship - More 4' then right_broadcast_programmes else 0 end) 
as WACMR4_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='World Club Championship - BBC' then right_broadcast_programmes else 0 end) 
as WCLBBBC_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='World Cup Qualifiers - BT Sport' then right_broadcast_programmes else 0 end) 
as WCQBTS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='World Darts Championship 2009-2012 Sky Sports' then right_broadcast_programmes else 0 end) 
as WDCSS_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='World snooker championship - BBC' then right_broadcast_programmes else 0 end) 
as WSCBBC_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='WWE Sky 1 and 2' then right_broadcast_programmes else 0 end) 
as WWES12_right_broadcast_programmes_LIVE
,sum(case when live=1 and analysis_right ='WWE Sky Sports' then right_broadcast_programmes else 0 end) 
as WWESS_right_broadcast_programmes_LIVE
,sum(case when Live=0 and analysis_right ='Africa Cup of Nations - Eurosport' then right_broadcast_programmes else 0 end) 
as AFCEUR_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Africa Cup of Nations - ITV' then right_broadcast_programmes else 0 end) 
as AFCITV_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Americas Cup - BBC' then right_broadcast_programmes else 0 end) 
as AMCBBC_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Asian Tour Golf 2012-2013 Sky Sports' then right_broadcast_programmes else 0 end) 
as ATGSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='ATP Tennis Masters Series  2011-2013 Sky Sports' then right_broadcast_programmes else 0 end) 
as ATPSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Australia Home Cricket  2012-2016 Sky Sports' then right_broadcast_programmes else 0 end) 
as AHCSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Australian Football - BT Sport' then right_broadcast_programmes else 0 end) 
as AUFBTS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Australian Open Tennis - BBC' then right_broadcast_programmes else 0 end) 
as AOTBBC_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Australian Open Tennis - Eurosport' then right_broadcast_programmes else 0 end) 
as AOTEUR_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Aviva Premiership - ESPN' then right_broadcast_programmes else 0 end) 
as AVPSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='BBC American Football' then right_broadcast_programmes else 0 end) 
as AFBBC_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='BBC Athletics' then right_broadcast_programmes else 0 end) 
as ATHBBC_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='BBC Boxing' then right_broadcast_programmes else 0 end) 
as BOXBBC_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='BBC Darts' then right_broadcast_programmes else 0 end) 
as DRTBBC_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='BBC Equestrian' then right_broadcast_programmes else 0 end) 
as EQUBBC_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='BBC Football' then right_broadcast_programmes else 0 end) 
as FOOTBBC_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='BBC Golf' then right_broadcast_programmes else 0 end) 
as GOLFBBC_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='BBC Motor Sport' then right_broadcast_programmes else 0 end) 
as MSPBBC_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='BBC Rugby' then right_broadcast_programmes else 0 end) 
as RUGBBC_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='BBC Snooker/Pool' then right_broadcast_programmes else 0 end) 
as SNPBBC_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='BBC Tennis' then right_broadcast_programmes else 0 end) 
as TENBBC_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='BBC Unknown' then right_broadcast_programmes else 0 end) 
as UNKBBC_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='BBC Watersports' then right_broadcast_programmes else 0 end) 
as WATBBC_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='BBC Wintersports' then right_broadcast_programmes else 0 end) 
as WINBBC_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Boxing  - Channel 5' then right_broadcast_programmes else 0 end) 
as BOXCH5_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Boxing - Matchroom 2012-2014 Sky Sports' then right_broadcast_programmes else 0 end) 
as BOXMSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Brazil Football - BT Sport' then right_broadcast_programmes else 0 end) 
as BFTBTS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='British & Irish Lions 2013 - Sky Sports' then right_broadcast_programmes else 0 end) 
as BILSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='British Open Golf - BBC' then right_broadcast_programmes else 0 end) 
as BOGSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport American Football' then right_broadcast_programmes else 0 end) 
as AFBTS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Athletics' then right_broadcast_programmes else 0 end) 
as ATHBTS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Baseball' then right_broadcast_programmes else 0 end) 
as BASEBTS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Basketball' then right_broadcast_programmes else 0 end) 
as BASKBTS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Boxing' then right_broadcast_programmes else 0 end) 
as BOXBTS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Cricket' then right_broadcast_programmes else 0 end) 
as CRIBTS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Equestrian' then right_broadcast_programmes else 0 end) 
as EQUBTS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Extreme' then right_broadcast_programmes else 0 end) 
as EXTBTS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Football' then right_broadcast_programmes else 0 end) 
as FOOTBTS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Motor Sport' then right_broadcast_programmes else 0 end) 
as MSPBTS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Rugby' then right_broadcast_programmes else 0 end) 
as RUGBTS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Tennis' then right_broadcast_programmes else 0 end) 
as TENBTS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Unknown' then right_broadcast_programmes else 0 end) 
as UNKBTS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='BT Sport Wintersports' then right_broadcast_programmes else 0 end) 
as WINBTS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Bundesliga - BT Sport' then right_broadcast_programmes else 0 end) 
as BUNBTS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Bundesliga- ESPN' then right_broadcast_programmes else 0 end) 
as BUNESPN_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Challenge Darts' then right_broadcast_programmes else 0 end) 
as DRTCHA_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Challenge Extreme' then right_broadcast_programmes else 0 end) 
as EXTCHA_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Challenge Unknown' then right_broadcast_programmes else 0 end) 
as UNKCHA_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Challenge Wrestling' then right_broadcast_programmes else 0 end) 
as WRECHA_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Champions League - ITV' then right_broadcast_programmes else 0 end) 
as CHLITV_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Champions Trophy - I C C Cricket Deal  2012-2015 Sky Sports' then right_broadcast_programmes else 0 end) 
as ICCSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 American Football' then right_broadcast_programmes else 0 end) 
as AMCH4_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 Athletics' then right_broadcast_programmes else 0 end) 
as ATHCH4_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 Boxing' then right_broadcast_programmes else 0 end) 
as BOXCH4_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 Equestrian' then right_broadcast_programmes else 0 end) 
as EQUCH4_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 Extreme' then right_broadcast_programmes else 0 end) 
as EXTCH4_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 Football' then right_broadcast_programmes else 0 end) 
as FOOTCH4_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 Motor Sport' then right_broadcast_programmes else 0 end) 
as MSPCH4_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 Racing' then right_broadcast_programmes else 0 end) 
as RACCH4_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 Unknown' then right_broadcast_programmes else 0 end) 
as UNKCH4_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 Watersports' then right_broadcast_programmes else 0 end) 
as WATCH4_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Channel 4 Wintersports' then right_broadcast_programmes else 0 end) 
as WINCH4_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Channel 5 Athletics' then right_broadcast_programmes else 0 end) 
as ATHCH5_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Channel 5 Boxing' then right_broadcast_programmes else 0 end) 
as BOXOCH5_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Channel 5 Cricket' then right_broadcast_programmes else 0 end) 
as CRICH5_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Channel 5 Motor Sport' then right_broadcast_programmes else 0 end) 
as MSPCH5_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Channel 5 Unknown' then right_broadcast_programmes else 0 end) 
as UNKCH5_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Channel 5 Wrestling' then right_broadcast_programmes else 0 end) 
as WRECH5_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Cheltenham Festival - Channel 4' then right_broadcast_programmes else 0 end) 
as CHELCH4_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Community Shield - ITV' then right_broadcast_programmes else 0 end) 
as CMSITV_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Confederations Cup - BBC' then right_broadcast_programmes else 0 end) 
as CONCBBC_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Conference - BT Sport' then right_broadcast_programmes else 0 end) 
as CONFBTS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Cycling - La Vuelta ITV' then right_broadcast_programmes else 0 end) 
as CLVITV_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Cycling - U C I World Tour Sky Sports' then right_broadcast_programmes else 0 end) 
as CUCISS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Cycling Tour of Britain - Eurosport' then right_broadcast_programmes else 0 end) 
as CTBEUR_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Cycling: tour of britain ITV4' then right_broadcast_programmes else 0 end) 
as CTCITV_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Derby - Channel 4' then right_broadcast_programmes else 0 end) 
as DERCH4_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='ECB (highlights) - Channel 5' then right_broadcast_programmes else 0 end) 
as ECBHCH5_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='ECB Cricket Sky Sports' then right_broadcast_programmes else 0 end) 
as GECRSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='ECB non-Test Cricket Sky Sports' then right_broadcast_programmes else 0 end) 
as ECBNSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='ECB Test Cricket Sky Sports' then right_broadcast_programmes else 0 end) 
as ECBTSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='England Football Internationals - ITV' then right_broadcast_programmes else 0 end) 
as GENGITV_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='England Friendlies (Football) - ITV' then right_broadcast_programmes else 0 end) 
as EFRITV_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='England Rugby Internationals 2010-2015 - Sky Sports' then right_broadcast_programmes else 0 end) 
as ENRSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='England World Cup Qualifying (Away) - ITV' then right_broadcast_programmes else 0 end) 
as EWQAITV_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='England World Cup Qualifying (Home) - ITV' then right_broadcast_programmes else 0 end) 
as EWQHITV_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='ESPN American Football' then right_broadcast_programmes else 0 end) 
as AMESPN_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Athletics' then right_broadcast_programmes else 0 end) 
as ATHESPN_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Baseball' then right_broadcast_programmes else 0 end) 
as BASEESPN_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Basketball' then right_broadcast_programmes else 0 end) 
as BASKESPN_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Boxing' then right_broadcast_programmes else 0 end) 
as BOXESPN_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Cricket' then right_broadcast_programmes else 0 end) 
as CRIESPN_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Darts' then right_broadcast_programmes else 0 end) 
as DARTESPN_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Equestrian' then right_broadcast_programmes else 0 end) 
as EQUESPN_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Extreme' then right_broadcast_programmes else 0 end) 
as EXTESPN_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Football' then right_broadcast_programmes else 0 end) 
as FOOTESPN_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Golf' then right_broadcast_programmes else 0 end) 
as GOLFESPN_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Ice Hockey' then right_broadcast_programmes else 0 end) 
as IHESPN_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Motor Sport' then right_broadcast_programmes else 0 end) 
as MSPESPN_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Racing' then right_broadcast_programmes else 0 end) 
as RACESPN_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Rugby' then right_broadcast_programmes else 0 end) 
as RUGESPN_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Snooker/Pool' then right_broadcast_programmes else 0 end) 
as SNPESPN_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Tennis' then right_broadcast_programmes else 0 end) 
as TENESPN_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Unknown' then right_broadcast_programmes else 0 end) 
as UNKESPN_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Watersports' then right_broadcast_programmes else 0 end) 
as WATESPN_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Wintersports' then right_broadcast_programmes else 0 end) 
as WINESPN_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='ESPN Wrestling' then right_broadcast_programmes else 0 end) 
as WREESPN_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Europa League - BT Sport' then right_broadcast_programmes else 0 end) 
as ELBTSP_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Europa League - ESPN' then right_broadcast_programmes else 0 end) 
as ELESPN_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Europa League - ITV' then right_broadcast_programmes else 0 end) 
as ELITV_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='European Tour Golf - Sky Sports' then right_broadcast_programmes else 0 end) 
as ETGSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport American Football' then right_broadcast_programmes else 0 end) 
as AMEUR_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Athletics' then right_broadcast_programmes else 0 end) 
as ATHEUR_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Baseball' then right_broadcast_programmes else 0 end) 
as BASEEUR_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Basketball' then right_broadcast_programmes else 0 end) 
as BASKEUR_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Boxing' then right_broadcast_programmes else 0 end) 
as BOXEUR_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Cricket' then right_broadcast_programmes else 0 end) 
as CRIEUR_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Darts' then right_broadcast_programmes else 0 end) 
as DARTEUR_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Equestrian' then right_broadcast_programmes else 0 end) 
as EQUEUR_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Extreme' then right_broadcast_programmes else 0 end) 
as EXTEUR_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Football' then right_broadcast_programmes else 0 end) 
as FOOTEUR_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Golf' then right_broadcast_programmes else 0 end) 
as GOLFEUR_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Ice Hockey' then right_broadcast_programmes else 0 end) 
as IHEUR_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Motor Sport' then right_broadcast_programmes else 0 end) 
as MSPEUR_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Racing' then right_broadcast_programmes else 0 end) 
as RACEUR_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Snooker/Pool' then right_broadcast_programmes else 0 end) 
as SNPEUR_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Tennis' then right_broadcast_programmes else 0 end) 
as TENEUR_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Unknown' then right_broadcast_programmes else 0 end) 
as UNKEUR_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Watersports' then right_broadcast_programmes else 0 end) 
as WATEUR_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Eurosport Wintersports' then right_broadcast_programmes else 0 end) 
as WINEUR_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='F1 - BBC' then right_broadcast_programmes else 0 end) 
as GF1BBC_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='F1 - Sky Sports' then right_broadcast_programmes else 0 end) 
as GF1SS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='F1 (non-Live)- BBC' then right_broadcast_programmes else 0 end) 
as F1NBBC_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='F1 (Practice Live)- BBC' then right_broadcast_programmes else 0 end) 
as F1PBBC_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='F1 (Qualifying Live)- BBC' then right_broadcast_programmes else 0 end) 
as F1QBBC_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='F1 (Race Live)- BBC' then right_broadcast_programmes else 0 end) 
as F1RBBC_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='FA Cup - ESPN' then right_broadcast_programmes else 0 end) 
as FACESPN_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='FA Cup - ITV' then right_broadcast_programmes else 0 end) 
as FACITV_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - Capital 1 Cup' then right_broadcast_programmes else 0 end) 
as FLCCSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Football League - Seasons 2012/13 To 14/15 Sky Sports - others' then right_broadcast_programmes else 0 end) 
as FLOTSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Formula One 2012-2018 - (non-Live) Sky Sports' then right_broadcast_programmes else 0 end) 
as F1NSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Formula One 2012-2018 - (Practice Live) Sky Sports' then right_broadcast_programmes else 0 end) 
as F1PSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Formula One 2012-2018 - (Qualifying Live) Sky Sports' then right_broadcast_programmes else 0 end) 
as F1QSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Formula One 2012-2018 - (Race Live) Sky Sports' then right_broadcast_programmes else 0 end) 
as F1RSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='French Open Tennis - Eurosport' then right_broadcast_programmes else 0 end) 
as FOTEUR_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='French Open Tennis - ITV' then right_broadcast_programmes else 0 end) 
as FOTITV_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Grand National - Channel 4' then right_broadcast_programmes else 0 end) 
as GDNCH4_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Heineken Cup 2010/11 To 2013/14- Sky Sports' then right_broadcast_programmes else 0 end) 
as HECSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='I R B Sevens 2008/09 -2014/15 - Sky Sports' then right_broadcast_programmes else 0 end) 
as IRBSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='IAAF World Athletics Championship - Eurosport' then right_broadcast_programmes else 0 end) 
as WACEUR_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='India Home Cricket 2012-2018 Sky Sports' then right_broadcast_programmes else 0 end) 
as IHCSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='India Premier League - ITV' then right_broadcast_programmes else 0 end) 
as IPLITV_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='International Freindlies - ESPN' then right_broadcast_programmes else 0 end) 
as IFESPN_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='International Friendlies - BT Sport' then right_broadcast_programmes else 0 end) 
as IFBTS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='ITV1 Boxing' then right_broadcast_programmes else 0 end) 
as BOXITV1_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='ITV1 Football' then right_broadcast_programmes else 0 end) 
as FOOTITV1_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='ITV1 Motor Sport' then right_broadcast_programmes else 0 end) 
as MOTSITV1_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='ITV1 Rugby' then right_broadcast_programmes else 0 end) 
as RUGITV1_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='ITV1 Snooker/Pool' then right_broadcast_programmes else 0 end) 
as SNPITV1_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='ITV1 Unknown' then right_broadcast_programmes else 0 end) 
as UNKITV1_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='ITV4 Boxing' then right_broadcast_programmes else 0 end) 
as BOXITV4_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='ITV4 Cricket' then right_broadcast_programmes else 0 end) 
as CRIITV4_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='ITV4 Darts' then right_broadcast_programmes else 0 end) 
as DARTITV4_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='ITV4 Extreme' then right_broadcast_programmes else 0 end) 
as EXTITV4_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='ITV4 Football' then right_broadcast_programmes else 0 end) 
as FOOTITV4_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='ITV4 Motor Sport' then right_broadcast_programmes else 0 end) 
as MSPITV4_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='ITV4 Rugby' then right_broadcast_programmes else 0 end) 
as RUGITV4_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='ITV4 Snooker/Pool' then right_broadcast_programmes else 0 end) 
as SNPITV4_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='ITV4 Tennis' then right_broadcast_programmes else 0 end) 
as TENITV4_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='ITV4 Unknown' then right_broadcast_programmes else 0 end) 
as UNKITV4_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Ligue 1 - BT Sport' then right_broadcast_programmes else 0 end) 
as L1BTS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Ligue 1 - ESPN' then right_broadcast_programmes else 0 end) 
as L1ESPN_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Match of the day - BBC' then right_broadcast_programmes else 0 end) 
as MOTDBBC_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Matchroom Output Deal  2010-2012 (Other)- Sky Sports' then right_broadcast_programmes else 0 end) 
as MROSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Matchroom Output Deal  2010-2012 (Pool)- Sky Sports' then right_broadcast_programmes else 0 end) 
as MRPSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Matchroom Output Deal  2010-2012 (Snooker)- Sky Sports' then right_broadcast_programmes else 0 end) 
as MRSSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Moto GP BBC' then right_broadcast_programmes else 0 end) 
as MGPBBC_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='NBA - Sky Sports' then right_broadcast_programmes else 0 end) 
as NBASS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='NFL - BBC' then right_broadcast_programmes else 0 end) 
as NFLBBC_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='NFL - Channel 4' then right_broadcast_programmes else 0 end) 
as NFLCH4_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='NFL Seasons 2012-2015 - Sky Sports' then right_broadcast_programmes else 0 end) 
as NFLSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Northern Ireland Football Association 2012-2014 - Sky Sports' then right_broadcast_programmes else 0 end) 
as NIFSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Oaks - Channel 4' then right_broadcast_programmes else 0 end) 
as OAKCH4_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Other American Football' then right_broadcast_programmes else 0 end) 
as AMOTH_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Other Athletics' then right_broadcast_programmes else 0 end) 
as ATHOTH_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Other Baseball' then right_broadcast_programmes else 0 end) 
as BASEOTH_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Other Basketball' then right_broadcast_programmes else 0 end) 
as BASKOTH_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Other Boxing' then right_broadcast_programmes else 0 end) 
as BOXOTH_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Other Cricket' then right_broadcast_programmes else 0 end) 
as CRIOTH_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Other Darts' then right_broadcast_programmes else 0 end) 
as DARTOTH_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Other Equestrian' then right_broadcast_programmes else 0 end) 
as EQUOTH_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Other Extreme' then right_broadcast_programmes else 0 end) 
as EXTOTH_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Other Fishing' then right_broadcast_programmes else 0 end) 
as FSHOTH_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Other Football' then right_broadcast_programmes else 0 end) 
as FOOTOTH_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Other Golf' then right_broadcast_programmes else 0 end) 
as GOLFOTH_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Other Ice Hockey' then right_broadcast_programmes else 0 end) 
as IHOTH_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Other Motor Sport' then right_broadcast_programmes else 0 end) 
as MSPOTH_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Other Racing' then right_broadcast_programmes else 0 end) 
as RACOTH_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Other Rugby' then right_broadcast_programmes else 0 end) 
as RUGOTH_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Other Rugby Internationals - ESPN' then right_broadcast_programmes else 0 end) 
as ORUGESPN_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Other Snooker/Pool' then right_broadcast_programmes else 0 end) 
as OTHSNP_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Other Tennis' then right_broadcast_programmes else 0 end) 
as OTHTEN_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Other Unknown' then right_broadcast_programmes else 0 end) 
as OTHUNK_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Other Watersports' then right_broadcast_programmes else 0 end) 
as OTHWAT_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Other Wintersports' then right_broadcast_programmes else 0 end) 
as OTHWIN_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Other Wrestling' then right_broadcast_programmes else 0 end) 
as OTHWRE_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='PGA Tour Golf 2010-2017 - Sky Sports' then right_broadcast_programmes else 0 end) 
as PGASS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Premier League - BT Sport' then right_broadcast_programmes else 0 end) 
as PLBTS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Premier League - ESPN' then right_broadcast_programmes else 0 end) 
as PLESPN_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Premier League Darts 2010-2013 - Sky Sports' then right_broadcast_programmes else 0 end) 
as PLDSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Premier League Football - Sky Sports' then right_broadcast_programmes else 0 end) 
as GPLSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Premier League Football - Sky Sports (Match Choice)' then right_broadcast_programmes else 0 end) 
as PLMCSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Premier League Football - Sky Sports (MNF)' then right_broadcast_programmes else 0 end) 
as PLMNFSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Premier League Football - Sky Sports (non Live)' then right_broadcast_programmes else 0 end) 
as PLNLSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Premier League Football - Sky Sports (other Live)' then right_broadcast_programmes else 0 end) 
as PLOLSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Premier League Football - Sky Sports (Sat Lunchtime)' then right_broadcast_programmes else 0 end) 
as PLSLSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Premier League Football - Sky Sports (Sat Night Live)' then right_broadcast_programmes else 0 end) 
as PLSNSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Premier League Football - Sky Sports (Sun 4pm)' then right_broadcast_programmes else 0 end) 
as PLS4SS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Premier League Football - Sky Sports (Sun Lunchtime)' then right_broadcast_programmes else 0 end) 
as PLSULSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Premiership Rugby - Sky Sports' then right_broadcast_programmes else 0 end) 
as PRUSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Republic Of Ireland FA - 2010/11 To 2013/14 - Sky Sports' then right_broadcast_programmes else 0 end) 
as ROISS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Royal Ascot - Channel 4' then right_broadcast_programmes else 0 end) 
as RASCH4_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Rugby Internationals (England) - BBC' then right_broadcast_programmes else 0 end) 
as RIEBBC_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Rugby Internationals (Ireland) - BBC' then right_broadcast_programmes else 0 end) 
as RIIBBC_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Rugby Internationals (Scotland) - BBC' then right_broadcast_programmes else 0 end) 
as RISBBC_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Rugby Internationals (Wales) - BBC' then right_broadcast_programmes else 0 end) 
as RIWBBC_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Rugby League  Challenge Cup- BBC' then right_broadcast_programmes else 0 end) 
as RLCCBBC_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Rugby League - Sky Sports' then right_broadcast_programmes else 0 end) 
as RLGSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Rugby League  World Cup- BBC' then right_broadcast_programmes else 0 end) 
as RLWCBBC_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Sanzar Rugby Union 2011 - 2015 - Sky Sports' then right_broadcast_programmes else 0 end) 
as SARUSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Scottish FA Deal - 2010/11 To 2013/14 - Sky Sports' then right_broadcast_programmes else 0 end) 
as SFASS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Serie A - BT Sport' then right_broadcast_programmes else 0 end) 
as SABTS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Serie A - ESPN' then right_broadcast_programmes else 0 end) 
as SAESPN_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='SFL - ESPN' then right_broadcast_programmes else 0 end) 
as SFLESPN_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Six Nations - BBC' then right_broadcast_programmes else 0 end) 
as SNRBBC_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Sky 1 and Sky 2 Boxing' then right_broadcast_programmes else 0 end) 
as BOXS12_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Sky 1 and Sky 2 Football' then right_broadcast_programmes else 0 end) 
as FOOTS12_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Sky 1 and Sky 2 Motor Sport' then right_broadcast_programmes else 0 end) 
as MSPS12_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Sky 1 and Sky 2 Unknown' then right_broadcast_programmes else 0 end) 
as UNKS12_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Sky 1 and Sky 2 Wrestling' then right_broadcast_programmes else 0 end) 
as WRES12_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports American Football' then right_broadcast_programmes else 0 end) 
as AMSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Athletics' then right_broadcast_programmes else 0 end) 
as ATHSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Baseball' then right_broadcast_programmes else 0 end) 
as BASESS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Basketball' then right_broadcast_programmes else 0 end) 
as BASKSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Boxing' then right_broadcast_programmes else 0 end) 
as BOXSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Cricket' then right_broadcast_programmes else 0 end) 
as CRISS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Darts' then right_broadcast_programmes else 0 end) 
as DARTSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Equestrian' then right_broadcast_programmes else 0 end) 
as EQUSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Extreme' then right_broadcast_programmes else 0 end) 
as EXTSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Fishing' then right_broadcast_programmes else 0 end) 
as FISHSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Football' then right_broadcast_programmes else 0 end) 
as FOOTSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Golf' then right_broadcast_programmes else 0 end) 
as GOLFSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Ice Hockey' then right_broadcast_programmes else 0 end) 
as IHSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Motor Sport' then right_broadcast_programmes else 0 end) 
as MSPSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Racing' then right_broadcast_programmes else 0 end) 
as RACSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Rugby' then right_broadcast_programmes else 0 end) 
as RUGSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Snooker/Pool' then right_broadcast_programmes else 0 end) 
as SNPSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Tennis' then right_broadcast_programmes else 0 end) 
as TENSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Unknown' then right_broadcast_programmes else 0 end) 
as UNKSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Watersports' then right_broadcast_programmes else 0 end) 
as WATSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Wintersports' then right_broadcast_programmes else 0 end) 
as WINSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Sky Sports Wrestling' then right_broadcast_programmes else 0 end) 
as WRESS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Solheim Cup 2013 Ladies Golf - Sky Sports' then right_broadcast_programmes else 0 end) 
as SOLSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='South African Home Cricket  2012-2020 Sky Sports' then right_broadcast_programmes else 0 end) 
as SACSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Spanish Football 2012/13 To 14/15 - Sky Sports' then right_broadcast_programmes else 0 end) 
as SPFSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='SPFL - BT Sport' then right_broadcast_programmes else 0 end) 
as SPFLBTS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='SPL - ESPN' then right_broadcast_programmes else 0 end) 
as SPLESPN_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='SPL - Sky Sports' then right_broadcast_programmes else 0 end) 
as SPLSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Sportfive World Cup 2014 Qualifers - Sky Sports' then right_broadcast_programmes else 0 end) 
as SP5SS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='The boat race - BBC' then right_broadcast_programmes else 0 end) 
as BTRBBC_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='The football league show - BBC' then right_broadcast_programmes else 0 end) 
as FLSBBC_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='The Masters Golf - BBC' then right_broadcast_programmes else 0 end) 
as MGBBC_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='TNA Wrestling Challenge' then right_broadcast_programmes else 0 end) 
as TNACHA_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Tour de France - Eurosport' then right_broadcast_programmes else 0 end) 
as TDFEUR_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Tour de France - ITV' then right_broadcast_programmes else 0 end) 
as TDFITV_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='U S Masters Golf 2011-2014 Sky Sports' then right_broadcast_programmes else 0 end) 
as USMGSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='U.S Open Tennis 2013-2015 Sky Sports' then right_broadcast_programmes else 0 end) 
as USOTSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='U.S Open/Womens/Seniors/Amateur Golf Sky Sports' then right_broadcast_programmes else 0 end) 
as USOGSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='UEFA Champions League -  Sky Sports' then right_broadcast_programmes else 0 end) 
as CLASS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='UEFA Champions League -  Sky Sports (non Live)' then right_broadcast_programmes else 0 end) 
as CLNSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='UEFA Champions League -  Sky Sports (other Live)' then right_broadcast_programmes else 0 end) 
as CLOSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='UEFA Champions League -  Sky Sports (Tue)' then right_broadcast_programmes else 0 end) 
as CLTSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='UEFA Champions League -  Sky Sports (Wed)' then right_broadcast_programmes else 0 end) 
as CLWSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='US Open Tennis - Eurosport' then right_broadcast_programmes else 0 end) 
as USOTEUR_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='USA Football - BT Sport' then right_broadcast_programmes else 0 end) 
as USFBTS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='USPGA Championship (2007-2016) Sky Sports' then right_broadcast_programmes else 0 end) 
as USPGASS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='WCQ - ESPN' then right_broadcast_programmes else 0 end) 
as WCQESPN_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Welsh Home International Football 2012-2014 Sky Sports' then right_broadcast_programmes else 0 end) 
as WIFSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='West Indies Home Cricket 2013-2020 Sky Sports' then right_broadcast_programmes else 0 end) 
as WICSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Wimbledon - BBC' then right_broadcast_programmes else 0 end) 
as WIMBBC_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='Womens Cricket - I C C Cricket Deal  2012-2015 Sky Sports' then right_broadcast_programmes else 0 end) 
as WICCSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='World Athletics Championship - More 4' then right_broadcast_programmes else 0 end) 
as WACMR4_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='World Club Championship - BBC' then right_broadcast_programmes else 0 end) 
as WCLBBBC_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='World Cup Qualifiers - BT Sport' then right_broadcast_programmes else 0 end) 
as WCQBTS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='World Darts Championship 2009-2012 Sky Sports' then right_broadcast_programmes else 0 end) 
as WDCSS_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='World snooker championship - BBC' then right_broadcast_programmes else 0 end) 
as WSCBBC_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='WWE Sky 1 and 2' then right_broadcast_programmes else 0 end) 
as WWES12_right_broadcast_programmesNon_Live
,sum(case when Live=0 and analysis_right ='WWE Sky Sports' then right_broadcast_programmes else 0 end) 
as WWESS_right_broadcast_programmesNon_Live
--select top 100 * from dbarnett.v250_right_viewable_account_summary_by_live_status
into dbarnett.v250_right_viewable_account_summary_by_live_status
from  dbarnett.v250_days_right_viewable_by_account_by_live_status
group by account_number
;
commit;
--select top 100 * from dbarnett.v250_days_right_viewable_by_account_by_live_status;
commit;

CREATE HG INDEX idx1 ON dbarnett.v250_right_viewable_account_summary (account_number);
CREATE HG INDEX idx1 ON dbarnett.v250_right_viewable_account_summary_by_live_status (account_number);
commit;

--select top 500 * from  dbarnett.v250_master_account_list as a;
--select top 500 * from dbarnett.v250_right_viewable_account_summary_by_live_status;
--select top 500 * from dbarnett.v250_unannualised_right_activity as b
---20140203
alter table dbarnett.v250_master_account_list add account_weight real;
update dbarnett.v250_master_account_list
set account_weight=case when b.account_weight  is null then 0 else b.account_weight end
from dbarnett.v250_master_account_list as a
left outer join dbarnett.v250_annualised_activity_table_for_workshop as b
on a.account_number = b.account_number
;
commit;
