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


--select top 100 * from dbarnett.v250_days_right_viewable_by_account_v09;

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


