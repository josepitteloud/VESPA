drop table dbarnett.v250_days_right_viewable_by_account_by_live_status 
drop table dbarnett.v250_days_right_viewable_by_account_by_live_status_00_to_01;
drop table dbarnett.v250_days_right_viewable_by_account_by_live_status_02_to_03;
drop table dbarnett.v250_days_right_viewable_by_account_by_live_status_04_to_05;
drop table dbarnett.v250_days_right_viewable_by_account_by_live_status_06_to_07;
drop table dbarnett.v250_days_right_viewable_by_account_by_live_status_08_to_09;

--select count(*) from dbarnett.v250_days_right_viewable_by_account_by_live_status_00_to_01

select * into dbarnett.v250_days_viewing_by_account_00_to_01
from dbarnett.v250_days_viewing_by_account
where right(account_number,1) in ('0','1')
;

commit;

commit;
CREATE HG INDEX idx1 ON dbarnett.v250_days_viewing_by_account_00_to_01 (account_number);
CREATE LF INDEX idx3 ON dbarnett.v250_days_viewing_by_account_00_to_01 (viewing_date);
commit;
--select top 100 *  from dbarnett.v250_rights_broadcast_by_live_status;
select a.account_number
,b.analysis_right
,b.live
,sum(days_with_viewing) as days_right_viewable
into dbarnett.v250_days_right_viewable_by_account_by_live_status_00_to_01
from dbarnett.v250_rights_broadcast_by_live_status as b 
left outer join dbarnett.v250_days_viewing_by_account_00_to_01 as a
on b.broadcast_date=a.viewing_date
group by  a.account_number
,b.analysis_right
,b.live
;
commit;
--select count(*) from  dbarnett.v250_days_right_viewable_by_account_by_live_status_06_to_07;
drop table dbarnett.v250_days_viewing_by_account_00_to_01;


select * into dbarnett.v250_days_viewing_by_account_02_to_03
from dbarnett.v250_days_viewing_by_account
where right(account_number,1) in ('2','3')
;

commit;

commit;
CREATE HG INDEX idx1 ON dbarnett.v250_days_viewing_by_account_02_to_03 (account_number);
CREATE LF INDEX idx3 ON dbarnett.v250_days_viewing_by_account_02_to_03 (viewing_date);
commit;
--select count(*) from dbarnett.v250_days_viewing_by_account_02_to_03;
--drop table dbarnett.v250_days_right_viewable_by_account_by_live_status_02_to_03;
select a.account_number
,b.analysis_right
,b.live
,sum(days_with_viewing) as days_right_viewable
into dbarnett.v250_days_right_viewable_by_account_by_live_status_02_to_03
from dbarnett.v250_rights_broadcast_by_live_status as b 
left outer join dbarnett.v250_days_viewing_by_account_02_to_03 as a
on b.broadcast_date=a.viewing_date
group by  a.account_number
,b.analysis_right
,b.live
;
commit;
--select count(*) from dbarnett.v250_days_right_viewable_by_account_by_live_status_02_to_03;
drop table dbarnett.v250_days_viewing_by_account_02_to_03;




select * into dbarnett.v250_days_viewing_by_account_04_to_05
from dbarnett.v250_days_viewing_by_account
where right(account_number,1) in ('4','5')
;

commit;

commit;
CREATE HG INDEX idx1 ON dbarnett.v250_days_viewing_by_account_04_to_05 (account_number);
CREATE LF INDEX idx3 ON dbarnett.v250_days_viewing_by_account_04_to_05 (viewing_date);
commit;

select a.account_number
,b.analysis_right
,b.live
,sum(days_with_viewing) as days_right_viewable
into dbarnett.v250_days_right_viewable_by_account_by_live_status_04_to_05
from dbarnett.v250_rights_broadcast_by_live_status as b 
left outer join dbarnett.v250_days_viewing_by_account_04_to_05 as a
on b.broadcast_date=a.viewing_date
group by  a.account_number
,b.analysis_right
,b.live
;
commit;

drop table dbarnett.v250_days_viewing_by_account_04_to_05;


--select count(*) from dbarnett.v250_days_viewing_by_account_06_to_07;
select * into dbarnett.v250_days_viewing_by_account_06_to_07
from dbarnett.v250_days_viewing_by_account
where right(account_number,1) in ('6','7')
;

commit;

commit;
CREATE HG INDEX idx1 ON dbarnett.v250_days_viewing_by_account_06_to_07 (account_number);
CREATE LF INDEX idx3 ON dbarnett.v250_days_viewing_by_account_06_to_07 (viewing_date);
commit;
--drop table dbarnett.v250_days_right_viewable_by_account_by_live_status_06_to_07;
select a.account_number
,b.analysis_right
,b.live
,sum(days_with_viewing) as days_right_viewable
into dbarnett.v250_days_right_viewable_by_account_by_live_status_06_to_07
from dbarnett.v250_rights_broadcast_by_live_status as b 
left outer join dbarnett.v250_days_viewing_by_account_06_to_07 as a
on b.broadcast_date=a.viewing_date
group by  a.account_number
,b.analysis_right
,b.live
;
commit;

drop table dbarnett.v250_days_viewing_by_account_06_to_07;

--select count(*) from dbarnett.v250_days_right_viewable_by_account_by_live_status;


select * into dbarnett.v250_days_viewing_by_account_08_to_09
from dbarnett.v250_days_viewing_by_account
where right(account_number,1) in ('8','9')
;

commit;

commit;
CREATE HG INDEX idx1 ON dbarnett.v250_days_viewing_by_account_08_to_09 (account_number);
CREATE LF INDEX idx3 ON dbarnett.v250_days_viewing_by_account_08_to_09 (viewing_date);
commit;

select a.account_number
,b.analysis_right
,b.live
,sum(days_with_viewing) as days_right_viewable
into dbarnett.v250_days_right_viewable_by_account_by_live_status_08_to_09
from dbarnett.v250_rights_broadcast_by_live_status as b 
left outer join dbarnett.v250_days_viewing_by_account_08_to_09 as a
on b.broadcast_date=a.viewing_date
group by  a.account_number
,b.analysis_right
,b.live
;
commit;

drop table dbarnett.v250_days_viewing_by_account_08_to_09;

---Append All Together----
select * into dbarnett.v250_days_right_viewable_by_account_by_live_status from  
dbarnett.v250_days_right_viewable_by_account_by_live_status_00_to_01;

insert into dbarnett.v250_days_right_viewable_by_account_by_live_status
(select * from dbarnett.v250_days_right_viewable_by_account_by_live_status_02_to_03);


insert into dbarnett.v250_days_right_viewable_by_account_by_live_status
(select * from dbarnett.v250_days_right_viewable_by_account_by_live_status_04_to_05);


insert into dbarnett.v250_days_right_viewable_by_account_by_live_status
(select * from dbarnett.v250_days_right_viewable_by_account_by_live_status_06_to_07);


insert into dbarnett.v250_days_right_viewable_by_account_by_live_status
(select * from dbarnett.v250_days_right_viewable_by_account_by_live_status_08_to_09);

commit;


select count(*) from dbarnett.v250_days_right_viewable_by_account_by_live_status;






