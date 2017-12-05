

---Import in Lookup Table for Account and Lounge Lab ID

--

create table dbarnett.project193_loungelab_account_lookup
(account_number varchar (50)
,lounge_lab_id varchar(50)
)
;
commit;

input into dbarnett.project193_loungelab_account_lookup 
from 'C:\Users\barnetd\Documents\Project V193 - Lounge Lab\Vespa raw Data 20130729\LL Data to Import\LL Data to Import.csv' format ascii;

commit;

--select * from dbarnett.project193_loungelab_account_lookup ;


--Import in Lounge Lab results---
create table dbarnett.project193_loungelab_survey_results
(
panelID varchar(255)
,Gender varchar(255)
,YearBorn varchar(255)
,Age varchar(255)
,Age_Group varchar(255)
,SocGrade varchar(255)
,Children varchar(255)
,CustomerTV varchar(255)
,SkyBasic varchar(255)
,SkyExtra varchar(255)
,SkySports varchar(255)
,SkyMovies varchar(255)
,SkyEverything varchar(255)
,Country varchar(255)
,SocNet varchar(255)
,QHH varchar(255)
,Q1 varchar(255)
,Q2 varchar(255)
,Q2a varchar(255)
,Q3 varchar(255)
,Q4_1 varchar(255)
,Q4_2 varchar(255)
,Q4_3 varchar(255)
,Q4_4 varchar(255)
,Q4_5 varchar(255)
,Q4_6 varchar(255)
,Q5_1 varchar(255)
,Q5_2 varchar(255)
,Q5_3 varchar(255)
,Q5_4 varchar(255)
,Q5_5 varchar(255)
,Q5_6 varchar(255)
,Q5_7 varchar(255)
,Q5_8 varchar(255)
,Q5_9 varchar(255)
,Q5_10 varchar(255)
,Q6_1 varchar(255)
,Q6_2 varchar(255)
,Q6_3 varchar(255)
,Q6_4 varchar(255)
,Q6_5 varchar(255)
,Q6_6 varchar(255)
,Q6_7 varchar(255)
,Q6_8 varchar(255)
,Q6_9 varchar(255)
,Q6_10 varchar(255)
,Q7ScriptingVar varchar(255)
,Q7 varchar(255)
,Q8 varchar(255)
,Q9_01 varchar(255)
,Q9_02 varchar(255)
,Q9_03 varchar(255)
,Q9_04 varchar(255)
,Q9_05 varchar(255)
,Q9_06 varchar(255)
,Q9_07 varchar(255)
,Q9_08 varchar(255)
,Q9_09 varchar(255)
,Q9_10 varchar(255)
,Q9_11 varchar(255)
,Q9_12 varchar(255)
,Q9_13 varchar(255)
,Q9_14 varchar(255)
,Q9_15 varchar(255)
,Q9_16 varchar(255)
,Q9_17 varchar(255)
,Q9_18 varchar(255)
,Q9_19 varchar(255)
,Q9_20 varchar(255)
,Q9_21 varchar(255)
,Q9_22 varchar(255)
,Q9_23 varchar(255)
,Q9_24 varchar(255)
,Q9_25 varchar(255)
,Q9_26 varchar(255)
,Q9_27 varchar(255)
,Q9_28 varchar(255)
,Q9_29 varchar(255)
,Q9_30 varchar(255)
,Q9_31 varchar(255)
,Q9_32 varchar(255)
,Q9_33 varchar(255)
,Q9_34 varchar(255)
,Q9_35 varchar(255)
,Q9_36 varchar(255)
,Q9_37 varchar(255)
,Q9_38 varchar(255)
,Q9_39 varchar(255)
,Q9_40 varchar(255)
,Q9_41 varchar(255)
,Q9_42 varchar(255)
,Q9_43 varchar(255)
,Q9_44 varchar(255)
,Q9_45 varchar(255)
,Q9_46 varchar(255)
,Q9_47 varchar(255)
,Q9_48 varchar(255)
,Q9_49 varchar(255)
,Q9_50 varchar(255)
,Q9_51 varchar(255)
,Q9_52 varchar(255)
,Q9_53 varchar(255)
,Q9_54 varchar(255)
,Q9_55 varchar(255)
,Q9_56 varchar(255)
,Q9_57 varchar(255)
,Q9_58 varchar(255)
,Q9_59 varchar(255)
,Q9_60 varchar(255)
,Q9_61 varchar(255)
,Q9_62 varchar(255)
,Q9_63 varchar(255)
,Q9_64 varchar(255)
,Q9_65 varchar(255)
,Q9_66 varchar(255)
,Q9_67 varchar(255)
,Q9_68 varchar(255)
,Q9_69 varchar(255)
,Q9_70 varchar(255)
,Q9_71 varchar(255)
,Q9_72 varchar(255)
,Q9_73 varchar(255)
,Q9_74 varchar(255)
,Q9_75 varchar(255)
,Q9_76 varchar(255)
,Q9_77 varchar(255)
,Q9_78 varchar(255)
,Q9_79 varchar(255)
,Q10 varchar(255)
,Q11 varchar(255)
,Q12_01 varchar(255)
,Q12_02 varchar(255)
,Q12_03 varchar(255)
,Q12_04 varchar(255)
,Q12_05 varchar(255)
,Q12_06 varchar(255)
,Q12_07 varchar(255)
,Q12_08 varchar(255)
,Q12_09 varchar(255)
,Q12_10 varchar(255)
,Q12_11 varchar(255)
,Q12_12 varchar(255)
,Q12_13 varchar(255)
,Q12_14 varchar(255)
,Q12_15 varchar(255)
,Q12_16 varchar(255)
,Q12_17 varchar(255)
,Q12_18 varchar(255)
,Q12_19 varchar(255)
,Q12_20 varchar(255)
,Q12_21 varchar(255)
,Q12_22 varchar(255)
,Q12_23 varchar(255)
,Q12_24 varchar(255)
,Q12_25 varchar(255)
,Q12_26 varchar(255)
,Q12_27 varchar(255)
,Q12_28 varchar(255)
,Q12_29 varchar(255)
,Q12_30 varchar(255)
,Q12_31 varchar(255)
,Q12_32 varchar(255)
,Q12_33 varchar(255)
,Q12_34 varchar(255)
,Q12_35 varchar(255)
,Q12_36 varchar(255)
,Q12_37 varchar(255)
,Q12_38 varchar(255)
,Q12_39 varchar(255)
,Q12_40 varchar(255)
,Q12_41 varchar(255)
,Q12_42 varchar(255)
,Q13 varchar(255)
,PH1 varchar(255)
,Complete varchar(255)
)
;
commit;

input into dbarnett.project193_loungelab_survey_results
from 'C:\Users\barnetd\Documents\Project V193 - Lounge Lab\Vespa raw Data 20130729\LL Data to Import\LL Survey Results.csv' format ascii;

commit;

---Add on account_number where a match---
alter table dbarnett.project193_loungelab_survey_results add account_number varchar(50);

update dbarnett.project193_loungelab_survey_results
set account_number=b.account_number
from dbarnett.project193_loungelab_survey_results as a
left outer join dbarnett.project193_loungelab_account_lookup as b
on a.panelID=b.lounge_lab_id
;
commit;

alter table dbarnett.project193_loungelab_survey_results add vespa_panel varchar(50);

update      dbarnett.project193_loungelab_survey_results   
set         vespa_panel=b.panel                                     
from        dbarnett.project193_loungelab_survey_results                      as a
inner join   VESPA_ANALYSTS.VESPA_SINGLE_BOX_VIEW       as b
on          a.account_number=b.account_number
;
commit;

---Get List of Account numbers for analysis
select distinct account_number from dbarnett.project193_loungelab_survey_results where account_number is not null;

--select vespa_panel , count(*) from dbarnett.project193_loungelab_survey_results where account_number is not null group by vespa_panel
--select * from dbarnett.project193_loungelab_survey_results
--select * from dbarnett.project193_loungelab_survey_results where q11 like '%Game of Thrones%'

--select 


--select top 100 * from sk_prod.CUST_LIST_MATCHING


---Lounge Lab Vespa Panel Viewing
/*
select * into dbarnett.v191_loungelab_accts_dbarnett from mawbya.v191_loungelab_accts;
commit;
--select * from dbarnett.v191_loungelab_accts_dbarnett;
create unique hg index idx1 on dbarnett.v191_loungelab_accts_dbarnett(account_number);
commit;
*/

--drop table v191_march_viewing; drop table v191_april_viewing; drop table v191_may_viewing; drop table v191_june_viewing; commit;


---Mar 2013
select 
a.account_number
,subscriber_id
,duration
,time_in_seconds_since_recording
,programme_instance_name
,channel_name
,panel_id
,live_recorded
,playback_speed
,reported_playback_speed
,broadcast_start_date_time_utc
,broadcast_end_date_time_utc
,event_start_date_time_utc
,event_end_date_time_utc
,instance_start_date_time_utc
,instance_end_date_time_utc
,capping_end_date_time_utc
,instance_start_date_time_utc as viewing_starts
,case when capping_end_date_time_utc<instance_end_date_time_utc then capping_end_date_time_utc else instance_end_date_time_utc end as viewing_stops
,dateadd(second,time_in_seconds_since_recording*-1,instance_start_date_time_utc) as viewing_broadcast_start_time
,dateadd(second,(datediff(second,viewing_stops,viewing_starts)),instance_start_date_time_utc) as viewing_broadcast_end_time
into v191_march_viewing
from  sk_prod.vespa_dp_prog_viewed_201303 as a
--left outer join dbarnett.v191_loungelab_accts_dbarnett as b
--on a.account_number = b.account_number
where account_number in 
('240035683103'
,'621812536392'
,'210039808360'
,'240029094077'
,'210060078255'
,'621114125290'
,'210094400087'
,'621845401135'
,'240005346897'
,'621018800691'
,'620004797168'
,'210070227488'
,'220008288767'
,'210119436132'
,'210020368010'
,'621024485826'
,'220013742907'
,'210082267233'
,'621076405565'
,'200003249642'
,'621742809356'
,'220011514696'
,'220000191761'
,'220015436946'
,'620032370129'
,'620026441001'
,'621888390641'
,'620057271731'
,'621867553524'
,'630041856083'
,'630135931149'
,'220020679787'
,'210000168736'
,'210000950060'
,'621043505554'
,'220005685684'
,'620013296160'
,'621193769125'
,'240017415193'
,'220017458898'
,'210125862214'
,'210000506363'
,'620018280854'
,'621901446172'
,'220019299951'
,'240009791619'
,'621893233760'
,'210010170079'
,'621429636932'
,'620055254085'
,'621874253696'
,'210015644979'
,'220015835345'
,'210044163140'
,'621032515622'
,'621847861807'
,'620023605012'
,'210036388390'
,'210040006467'
,'620021982231'
,'220015426442'
,'220000822142'
,'620032381456'
,'220000687594'
,'620036895634'
,'621854344101'
,'210037556946'
,'210105218007'
,'630151730771'
,'630141151211'
,'621893588635'
,'210049337830')
and capping_end_date_time_utc >instance_start_date_time_utc
and panel_id = 12
;
commit;


---April
select 
a.account_number
,subscriber_id
,duration
,time_in_seconds_since_recording
,programme_instance_name
,channel_name
,panel_id
,live_recorded
,playback_speed
,reported_playback_speed
,broadcast_start_date_time_utc
,broadcast_end_date_time_utc
,event_start_date_time_utc
,event_end_date_time_utc
,instance_start_date_time_utc
,instance_end_date_time_utc
,capping_end_date_time_utc
,instance_start_date_time_utc as viewing_starts
,case when capping_end_date_time_utc<instance_end_date_time_utc then capping_end_date_time_utc else instance_end_date_time_utc end as viewing_stops
,dateadd(second,time_in_seconds_since_recording*-1,instance_start_date_time_utc) as viewing_broadcast_start_time
,dateadd(second,(datediff(second,viewing_stops,viewing_starts)),instance_start_date_time_utc) as viewing_broadcast_end_time
into v191_april_viewing
from  sk_prod.vespa_dp_prog_viewed_201304 as a
--left outer join dbarnett.v191_loungelab_accts_dbarnett as b
--on a.account_number = b.account_number
where account_number in 
('240035683103'
,'621812536392'
,'210039808360'
,'240029094077'
,'210060078255'
,'621114125290'
,'210094400087'
,'621845401135'
,'240005346897'
,'621018800691'
,'620004797168'
,'210070227488'
,'220008288767'
,'210119436132'
,'210020368010'
,'621024485826'
,'220013742907'
,'210082267233'
,'621076405565'
,'200003249642'
,'621742809356'
,'220011514696'
,'220000191761'
,'220015436946'
,'620032370129'
,'620026441001'
,'621888390641'
,'620057271731'
,'621867553524'
,'630041856083'
,'630135931149'
,'220020679787'
,'210000168736'
,'210000950060'
,'621043505554'
,'220005685684'
,'620013296160'
,'621193769125'
,'240017415193'
,'220017458898'
,'210125862214'
,'210000506363'
,'620018280854'
,'621901446172'
,'220019299951'
,'240009791619'
,'621893233760'
,'210010170079'
,'621429636932'
,'620055254085'
,'621874253696'
,'210015644979'
,'220015835345'
,'210044163140'
,'621032515622'
,'621847861807'
,'620023605012'
,'210036388390'
,'210040006467'
,'620021982231'
,'220015426442'
,'220000822142'
,'620032381456'
,'220000687594'
,'620036895634'
,'621854344101'
,'210037556946'
,'210105218007'
,'630151730771'
,'630141151211'
,'621893588635'
,'210049337830'
)
and capping_end_date_time_utc >instance_start_date_time_utc
and panel_id = 12
;
commit;

---May Viewing

select 
a.account_number
,subscriber_id
,duration
,time_in_seconds_since_recording
,programme_instance_name
,channel_name
,panel_id
,live_recorded
,playback_speed
,reported_playback_speed
,broadcast_start_date_time_utc
,broadcast_end_date_time_utc
,event_start_date_time_utc
,event_end_date_time_utc
,instance_start_date_time_utc
,instance_end_date_time_utc
,capping_end_date_time_utc
,instance_start_date_time_utc as viewing_starts
,case when capping_end_date_time_utc<instance_end_date_time_utc then capping_end_date_time_utc else instance_end_date_time_utc end as viewing_stops
,dateadd(second,time_in_seconds_since_recording*-1,instance_start_date_time_utc) as viewing_broadcast_start_time
,dateadd(second,(datediff(second,viewing_stops,viewing_starts)),instance_start_date_time_utc) as viewing_broadcast_end_time
into v191_may_viewing
from  sk_prod.vespa_dp_prog_viewed_201305 as a
where account_number in 
('240035683103'
,'621812536392'
,'210039808360'
,'240029094077'
,'210060078255'
,'621114125290'
,'210094400087'
,'621845401135'
,'240005346897'
,'621018800691'
,'620004797168'
,'210070227488'
,'220008288767'
,'210119436132'
,'210020368010'
,'621024485826'
,'220013742907'
,'210082267233'
,'621076405565'
,'200003249642'
,'621742809356'
,'220011514696'
,'220000191761'
,'220015436946'
,'620032370129'
,'620026441001'
,'621888390641'
,'620057271731'
,'621867553524'
,'630041856083'
,'630135931149'
,'220020679787'
,'210000168736'
,'210000950060'
,'621043505554'
,'220005685684'
,'620013296160'
,'621193769125'
,'240017415193'
,'220017458898'
,'210125862214'
,'210000506363'
,'620018280854'
,'621901446172'
,'220019299951'
,'240009791619'
,'621893233760'
,'210010170079'
,'621429636932'
,'620055254085'
,'621874253696'
,'210015644979'
,'220015835345'
,'210044163140'
,'621032515622'
,'621847861807'
,'620023605012'
,'210036388390'
,'210040006467'
,'620021982231'
,'220015426442'
,'220000822142'
,'620032381456'
,'220000687594'
,'620036895634'
,'621854344101'
,'210037556946'
,'210105218007'
,'630151730771'
,'630141151211'
,'621893588635'
,'210049337830'
)
and capping_end_date_time_utc >instance_start_date_time_utc
and panel_id = 12
;

commit;

---June Viewing
select 
a.account_number
,subscriber_id
,duration
,time_in_seconds_since_recording
,programme_instance_name
,channel_name
,panel_id
,live_recorded
,playback_speed
,reported_playback_speed
,broadcast_start_date_time_utc
,broadcast_end_date_time_utc
,event_start_date_time_utc
,event_end_date_time_utc
,instance_start_date_time_utc
,instance_end_date_time_utc
,capping_end_date_time_utc
,instance_start_date_time_utc as viewing_starts
,case when capping_end_date_time_utc<instance_end_date_time_utc then capping_end_date_time_utc else instance_end_date_time_utc end as viewing_stops
,dateadd(second,time_in_seconds_since_recording*-1,instance_start_date_time_utc) as viewing_broadcast_start_time
,dateadd(second,(datediff(second,viewing_stops,viewing_starts)),instance_start_date_time_utc) as viewing_broadcast_end_time
into v191_june_viewing
from  sk_prod.vespa_dp_prog_viewed_201306 as a
--left outer join dbarnett.v191_loungelab_accts_dbarnett as b
--on a.account_number = b.account_number
where a.account_number in 
('240035683103'
,'621812536392'
,'210039808360'
,'240029094077'
,'210060078255'
,'621114125290'
,'210094400087'
,'621845401135'
,'240005346897'
,'621018800691'
,'620004797168'
,'210070227488'
,'220008288767'
,'210119436132'
,'210020368010'
,'621024485826'
,'220013742907'
,'210082267233'
,'621076405565'
,'200003249642'
,'621742809356'
,'220011514696'
,'220000191761'
,'220015436946'
,'620032370129'
,'620026441001'
,'621888390641'
,'620057271731'
,'621867553524'
,'630041856083'
,'630135931149'
,'220020679787'
,'210000168736'
,'210000950060'
,'621043505554'
,'220005685684'
,'620013296160'
,'621193769125'
,'240017415193'
,'220017458898'
,'210125862214'
,'210000506363'
,'620018280854'
,'621901446172'
,'220019299951'
,'240009791619'
,'621893233760'
,'210010170079'
,'621429636932'
,'620055254085'
,'621874253696'
,'210015644979'
,'220015835345'
,'210044163140'
,'621032515622'
,'621847861807'
,'620023605012'
,'210036388390'
,'210040006467'
,'620021982231'
,'220015426442'
,'220000822142'
,'620032381456'
,'220000687594'
,'620036895634'
,'621854344101'
,'210037556946'
,'210105218007'
,'630151730771'
,'630141151211'
,'621893588635'
,'210049337830'
)
and capping_end_date_time_utc >instance_start_date_time_utc
and panel_id = 12
;
commit;

/*
select count (distinct account_number) from v191_march_viewing; 17
select count (distinct account_number) from v191_april_viewing;  --20
select count (distinct account_number) from v191_may_viewing;  --21
select count (distinct account_number) from v191_june_viewing; -- 25

*/

---Group Together by account and Viewing Date

--drop table v191_combined_viewing_by_account;
---Add March Data--

select account_number
,dateformat(viewing_starts,'YYYY-MM-DD') as viewing_date
into v191_combined_viewing_by_account
from v191_march_viewing
where dateformat(viewing_starts,'HH') not in ('00','01','02','03','04','05')
group by account_number
,viewing_date
;
insert  into v191_combined_viewing_by_account
select account_number
,dateformat(viewing_starts,'YYYY-MM-DD') as viewing_date

from v191_april_viewing
where dateformat(viewing_starts,'HH') not in ('00','01','02','03','04','05')
group by account_number
,viewing_date
;

insert  into v191_combined_viewing_by_account
select account_number
,dateformat(viewing_starts,'YYYY-MM-DD') as viewing_date
from v191_may_viewing
where dateformat(viewing_starts,'HH') not in ('00','01','02','03','04','05')
group by account_number
,viewing_date
;


insert  into v191_combined_viewing_by_account
select account_number
,dateformat(viewing_starts,'YYYY-MM-DD') as viewing_date
from v191_june_viewing
where dateformat(viewing_starts,'HH') not in ('00','01','02','03','04','05')
group by account_number
,viewing_date
;





--drop table #summary_by_account;
select account_number
, count(distinct viewing_date) as days_viewing
into #summary_by_account
from v191_combined_viewing_by_account
group by account_number
;


select days_viewing
,count(*) as accounts
from #summary_by_account
group by days_viewing
order by days_viewing
;

--drop table #summary_by_viewing_date;
select viewing_date
, count(distinct account_number) as accounts_viewing
into #summary_by_viewing_date
from v191_combined_viewing_by_account
group by viewing_date
;



select * from #summary_by_viewing_date
order by viewing_date
;

----Test of count by day in June---
select dateformat(instance_start_date_time_utc,'YYYY-MM-DD') as viewing_date
,count(*) as records
into #count_by_day
from sk_prod.vespa_dp_prog_viewed_201306
group by viewing_date
order by viewing_date
;
commit;

select * from  #count_by_day;

commit;

select top 500 account_number, instance_start_date_time_utc ,instance_end_date_time_utc  from sk_prod.vespa_dp_prog_viewed_201305
where dateformat(instance_start_date_time_utc,'YYYY-MM-DD HH') = '2013-05-31 23'
;

select  account_number,subscriber_id, instance_start_date_time_utc ,instance_end_date_time_utc,channel_name  from sk_prod.vespa_dp_prog_viewed_201305
where dateformat(instance_start_date_time_utc,'YYYY-MM-DD HH') = '2013-05-31 23'
and account_number ='210083669023'
order by instance_start_date_time_utc;



select  *  from sk_prod.vespa_dp_prog_viewed_201305
where dateformat(instance_start_date_time_utc,'YYYY-MM-DD HH') = '2013-05-31 23'
and account_number ='210083669023'
order by instance_start_date_time_utc;

commit;

--Hard code accounts

--delete capped records

--select count(distinct account_number) from v191_may_viewing;
--drop table v191_may_viewing; commit;



/*

select top 500 account_number from sk_prod.vespa_dp_prog_viewed_201305


select * from  sk_prod.vespa_dp_prog_viewed_201305 where account_number = '620003601411' order by instance_start_date_time_utc

select 
account_number
,subscriber_id
,duration
,time_in_seconds_since_recording
,programme_instance_name
,channel_name
,panel_id
,live_recorded
,playback_speed
,reported_playback_speed
,broadcast_start_date_time_utc
,broadcast_end_date_time_utc
,event_start_date_time_utc
,event_end_date_time_utc
,instance_start_date_time_utc
,instance_end_date_time_utc
,capping_end_date_time_utc

from  sk_prod.vespa_dp_prog_viewed_201305 where account_number = '240028852996' order by instance_start_date_time_utc
;


commit;





select panel_id , count(*) from  sk_prod.vespa_dp_prog_viewed_201305 group by panel_id

select count(*) from  sk_prod.vespa_dp_prog_viewed_201305 where capping_end_date_time_utc is not null
select count(*) from  sk_prod.vespa_dp_prog_viewed_201305 

select count(*) from  sk_prod.vespa_dp_prog_viewed_201305 where capping_end_date_time_utc <instance_end_date_time_utc

select datediff(minute,capping_end_date_time_utc,event_start_date_time_utc) as time_diff ,count(*) from  sk_prod.vespa_dp_prog_viewed_201305
where capping_end_date_time_utc <instance_start_date_time_utc group by time_diff order by time_diff

commit;




select count(*) from  sk_prod.vespa_dp_prog_viewed_201305 where account_number = '620003601411' and capping_end_date_time_utc is not null


select top 100 account_number from  sk_prod.vespa_dp_prog_viewed_201305 where capping_end_date_time_utc is not null

select panel_id , count(*)  from sk_prod.VESPA_STB_LOG_SUMMARY group by panel_id; 



*/
