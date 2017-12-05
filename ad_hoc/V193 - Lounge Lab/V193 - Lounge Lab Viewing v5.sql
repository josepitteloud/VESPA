

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

---Add on extra accounts from July 12th Data
create table dbarnett.project193_loungelab_account_lookup_jul_12th
(account_number varchar (50)
,lounge_lab_id varchar(50)
)
;
commit;

input into dbarnett.project193_loungelab_account_lookup_jul_12th 
from 'C:\Users\barnetd\Documents\Project V193 - Lounge Lab\Vespa raw Data 20130729\LL Data to Import\LL Data to Import July 12th.csv' format ascii;

commit;

---Add New Accounts on to list--

select a.account_number
,a.lounge_lab_id
into #new_records
from dbarnett.project193_loungelab_account_lookup_jul_12th as a
left outer join dbarnett.project193_loungelab_account_lookup as b
on a.account_number = b.account_number
where b.account_number is null
;

insert into dbarnett.project193_loungelab_account_lookup

select  a.account_number
,a.lounge_lab_id
from #new_records as a
;
commit;
--drop table dbarnett.project193_loungelab_survey_results;
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
,Q7 varchar(600)
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
,Q7_01 Varchar (255),
Q7_02 Varchar (255),
Q7_03 Varchar (255),
Q7_04 Varchar (255),
Q7_05 Varchar (255),
Q7_06 Varchar (255),
Q7_07 Varchar (255),
Q7_08 Varchar (255),
Q7_09 Varchar (255),
Q7_10 Varchar (255),
Q7_11 Varchar (255),
Q7_12 Varchar (255),
Q7_13 Varchar (255),
Q7_14 Varchar (255),
Q7_15 Varchar (255),
Q7_16 Varchar (255),
Q7_17 Varchar (255),
Q7_18 Varchar (255),
Q7_19 Varchar (255),
Q7_20 Varchar (255),
Q7_21 Varchar (255),
Q7_22 Varchar (255),
Q7_23 Varchar (255),
Q7_24 Varchar (255),
Q7_25 Varchar (255),
Q7_26 Varchar (255),
Q7_27 Varchar (255),
Q7_28 Varchar (255),
Q7_29 Varchar (255),
Q7_30 Varchar (255),
Q7_31 Varchar (255),
Q7_32 Varchar (255),
Q7_33 Varchar (255),
Q7_34 Varchar (255),
Q7_35 Varchar (255),
Q7_36 Varchar (255),
Q7_37 Varchar (255),
Q7_38 Varchar (255),
Q7_39 Varchar (255),
Q7_40 Varchar (255),
Q7_41 Varchar (255),
Q7_42 Varchar (255),
Q7_43 Varchar (255),
Q7_44 Varchar (255),
Q7_45 Varchar (255),
Q7_46 Varchar (255),
Q7_47 Varchar (255),
Q7_48 Varchar (255),
Q7_49 Varchar (255),
Q7_50 Varchar (255),
Q7_51 Varchar (255),
Q7_52 Varchar (255),
Q7_53 Varchar (255),
Q8_01 Varchar (255),
Q8_02 Varchar (255),
Q8_03 Varchar (255),
Q8_04 Varchar (255),
Q8_05 Varchar (255),
Q8_06 Varchar (255),
Q8_07 Varchar (255),
Q8_08 Varchar (255),
Q8_09 Varchar (255),
Q8_10 Varchar (255),
Q8_11 Varchar (255),
Q8_12 Varchar (255),
Q8_13 Varchar (255),
Q8_14 Varchar (255),
Q8_15 Varchar (255),
Q8_16 Varchar (255),
Q8_17 Varchar (255),
Q8_18 Varchar (255),
Q8_19 Varchar (255),
Q8_20 Varchar (255),
Q10_01 Varchar (255),
Q10_02 Varchar (255),
Q10_03 Varchar (255),
Q10_04 Varchar (255),
Q10_05 Varchar (255),
Q10_06 Varchar (255),
Q10_07 Varchar (255),
Q10_08 Varchar (255),
Q10_09 Varchar (255),
Q10_10 Varchar (255),
Q10_11 Varchar (255),
Q10_12 Varchar (255),
Q10_13 Varchar (255),
Q10_14 Varchar (255),
Q10_15 Varchar (255),
Q10_16 Varchar (255),
Q10_17 Varchar (255),
Q10_18 Varchar (255),
Q10_19 Varchar (255),
Q10_20 Varchar (255)
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
--select * from VESPA_ANALYSTS.VESPA_SINGLE_BOX_VIEW;
---Get List of Account numbers for analysis
select distinct account_number from dbarnett.project193_loungelab_survey_results where account_number is not null;
grant all on dbarnett.project193_loungelab_survey_results to public;
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
('630002857641',
'240035683103',
'621812536392',
'210039808360',
'210040565645',
'621043177651',
'220011515537',
'210098638567',
'240029094077',
'210072839751',
'620007873362',
'210060078255',
'621130172664',
'621114125290',
'200007739713',
'210094400087',
'621845401135',
'240005346897',
'621018800691',
'620004797168',
'210070227488',
'220025300223',
'220008288767',
'210119436132',
'210020368010',
'621358947276',
'621024485826',
'220013742907',
'620043573828',
'621643875993',
'210082267233',
'621076405565',
'630009074497',
'620037691016',
'621011541482',
'621026591795',
'200003249642',
'621742809356',
'220011514696',
'621240843535',
'220000191761',
'220015436946',
'620032370129',
'620026441001',
'621888390641',
'620057271731',
'621867553524',
'630041856083',
'630135931149',
'220020679787',
'210000168736',
'210000950060',
'621043505554',
'621043879439',
'220005685684',
'220013716422',
'620013296160',
'621193769125',
'220018110282',
'240017415193',
'220017458898',
'210094296527',
'210125862214',
'210000506363',
'620012338732',
'620018280854',
'621901446172',
'220019299951',
'621392362128',
'240009791619',
'630027931827',
'621893233760',
'210010170079',
'621429636932',
'210075522669',
'620024426806',
'620055254085',
'621874253696',
'220005523067',
'210015644979',
'220015835345',
'210044163140',
'621032515622',
'621847861807',
'210145295114',
'620038519265',
'210037414724',
'620023605012',
'210036388390',
'210040006467',
'620021982231',
'210146497453',
'220015426442',
'210000814743',
'220000822142',
'620032381456',
'210124147732',
'220000687594',
'630060016924',
'620036895634',
'621854344101',
'210113818145',
'210037556946',
'210105218007',
'630105944338',
'630151730771',
'630141151211',
'621893588635',
'210049337830'
)
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
('630002857641',
'240035683103',
'621812536392',
'210039808360',
'210040565645',
'621043177651',
'220011515537',
'210098638567',
'240029094077',
'210072839751',
'620007873362',
'210060078255',
'621130172664',
'621114125290',
'200007739713',
'210094400087',
'621845401135',
'240005346897',
'621018800691',
'620004797168',
'210070227488',
'220025300223',
'220008288767',
'210119436132',
'210020368010',
'621358947276',
'621024485826',
'220013742907',
'620043573828',
'621643875993',
'210082267233',
'621076405565',
'630009074497',
'620037691016',
'621011541482',
'621026591795',
'200003249642',
'621742809356',
'220011514696',
'621240843535',
'220000191761',
'220015436946',
'620032370129',
'620026441001',
'621888390641',
'620057271731',
'621867553524',
'630041856083',
'630135931149',
'220020679787',
'210000168736',
'210000950060',
'621043505554',
'621043879439',
'220005685684',
'220013716422',
'620013296160',
'621193769125',
'220018110282',
'240017415193',
'220017458898',
'210094296527',
'210125862214',
'210000506363',
'620012338732',
'620018280854',
'621901446172',
'220019299951',
'621392362128',
'240009791619',
'630027931827',
'621893233760',
'210010170079',
'621429636932',
'210075522669',
'620024426806',
'620055254085',
'621874253696',
'220005523067',
'210015644979',
'220015835345',
'210044163140',
'621032515622',
'621847861807',
'210145295114',
'620038519265',
'210037414724',
'620023605012',
'210036388390',
'210040006467',
'620021982231',
'210146497453',
'220015426442',
'210000814743',
'220000822142',
'620032381456',
'210124147732',
'220000687594',
'630060016924',
'620036895634',
'621854344101',
'210113818145',
'210037556946',
'210105218007',
'630105944338',
'630151730771',
'630141151211',
'621893588635',
'210049337830'

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
('630002857641',
'240035683103',
'621812536392',
'210039808360',
'210040565645',
'621043177651',
'220011515537',
'210098638567',
'240029094077',
'210072839751',
'620007873362',
'210060078255',
'621130172664',
'621114125290',
'200007739713',
'210094400087',
'621845401135',
'240005346897',
'621018800691',
'620004797168',
'210070227488',
'220025300223',
'220008288767',
'210119436132',
'210020368010',
'621358947276',
'621024485826',
'220013742907',
'620043573828',
'621643875993',
'210082267233',
'621076405565',
'630009074497',
'620037691016',
'621011541482',
'621026591795',
'200003249642',
'621742809356',
'220011514696',
'621240843535',
'220000191761',
'220015436946',
'620032370129',
'620026441001',
'621888390641',
'620057271731',
'621867553524',
'630041856083',
'630135931149',
'220020679787',
'210000168736',
'210000950060',
'621043505554',
'621043879439',
'220005685684',
'220013716422',
'620013296160',
'621193769125',
'220018110282',
'240017415193',
'220017458898',
'210094296527',
'210125862214',
'210000506363',
'620012338732',
'620018280854',
'621901446172',
'220019299951',
'621392362128',
'240009791619',
'630027931827',
'621893233760',
'210010170079',
'621429636932',
'210075522669',
'620024426806',
'620055254085',
'621874253696',
'220005523067',
'210015644979',
'220015835345',
'210044163140',
'621032515622',
'621847861807',
'210145295114',
'620038519265',
'210037414724',
'620023605012',
'210036388390',
'210040006467',
'620021982231',
'210146497453',
'220015426442',
'210000814743',
'220000822142',
'620032381456',
'210124147732',
'220000687594',
'630060016924',
'620036895634',
'621854344101',
'210113818145',
'210037556946',
'210105218007',
'630105944338',
'630151730771',
'630141151211',
'621893588635',
'210049337830'

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
('630002857641',
'240035683103',
'621812536392',
'210039808360',
'210040565645',
'621043177651',
'220011515537',
'210098638567',
'240029094077',
'210072839751',
'620007873362',
'210060078255',
'621130172664',
'621114125290',
'200007739713',
'210094400087',
'621845401135',
'240005346897',
'621018800691',
'620004797168',
'210070227488',
'220025300223',
'220008288767',
'210119436132',
'210020368010',
'621358947276',
'621024485826',
'220013742907',
'620043573828',
'621643875993',
'210082267233',
'621076405565',
'630009074497',
'620037691016',
'621011541482',
'621026591795',
'200003249642',
'621742809356',
'220011514696',
'621240843535',
'220000191761',
'220015436946',
'620032370129',
'620026441001',
'621888390641',
'620057271731',
'621867553524',
'630041856083',
'630135931149',
'220020679787',
'210000168736',
'210000950060',
'621043505554',
'621043879439',
'220005685684',
'220013716422',
'620013296160',
'621193769125',
'220018110282',
'240017415193',
'220017458898',
'210094296527',
'210125862214',
'210000506363',
'620012338732',
'620018280854',
'621901446172',
'220019299951',
'621392362128',
'240009791619',
'630027931827',
'621893233760',
'210010170079',
'621429636932',
'210075522669',
'620024426806',
'620055254085',
'621874253696',
'220005523067',
'210015644979',
'220015835345',
'210044163140',
'621032515622',
'621847861807',
'210145295114',
'620038519265',
'210037414724',
'620023605012',
'210036388390',
'210040006467',
'620021982231',
'210146497453',
'220015426442',
'210000814743',
'220000822142',
'620032381456',
'210124147732',
'220000687594',
'630060016924',
'620036895634',
'621854344101',
'210113818145',
'210037556946',
'210105218007',
'630105944338',
'630151730771',
'630141151211',
'621893588635',
'210049337830'

)
and capping_end_date_time_utc >instance_start_date_time_utc
and panel_id = 12
;
commit;

/*
select count (distinct account_number) from v191_march_viewing; 31
select count (distinct account_number) from v191_april_viewing;  --34
select count (distinct account_number) from v191_may_viewing;  --35
select count (distinct account_number) from v191_june_viewing; -- 39

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

---Add Days Viewing Back to Survey Data--

alter table dbarnett.project193_loungelab_survey_results add days_viewing INTEGER;

update dbarnett.project193_loungelab_survey_results
set days_viewing=b.days_viewing
from dbarnett.project193_loungelab_survey_results as a
left outer join #summary_by_account as b
on a.account_number=b.account_number
;
commit;


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
commit;

--select * from dbarnett.project193_loungelab_survey_results;

----PART II - Survey Analysis----

select Age_Group
,count(*) as responses
,sum(case when days_viewing is not null then 1 else 0 end) as on_vespa
from  dbarnett.project193_loungelab_survey_results
group by age_group
order by age_group
;


select SkyBasic	,SkyExtra,	SkySports,	SkyMovies,	SkyEverything
,count(*) as responses
,sum(case when days_viewing is not null then 1 else 0 end) as on_vespa
from  dbarnett.project193_loungelab_survey_results
group by SkyBasic	,SkyExtra,	SkySports,	SkyMovies,	SkyEverything
order by SkyBasic	,SkyExtra,	SkySports,	SkyMovies,	SkyEverything
;

select qhh
,count(*) as responses
,sum(case when days_viewing is not null then 1 else 0 end) as on_vespa
from  dbarnett.project193_loungelab_survey_results
group by qhh
order by on_vespa desc
;


select children
,count(*) as responses
,sum(case when days_viewing is not null then 1 else 0 end) as on_vespa
from  dbarnett.project193_loungelab_survey_results
group by children
order by on_vespa desc
;


commit;


select socgrade
,count(*) as responses
,sum(case when days_viewing is not null then 1 else 0 end) as on_vespa
from  dbarnett.project193_loungelab_survey_results
group by socgrade
order by socgrade 
;



select q2a as tenure
,count(*) as responses
,sum(case when days_viewing is not null then 1 else 0 end) as on_vespa
from  dbarnett.project193_loungelab_survey_results
group by tenure
order by tenure 
;


select q13 as likelihood_to_stay
,count(*) as responses
,sum(case when days_viewing is not null then 1 else 0 end) as on_vespa
from  dbarnett.project193_loungelab_survey_results
group by likelihood_to_stay
order by likelihood_to_stay 
;

commit;


---Question 9 - Favourite Channels---

select account_number

---Top 10 
,case when q9_01 is not null then 1 else 0 end as Top_10_4Seven
,case when q9_02 is not null then 1 else 0 end as Top_10_5_USA
,case when q9_03 is not null then 1 else 0 end as Top_10_5Star
,case when q9_04 is not null then 1 else 0 end as Top_10_Alibi
,case when q9_05 is not null then 1 else 0 end as Top_10_Animal_Planet
,case when q9_06 is not null then 1 else 0 end as Top_10_BBC_1
,case when q9_07 is not null then 1 else 0 end as Top_10_BBC_News
,case when q9_08 is not null then 1 else 0 end as Top_10_BBC2
,case when q9_09 is not null then 1 else 0 end as Top_10_BBC3
,case when q9_10 is not null then 1 else 0 end as Top_10_BBC4
,case when q9_11 is not null then 1 else 0 end as Top_10_Bio
,case when q9_12 is not null then 1 else 0 end as Top_10_Cartoon_Network
,case when q9_13 is not null then 1 else 0 end as Top_10_CBBC
,case when q9_14 is not null then 1 else 0 end as Top_10_CBeebies
,case when q9_15 is not null then 1 else 0 end as Top_10_Challenge
,case when q9_16 is not null then 1 else 0 end as Top_10_Channel_4
,case when q9_17 is not null then 1 else 0 end as Top_10_Channel_5
,case when q9_18 is not null then 1 else 0 end as Top_10_Comedy_Central
,case when q9_19 is not null then 1 else 0 end as Top_10_Crime___Investigation_Network
,case when q9_20 is not null then 1 else 0 end as Top_10_Dave
,case when q9_21 is not null then 1 else 0 end as Top_10_Discovery_Channel
,case when q9_22 is not null then 1 else 0 end as Top_10_Discovery_History
,case when q9_23 is not null then 1 else 0 end as Top_10_Discovery_Home___Health
,case when q9_24 is not null then 1 else 0 end as Top_10_Discovery_Science
,case when q9_25 is not null then 1 else 0 end as Top_10_Discovery_Shed
,case when q9_26 is not null then 1 else 0 end as Top_10_Discovery_Turbo
,case when q9_27 is not null then 1 else 0 end as Top_10_Disney_Channel
,case when q9_28 is not null then 1 else 0 end as Top_10_Disney_Junior
,case when q9_29 is not null then 1 else 0 end as Top_10_Disney_XD
,case when q9_30 is not null then 1 else 0 end as Top_10_DMAX
,case when q9_31 is not null then 1 else 0 end as Top_10_Drama
,case when q9_32 is not null then 1 else 0 end as Top_10_E_Entertainment
,case when q9_33 is not null then 1 else 0 end as Top_10_E4
,case when q9_34 is not null then 1 else 0 end as Top_10_Eden
,case when q9_35 is not null then 1 else 0 end as Top_10_ESPN_
,case when q9_36 is not null then 1 else 0 end as Top_10_ESPN_Classic
,case when q9_37 is not null then 1 else 0 end as Top_10_Eurosport
,case when q9_38 is not null then 1 else 0 end as Top_10_Extreme_Sports
,case when q9_39 is not null then 1 else 0 end as Top_10_Film4
,case when q9_40 is not null then 1 else 0 end as Top_10_FOX
,case when q9_41 is not null then 1 else 0 end as Top_10_G_O_L_D
,case when q9_42 is not null then 1 else 0 end as Top_10_Good_Food
,case when q9_43 is not null then 1 else 0 end as Top_10_H2
,case when q9_44 is not null then 1 else 0 end as Top_10_History_Channel
,case when q9_45 is not null then 1 else 0 end as Top_10_Home
,case when q9_46 is not null then 1 else 0 end as Top_10_Investigation_Discovery
,case when q9_47 is not null then 1 else 0 end as Top_10_ITV1
,case when q9_48 is not null then 1 else 0 end as Top_10_ITV2
,case when q9_49 is not null then 1 else 0 end as Top_10_ITV3
,case when q9_50 is not null then 1 else 0 end as Top_10_ITV4
,case when q9_51 is not null then 1 else 0 end as Top_10_More4
,case when q9_52 is not null then 1 else 0 end as Top_10_MTV
,case when q9_53 is not null then 1 else 0 end as Top_10_Nat_Geo
,case when q9_54 is not null then 1 else 0 end as Top_10_Nat_Geo_Wild
,case when q9_55 is not null then 1 else 0 end as Top_10_Nick_Jr
,case when q9_56 is not null then 1 else 0 end as Top_10_Nickelodeon
,case when q9_57 is not null then 1 else 0 end as Top_10_PBS_America
,case when q9_58 is not null then 1 else 0 end as Top_10_Pick_TV
,case when q9_59 is not null then 1 else 0 end as Top_10_QVC
,case when q9_60 is not null then 1 else 0 end as Top_10_Really
,case when q9_61 is not null then 1 else 0 end as Top_10_Sky_1
,case when q9_62 is not null then 1 else 0 end as Top_10_Sky_2
,case when q9_63 is not null then 1 else 0 end as Top_10_Sky_Arts
,case when q9_64 is not null then 1 else 0 end as Top_10_Sky_Atlantic
,case when q9_65 is not null then 1 else 0 end as Top_10_Sky_Living
,case when q9_66 is not null then 1 else 0 end as Top_10_Sky_Movies
,case when q9_67 is not null then 1 else 0 end as Top_10_Sky_News
,case when q9_68 is not null then 1 else 0 end as Top_10_Sky_Sports
,case when q9_69 is not null then 1 else 0 end as Top_10_Sky_Sports_F1
,case when q9_70 is not null then 1 else 0 end as Top_10_Sky_Sports_News
,case when q9_71 is not null then 1 else 0 end as Top_10_Star_Life_OK
,case when q9_72 is not null then 1 else 0 end as Top_10_Star_Plus
,case when q9_73 is not null then 1 else 0 end as Top_10_Syfy
,case when q9_74 is not null then 1 else 0 end as Top_10_TCM
,case when q9_75 is not null then 1 else 0 end as Top_10_The_Food_Network
,case when q9_76 is not null then 1 else 0 end as Top_10_TLC
,case when q9_77 is not null then 1 else 0 end as Top_10_Universal
,case when q9_78 is not null then 1 else 0 end as Top_10_Watch
,case when q9_79 is not null then 1 else 0 end as Top_10_Yesterday

from dbarnett.project193_loungelab_survey_results
--group by account_number
where days_viewing is not null
;

select * from dbarnett.project193_loungelab_survey_results where q12_42 is not null

commit;


--Create total details from viewing----

select * into v193_full_viewing from v191_march_viewing;

insert into v193_full_viewing
select * from v191_april_viewing
;

insert into v193_full_viewing
select * from v191_may_viewing
;

insert into v193_full_viewing
select * from v191_june_viewing
;

commit;
---Create Channel_Lookup
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
--drop table #summary_by_account_and_channel;
select case when a.channel_name in (
'BBC 1'
,'BBC 1 South East'
,'BBC 1 North West'
,'BBC 1 West Midlands'
,'BBC 1 Yorkshire'
,'BBC 1 North East & Cumbria'
,'BBC 1 Yorkshire & Lincolnshire'
,'BBC 1 East') then 'BBC ONE'


when a.channel_name in ('BBC2') then 'BBC TWO'
when a.channel_name in  ('Channel 5 Part Network') then 'Channel 5'
when a.channel_name in ('ITV1 STV Scotland Central'
,'ITV1 Meridian South'
,'ITV1 Meridian South East'
,'ITV1 HD South East'
,'ITV1 Yorkshire West'
,'ITV1 Central West'
,'ITV1 Meridian North'
,'ITV1 Anglia West'
,'ITV1 STV East') then 'ITV1'
when a.channel_name in  ('Channel 4+1 Scotland'
,'Channel 4+1 North'
,'Channel 4+1 London'
,'Channel 4+1 South') then 'Channel 4' 

  when channel_name_inc_hd_staggercast in ('Nick Jr','Nick Jr 2') then  'Nick Jr'
  when channel_name_inc_hd_staggercast in ('Sky Living','Sky Livingit') then  'Sky Living'
  when channel_name_inc_hd_staggercast in ('Comedy Central','Comedy Central Extra') then  'Comedy Central'
  when channel_name_inc_hd_staggercast in ('Eurosport','Eurosport2') then  'Eurosport'
  when channel_name_inc_hd_staggercast in ('More4','More 4') then  'More4'
when channel_name_inc_hd_staggercast in ('Sky Arts 1','Sky Arts 2') then  'Sky Arts 1 or 2'
--when channel_name_inc_hd_staggercast in ('Sky 1','Sky 2') then  'Sky 1 or 2'
when channel_name_inc_hd_staggercast in ('Sky Sports 1'
,'Sky Sports 2'
,'Sky Sports 3'
,'Sky Sports 4'
,'Football First') then  'Sky Sports'
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
,'Sky Drama & Romance'
,'Sky Oscars'
) then  'Sky Movies'
 else  channel_name_inc_hd_staggercast end as channel_name_inc_hd_staggercast_channel_families

,account_number
,sum(datediff(second,viewing_starts,viewing_stops)) as seconds_viewed
into #summary_by_account_and_channel
from v193_full_viewing as a
left outer join #channel_lookup as b
on a.channel_name=b.channel_name
group by account_number ,channel_name_inc_hd_staggercast_channel_families
order by seconds_viewed desc
;

---Get 10 Most Watched per account
--drop table #summary_by_account_and_channel_ranked;
select account_number
,channel_name_inc_hd_staggercast_channel_families
,seconds_viewed
,rank() over (partition by account_number order by seconds_viewed desc  ,channel_name_inc_hd_staggercast_channel_families desc) as rank_total_duration

into #summary_by_account_and_channel_ranked

from #summary_by_account_and_channel

group by account_number
,channel_name_inc_hd_staggercast_channel_families
,seconds_viewed
--having rank_total_duration<=10
;commit;
delete from #summary_by_account_and_channel_ranked where rank_total_duration>10;

--select * from #summary_by_account_and_channel_ranked;

select channel_name_inc_hd_staggercast_channel_families
,count(*) as accounts_with_channel
from #summary_by_account_and_channel_ranked
group by channel_name_inc_hd_staggercast_channel_families
order by accounts_with_channel desc
;

---Import in top 10 chanels by Panel ID
--drop table dbarnett.project193_loungelab_favourite_10_channels;
create table dbarnett.project193_loungelab_favourite_10_channels
(panelid varchar (50)
,channel varchar(90)
,channel_order integer
)
;
commit;

input into dbarnett.project193_loungelab_favourite_10_channels
from 'C:\Users\barnetd\Documents\Project V193 - Lounge Lab\Vespa raw Data 20130729\LL Data to Import\Top 10 Channels by Panel ID.csv' format ascii;

commit;

---Add on Account_number and Vespa stats---


alter table dbarnett.project193_loungelab_favourite_10_channels add account_number varchar(50);
alter table dbarnett.project193_loungelab_favourite_10_channels add days_viewing integer;


update dbarnett.project193_loungelab_favourite_10_channels
set account_number=b.account_number
,days_viewing=b.days_viewing
from  dbarnett.project193_loungelab_favourite_10_channels as a
left outer join dbarnett.project193_loungelab_survey_results as b
on a.panelid=b.panelid
;

---Change FOX to FX to match EPG data---

update dbarnett.project193_loungelab_favourite_10_channels
set channel=case when channel = 'FOX' then 'FX' else channel end
from dbarnett.project193_loungelab_favourite_10_channels
;
commit;



--select count(*) from dbarnett.project193_loungelab_favourite_10_channels;
--sleect count
--selec
--Match Survey with Vespa Viewing---
--drop table #top_10_matches;
select a.account_number
,channel_name_inc_hd_staggercast_channel_families
,max(case when b.account_number is not null then 1 else 0 end) as any_ac_match
,max(case when channel_name_inc_hd_staggercast_channel_families=b.channel then 1 else 0 end) as channel_in_favourite_10
into #top_10_matches
from #summary_by_account_and_channel_ranked as a
left outer join dbarnett.project193_loungelab_favourite_10_channels as b
on a.account_number = b.account_number
--where  days_viewing is not null
group by a.account_number
,channel_name_inc_hd_staggercast_channel_families
;

select channel_name_inc_hd_staggercast_channel_families
,count(*) as times_in_top_10_viewing
,sum(channel_in_favourite_10) as channel_in_favourite_10
from #top_10_matches
group by channel_name_inc_hd_staggercast_channel_families
order by times_in_top_10_viewing desc
;

---Analysis Other way - of 10 ten favourite - how many in top 10 viewing

select a.account_number
,channel
--,max(case when b.account_number is not null then 1 else 0 end) as any_ac_match
,max(case when b.channel_name_inc_hd_staggercast_channel_families=a.channel then 1 else 0 end) as channel_in_top_10_viewed
into #top_10_matches_viewed_from_favourites
from dbarnett.project193_loungelab_favourite_10_channels as a
left outer join #summary_by_account_and_channel_ranked as b 
on a.account_number = b.account_number
where  days_viewing is not null
group by a.account_number
,channel
;

select channel
,count(*) as times_in_top_10_favourites
,sum(channel_in_top_10_viewed) as channel_top_10_viewed
from #top_10_matches_viewed_from_favourites
group by channel
order by times_in_top_10_favourites desc
;

commit;



--select * from dbarnett.project193_loungelab_top_10_channels_viewed;


--group by account_number

;

commit;
--Q7 Analysis

select panelid
,account_number
,case when days_viewing>0 then 1 else 0 end as on_vespa
,case when upper(q7) like '%4SEVEN%' then 1 else 0 end as Channel_Watched_4Seven
,case when upper(q7) like '%5 USA%' then 1 else 0 end as Channel_Watched_5_USA
,case when upper(q7) like '%5*%' then 1 else 0 end as Channel_Watched_5Star
,case when upper(q7) like '%ALIBI%' then 1 else 0 end as Channel_Watched_Alibi
,case when upper(q7) like '%ANIMAL PLANET%' then 1 else 0 end as Channel_Watched_Animal_Planet
,case when upper(q7) like '%BBC1%' then 1 else 0 end as Channel_Watched_BBC_1
,case when upper(q7) like '%BBC NEWS%' then 1 else 0 end as Channel_Watched_BBC_News
,case when upper(q7) like '%BBC2%' then 1 else 0 end as Channel_Watched_BBC2
,case when upper(q7) like '%BBC3%' then 1 else 0 end as Channel_Watched_BBC3
,case when upper(q7) like '%BBC4%' then 1 else 0 end as Channel_Watched_BBC4
,case when upper(q7) like '%BIO%' then 1 else 0 end as Channel_Watched_Bio
,case when upper(q7) like '%CARTOON NETWORK%' then 1 else 0 end as Channel_Watched_Cartoon_Network
,case when upper(q7) like '%CBBC%' then 1 else 0 end as Channel_Watched_CBBC
,case when upper(q7) like '%CBEEBIES%' then 1 else 0 end as Channel_Watched_CBeebies
,case when upper(q7) like '%CHALLENGE%' then 1 else 0 end as Channel_Watched_Challenge
,case when upper(q7) like '%CHANNEL 4%' then 1 else 0 end as Channel_Watched_Channel_4
,case when upper(q7) like '%CHANNEL 5%' then 1 else 0 end as Channel_Watched_Channel_5
,case when upper(q7) like '%COMEDY CENTRAL%' then 1 else 0 end as Channel_Watched_Comedy_Central
,case when upper(q7) like '%CRIME & INVESTIGATION NETWORK%' then 1 else 0 end as Channel_Watched_Crime___Investigation_Network
,case when upper(q7) like '%DAVE%' then 1 else 0 end as Channel_Watched_Dave
,case when upper(q7) like '%DISCOVERY CHANNEL%' then 1 else 0 end as Channel_Watched_Discovery_Channel
,case when upper(q7) like '%DISCOVERY HISTORY%' then 1 else 0 end as Channel_Watched_Discovery_History
,case when upper(q7) like '%DISCOVERY HOME & HEALTH%' then 1 else 0 end as Channel_Watched_Discovery_Home___Health
,case when upper(q7) like '%DISCOVERY SCIENCE%' then 1 else 0 end as Channel_Watched_Discovery_Science
,case when upper(q7) like '%DISCOVERY SHED%' then 1 else 0 end as Channel_Watched_Discovery_Shed
,case when upper(q7) like '%DISCOVERY TURBO%' then 1 else 0 end as Channel_Watched_Discovery_Turbo
,case when upper(q7) like '%DISNEY CHANNEL%' then 1 else 0 end as Channel_Watched_Disney_Channel
,case when upper(q7) like '%DISNEY JUNIOR%' then 1 else 0 end as Channel_Watched_Disney_Junior
,case when upper(q7) like '%DISNEY XD%' then 1 else 0 end as Channel_Watched_Disney_XD
,case when upper(q7) like '%DMAX%' then 1 else 0 end as Channel_Watched_DMAX
,case when upper(q7) like '%DRAMA%' then 1 else 0 end as Channel_Watched_Drama
,case when upper(q7) like '%E! ENTERTAINMENT%' then 1 else 0 end as Channel_Watched_E__Entertainment
,case when upper(q7) like '%E4%' then 1 else 0 end as Channel_Watched_E4
,case when upper(q7) like '%EDEN%' then 1 else 0 end as Channel_Watched_Eden
,case when upper(q7) like '%ESPN %' then 1 else 0 end as Channel_Watched_ESPN_
,case when upper(q7) like '%ESPN CLASSIC%' then 1 else 0 end as Channel_Watched_ESPN_Classic
,case when upper(q7) like '%EUROSPORT%' then 1 else 0 end as Channel_Watched_Eurosport
,case when upper(q7) like '%EXTREME SPORTS%' then 1 else 0 end as Channel_Watched_Extreme_Sports
,case when upper(q7) like '%FILM4%' then 1 else 0 end as Channel_Watched_Film4
,case when upper(q7) like '%FOX%' then 1 else 0 end as Channel_Watched_FOX
,case when upper(q7) like '%G.O.L.D%' then 1 else 0 end as Channel_Watched_G_O_L_D
,case when upper(q7) like '%GOOD FOOD%' then 1 else 0 end as Channel_Watched_Good_Food
,case when upper(q7) like '%H2%' then 1 else 0 end as Channel_Watched_H2
,case when upper(q7) like '%HISTORY CHANNEL%' then 1 else 0 end as Channel_Watched_History_Channel
,case when upper(q7) like '%HOME%' then 1 else 0 end as Channel_Watched_Home
,case when upper(q7) like '%INVESTIGATION DISCOVERY%' then 1 else 0 end as Channel_Watched_Investigation_Discovery
,case when upper(q7) like '%ITV1%' then 1 else 0 end as Channel_Watched_ITV1
,case when upper(q7) like '%ITV2%' then 1 else 0 end as Channel_Watched_ITV2
,case when upper(q7) like '%ITV3%' then 1 else 0 end as Channel_Watched_ITV3
,case when upper(q7) like '%ITV4%' then 1 else 0 end as Channel_Watched_ITV4
,case when upper(q7) like '%MORE4%' then 1 else 0 end as Channel_Watched_More4
,case when upper(q7) like '%MTV%' then 1 else 0 end as Channel_Watched_MTV
,case when upper(q7) like '%NAT GEO%' then 1 else 0 end as Channel_Watched_Nat_Geo
,case when upper(q7) like '%NAT GEO WILD%' then 1 else 0 end as Channel_Watched_Nat_Geo_Wild
,case when upper(q7) like '%NICK JR%' then 1 else 0 end as Channel_Watched_Nick_Jr
,case when upper(q7) like '%NICKELODEON%' then 1 else 0 end as Channel_Watched_Nickelodeon
,case when upper(q7) like '%PBS AMERICA%' then 1 else 0 end as Channel_Watched_PBS_America
,case when upper(q7) like '%PICK TV%' then 1 else 0 end as Channel_Watched_Pick_TV
,case when upper(q7) like '%QVC%' then 1 else 0 end as Channel_Watched_QVC
,case when upper(q7) like '%REALLY%' then 1 else 0 end as Channel_Watched_Really
,case when upper(q7) like '%SKY 1%' then 1 else 0 end as Channel_Watched_Sky_1
,case when upper(q7) like '%SKY 2%' then 1 else 0 end as Channel_Watched_Sky_2
,case when upper(q7) like '%SKY ARTS%' then 1 else 0 end as Channel_Watched_Sky_Arts
,case when upper(q7) like '%SKY ATLANTIC%' then 1 else 0 end as Channel_Watched_Sky_Atlantic
,case when upper(q7) like '%SKY LIVING%' then 1 else 0 end as Channel_Watched_Sky_Living
,case when upper(q7) like '%SKY MOVIES%' then 1 else 0 end as Channel_Watched_Sky_Movies
,case when upper(q7) like '%SKY NEWS%' then 1 else 0 end as Channel_Watched_Sky_News
,case when upper(q7) like '%SKY SPORTS%' then 1 else 0 end as Channel_Watched_Sky_Sports
,case when upper(q7) like '%SKY SPORTS F1%' then 1 else 0 end as Channel_Watched_Sky_Sports_F1
,case when upper(q7) like '%SKY SPORTS NEWS%' then 1 else 0 end as Channel_Watched_Sky_Sports_News
,case when upper(q7) like '%STAR LIFE OK%' then 1 else 0 end as Channel_Watched_Star_Life_OK
,case when upper(q7) like '%STAR PLUS%' then 1 else 0 end as Channel_Watched_Star_Plus
,case when upper(q7) like '%SYFY%' then 1 else 0 end as Channel_Watched_Syfy
,case when upper(q7) like '%TCM%' then 1 else 0 end as Channel_Watched_TCM
,case when upper(q7) like '%THE FOOD NETWORK%' then 1 else 0 end as Channel_Watched_The_Food_Network
,case when upper(q7) like '%TLC%' then 1 else 0 end as Channel_Watched_TLC
,case when upper(q7) like '%UNIVERSAL%' then 1 else 0 end as Channel_Watched_Universal
,case when upper(q7) like '%WATCH%' then 1 else 0 end as Channel_Watched_Watch
,case when upper(q7) like '%YESTERDAY%' then 1 else 0 end as Channel_Watched_Yesterday
from dbarnett.project193_loungelab_survey_results
where on_vespa=1
; 


--ESPN Classic
--select Q7 from dbarnett.project193_loungelab_survey_results;

commit;


--Q9 Output for Excel
select *,case when days_viewing>0 then 1 else 0 end as on_vespa from dbarnett.project193_loungelab_survey_results where on_vespa=1;


--Sky Atlantic Actual and Favourite viewing---

---Group New and Non New EPG Titles--
--drop table #sky_atlantic_viewing_by_account;
select case when upper(left(programme_instance_name,4)) = 'NEW ' then right(programme_instance_name,len(programme_instance_name)-4) else programme_instance_name end
as epg_title

,case when a.channel_name in (
'BBC 1'
,'BBC 1 South East'
,'BBC 1 North West'
,'BBC 1 West Midlands'
,'BBC 1 Yorkshire'
,'BBC 1 North East & Cumbria'
,'BBC 1 Yorkshire & Lincolnshire'
,'BBC 1 East') then 'BBC ONE'


when a.channel_name in ('BBC2') then 'BBC TWO'
when a.channel_name in  ('Channel 5 Part Network') then 'Channel 5'
when a.channel_name in ('ITV1 STV Scotland Central'
,'ITV1 Meridian South'
,'ITV1 Meridian South East'
,'ITV1 HD South East'
,'ITV1 Yorkshire West'
,'ITV1 Central West'
,'ITV1 Meridian North'
,'ITV1 Anglia West'
,'ITV1 STV East') then 'ITV1'
when a.channel_name in  ('Channel 4+1 Scotland'
,'Channel 4+1 North'
,'Channel 4+1 London'
,'Channel 4+1 South') then 'Channel 4' 

  when channel_name_inc_hd_staggercast in ('Nick Jr','Nick Jr 2') then  'Nick Jr'
  when channel_name_inc_hd_staggercast in ('Sky Living','Sky Livingit') then  'Sky Living'
  when channel_name_inc_hd_staggercast in ('Comedy Central','Comedy Central Extra') then  'Comedy Central'
  when channel_name_inc_hd_staggercast in ('Eurosport','Eurosport2') then  'Eurosport'
  when channel_name_inc_hd_staggercast in ('More4','More 4') then  'More4'
when channel_name_inc_hd_staggercast in ('Sky Arts 1','Sky Arts 2') then  'Sky Arts 1 or 2'
--when channel_name_inc_hd_staggercast in ('Sky 1','Sky 2') then  'Sky 1 or 2'
when channel_name_inc_hd_staggercast in ('Sky Sports 1'
,'Sky Sports 2'
,'Sky Sports 3'
,'Sky Sports 4'
,'Football First') then  'Sky Sports'
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
,'Sky Drama & Romance'
,'Sky Oscars'
) then  'Sky Movies'
 else  channel_name_inc_hd_staggercast end as channel_name_inc_hd_staggercast_channel_families

,account_number
,sum(datediff(second,viewing_starts,viewing_stops)) as seconds_viewed
into #sky_atlantic_viewing_by_account
from v193_full_viewing as a
left outer join #channel_lookup as b
on a.channel_name=b.channel_name
where channel_name_inc_hd_staggercast_channel_families='Sky Atlantic'
group by epg_title,channel_name_inc_hd_staggercast_channel_families
,account_number
order by seconds_viewed desc
;

--


select a.*,b.days_viewing,q9_64 from #sky_atlantic_viewing_by_account as a
left outer join  dbarnett.project193_loungelab_survey_results as b
on a.account_number=b.account_number;


--Get Sky Atlantic Rating By Account
select account_number ,q9_64 ,days_viewing from dbarnett.project193_loungelab_survey_results where days_viewing>0


---Sky Atlantic as proportion of Total Viewing---
select account_number
,sum( case
when channel_name_inc_hd_staggercast in ('Sky Atlantic') then datediff(second,viewing_starts,viewing_stops) else 0 end) as seconds_viewed_sky_atlantic
,sum(  datediff(second,viewing_starts,viewing_stops)) as seconds_viewed_total
--into #sky_atlantic_and_other_channel_viewing_by_account
from v193_full_viewing as a
left outer join #channel_lookup as b
on a.channel_name=b.channel_name
group by account_number
;






/*

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

select channel_name_inc_hd_staggercast as channel_grouped

,sum(datediff(second,viewing_starts,viewing_stops)) as seconds_viewed

from v193_full_viewing as a
left outer join #channel_lookup as b
on a.channel_name=b.channel_name
group by channel_grouped
order by seconds_viewed desc
;

--drop table V159_Tenure_10_16mth_Viewing; commit;




*/
*/