


----Total Broadcast Info by Right----

--drop table dbarnett.v250_epg_list;
select a.service_key
,a.genre_description
,a.sub_genre_description
,a.dk_programme_instance_dim
,a.programme_instance_duration
,cast (a.broadcast_start_date_time_local as date) as broadcast_date
,a.broadcast_start_date_time_local
,b.channel_name
,case when b.channel_name in ('BBC 1','BBC 2','BBC HD','BBC Three') then 'BBC'
 when b.channel_name in ('BT Sport 1','BT Sport 2') then 'BT Sport'
when b.channel_name in ('Eurosport','Eurosport UK','Eurosport2','Eurosport2 UK') then 'Eurosport'
 when b.channel_name in ('Challenge','Channel 4','Channel 5','ESPN','ITV1','ITV4') then b.channel_name
 when b.channel_name in ('ESPN Classic','ESPN America') then 'ESPN'
when b.channel_name in ('Sky Sports 1','Sky Sports 2','Sky Sports 3','Sky Sports 4','Sky Sports F1') then 'Sky Sports'
when b.channel_name in ('Sky1','Sky2') then 'Sky 1 and Sky 2'
 else 'Other' end as channel_name_grouped
,case when c.live=1 then 1 else 0 end as live_status
,case when analysis_right_new is null then channel_name_grouped +' '+ a.sub_genre_description else analysis_right_new end as analysis_right_full

into dbarnett.v250_epg_list
from sk_prod.Vespa_programme_schedule as a
left outer join v250_channel_to_service_key_lookup_deduped as b
on a.service_key=b.service_key
left outer join dbarnett.v250_sports_rights_epg_data_for_analysis as c
on a.dk_programme_instance_dim=c.dk_programme_instance_dim
where  ( a.service_key in (6000,4523
,3615
,1758
,3150
,3022
,3027
,3023
,3028
,3515
,4402
,3255
,3411
,5416
,3209
,4401
,3406
,5087
,3638
,3107
,5353
,3825
,2330
,2303
,4151
,3813
,2402
,3210
,5071
,5068
,3580
,4753
,3806
,1354
,1355
,6753
,5703
,5708
,3645
,2156
,2006
,2012
,2018
,2011
,2072
,3544
,2061
,3208
,4100
,3719
,5905
,1873
,2611
,4205
,3260
,5530
,5609
,3417
,3653
,3625
,3627
,3752
,2552
,4077
,1371
,2020
,2019
,4610
,3617
,3352
,3211
,4547
,2202
,1626
,1830
,3403
,4102
,3505
,3407
,1757
,4420
,4541
,3815
,1448
,4130
,6273
,4505
,5610
,3611
,5337
,5602
,3714
,2510
,1813
,1872
,3410
,5741
,4215
,1825
,4034
,6233
,6231
,6232
,4115
,2306
,3012
,3830
,3632
,2401
,2407
,2408
,2403
,2406
,4548
,2405
,2409
,4071
,1881
,1887
,2522
,1884
,1843
,3777
,3609
,3618
,1370
,2612
,1360
,1628
,2302
,1151
,3141
,3639
,3221
,2142
,2121
,4604
,4004
,4009
,3101
,1627
,5165
,3781
,3590
,1357
,4560
,1305
,3010
,4407
,4110
,2304
,2308
,1874
,5706
,1875
,2619
,2301
,1894
,3001
,2413
,5740
,5900
,3916
,3641
,6240
,6260
,6272
,6391
,6532
,6533
,6534
,5707
,3354
,3359
,5761
,3357
,1853
,3386
,3656
,1858
,4262
,4216
,5070
,2609
,3732
,4089
,1877
,3682
,2603
,3541
,4007
,1879
,3340
,3735
,3708
,3516
,3831
,2501
,2508
,2509
,2507
,2516
,2506
,2521
,3508
,2512
,2503
,5715
,5285
,1834
,3731
,1847
,1806
,1821
,5521
,3616
,3147
,3356
,1857
,4340
,1846
,1849
,3510
,3914
,3409
,3800
,5500
,3258
,1832
,3750
,4263
,5311
,5952
,4350
,4201
,3415
,3646
,3000
,6761
,5907
,4105
,5915
,3636
,2325
,3412
,3915
,4001
,5608
,3353
,1251
,1252
,1256
,3213
,5701
,3631
,3630
,4409
,4210
,3601
,4551
,6758
,4933
,4015
,1001
,1752
,1753
,1412
,1812
,1002
,1818
,1816
,1808
,1815
,1811
,2201
,2203
,2207
,1838
,1404
,1409
,1807
,1701
,1301
,1302
,1333
,1322
,1306
,1314
,1402
,1833
,1430
,3215
,1350
,3709
,3358
,2601
,3613
,3612
,1772
,3608
,5300
,1771
,3603
,2711
,5712
,3408
,3206
,2505
,5605
,1372
,3811
,1253
,3805
,3547
,1802
,3525
,3935
,3780
,3812
,4644
,5607
,4266
,3643
,3751
,1255
,3351
,1805
,5882
,4410
,3251
,3104
,1842
,4360
,3720
,2502
,3531
,2511
,3108
,3810
,2617
,4550
,2305
,2608
,2607
,2606
) or c.record_id = 2297633)
   and (analysis_right_new is not null or a.genre_description='Sports')

;
commit;

----Add an Analysis Right Grouped for Certain Sports e.g., Premier League/F1---
--select distinct analysis_right_full from dbarnett.v250_epg_list order by analysis_right_full;
--alter table 
--alter table dbarnett.v250_epg_list delete analysis_right_grouped
alter table dbarnett.v250_epg_list add analysis_right_grouped varchar(80);

update dbarnett.v250_epg_list
set analysis_right_grouped = 
case when analysis_right_full in ('ECB Test Cricket Sky Sports'
,'ECB non-Test Cricket Sky Sports')
then 'ECB Cricket Sky Sports'
when analysis_right_full in (
'F1 (Practice Live)- BBC'
,'F1 (Qualifying Live)- BBC'
,'F1 (Race Live)- BBC'
,'F1 (non-Live)- BBC')
then 'F1 - BBC'
when analysis_right_full in 
('Formula One 2012-2018 - (Practice Live) Sky Sports'
,'Formula One 2012-2018 - (Qualifying Live) Sky Sports'
,'Formula One 2012-2018 - (Race Live) Sky Sports'
,'Formula One 2012-2018 - (non-Live) Sky Sports')
then 'F1 - Sky Sports'

when analysis_right_full in (
'Premier League Football - Sky Sports (MNF)'
,'Premier League Football - Sky Sports (Match Choice)'
,'Premier League Football - Sky Sports (Sat Lunchtime)'
,'Premier League Football - Sky Sports (Sat Night Live)'
,'Premier League Football - Sky Sports (Sun 4pm)'
,'Premier League Football - Sky Sports (Sun Lunchtime)'
,'Premier League Football - Sky Sports (non Live)'
,'Premier League Football - Sky Sports (other Live)')
then 'Premier League Football - Sky Sports'
when 
analysis_right_full
in ('England Friendlies (Football) - ITV'
,'England World Cup Qualifying (Away) - ITV'
,'England World Cup Qualifying (Home) - ITV')
then 'England Football Internationals - ITV'

else analysis_right_full end
from dbarnett.v250_epg_list
;
commit;






---Dedupe to countr progs by right---

--select top 100 * from dbarnett.v250_epg_list;

---Dedupe list---

select channel_name
,broadcast_start_date_time_local
,broadcast_date
,analysis_right_full
,live_status
,max(programme_instance_duration) as total_broadcast
into #v250_epg_list
from dbarnett.v250_epg_list
group by channel_name
,broadcast_start_date_time_local
,broadcast_date
,analysis_right_full
,live_status
;
--drop table #summary_by_analysis_right_by_live_status_overall;
select analysis_right_full
,count(distinct broadcast_date) as days_broadcast
,sum(total_broadcast) as right_broadcast_duration
,count(*) as total_programmes_broadcast
into #summary_by_analysis_right_overall
from #v250_epg_list
where broadcast_date between '2012-11-01' and '2013-10-31'
group by analysis_right_full
;
commit;
--select * from #summary_by_analysis_right_overall;

select analysis_right_full
,live_status
,count(distinct broadcast_date) as days_broadcast
,sum(total_broadcast) as right_broadcast_duration
,count(*) as total_programmes_broadcast
into #summary_by_analysis_right_by_live_status_overall
from #v250_epg_list
where broadcast_date between '2012-11-01' and '2013-10-31'
group by analysis_right_full
,live_status
;
commit;
--select * from #summary_by_analysis_right_by_live_status_overall;

-----Repeat by Grouped Rights----
--drop table #v250_epg_list_rights_grouped; drop table #summary_by_analysis_right_overall_grouped; drop table #summary_by_analysis_right_by_live_status_overall_grouped;
select channel_name
,broadcast_start_date_time_local
,broadcast_date
,analysis_right_grouped
,live_status
,max(programme_instance_duration) as total_broadcast
into #v250_epg_list_rights_grouped
from dbarnett.v250_epg_list
where analysis_right_grouped<>analysis_right_full
group by channel_name
,broadcast_start_date_time_local
,broadcast_date
,analysis_right_grouped
,live_status
;
--drop table #summary_by_analysis_right_by_live_status_overall;
select analysis_right_grouped
,count(distinct broadcast_date) as days_broadcast
,sum(total_broadcast) as right_broadcast_duration
,count(*) as total_programmes_broadcast
into #summary_by_analysis_right_overall_grouped
from #v250_epg_list_rights_grouped
where broadcast_date between '2012-11-01' and '2013-10-31'
group by analysis_right_grouped
;
commit;
--select * from #summary_by_analysis_right_overall_grouped;

select analysis_right_grouped
,live_status
,count(distinct broadcast_date) as days_broadcast
,sum(total_broadcast) as right_broadcast_duration
,count(*) as total_programmes_broadcast
into #summary_by_analysis_right_by_live_status_overall_grouped
from #v250_epg_list_rights_grouped
where broadcast_date between '2012-11-01' and '2013-10-31'
group by analysis_right_grouped
,live_status
;
commit;

--select * from #summary_by_analysis_right_by_live_status_overall_grouped;




--select * from #summary_by_analysis_right_by_live_status_overall_grouped;

---select top 100 * from dbarnett.v250_sports_rights_epg_data_for_analysis where analysis_right<>analysis_right_new

--select analysis_right, analysis_right_new from dbarnett.v250_sports_rights_epg_data_for_analysis group by  analysis_right, analysis_right_new;

