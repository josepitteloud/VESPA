/*------------------------------------------------------------------------------
        Project: V140 - Sky Store Analysis
        Version: 1
        Created: 20130110
        Lead: Susanne Chan
        Analyst: Dan Barnett
        SK Prod: 4
*/------------------------------------------------------------------------------

---repeat the analysis from V134 but with Target Audiences changed to that for Sky Store/SBO usage

select a.account_number
,b.weighting as overall_project_weighting
into project_140_base_Accounts
from  vespa_analysts.SC2_intervals as a
inner join vespa_analysts.SC2_weightings as b
on  cast('2012-11-15' as date) = b.scaling_day
and a.scaling_segment_ID = b.scaling_segment_ID
and cast('2012-11-15' as date) between a.reporting_starts and a.reporting_ends
;
commit;
CREATE   HG INDEX idx01 ON project_140_base_Accounts(account_number);
commit;

alter table project_140_base_Accounts add vespa_seg varchar(100);

update project_140_base_Accounts
set vespa_seg = b.vespa_seg
from project_140_base_Accounts as a
left outer join rangep.SBO_dtv_base as b
on a.account_number =b.account_number
;

commit;
----Use viewing from Project 134 
--select top 500 * from project134_3_plus_minute_prog_viewed_deduped;

--drop table project_140_3_plus_minute_summary_by_programme;
select grouped_channel
,non_staggercast_broadcast_time_utc
,service_key_detail
,case when vespa_seg is null then 'e) Other' else vespa_seg end as vespa_segment

,media_pack
,primary_sales_house
,sum(overall_project_weighting) as accounts
into project_140_3_plus_minute_summary_by_programme
from project134_3_plus_minute_prog_viewed_deduped as a
left outer join project_140_base_Accounts as b
on a.account_number = b.account_number
left outer join vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES as c
on a.service_key_detail = c.service_key
group by grouped_channel
,non_staggercast_broadcast_time_utc
,service_key_detail
,vespa_segment
,media_pack
,primary_sales_house
;
commit;

alter table project_140_3_plus_minute_summary_by_programme add programme_name varchar(255);

update project_140_3_plus_minute_summary_by_programme
set programme_name=b.programme_instance_name
from project_140_3_plus_minute_summary_by_programme as a
left outer join sk_prod.VESPA_PROGRAMME_SCHEDULE as b
on a.service_key_detail = b.service_key and a.non_staggercast_broadcast_time_utc=b.broadcast_start_date_time_utc
;
commit;

--select top 100 * from project_140_3_plus_minute_summary_by_programme;



---Run Efficency Index at individual Media Level---
--drop table project134_3_plus_minute_summary_by_media_pack;
select a.account_number
,overall_project_weighting
,case when vespa_seg is null then 'e) Other' else vespa_seg end as vespa_segment

,max(case when media_pack = 'DOCUMENTARIES' then 1 else 0 end) as documentaries
,max(case when media_pack = 'ENTERTAINMENT' then 1 else 0 end) as entertainment
,max(case when media_pack = 'NEWS' then 1 else 0 end) as news
,max(case when media_pack = 'MOVIES' then 1 else 0 end) as movies
,max(case when media_pack = 'KIDS' then 1 else 0 end) as kids
,max(case when media_pack = 'MUSIC' then 1 else 0 end) as music
,max(case when media_pack = 'LIFESTYLE & CULTURE' then 1 else 0 end) as Lifestyle_Culture
,max(case when media_pack = 'SPORTS' then 1 else 0 end) as Sports
,max(case when media_pack = 'C4' then 1 else 0 end) as C4
,max(case when media_pack = 'C4 Digital' then 1 else 0 end) as C4_Digital
,max(case when media_pack = 'FIVE' then 1 else 0 end) as FIVE
,max(case when media_pack = 'FIVE Digital' then 1 else 0 end) as FIVE_Digital
,max(case when media_pack = 'ITV' then 1 else 0 end) as ITV
,max(case when media_pack = 'ITV Digital' then 1 else 0 end) as ITV_Digital
,max(case when media_pack = 'UKTV' then 1 else 0 end) as UKTV
into project_140_3_plus_minute_summary_by_media_pack
from project134_3_plus_minute_prog_viewed_deduped as a
left outer join project_140_base_Accounts as b
on a.account_number = b.account_number
left outer join vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES as c
on a.service_key_detail = c.service_key
where overall_project_weighting>0
group by vespa_segment
,a.account_number
,overall_project_weighting
;
commit;

--select top 100 * from project_140_3_plus_minute_summary_by_media_pack;



select vespa_segment
,documentaries
,entertainment
,news
,movies
,kids
,music
,Lifestyle_Culture
,Sports
,C4_Digital
,FIVE
,FIVE_Digital
,ITV
,ITV_Digital
,UKTV

, sum(documentaries*overall_project_weighting) as documentary_total
, sum(entertainment*overall_project_weighting) as entertainment_total
, sum(news*overall_project_weighting) as news_total
, sum(movies*overall_project_weighting) as movies_total
, sum(kids*overall_project_weighting) as kids_total
, sum(music*overall_project_weighting) as music_total
, sum(Lifestyle_Culture*overall_project_weighting) as Lifestyle_Culture_total
, sum(Sports*overall_project_weighting) as Sports_total

, sum(C4*overall_project_weighting) as C4_total
, sum(C4_Digital*overall_project_weighting) as C4_Digital_total
, sum(FIVE*overall_project_weighting) as FIVE_total
, sum(FIVE_Digital*overall_project_weighting) as FIVE_Digital_total
, sum(ITV*overall_project_weighting) as ITV_total
, sum(ITV_Digital*overall_project_weighting) as ITV_Digital_total
, sum(UKTV*overall_project_weighting) as UKTV_total

from project_140_3_plus_minute_summary_by_media_pack
group by vespa_segment
,documentaries
,entertainment
,news
,movies
,kids
,music
,Lifestyle_Culture
,Sports
,C4_Digital
,FIVE
,FIVE_Digital
,ITV
,ITV_Digital
,UKTV
order by vespa_segment
,documentaries
,entertainment
,news
,movies
,kids
,music
,Lifestyle_Culture
,Sports
,C4_Digital
,FIVE
,FIVE_Digital
,ITV
,ITV_Digital
,UKTV
;
--commit;

output to 'C:\Users\barnetd\Documents\Project 140 - Sky Store Analysis\sky store media pivot data.csv' format ascii;

update project134_3_plus_minute_prog_viewed_deduped
set grouped_channel=case    when b.channel_name in ('Sky Showcase','Sky ShowcseHD') then 'Sky Showcase'
                            when b.channel_name in ('Sky Movies 007','Sky 007 HD') then 'Sky Movies 007' else null end
from project_140_3_plus_minute_summary_by_programme_new_segments as a
left outer join sk_prod.VESPA_PROGRAMME_SCHEDULE as b
on a.service_key_detail = b.service_key and a.non_staggercast_broadcast_time_utc=b.broadcast_start_date_time_utc
where grouped_channel='Sky Summer'
;
commit;

----Split By Channel----
--drop table  project_140_3_plus_minute_summary_by_channel;
select vespa_seg
,a.account_number
,overall_project_weighting

,grouped_channel
,media_pack

into project_140_3_plus_minute_summary_by_channel
from project134_3_plus_minute_prog_viewed_deduped as a
left outer join project_140_base_Accounts as b
on a.account_number = b.account_number
left outer join vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES as c
on a.service_key_detail = c.service_key
where overall_project_weighting>0
group by vespa_seg
,a.account_number
,overall_project_weighting
,grouped_channel
,media_pack
;
commit;

select media_pack
,grouped_channel
,sum(case when vespa_seg is not null then overall_project_weighting else 0 end) as total_accounts_all_target
,sum(case when vespa_seg is null then overall_project_weighting else 0 end) as total_accounts_all_non_target

,sum(case when vespa_seg in 
    (   'b) SBO Only - HD+ Box',
        'c) Purchased both - Store Users',
        'd) Purchased both - Lapsed Users')
 then overall_project_weighting
 else 0 end) as total_accounts_seg_b_to_d_target
,sum(case when vespa_seg not in (   'b) SBO Only - HD+ Box',
        'c) Purchased both - Store Users',
        'd) Purchased both - Lapsed Users')
 then overall_project_weighting
          when vespa_seg is null then overall_project_weighting  
 else 0 end) as total_accounts_seg_b_to_d_non_target


,sum(case when vespa_seg  in ('a) Never used - Historic Purchase HD+ Box') then overall_project_weighting else 0 end) as total_accounts_seg_a_target
,sum(case when vespa_seg not in ('a) Never used - Historic Purchase HD+ Box') then overall_project_weighting
          when vespa_seg is null then overall_project_weighting  
         else 0 end) as total_accounts_seg_a_non_target

from project_140_3_plus_minute_summary_by_channel
group by media_pack
,grouped_channel;

select distinct vespa_seg from project_140_3_plus_minute_summary_by_channel





--alter table project_140_3_plus_minute_summary_by_programme rename project_140_3_plus_minute_summary_by_programme_new_segments; commit;
--drop table project_140_3_plus_minute_summary_by_programme;
select grouped_channel
,non_staggercast_broadcast_time_utc
,media_pack
,primary_sales_house
,service_key_detail
,vespa_seg
,sum(overall_project_weighting) as accounts
into project_140_3_plus_minute_summary_by_programme_new_segments
from project134_3_plus_minute_prog_viewed_deduped as a
left outer join project_140_base_Accounts as b
on a.account_number = b.account_number
left outer join vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES as c
on a.service_key_detail = c.service_key
group by grouped_channel
,non_staggercast_broadcast_time_utc
,media_pack
,service_key_detail
,primary_sales_house
,vespa_seg
;
commit;

alter table project_140_3_plus_minute_summary_by_programme_new_segments add programme_name varchar(255);

update project_140_3_plus_minute_summary_by_programme_new_segments
set programme_name=b.programme_instance_name
from project_140_3_plus_minute_summary_by_programme_new_segments as a
left outer join sk_prod.VESPA_PROGRAMME_SCHEDULE as b
on a.service_key_detail = b.service_key and a.non_staggercast_broadcast_time_utc=b.broadcast_start_date_time_utc
;
commit;

---Update to Correct Sky Summer/Sky Movies/Sky 007 changes in Service Key---

update project_140_3_plus_minute_summary_by_programme_new_segments
set grouped_channel=case    when b.channel_name in ('Sky Showcase','Sky ShowcseHD') then 'Sky Showcase'
                            when b.channel_name in ('Sky Movies 007','Sky 007 HD') then 'Sky Movies 007' else null end
from project_140_3_plus_minute_summary_by_programme_new_segments as a
left outer join sk_prod.VESPA_PROGRAMME_SCHEDULE as b
on a.service_key_detail = b.service_key and a.non_staggercast_broadcast_time_utc=b.broadcast_start_date_time_utc
where grouped_channel='Sky Summer'
;
commit;

--select top 100 * from project_140_3_plus_minute_summary_by_programme_new_segments where programme_name='Tomorrow Never Dies'
--select top 100 * from sk_prod.VESPA_PROGRAMME_SCHEDULE where broadcast_start_date_time_utc='2012-10-18 21:20:00' and service_key = 4033
--drop table #summary_by_programmes;
select media_pack
,grouped_channel
,programme_name
,case 
when dateformat(non_staggercast_broadcast_time_utc,'YYYY-MM-DD-HH') between '2010-03-28-02' and '2010-10-31-02' then dateadd(hh,1,non_staggercast_broadcast_time_utc) 
when dateformat(non_staggercast_broadcast_time_utc,'YYYY-MM-DD-HH') between '2011-03-27-02' and '2011-10-30-02' then dateadd(hh,1,non_staggercast_broadcast_time_utc) 
when dateformat(non_staggercast_broadcast_time_utc,'YYYY-MM-DD-HH') between '2012-03-25-02' and '2012-10-28-02' then dateadd(hh,1,non_staggercast_broadcast_time_utc) 
                    else non_staggercast_broadcast_time_utc  end as non_staggercast_broadcast_time_local
,sum(case when vespa_seg is not null then accounts else 0 end) as total_accounts_all_target
,sum(case when vespa_seg is null then accounts else 0 end) as total_accounts_all_non_target

,sum(case when vespa_seg in 
    (   'b) SBO Only - HD+ Box',
        'c) Purchased both - Store Users',
        'd) Purchased both - Lapsed Users')
 then accounts
 else 0 end) as total_accounts_seg_b_to_d_target
,sum(case when vespa_seg not in (   'b) SBO Only - HD+ Box',
        'c) Purchased both - Store Users',
        'd) Purchased both - Lapsed Users')
 then accounts
          when vespa_seg is null then accounts  
 else 0 end) as total_accounts_seg_b_to_d_non_target


,sum(case when vespa_seg  in ('a) Never used - Historic Purchase HD+ Box') then accounts else 0 end) as total_accounts_seg_a_target
,sum(case when vespa_seg not in ('a) Never used - Historic Purchase HD+ Box') then accounts
          when vespa_seg is null then accounts  
         else 0 end) as total_accounts_seg_a_non_target
,sum(accounts) as total_accounts_all
,rank() over  (partition by media_pack order by total_accounts_all_target desc) as rank_prog_all
,rank() over  (partition by media_pack order by total_accounts_seg_b_to_d_target desc) as rank_prog_seg_b_to_d_target
,rank() over  (partition by media_pack order by total_accounts_seg_a_target desc) as rank_prog_seg_a_target
into #summary_by_programmes
from project_140_3_plus_minute_summary_by_programme_new_segments
--where media_pack<>'BBC'
group by media_pack
,grouped_channel
,non_staggercast_broadcast_time_local
,programme_name
order by total_accounts_all desc
;
commit;


select * from #summary_by_programmes where rank_prog_all <=300 and media_pack is not null;
select * from #summary_by_programmes where rank_prog_seg_b_to_d_target <=300 and media_pack is not null;
select * from #summary_by_programmes where rank_prog_seg_a_target <=300 and media_pack is not null;

commit;



/*

select vespa_seg ,count(*) 
from rangep.SBO_dtv_base as a 
left outer join sk_prod.cust_single_account_view as b 
on a.account_number =b.account_number
where pty_country_code = 'GBR'

group by vespa_seg order by vespa_seg;


select vespa_seg , count(*) from rangep.SBO_dtv_base group by vespa_seg order by vespa_seg;

select vespa_seg , sum(overall_project_weighting) as accounts from project_140_base_Accounts group by vespa_seg order by vespa_seg;

select account

commit;
*/


