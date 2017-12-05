

----Sky Arts Channel Viewing ---

--Top Programmes in period---

---Create a summary of one record per programme per account--
--drop table v141_summary_by_account_and_programme_sky_arts  ;
select programme_trans_sk
,account_number
,channel_name_inc_hd
,epg_title
,sum(viewing_duration) as seconds_viewed_weighted
into v141_summary_by_account_and_programme_sky_arts        
from  v141_live_playback_viewing as a
where channel_name_inc_hd in (
'Sky Arts 1',
'Sky Arts 2')
group by programme_trans_sk
,account_number,
channel_name_inc_hd
,epg_title
;

---Add on accoun_weight--

alter table v141_summary_by_account_and_programme_sky_arts  add account_weight double;

update v141_summary_by_account_and_programme_sky_arts  
set account_weight=b.account_weight
from v141_summary_by_account_and_programme_sky_arts   as a
left outer join v141_accounts_for_profiling as b
on a.account_number = b.account_number
;

commit;

delete from v141_summary_by_account_and_programme_sky_arts where (account_weight=0 or account_weight is null);
commit;

--select top 100 * from v141_summary_by_account_and_programme_sky_arts;

select  a.channel_name_inc_hd
,a.epg_title
,genre_description
,tx_date_utc
,sum(account_weight) as weighted_accounts_viewing
,sum(account_weight*seconds_viewed_weighted) as weighted_accounts_viewing_seconds
into #viewing_all_programmes
from v141_summary_by_account_and_programme_sky_arts as a 
left outer join V141_Viewing_detail as b
on a.programme_trans_sk=b.programme_trans_sk
where  tx_date_utc>='2012-10-01' 
group by  a.channel_name_inc_hd
,a.epg_title
,genre_description
,tx_date_utc
;

select * from #viewing_all_programmes order by weighted_accounts_viewing desc;

--select top 100 * from  V141_Viewing_detail;
commit;


