
--select top 100 * from v223_unbundling_viewing_summary_by_account;

create variable @total_programmes_3min_plus_sky_sports_champions_league integer;
set @total_programmes_3min_plus_sky_sports_champions_league
=( select count(*) from v223_unbundling_viewing_summary_by_account where annualised_programmes_3min_plus_football_champions_league_sky_sports>0);

create variable @total_programmes_3min_plus_non_sky_sports_champions_league integer;
set @total_programmes_3min_plus_non_sky_sports_champions_league
=( select count(*) from v223_unbundling_viewing_summary_by_account where annualised_programmes_3min_plus_football_champions_league_non_sky_sports>0);

create variable @total_programmes_3min_plus_sky_sports_premier_league integer;
set @total_programmes_3min_plus_sky_sports_premier_league
=( select count(*) from v223_unbundling_viewing_summary_by_account where annualised_programmes_3min_plus_football_premier_league_sky_sports>0);

create variable @total_programmes_3min_plus_non_sky_sports_premier_league integer;
set @total_programmes_3min_plus_sky_sports_premier_league
=( select count(*) from v223_unbundling_viewing_summary_by_account where annualised_programmes_3min_plus_football_premier_league_ESPN_BT>0);


alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_engaged_football_champions_league_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_engaged_football_champions_league_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_engaged_football_premier_league_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_engaged_football_premier_league_non_sky_sports integer;

alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_engaged_football_champions_league_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_engaged_football_champions_league_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_engaged_football_premier_league_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_engaged_football_premier_league_non_sky_sports integer;


update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_engaged_football_champions_league_sky_sports = case when annualised_programmes_3min_plus_football_champions_league_sky_sports=0 then 999 
when abs(rank_prog_engaged_football_champions_league_sky_sports/(@total_programmes_3min_plus_sky_sports_champions_league/100))+1 >100 then 100 
else abs(rank_prog_engaged_football_champions_league_sky_sports/(@total_programmes_3min_plus_sky_sports_champions_league/100))+1 end
,phase_2_decile_engaged_football_champions_league_sky_sports = case when  annualised_programmes_3min_plus_football_champions_league_sky_sports=0 then 99 
when abs(rank_prog_engaged_football_champions_league_sky_sports/(@total_programmes_3min_plus_sky_sports_champions_league/10))+1 >10 then 10 
else 
abs(rank_prog_engaged_football_champions_league_sky_sports/(@total_programmes_3min_plus_sky_sports_champions_league/10))+1 end
;
commit;

update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_engaged_football_champions_league_non_sky_sports = case when annualised_programmes_3min_plus_football_champions_league_non_sky_sports=0 then 999 
when abs(rank_prog_engaged_football_champions_league_non_sky_sports/(@total_programmes_3min_plus_non_sky_sports_champions_league/100))+1 >100 then 100 
else abs(rank_prog_engaged_football_champions_league_non_sky_sports/(@total_programmes_3min_plus_non_sky_sports_champions_league/100))+1 end
,phase_2_decile_engaged_football_champions_league_non_sky_sports = case when  annualised_programmes_3min_plus_football_champions_league_non_sky_sports=0 then 99 
when abs(rank_prog_engaged_football_champions_league_non_sky_sports/(@total_programmes_3min_plus_non_sky_sports_champions_league/10))+1 >10 then 10 
else 
abs(rank_prog_engaged_football_champions_league_non_sky_sports/(@total_programmes_3min_plus_non_sky_sports_champions_league/10))+1 end
;
commit;

--select @total_programmes_3min_plus_sky_sports_premier_league;
--select rank_prog_engaged_football_premier_league_sky_sports from v223_unbundling_viewing_summary_by_account
--select rank_prog_engaged_football_premier_league_sky_sports from v223_unbundling_viewing_summary_by_account

update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_engaged_football_premier_league_sky_sports = case when annualised_programmes_3min_plus_football_premier_league_sky_sports=0 then 999 
when abs(rank_prog_engaged_football_premier_league_sky_sports/(@total_programmes_3min_plus_sky_sports_premier_league/100))+1 >100 then 100 
else abs(rank_prog_engaged_football_premier_league_sky_sports/(@total_programmes_3min_plus_sky_sports_premier_league/100))+1 end
,phase_2_decile_engaged_football_premier_league_sky_sports = case when  annualised_programmes_3min_plus_football_premier_league_sky_sports=0 then 99 
when abs(rank_prog_engaged_football_premier_league_sky_sports/(@total_programmes_3min_plus_sky_sports_premier_league/10))+1 >10 then 10 
else 
abs(rank_prog_engaged_football_premier_league_sky_sports/(@total_programmes_3min_plus_sky_sports_premier_league/10))+1 end
;
commit;

/*
update dbarnett.v223_Unbundling_pivot_activity_data
set phase_2_percentile_engaged_football_premier_league_sky_sports=b.phase_2_percentile_engaged_football_premier_league_sky_sports
,phase_2_decile_engaged_football_premier_league_sky_sports=b.phase_2_decile_engaged_football_premier_league_sky_sports
from dbarnett.v223_Unbundling_pivot_activity_data as a
left outer join v223_unbundling_viewing_summary_by_account as b
on a.account_number = b.account_number
;
commit;
*/

/*

select phase_2_decile_engaged_football_premier_league_sky_sports , count(*),sum(account_weight) from v223_unbundling_viewing_summary_by_account group by phase_2_decile_engaged_football_premier_league_sky_sports order by phase_2_decile_engaged_football_premier_league_sky_sports


select round(annualised_programmes_engaged_football_premier_league_sky_sports,0) as progs 
,min(rank_prog_engaged_football_premier_league_sky_sports) as min_rank
 , max(rank_prog_engaged_football_premier_league_sky_sports) as max_rank
,min (phase_2_decile_engaged_football_premier_league_sky_sports) as min_dec
, max(phase_2_decile_engaged_football_premier_league_sky_sports) as max_dec
, count(*),sum(account_weight) from v223_unbundling_viewing_summary_by_account
 group by progs order by progs;


select phase_2_decile_engaged_football_premier_league_sky_sports , count(*),sum(account_weight) from dbarnett.v223_Unbundling_pivot_activity_data group by phase_2_decile_engaged_football_premier_league_sky_sports order by phase_2_decile_engaged_football_premier_league_sky_sports

--annualised_programmes_engaged_football_premier_league_sky_sports
*/





update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_engaged_football_premier_league_non_sky_sports = case when annualised_programmes_3min_plus_football_premier_league_ESPN_BT=0 then 999 
when abs(rank_prog_engaged_football_premier_league_ESPN_BT/(@total_programmes_3min_plus_non_sky_sports_premier_league/100))+1 >100 then 100 
else abs(rank_prog_engaged_football_premier_league_ESPN_BT/(@total_programmes_3min_plus_non_sky_sports_premier_league/100))+1 end
,phase_2_decile_engaged_football_premier_league_non_sky_sports = case when  annualised_programmes_3min_plus_football_premier_league_ESPN_BT=0 then 99 
when abs(rank_prog_engaged_football_premier_league_ESPN_BT/(@total_programmes_3min_plus_non_sky_sports_premier_league/10))+1 >10 then 10 
else 
abs(rank_prog_engaged_football_premier_league_ESPN_BT/(@total_programmes_3min_plus_non_sky_sports_premier_league/10))+1 end
;
commit;



alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_3min_plus_football_champions_league_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_3min_plus_football_champions_league_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_3min_plus_football_premier_league_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_3min_plus_football_premier_league_non_sky_sports integer;

alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_3min_plus_football_champions_league_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_3min_plus_football_champions_league_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_3min_plus_football_premier_league_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_3min_plus_football_premier_league_non_sky_sports integer;

update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_3min_plus_football_champions_league_sky_sports = case when annualised_programmes_3min_plus_football_champions_league_sky_sports=0 then 999 
when abs(rank_prog_3min_plus_football_champions_league_sky_sports/(@total_programmes_3min_plus_sky_sports_champions_league/100))+1 >100 then 100 
else abs(rank_prog_3min_plus_football_champions_league_sky_sports/(@total_programmes_3min_plus_sky_sports_champions_league/100))+1 end
,phase_2_decile_3min_plus_football_champions_league_sky_sports = case when  annualised_programmes_3min_plus_football_champions_league_sky_sports=0 then 99 
when abs(rank_prog_3min_plus_football_champions_league_sky_sports/(@total_programmes_3min_plus_sky_sports_champions_league/10))+1 >10 then 10 
else 
abs(rank_prog_3min_plus_football_champions_league_sky_sports/(@total_programmes_3min_plus_sky_sports_champions_league/10))+1 end
;
commit;

update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_3min_plus_football_champions_league_non_sky_sports = case when annualised_programmes_3min_plus_football_champions_league_non_sky_sports=0 then 999 
when abs(rank_prog_3min_plus_football_champions_league_non_sky_sports/(@total_programmes_3min_plus_non_sky_sports_champions_league/100))+1 >100 then 100 
else abs(rank_prog_3min_plus_football_champions_league_non_sky_sports/(@total_programmes_3min_plus_non_sky_sports_champions_league/100))+1 end
,phase_2_decile_3min_plus_football_champions_league_non_sky_sports = case when  annualised_programmes_3min_plus_football_champions_league_non_sky_sports=0 then 99 
when abs(rank_prog_3min_plus_football_champions_league_non_sky_sports/(@total_programmes_3min_plus_non_sky_sports_champions_league/10))+1 >10 then 10 
else 
abs(rank_prog_3min_plus_football_champions_league_non_sky_sports/(@total_programmes_3min_plus_non_sky_sports_champions_league/10))+1 end
;
commit;




update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_3min_plus_football_premier_league_sky_sports = case when annualised_programmes_3min_plus_football_premier_league_sky_sports=0 then 999 
when abs(rank_prog_3min_plus_football_premier_league_sky_sports/(@total_programmes_3min_plus_sky_sports_premier_league/100))+1 >100 then 100 
else abs(rank_prog_3min_plus_football_premier_league_sky_sports/(@total_programmes_3min_plus_sky_sports_premier_league/100))+1 end
,phase_2_decile_3min_plus_football_premier_league_sky_sports = case when  annualised_programmes_3min_plus_football_premier_league_sky_sports=0 then 99 
when abs(rank_prog_3min_plus_football_premier_league_sky_sports/(@total_programmes_3min_plus_sky_sports_premier_league/10))+1 >10 then 10 
else 
abs(rank_prog_3min_plus_football_premier_league_sky_sports/(@total_programmes_3min_plus_sky_sports_premier_league/10))+1 end
;
commit;

/*

select rank_prog_3min_plus_football_premier_league_sky_sports from dbarnett.v223_Unbundling_pivot_activity_data
select sum(case when  annualised_programmes_3min_plus_football_premier_league_sky_sports>0 then 1 else 0 end), @total_programmes_3min_plus_sky_sports_premier_league
from v223_unbundling_viewing_summary_by_account

select annualised_programmes_3min_plus_football_premier_league_sky_sports ,rank_prog_3min_plus_football_premier_league_sky_sports
-- count(*) 
from v223_unbundling_viewing_summary_by_account group by annualised_programmes_3min_plus_football_premier_league_sky_sports order by annualised_programmes_3min_plus_football_premier_league_sky_sports
*/


update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_3min_plus_football_premier_league_non_sky_sports = case when annualised_programmes_3min_plus_football_premier_league_ESPN_BT=0 then 999 
when abs(rank_prog_3min_plus_football_premier_league_ESPN_BT/(@total_programmes_3min_plus_non_sky_sports_premier_league/100))+1 >100 then 100 
else abs(rank_prog_3min_plus_football_premier_league_ESPN_BT/(@total_programmes_3min_plus_non_sky_sports_premier_league/100))+1 end
,phase_2_decile_3min_plus_football_premier_league_non_sky_sports = case when  annualised_programmes_3min_plus_football_premier_league_ESPN_BT=0 then 99 
when abs(rank_prog_3min_plus_football_premier_league_ESPN_BT/(@total_programmes_3min_plus_non_sky_sports_premier_league/10))+1 >10 then 10 
else 
abs(rank_prog_3min_plus_football_premier_league_ESPN_BT/(@total_programmes_3min_plus_non_sky_sports_premier_league/10))+1 end
;
commit;

----------------------
---Update second set of ranks---
select account_number

,rank() over (  ORDER BY annualised_programmes_engaged_football_premier_league_sky_sports  desc ,  minutes_football_premier_league_sky_sports desc) as rank_prog_engaged_football_premier_league_sky_sports

into #rank_minutes_details_engaged
from v223_unbundling_viewing_summary_by_account
where days_with_viewing>=280
;
commit;


exec sp_create_tmp_table_idx '#rank_minutes_details_engaged', 'account_number';
commit;


update v223_unbundling_viewing_summary_by_account

---Update Engaged progs
 set rank_prog_engaged_football_premier_league_sky_sports  =b.rank_prog_engaged_football_premier_league_sky_sports 

from v223_unbundling_viewing_summary_by_account as a
left outer join #rank_minutes_details_engaged as b
on a.account_number = b.account_number
where days_with_viewing>=280
;
commit;

--select max(rank_prog_engaged_football_premier_league_sky_sports) from v223_unbundling_viewing_summary_by_account where rank_prog_engaged_football_premier_league_sky_sports<160389




