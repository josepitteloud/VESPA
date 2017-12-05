

select account_number
,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D') and sub_genre_description='Football'
 then  viewing_duration else 0 end) as viewing_duration_sky_sports_football

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels' ,'Sky 3D') and sub_genre_description='Football'
 then  viewing_duration else 0 end) as viewing_duration_non_sky_sports_football

,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and sub_genre_description='Football'
 and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_sky_sports_football

,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels' ,'Sky 3D')
and sub_genre_description='Football'
 and  viewing_duration>=180 then 1 else 0 end) as programmes_3min_plus_non_sky_sports_football


,sum(case when channel_name_inc_hd_staggercast_channel_families in ('Sky Sports Channels' ,'Sky 3D')
and sub_genre_description='Football' and 
(viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) 
as programmes_engaged_sky_sports_football


,sum(case when channel_name_inc_hd_staggercast_channel_families not in ('Sky Sports Channels' ,'Sky 3D')
and sub_genre_description='Football' and 
(viewing_duration>=3600 or viewing_duration/cast(programme_instance_duration as real)>=0.6) then 1 else 0 end) 
as programmes_engaged_non_sky_sports_football


into v223_unbundling_viewing_summary_by_account_phase_2
from dbarnett.v223_all_sports_programmes_viewed_sample  as a
--left outer join dbarnett.v223_sports_epg_lookup_aug_12_jul_13 as b
--on a.dk_programme_instance_dim=b.dk_programme_instance_dim
group by account_number
;
commit;

---Add these variables on to main table---

alter table v223_unbundling_viewing_summary_by_account add 
(viewing_duration_sky_sports_football integer
,viewing_duration_non_sky_sports_football integer
,programmes_3min_plus_sky_sports_football integer
,programmes_3min_plus_non_sky_sports_football integer
,programmes_engaged_sky_sports_football integer
,programmes_engaged_non_sky_sports_football integer

,viewing_duration_sky_sports_exc_wwe integer
,programmes_3min_plus_sky_sports_exc_wwe integer
,programmes_engaged_sky_sports_exc_wwe integer
)
;
commit;

--alter table v223_unbundling_viewing_summary_by_account delete  programmes_engaged_sky_sports__exc_wwe; commit;
--alter table v223_unbundling_viewing_summary_by_account add  programmes_engaged_sky_sports_exc_wwe integer; commit;


commit;
CREATE HG INDEX idx1 ON v223_unbundling_viewing_summary_by_account_phase_2 (account_number);

update v223_unbundling_viewing_summary_by_account
set viewing_duration_sky_sports_football =b.viewing_duration_sky_sports_football
,viewing_duration_non_sky_sports_football =b.viewing_duration_non_sky_sports_football
,programmes_3min_plus_sky_sports_football =b.programmes_3min_plus_sky_sports_football
,programmes_3min_plus_non_sky_sports_football =b.programmes_3min_plus_non_sky_sports_football
,programmes_engaged_sky_sports_football =b.programmes_engaged_sky_sports_football
,programmes_engaged_non_sky_sports_football =b.programmes_engaged_non_sky_sports_football
from v223_unbundling_viewing_summary_by_account as a
left outer join v223_unbundling_viewing_summary_by_account_phase_2 as b
on a.account_number = b.account_number
;


update v223_unbundling_viewing_summary_by_account
 set viewing_duration_sky_sports_exc_wwe =viewing_duration_Sky_Sports_total-viewing_duration_Sky_Sports_WWE
,programmes_3min_plus_sky_sports_exc_wwe =programmes_3min_plus_Sky_Sports_total-programmes_3min_plus_Sky_Sports_WWE
,programmes_engaged_sky_sports_exc_wwe =programmes_engaged_Sky_Sports_total-programmes_engaged_Sky_Sports_WWE
from v223_unbundling_viewing_summary_by_account
;
commit;

--select top 100 viewing_duration_sky_sports_exc_wwe from v223_unbundling_viewing_summary_by_account

----Update Annualised version of figures---

alter table v223_unbundling_viewing_summary_by_account add 
(minutes_sky_sports_football real
,minutes_non_sky_sports_football real
,minutes_sky_sports_exc_wwe real

,annualised_programmes_3min_plus_sky_sports_football real
,annualised_programmes_3min_plus_non_sky_sports_football real
,annualised_programmes_3min_plus_sky_sports_exc_wwe real

,annualised_programmes_engaged_sky_sports_football real
,annualised_programmes_engaged_non_sky_sports_football real
,annualised_programmes_engaged_sky_sports_exc_wwe real
)
;
commit;

update v223_unbundling_viewing_summary_by_account

set minutes_sky_sports_football =(viewing_duration_sky_sports_football
)/60*(365/cast(days_with_viewing as real))
,minutes_non_sky_sports_football =(viewing_duration_non_sky_sports_football
)/60*(365/cast(days_with_viewing as real))

,annualised_programmes_3min_plus_sky_sports_football 
=programmes_3min_plus_sky_sports_football*(365/cast(days_with_viewing as real))

,annualised_programmes_3min_plus_non_sky_sports_football 
=programmes_3min_plus_non_sky_sports_football*(365/cast(days_with_viewing as real))

,annualised_programmes_engaged_sky_sports_football 
=programmes_engaged_sky_sports_football*(365/cast(days_with_viewing as real))

,annualised_programmes_engaged_non_sky_sports_football
=programmes_engaged_non_sky_sports_football*(365/cast(days_with_viewing as real))

,minutes_sky_sports_exc_wwe =(viewing_duration_sky_sports_exc_wwe
)/60*(365/cast(days_with_viewing as real))
,annualised_programmes_3min_plus_sky_sports_exc_wwe 
=programmes_3min_plus_sky_sports_exc_wwe *(365/cast(days_with_viewing as real))
,annualised_programmes_engaged_sky_sports_exc_wwe 
=programmes_engaged_sky_sports_exc_wwe *(365/cast(days_with_viewing as real))
from v223_unbundling_viewing_summary_by_account
;
commit;

--select top 100 rank_minutes_sky_sports_football from v223_unbundling_viewing_summary_by_account


alter table v223_unbundling_viewing_summary_by_account add 
(
rank_minutes_sky_sports_football integer
,rank_minutes_non_sky_sports_football integer
,rank_prog_3min_plus_sky_sports_football integer
,rank_prog_3min_plus_non_sky_sports_football integer
,rank_prog_engaged_sky_sports_football integer
,rank_prog_engaged_non_sky_sports_football integer

,rank_minutes_sky_sports_exc_wwe integer
,rank_prog_3min_plus_sky_sports_exc_wwe integer
,rank_prog_engaged_sky_sports_exc_wwe integer
)
;
commit;
select account_number

,rank() over (  ORDER BY  minutes_sky_sports_football desc) as rank_minutes_sky_sports_football
,rank() over (  ORDER BY  minutes_non_sky_sports_football desc) as rank_minutes_non_sky_sports_football
,rank() over (  ORDER BY  minutes_sky_sports_exc_wwe desc) as rank_minutes_sky_sports_exc_wwe

,rank() over (  ORDER BY annualised_programmes_3min_plus_sky_sports_football   desc ,  minutes_sky_sports_football desc) as rank_prog_3min_plus_sky_sports_football
,rank() over (  ORDER BY annualised_programmes_3min_plus_non_sky_sports_football   desc ,  minutes_non_sky_sports_football desc) as rank_prog_3min_plus_non_sky_sports_football
,rank() over (  ORDER BY annualised_programmes_3min_plus_sky_sports_exc_wwe   desc ,  minutes_sky_sports_exc_wwe desc) as rank_prog_3min_plus_sky_sports_exc_wwe

,rank() over (  ORDER BY annualised_programmes_engaged_sky_sports_football  desc ,  minutes_sky_sports_football desc) as rank_prog_engaged_sky_sports_football
,rank() over (  ORDER BY annualised_programmes_engaged_non_sky_sports_football  desc ,  minutes_non_sky_sports_football desc) as rank_prog_engaged_non_sky_sports_football
,rank() over (  ORDER BY annualised_programmes_engaged_sky_sports_exc_wwe  desc ,  minutes_sky_sports_exc_wwe desc) as rank_prog_engaged_sky_sports_exc_wwe

into #rank_minutes_details
from v223_unbundling_viewing_summary_by_account
where days_with_viewing>=280
;
commit;

--select top 500 * from #rank_minutes_details;
exec sp_create_tmp_table_idx '#rank_minutes_details', 'account_number';
commit;


update v223_unbundling_viewing_summary_by_account
set 
rank_minutes_sky_sports_football=b.rank_minutes_sky_sports_football
,rank_minutes_non_sky_sports_football=b.rank_minutes_non_sky_sports_football
,rank_minutes_sky_sports_exc_wwe=b.rank_minutes_sky_sports_exc_wwe

,rank_prog_3min_plus_sky_sports_football=b.rank_prog_3min_plus_sky_sports_football
,rank_prog_3min_plus_non_sky_sports_football=b.rank_prog_3min_plus_non_sky_sports_football
,rank_prog_3min_plus_sky_sports_exc_wwe=b.rank_prog_3min_plus_sky_sports_exc_wwe

,rank_prog_engaged_sky_sports_football=b.rank_prog_engaged_sky_sports_football
,rank_prog_engaged_non_sky_sports_football=b.rank_prog_engaged_non_sky_sports_football
,rank_prog_engaged_sky_sports_exc_wwe=b.rank_prog_engaged_sky_sports_exc_wwe

from v223_unbundling_viewing_summary_by_account as a
left outer join #rank_minutes_details as b
on a.account_number = b.account_number
;

commit;


---Add on Percentiles and Deciles--

alter table v223_unbundling_viewing_summary_by_account add
(percentile_minutes_sky_sports_football integer
,percentile_minutes_non_sky_sports_football  integer
,percentile_minutes_sky_sports_exc_wwe  integer

,decile_minutes_sky_sports_football integer
,decile_minutes_non_sky_sports_football  integer
,decile_minutes_sky_sports_exc_wwe  integer


,percentile_prog_3min_plus_sky_sports_football integer
,percentile_prog_3min_plus_non_sky_sports_football  integer
,percentile_prog_3min_plus_sky_sports_exc_wwe  integer

,decile_prog_3min_plus_sky_sports_football integer
,decile_prog_3min_plus_non_sky_sports_football  integer
,decile_prog_3min_plus_sky_sports_exc_wwe  integer


,percentile_prog_engaged_sky_sports_football integer
,percentile_prog_engaged_non_sky_sports_football  integer
,percentile_prog_engaged_sky_sports_exc_wwe  integer

,decile_prog_engaged_sky_sports_football integer
,decile_prog_engaged_non_sky_sports_football  integer
,decile_prog_engaged_sky_sports_exc_wwe  integer

)
;





update v223_unbundling_viewing_summary_by_account 
set  
percentile_minutes_sky_sports_football =case when minutes_sky_sports_football =-1 then 999 when minutes_sky_sports_football =0 then 100 else abs(rank_minutes_sky_sports_football /3000)+1 end
,percentile_minutes_non_sky_sports_football =case when minutes_non_sky_sports_football =-1 then 999 when minutes_non_sky_sports_football =0 then 100 else abs(rank_minutes_non_sky_sports_football /3000)+1 end
,percentile_minutes_sky_sports_exc_wwe =case when minutes_sky_sports_exc_wwe =-1 then 999 when minutes_sky_sports_exc_wwe =0 then 100 else abs(rank_minutes_sky_sports_exc_wwe /3000)+1 end

,decile_minutes_sky_sports_football =case when minutes_sky_sports_football =-1 then 99 when minutes_sky_sports_football =0 then 10 else abs(rank_minutes_sky_sports_football /30000)+1 end
,decile_minutes_non_sky_sports_football =case when minutes_non_sky_sports_football =-1 then 99 when minutes_non_sky_sports_football =0 then 10 else abs(rank_minutes_non_sky_sports_football /30000)+1 end
,decile_minutes_sky_sports_exc_wwe =case when minutes_sky_sports_exc_wwe =-1 then 99 when minutes_sky_sports_exc_wwe =0 then 10 else abs(rank_minutes_sky_sports_exc_wwe /30000)+1 end


,percentile_prog_3min_plus_sky_sports_football =case when annualised_programmes_3min_plus_sky_sports_football =-1 then 999 when annualised_programmes_3min_plus_sky_sports_football =0 then 100 else abs(rank_prog_3min_plus_sky_sports_football /3000)+1 end
,percentile_prog_3min_plus_non_sky_sports_football =case when annualised_programmes_3min_plus_non_sky_sports_football =-1 then 999 when annualised_programmes_3min_plus_non_sky_sports_football =0 then 100 else abs(rank_prog_3min_plus_non_sky_sports_football /3000)+1 end
,percentile_prog_3min_plus_sky_sports_exc_wwe =case when annualised_programmes_3min_plus_sky_sports_exc_wwe =-1 then 999 when annualised_programmes_3min_plus_sky_sports_exc_wwe =0 then 100 else abs(rank_prog_3min_plus_sky_sports_exc_wwe /3000)+1 end

,decile_prog_3min_plus_sky_sports_football =case when annualised_programmes_3min_plus_sky_sports_football =-1 then 99 when annualised_programmes_3min_plus_sky_sports_football =0 then 10 else abs(rank_prog_3min_plus_sky_sports_football /30000)+1 end
,decile_prog_3min_plus_non_sky_sports_football =case when annualised_programmes_3min_plus_non_sky_sports_football =-1 then 99 when annualised_programmes_3min_plus_non_sky_sports_football =0 then 10 else abs(rank_prog_3min_plus_non_sky_sports_football /30000)+1 end
,decile_prog_3min_plus_sky_sports_exc_wwe =case when annualised_programmes_3min_plus_sky_sports_exc_wwe =-1 then 99 when annualised_programmes_3min_plus_sky_sports_exc_wwe =0 then 10 else abs(rank_prog_3min_plus_sky_sports_exc_wwe /30000)+1 end

,percentile_prog_engaged_sky_sports_football =case when annualised_programmes_engaged_sky_sports_football =-1 then 999 when annualised_programmes_engaged_sky_sports_football =0 then 100 else abs(rank_prog_engaged_sky_sports_football /3000)+1 end
,percentile_prog_engaged_non_sky_sports_football =case when annualised_programmes_engaged_non_sky_sports_football =-1 then 999 when annualised_programmes_engaged_non_sky_sports_football =0 then 100 else abs(rank_prog_engaged_non_sky_sports_football /3000)+1 end
,percentile_prog_engaged_sky_sports_exc_wwe =case when annualised_programmes_engaged_sky_sports_exc_wwe =-1 then 999 when annualised_programmes_engaged_sky_sports_exc_wwe =0 then 100 else abs(rank_prog_engaged_sky_sports_exc_wwe /3000)+1 end

,decile_prog_engaged_sky_sports_football =case when annualised_programmes_engaged_sky_sports_football =-1 then 99 when annualised_programmes_engaged_sky_sports_football =0 then 10 else abs(rank_prog_engaged_sky_sports_football /30000)+1 end
,decile_prog_engaged_non_sky_sports_football =case when annualised_programmes_engaged_non_sky_sports_football =-1 then 99 when annualised_programmes_engaged_non_sky_sports_football =0 then 10 else abs(rank_prog_engaged_non_sky_sports_football /30000)+1 end
,decile_prog_engaged_sky_sports_exc_wwe =case when annualised_programmes_engaged_sky_sports_exc_wwe =-1 then 99 when annualised_programmes_engaged_sky_sports_exc_wwe =0 then 10 else abs(rank_prog_engaged_sky_sports_exc_wwe /30000)+1 end

from v223_unbundling_viewing_summary_by_account 
;

commit;


----Create New Percentile/Decile calculations----


--Create Macro variables for number of records that have #3min progs >0

create variable @total_programmes_3min_plus_sport integer;
set @total_programmes_3min_plus_sport
=( select count(*) from v223_unbundling_viewing_summary_by_account where annualised_programmes_3min_plus_sport>0);

create variable @total_programmes_3min_plus_sky_sports_football integer;
set @total_programmes_3min_plus_sky_sports_football
=( select count(*) from v223_unbundling_viewing_summary_by_account where annualised_programmes_3min_plus_sky_sports_football>0);

create variable @total_programmes_3min_plus_non_sky_sports_football integer;
set @total_programmes_3min_plus_non_sky_sports_football
=( select count(*) from v223_unbundling_viewing_summary_by_account where annualised_programmes_3min_plus_non_sky_sports_football>0);


create variable @total_programmes_3min_plus_golf_sky_sports integer;
set @total_programmes_3min_plus_golf_sky_sports
=( select count(*) from v223_unbundling_viewing_summary_by_account where annualised_programmes_3min_plus_golf_sky_sports>0);

create variable @total_programmes_3min_plus_golf_non_sky_sports integer;
set @total_programmes_3min_plus_golf_non_sky_sports
=( select count(*) from v223_unbundling_viewing_summary_by_account where annualised_programmes_3min_plus_golf_non_sky_sports>0);


create variable @total_programmes_3min_plus_rugby_sky_sports integer;
set @total_programmes_3min_plus_rugby_sky_sports
=( select count(*) from v223_unbundling_viewing_summary_by_account where annualised_programmes_3min_plus_rugby_sky_sports>0);

create variable @total_programmes_3min_plus_rugby_non_sky_sports integer;
set @total_programmes_3min_plus_rugby_non_sky_sports
=( select count(*) from v223_unbundling_viewing_summary_by_account where annualised_programmes_3min_plus_rugby_non_sky_sports>0);


create variable @total_programmes_3min_plus_cricket_sky_sports integer;
set @total_programmes_3min_plus_cricket_sky_sports
=( select count(*) from v223_unbundling_viewing_summary_by_account where annualised_programmes_3min_plus_Sky_Sports_cricket_overall>0);

create variable @total_programmes_3min_plus_cricket_non_sky_sports integer;
set @total_programmes_3min_plus_cricket_non_sky_sports
=( select count(*) from v223_unbundling_viewing_summary_by_account where annualised_programmes_3min_plus_non_Sky_Sports_cricket_overall>0);


create variable @total_programmes_3min_plus_tennis_sky_sports integer;
set @total_programmes_3min_plus_tennis_sky_sports
=( select count(*) from v223_unbundling_viewing_summary_by_account where annualised_programmes_3min_plus_tennis_sky_sports>0);

create variable @total_programmes_3min_plus_tennis_non_sky_sports integer;
set @total_programmes_3min_plus_tennis_non_sky_sports
=( select count(*) from v223_unbundling_viewing_summary_by_account where annualised_programmes_3min_plus_tennis_non_sky_sports>0);


create variable @total_programmes_3min_plus_f1_sky_sports integer;
set @total_programmes_3min_plus_f1_sky_sports
=( select count(*) from v223_unbundling_viewing_summary_by_account where annualised_programmes_3min_plus_f1_sky_sports>0);

create variable @total_programmes_3min_plus_f1_non_sky_sports integer;
set @total_programmes_3min_plus_f1_non_sky_sports
=( select count(*) from v223_unbundling_viewing_summary_by_account where annualised_programmes_3min_plus_f1_non_sky_sports>0);


commit;
create variable @total_programmes_3min_plus_sport_sky_sports integer;
set @total_programmes_3min_plus_sport_sky_sports
=( select count(*) from v223_unbundling_viewing_summary_by_account where annualised_programmes_3min_plus_sport_sky_sports>0);



alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_engaged_sport integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_engaged_sport integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_engaged_football_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_engaged_football_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_engaged_football_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_engaged_football_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_engaged_rugby_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_engaged_rugby_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_engaged_rugby_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_engaged_rugby_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_engaged_cricket_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_engaged_cricket_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_engaged_cricket_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_engaged_cricket_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_engaged_F1_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_engaged_F1_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_engaged_F1_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_engaged_F1_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_engaged_Golf_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_engaged_Golf_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_engaged_Golf_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_engaged_Golf_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_engaged_Tennis_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_engaged_Tennis_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_engaged_Tennis_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_engaged_Tennis_non_sky_sports integer;



alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_engaged_sport_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_engaged_sport_sky_sports integer;

update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_engaged_sport_sky_sports = case when annualised_programmes_3min_plus_sport_sky_sports=0 then 999 
when abs(rank_prog_engaged_sport_sky_sports/(@total_programmes_3min_plus_sport_sky_sports/100))+1 >100 then 100 
else abs(rank_prog_engaged_sport_sky_sports/(@total_programmes_3min_plus_sport_sky_sports/100))+1 end
,phase_2_decile_engaged_sport_sky_sports = case when annualised_programmes_3min_plus_sport_sky_sports=0 then 99 
when abs(rank_prog_engaged_sport_sky_sports/(@total_programmes_3min_plus_sport_sky_sports/10))+1 >10 then 10 
else 
abs(rank_prog_engaged_sport_sky_sports/(@total_programmes_3min_plus_sport_sky_sports/10))+1 end
;
commit;



update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_engaged_sport = case when annualised_programmes_3min_plus_sport=0 then 999 
when abs(rank_prog_engaged_sport/(@total_programmes_3min_plus_sport/100))+1 >100 then 100 
else abs(rank_prog_engaged_sport/(@total_programmes_3min_plus_sport/100))+1 end
,phase_2_decile_engaged_sport = case when annualised_programmes_3min_plus_sport=0 then 99 
when abs(rank_prog_engaged_sport/(@total_programmes_3min_plus_sport/10))+1 >10 then 10 
else 
abs(rank_prog_engaged_sport/(@total_programmes_3min_plus_sport/10))+1 end
;
commit;

--@total_programmes_3min_plus_sky_sports_football
update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_engaged_football_sky_sports = case when annualised_programmes_3min_plus_sky_sports_football=0 then 999 
when abs(rank_prog_engaged_sky_sports_football/(@total_programmes_3min_plus_sky_sports_football/100))+1 >100 then 100 
else 
abs(rank_prog_engaged_sky_sports_football/(@total_programmes_3min_plus_sky_sports_football/100))+1 end
,phase_2_decile_engaged_football_sky_sports = case when annualised_programmes_3min_plus_sky_sports_football=0 then 99 
when abs(rank_prog_engaged_sky_sports_football/(@total_programmes_3min_plus_sky_sports_football/10))+1 >10 then 10 
else 
abs(rank_prog_engaged_sky_sports_football/(@total_programmes_3min_plus_sky_sports_football/10))+1 end
;
commit;


update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_engaged_football_non_sky_sports = case when annualised_programmes_3min_plus_non_sky_sports_football=0 then 999 
when abs(rank_prog_engaged_non_sky_sports_football/(@total_programmes_3min_plus_non_sky_sports_football/100))+1 >100 then 100 else 
abs(rank_prog_engaged_non_sky_sports_football/(@total_programmes_3min_plus_non_sky_sports_football/100))+1 end
,phase_2_decile_engaged_football_non_sky_sports = case when annualised_programmes_3min_plus_non_sky_sports_football=0 then 99 
when abs(rank_prog_engaged_non_sky_sports_football/(@total_programmes_3min_plus_non_sky_sports_football/10))+1 >10 then 10 else 
abs(rank_prog_engaged_non_sky_sports_football/(@total_programmes_3min_plus_non_sky_sports_football/10))+1 end
;
commit;



--rugby

update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_engaged_rugby_sky_sports = case when annualised_programmes_3min_plus_rugby_sky_sports=0 then 999 
when abs(rank_prog_engaged_rugby_sky_sports/(@total_programmes_3min_plus_rugby_sky_sports/100))+1 >100 then 100 else 
abs(rank_prog_engaged_rugby_sky_sports/(@total_programmes_3min_plus_rugby_sky_sports/100))+1 end
,phase_2_decile_engaged_rugby_sky_sports = case when annualised_programmes_3min_plus_rugby_sky_sports=0 then 99 
when abs(rank_prog_engaged_rugby_sky_sports/(@total_programmes_3min_plus_rugby_sky_sports/10))+1 >10 then 10 else  
abs(rank_prog_engaged_rugby_sky_sports/(@total_programmes_3min_plus_rugby_sky_sports/10))+1 end
;
commit;


update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_engaged_rugby_non_sky_sports = case when annualised_programmes_3min_plus_rugby_non_sky_sports=0 then 999 
when abs(rank_prog_engaged_rugby_non_sky_sports/(@total_programmes_3min_plus_rugby_non_sky_sports/100))+1 >100 then 100 else 
abs(rank_prog_engaged_rugby_non_sky_sports/(@total_programmes_3min_plus_rugby_non_sky_sports/100))+1 end
,phase_2_decile_engaged_rugby_non_sky_sports = case when annualised_programmes_3min_plus_rugby_non_sky_sports=0 then 99 
when abs(rank_prog_engaged_rugby_non_sky_sports/(@total_programmes_3min_plus_rugby_non_sky_sports/10))+1 >10 then 10 else 
 
abs(rank_prog_engaged_rugby_non_sky_sports/(@total_programmes_3min_plus_rugby_non_sky_sports/10))+1 end
;
commit;

---cricket



update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_engaged_cricket_sky_sports = case when annualised_programmes_3min_plus_Sky_Sports_cricket_overall=0 then 999 
when abs(rank_prog_engaged_Sky_Sports_cricket_overall/(@total_programmes_3min_plus_cricket_sky_sports/100))+1 >100 then 100 else 
abs(rank_prog_engaged_Sky_Sports_cricket_overall/(@total_programmes_3min_plus_cricket_sky_sports/100))+1 end
,phase_2_decile_engaged_cricket_sky_sports = case when annualised_programmes_3min_plus_Sky_Sports_cricket_overall=0 then 99  
when abs(rank_prog_engaged_Sky_Sports_cricket_overall/(@total_programmes_3min_plus_cricket_sky_sports/10))+1 >10 then 10 else 

abs(rank_prog_engaged_Sky_Sports_cricket_overall/(@total_programmes_3min_plus_cricket_sky_sports/10))+1 end
;
commit;


update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_engaged_cricket_non_sky_sports = case when annualised_programmes_3min_plus_non_Sky_Sports_cricket_overall=0 then 999 
when abs(rank_prog_engaged_non_Sky_Sports_cricket_overall/(@total_programmes_3min_plus_cricket_non_sky_sports/100))+1 >100 then 100 else 
abs(rank_prog_engaged_non_Sky_Sports_cricket_overall/(@total_programmes_3min_plus_cricket_non_sky_sports/100))+1 end
,phase_2_decile_engaged_cricket_non_sky_sports = case when annualised_programmes_3min_plus_non_Sky_Sports_cricket_overall=0 then 99  
when abs(rank_prog_engaged_non_Sky_Sports_cricket_overall/(@total_programmes_3min_plus_cricket_non_sky_sports/10))+1 >10 then 10 else 

abs(rank_prog_engaged_non_Sky_Sports_cricket_overall/(@total_programmes_3min_plus_cricket_non_sky_sports/10))+1 end
;
commit;

--F1

update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_engaged_F1_sky_sports = case when annualised_programmes_3min_plus_F1_sky_sports=0 then 999 
when abs(rank_prog_engaged_F1_sky_sports/(@total_programmes_3min_plus_F1_sky_sports/100))+1 >100 then 100 else 
abs(rank_prog_engaged_F1_sky_sports/(@total_programmes_3min_plus_F1_sky_sports/100))+1 end
,phase_2_decile_engaged_F1_sky_sports = case when annualised_programmes_3min_plus_F1_sky_sports=0 then 99 
when abs(rank_prog_engaged_F1_sky_sports/(@total_programmes_3min_plus_F1_sky_sports/10))+1 >10 then 10 else 
 
abs(rank_prog_engaged_F1_sky_sports/(@total_programmes_3min_plus_F1_sky_sports/10))+1 end
;
commit;


update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_engaged_F1_non_sky_sports = case when annualised_programmes_3min_plus_F1_non_sky_sports=0 then 999 
when abs(rank_prog_engaged_F1_non_sky_sports/(@total_programmes_3min_plus_F1_non_sky_sports/100))+1 >100 then 100 else 
abs(rank_prog_engaged_F1_non_sky_sports/(@total_programmes_3min_plus_F1_non_sky_sports/100))+1 end
,phase_2_decile_engaged_F1_non_sky_sports = case when annualised_programmes_3min_plus_F1_non_sky_sports=0 then 99  
when abs(rank_prog_engaged_F1_non_sky_sports/(@total_programmes_3min_plus_F1_non_sky_sports/10))+1 >10 then 10 else 

abs(rank_prog_engaged_F1_non_sky_sports/(@total_programmes_3min_plus_F1_non_sky_sports/10))+1 end
;
commit;


--Golf

update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_engaged_Golf_sky_sports = case when annualised_programmes_3min_plus_Golf_sky_sports=0 then 999 
when abs(rank_prog_engaged_Golf_sky_sports/(@total_programmes_3min_plus_Golf_sky_sports/100))+1 >100 then 100 else 
abs(rank_prog_engaged_Golf_sky_sports/(@total_programmes_3min_plus_Golf_sky_sports/100))+1 end
,phase_2_decile_engaged_Golf_sky_sports = case when annualised_programmes_3min_plus_Golf_sky_sports=0 then 99  
when abs(rank_prog_engaged_Golf_sky_sports/(@total_programmes_3min_plus_Golf_sky_sports/10))+1 >10 then 10 else 

abs(rank_prog_engaged_Golf_sky_sports/(@total_programmes_3min_plus_Golf_sky_sports/10))+1 end
;
commit;


update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_engaged_Golf_non_sky_sports = case when annualised_programmes_3min_plus_Golf_non_sky_sports=0 then 999 
when abs(rank_prog_engaged_Golf_non_sky_sports/(@total_programmes_3min_plus_Golf_non_sky_sports/100))+1 >100 then 100 else 
abs(rank_prog_engaged_Golf_non_sky_sports/(@total_programmes_3min_plus_Golf_non_sky_sports/100))+1 end
,phase_2_decile_engaged_Golf_non_sky_sports = case when annualised_programmes_3min_plus_Golf_non_sky_sports=0 then 99 
when abs(rank_prog_engaged_Golf_non_sky_sports/(@total_programmes_3min_plus_Golf_non_sky_sports/10))+1 >10 then 10 else 
 
abs(rank_prog_engaged_Golf_non_sky_sports/(@total_programmes_3min_plus_Golf_non_sky_sports/10))+1 end
;
commit;


---Tennis

update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_engaged_Tennis_sky_sports = case when annualised_programmes_3min_plus_Tennis_sky_sports=0 then 999 
when abs(rank_prog_engaged_Tennis_sky_sports/(@total_programmes_3min_plus_Tennis_sky_sports/100))+1 >100 then 100 else 
abs(rank_prog_engaged_Tennis_sky_sports/(@total_programmes_3min_plus_Tennis_sky_sports/100))+1 end
,phase_2_decile_engaged_Tennis_sky_sports = case when annualised_programmes_3min_plus_Tennis_sky_sports=0 then 99 
when abs(rank_prog_engaged_Tennis_sky_sports/(@total_programmes_3min_plus_Tennis_sky_sports/10))+1 >10 then 10 else 
 
abs(rank_prog_engaged_Tennis_sky_sports/(@total_programmes_3min_plus_Tennis_sky_sports/10))+1 end
;
commit;


update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_engaged_Tennis_non_sky_sports = case when annualised_programmes_3min_plus_Tennis_non_sky_sports=0 then 999 
when abs(rank_prog_engaged_Tennis_non_sky_sports/(@total_programmes_3min_plus_Tennis_non_sky_sports/100))+1 >100 then 100 else 
abs(rank_prog_engaged_Tennis_non_sky_sports/(@total_programmes_3min_plus_Tennis_non_sky_sports/100))+1 end
,phase_2_decile_engaged_Tennis_non_sky_sports = case when annualised_programmes_3min_plus_Tennis_non_sky_sports=0 then 99 
when abs(rank_prog_engaged_Tennis_non_sky_sports/(@total_programmes_3min_plus_Tennis_non_sky_sports/10))+1 >10 then 10 else 
 
abs(rank_prog_engaged_Tennis_non_sky_sports/(@total_programmes_3min_plus_Tennis_non_sky_sports/10))+1 end
;
commit;


---repeat Phase 2 Deciles/Percentiles but for 3min+ Viewing---

commit;


alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_3min_plus_sport integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_3min_plus_sport integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_3min_plus_football_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_3min_plus_football_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_3min_plus_football_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_3min_plus_football_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_3min_plus_rugby_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_3min_plus_rugby_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_3min_plus_rugby_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_3min_plus_rugby_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_3min_plus_cricket_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_3min_plus_cricket_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_3min_plus_cricket_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_3min_plus_cricket_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_3min_plus_F1_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_3min_plus_F1_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_3min_plus_F1_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_3min_plus_F1_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_3min_plus_Golf_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_3min_plus_Golf_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_3min_plus_Golf_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_3min_plus_Golf_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_3min_plus_Tennis_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_3min_plus_Tennis_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_3min_plus_Tennis_non_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_3min_plus_Tennis_non_sky_sports integer;


alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_3min_plus_sport_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_3min_plus_sport_sky_sports integer;


update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_3min_plus_sport_sky_sports = case when annualised_programmes_3min_plus_sport_sky_sports=0 then 999 
when abs(rank_prog_3min_plus_sport_sky_sports/(@total_programmes_3min_plus_sport_sky_sports/100))+1 >100 then 100 
else abs(rank_prog_3min_plus_sport_sky_sports/(@total_programmes_3min_plus_sport_sky_sports/100))+1 end
,phase_2_decile_3min_plus_sport_sky_sports = case when annualised_programmes_3min_plus_sport_sky_sports=0 then 99 
when abs(rank_prog_3min_plus_sport_sky_sports/(@total_programmes_3min_plus_sport_sky_sports/10))+1 >10 then 10 
else 
abs(rank_prog_3min_plus_sport_sky_sports/(@total_programmes_3min_plus_sport_sky_sports/10))+1 end
;
commit;

update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_3min_plus_sport = case when annualised_programmes_3min_plus_sport=0 then 999 
when abs(rank_prog_3min_plus_sport/(@total_programmes_3min_plus_sport/100))+1 >100 then 100 
else abs(rank_prog_3min_plus_sport/(@total_programmes_3min_plus_sport/100))+1 end
,phase_2_decile_3min_plus_sport = case when annualised_programmes_3min_plus_sport=0 then 99 
when abs(rank_prog_3min_plus_sport/(@total_programmes_3min_plus_sport/10))+1 >10 then 10 
else 
abs(rank_prog_3min_plus_sport/(@total_programmes_3min_plus_sport/10))+1 end
;
commit;

--@total_programmes_3min_plus_sky_sports_football
update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_3min_plus_football_sky_sports = case when annualised_programmes_3min_plus_sky_sports_football=0 then 999 
when abs(rank_prog_3min_plus_sky_sports_football/(@total_programmes_3min_plus_sky_sports_football/100))+1 >100 then 100 
else 
abs(rank_prog_3min_plus_sky_sports_football/(@total_programmes_3min_plus_sky_sports_football/100))+1 end
,phase_2_decile_3min_plus_football_sky_sports = case when annualised_programmes_3min_plus_sky_sports_football=0 then 99 
when abs(rank_prog_3min_plus_sky_sports_football/(@total_programmes_3min_plus_sky_sports_football/10))+1 >10 then 10 
else 
abs(rank_prog_3min_plus_sky_sports_football/(@total_programmes_3min_plus_sky_sports_football/10))+1 end
;
commit;


update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_3min_plus_football_non_sky_sports = case when annualised_programmes_3min_plus_non_sky_sports_football=0 then 999 
when abs(rank_prog_3min_plus_non_sky_sports_football/(@total_programmes_3min_plus_non_sky_sports_football/100))+1 >100 then 100 else 
abs(rank_prog_3min_plus_non_sky_sports_football/(@total_programmes_3min_plus_non_sky_sports_football/100))+1 end
,phase_2_decile_3min_plus_football_non_sky_sports = case when annualised_programmes_3min_plus_non_sky_sports_football=0 then 99 
when abs(rank_prog_3min_plus_non_sky_sports_football/(@total_programmes_3min_plus_non_sky_sports_football/10))+1 >10 then 10 else 
abs(rank_prog_3min_plus_non_sky_sports_football/(@total_programmes_3min_plus_non_sky_sports_football/10))+1 end
;
commit;



--rugby

update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_3min_plus_rugby_sky_sports = case when annualised_programmes_3min_plus_rugby_sky_sports=0 then 999 
when abs(rank_prog_3min_plus_rugby_sky_sports/(@total_programmes_3min_plus_rugby_sky_sports/100))+1 >100 then 100 else 
abs(rank_prog_3min_plus_rugby_sky_sports/(@total_programmes_3min_plus_rugby_sky_sports/100))+1 end
,phase_2_decile_3min_plus_rugby_sky_sports = case when annualised_programmes_3min_plus_rugby_sky_sports=0 then 99 
when abs(rank_prog_3min_plus_rugby_sky_sports/(@total_programmes_3min_plus_rugby_sky_sports/10))+1 >10 then 10 else  
abs(rank_prog_3min_plus_rugby_sky_sports/(@total_programmes_3min_plus_rugby_sky_sports/10))+1 end
;
commit;


update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_3min_plus_rugby_non_sky_sports = case when annualised_programmes_3min_plus_rugby_non_sky_sports=0 then 999 
when abs(rank_prog_3min_plus_rugby_non_sky_sports/(@total_programmes_3min_plus_rugby_non_sky_sports/100))+1 >100 then 100 else 
abs(rank_prog_3min_plus_rugby_non_sky_sports/(@total_programmes_3min_plus_rugby_non_sky_sports/100))+1 end
,phase_2_decile_3min_plus_rugby_non_sky_sports = case when annualised_programmes_3min_plus_rugby_non_sky_sports=0 then 99 
when abs(rank_prog_3min_plus_rugby_non_sky_sports/(@total_programmes_3min_plus_rugby_non_sky_sports/10))+1 >10 then 10 else 
 
abs(rank_prog_3min_plus_rugby_non_sky_sports/(@total_programmes_3min_plus_rugby_non_sky_sports/10))+1 end
;
commit;

---cricket



update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_3min_plus_cricket_sky_sports = case when annualised_programmes_3min_plus_Sky_Sports_cricket_overall=0 then 999 
when abs(rank_prog_3min_plus_Sky_Sports_cricket_overall/(@total_programmes_3min_plus_cricket_sky_sports/100))+1 >100 then 100 else 
abs(rank_prog_3min_plus_Sky_Sports_cricket_overall/(@total_programmes_3min_plus_cricket_sky_sports/100))+1 end
,phase_2_decile_3min_plus_cricket_sky_sports = case when annualised_programmes_3min_plus_Sky_Sports_cricket_overall=0 then 99  
when abs(rank_prog_3min_plus_Sky_Sports_cricket_overall/(@total_programmes_3min_plus_cricket_sky_sports/10))+1 >10 then 10 else 

abs(rank_prog_3min_plus_Sky_Sports_cricket_overall/(@total_programmes_3min_plus_cricket_sky_sports/10))+1 end
;
commit;


update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_3min_plus_cricket_non_sky_sports = case when annualised_programmes_3min_plus_non_Sky_Sports_cricket_overall=0 then 999 
when abs(rank_prog_3min_plus_non_Sky_Sports_cricket_overall/(@total_programmes_3min_plus_cricket_non_sky_sports/100))+1 >100 then 100 else 
abs(rank_prog_3min_plus_non_Sky_Sports_cricket_overall/(@total_programmes_3min_plus_cricket_non_sky_sports/100))+1 end
,phase_2_decile_3min_plus_cricket_non_sky_sports = case when annualised_programmes_3min_plus_non_Sky_Sports_cricket_overall=0 then 99  
when abs(rank_prog_3min_plus_non_Sky_Sports_cricket_overall/(@total_programmes_3min_plus_cricket_non_sky_sports/10))+1 >10 then 10 else 

abs(rank_prog_3min_plus_non_Sky_Sports_cricket_overall/(@total_programmes_3min_plus_cricket_non_sky_sports/10))+1 end
;
commit;

--F1

update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_3min_plus_F1_sky_sports = case when annualised_programmes_3min_plus_F1_sky_sports=0 then 999 
when abs(rank_prog_3min_plus_F1_sky_sports/(@total_programmes_3min_plus_F1_sky_sports/100))+1 >100 then 100 else 
abs(rank_prog_3min_plus_F1_sky_sports/(@total_programmes_3min_plus_F1_sky_sports/100))+1 end
,phase_2_decile_3min_plus_F1_sky_sports = case when annualised_programmes_3min_plus_F1_sky_sports=0 then 99 
when abs(rank_prog_3min_plus_F1_sky_sports/(@total_programmes_3min_plus_F1_sky_sports/10))+1 >10 then 10 else 
 
abs(rank_prog_3min_plus_F1_sky_sports/(@total_programmes_3min_plus_F1_sky_sports/10))+1 end
;
commit;


update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_3min_plus_F1_non_sky_sports = case when annualised_programmes_3min_plus_F1_non_sky_sports=0 then 999 
when abs(rank_prog_3min_plus_F1_non_sky_sports/(@total_programmes_3min_plus_F1_non_sky_sports/100))+1 >100 then 100 else 
abs(rank_prog_3min_plus_F1_non_sky_sports/(@total_programmes_3min_plus_F1_non_sky_sports/100))+1 end
,phase_2_decile_3min_plus_F1_non_sky_sports = case when annualised_programmes_3min_plus_F1_non_sky_sports=0 then 99  
when abs(rank_prog_3min_plus_F1_non_sky_sports/(@total_programmes_3min_plus_F1_non_sky_sports/10))+1 >10 then 10 else 

abs(rank_prog_3min_plus_F1_non_sky_sports/(@total_programmes_3min_plus_F1_non_sky_sports/10))+1 end
;
commit;


--Golf

update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_3min_plus_Golf_sky_sports = case when annualised_programmes_3min_plus_Golf_sky_sports=0 then 999 
when abs(rank_prog_3min_plus_Golf_sky_sports/(@total_programmes_3min_plus_Golf_sky_sports/100))+1 >100 then 100 else 
abs(rank_prog_3min_plus_Golf_sky_sports/(@total_programmes_3min_plus_Golf_sky_sports/100))+1 end
,phase_2_decile_3min_plus_Golf_sky_sports = case when annualised_programmes_3min_plus_Golf_sky_sports=0 then 99  
when abs(rank_prog_3min_plus_Golf_sky_sports/(@total_programmes_3min_plus_Golf_sky_sports/10))+1 >10 then 10 else 

abs(rank_prog_3min_plus_Golf_sky_sports/(@total_programmes_3min_plus_Golf_sky_sports/10))+1 end
;
commit;


update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_3min_plus_Golf_non_sky_sports = case when annualised_programmes_3min_plus_Golf_non_sky_sports=0 then 999 
when abs(rank_prog_3min_plus_Golf_non_sky_sports/(@total_programmes_3min_plus_Golf_non_sky_sports/100))+1 >100 then 100 else 
abs(rank_prog_3min_plus_Golf_non_sky_sports/(@total_programmes_3min_plus_Golf_non_sky_sports/100))+1 end
,phase_2_decile_3min_plus_Golf_non_sky_sports = case when annualised_programmes_3min_plus_Golf_non_sky_sports=0 then 99 
when abs(rank_prog_3min_plus_Golf_non_sky_sports/(@total_programmes_3min_plus_Golf_non_sky_sports/10))+1 >10 then 10 else 
 
abs(rank_prog_3min_plus_Golf_non_sky_sports/(@total_programmes_3min_plus_Golf_non_sky_sports/10))+1 end
;
commit;


---Tennis

update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_3min_plus_Tennis_sky_sports = case when annualised_programmes_3min_plus_Tennis_sky_sports=0 then 999 
when abs(rank_prog_3min_plus_Tennis_sky_sports/(@total_programmes_3min_plus_Tennis_sky_sports/100))+1 >100 then 100 else 
abs(rank_prog_3min_plus_Tennis_sky_sports/(@total_programmes_3min_plus_Tennis_sky_sports/100))+1 end
,phase_2_decile_3min_plus_Tennis_sky_sports = case when annualised_programmes_3min_plus_Tennis_sky_sports=0 then 99 
when abs(rank_prog_3min_plus_Tennis_sky_sports/(@total_programmes_3min_plus_Tennis_sky_sports/10))+1 >10 then 10 else 
 
abs(rank_prog_3min_plus_Tennis_sky_sports/(@total_programmes_3min_plus_Tennis_sky_sports/10))+1 end
;
commit;


update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_3min_plus_Tennis_non_sky_sports = case when annualised_programmes_3min_plus_Tennis_non_sky_sports=0 then 999 
when abs(rank_prog_3min_plus_Tennis_non_sky_sports/(@total_programmes_3min_plus_Tennis_non_sky_sports/100))+1 >100 then 100 else 
abs(rank_prog_3min_plus_Tennis_non_sky_sports/(@total_programmes_3min_plus_Tennis_non_sky_sports/100))+1 end
,phase_2_decile_3min_plus_Tennis_non_sky_sports = case when annualised_programmes_3min_plus_Tennis_non_sky_sports=0 then 99 
when abs(rank_prog_3min_plus_Tennis_non_sky_sports/(@total_programmes_3min_plus_Tennis_non_sky_sports/10))+1 >10 then 10 else 
 
abs(rank_prog_3min_plus_Tennis_non_sky_sports/(@total_programmes_3min_plus_Tennis_non_sky_sports/10))+1 end
;
commit;


--select top 100 * from v223_unbundling_viewing_summary_by_account;




alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_engaged_sport_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_engaged_sport_sky_sports integer;

update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_engaged_sport_sky_sports = case when annualised_programmes_3min_plus_sport_sky_sports=0 then 999 
when abs(rank_prog_engaged_sport_sky_sports/(@total_programmes_3min_plus_sport_sky_sports/100))+1 >100 then 100 
else abs(rank_prog_engaged_sport_sky_sports/(@total_programmes_3min_plus_sport_sky_sports/100))+1 end
,phase_2_decile_engaged_sport_sky_sports = case when annualised_programmes_3min_plus_sport_sky_sports=0 then 99 
when abs(rank_prog_engaged_sport_sky_sports/(@total_programmes_3min_plus_sport_sky_sports/10))+1 >10 then 10 
else 
abs(rank_prog_engaged_sport_sky_sports/(@total_programmes_3min_plus_sport_sky_sports/10))+1 end
;
commit;

alter table v223_unbundling_viewing_summary_by_account  add phase_2_percentile_3min_plus_sport_sky_sports integer;
alter table v223_unbundling_viewing_summary_by_account  add phase_2_decile_3min_plus_sport_sky_sports integer;


update v223_unbundling_viewing_summary_by_account
set phase_2_percentile_3min_plus_sport_sky_sports = case when annualised_programmes_3min_plus_sport_sky_sports=0 then 999 
when abs(rank_prog_3min_plus_sport_sky_sports/(@total_programmes_3min_plus_sport_sky_sports/100))+1 >100 then 100 
else abs(rank_prog_3min_plus_sport_sky_sports/(@total_programmes_3min_plus_sport_sky_sports/100))+1 end
,phase_2_decile_3min_plus_sport_sky_sports = case when annualised_programmes_3min_plus_sport_sky_sports=0 then 99 
when abs(rank_prog_3min_plus_sport_sky_sports/(@total_programmes_3min_plus_sport_sky_sports/10))+1 >10 then 10 
else 
abs(rank_prog_3min_plus_sport_sky_sports/(@total_programmes_3min_plus_sport_sky_sports/10))+1 end
;
commit;




--select top 10 * from v223_unbundling_viewing_summary_by_account;
--select @total_programmes_3min_plus_Tennis_non_sky_sports
/*
alter table v223_unbundling_viewing_summary_by_account  delete phase_2_percentile_engaged_Tennis_non_sky_sports;
alter table v223_unbundling_viewing_summary_by_account  delete phase_2_decile_engaged_Tennis_non_sky_sports;
commit;
*/



--select @total_programmes_3min_plus_sport
--select decile_prog_engaged_sky_sports_exc_wwe , count(*) from v223_unbundling_viewing_summary_by_account group by decile_prog_engaged_sky_sports_exc_wwe order by decile_prog_engaged_sky_sports_exc_wwe
--select phase_2_percentile_engaged_Football_sky_sports , count(*) from v223_unbundling_viewing_summary_by_account group by phase_2_percentile_engaged_Football_sky_sports order by phase_2_percentile_engaged_Football_sky_sports
--select phase_2_percentile_engaged_Tennis_non_sky_sports , count(*) from v223_unbundling_viewing_summary_by_account group by phase_2_percentile_engaged_Tennis_non_sky_sports order by phase_2_percentile_engaged_Tennis_non_sky_sports

--select phase_2_decile_engaged_Tennis_non_sky_sports , count(*) from v223_unbundling_viewing_summary_by_account group by phase_2_decile_engaged_Tennis_non_sky_sports order by phase_2_decile_engaged_Tennis_non_sky_sports

commit;
--select phase_2_percentile_engaged_sport , count(*) from v223_unbundling_viewing_summary_by_account group by phase_2_percentile_engaged_sport order by phase_2_percentile_engaged_sport
--select phase_2_percentile_3min_plus_sport , count(*) from v223_unbundling_viewing_summary_by_account group by phase_2_percentile_3min_plus_sport order by phase_2_percentile_3min_plus_sport



--select * from v223_unbundling_viewing_summary_by_account where phase_2_percentile_engaged_Football_sky_sports=100

--select @total_programmes_3min_plus_sky_sports_football;
/*
select abs(rank_prog_engaged_sport/(@total_programmes_3min_plus_sport/10))+1 as val2
,rank_prog_engaged_sport
,@total_programmes_3min_plus_sport
from v223_unbundling_viewing_summary_by_account
;
*/

/* select top 500 account_number
,minutes_sky_sports_football_total
,minutes_non_sky_sports_football_total
,annualised_programmes_3min_plus_sky_sports_football_total 
,annualised_programmes_3min_plus_non_sky_sports_football_total
,annualised_programmes_engaged_sky_sports_football_total 
,annualised_programmes_engaged_non_sky_sports_football_total


,phase_2_percentile_engaged_sport 
,phase_2_decile_engaged_sport 
,phase_2_percentile_engaged_football_sky_sports 
,phase_2_decile_engaged_football_sky_sports 
,phase_2_percentile_engaged_football_non_sky_sports 
,phase_2_decile_engaged_football_non_sky_sports 
,phase_2_percentile_engaged_rugby_sky_sports 
,phase_2_decile_engaged_rugby_sky_sports 
,phase_2_percentile_engaged_rugby_non_sky_sports 
,phase_2_decile_engaged_rugby_non_sky_sports 
,phase_2_percentile_engaged_cricket_sky_sports 
,phase_2_decile_engaged_cricket_sky_sports 
,phase_2_percentile_engaged_cricket_non_sky_sports 
,phase_2_decile_engaged_cricket_non_sky_sports 
,phase_2_percentile_engaged_F1_sky_sports 
,phase_2_decile_engaged_F1_sky_sports 
,phase_2_percentile_engaged_F1_non_sky_sports 
,phase_2_decile_engaged_F1_non_sky_sports 
,phase_2_percentile_engaged_Golf_sky_sports 
,phase_2_decile_engaged_Golf_sky_sports 
,phase_2_percentile_engaged_Golf_non_sky_sports 
,phase_2_decile_engaged_Golf_non_sky_sports 
,phase_2_percentile_engaged_Tennis_sky_sports 
,phase_2_decile_engaged_Tennis_sky_sports 
,phase_2_percentile_engaged_Tennis_non_sky_sports 
,phase_2_decile_engaged_Tennis_non_sky_sports
from dbarnett.v223_Unbundling_pivot_activity_data
*/

commit;

















