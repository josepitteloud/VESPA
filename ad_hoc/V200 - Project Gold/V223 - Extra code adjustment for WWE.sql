


update v223_unbundling_viewing_summary_by_account
set programmes_3min_plus_WWE=programmes_3min_plus_Sky_Sports_WWE+programmes_3min_plus_SBO_WWE+programmes_3min_plus_Sky_1_or_2_WWE

,programmes_engaged_WWE=programmes_engaged_Sky_Sports_WWE+programmes_engaged_SBO_WWE+programmes_engaged_Sky_1_or_2_WWE

,annualised_programmes_3min_plus_wwe=annualised_programmes_3min_plus_wwe_sky_sports+annualised_programmes_3min_plus_wwe_sbo
+annualised_programmes_3min_plus_wwe_sky_1_or_2

,annualised_programmes_engaged_wwe=annualised_programmes_engaged_wwe_sky_sports+annualised_programmes_engaged_wwe_sbo+
annualised_programmes_engaged_wwe_sky_1_or_2

from v223_unbundling_viewing_summary_by_account
;
commit;


select account_number
,rank() over (  ORDER BY annualised_programmes_3min_plus_wwe  desc ,  minutes_wwe desc) as rank_prog_3min_plus_wwe
,rank() over (  ORDER BY annualised_programmes_engaged_wwe  desc ,  minutes_wwe desc) as rank_prog_engaged_wwe
into #rank_minutes_details
from v223_unbundling_viewing_summary_by_account
where days_with_viewing>=280
;
commit;


exec sp_create_tmp_table_idx '#rank_minutes_details', 'account_number';
commit;


update v223_unbundling_viewing_summary_by_account
set rank_prog_3min_plus_wwe  =b.rank_prog_3min_plus_wwe 
,rank_prog_engaged_wwe  =b.rank_prog_engaged_wwe 
from v223_unbundling_viewing_summary_by_account as a
left outer join #rank_minutes_details as b
on a.account_number = b.account_number
;

commit;

--select top 500 programmes_3min_plus_WWE ,rank_prog_3min_plus_wwe from  v223_unbundling_viewing_summary_by_account










---Update Ranks  
,rank_prog_3min_plus_wwe integer
,rank_prog_engaged_wwe integer






update v223_unbundling_viewing_summary_by_account
set 





