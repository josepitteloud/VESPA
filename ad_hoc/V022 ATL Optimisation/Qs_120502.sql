  select id
        ,sum(case when lnk.id is null then 0 else 1 end) as vespa_impacts_raw
        ,sum(weightings)                                 as vespa_impacts_weighted
        ,sum(case when hd_model_score       > 0                         then hd_model_score      * weightings * .7194022356133004 else 0 end) as hd_model_score
        ,sum(case when movies_model_score   > 0                         then movies_model_score  * weightings * .9203644912640983 else 0 end) as movies_model_score
    from project_022_accounts_spots_early                   as lnk
         inner join vespa_analysts.project_022_sky_acounts_by_account  as acc on lnk.account_number = acc.account_number
     and (id_type = 'full 10%' or id_type is null)
group by id



select brand,film_code from project_022_all_techedge_spots group by brand,film_code

select id
into vespa_analysts.v022_ids
from project_022_all_techedge_spots
where left(film_code,10) in ('WCRSKYD410','WCRSKYD411','WCRSKYD416','WCRSKYD420','WCRSKYD421','WCRSKYD422','WCRSKYD423')

create hg index idx1 on v022_ids(id);
create hg index idx1 on project_022_accounts_spots_early(id);

select distinct(account_number)
into vespa_analysts.v022_accounts
from project_022_accounts_spots_early as bas
inner join v022_ids as ids on bas.id=ids.id
where id_type = 'full 10%'

alter table vespa_analysts.v022_accounts add barb_zero bit default 0;

  update vespa_analysts.v022_accounts as bas
     set barb_zero = 1
    from project_022_accounts_spots_early as res
         inner join vespa_analysts.project_022_all_techedge_spots as spt on res.id=spt.id
   where bas.account_number = res.account_number
     and spt.tvr=0
     and id_type = 'full 10%'

alter table vespa_analysts.v022_accounts add barb_non_zero bit default 0;

  update vespa_analysts.v022_accounts as bas
     set barb_non_zero = 1
    from project_022_accounts_spots_early as res
         inner join vespa_analysts.project_022_all_techedge_spots as spt on res.id=spt.id
   where bas.account_number = res.account_number
     and spt.tvr > 0
     and id_type = 'full 10%'

  select count(1)
        ,sum(barb_zero)
        ,sum(case when barb_zero = 1 and barb_non_zero = 0 then 1 else 0 end)
        ,sum(case when barb_non_zero = 0 then 1 else 0 end)
    from v022_accounts

