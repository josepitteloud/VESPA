
---Reworked version of Raghu's DOwngrade Model Code to work within project v250

----Add on Downgrade Model Details----
---Generate table for model creation to use Raghu's downgrade model code---



select account_number
,cast ('2013-10-31' as date) as obs_dt
into dbarnett.v250_gold_downgrades_active_base_vespa
from dbarnett.v250_Account_profiling
;
commit;



----------------------------------------------------------------------------------------------------

--SEGMENTATION IS COMPLETE

-- DATA FOR MODELS STARTS BELOW

----------------------------------------------------------------------------------------------------

alter table dbarnett.v250_gold_downgrades_active_base_vespa
add(    last_active_block date default null,
        num_active_block_L24M integer default 0);


update dbarnett.v250_gold_downgrades_active_base_vespa base
set
base.num_active_block_L24M   = tgt.num_ab_24m
,base.last_active_block      = tgt.last_ab
from
(
select           base.account_number
                ,base.obs_dt
                ,sum(case when tgt.TypeofEvent = 'AB' and tgt.effective_from_dt between DATEADD(MM,-24,base.obs_dt) and base.obs_dt then 1 else 0 end) as num_ab_24m
                ,min(case when tgt.TypeofEvent = 'AB' and tgt.effective_from_dt <= base.obs_dt then tgt.effective_from_dt else null end)                 as last_ab
from yarlagaddar.View_CUST_CHURN_HIST tgt
        inner join
         dbarnett.v250_gold_downgrades_active_base_vespa base on tgt.account_number = base.account_number
         where tgt.effective_from_dt <= base.obs_dt
group by base.account_number, base.obs_dt) as tgt
where   base.account_number = tgt.account_number
and   base.obs_dt = tgt.obs_dt;

commit
;

---------------------------------------------------------------------------------------------------



select  base.account_number,
        base.obs_dt,
        offer_id,
        offer_amount,
        offer_duration_months,
        offer_dim_description,
        offer_end_dt,
        rank() over(partition by base.account_number order by offer_amount asc) as frank
into    -- drop table
        #all_offers
from    dbarnett.v250_gold_downgrades_active_base_vespa base
        inner join
        yarlagaddar.View_CUST_OFFER_HIST tgt
        on base.account_number = tgt.account_number
and     upper(offer_dim_description) not like '%SKY+%'
and     upper(offer_dim_description) not like '%BOX%'
and     upper(offer_dim_description) not like '%INSTALL%'
and     upper(offer_dim_description) not like '%PROTECT%'
and     upper(offer_dim_description) not like '%WIRELESS%'
and     upper(offer_dim_description) not like '%SET UP%'
group by base.account_number,
        base.obs_dt,
        offer_id,
        offer_amount,
        offer_duration_months,
        offer_dim_description,
        offer_end_dt
having  offer_end_dt between obs_dt - 90 and obs_dt + 90;

delete from #all_offers where offer_amount >= 0.00;

--alter table dbarnett.v250_gold_downgrades_active_base_vespa delete offer_end_dg;
alter table dbarnett.v250_gold_downgrades_active_base_vespa add offer_end_dg integer default 0;

update dbarnett.v250_gold_downgrades_active_base_vespa base
set offer_end_dg = 1
from #all_offers tgt
where base.account_number = tgt.account_number
and base.obs_dt = tgt.obs_dt;

-- select distinct offer_end_dg from gold_downgrades_active_base_vespa

----------------------------------------------------------------------------------------------------

alter table dbarnett.v250_gold_downgrades_active_base_vespa
add(    ta_or_pat_past_24_2m                  bit        default 0);


update dbarnett.v250_gold_downgrades_active_base_vespa base
set

base.ta_or_pat_past_24_2m = tgt.ta_or_pat_past_24_2m
from
(
select           base.account_number
                ,base.obs_dt
                ,max(case when tgt.TypeofEvent in ('PAT','TA') and tgt.event_dt between DATEADD(MM,-24,base.obs_dt) and DATEADD(MM,-2,base.obs_dt) then 1 else 0 end) as ta_or_pat_past_24_2m
                from yarlagaddar.View_CUST_CALLS_HIST tgt
        inner join
         dbarnett.v250_gold_downgrades_active_base_vespa base on tgt.account_number = base.account_number
         where tgt.event_dt <= base.obs_dt
group by base.account_number, base.obs_dt) as tgt
where   base.account_number = tgt.account_number
and   base.obs_dt = tgt.obs_dt;

commit
;


alter table     dbarnett.v250_gold_downgrades_active_base_vespa    add(    last_activation_dt date default null,
                                                        first_activation_dt date default null)  ;

UPDATE dbarnett.v250_gold_downgrades_active_base_vespa base
   SET base.last_activation_dt   = tgt.Last_activation_dt,
       base.first_activation_dt  = tgt.first_active_dt
FROM   (
                     SELECT  base.account_number
                            ,base.obs_dt
                            ,MAX(csh.status_start_dt) as Last_activation_dt
                            ,MAX(csh.created_dt) AS Last_Created_dt
                            ,MIN(csh.effective_from_dt) AS first_active_dt
                      FROM   sk_prod.cust_subs_hist AS csh
                             INNER JOIN dbarnett.v250_gold_downgrades_active_base_vespa  as base on csh.account_number = base.account_number
                      WHERE  csh.subscription_type =  'DTV PACKAGE'
                        AND  csh.subscription_sub_type ='DTV Primary Viewing'
                        AND  csh.status_code_changed = 'Y'
                        AND  csh.status_code = 'AC'
                        AND  csh.prev_status_code NOT IN ('PC','AB')
                        AND  csh.effective_from_dt < base.obs_dt
                   GROUP BY  base.account_number, base.obs_dt
                  ) AS tgt where base.account_number = tgt.account_number and base.obs_dt = tgt.obs_dt
                  ;
Commit;


----------------------------------------------------------------------------------------------------
alter table dbarnett.v250_gold_downgrades_active_base_vespa
add(    num_sports_downgrades_ever integer default 0,
        last_sports_downgrade date default null,
        num_movies_downgrades_ever integer default 0,
        last_movies_downgrade date default null,
        num_sports_upgrades_ever integer default 0,
        last_sports_upgrade date default null,
        num_movies_upgrades_ever integer default 0,
        last_movies_upgrade date default null);

update  dbarnett.v250_gold_downgrades_active_base_vespa base
set

base.num_sports_downgrades_ever = tgt.num_sports_downgrades_ever,
base.last_sports_downgrade      = tgt.last_sports_downgrade,
base.num_movies_downgrades_ever = tgt.num_movies_downgrades_ever,
base.last_movies_downgrade = tgt.last_movies_downgrade,
base.num_sports_upgrades_ever = tgt.num_sports_upgrades_ever,
base.last_sports_upgrade = tgt.last_sports_upgrade,
base.num_movies_upgrades_ever = tgt.num_movies_upgrades_ever,
base.last_movies_upgrade = tgt.last_movies_upgrade

from
(
select           base.account_number
                ,base.obs_dt
                ,sum(case when tgt.TypeofEvent = 'SD' and tgt.effective_from_dt < base.obs_dt then 1 else 0 end)  as num_sports_downgrades_ever
                ,max(case when tgt.TypeofEvent = 'SD' and tgt.effective_from_dt < base.obs_dt then tgt.effective_from_dt else null end)  as last_sports_downgrade
                ,sum(case when tgt.TypeofEvent = 'MD' and tgt.effective_from_dt < base.obs_dt then 1 else 0 end)  as num_movies_downgrades_ever
                ,max(case when tgt.TypeofEvent = 'MD' and tgt.effective_from_dt < base.obs_dt then tgt.effective_from_dt else null end)  as last_movies_downgrade
                ,sum(case when tgt.TypeofEvent = 'SU' and tgt.effective_from_dt < base.obs_dt then 1 else 0 end)  as num_sports_upgrades_ever
                ,max(case when tgt.TypeofEvent = 'SU' and tgt.effective_from_dt < base.obs_dt then tgt.effective_from_dt else null end)  as last_sports_upgrade
                ,sum(case when tgt.TypeofEvent = 'MU' and tgt.effective_from_dt < base.obs_dt then 1 else 0 end)  as num_movies_upgrades_ever
                ,max(case when tgt.TypeofEvent = 'MU' and tgt.effective_from_dt < base.obs_dt then tgt.effective_from_dt else null end)  as last_movies_upgrade
from yarlagaddar.View_CUST_PACKAGE_MOVEMENTS_HIST tgt
        inner join
         dbarnett.v250_gold_downgrades_active_base_vespa base on tgt.account_number = base.account_number
where tgt.effective_from_dt <= base.obs_dt
group by base.account_number, base.obs_dt
) as tgt
where base.account_number = tgt.account_number
and   base.obs_dt = tgt.obs_dt;





alter table     dbarnett.v250_gold_downgrades_active_base_vespa add last_movies_movement date default null;

update  dbarnett.v250_gold_downgrades_active_base_vespa base
set     last_movies_movement = yy
from
(       select  account_number, obs_dt, max(xx) as yy
                from
 (select  account_number, obs_dt, last_movies_upgrade as xx
        from dbarnett.v250_gold_downgrades_active_base_vespa
   union all
    select  account_number, obs_dt, last_movies_downgrade as xx
        from dbarnett.v250_gold_downgrades_active_base_vespa) as tgt
        group by account_number, obs_dt) as tgt

where base.account_number = tgt.account_number
and   base.obs_dt = tgt.obs_dt;

-- select top 10 * from dbarnett.v250_gold_downgrades_active_base_vespa

----------------------------------------------------------------------------------------------------

alter table dbarnett.v250_gold_downgrades_active_base_vespa
add movies_tenure_date date default null;

update  dbarnett.v250_gold_downgrades_active_base_vespa base
set     movies_tenure_date = yy
from
(       select  account_number, obs_dt, max(xx) as yy
                from
 (select  account_number, obs_dt, last_movies_upgrade as xx
        from dbarnett.v250_gold_downgrades_active_base_vespa
   union all
    select  account_number, obs_dt, last_activation_dt as xx

        from dbarnett.v250_gold_downgrades_active_base_vespa
    union all
    select  account_number, obs_dt, first_activation_dt as xx
        from dbarnett.v250_gold_downgrades_active_base_vespa) as tgt
        group by account_number, obs_dt) as tgt
where base.account_number = tgt.account_number
and   base.obs_dt = tgt.obs_dt;

----------------------------------------------------------------------------------------------------

alter table dbarnett.v250_gold_downgrades_active_base_vespa
add sports_tenure_date date default null;

update  dbarnett.v250_gold_downgrades_active_base_vespa base
set     sports_tenure_date = yy
from
(       select  account_number, obs_dt, max(xx) as yy
                from
 (select  account_number, obs_dt, last_sports_upgrade as xx
        from dbarnett.v250_gold_downgrades_active_base_vespa
   union all
    select  account_number, obs_dt, last_activation_dt as xx
        from dbarnett.v250_gold_downgrades_active_base_vespa
    union all
    select  account_number, obs_dt, first_activation_dt as xx
        from dbarnett.v250_gold_downgrades_active_base_vespa) as tgt
        group by account_number, obs_dt) as tgt
where base.account_number = tgt.account_number
and   base.obs_dt = tgt.obs_dt

;alter table     dbarnett.v250_gold_downgrades_active_base_vespa add last_sports_movement date default null;

update  dbarnett.v250_gold_downgrades_active_base_vespa base
set     last_sports_movement = yy
from
(       select  account_number, obs_dt, max(xx) as yy
                from
 (select  account_number, obs_dt, last_sports_upgrade as xx
        from dbarnett.v250_gold_downgrades_active_base_vespa
   union all
    select  account_number, obs_dt, last_sports_downgrade as xx
        from dbarnett.v250_gold_downgrades_active_base_vespa) as tgt
        group by account_number, obs_dt) as tgt

where base.account_number = tgt.account_number
and   base.obs_dt = tgt.obs_dt;

----------------------------------------------------------------------------------------------------

---Add Current Movies and Sports status---
--select top 100 * from dbarnett.v250_Account_profiling;

alter table dbarnett.v250_gold_downgrades_active_base_vespa add movies integer;
alter table dbarnett.v250_gold_downgrades_active_base_vespa add sports integer;

update dbarnett.v250_gold_downgrades_active_base_vespa 

set movies=b.movies_premiums
,sports=b.sports_premiums

from dbarnett.v250_gold_downgrades_active_base_vespa as a
left outer join dbarnett.v250_Account_profiling as b
on a.account_number = b.account_number
;
commit;








alter table dbarnett.v250_gold_downgrades_active_base_vespa
add movies_segment varchar(30) default null;

update dbarnett.v250_gold_downgrades_active_base_vespa --3
set movies_segment = 'A) Bedding In'
where first_activation_dt >= dateadd(yy,-2,obs_dt);

update dbarnett.v250_gold_downgrades_active_base_vespa base --2
set movies_segment = 'B) Unstable Customers'
where num_movies_downgrades_ever + num_movies_upgrades_ever >=2
and   last_movies_movement >= dateadd(yy,-2,obs_dt);

update dbarnett.v250_gold_downgrades_active_base_vespa base  --1
set movies_segment = 'C) Ex-Unstable Customers'
where num_movies_downgrades_ever + num_movies_upgrades_ever >=2
and   last_movies_movement < dateadd(yy,-2,base.obs_dt);

update dbarnett.v250_gold_downgrades_active_base_vespa base --6
set movies_segment = 'D) Stable Movies'
where num_movies_downgrades_ever + num_movies_upgrades_ever < 2
and movies > 0
and movies_segment is null;

update dbarnett.v250_gold_downgrades_active_base_vespa base --4
set movies_segment = 'E) Stable Non-Movies'
where num_movies_downgrades_ever = 1
and movies = 0
and movies_segment is null;

update dbarnett.v250_gold_downgrades_active_base_vespa base --5
set movies_segment = 'F) Never Hads'
where num_movies_downgrades_ever + num_movies_upgrades_ever = 0
and movies = 0
and movies_segment is null
;

----------------------------------------------------------------------------------------------------

alter table dbarnett.v250_gold_downgrades_active_base_vespa
add sports_segment varchar(30) default null;

update dbarnett.v250_gold_downgrades_active_base_vespa
set sports_segment = 'A) Bedding In'
where first_activation_dt >= dateadd(yy,-2,obs_dt);

update dbarnett.v250_gold_downgrades_active_base_vespa base
set sports_segment = 'B) Unstable Customers'
where num_sports_downgrades_ever + num_sports_upgrades_ever >=2
and   last_sports_movement >= dateadd(yy,-2,obs_dt);

update dbarnett.v250_gold_downgrades_active_base_vespa base
set sports_segment = 'C) Ex-Unstable Customers'
where num_sports_downgrades_ever + num_sports_upgrades_ever >=2
and   last_sports_movement < dateadd(yy,-2,base.obs_dt);

update dbarnett.v250_gold_downgrades_active_base_vespa base
set sports_segment = 'D) Stable Sports'
where num_sports_downgrades_ever + num_sports_upgrades_ever < 2
and sports > 0
and sports_segment is null;

update dbarnett.v250_gold_downgrades_active_base_vespa base
set sports_segment = 'E) Stable Non-Sports'
where num_sports_downgrades_ever = 1
and sports = 0
and sports_segment is null;

update dbarnett.v250_gold_downgrades_active_base_vespa base
set sports_segment = 'F) Never Hads'
where num_sports_downgrades_ever + num_sports_upgrades_ever = 0
and sports = 0
and sports_segment is null
;

create variable @endDate date;
set @endDate='2013-10-31';
commit;

/*

alter table dbarnett.v250_gold_downgrades_active_base_vespa delete skygo_distinct_activitydate_last90days ;
alter table dbarnett.v250_gold_downgrades_active_base_vespa delete skygo_distinct_activitydate_last180days ;
alter table dbarnett.v250_gold_downgrades_active_base_vespa delete skygo_distinct_activitydate_last270days ;
alter table dbarnett.v250_gold_downgrades_active_base_vespa delete skygo_distinct_activitydate_last360days ;

alter table dbarnett.v250_gold_downgrades_active_base_vespa delete od_distinct_activitydate_last90days ;
alter table dbarnett.v250_gold_downgrades_active_base_vespa delete od_distinct_activitydate_last180days ;
alter table dbarnett.v250_gold_downgrades_active_base_vespa delete od_distinct_activitydate_last270days ;
alter table dbarnett.v250_gold_downgrades_active_base_vespa delete od_distinct_activitydate_last360days ;
*/


alter table dbarnett.v250_gold_downgrades_active_base_vespa add skygo_distinct_activitydate_last90days integer default 0;
alter table dbarnett.v250_gold_downgrades_active_base_vespa add skygo_distinct_activitydate_last180days integer default 0;
alter table dbarnett.v250_gold_downgrades_active_base_vespa add skygo_distinct_activitydate_last270days integer default 0;
alter table dbarnett.v250_gold_downgrades_active_base_vespa add skygo_distinct_activitydate_last360days integer default 0;

alter table dbarnett.v250_gold_downgrades_active_base_vespa add od_distinct_activitydate_last90days integer default 0;
alter table dbarnett.v250_gold_downgrades_active_base_vespa add od_distinct_activitydate_last180days integer default 0;
alter table dbarnett.v250_gold_downgrades_active_base_vespa add od_distinct_activitydate_last270days integer default 0;
alter table dbarnett.v250_gold_downgrades_active_base_vespa add od_distinct_activitydate_last360days integer default 0;



UPDATE  dbarnett.v250_gold_downgrades_active_base_vespa
   SET  BASE.skygo_distinct_activitydate_last90days    = CFPH_T.skygo_distinct_activitydate_last90days
        ,BASE.skygo_distinct_activitydate_last180days    = CFPH_T.skygo_distinct_activitydate_last180days
        ,BASE.skygo_distinct_activitydate_last270days   = CFPH_T.skygo_distinct_activitydate_last270days
        ,BASE.skygo_distinct_activitydate_last360days   = CFPH_T.skygo_distinct_activitydate_last360days
        ,BASE.od_distinct_activitydate_last90days      = CFPH_T.od_distinct_activitydate_last90days
        ,BASE.od_distinct_activitydate_last180days      = CFPH_T.od_distinct_activitydate_last180days
        ,BASE.od_distinct_activitydate_last270days      = CFPH_T.od_distinct_activitydate_last270days
        ,BASE.od_distinct_activitydate_last360days      = CFPH_T.od_distinct_activitydate_last360days
  FROM dbarnett.v250_gold_downgrades_active_base_vespa AS BASE
       INNER JOIN
                 (
                    SELECT  CFPH.account_number
                           ,SUM(CASE WHEN CFPH.TypeOfEvent = 'SG'  AND CFPH.event_dt BETWEEN dateadd(day,-90,@endDate) AND @endDate THEN 1 ELSE 0 END) AS skygo_distinct_activitydate_last90days
                           ,SUM(CASE WHEN CFPH.TypeOfEvent = 'SG'  AND CFPH.event_dt BETWEEN dateadd(day,-180,@endDate) AND @endDate THEN 1 ELSE 0 END) AS skygo_distinct_activitydate_last180days
                           ,SUM(CASE WHEN CFPH.TypeOfEvent = 'SG'  AND CFPH.event_dt BETWEEN dateadd(day,-270,@endDate) AND @endDate THEN 1 ELSE 0 END) AS skygo_distinct_activitydate_last270days
                           ,SUM(CASE WHEN CFPH.TypeOfEvent = 'SG'  AND CFPH.event_dt BETWEEN dateadd(day,-360,@endDate) AND @endDate THEN 1 ELSE 0 END) AS skygo_distinct_activitydate_last360days

                           ,SUM(CASE WHEN CFPH.TypeOfEvent = 'OD'  AND CFPH.event_dt BETWEEN dateadd(day,-90,@endDate) AND @endDate THEN 1 ELSE 0 END) AS od_distinct_activitydate_last90days
                           ,SUM(CASE WHEN CFPH.TypeOfEvent = 'OD'  AND CFPH.event_dt BETWEEN dateadd(day,-180,@endDate) AND @endDate THEN 1 ELSE 0 END) AS od_distinct_activitydate_last180days
                           ,SUM(CASE WHEN CFPH.TypeOfEvent = 'OD'  AND CFPH.event_dt BETWEEN dateadd(day,-270,@endDate) AND @endDate THEN 1 ELSE 0 END) AS od_distinct_activitydate_last270days
                           ,SUM(CASE WHEN CFPH.TypeOfEvent = 'OD'  AND CFPH.event_dt BETWEEN dateadd(day,-360,@endDate) AND @endDate THEN 1 ELSE 0 END) AS od_distinct_activitydate_last360days
                      FROM dbarnett.v250_gold_downgrades_active_base_vespa AS BASE_T
                           INNER JOIN 	Yarlagaddar.View_CUST_FREE_PRODUCTS_HIST AS CFPH ON BASE_T.account_number = CFPH.account_number
                     WHERE CFPH.event_dt BETWEEN dateadd(day,-360,@endDate) AND @endDate
                    GROUP BY CFPH.account_number
                 ) AS CFPH_T  ON BASE.account_number = CFPH_T.account_number
;

commit;

--select max(event_dt) from 	Yarlagaddar.View_CUST_FREE_PRODUCTS_HIST;


----------------------------------------------------------------------------------------------------
-- MODEL DERIVATION
----------------------------------------------------------------------------------------------------
--select od_distinct_activitydate_last180days,count(*) from dbarnett.v250_gold_downgrades_active_base_vespa group by od_distinct_activitydate_last180days
alter table dbarnett.v250_gold_downgrades_active_base_vespa
add movies_downgrade_model varchar(100) default null;

alter table dbarnett.v250_gold_downgrades_active_base_vespa
add sports_downgrade_model varchar(100) default null;

update dbarnett.v250_gold_downgrades_active_base_vespa set movies_downgrade_model = case when movies = 0 then '9) Not On Movies' 
                when num_active_block_L24M > 0 and ta_or_pat_past_24_2m = 0                                             then '1a) AB History: No TA/PAT'

                when num_active_block_L24M > 0 and ta_or_pat_past_24_2m > 0                                             then '1b) AB History: TA/PAT'

                when datediff(day,last_activation_dt,obs_dt) <=  730 and ta_or_pat_past_24_2m = 0                       then '2a) Sky Tenure <2yrs: No TA/PAT'

                when datediff(day,last_activation_dt,obs_dt) <=  730 and ta_or_pat_past_24_2m > 0                       then '2b) Sky Tenure <2yrs: TA/PAT'

                when movies = 2         and datediff(day,movies_tenure_date,obs_dt) <=  365 and od_distinct_activitydate_last180days = 0
                                        and offer_end_dg = 1
                                        and num_movies_downgrades_ever >= 1                                             then '3a) <1yr Prem Tenure, Unstable, Offer Seekers: No OD'

                when movies = 2         and datediff(day,movies_tenure_date,obs_dt) <=  365 and od_distinct_activitydate_last180days > 0
                                        and offer_end_dg = 1
                                        and num_movies_downgrades_ever >= 1                                             then '3b) <1yr Prem Tenure, Unstable, Offer Seekers: OD'

                when movies = 2         and datediff(day,movies_tenure_date,obs_dt) <=  365 and od_distinct_activitydate_last180days = 0
                                        and num_movies_downgrades_ever  = 0                                             then '5a) <1yr Prem Tenure, First Downgrade: No OD'

                when movies = 2         and datediff(day,movies_tenure_date,obs_dt) <=  365 and  od_distinct_activitydate_last180days > 0
                                        and num_movies_downgrades_ever  = 0                                             then '5b) <1yr Prem Tenure, First Downgrade: OD'

                when movies = 2         and datediff(day,movies_tenure_date,obs_dt) >  365 and ta_or_pat_past_24_2m = 0
                                        and num_movies_downgrades_ever >= 1                                             then '6a) >1yr Prem Tenure Unstable: No TA/PAT'

                when movies = 2         and datediff(day,movies_tenure_date,obs_dt) >  365 and ta_or_pat_past_24_2m > 0
                                        and num_movies_downgrades_ever >= 1                                             then '6b) >1yr Prem Tenure Unstable: TA/PAT'

                when movies = 2         and datediff(day,movies_tenure_date,obs_dt) >  365 and ta_or_pat_past_24_2m = 0 and od_distinct_activitydate_last180days = 0
                                        and offer_end_dg = 1
                                        and num_movies_downgrades_ever  = 0                                             then '7a) >1yr Prem Tenure, Offer Seekers, First Downgrade: No TA/PAT No OD'

                when movies = 2         and datediff(day,movies_tenure_date,obs_dt) >  365 and ta_or_pat_past_24_2m = 0 and od_distinct_activitydate_last180days > 0
                                        and offer_end_dg = 1
                                        and num_movies_downgrades_ever  = 0                                             then '7b) >1yr Prem Tenure, Offer Seekers, First Downgrade: No TA/PAT OD'

                when movies = 2         and datediff(day,movies_tenure_date,obs_dt) >  365 and ta_or_pat_past_24_2m > 0
                                        and offer_end_dg = 1
                                        and num_movies_downgrades_ever  = 0                                             then '7c) >1yr Prem Tenure, Offer Seekers, First Downgrade: TA/PAT'

                when movies = 2         and datediff(day,movies_tenure_date,obs_dt) >  365 and ta_or_pat_past_24_2m = 0
                                        and offer_end_dg = 0
                                        and num_movies_downgrades_ever  = 0                                             then '8a) >1yr Prem Tenure, Full Price, First Downgrade: No TA/PAT'

                when movies = 2         and datediff(day,movies_tenure_date,obs_dt) >  365 and ta_or_pat_past_24_2m > 0
                                        and offer_end_dg = 0
                                        and num_movies_downgrades_ever  = 0                                             then '8b) >1yr Prem Tenure, Full Price, First Downgrade: TA/PAT'

                
                when od_distinct_activitydate_last180days >0                                                         then '4b) <1Yr Prem Tenure, Full Price, Unstable: OD'

                when od_distinct_activitydate_last180days = 0                                                           then '4a) <1Yr Prem Tenure, Full Price, Unstable: No OD'

                                                                                                                        else '4) <1Yr Prem Tenure, Full Price, Unstable'
                end;

/*
select movies ,od_distinct_activitydate_last180days,num_movies_downgrades_ever,movies_tenure_date,ta_or_pat_past_24_2m 
from dbarnett.v250_gold_downgrades_active_base_vespa
where movies_downgrade_model='4a) <1Yr Prem Tenure, Full Price, Unstable: No OD'


select movies_downgrade_model , count(*)
from dbarnett.v250_gold_downgrades_active_base_vespa
group by movies_downgrade_model


select sports_downgrade_model , count(*)
from dbarnett.v250_gold_downgrades_active_base_vespa
group by sports_downgrade_model

*/


----------------------------------------------------------------------------------------------------



update dbarnett.v250_gold_downgrades_active_base_vespa set sports_downgrade_model = case when sports = 0 then '9) Not On Sports' 
                when num_active_block_L24M > 0 and ta_or_pat_past_24_2m = 0                                             then '1a) AB History: No TA/PAT'

                when num_active_block_L24M > 0 and ta_or_pat_past_24_2m > 0                                             then '1b) AB History: TA/PAT'

                when datediff(day,last_activation_dt,obs_dt) <=  730 and ta_or_pat_past_24_2m = 0                       then '2a) Sky Tenure <2yrs: No TA/PAT'

                when datediff(day,last_activation_dt,obs_dt) <=  730 and ta_or_pat_past_24_2m > 0                       then '2b) Sky Tenure <2yrs: TA/PAT'

                when sports = 2         and datediff(day,sports_tenure_date,obs_dt) <=  365 and skygo_distinct_activitydate_last180days = 0
                                        and offer_end_dg = 1
                                        and num_sports_downgrades_ever >= 1                                             then '3a) <1yr Prem Tenure, Unstable, Offer Seekers: No Sky Go'

                when sports = 2         and datediff(day,sports_tenure_date,obs_dt) <=  365 and skygo_distinct_activitydate_last180days > 0
                                        and offer_end_dg = 1
                                        and num_sports_downgrades_ever >= 1                                             then '3b) <1yr Prem Tenure, Unstable, Offer Seekers: Sky Go'

                when sports = 2         and datediff(day,sports_tenure_date,obs_dt) <=  365 and skygo_distinct_activitydate_last180days = 0
                                        and num_sports_downgrades_ever  = 0                                             then '5a) <1yr Prem Tenure, First Downgrade: No Sky Go'

                when sports = 2         and datediff(day,sports_tenure_date,obs_dt) <=  365 and  skygo_distinct_activitydate_last180days > 0
                                        and num_sports_downgrades_ever  = 0                                             then '5b) <1yr Prem Tenure, First Downgrade: Sky Go'

                when sports = 2         and datediff(day,sports_tenure_date,obs_dt) >  365 and ta_or_pat_past_24_2m = 0
                                        and num_sports_downgrades_ever >= 1                                             then '6a) >1yr Prem Tenure Unstable: No TA/PAT'

                when sports = 2         and datediff(day,sports_tenure_date,obs_dt) >  365 and ta_or_pat_past_24_2m > 0
                                        and num_sports_downgrades_ever >= 1                                             then '6b) >1yr Prem Tenure Unstable: TA/PAT'

                when sports = 2         and datediff(day,sports_tenure_date,obs_dt) >  365 and ta_or_pat_past_24_2m = 0
                                        and offer_end_dg = 1
                                        and num_sports_downgrades_ever  = 0                                             then '7a) >1yr Prem Tenure, Offer Seekers, First Downgrade: No TA/PAT'

                when sports = 2         and datediff(day,sports_tenure_date,obs_dt) >  365 and ta_or_pat_past_24_2m > 0
                                        and offer_end_dg = 1
                                        and num_sports_downgrades_ever  = 0                                             then '7b) >1yr Prem Tenure, Offer Seekers, First Downgrade: TA/PAT'

                when sports = 2         and datediff(day,sports_tenure_date,obs_dt) >  365 and ta_or_pat_past_24_2m = 0 and datediff(day,first_activation_dt,obs_dt) <= 3650
                                        and offer_end_dg = 0
                                        and num_sports_downgrades_ever  = 0                                             then '8a) >1yr Prem Tenure, Full Price, First Downgrade: No TA/PAT <10yrs Tenure'

                when sports = 2         and datediff(day,sports_tenure_date,obs_dt) >  365 and ta_or_pat_past_24_2m = 0 and datediff(day,first_activation_dt,obs_dt) > 3650
                                        and offer_end_dg = 0
                                        and num_sports_downgrades_ever  = 0                                             then '8b) >1yr Prem Tenure, Full Price, First Downgrade: No TA/PAT >10yrs Tenure'

                when sports = 2         and datediff(day,sports_tenure_date,obs_dt) >  365 and ta_or_pat_past_24_2m > 0
                                        and offer_end_dg = 0
                                        and num_sports_downgrades_ever  = 0                                             then '8c) >1yr Prem Tenure, Full Price, First Downgrade: TA/PAT'

                when od_distinct_activitydate_last180days >0                                                           then '4b) <1Yr Prem Tenure, Full Price, Unstable: Sky Go'

                when od_distinct_activitydate_last180days = 0                                                           then '4a) <1Yr Prem Tenure, Full Price, Unstable: No Sky Go'

                else '4) <1Yr Prem Tenure, Full Price, Unstable'

                end;



-----------------------------------------------------------------------------------------------------


---Add on Models--


alter table dbarnett.v250_Account_profiling add movies_downgrade_model varchar(100);
alter table dbarnett.v250_Account_profiling add sports_downgrade_model varchar(100);


update dbarnett.v250_Account_profiling
set movies_downgrade_model=b.movies_downgrade_model
,sports_downgrade_model=b.sports_downgrade_model
from dbarnett.v250_Account_profiling  as a
left outer join dbarnett.v250_gold_downgrades_active_base_vespa as b
on a.account_number = b.account_number
;

commit;

