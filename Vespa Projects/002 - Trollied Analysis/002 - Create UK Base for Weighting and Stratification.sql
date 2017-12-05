/*------------------------------------------------------------------------------
        Project: Postratification Sky base
        Version: 1
        Created: 20111108
        Lead: Gavin Meggs
        Analyst: Sarah Jackson
        SK Prod: 10

        Updated 20111129 DBarnett

        --Main Changes 
            - Add whether Primary/Secondary Box
            - Remove Non UK Accounts
            - Remove Accounts that Are in Active Block

*/------------------------------------------------------------------------------
/*
        Purpose
        -------
        To calculate weightings for the chosen sampling drivers using postratification

        SECTIONS
        --------

        PART A   - Sky population and sample
             A01 - Create Base
             A02 - Populate ILU variables
             A03 - Populate Sky Variables

        PART B   - Add Log activity data

        PART C   - Stratified Sampling
             C01 - Create Random number
             C02 - Work out size of sample for each combination
             C03 - Random sampling
             C03 - Flag accounts which make up 150k sample

        PART D   - Calculation of Weights and Scaling factor
             D01 - Calculate weights based on 150k sample
             D02 - Allocate weights to account_numbers
             D03 - Create Output tables

        Tables
        -------
        vespa_analysts.sky_base_v2_2011_08_11
        vespa_analysts.stratifiedsampling
        vespa_analysts.Sky_weightings_table_all_variables_HH
        vespa_analysts.sky_weightings
        vespa_analysts.output1


*/

--------------------------------------------------------------------------------
-- PART A SETUP - BASE TABLE
--------------------------------------------------------------------------------

/*
PART A   - Sky population and sample
     A01 - Create Base
     A02 - Populate ILU variables
     A03 - Populate Sky Variables
*/

--------------------------------------------------------------- A01 - Create Base
-- A01 - Create Base

--creating a date variable to use throughout the code
  create variable @target_date date;
     set @target_date = '20110811';


if object_id('sky_base_v2_2011_08_11') is not null drop table vespa_analysts.sky_base_v2_2011_08_11;
CREATE TABLE vespa_analysts.sky_base_v2_2011_08_11 ( -- drop table govt_region_base
         account_number                  varchar(30)   NOT NULL
         ,cb_key_household                 bigint      NOT NULL
         ,current_short_description      varchar(70)
         ,postcode                        varchar(10)  default 'Unknown'
        ,service_instance_id            varchar(50) 
        ,SUBSCRIPTION_SUB_TYPE          varchar(50) 
);

create unique hg index idx1 on vespa_analysts.sky_base_v2_2011_08_11(account_number);
create hg index idx2 on vespa_analysts.sky_base_v2_2011_08_11(cb_key_household);

grant all on vespa_analysts.sky_base_v2_2011_08_11               to public;


select account_number
        , cb_key_household
        , csh.current_short_description
        ,service_instance_id
        ,SUBSCRIPTION_SUB_TYPE
        , rank() over (partition by account_number order by effective_from_dt, cb_row_id) as rank
into #sky_accounts -- drop table #sky_accounts
from sk_prod.cust_subs_hist as csh
where SUBSCRIPTION_SUB_TYPE in ('DTV Primary Viewing','DTV Extra Subscription') --the DTV + Multiroom sub Type
   and status_code in ('AC','PC')               --Active Status Codes (ACtive Block Removed)
   and effective_from_dt <= @target_date             --Start on or before 1st Jan
   and effective_to_dt > @target_date                --ends after 1st Jan
   and effective_from_dt<>effective_to_dt            --ignore all but the last thing each customer did in a day
and cb_key_household > 0
and cb_key_household is not null
and account_number is not null;
commit;

--10036618 Row(s) affected

delete from #sky_accounts where rank>1;
commit;

--850 Row(s) affected
create unique hg index idx1 on #sky_accounts(account_number);

create  hg index idx2 on #sky_accounts(cb_key_household);
--select count(distinct account_number) from #sky_accounts;
--select count(account_number), count(distinct account_number), count(cb_key_household), count(distinct cb_key_household), count(*) from vespa_analysts.sky_base_v2_2011_08_11
--10035768        10035768        10035768        9546754 10035768

insert into vespa_analysts.sky_base_v2_2011_08_11 (account_number, cb_key_household, current_short_description,service_instance_id,SUBSCRIPTION_SUB_TYPE)
select account_number, cb_key_household, current_short_description,service_instance_id,SUBSCRIPTION_SUB_TYPE
from #sky_accounts
;
commit;

/* One off code to exclude active block customers from the file as tables previously run with these included
--alter table vespa_analysts.sky_base_v2_2011_08_11 add ac_pc_account integer default 0;

Update vespa_analysts.sky_base_v2_2011_08_11 base
set base.ac_pc_account = case when acc.service_instance_id is not null then 1 else 0 end
from vespa_analysts.sky_base_v2_2011_08_11 base
left outer join #sky_accounts acc
on base.service_instance_id = acc.service_instance_id;
commit;

select ac_pc_account , count(*) from vespa_analysts.sky_base_v2_2011_08_11 base group by ac_pc_account order by ac_pc_account

delete from vespa_analysts.sky_base_v2_2011_08_11 where ac_pc_account = 0;
commit;
*/



-------------------------------------------------------------------------------- A02 - Populate ILU variables

-- A02 - Populate ILU variables



Update vespa_analysts.sky_base_v2_2011_08_11 base
set base.postcode = sav.cb_address_postcode
from sk_prod.cust_single_account_view sav
where base.account_number = sav.account_number;
commit;

--10033571 Row(s) affected

--8,842,102 Row(s) affected

create  hg index idx3 on vespa_analysts.sky_base_v2_2011_08_11(postcode);
-- add ilu variables

alter table vespa_analysts.sky_base_v2_2011_08_11
add pty_country_code varchar(10) default 'Unknown',
add HHAfflu varchar(10) default 'Unknown',
add gov_region varchar(50) default '14. Unknown',
add lifestage varchar(50) default 'Unknown';
commit;

--Use family key and correspondent flag for ILU data to create a linking table from your table to SK_PROD.ILU

SELECT  ilu.cb_row_id
                   ,base.account_number
                   ,base.cb_key_household
                   ,MAX(CASE WHEN ilu.ilu_correspondent = 'P1' THEN 1 ELSE 0 END) as P1
                   ,MAX(CASE WHEN ilu.ilu_correspondent = 'P2' THEN 1 ELSE 0 END) as P2
                   ,MAX(CASE WHEN ilu.ilu_correspondent = 'OR' THEN 1 ELSE 0 END) as OR1
                   into #temp -- drop table #temp
              FROM  sk_prod.ilu as ilu
                        inner join vespa_analysts.sky_base_v2_2011_08_11 as base on base.cb_key_household = ilu.cb_key_household
                                                and base.cb_key_household is not null
                                                and  base.cb_key_household > 0
          GROUP BY  ilu.cb_row_id, base.account_number, base.cb_key_household
            HAVING  P1 + P2 + OR1 > 0;
            commit;

--19804573 Row(s) affected

SELECT  cb_row_id
       ,account_number
       ,cb_key_household
       ,CASE WHEN P1 = 1  THEN 1
             WHEN P2 = 1  THEN 2
             ELSE              3
         END AS Correspondent
       ,rank() over(PARTITION BY account_number ORDER BY Correspondent asc, cb_row_id desc) as rank
 INTO  #ILU -- drop table #ilu
 FROM #temp;


--19,804,573 Row(s) affected

DELETE FROM #ILU where rank > 1;
--10,970,234 Row(s) affected


--8,834,339

 update vespa_analysts.sky_base_v2_2011_08_11 as bas
     set pty_country_code=sav.pty_country_code
    from sk_prod.cust_single_account_view as sav
   where bas.account_number = sav.account_number;
   commit;

--10,035,542 Row(s) affected


  update vespa_analysts.sky_base_v2_2011_08_11
     set lifestage = case ilu.ilu_hhlifestage when  1 then '18-24 ,Left home'
                                              when  2 then '25-34 ,Single (no kids)'
                                              when  3 then '25-34 ,Couple (no kids)'
                                              when  4 then '25-34 ,Child 0-4'
                                              when  5 then '25-34 ,Child5-7'
                                              when  6 then '25-34 ,Child 8-16'
                                              when  7 then '35-44 ,Single (no kids)'
                                              when  8 then '35-44 ,Couple (no kids)'
                                              when  9 then '45-54 ,Single (no kids)'
                                              when 10 then '45-54 ,Couple (no kids)'
                                              when 11 then '35-54 ,Child 0-4'
                                              when 12 then '35-54 ,Child 5-10'
                                              when 13 then '35-54 ,Child 11-16'
                                              when 14 then '35-54 ,Grown up children at home'
                                              when 15 then '55-64 ,Not retired - single'
                                              when 16 then '55-64 ,Not retired - couple'
                                              when 17 then '55-64 ,Retired'
                                              when 18 then '65-74 ,Not retired'
                                              when 19 then '65-74 ,Retired single'
                                              when 20 then '65-74 ,Retired couple'
                                              when 21 then '75+   ,Single'
                                              when 22 then '75+   ,Couple'
                                              else         'Unknown' end
    FROM vespa_analysts.sky_base_v2_2011_08_11 as base
       INNER JOIN #ILU on base.account_number = #ilu.account_number
       INNER JOIN sk_prod.ilu as ilu on #ilu.cb_row_id = ilu.cb_row_id;
       --inner join sk_prod.ilu as ilu on ilu.cb_key_household = base.cb_key_household;

--8,834,543 Row(s) affected

/*
QA
select account_number
into #nomatch
from vespa_analysts.sky_base_v2_2011_08_11
 where lifestage is null
--1201429 Row(s) affected


select count(*)
from #nomatch as base
        inner join #ilu nm on nm.account_number = base.account_number
--0


select count(*)
from #nomatch as base
        inner join #temp nm on nm.account_number = base.account_number
--0
*/




--add third party vars
update vespa_analysts.sky_base_v2_2011_08_11 t1
set
HHAfflu=CASE WHEN t2.ilu_hhafflu in (1,2,3,4)  THEN 'Very Low'
             WHEN t2.ilu_hhafflu in (5,6)      THEN 'Low'
             WHEN t2.ilu_hhafflu in (7,8)      THEN 'Mid Low'
             WHEN t2.ilu_hhafflu in (9,10)     THEN 'Mid'
             WHEN t2.ilu_hhafflu in (11,12)    THEN 'Mid High'
             WHEN t2.ilu_hhafflu in (13,14,15) THEN 'High'
             WHEN t2.ilu_hhafflu in (16,17)    THEN 'Very High'
             ELSE                                   'Unknown'
        END
FROM vespa_analysts.sky_base_v2_2011_08_11 as t1
       INNER JOIN #ILU on t1.account_number = #ilu.account_number
       INNER JOIN sk_prod.ilu as t2 on #ilu.cb_row_id = t2.cb_row_id;
       --inner join sk_prod.ilu as t2 on t2.cb_key_household = t1.cb_key_household;
commit;

--8,834,543 Row(s) affected
--add government region
update vespa_analysts.sky_base_v2_2011_08_11 t1
set gov_region=case when reg.government_region = 'North East'               Then '01. North East'
                    when reg.government_region = 'North West'               Then '02. North West'
                    when reg.government_region = 'Yorkshire and The Humber' Then '03. Yorkshire and The Humber'
                    when reg.government_region = 'East Midlands'            Then '04. East Midlands'
                    when reg.government_region = 'West Midlands'            Then '05. West Midlands'
                    when reg.government_region = 'East of England'          Then '06. East of England'
                    when reg.government_region = 'London'                   Then '07. London'
                    when reg.government_region = 'South East'               Then '08. South East'
                    when reg.government_region = 'South West'               Then '09. South West'
                    when reg.government_region = 'Scotland'                 Then '10. Scotland'
                    when reg.government_region = 'Northern Ireland'         Then '11. Northern Ireland'
                    when reg.government_region = 'Wales'                    Then '13. Wales'
                    when trim(t1.pty_country_code) = 'ROI'                  Then '12. ROI'
                    else '14. Unknown'
               end
from sk_prod.BROADBAND_POSTCODE_EXCHANGE as reg
where replace(t1.postcode, ' ','')=replace(reg.cb_address_postcode, ' ','');
commit;



--ISBA Region

alter table vespa_analysts.sky_base_v2_2011_08_11
add isba_tv_region varchar(20) default 'Unknown';

update vespa_analysts.sky_base_v2_2011_08_11 t1
set isba_tv_region=sav.isba_tv_region
from sk_prod.cust_single_account_view sav
where t1.account_number=sav.account_number;
commit;

--10,035,542 Row(s) affected

create lf index idx_lifestage_lf           on vespa_analysts.sky_base_v2_2011_08_11(lifestage);
create lf index idx_hhafflu_lf             on vespa_analysts.sky_base_v2_2011_08_11(hhafflu);
create lf index idx_isba_lf                on vespa_analysts.sky_base_v2_2011_08_11(isba_tv_region);

-- Add Sample flags for vespa data

Alter table vespa_analysts.sky_base_v2_2011_08_11
add vespa_flag tinyint default 0;

Update vespa_analysts.sky_base_v2_2011_08_11
   set vespa_flag = 1
from vespa_analysts.sky_base_v2_2011_08_11 base
    inner join jchung.session_capping_dataset as scd on scd.account_number = base.account_number
commit;
--354525 Row(s) affected

---------------------------------------------------------------------------------- A03 - Populate Sky Variables

-- A03 - Populate Sky Variables


Alter table vespa_analysts.sky_base_v2_2011_08_11
add pvr tinyint default 0,
add box_type varchar(2) default 'SD',
add primary_box bit default 0,
add package varchar(30) default 'Basic';
commit;


--Add on box details – most recent dw_created_dt for a box (where a box hasn’t been replaced at that date)  taken from cust_set_top_box.  
--This removes instances where more than one box potentially live for a subscriber_id at a time (due to null box installed and replaced dates).

SELECT account_number
,service_instance_id
,max(dw_created_dt) as max_dw_created_dt
  INTO #boxes -- drop table #boxes
  FROM sk_prod.CUST_SET_TOP_BOX  
 WHERE (box_installed_dt <= cast('2011-08-10'  as date) 
   AND box_replaced_dt   > cast('2011-08-10'  as date)) or box_installed_dt is null
group by account_number
,service_instance_id
 ;

--select count(*) from vespa_analysts.aug_22_base_details;
commit;


commit;
exec sp_create_tmp_table_idx '#boxes', 'account_number';
exec sp_create_tmp_table_idx '#boxes', 'service_instance_id';
exec sp_create_tmp_table_idx '#boxes', 'max_dw_created_dt';
--select account_number , count(di

---Create table of one record per service_instance_id---
SELECT acc.account_number
,acc.service_instance_id
,min(stb.x_pvr_type) as pvr_type
,min(stb.x_box_type) as box_type
,min(stb.x_description) as description_x
,min(stb.x_manufacturer) as manufacturer
,min(stb.x_model_number) as model_number
  INTO #boxes_with_model_info -- drop table #boxes
  FROM #boxes  AS acc left outer join sk_prod.CUST_SET_TOP_BOX AS stb 
        ON acc.account_number = stb.account_number
 and acc.max_dw_created_dt=stb.dw_created_dt
group by acc.account_number
,acc.service_instance_id
 ;

commit;
exec sp_create_tmp_table_idx '#boxes_with_model_info', 'service_instance_id';


alter table vespa_analysts.sky_base_v2_2011_08_11 add x_pvr_type  varchar(50);
alter table vespa_analysts.sky_base_v2_2011_08_11 add x_box_type  varchar(20);
alter table vespa_analysts.sky_base_v2_2011_08_11 add x_description  varchar(100);
alter table vespa_analysts.sky_base_v2_2011_08_11 add x_manufacturer  varchar(50);
alter table vespa_analysts.sky_base_v2_2011_08_11 add x_model_number  varchar(50);

update  vespa_analysts.sky_base_v2_2011_08_11
set x_pvr_type=b.pvr_type
,x_box_type=b.box_type

,x_description=b.description_x
,x_manufacturer=b.manufacturer
,x_model_number=b.model_number
from vespa_analysts.sky_base_v2_2011_08_11 as a
left outer join #boxes_with_model_info as b
on a.service_instance_id=b.service_instance_id
;
commit;

update vespa_analysts.sky_base_v2_2011_08_11
set pvr =case when x_pvr_type like '%PVR%' then 1 else 0 end
,box_type =case when x_box_type like '%HD%' then 'HD' else 'SD' end
from vespa_analysts.sky_base_v2_2011_08_11
;

--exec gen_create_table 'sk_prod.cust_set_top_box'
--21,501,248 Row(s) affected

--10019501 Row(s) affected
--QA
--select pvr,box_type,count(*) as cow from vespa_analysts.sky_base_v2_2011_08_11 group by  pvr,box_type

---Create src_system_id lookup

select src_system_id
,min(cast(si_external_identifier as integer)) as subscriberid
into #subs_details
from
sk_prod.CUST_SERVICE_INSTANCE as b
where si_service_instance_type in ('Primary DTV','Secondary DTV (extra digiboxes)')
group by src_system_id
;


commit;
exec sp_create_tmp_table_idx '#subs_details', 'src_system_id';

commit;


--alter table vespa_analysts.F1_analysis_20111104 delete subscription_type;
alter table vespa_analysts.sky_base_v2_2011_08_11 add subscriber_id bigint;

update vespa_analysts.sky_base_v2_2011_08_11
set subscriber_id=b.subscriberid
from vespa_analysts.sky_base_v2_2011_08_11 as a
left outer join #subs_details as b
on a.service_instance_id=b.src_system_id
;
commit;



--select primary_box , count(*) from vespa_analysts.sky_base_v2_2011_08_11 group by primary_box 
--283468 Row(s) affected

--select top 100 * from sk_prod.vespa_stb_log_snapshot;


--package
  update vespa_analysts.sky_base_v2_2011_08_11
     set package = case when cel.prem_sports = 2 and cel.prem_movies = 2 then 'Top Tier'
                        when cel.prem_sports = 2 and cel.prem_movies = 0 then 'Dual Sports'
                        when cel.prem_sports = 0 and cel.prem_movies = 2 then 'Dual Movies'
                        when cel.prem_sports = 1 and cel.prem_movies = 0 then 'Single Sports'
                        when cel.prem_sports = 0 and cel.prem_movies = 1 then 'Single Movies'
                        when cel.prem_sports > 0 or  cel.prem_movies > 0 then 'Other Premiums'
                        else                                                  'Basic' end
    from vespa_analysts.sky_base_v2_2011_08_11                     as bas
         inner join sk_prod.cust_entitlement_lookup as cel on bas.current_short_description = cel.short_description
--   where bas.account_number = csh.account_number;


 --10035720 Row(s) affected

create lf index idx_pvr_lf           on vespa_analysts.sky_base_v2_2011_08_11(pvr);
create lf index idx_box_type_lf      on vespa_analysts.sky_base_v2_2011_08_11(box_type);
create lf index idx_package_lf       on vespa_analysts.sky_base_v2_2011_08_11(package);
/*
alter table vespa_analysts.sky_base_v2_2011_08_11
add hh_package_rank integer default 0;
*/

----Add on Account Type (to only keep UK standard accounts)

alter table  vespa_analysts.sky_base_v2_2011_08_11 add uk_standard_account tinyint default 0;

update  vespa_analysts.sky_base_v2_2011_08_11
set uk_standard_account =case when b.acct_type='Standard' and b.account_number <>'?' and b.pty_country_code ='GBR' then 1 else 0 end
from  vespa_analysts.sky_base_v2_2011_08_11 as a
left outer join sk_prod.cust_single_account_view as b
on a.account_number = b.account_number
;

commit;

--Remove non-uk subscriptions to create the ‘UK Active Base’

delete from  vespa_analysts.sky_base_v2_2011_08_11 where uk_standard_account=0;

-----UPDATE PRIMARY Box DEFN

USE SUB SUB TYPE

----Phase B---
----Add on Viewing activity for each day-----

--------------Add On Viewing Logs-----------------------------------------------------------
-------------------------------------------Multiple Day Union-------------------------------

--drop table #vespa_week_combined;
select vev.subscriber_id 
,vev.document_creation_date
,vev.stb_log_creation_date
,vev.adjusted_event_start_time
,vev.x_adjusted_event_end_time
,vev.event_type

into #vespa_week_combined
from sk_prod.VESPA_STB_PROG_EVENTS_20110811 as vev
where
panel_id in (4,5)


union all

select  vev2.subscriber_id 
,vev2.document_creation_date
,vev2.stb_log_creation_date
,vev2.adjusted_event_start_time
,vev2.x_adjusted_event_end_time
,vev2.event_type
from sk_prod.VESPA_STB_PROG_EVENTS_20110812 as vev2
where
-- (play_back_speed is null or play_back_speed=2   ) and x_programme_viewed_duration>0
--and 
panel_id in (4,5)


union all

select  vev3.subscriber_id 
,vev3.document_creation_date
,vev3.stb_log_creation_date
,vev3.adjusted_event_start_time
,vev3.x_adjusted_event_end_time
,vev3.event_type
from sk_prod.VESPA_STB_PROG_EVENTS_20110813 as vev3
where
-- (play_back_speed is null or play_back_speed=2   ) and x_programme_viewed_duration>0
--and 
panel_id in (4,5)


union all

select  vev4.subscriber_id 
,vev4.document_creation_date
,vev4.stb_log_creation_date
,vev4.adjusted_event_start_time
,vev4.x_adjusted_event_end_time
,vev4.event_type
from sk_prod.VESPA_STB_PROG_EVENTS_20110814 as vev4
where
-- (play_back_speed is null or play_back_speed=2   ) and x_programme_viewed_duration>0
--and 
panel_id in (4,5)



union all

select  vev5.subscriber_id 
,vev5.document_creation_date
,vev5.stb_log_creation_date
,vev5.adjusted_event_start_time
,vev5.x_adjusted_event_end_time
,vev5.event_type
from sk_prod.VESPA_STB_PROG_EVENTS_20110815 as vev5
where
-- (play_back_speed is null or play_back_speed=2   ) and x_programme_viewed_duration>0
--and 
panel_id in (4,5)



union all

select  vev6.subscriber_id 
,vev6.document_creation_date
,vev6.stb_log_creation_date
,vev6.adjusted_event_start_time
,vev6.x_adjusted_event_end_time
,vev6.event_type
from sk_prod.VESPA_STB_PROG_EVENTS_20110816 as vev6
where
-- (play_back_speed is null or play_back_speed=2   ) and x_programme_viewed_duration>0
--and 
panel_id in (4,5)


union all

select  vev7.subscriber_id 
,vev7.document_creation_date
,vev7.stb_log_creation_date
,vev7.adjusted_event_start_time
,vev7.x_adjusted_event_end_time
,vev7.event_type
from sk_prod.VESPA_STB_PROG_EVENTS_20110817 as vev7
where
-- (play_back_speed is null or play_back_speed=2   ) and x_programme_viewed_duration>0
--and 
panel_id in (4,5)


union all

select  vev8.subscriber_id 
,vev8.document_creation_date
,vev8.stb_log_creation_date
,vev8.adjusted_event_start_time
,vev8.x_adjusted_event_end_time
,vev8.event_type
from sk_prod.VESPA_STB_PROG_EVENTS_20110818 as vev8
where
-- (play_back_speed is null or play_back_speed=2   ) and x_programme_viewed_duration>0
--and 
panel_id in (4,5)


;


--select top 100 * from sk_prod.VESPA_STB_PROG_EVENTS_20110801;


commit;



--select top 500 * from #vespa_week_combined order by subscriber_id , adjusted_event_start_time,x_adjusted_event_end_time;




--drop table vespa_analysts.daily_summary_by_subscriber_20110811;
commit;
select subscriber_id
,min(adjusted_event_start_time) as first_event_date


,max(case   when event_type = 'evEmptyLog' and document_creation_date between '2011-08-11 05:00:00' and '2011-08-12 04:59:59' then 1 
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-11 05:00:00' and  adjusted_event_start_time <'2011-08-12 04:59:59' then 1
else 0
 end) as events_2011_08_11

,max(case   when event_type = 'evEmptyLog' and document_creation_date between '2011-08-12 05:00:00' and '2011-08-13 04:59:59' then 1 
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-12 05:00:00' and  adjusted_event_start_time <'2011-08-13 04:59:59' then 1
else 0
 end) as events_2011_08_12

,max(case   when event_type = 'evEmptyLog' and document_creation_date between '2011-08-13 05:00:00' and '2011-08-14 04:59:59' then 1 
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-13 05:00:00' and  adjusted_event_start_time <'2011-08-14 04:59:59' then 1
else 0
 end) as events_2011_08_13

,max(case   when event_type = 'evEmptyLog' and document_creation_date between '2011-08-14 05:00:00' and '2011-08-15 04:59:59' then 1 
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-14 05:00:00' and  adjusted_event_start_time <'2011-08-15 04:59:59' then 1
else 0
 end) as events_2011_08_14

,max(case   when event_type = 'evEmptyLog' and document_creation_date between '2011-08-15 05:00:00' and '2011-08-16 04:59:59' then 1 
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-15 05:00:00' and  adjusted_event_start_time <'2011-08-16 04:59:59' then 1
else 0
 end) as events_2011_08_15

,max(case   when event_type = 'evEmptyLog' and document_creation_date between '2011-08-16 05:00:00' and '2011-08-17 04:59:59' then 1 
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-16 05:00:00' and  adjusted_event_start_time <'2011-08-17 04:59:59' then 1
else 0
 end) as events_2011_08_16

,max(case   when event_type = 'evEmptyLog' and document_creation_date between '2011-08-17 05:00:00' and '2011-08-18 04:59:59' then 1 
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-17 05:00:00' and  adjusted_event_start_time <'2011-08-18 04:59:59' then 1
else 0
 end) as events_2011_08_17

,max(case   when event_type = 'evEmptyLog' and document_creation_date between '2011-08-18 05:00:00' and '2011-08-19 04:59:59' then 1 
            when event_type <> 'evEmptyLog' and adjusted_event_start_time >='2011-08-18 05:00:00' and  adjusted_event_start_time <'2011-08-19 04:59:59' then 1
else 0
 end) as events_2011_08_18
into vespa_analysts.daily_summary_by_subscriber_20110811
from #vespa_week_combined
group by subscriber_id

;

--select sum(events_2011_08_22) from vespa_analysts.daily_summary_by_subscriber_20110811;
commit;
---Append Number of distinct days returning viewing data (needs to be 8 for full data)----






--create hg index idx1 on vespa_analysts.daily_summary_by_subscriber_20110811(subscriber_id);
alter table vespa_analysts.sky_base_v2_2011_08_11 add days_returning_data integer;

alter table vespa_analysts.sky_base_v2_2011_08_11 add days_returning_data_2011_08_11 integer;
alter table vespa_analysts.sky_base_v2_2011_08_11 add days_returning_data_2011_08_12 integer;
alter table vespa_analysts.sky_base_v2_2011_08_11 add days_returning_data_2011_08_13 integer;
alter table vespa_analysts.sky_base_v2_2011_08_11 add days_returning_data_2011_08_14 integer;
alter table vespa_analysts.sky_base_v2_2011_08_11 add days_returning_data_2011_08_15 integer;
alter table vespa_analysts.sky_base_v2_2011_08_11 add days_returning_data_2011_08_16 integer;
alter table vespa_analysts.sky_base_v2_2011_08_11 add days_returning_data_2011_08_17 integer;
alter table vespa_analysts.sky_base_v2_2011_08_11 add days_returning_data_2011_08_18 integer;



--alter table vespa_analysts.uk_base_20110811 add days_returning_data_2011_06_20 integer;

update vespa_analysts.sky_base_v2_2011_08_11
set days_returning_data=case when events_2011_08_18 is null then 0 else events_2011_08_11+events_2011_08_12+events_2011_08_13+events_2011_08_14+events_2011_08_15+events_2011_08_16+events_2011_08_17+events_2011_08_18 end 
,days_returning_data_2011_08_11=case when events_2011_08_11 is null then 0 else events_2011_08_11 end 
,days_returning_data_2011_08_12=case when events_2011_08_12 is null then 0 else events_2011_08_12 end 
,days_returning_data_2011_08_13=case when events_2011_08_13 is null then 0 else events_2011_08_13 end 
,days_returning_data_2011_08_14=case when events_2011_08_14 is null then 0 else events_2011_08_14 end 
,days_returning_data_2011_08_15=case when events_2011_08_15 is null then 0 else events_2011_08_15 end 
,days_returning_data_2011_08_16=case when events_2011_08_16 is null then 0 else events_2011_08_16 end 
,days_returning_data_2011_08_17=case when events_2011_08_17 is null then 0 else events_2011_08_17 end 
,days_returning_data_2011_08_18=case when events_2011_08_18 is null then 0 else events_2011_08_18 end 
from vespa_analysts.sky_base_v2_2011_08_11 as a
left outer join vespa_analysts.daily_summary_by_subscriber_20110811 as b
on a.subscriber_id=b.subscriber_id
;
commit;

--select top 500 * from vespa_analysts.sky_base_v2_2011_08_11;


--------------------------------------------------------------------------------
-- PART B - STRATIFIED SAMPLING
--------------------------------------------------------------------------------

/*
PART B   - Stratified Sampling
     B01 - Create Random number
     B02 - Work out size of sample for each combination
     B03 - Random sampling
     B04 - Flag accounts which make up 150k sample
*/

/*
---------------------------------------------------------------------------------- B01 - Create Random number

-- B01 - Create Random number



--Create a pretty random multiplier
CREATE VARIABLE multiplier bigint; --Has to be a bigint if you are dealing with millions of records.
SET multiplier = DATEPART(millisecond,now())+1; -- pretty random number between 1 and 1000

-- Populate random number
ALTER TABLE vespa_analysts.sky_base_v2_2011_08_11 add rand_num DECIMAL(22,20);          --Make a decimal field to 20 places.
UPDATE vespa_analysts.sky_base_v2_2011_08_11 SET rand_num = rand(number(*)*multiplier); --The Number(*) function just gives a sequential number.

--354525 Row(s) affected

CREATE hg INDEX idx_rand_num_hg ON vespa_analysts.sky_base_v2_2011_08_11(rand_num);


--select top 100 * from vespa_analysts.sky_base_v2_2011_08_11;
*/

---------------------------------------------------------------------------------- B02 - Work out size of sample for each combination

-- B02 - Work out size of sample for each combination


if object_id('vespa_analysts.stratifiedsampling_20110811') is not null drop table vespa_analysts.stratifiedsampling_20110811;
CREATE TABLE vespa_analysts.stratifiedsampling_20110811 ( -- drop table vespa_analysts.stratifiedsampling_20110811
         lifestage          varchar(50)
         ,HHAfflu           varchar(10)
         ,isba_tv_region    varchar(20)
         ,pvr               tinyint
         ,package           varchar(30)
        ,primary_box        integer
         ,boxes             integer
         ,box_data_20110811         integer
         ,box_data_20110812         integer
         ,box_data_20110813         integer
         ,box_data_20110814         integer
         ,box_data_20110815         integer
         ,box_data_20110816         integer
         ,box_data_20110817         integer
         ,box_data_20110818         integer

         )

commit;
grant select, insert, delete, update on  vespa_analysts.stratifiedsampling_20110811 to greenj, dbarnett, jacksons, stafforr, bthakrar, sbednaszynski, jchung, smoore , vespa_analysts;

commit;

Insert into vespa_analysts.stratifiedsampling_20110811 (
         lifestage
         ,HHAfflu
         ,isba_tv_region
         ,pvr
         ,package
        ,primary_box
         ,boxes
         ,box_data_20110811         
         ,box_data_20110812         
         ,box_data_20110813         
         ,box_data_20110814         
         ,box_data_20110815         
         ,box_data_20110816         
         ,box_data_20110817         
         ,box_data_20110818         
)
select
          Lifestage
         ,HHAfflu
         ,isba_tv_region
         ,pvr
         ,package
        ,primary_box
         ,count(*) as boxes
         ,sum(days_returning_data_2011_08_11) as box_data_20110811         
         ,sum(days_returning_data_2011_08_12) as box_data_20110812         
         ,sum(days_returning_data_2011_08_13) as box_data_20110813         
         ,sum(days_returning_data_2011_08_14) as box_data_20110814         
         ,sum(days_returning_data_2011_08_15) as box_data_20110815         
         ,sum(days_returning_data_2011_08_16) as box_data_20110816         
         ,sum(days_returning_data_2011_08_17) as box_data_20110817         
         ,sum(days_returning_data_2011_08_18) as box_data_20110818         

from vespa_analysts.sky_base_v2_2011_08_11 pb
group by lifestage, HHAfflu, isba_tv_region, pvr, package ,primary_box
order by lifestage, HHAfflu, isba_tv_region, pvr, package ,primary_box;
commit;

--select * from vespa_analysts.stratifiedsampling_20110811 order by boxes desc;








Create variable @total_sample numeric(28,20);
set @total_sample = (select sum(sample_hh) from vespa_analysts.stratifiedsampling_20110811)

Update vespa_analysts.stratifiedsampling_20110811
set num_records = 0;

update vespa_analysts.stratifiedsampling_20110811
   set num_records = case when sample_hh > 0 then sample_hh * (150000.00000000 / cast(@total_sample as float))
                          else 0 end ;

update vespa_analysts.stratifiedsampling_20110811
   set num_records = case when num_records > 0 and num_records < 1 then 1
                          else round(num_records,0) end;

/*
Notes on Stratified Sampling code
---------------------------------

This piece of code returns all 10 million account numbers and creates a flag which determines whether or not
the account should be included in the new sample.
It does this by ranking over the 5 variables and flagging a "1" if the rank is less than or equal to the required number of records
and a "0" if the rank goes over the required number of records.
The query links the account number to the required combination of variables through its join on the 5 variables.

*/


---------------------------------------------------------------------------------- B03 - Random sampling

-- B03 - Random sampling




select
        cb_key_household,
        rank() over (partition by base.lifestage, base.hhafflu,base.isba_tv_region, base.pvr, base.package
                                      order by base.rand_num) as rank
        ,ss.num_records
into #temp -- drop table #temp
from vespa_analysts.sky_base_v2_2011_08_11 as base
        inner join vespa_analysts.stratifiedsampling_20110811 as ss on ss.lifestage = base.lifestage
                                           and ss.hhafflu = base.hhafflu
                                           and ss.isba_tv_region = base.isba_tv_region
                                           and ss.pvr = base.pvr
                                           and ss.package = base.package
where base.vespa_flag = 1;
--354525 Row(s) affected
--10035768 Row(s) affected


update #temp
set num_records = case when num_records >= rank then 1 else 0 end


create hg index indx1 on vespa_analysts.stratifiedsampling_20110811(lifestage,hhafflu,isba_tv_region,pvr,package)



---------------------------------------------------------------------------------- B04 - Flag accounts which make up 150k sample

-- B04 - Flag accounts which make up 150k sample


alter table vespa_analysts.sky_base_v2_2011_08_11
add sample_flag tinyint default 0;

Update vespa_analysts.sky_base_v2_2011_08_11
set sample_flag = 1
from vespa_analysts.sky_base_v2_2011_08_11 as base
        inner join #temp as tem on tem.cb_key_household = base.cb_key_household
where tem.num_records = 1;



--------------------------------------------------------------------------------
-- PART C - CALCULATION OF WEIGHTS AND SCALING FACTOR
--------------------------------------------------------------------------------

/*
PART C   - Calculation of Weights and Scaling factor
     C01 - Calculate weights based on 150k sample
     C02 - Allocate weights to account_numbers
     C03 - Create Output tables
*/


---------------------------------------------------------------------------------- C01 - Calculate weights based on 150k sample

-- C01 - Calculate weights based on 150k sample

if object_id('Sky_weightings_table_all_variables_HH') is not null drop table Sky_weightings_table_all_variables_HH;
CREATE TABLE Sky_weightings_table_all_variables_HH ( -- drop table govt_region_base
         lifestage          varchar(50)
         ,HHAfflu           varchar(10)
         ,isba_tv_region    varchar(20)
         ,pvr               tinyint
         ,package           varchar(30)
         ,sky_hh             integer
         ,sky_proportion     decimal(20,16)
         ,sample_hh         integer
         ,sample_proportion decimal(20,16)
         ,weighting_ratio   decimal(20,16))


commit;
grant select, insert, delete, update on  Sky_weightings_table_all_variables_HH to greenj, dbarnett, jacksons, stafforr, bthakrar, sbednaszynski, jchung, smoore;


Insert into Sky_weightings_table_all_variables_HH (
         lifestage
         ,HHAfflu
         ,isba_tv_region
         ,pvr
         ,package
         ,sky_hh
         ,sample_hh)
select
          Lifestage
         ,HHAfflu
         ,isba_tv_region
         ,pvr
         ,package
         ,count(distinct account_number) as sky_hh
         ,count(distinct case when sample_flag =1 then account_number else null end) as sample_hh
from vespa_analysts.sky_base_v2_2011_08_11 pb
group by lifestage, HHAfflu, isba_tv_region, pvr, package
order by lifestage, HHAfflu, isba_tv_region, pvr, package;
commit;


select * from Sky_weightings_table_all_variables_HH
--UK proportion

Create variable @sky_total numeric(28,20);
set @sky_total = (select sum(sky_hh) from Sky_weightings_table_all_variables_HH)

update Sky_weightings_table_all_variables_HH
   set sky_proportion = cast(sky_hh as float) / cast(@sky_total as float);

--Sample proportion

Create variable @Sample_total numeric(28,20);
set @Sample_total = (select sum(sample_hh) from Sky_weightings_table_all_variables_HH)

update Sky_weightings_table_all_variables_HH
   set sample_proportion = cast(sample_hh as float) / cast(@sample_total as float);

-- Calculate ratios
Update Sky_weightings_table_all_variables_HH
   set weighting_ratio = 1
where sample_hh = 0


Update Sky_weightings_table_all_variables_HH
   set weighting_ratio = (sky_proportion) / (sample_proportion)
where sample_hh <> 0


-- Update Sky_base with weightings

create lf index idx_lifestage_lf           on vespa_analysts.sky_base_v2_2011_08_11(lifestage);
create lf index idx_hhafflu_lf           on vespa_analysts.sky_base_v2_2011_08_11(hhafflu);
create lf index idx_isba_lf           on vespa_analysts.sky_base_v2_2011_08_11(isba_tv_region);



create lf index idx_lifestage_lf           on Sky_weightings_table_all_variables_HH(lifestage);
create lf index idx_hhafflu_lf             on Sky_weightings_table_all_variables_HH(hhafflu);
create lf index idx_isba_lf                on Sky_weightings_table_all_variables_HH(isba_tv_region);
create lf index idx_pvr_lf                 on Sky_weightings_table_all_variables_HH(pvr);
create lf index idx_package_lf             on Sky_weightings_table_all_variables_HH(package);



---------------------------------------------------------------------------------- C02 - Allocate weights to account_numbers

-- C02 - Allocate weights to account_numbers



alter table vespa_analysts.sky_base_v2_2011_08_11
add Weighting decimal(20,16) --default 1;


Update vespa_analysts.sky_base_v2_2011_08_11 base
   set weighting = weighting_ratio
  from Sky_weightings_table_all_variables_HH we
 where sample_flag = 1
   and base.lifestage = we.lifestage
   and base.hhafflu = we.hhafflu
   and base.isba_tv_region = we.isba_tv_region
   and base.pvr = we.pvr
   and base.package = we.package;
--152693 Row(s) affected



alter table vespa_analysts.sky_base_v2_2011_08_11
add weighting_scaled decimal(20,16)


Create variable @weightings_total numeric(28,20);
set @weightings_total = (select sum(weighting) from vespa_analysts.sky_base_v2_2011_08_11)


/*
Create variable @sky_total numeric(28,20);
set @sky_total = (select sum(sky_hh) from Sky_weightings_table_all_variables_HH)
*/
--149619.70

Create variable @scaling_factor numeric(28,20);
set @scaling_factor = cast(@sky_total as float) / cast(@weightings_total as float)


update vespa_analysts.sky_base_v2_2011_08_11
set weighting_scaled = cast(weighting as float) * cast(@scaling_factor as float)


commit;


---------------------------------------------------------------------------------- C03 - Create Output tables

-- C03 - Create Output tables

--List of 150K accounts with weightings

if object_id('Sky_weightings') is not null drop table Sky_weightings;
CREATE TABLE Sky_weightings ( -- drop table Sky_weightings
          cb_key_household                 bigint
         ,Weighting                     decimal(10,3) NOT NULL);

commit;
grant select, insert, delete, update on  Sky_weightings to public;

Insert into Sky_weightings (cb_key_household, weighting)
select cb_key_household, min(weighting)
from vespa_analysts.sky_base_v2_2011_08_11
where sample_flag = 1
group by cb_key_household;
commit;

Create variable @weightings numeric(28,20);
set @weightings = (select sum(weighting) from sky_weightings)

--Fudge because demo is at household level and we want to ensure sum of weightings
--is equal to total accounts of 10 million.

Create variable @sky numeric(28,20);
set @sky = (select count(distinct account_number) from vespa_analysts.sky_base_v2_2011_08_11)

Create variable @scaling numeric(28,20);
set @scaling = cast(@sky as float) / cast(@weightings as float)


update sky_weightings
set weighting = cast(weighting as float) * cast(@scaling as float)
/*
select sum(weighting) from sky_weightings
--10035694.262
*/
/*
select count(distinct cb_key_household) from vespa_analysts.sky_base_v2_2011_08_11 where sample_flag = 1
--148344
select sum(weighting) from sky_weightings
--9017074.930

select count(distinct cb_key_household) from vespa_analysts.sky_base_v2_2011_08_11
--9546754

Therefore weightings will underestimate by 5%
*/

if object_id('output1') is not null drop table output1;
CREATE TABLE output1 ( -- drop table output1
         lifestage          varchar(50)
         ,HHAfflu           varchar(10)
         ,isba_tv_region    varchar(20)
         ,pvr               tinyint
         ,package           varchar(30)
         ,uk_hh             integer
         ,sample_hh         integer
         ,weighting_ratio   decimal(20,16) default 0.0)


commit;
grant select, insert, delete, update on  output1 to greenj, dbarnett, jacksons, stafforr, bthakrar, sbednaszynski, jchung, smoore vespa_analysts;

--133851

Insert into output1 (
         lifestage
         ,HHAfflu
         ,isba_tv_region
         ,pvr
         ,package
         ,uk_hh
         ,sample_hh
         ,weighting_ratio)
select
          Lifestage
         ,HHAfflu
         ,isba_tv_region
         ,pvr
         ,package
         ,count(distinct account_number) as uk_hh
         ,count(distinct case when sample_flag =1 then account_number else null end) as sample_hh
         ,max(weighting_scaled) as weighting
from vespa_analysts.sky_base_v2_2011_08_11 pb
group by lifestage, HHAfflu, isba_tv_region, pvr, package
order by lifestage, HHAfflu, isba_tv_region, pvr, package;
commit;


update output1
set weighting_ratio = 0
where weighting_ratio is null;

select * from jacksons.output1 order by weighting_ratio desc;


select count(*) as cells
,sum(uk_hh) as households
,sum(sample_hh) as sample_households
from jacksons.output1




/*
--select top 500 * from vespa_analysts.sky_base_v2_2011_08_11;
--select days_returning_data_2011_08_11 ,count(*) from vespa_analysts.sky_base_v2_2011_08_11 group by days_returning_data_2011_08_11 order by days_returning_data_2011_08_11;



select cb_key_household 
, count(distinct account_number) as distinct_accounts
, count(distinct subscriber_id) as distinct_subscriber_id
into #summary_by_hh
from vespa_analysts.sky_base_v2_2011_08_11
group by cb_key_household
;

select distinct_accounts ,count(*) as households from #summary_by_hh group by distinct_accounts order by distinct_accounts;

select * from #summary_by_hh where distinct_accounts=3;

select cb_address_status , cb_address_dps , account_number , acct_fo_created_dt ,cb_address_line_1 ,cb_address_line_2 ,cb_address_line_3 , cb_address_line_4 , cb_address_postcode , acct_status_code ,cb_key_household  from sk_prod.cust_single_account_view where cb_key_household =1963502818295808


select acct_fo_created_dt , count(*) from sk_prod.cust_single_account_view where ph_subs_first_activation_dt is null group by acct_fo_created_dt order by acct_fo_created_dt

select * from sk_prod.cust_subs_hist where account_number = '621642578069'
*/

