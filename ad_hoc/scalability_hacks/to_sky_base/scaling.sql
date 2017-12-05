/*------------------------------------------------------------------------------
        Project: Scaling
        Version: 1
        Created: 25/10/2011
        Lead:    Sarah Moore
        Analyst: Julie Chung
        SK Prod: 4
*/------------------------------------------------------------------------------
/*
Purpose: To produce a pivot table based on one month's viewing data (01/07/2011).

We have only extracted actual viewing data e.g. EPG and interactive services have been excluded.
This is at session level

*/

drop table session_capping_dataset;

-- Create base table
create table   session_capping_dataset(account_number            varchar(30)
                                      ,subscriber_id             bigint
                                      ,cb_key_household          bigint
                                      ,channel_name              varchar(50)
                                      ,adjusted_event_date       date
                                      ,adjusted_event_start_time datetime
                                      ,x_viewing_start_time      datetime
                                      ,x_viewing_end_time        datetime
                                      ,x_event_duration          int
                                      ,recorded_time_UTC         datetime
                                      ,pvr                       bit        default 0
                                      ,box_type                  varchar(2) default 'SD'
                                      ,primary_box               bit        default 0
                                      ,package                   varchar(30)
                                      ,lifestage                 varchar(50)
                                      ,day_part                  varchar(30)
                                      ,live                      bit        default 0 --1= live, 0=playback
                                      ,programme_trans_sk        bigint
                                      ,genre_description         varchar(20)
                                      ,start_hour                int
                                      ,dur_mins                  int
                                      ,dur_hours                 int
                                      ,dur_days                  int
);
commit;

--create table for all events for a single day's viewing events. This is at event level.
--Regional channel variations are grouped together into BBC1, BBC2 and ITV1.
--The rest of the top 50 channels (based on total viewing duration) are listed individually,
--and others are grouped together as 'Other Channel'

----channel distribution
--   select case when channel_name like 'BBC 1%' then 'BBC 1'
--               when channel_name like 'BBC 2%' then 'BBC 2'
--               when channel_name like 'ITV1%'  then 'ITV1'
--               else channel_name end as chan
--         ,count(*) as cow
--         ,sum(x_event_duration)
--     from session_capping_dataset
-- group by chan
-- order by cow desc

--drop procedure insert_recs;

create procedure insert_recs(@dset varchar(50)='tname') as

begin

execute('  insert into session_capping_dataset(account_number
                                     ,subscriber_id
                                     ,channel_name
                                     ,adjusted_event_date
                                     ,adjusted_event_start_time
                                     ,x_viewing_start_time
                                     ,x_viewing_end_time
                                     ,x_event_duration
                                     ,recorded_time_UTC
                                     ,day_part
                                     ,live
                                     ,programme_trans_sk
                                     ,genre_description
)
  select account_number
        ,subscriber_id
        ,case when channel_name like ''BBC 1%'' then ''BBC 1''
              when channel_name like ''ITV1%''  then ''ITV1''
              when channel_name like ''BBC 2%'' then ''BBC 2''
              when channel_name in (''BBC One HD'',''Channel 4'',''Sky Sp NewsHD'',''Sky Sports HD1''
                                   ,''Channel 5'',''Sky1 HD'',''Disney Chnl'',''CBeebies'',''Sky News HD''
                                   ,''Sky Spts News'',''STV'',''Disney Junior'',''GOLD'',''BBC THREE'',''Nick Jr.''
                                   ,''Dave'',''Watch'',''ITV2 HD'',''Anytime'',''Sky1'',''Comedy Cen HD''
                                   ,''Disney Chnl+1'',''Sky Sports 1'',''BBC NEWS'',''Sky Living HD''
                                   ,''Channel 4 HD'',''Sky Prem+1'',''Boomerang'',''ITV2'',''MTV'',''ComedyCtrl+1''
                                   ,''SkyPremiereHD'',''Sky Sports HD2'',''STAR Plus'',''BBC HD'',''Sky News''
                                   ,''Disney Junior+'',''Channel 4 +1'',''E4'',''SkyShowcseHD'',''Cartoon Netwrk''
                                   ,''E4+1'',''Nick Jr. 2'',''Universal HD'',''ComedyCentral'',''E4 HD''
                                   ,''Sky Living'') then channel_name
              else ''Other Channel'' end
        ,adjusted_event_date
        ,adjusted_event_start_time
        ,x_viewing_start_time
        ,x_viewing_end_time
        ,x_event_duration
        ,recorded_time_UTC
        ,x_viewing_time_of_day
        ,case when play_back_speed is null then 1 else 0 end as live
        ,vev.programme_trans_sk
        ,genre_description
    from '||@dset||' as vev
         left join sk_prod.vespa_epg_dim       as epg on vev.programme_trans_sk = epg.programme_trans_sk
   where video_playing_flag = 1
     and adjusted_event_start_time <> x_adjusted_event_end_time
     and (    x_type_of_viewing_event in (''TV Channel Viewing'',''Sky+ time-shifted viewing event'')
          or (x_type_of_viewing_event = (''Other Service Viewing Event'')
              and x_si_service_type = ''High Definition TV test service''))
     and panel_id = 5
group by account_number
        ,subscriber_id
        ,channel_name
        ,adjusted_event_date
        ,adjusted_event_start_time
        ,x_viewing_start_time
        ,x_viewing_end_time
        ,x_event_duration
        ,recorded_time_UTC
        ,x_viewing_time_of_day
        ,live
        ,vev.programme_trans_sk
        ,genre_description
')

commit
end;

execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110701''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110702''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110703''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110704''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110705''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110706''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110707''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110708''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110709''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110710''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110711''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110712''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110713''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110714''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110715''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110716''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110717''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110718''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110719''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110720''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110721''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110722''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110723''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110724''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110725''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110726''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110727''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110728''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110729''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110730''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110731''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110801''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110802''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110803''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110804''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110805''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110806''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110807''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110808''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110809''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110810''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110811''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110812''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110813''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110814''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110815''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110816''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110817''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110818''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110819''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110820''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110821''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110822''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110823''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110824''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110825''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110826''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110827''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110828''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110829''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110830''');
execute('insert_recs @dset =''sk_prod.VESPA_STB_PROG_EVENTS_20110831''');

--QA
--select count(*),count(distinct account_number),count(distinct subscriber_id) from session_capping_dataset
--774761164       340816  423780

--select distinct adjusted_event_date,count(*) from session_capping_dataset group by adjusted_event_date

--duration bandings for viewing sessions
  update session_capping_dataset
     set start_hour = datepart (hour, adjusted_event_start_time)
        ,dur_mins   = cast(x_event_duration/ 60    as int)
        ,dur_hours  = cast(x_event_duration/ 3600  as int)
        ,dur_days   = cast(x_event_duration/ 86400 as int); --6,257,732
        commit;

--creating a date variable to use throughout the code
  create variable @target_date date;
     set @target_date = '2011-08-31';

--to calculate PVR and Box type at subscriber level
  select cast(csi.si_external_identifier as bigint) as subscriber
        ,stb.x_pvr_type
        ,stb.x_box_type
    into #box_data
    from sk_prod.cust_service_instance       as csi
         inner join sk_prod.cust_subs_hist   as csh on csi.src_system_id       = csh.service_instance_id
         inner join sk_prod.cust_set_top_box as stb on csh.service_instance_id = stb.service_instance_id
   where csh.effective_from_dt <= @target_date
     and csh.effective_to_dt   >  @target_date
     and csh.subscription_sub_type in ('DTV Primary Viewing', 'DTV Extra subscription')
group by subscriber
        ,stb.x_pvr_type
        ,stb.x_box_type
; --21,266,857

commit;
create index idx_subscriber_hg on #box_data(subscriber);
create index idx_subscriber_hg on session_capping_dataset(subscriber_id);
create index idx_account_number_hg on session_capping_dataset(account_number);

--update PVR flag at subscriber level
  update session_capping_dataset as bas
     set pvr = 1
    from #box_data
   where x_pvr_type like '%PVR%'
     and bas.subscriber_id = #box_data.subscriber
; --5,481,638

--update box type at subscriber level
  update session_capping_dataset as bas
     set box_type = case when x_box_type like '%HD%' then 'HD' else 'SD' end
    from #box_data
   where bas.subscriber_id = #box_data.subscriber
; --5,488,010

--QA
--select pvr,box_type,count(*) as cow from session_capping_dataset group by  pvr,box_type
-- pvr box_type cow
-- 1 HD 3572608
-- 0 HD 991
-- 1 SD 1909030
-- 0 SD 723395

--update primary box
  update session_capping_dataset as bas
     set primary_box = 1
    from sk_prod.vespa_stb_log_snapshot as stb
   where bas.subscriber_id = stb.subscriber_id
     and service_instance_type = 'P'
; --4,560,899

--package
  update session_capping_dataset as bas
     set package = case when cel.prem_sports = 2 and cel.prem_movies = 2 then 'Top Tier'
                        when cel.prem_sports = 2 and cel.prem_movies = 0 then 'Dual Sports'
                        when cel.prem_sports = 0 and cel.prem_movies = 2 then 'Dual Movies'
                        when cel.prem_sports = 1 and cel.prem_movies = 0 then 'Single Sports'
                        when cel.prem_sports = 0 and cel.prem_movies = 1 then 'Single Movies'
                        when cel.prem_sports > 0 or  cel.prem_movies > 0 then 'Other Premiums'
                        else                                                  'Basic' end
    from sk_prod.cust_subs_hist                     as csh
         inner join sk_prod.cust_entitlement_lookup as cel on csh.current_short_description = cel.short_description
   where bas.account_number = csh.account_number
     and csh.subscription_sub_type ='DTV Primary Viewing'
     and csh.status_code in ('AB','AC','PC')
     and csh.effective_from_dt <= adjusted_event_date
     and csh.effective_to_dt   >  adjusted_event_date
; --6,206,024

--add ilu variables
alter table session_capping_dataset
add pty_country_code varchar(10),
add postcode varchar(10),
add HHsize integer,
add HHComposition varchar(2),
add OwnRnt integer,
add HHLenres integer,
add PropertyType integer,
add NumBedrooms integer,
add HHKids integer,
add HHAfflu integer,
add hhpcown tinyint,
add broadband tinyint,
add hhtechnorank integer,
add hhpersonicx varchar(5),
add exchange_status varchar(20),
add tenure varchar(50) default 'Unknown',
add gov_region varchar(50),
add isba_tv_region varchar(20);
--add product_holdings varchar(50),
--add box_holding varchar(20),
--add value_segments varchar(50);
commit;

--select top 10 * from session_capping_dataset

update session_capping_dataset as bas
set cb_key_household=sav.cb_key_household
   ,postcode=sav.cb_address_postcode
   ,pty_country_code=sav.pty_country_code
from sk_prod.cust_single_account_view as sav
where bas.account_number = sav.account_number;
commit;
--774761164

select ilu.cb_row_id
      ,bas.account_number
      ,bas.cb_key_household
      ,max(case when ilu.ilu_correspondent = 'P1' then 1 else 0 end) as p1
      ,max(case when ilu.ilu_correspondent = 'P2' then 1 else 0 end) as p2
      ,max(case when ilu.ilu_correspondent = 'OR' then 1 else 0 end) as or1
into #temp
from sk_prod.ilu as ilu
inner join
session_capping_dataset as bas
on bas.cb_key_household = ilu.cb_key_household
and bas.cb_key_household is not null
and bas.cb_key_household > 0
group by ilu.cb_row_id
        ,bas.account_number
        ,bas.cb_key_household
having p1 + p2 + or1 > 0;
 --711048

select cb_row_id
      ,account_number
      ,cb_key_household
      ,case when p1 = 1 then 1
            when p2 = 1 then 2
            else        3
       end as correspondent
      ,rank() over(partition by account_number order by correspondent asc, cb_row_id desc) as rank
into #ilu
from #temp;
--711048

commit;
create hg index idx_cb_row_id_hg      on #ilu(cb_row_id);
create hg index idx_account_number_hg on #ilu(account_number);

delete from #ilu
where rank > 1;
--388586

select count(*),count(distinct account_number) from #ilu;
--322462

update session_capping_dataset as bas
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
from #ilu
inner join
sk_prod.ilu as ilu
on #ilu.cb_row_id = ilu.cb_row_id
where bas.account_number = #ilu.account_number;
--739359295

--add third party vars
update session_capping_dataset t1
set hhsize=t2.ilu_hhsize,
hhcomposition=t2.ilu_hhcomposition,
ownrnt=t2.ilu_p1ownrnt,
hhlenres=t2.ilu_hhlenres,
PropertyType=t2.ILU_P1PropertyType,
NumBedrooms=t2.ilu_p1numbedrooms,
HHKids=t2.ilu_hhkids,
HHAfflu=t2.ilu_hhafflu,
hhpcown=t2.ilu_hhpcown,
broadband=t2.broadband,
hhtechnorank=t2.ilu_hhtechnorank,
hhpersonicx=t2.ils_hhpersonicx2
from #ilu
inner join
sk_prod.ilu as t2
on #ilu.cb_row_id=t2.cb_row_id
where t1.account_number=#ilu.account_number;
commit;
--739359295

--add government region
update session_capping_dataset t1
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
--773513448

--add on-net/off-net
update session_capping_dataset t1
set exchange_status=t2.new_status
from postcode_lkup3 t2
where replace(t1.postcode, ' ','') *= replace(t2.address_postcode, ' ','');
commit;

update session_capping_dataset t1
set exchange_status=case
                      when exchange_status in ('Onnet','Midnet','Soldout','Soldout Midnet') then 'Onnet'
                      when exchange_status in ('Offnet','Futurenet') then 'Offnet'
                      else exchange_status
                    end;
commit;
--774761164

--add tenure & isba_region
update session_capping_dataset t1
set tenure=case when datediff(day,acct_first_account_activation_dt,@target_date) <=   91 then 'A) 0-3 Months'
                when datediff(day,acct_first_account_activation_dt,@target_date) <=  182 then 'B) 4-6 Months'
                when datediff(day,acct_first_account_activation_dt,@target_date) <=  365 then 'C) 6-12 Months'
                when datediff(day,acct_first_account_activation_dt,@target_date) <=  730 then 'D) 1-2 Years'
                when datediff(day,acct_first_account_activation_dt,@target_date) <= 1095 then 'E) 2-3 Years'
                when datediff(day,acct_first_account_activation_dt,@target_date) <= 1825 then 'F) 3-5 Years'
                when datediff(day,acct_first_account_activation_dt,@target_date) <= 3650 then 'G) 5-10 Years'
                when datediff(day,acct_first_account_activation_dt,@target_date) > 3650 then  'H) 10 Years+ '
                else 'I) Unknown'
           end
   ,isba_tv_region=sav.isba_tv_region
from sk_prod.cust_single_account_view sav
where t1.account_number=sav.account_number;
commit;
--774761164

create lf index idx_start_hour_lf    on session_capping_dataset(start_hour);
create lf index idx_pvr_lf           on session_capping_dataset(pvr);
create lf index idx_box_type_lf      on session_capping_dataset(box_type);
create lf index idx_primary_box_lf   on session_capping_dataset(primary_box);
create lf index idx_package_lf       on session_capping_dataset(package);
create lf index idx_lifestage_lf     on session_capping_dataset(lifestage);
create lf index idx_dur_hours_lf     on session_capping_dataset(dur_hours);
create lf index idx_band_dur_days_lf on session_capping_dataset(band_dur_days);
create lf index idx_day_part_lf      on session_capping_dataset(day_part);
create lf index idx_live_lf          on session_capping_dataset(live);

--select top 10 * from session_capping_dataset;
--select distinct gov_region from session_capping_dataset;
--select distinct exchange_status from session_capping_dataset;
--select distinct hhlenres from session_capping_dataset

--add capping rules

--output
select adjusted_event_date
      ,start_hour
      ,genre_description
      ,pvr
      ,box_type
      ,primary_box
      ,lifestage
      ,CASE WHEN hhafflu in (1,2,3,4)  THEN 'Very Low'
            WHEN hhafflu in (5,6)      THEN 'Low'
            WHEN hhafflu in (7,8)      THEN 'Mid Low'
            WHEN hhafflu in (9,10)     THEN 'Mid'
            WHEN hhafflu in (11,12)    THEN 'Mid High'
            WHEN hhafflu in (13,14,15) THEN 'High'
            WHEN hhafflu in (16,17)    THEN 'Very High'
            ELSE                            'Unknown'
       END as HHAfflu
      ,count(distinct subscriber_id)    as boxes
      ,count(*)                         as views
      ,count(distinct cb_key_household) as hh
      ,sum(dur_hours) as hours
into --drop table
scaling_pivot1
from session_capping_dataset
group by adjusted_event_date
      ,start_hour
      ,genre_description
      ,pvr
      ,box_type
      ,primary_box
      ,lifestage
      ,HHAfflu;
commit;
--3955096

select adjusted_event_date
      ,start_hour
      ,genre_description
      ,pvr
      ,box_type
      ,primary_box
      ,hhpersonicx
      ,HHsize
      ,HHKids
      ,count(distinct subscriber_id)    as boxes
      ,count(*)                         as views
      ,count(distinct cb_key_household) as hh
      ,sum(dur_hours) as hours
into --drop table
scaling_pivot2
from session_capping_dataset
group by adjusted_event_date
      ,start_hour
      ,genre_description
      ,pvr
      ,box_type
      ,primary_box
      ,hhpersonicx
      ,HHsize
      ,HHKids;
commit;
--9690094

select adjusted_event_date
      ,start_hour
      ,genre_description
      ,pvr
      ,box_type
      ,primary_box
      ,HHComposition
      ,hhtechnorank
      ,count(distinct subscriber_id)    as boxes
      ,count(*)                         as views
      ,count(distinct cb_key_household) as hh
      ,sum(dur_hours) as hours
into --drop table
scaling_pivot3
from session_capping_dataset
group by adjusted_event_date
      ,start_hour
      ,genre_description
      ,pvr
      ,box_type
      ,primary_box
      ,HHComposition
      ,hhtechnorank;
commit;
--3476075

select adjusted_event_date
      ,start_hour
      ,genre_description
      ,pvr
      ,box_type
      ,primary_box
      ,case when ownrnt=1 then '1) Home Owners'
            when ownrnt=2 then '2) Private Renters'
            when ownrnt=3 then '3) Council Renters'
            when ownrnt=4 then '4) Living with Parents'
            else               '5) Unknown'
       end as OwnRnt
      ,case when hhlenres=0 then '0) Unknown'
            when hhlenres=1 then '1) Up to 2 Years'
            when hhlenres=2 then '2) 3-5 Years'
            when hhlenres=3 then '3) 6-10 Years'
            when hhlenres=4 then '4) 11+ Years'
            else                 '0) Unknown'
       end as HHLenres
      ,case when PropertyType=1 then '1) Flat/Maisonette'
            when PropertyType=2 then '2) Terraced'
            when PropertyType=3 then '3) Semi-Detached'
            when PropertyType=4 then '4) Detached'
            when PropertyType=5 then '5) Bungalow'
            else                     '6) Unknown'
       end as PropertyType
      ,NumBedrooms
      ,count(distinct subscriber_id)    as boxes
      ,count(*)                         as views
      ,count(distinct cb_key_household) as hh
      ,sum(dur_hours) as hours
into --drop table
scaling_pivot4
from session_capping_dataset
group by adjusted_event_date
      ,start_hour
      ,genre_description
      ,pvr
      ,box_type
      ,primary_box
      ,OwnRnt
      ,HHLenres
      ,PropertyType
      ,NumBedrooms;
commit;
--4183447

select adjusted_event_date
      ,start_hour
      ,genre_description
      ,pvr
      ,box_type
      ,primary_box
      ,package
      ,tenure
      ,count(distinct subscriber_id)    as boxes
      ,count(*)                         as views
      ,count(distinct cb_key_household) as hh
      ,sum(dur_hours) as hours
into --drop table
scaling_pivot5
from session_capping_dataset
group by  adjusted_event_date
      ,start_hour
      ,genre_description
      ,pvr
      ,box_type
      ,primary_box
      ,package
      ,tenure;
commit;
--7728610

select adjusted_event_date
      ,start_hour
      ,genre_description
      ,pvr
      ,box_type
      ,primary_box
      ,hhpcown
      ,broadband
      ,exchange_status
      ,count(distinct subscriber_id)    as boxes
      ,count(*)                         as views
      ,count(distinct cb_key_household) as hh
      ,sum(dur_hours) as hours
into --drop table
scaling_pivot6
from session_capping_dataset
group by  adjusted_event_date
      ,start_hour
      ,genre_description
      ,pvr
      ,box_type
      ,primary_box
      ,hhpcown
      ,broadband
      ,exchange_status;
commit;
--428693

select adjusted_event_date
      ,start_hour
      ,genre_description
      ,pvr
      ,box_type
      ,primary_box
      ,isba
      ,gov_region
      ,count(distinct subscriber_id)    as boxes
      ,count(*)                         as views
      ,count(distinct cb_key_household) as hh
      ,sum(dur_hours) as hours
into --drop table
scaling_pivot7
from session_capping_dataset
group by  adjusted_event_date
      ,start_hour
      ,genre_description
      ,pvr
      ,box_type
      ,primary_box
      ,isba
      ,gov_region;
commit;
--7728610

select case when ownrnt=1 then '1) Home Owners'
            when ownrnt=2 then '2) Private Renters'
            when ownrnt=3 then '3) Council Renters'
            when ownrnt=4 then '4) Living with Parents'
            else               '5) Unknown'
       end as OwnRnt
,lifestage
,CASE WHEN hhafflu in (1,2,3,4)  THEN 'Very Low'
            WHEN hhafflu in (5,6)      THEN 'Low'
            WHEN hhafflu in (7,8)      THEN 'Mid Low'
            WHEN hhafflu in (9,10)     THEN 'Mid'
            WHEN hhafflu in (11,12)    THEN 'Mid High'
            WHEN hhafflu in (13,14,15) THEN 'High'
            WHEN hhafflu in (16,17)    THEN 'Very High'
            ELSE                            'Unknown'
       END as HHAfflu
       ,HHComposition
,count(distinct subscriber_id)    as boxes
,count(*)                         as views
,count(distinct cb_key_household) as hh
,sum(dur_hours) as hours
into --drop table
scaling_pivot8
from session_capping_dataset
group by ownrnt,
lifestage,
hhafflu,
hhcomposition;
commit;

--select distinct epg_channel,epg_title from sk_prod.vespa_epg_dim where genre_description='Specialist';

select genre_description
,channel_name
,case when gov_region='10. Scotland' then 'Scotland'
      when gov_region in ('11. Northern Ireland','12. ROI') then 'Ireland'
      when gov_region='14. Unknown' then 'Unknown'
      else 'UK'
 end as gov_region
,count(distinct subscriber_id)    as boxes
,count(*)                         as views
,count(distinct cb_key_household) as hh
,sum(dur_hours) as hours
into --drop table
scaling_pivot9
from session_capping_dataset
group by genre_description
,channel_name
,gov_region;
commit;
--603

select * from pivot9;

select distinct gov_region from pivot9;

select adjusted_event_date
      ,live
      ,pvr
      ,box_type
      ,primary_box
      ,lifestage
      ,HHAfflu
      ,hhpersonicx
      ,HHsize
      ,HHKids
      ,HHComposition
      ,hhtechnorank
      ,OwnRnt
      ,HHLenres
      ,PropertyType
      ,NumBedrooms
      ,package
      ,tenure
      ,hhpcown
      ,broadband
      ,exchange_status
      ,gov_region
      ,isba
      ,count(distinct subscriber_id)    as boxes
      ,count(*)                         as views
      ,count(distinct cb_key_household) as hh
      ,sum(dur_hours) as hours
into scaling_pivot10
group by adjusted_event_date
      ,live
      ,lifestage
      ,HHAfflu
      ,hhpersonicx
      ,HHsize
      ,HHKids
      ,HHComposition
      ,hhtechnorank
      ,OwnRnt
      ,HHLenres
      ,PropertyType
      ,NumBedrooms
      ,package
      ,tenure
      ,hhpcown
      ,broadband
      ,exchange_status
      ,gov_region
      ,isba;
commit;

--granting permissions on data tables
grant all on session_capping_dataset to public;
grant all on scaling_pivot1 to public;
grant all on scaling_pivot2 to public;
grant all on scaling_pivot3 to public;
grant all on scaling_pivot4 to public;
grant all on scaling_pivot5 to public;
grant all on scaling_pivot6 to public;
grant all on scaling_pivot7 to public;
grant all on scaling_pivot8 to public;
grant all on scaling_pivot9 to public;
grant all on scaling_pivot10 to public;

select distinct lifestage,count(distinct cb_key_household) from session_capping_dataset group by lifestage;
select count(distinct cb_key_household) from session_capping_dataset;
