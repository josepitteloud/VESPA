--Code to create new scaling segment ids, with population and sample size values

begin

declare @scaling_date date
set     @scaling_date = '2013-07-14'
commit

--Create table with adsmartable_universe segments and new concatenated scaling segmetns

--Patrick Igonor has created the tables for the other segments
if object_id('V154_adsmartable_universe_v2') is not null drop table V154_adsmartable_universe_v2
select distinct(sky_base_universe) as sky_base_universe
        into glasera.V154_adsmartable_universe_v2
        from V154_accounts_proxy_consent
commit

--Create tables containing new variables
if object_id('V154_no_of_stbs') is not null drop table V154_no_of_stbs
create table V154_no_of_stbs (No_of_stbs varchar(12))
insert into V154_no_of_stbs values ('Single')
insert into V154_no_of_stbs values ('Multiple')
if object_id('V154_hd_subscription') is not null drop table V154_hd_subscription
create table V154_hd_subscription (hd_subscription varchar(12))
insert into V154_hd_subscription values ('Yes')
insert into V154_hd_subscription values ('No')
if object_id('V154_pvr') is not null drop table V154_pvr
create table V154_pvr (pvr varchar(12))
insert into V154_pvr values ('Yes')
insert into V154_pvr values ('No')

--Create blank table for updated segments
--Update 27/09/2013, remove boxtype anmd add no_of_stbs, hd_subscription and pvr
if object_id('V154_segment_lookup_v1_1') is not null drop table V154_segment_lookup_v1_1
create table V154_segment_lookup_v1_1 (
         updated_scaling_segment_id INT IDENTITY PRIMARY KEY
        ,sky_base_universe varchar(40)
        ,isba_tv_region varchar(25)
        ,hhcomposition  varchar(40)
        ,tenure         varchar(25)
        ,package        varchar(25)
        ,no_of_stbs     varchar(12)
        ,hd_subscription varchar(4)
        ,pvr            varchar(4)
)
commit

--Need to create the new segments in two parts.
--One part sets pvr to 'No' when the universe isn't adsmartable, the other part includes all adsmartable universes when pvr is 'Yes'
insert into V154_segment_lookup_v1_1
        (sky_base_universe
        ,isba_tv_region
        ,hhcomposition
        ,tenure
        ,package
        ,no_of_stbs
        ,hd_subscription
        ,pvr)
        select * from (select distinct(sky_base_universe) as sky_base_universe from V154_adsmartable_universe_v2 where sky_base_universe like 'Not%') as sub
        cross join (select distinct(isba_tv_region_v2) as isba_tv_region from igonorp.V154_isba_tv_region_v2) as sub1
        cross join (select distinct(hhcomposition_v2) as hhcomposition from igonorp.V154_hhcomposition_v2) as sub2
        cross join (select distinct(tenure_v2) as tenure from igonorp.V154_tenure_v2) as sub3
        cross join (select distinct(package_v2) as package from igonorp.V154_package_v2) as sub4
        cross join (select distinct(No_of_stbs) as No_of_stbs from V154_no_of_stbs) as sub5
        cross join (select distinct(hd_subscription) as hd_subscription from V154_hd_subscription) as sub6
        cross join (select 'No' as pvr) as sub7
commit
--Need to create the new segments in two parts.
--One part sets pvr to 'No' when the unvierse isn't adsmartable, the other part includes all adsmartable universes when pvr is 'Yes'
insert into V154_segment_lookup_v1_1
        (sky_base_universe
        ,isba_tv_region
        ,hhcomposition
        ,tenure
        ,package
        ,no_of_stbs
        ,hd_subscription
        ,pvr)
        select * from V154_adsmartable_universe_v2
        cross join (select distinct(isba_tv_region_v2) as isba_tv_region from igonorp.V154_isba_tv_region_v2) as sub1
        cross join (select distinct(hhcomposition_v2) as hhcomposition from igonorp.V154_hhcomposition_v2) as sub2
        cross join (select distinct(tenure_v2) as tenure from igonorp.V154_tenure_v2) as sub3
        cross join (select distinct(package_v2) as package from igonorp.V154_package_v2) as sub4
        cross join (select distinct(No_of_stbs) as No_of_stbs from V154_no_of_stbs) as sub5
        cross join (select distinct(hd_subscription) as hd_subscription from V154_hd_subscription) as sub6
        cross join (select 'Yes' as pvr) as sub7
commit

--Create table containing account_numbers with updated scaling segments
if object_id('V154_account_numbers_variables_v2') is not null drop table V154_account_numbers_variables_v2
select distinct account_number
               ,isba_tv_region
               ,hhcomposition
               ,tenure
               ,package
               ,boxtype
               ,updated_adsmart_capable as adsmart_capable
               ,updated_adsmartable as adsmartable
        into    V154_account_numbers_variables_v2
        from    V154_account_numbers_variables
commit

--Add sky_base_universe to table
alter table V154_account_numbers_variables_v2 add sky_base_universe varchar(40)
update      V154_account_numbers_variables_v2
        set sky_base_universe = case
       when adsmart_capable = 1 and adsmartable = 1 then 'Adsmartable with consent'
       when adsmart_capable = 1 and adsmartable = 0 then 'Adsmartable but no consent'
       when adsmart_capable = 0 and adsmartable = 0 then 'Not adsmartable'
       else 'Unknown' end
commit

--Need to add to table which vespa universe the account number are in
alter table V154_account_numbers_variables_v2
        add (vespa_indicator        int default 0
            ,vespa_universe         varchar(40) default 'Non-vespa')
--Obtain vespa account_numbers from panel_data
IF object_id('vespa_account_numbers') IS NOT NULL DROP TABLE vespa_account_numbers
SELECT           intr.account_number
               ,scaling_segment_id
        into    vespa_account_numbers
        from    vespa_analysts.SC2_intervals intr
  inner join (
                select       sbv.account_number
                        from vespa_analysts.panel_data alt
                  inner join Vespa_Analysts.Vespa_Single_Box_View sbv
                          on cast(alt.subscriber_id as int) = cast(sbv.subscriber_id as int)
                       where alt.panel = 12
                         and dt = @scaling_date) as sub1
          on intr.account_number = sub1.account_number
       where reporting_starts <= @scaling_date
         and reporting_ends   >= @scaling_date
     order by scaling_segment_id, intr.account_number
commit

update      V154_account_numbers_variables_v2
    set     vespa_indicator = 1
    where   account_number in (select account_number from vespa_account_numbers)
update      V154_account_numbers_variables_v2
    set     vespa_universe = 'Vespa not adsmartable'
    where   vespa_indicator = 1 and adsmartable = 0
update      V154_account_numbers_variables_v2
    set     vespa_universe = 'Vespa adsmartable'
    where   vespa_indicator = 1 and adsmartable = 1
commit

alter table V154_account_numbers_variables_v2
        add     (isba_tv_region_v2 varchar(25)
                ,hhcomposition_v2  varchar(40)
                ,tenure_v2         varchar(25)
                ,package_v2        varchar(25)
                ,boxtype_v2        varchar(40))

update      V154_account_numbers_variables_v2 a
        set a.isba_tv_region_v2 = b.isba_tv_region_v2
       from igonorp.V154_isba_tv_region_v2 b
      where a.isba_tv_region = b.isba_tv_region
update      V154_account_numbers_variables_v2 a
        set a.hhcomposition_v2 = b.hhcomposition_v2
       from igonorp.V154_hhcomposition_v2 b
      where left(a.hhcomposition, 2) = left(b.hhcomposition, 2)
update      V154_account_numbers_variables_v2 a
        set a.hhcomposition_v2 = b.hhcomposition_v2
       from igonorp.V154_hhcomposition_v2 b
      where left(a.hhcomposition, 1) = left(b.hhcomposition, 1)
        and left(a.hhcomposition, 1) = 'U'
update      V154_account_numbers_variables_v2 a
        set a.tenure_v2 = b.tenure_v2
       from igonorp.V154_tenure_v2 b
      where a.tenure = b.tenure
update      V154_account_numbers_variables_v2 a
        set a.package_v2 = b.package_v2
       from igonorp.V154_package_v2 b
      where a.package = b.package
update      V154_account_numbers_variables_v2 a
        set a.boxtype_v2 = b.boxtype_v3
       from igonorp.V154_boxtype_v3 b
      where a.boxtype = b.boxtype
commit

--Update 27/09/2013
--Include newly created segment
alter table V154_account_numbers_variables_v2
        add     (no_of_stbs      varchar(12)
                ,hd_subscription varchar(4)
                ,pvr             varchar(4))

update      V154_account_numbers_variables_v2 a
        set no_of_stbs =
        case
        when boxtype like 'A)%' then  'Single'
        when boxtype like 'B)%' then  'Single'
        when boxtype like 'C)%' then  'Single'
        when boxtype like 'D)%' then  'Single'
        when boxtype like 'E)%' then  'Multiple'
        when boxtype like 'F)%' then  'Multiple'
        when boxtype like 'G)%' then  'Multiple'
        when boxtype like 'H)%' then  'Multiple'
        when boxtype like 'I)%' then  'Multiple'
        when boxtype like 'J)%' then  'Multiple'
        when boxtype like 'K)%' then  'Multiple'
        when boxtype like 'L)%' then  'Multiple'
        when boxtype like 'M)%' then  'Multiple'
        end

update      V154_account_numbers_variables_v2 a
        set hd_subscription =
        case
        when boxtype like 'A)%' then  'No'
        when boxtype like 'B)%' then  'Yes'
        when boxtype like 'C)%' then  'No'
        when boxtype like 'D)%' then  'No'
        when boxtype like 'E)%' then  'Yes'
        when boxtype like 'F)%' then  'Yes'
        when boxtype like 'G)%' then  'Yes'
        when boxtype like 'H)%' then  'No'
        when boxtype like 'I)%' then  'No'
        when boxtype like 'J)%' then  'No'
        when boxtype like 'K)%' then  'No'
        when boxtype like 'L)%' then  'No'
        when boxtype like 'M)%' then  'No'
        end


update      V154_account_numbers_variables_v2 a
        set a.pvr = b.pvr
       from (select account_number, max(pvr) as pvr
               from igonorp.Final_Boxtype_Descr_Sub_ID_PVR_Combined
           group by account_number
                ) b
      where a.account_number = b.account_number
commit

--Infer pvr value for those accounts that have a null value of pvr
update      V154_account_numbers_variables_v2
        set pvr =
       case
       when boxtype like 'D)%' then 'No'
       when boxtype like 'M)%' then 'No'
       else 'Yes'
        end
      where pvr is null
commit

--Also ensure that adsmartable boxes have pvr set to 'Yes'
update      V154_account_numbers_variables_v2
        set pvr = 'Yes'
      where sky_base_universe like 'Adsmartable%'
        and pvr = 'No'
commit

--Add a new scaling segment id
alter table V154_account_numbers_variables_v2
        add     updated_scaling_segment_id int

update      V154_account_numbers_variables_v2 a
        set a.updated_scaling_segment_id = b.updated_scaling_segment_id
       from V154_segment_lookup_v1_1 b
      where a.sky_base_universe = b.sky_base_universe
        and a.isba_tv_region_v2 = b.isba_tv_region
        and left(a.hhcomposition_v2, 2) = left(b.hhcomposition, 2)
        and left(a.tenure_v2, 2) = left(b.tenure, 2)
        and a.package_v2 = b.package
        and a.no_of_stbs = b.no_of_stbs
        and a.hd_subscription = b.hd_subscription
        and a.pvr = b.pvr
commit

--Need to run here the code to create our weighting_universe column
--Find proportion of adsmartable people who have given consent.
if object_id('temp_account_counts') is not null drop table temp_account_counts
select  distinct
        isba_tv_region
       ,hhcomposition
       ,tenure
       ,package
       ,no_of_stbs
       ,hd_subscription
       ,pvr
into    temp_account_counts
from    V154_segment_lookup_v1_1

alter table temp_account_counts
        add (
         adsmartable_accounts            integer default 0
        ,adsmartable_no_consent_accounts integer default 0
        ,proportion_no_consent           double  default 0
)

update        temp_account_counts a
        set   adsmartable_accounts = b.counts
       from   (select         isba_tv_region_v2
                             ,hhcomposition_v2
                             ,tenure_v2
                             ,package_v2
                             ,no_of_stbs
                             ,hd_subscription
                             ,pvr
                             ,count(*) as counts
                        from  V154_account_numbers_variables_v2
                       where  sky_base_universe like 'Adsmartable%'
                    group by  isba_tv_region_v2
                             ,hhcomposition_v2
                             ,tenure_v2
                             ,package_v2
                             ,no_of_stbs
                             ,hd_subscription
                             ,pvr) b
      where a.isba_tv_region = b.isba_tv_region_v2
        and left(a.hhcomposition, 2) = left(b.hhcomposition_v2, 2)
        and left(a.tenure, 2) = left(b.tenure_v2, 2)
        and a.package = b.package_v2
        and a.no_of_stbs = b.no_of_stbs
        and a.hd_subscription = b.hd_subscription
        and a.pvr = b.pvr
commit
update        temp_account_counts a
        set   adsmartable_no_consent_accounts = b.counts
       from   (select         isba_tv_region_v2
                             ,hhcomposition_v2
                             ,tenure_v2
                             ,package_v2
                             ,no_of_stbs
                             ,hd_subscription
                             ,pvr
                             ,count(*) as counts
                        from  V154_account_numbers_variables_v2
                       where  sky_base_universe = 'Adsmartable but no consent'
                    group by  isba_tv_region_v2
                             ,hhcomposition_v2
                             ,tenure_v2
                             ,package_v2
                             ,no_of_stbs
                             ,hd_subscription
                             ,pvr) b
      where a.isba_tv_region = b.isba_tv_region_v2
        and left(a.hhcomposition, 2) = left(b.hhcomposition_v2, 2)
        and left(a.tenure, 2) = left(b.tenure_v2, 2)
        and a.package = b.package_v2
        and a.no_of_stbs = b.no_of_stbs
        and a.hd_subscription = b.hd_subscription
        and a.pvr = b.pvr
commit

update        temp_account_counts
        set   proportion_no_consent = 1.0 * adsmartable_no_consent_accounts / adsmartable_accounts
      where   adsmartable_accounts > 0
commit

--Add random number
alter table V154_account_numbers_variables_v2
        add (random_number          real
            ,weighting_universe     varchar(30))
     update V154_account_numbers_variables_v2
        set random_number =  RAND(NUMBER(*)*(DATEPART(MS,NOW())+1))
      where vespa_indicator = 1 and sky_base_universe like 'Adsmartable%'
create      hg index indx_temp_rand on V154_account_numbers_variables_v2(random_number)

update      V154_account_numbers_variables_v2
      set   vespa_universe = 'Non-vespa'
    where   vespa_indicator = 0
update      V154_account_numbers_variables_v2
      set   vespa_universe = 'Vespa not adsmartable'
    where   vespa_indicator = 1 and sky_base_universe like 'Not adsmartable'
update      V154_account_numbers_variables_v2
      set   vespa_universe = 'Vespa adsmartable'
    where   vespa_indicator = 1 and sky_base_universe like 'Adsmartable%'

update      V154_account_numbers_variables_v2
       set  vespa_universe = 'Vespa but no consent'
      from  V154_account_numbers_variables_v2  b
inner join  temp_account_counts                a
        on  a.isba_tv_region = b.isba_tv_region_v2
        and left(a.hhcomposition, 2) = left(b.hhcomposition_v2, 2)
        and left(a.tenure, 2) = left(b.tenure_v2, 2)
        and a.package = b.package_v2
        and a.no_of_stbs = b.no_of_stbs
        and a.hd_subscription = b.hd_subscription
        and a.pvr = b.pvr
     where  vespa_indicator = 1
      and   vespa_universe = 'Vespa adsmartable'
      and   random_number <= proportion_no_consent
commit

update      V154_account_numbers_variables_v2
       set  weighting_universe = sky_base_universe
update      V154_account_numbers_variables_v2
       set  weighting_universe = 'Adsmartable but no consent'
     where  vespa_universe = 'Vespa but no consent'

if object_id('V154_segments_aggregated') is not null drop table V154_segments_aggregated
select  a.*, coalesce(b.num_accounts, 0) as sky_base_accounts
        into V154_segments_aggregated
        from V154_segment_lookup_v1_1 a
   left join (select   updated_scaling_segment_id
                      ,count(distinct account_number) as num_accounts
                  from V154_account_numbers_variables_v2
              group by updated_scaling_segment_id) as b
          on a.updated_scaling_segment_id = b.updated_scaling_segment_id
commit

alter table V154_segments_aggregated
        add vespa_accounts int default 0

--Need to sum the vespa accounts separately since they need to be summed by weighting universe
--but they will be joined to a different scaling segment id
IF object_id('temp_vespa_aggregated') IS NOT NULL DROP TABLE temp_vespa_aggregated
select distinct
     isba_tv_region_v2 as isba_tv_region
    ,hhcomposition_v2 as hhcomposition
    ,tenure_v2 as tenure
    ,package_v2 as package
    ,no_of_stbs
    ,hd_subscription
    ,pvr
    ,weighting_universe as sky_base_universe
    ,sum(vespa_indicator) as vespa_accounts
    into temp_vespa_aggregated
    from V154_account_numbers_variables_v2
    group by
             weighting_universe
            ,isba_tv_region_v2
            ,hhcomposition_v2
            ,tenure_v2
            ,package_v2
            ,no_of_stbs
            ,hd_subscription
            ,pvr

    update  V154_segments_aggregated agg
        set agg.vespa_accounts = tva.vespa_accounts
       from temp_vespa_aggregated tva
      where agg.sky_base_universe = tva.sky_base_universe
        and agg.isba_tv_region = tva.isba_tv_region
         and agg.hhcomposition = tva.hhcomposition
         and agg.tenure = tva.tenure
        and agg.package = tva.package
        and agg.no_of_stbs = tva.no_of_stbs
        and agg.hd_subscription = tva.hd_subscription
        and agg.pvr = tva.pvr
commit

IF object_id('temp_vespa_aggregated') IS NOT NULL DROP TABLE temp_vespa_aggregated
commit

--Create table with new scaling variables for use with RIM weighting algorithm
if object_id('V154_Variables_lookup_v1_1') is not null drop table V154_Variables_lookup_v1_1
create table V154_Variables_lookup_v1_1 (
         updated_scaling_segment_id INT IDENTITY PRIMARY KEY
        ,scaling_variable varchar(40)
)
commit
insert into V154_Variables_lookup_v1_1(scaling_variable) values('isba_tv_region')
insert into V154_Variables_lookup_v1_1(scaling_variable) values('hhcomposition')
insert into V154_Variables_lookup_v1_1(scaling_variable) values('tenure')
insert into V154_Variables_lookup_v1_1(scaling_variable) values('package')
insert into V154_Variables_lookup_v1_1(scaling_variable) values('no_of_stbs')
insert into V154_Variables_lookup_v1_1(scaling_variable) values('hd_subscription')
insert into V154_Variables_lookup_v1_1(scaling_variable) values('pvr')

end

