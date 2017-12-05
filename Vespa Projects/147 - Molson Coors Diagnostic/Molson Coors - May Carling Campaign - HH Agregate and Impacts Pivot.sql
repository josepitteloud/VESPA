------------------------------------------------------------------------
--                                                                    --
--        Project: Molson Coors -MAY CARLING CAMPAIGN                 --
--                 HH AGGREGATE AND IMPACT TABLES                     --
--        Version: v1.0                                               --
--        Created: 23/04/2013                                         --
--        Lead:                                                       --
--        Analyst: Hannah Starmer                                     --
--        SK Prod: 5                                                  --
--                                                                    --
--                                                                    --
--        PART A. HOUSEHOLD AGGREGATE                                 --
--        PART B. IMPACTS                                             --
--        PART C. REACH                                               --
--        PART D.                                                     --
--        PART E.                                                     --
--        PART F.                                                     --
--        PART G.                                                     --
--                                                                    --
--                                                                    --
--                                                                    --
--                                                                    --
--                                                                    --
--                                                                    --
------------------------------------------------------------------------

------------
-- SET UP --
------------

CREATE VARIABLE @var_period_start       datetime;
CREATE VARIABLE @var_period_end         datetime;
CREATE VARIABLE @var_sql                varchar(4000);
CREATE VARIABLE @scanning_day           datetime;
CREATE VARIABLE @var_num_days           smallint;
CREATE VARIABLE @var_cntr               smallint;

-- THINGS YOU NEED TO CHANGE --

SET @var_period_start  = '2012-05-01';
SET @var_period_end    = '2012-06-07';
-------------------------------------------------
--         PART A: HOUSEHOLD AGGREGATE         --
-------------------------------------------------


-- IDENTIFY IF HOUSEHOLD CONTAINS A MALE, BROUGHT AGE RANGE OR ASPIRATIONAL AGE RANGE --

if object_id('hh_age_gender') is not null drop table hh_age_gender;
select    a.cb_key_household
         ,max(a.hh_contain_18_to_24) as hh_contain_18_to_24
         ,max(a.hh_contain_16_to_34) as hh_contain_16_to_34
         ,max(hh_contain_over_18) as hh_contain_over_18
         ,max(a.male_present) as hh_contain_male
into      hh_age_gender
from      (select  cb_key_household
                  ,cb_key_db_individual
                  ,case when p_actual_age between 18 and 24 then 1 else 0 end as hh_contain_18_to_24
                  ,case when p_actual_age between 16 and 34 then 1 else 0 end as hh_contain_16_to_34
                  ,case when p_actual_age>18 then 1 else 0 end as hh_contain_over_18
                  ,case when p_gender in ('0') then 1 else 0 end as male_present
           from sk_prod.experian_consumerview) A
group by cb_key_household;



-- PICK UP MOSAIC TYPE  --
if object_id('hh_mosaic') is not null drop table hh_mosaic;
select      cb_key_household
           ,max(h_mosaic_uk_type) as h_mosaic_uk_type
into       hh_mosaic
from       sk_prod.experian_consumerview
group by   cb_key_household;




-- USE MOSAIC GROUPS TO CREATE BUCKLE SEGMENTS --
-- CREATE BROUGHT AUDIENCE AND ASPIRATIONAL AUDIENCE FLAGS --

if object_id('hh_experian') is not null drop table hh_experian;
select     b.cb_key_household
          ,b.h_mosaic_uk_type
          ,c.hh_contain_18_to_24
          ,c.hh_contain_16_to_34
          ,c.hh_contain_over_18
          ,c.hh_contain_male
          ,case when c.hh_contain_male=1 and c.hh_contain_16_to_34=1 then 1 else 0 end as brought_audience
          ,case when c.hh_contain_male=1 and c.hh_contain_18_to_24=1 then 1 else 0 end as aspirational_audience
          ,case when b.h_mosaic_uk_type in ('16','34','35','37','45','46','47','48',
                                            '49','57','58','59','60','62','64','65','66','67') then 1 else 0 end as Social_Explorers
          ,case when b.h_mosaic_uk_type in ('16','34','35','37','45','46','47','48',
                                            '49','57','58','59','60','62','64','65','66','67') then 'Social Explorers'
                when b.h_mosaic_uk_type in ('1','2','5','7','9','27','29','30',
                                            '31','32','33','36','55','61','63') then 'Enthusiastic Influencers'
                when b.h_mosaic_uk_type in ('13','17','18','21','23',
                                            '24','25','26','54') then 'Content Routiners'
                when b.h_mosaic_uk_type in ('14','15','28','38','39','40','41',
                                            '42','50','51','52','53','56') then 'Safe & Savvy'
                when b.h_mosaic_uk_type in ('3','4','6','8','10',
                                            '11','12','19','20','22') then 'Considered Balancers'
                when b.h_mosaic_uk_type in ('43','44','99') then 'Do Not Target'
                else 'Unknown'
           end as buckle_segments
into      hh_experian
from      hh_mosaic  B
left outer join
          hh_age_gender                C
on b.cb_key_household=c.cb_key_household;

--select count(*),count(distinct cb_key_household) from hh_experian;
------------------------------
-- IDENTIFY ACTIVE SKY BASE --
------------------------------

if object_id('sky_active_customers') is not null drop table sky_active_customers;

SELECT   account_number
        ,cb_key_household
        ,current_short_description
        ,rank() over (PARTITION BY account_number ORDER BY effective_from_dt desc, cb_row_id) AS rank
        ,convert(bit, 0)  AS uk_standard_account
INTO sky_active_customers
FROM sk_prod.cust_subs_hist
WHERE subscription_sub_type IN ('DTV Primary Viewing')
        AND status_code IN ('AC','AB','PC')
        AND effective_from_dt    <= @var_period_start
        AND effective_to_dt      > @var_period_start
        AND effective_from_dt    <> effective_to_dt
        AND EFFECTIVE_FROM_DT    IS NOT NULL
        AND cb_key_household     > 0
        AND cb_key_household     IS NOT NULL
        AND account_number       IS NOT NULL
        AND service_instance_id  IS NOT NULL;

-- REMOVE DUPLICATES --
delete from sky_active_customers where rank > 1;

-- TAKE ONLY UK CUSTOMERS --

UPDATE sky_active_customers
  SET
  uk_standard_account = CASE WHEN b.acct_type='Standard' AND b.account_number <>'?' AND b.pty_country_code ='GBR'
                             THEN 1
                             ELSE 0 END
FROM sky_active_customers AS a
inner join sk_prod.cust_single_account_view AS b
ON a.account_number = b.account_number;


DELETE FROM sky_active_customers WHERE uk_standard_account = 0;


-- SUMMARISE TABLE UP TO HOUSEHOLD LEVEL --


if object_id('sky_active_households') is not null drop table sky_active_households;
select  cb_key_household
       ,sum(uk_standard_account) as ac
into sky_active_households
from sky_active_customers
group by cb_key_household;

-- CREATE HOUSEHOLD AGGREGATE WHICH INCLUDES SKY BASE --

if object_id('carling_hh_aggregate') is not null drop table carling_hh_aggregate;
select     COALESCE(a.cb_key_household,b.cb_key_household) as cb_key_household
          ,case when a.ac is not null then 1 else 0 end as sky_flag
          ,b.h_mosaic_uk_type
          ,b.hh_contain_18_to_24
          ,b.hh_contain_16_to_34
          ,b.hh_contain_over_18
          ,b.hh_contain_male
          ,b.brought_audience
          ,b.aspirational_audience
          ,b.buckle_segments
into   carling_hh_aggregate
from   sky_active_households         A
left outer join
       hh_experian     B
on a.cb_key_household=b.cb_key_household;

--select count(*),count(distinct cb_key_household) from carling_hh_aggregate;
--SELECT TOP 10 * FROM carling_hh_aggregate;


--------------------------------------------------------
--         PART B: CREATE CLIENT SPOTS FILE           --
--------------------------------------------------------

-- PULL DATA TOGETHER TO CREATE AN IMPACTS TABLE --

drop table carling_viewing_data;
select      a.*
           ,c.barb_date_of_transmission
           ,left(c.barb_spot_start_time,2) || ':' || substr(c.barb_spot_start_time,3,2) || ':' || right(c.barb_spot_start_time,2) as barb_spot_start_time
           ,c.barb_spot_start_time as barb_spot_start_unformated
           ,case when cast(c.barb_spot_start_time as integer)
                between 60000 and 085959 then 'Breakfast Time'
                when cast(c.barb_spot_start_time as integer)
                between 090000 and 172959 then 'Daytime'
                when cast(c.barb_spot_start_time as integer)
                between 173000 and 195959 then 'Early Peak'
                when cast(c.barb_spot_start_time as integer)
                between 200000 and 225999 then 'Late Peak'
                when cast(c.barb_spot_start_time as integer)
                between 230000 and 235959 then 'Post Peak'
                when cast(c.barb_spot_start_time as integer)
                between 240000 and 242959 then 'Post Peak'
                when cast(c.barb_spot_start_time as integer)
                between 243000 and 295959 then 'Night Time'
            else 'Unknown'
            end as daypart
           ,c.clearcast_commercial_no
           ,case when c.clearcast_commercial_no in ('VCCMCCA014030','VCCMCCA015010','VCCMCCA018010') then 'Carling'
                 when c.clearcast_commercial_no in ('AAEFOST024040','AAEFOST028030','CHFFOCG001065') then 'Fosters lager'
                 when c.clearcast_commercial_no in ('FOLCARL015030','FOLCARL016030','FOLCBFA001090','FOLCBFA002060',
                                                    'FOLCBFA003030','FOLCBFA004030','ICPCBFB219030','POACARL063015',
                                                    'POACARL065015','POACARL067015') then 'Carlsberg lager'
                 else 'Unknown'
            end as client_spot_flag
           ,case when c.ratecard_weighting is null then cast(c.spot_duration as decimal (7,3))/cast(30 as decimal (7,3))
                 else c.ratecard_weighting end as ratecard_weighting
           ,(cast((scaling_weighting*ratecard_weighting) as float)) as adjusted_scaling
           ,b.genre_description
           ,b.sub_genre_description
           ,b.programme_name
into       carling_viewing_data
from       spot_customer_viewing_capped_thin    A
left join
           spot_data                            C
on         a.pk_spot_id=c.pk_spot_id
left join
           spot_data_prog_instance_ph1          B
on         a.pk_spot_id=b.pk_spot_id
and        a.dk_programme_instance_dim=b.dk_programme_instance_dim
where      a.spot_viewed_duration=c.spot_duration;




-- QA THAT WE ARE GETTING SAME TOTAL IMPACTS AS AT END OF VIEWING CODE --

select    barb_date_of_transmission
         ,clearcast_commercial_no
         ,sum(scaling_weighting)
         ,sum(Cast((ratecard_weighting*scaling_weighting) as float))
         ,sum(adjusted_scaling)
from      carling_viewing_data
group by  barb_date_of_transmission
         ,clearcast_commercial_no;


------------------------------------
-- APPEND A FEW ADDITIONAL FIELDS --
------------------------------------


alter table carling_viewing_data
 add      (cb_key_household            bigint
          ,channel_new                 varchar(50)
          ,media_pack                  varchar(25)
          ,sales_house                 varchar(25)
          ,hh_contain_18_to_24         integer
          ,hh_contain_16_to_34         integer
          ,hh_contain_over_18          integer
          ,hh_contain_male             integer
          ,brought_audience            integer
          ,aspirational_audience       integer
          ,buckle_segments             varchar(25)
          ,brought_audience_hh         float
          ,aspirational_audience_hh    float
          ,social_explorers_hh         float
          ,enthusiastic_influencers_hh float
          ,content_routiners_hh        float
          ,safe_and_savy_hh            float
          ,considered_balancers_hh     float
          ,do_not_targets_hh           float);


-- CB_KEY_HOUSEHOLD FIELD --

drop table acct_hh;
select  distinct account_number
       ,cb_key_household
into    acct_hh
from    sk_prod.cust_single_account_view
where   account_number in (select distinct account_number from carling_viewing_data);

update      carling_viewing_data
set         a.cb_key_household=b.cb_key_household
from        carling_viewing_data     A
left join
            acct_hh                  B
on          a.account_number=b.account_number;


-- CHANNEL NAME --

-- CREATE CHANNEL LOOKUP --
/*
select
      distinct lsp.te_channel,
      sd.*
into #spot_additions
FROM      spot_data sd
LEFT JOIN vespa_analysts.channel_map_dev_log_station_panel lsp
ON        sd.log_station_code = lsp.log_station_code
and       sd.sti_code = lsp.sti_code;

select service_key,te_channel,count(*)
into #service
from #spot_additions
group by service_key,te_channel;


--drop table te_channel_lkup;
select a.service_key
      ,a.te_channel
      ,case when a.service_key in (2402) then 'Animal Planet'
            when a.service_key in (1873) then 'Bio'
            when a.service_key in (3619) then 'Bio +1'
            when a.service_key in (1621,1622,1624) then 'CH4'
            when a.service_key in (1671,1673) then 'CH4+1'
            when a.service_key in (1800,1801,1828,1829) then 'Channel 5'
            when a.service_key in (1448) then 'CI Network '
            when a.service_key in (2510) then 'Comedy Central'
            when a.service_key in (2306) then 'Dave'
            when a.service_key in (2401) then 'Discovery'
            when a.service_key in (1627) then 'Film4'
            when a.service_key in (1628) then 'E4'
            when a.service_key in (2302) then 'Eden'
            when a.service_key in (3141) then 'ESPN'
            when a.service_key in (1305) then 'FOX'
            when a.service_key in (2308) then 'Good Food'
            when a.service_key in (1875) then 'History'
            when a.service_key in (6240) then 'ITV2'
            when a.service_key in (6015,6130,6140,6141,6142,6143,6160,6180,6089,6011,6390,6210,6220) then 'ITV1'
            when a.service_key in (6125,6128) then 'ITV1 +1'
            when a.service_key in (6260,6260,6533) then 'ITV3'
            when a.service_key in (6261)  then 'ITV3+1'
            when a.service_key in (6272) then 'ITV4'
            when a.service_key in (3340) then 'More4 '
            when a.service_key in (1806) then 'National Geographic'
            when a.service_key in (1847) then 'National Geographic Wild'
            when a.service_key in (1402) then 'Sky 1'
            when a.service_key in (1752) then 'Sky Arts 1'
            when a.service_key in (1753) then 'Sky Arts 2'
            when a.service_key in (1412) then 'Sky Atlantic'
            when a.service_key in (2201) then 'Sky Living'
            when a.service_key in (1404) then 'Sky News'
            when a.service_key in (1301,4002) then 'Sky Sports 1'
            when a.service_key in (1302,4081) then 'Sky Sports 2'
            when a.service_key in (1333,4022) then 'Sky Sports 3'
            when a.service_key in (1322,4026) then 'Sky Sports 4'
            when a.service_key in (1471) then 'Sky Sports Active 1'
            when a.service_key in (1472) then 'Sky Sports Active 2'
            when a.service_key in (1473) then 'Sky Sports Active 3'
            when a.service_key in (1474) then 'Sky Sports Active 4'
            when a.service_key in (1475) then 'Sky Sports Active 5'
            when a.service_key in (1306,3835) then 'Sky Sports F1'
            when a.service_key in (1314,4049) then 'Sky Sports News'
            when a.service_key in (1842) then 'Universal'
            when a.service_key in (2617) then 'Watch'
            when a.service_key in (4077) then 'Cartoon Network'
            when a.service_key in (1843) then 'Disney XD'
            when a.service_key in (2501) then 'MTV'
            when a.service_key in (3508,4006) then 'MTV Live'
            when a.service_key in (1846) then 'Nickelodeon uk'
            when a.service_key in (3630) then 'Sahara One'
            when a.service_key in (1814) then 'Sky Movies Showcase'
            when a.service_key in (1001) then 'Sky Movies Action & Adventure'
            when a.service_key in (1812) then 'Sky Movies Classics'
            when a.service_key in (1002) then 'Sky Movies Comedy'
            when a.service_key in (1818) then 'Sky Movies Crime & Thriller'
            when a.service_key in (1816) then 'Sky Movies Drama & Romance'
            when a.service_key in (1808) then 'Sky Movies Family'
            when a.service_key in (1811) then 'Sky Movies Indie'
            when a.service_key in (1815,4015) then 'Sky Movies Modern Greats'
            when a.service_key in (1409) then 'Sky Movies Premiere'
            when a.service_key in (1807) then 'Sky Movies SciFi/Horror'
            when a.service_key in (1771) then 'Star Plus'
            when a.service_key in (2505) then 'Syfy'
            when a.service_key in (1371)  then 'Cartoonito'
            when a.service_key in (1372)  then 'TCM 2'
            when a.service_key in (1771)  then 'STAR Plus'
            when a.service_key in (1808,4018)  then 'Sky Family'
            when a.service_key in (1842)  then 'Universal'
            when a.service_key in (1843,4070)  then 'Disney XD'
            when a.service_key in (1844)  then 'Disney XD+1'
            when a.service_key in (1845)  then 'Nick Replay'
            when a.service_key in (1846,4069)  then 'Nickelodeon'
            when a.service_key in (1849)  then 'Nicktoons'
            when a.service_key in (1857)  then 'Nick Jr.'
            when a.service_key in (1872)  then 'Community Channel'
            when a.service_key in (2606)  then 'Zing'
            when a.service_key in (2619)  then 'HiTV'
            when a.service_key in (3001)  then 'Horse & Country'
            when a.service_key in (3104)  then 'UMP Stars'
            when a.service_key in (3108)  then 'Vox Africa'
            when a.service_key in (3207)  then 'Universal+1'
            when a.service_key in (3220)  then 'Men & Movies  +1'
            when a.service_key in (3251)  then 'UMP Movies'
            when a.service_key in (3408)  then 'Sunrise TV'
            when a.service_key in (3531)  then 'Vintage TV'
            when a.service_key in (3541)  then 'Men & Movies'
            when a.service_key in (3608)  then 'Star Life OK'
            when a.service_key in (3613)  then 'STAR Gold'
            when a.service_key in (3630)  then 'Sahara One'
            when a.service_key in (3631)  then 'SONY SAB'
            when a.service_key in (3643)  then 'True Movies 1'
            when a.service_key in (3708)  then 'movies4men'
            when a.service_key in (3721)  then 'Movies4Men +1'
            when a.service_key in (3731)  then 'My Channel'
            when a.service_key in (3732)  then 'LiverpoolFCTV'
            when a.service_key in (3735)  then 'Motors TV'
            when a.service_key in (3750)  then 'POP'
            when a.service_key in (3751)  then 'True Movies 2'
            when a.service_key in (3780)  then 'Tiny Pop'
            when a.service_key in (3806)  then 'ARY World'
            when a.service_key in (5601,4077)  then 'Cartoon Network'
            when a.service_key in (3028) then '5* +1'
            when a.service_key in (2413) then 'Investigation'
            else te_channel
       end as channel
into te_channel_lkup
from #service  a;
*/

update            carling_viewing_data
set               cub.channel_new= tmp.channel
from              carling_viewing_data as cub
left outer join   te_channel_lkup as tmp
on                tmp.service_key = cub.service_key;

-- MEDIA PACK AND SALES HOUSE --
/*
select   ska.service_key as service_key
        ,ska.full_name
        ,PACK.NAME
        ,cgroup.primary_sales_house
        ,(case when pack.name is null then cgroup.channel_group
                else pack.name end) as channel_category
into #packs
from vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_ATTRIBUTES ska
left join
        (select a.service_key
               ,b.name
         from   vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_LANDMARK a
         join   neighbom.CHANNEL_MAP_DEV_LANDMARK_CHANNEL_PACK_LOOKUP b
         on     a.sare_no between b.sare_no and b.sare_no + 999
         where  a.service_key <> 0) pack
     on ska.service_key = pack.service_key
left join
        (select distinct a.service_key
               ,b.primary_sales_house
               ,b.channel_group
         from   vespa_analysts.CHANNEL_MAP_DEV_SERVICE_KEY_BARB a
         join   neighbom.CHANNEL_MAP_DEV_BARB_CHANNEL_GROUP b
         on a.log_station_code = b.log_station_code
         and a.sti_code = b.sti_code
         where service_key <>0) cgroup
    on   ska.service_key = cgroup.service_key
where    cgroup.primary_sales_house is not null
order by cgroup.primary_sales_house
        ,channel_category;

--if object_id('LkUpPack') is not null drop table LkUpPack;
SELECT    primary_sales_house
         ,service_key
         ,full_name
         ,(case
                when service_key = 3777 OR service_key = 6756 then 'LIFESTYLE & CULTURE'
                when service_key = 4040 then 'SPORTS'
                when service_key = 1845 OR service_key = 4069 OR service_key = 1859 then 'KIDS'
                when service_key = 4006 then 'MUSIC'
                when service_key = 3621 OR service_key = 4080 then 'ENTERTAINMENT'
                when service_key = 3760 then 'DOCUMENTARIES'
                when service_key = 1757 then 'MISCELLANEOUS'
                when service_key = 3639 OR service_key = 4057 then 'Media Partners'
          else channel_category END) AS channel_category
INTO      LkUpPack
FROM     #packs
order by  primary_sales_house
         ,channel_category;
*/

update            carling_viewing_data
set               cub.media_pack = tmp.channel_category
                  ,cub.sales_house = tmp.primary_sales_house
from              carling_viewing_data as cub
left outer join   LkUpPack as tmp
on                tmp.service_key = cub.service_key;

-- HOUSEHOLD FLAGS --


update        carling_viewing_data
set           a.hh_contain_18_to_24 = b.hh_contain_18_to_24
             ,a.hh_contain_16_to_34 = b.hh_contain_16_to_34
             ,a.hh_contain_over_18 = b.hh_contain_over_18
             ,a.hh_contain_male = b.hh_contain_male
             ,a.brought_audience = b.brought_audience
             ,a.aspirational_audience = b.aspirational_audience
             ,a.buckle_segments = b.buckle_segments
from          carling_viewing_data      A
left join
              carling_hh_aggregate      B
on            a.cb_key_household=b.cb_key_household;



update         carling_viewing_data
set            a.brought_audience_hh = (case when a.brought_audience=1 then a.adjusted_scaling else 0 end)
              ,a.aspirational_audience_hh  = (case when a.aspirational_audience=1 then a.adjusted_scaling else 0 end)
              ,a.social_explorers_hh  = (case when a.buckle_segments in ('Social Explorers') then a.adjusted_scaling else 0 end)
              ,a.enthusiastic_influencers_hh  = (case when a.buckle_segments in ('Enthusiastic Influencers') then a.adjusted_scaling else 0 end)
              ,a.content_routiners_hh  = (case when a.buckle_segments in ('Content Routiners') then a.adjusted_scaling else 0 end)
              ,a.safe_and_savy_hh  = (case when a.buckle_segments in ('Safe & Savvy') then a.adjusted_scaling else 0 end)
              ,a.considered_balancers_hh  = (case when a.buckle_segments in ('Considered Balancers') then a.adjusted_scaling else 0 end)
              ,a.do_not_targets_hh  = (case when a.buckle_segments in ('Do Not Target') then a.adjusted_scaling else 0 end)
from           carling_viewing_data        A;

-- QA --
/*
select   barb_date_of_transmission,clearcast_commercial_no,sum(scaling_weighting)
from     carling_viewing_data
group by barb_date_of_transmission,clearcast_commercial_no;
*/

-- IMPACT PIVOT TABLE --


if object_id('carling_impacts') is not null drop table carling_impacts;

select     barb_date_of_transmission
          ,barb_spot_start_time
          ,daypart
          ,client_spot_flag
          ,clearcast_commercial_no
          ,sales_house
          ,media_pack
          ,channel_new
          ,genre_description
          ,sub_genre_description
          ,programme_name
          ,min(spot_viewed_duration ) as spot_duration
          ,sum(cast(adjusted_scaling as float)) as total_impacts
          ,sum(brought_audience_hh ) as brought_audience_impacts
          ,sum(aspirational_audience_hh ) as aspirational_audience_impacts
          ,sum(social_explorers_hh ) as social_explorers_impacts
          ,sum(enthusiastic_influencers_hh) as enthusiastic_influencers_impacts
          ,sum(content_routiners_hh) as content_routiners_impacts
          ,sum(safe_and_savy_hh) as safe_and_savy_impacts
          ,sum(considered_balancers_hh) as considered_balancers_impacts
          ,sum(do_not_targets_hh ) as do_not_targets_impacts
into       carling_impacts
from       carling_viewing_data
group by   barb_date_of_transmission
          ,barb_spot_start_time
          ,daypart
          ,clearcast_commercial_no
          ,client_spot_flag
          ,sales_house
          ,media_pack
          ,channel_new
          ,genre_description
          ,sub_genre_description
          ,programme_name;

select * from carling_impacts;

/*
-- QA --
select    barb_date_of_transmission
         ,clearcast_commercial_no
         ,sum(total_impacts)
from      carling_impacts
group by  barb_date_of_transmission
         ,clearcast_commercial_no;
*/




-- APPEND HH AGGREGATE TABLE WITH EXPOSURE TO COMMERCIALS FLAGS --

drop table carling_hh_update;
select     cb_key_household
          ,sum(case when client_spot_flag in ('Carling') then 1 else null end) as carling_exposed_hh
          ,sum(case when client_spot_flag in ('Fosters lager') then 1 else null end) as fosters_exposed_hh
          ,sum(case when client_spot_flag in ('Carlsberg lager') then 1 else null end) as Carlsberg_exposed_hh
into       carling_hh_update
from       carling_viewing_data
group by   cb_key_household;


-- UPDATE HH AGGREGATE TABLES --


alter table carling_hh_aggregate
      add ( carling_exposed integer
           ,fosters_exposed integer
           ,Carlsberg_exposed integer);


update carling_hh_aggregate
 set   a.carling_exposed=(case when b.carling_exposed_hh >0 then 1 else 0 end)
      ,a.fosters_exposed=(case when b.fosters_exposed_hh >0 then 1 else 0 end)
      ,a.Carlsberg_exposed=(case when b.Carlsberg_exposed_hh >0 then 1 else 0 end)
 from  carling_hh_aggregate    A
 left outer join
       carling_hh_update       B
 on    a.cb_key_household=b.cb_key_household;


--------------------------------------------------------
-- REACH CALCULATIONS  --
--------------------------------------------------------

-- TAKE MIDDAY WEIGHT --

-- MIDDAY WEIGHT 13TH AUGUST --

drop table carling_midday_scaling;

select    a.account_number
         ,cast('2012-05-18' as date) as scaling_date
         ,a.reporting_starts
         ,a.reporting_ends
         ,a.scaling_segment_id
into     carling_midday_scaling
from     vespa_analysts.SC2_intervals   A
where    scaling_date between a.reporting_starts and a.reporting_ends;

ALTER TABLE carling_midday_scaling
ADD (weightings            float);

update    carling_midday_scaling
set       weightings = s.weighting
from      carling_midday_scaling as b
inner join
           vespa_analysts.SC2_weightings as s
on        b.scaling_date = s.scaling_day
and       b.scaling_segment_ID = s.scaling_segment_ID;

-- GET LOOKUP BETWEEN ACCOUNT TO CB_KEY_HOUSEHOLD --

drop table acct_hh;
select account_number,cb_key_household
into   acct_hh
from   sk_prod.cust_single_account_view
where  account_number in (select distinct account_number from carling_midday_scaling);

-- JOIN AND SUMMARISE TO HOUSEHOLD LEVEL --

alter table carling_midday_scaling
      add ( cb_key_household  bigint);

update carling_midday_scaling
 set   a.cb_key_household=b.cb_key_household
 from  carling_midday_scaling           A
 left outer join
       acct_hh                               B
 on    a.account_number=b.account_number;

-- TO HOUSEHOLD LEVEL --

drop table carling_midday_scaling_hh;
select    a.cb_key_household
         ,sum(a.weightings) as midday_weightings
into      carling_midday_scaling_hh
from      carling_midday_scaling    A
group by  a.cb_key_household;

-- UPDATE HH AGGREGATE TABLE --

alter table carling_hh_aggregate
      add ( midday_weightings  integer);


update carling_hh_aggregate A
 set   a.midday_weightings = b.midday_weightings
 from  carling_hh_aggregate    A
 left outer join
       carling_midday_scaling_hh       B
 on    a.cb_key_household=b.cb_key_household;


-- UPDATE CARLING VIEWING TABLE   --

alter table carling_viewing_data
      add ( midday_weightings  integer);


update carling_viewing_data
 set   a.midday_weightings=b.midday_weightings
 from  carling_viewing_data    A
 left outer join
       carling_midday_scaling_hh       B
 on    a.cb_key_household=b.cb_key_household;




