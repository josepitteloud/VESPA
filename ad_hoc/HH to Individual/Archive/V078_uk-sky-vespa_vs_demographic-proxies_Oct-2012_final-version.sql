/*
                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$     ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$      ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$=      ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$       ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$        ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=        ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$         ODD   ZDDDDDDDN
                                      $$$           .      $DDZ
                                     $$$                  ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES


        VESPA V078 - Household to Individual
        --------------------------------
        Author  : Alan Barber
        Date    : October 2012



SECTIONS
----------------

code_location_A01       Build Household(HH) Demographics for the UK
code_location_A02       Summarise the maximum number of different genders, age groupings per HH
code_location_A03       Count the number of people in each HH by gender / age_band grouping
code_location_A04       Create the UK Summary table
code_location_A05       Insert cb_key_households from A02 into the table (as one row per household)
----------------------------------------------------------
code_location_B01       Build information about the Skybase households and accounts
code_location_B02       Identify multiple account households
code_location_B03       Create a summary of the Skybase accounts
code_location_B04       Update missing households from the UK Summary from the
                        Skybase accounts, and higlight multiple a/c households
code_location_B05       Update Skybase accounts into the UK Summary
----------------------------------------------------------
code_location_C01       Update UK Demographics into the UK Summary
----------------------------------------------------------
code_location_D01       Flag households that can act as a Single_Proxy (where all
                        residents are iof the same gender and in the the same age_band)
code_location_D02       Flag households that are single GENDER, without kids
code_location_D03       Flag households that are single AGE, without kids
code_location_D04       Flag households that are single GENDER, with kids
code_location_D05       Flag households that are single AGE, with kids
code_location_D06       Flag households that are single AGE, with kids
code_location_D07       Flag households that are single AGE, with or without kids
----------------------------------------------------------
code_location_E01       Add scaling information to the UK Summary table
----------------------------------------------------------
code_location_F01       Grant permissions to the UK Summary table
----------------------------------------------------------
code_location_G01       Now we need to extract program data for viewing events
                        between the 14th & 20th August, 3mins continuous viewing or longer
code_location_G02       Prepare Capped data for the viewing event data.
code_location_G03       Add service key to the capped viewing data.
code_location_G04       De-dupe viewing event data
code_location_G05       Add scaling weights

code_location_H01       Create table to hold the single proxy viewing data
code_location_H02       Create table to hold the ADULT proxy viewing data
code_location_H03       Create table to hold the GENDER proxy viewing data
----------------------------------------------------------
code_location_I01       Overview Counts
code_location_I02       INDIVIDUAL/HOUSEHOLD LEVEL COUNTS
                        for example,  HOW MANY MALES in AGE_BAND x
----------------------------------------------------------
*/




CREATE VARIABLE @skybase_target_date date;--used to denote the day the Skybase is built from
SET @skybase_target_date = '2012-08-14'




/* code_location_A01 *************************************************************************
 *****                                                              UU    UU   KK   KK      **
 *****     R U N       T H E        D E M O G R A P H I C          UU    UU   KK  KK        **
 *****                                                            UU    UU   KKKK           **
 *****           C O D E     F O R     the                        UU   UU    KK  KK         **
 *****                                                             UUUU     KK    KK        **
 *********************************************************************************************/


IF object_id('V078_UK_HH_demographics') IS NOT NULL
    DROP TABLE V078_UK_HH_demographics;

--So use this to extract demographic data counts from the Experian data

select distinct -- only need one entry per individual rather than for each viewing event
       null account_number,
       e.cb_key_household,
       e.cb_row_id,
       case e.p_age_coarse when 'Unknown' then null
                         else convert(tinyint, e.p_age_coarse)               -- Age: 18-25, 26-35, 36-45, 46-55, 56-65, 66+
        end age_id,
       case e.p_age_coarse when '0' then '18-25'  -- needs changing to [p_age_coarse]
                         when '1' then '26-35'
                         when '2' then '36-45'
                         when '3' then '46-55'
                         when '4' then '56-65'
                         when '5' then '66+'
                         else 'Unknown'                -- Age: 18-25, 26-35, 36-45, 46-55, 56-65, 66+
        end age_band,
       case e.p_gender when '0' then 0                  -- this is correct for the new Exp version
                       when '1' then 1
                       else null
        end gender_id,                                                  -- male/female/unknown
       case e.p_gender when '0' then 'Male'
                       when '1' then 'Female'
                       else 'Unknown'
        end gender_desc,

        -- might need to update this to h_family_lifestage_2011
        -- or use the new field 'h_households_with_children_2011'
        case when h_family_lifestage in ('02','03','06','07','10') then 1 else 0 end has_child  
 into V078_UK_HH_demographics
  from sk_prod.experian_consumerview e
 where e.p_age_coarse in ('0','1','2','3','4','5','U')          -- Age: 18-25, 26-35, 36-45, 46-55, 56-65, 66+
   and e.p_gender in ('0','1','U')                              -- male/female/unknown
   and e.cb_address_status = '1'      --and paf valid
   and e.cb_address_dps is not null ;  --and paf valid;

--49,410,460 Row(s) affected

commit;

-- ok so now we have the household attributes for the UK
-- and we need to gather some tables together so we can work out which ones can be proxies





/* code_location_A02 ****************************************************************************************
 *****                                                                                                  *****
 *****      Use dense_rank to summarise the maximum number of different genders, age groupings in a     *****
 *****      household the number of people, and if the household has children                           *****
 *****                                                                                                  *****
 ************************************************************************************************************/

IF object_id('barbera.V078_UK_HH_demographics_max_hh_ranks_tmp') IS NOT NULL
    DROP TABLE barbera.V078_UK_HH_demographics_max_hh_ranks_tmp;


--create table that will tell us what the max bandings per household are for age, gender, person
 select  cb.cb_key_household,
         max(age_rank) max_hh_age_rank,
         max(gender_rank) max_hh_gender_rank,
         max(person_rank) max_hh_person_rank,
         max(has_child) hh_has_child
   into  V078_UK_HH_demographics_max_hh_ranks_tmp
   from (
         select d.cb_key_household,
                d.gender_desc,
                dense_rank() over(partition by account_number, cb_key_household  order by age_id) age_rank,
                dense_rank() over(partition by account_number, cb_key_household  order by gender_id) gender_rank,
                dense_rank() over(partition by account_number, cb_key_household  order by cb_row_id) person_rank,
                has_child
           from V078_UK_HH_demographics d
       ) cb
 group by cb.cb_key_household
 order by cb.cb_key_household;

--24,848,967 Row(s) affected
commit;


--add indexes
create unique index v078_uk_hh_maxhh_cbkeyhh_idx on V078_uk_HH_demographics_max_hh_ranks_tmp (cb_key_household asc);
commit;



/* code_location_A03 ****************************************************************************************
 *****                                                                                                  *****
 *****     count the number of people in a household that fall into each gender / age_band grouping     *****
 *****                                                                                                  *****
 ************************************************************************************************************/

--create table that will tell us individual counts per household  by gender and age_band

IF object_id('V078_UK_HH_gender_age_hh_counts_tmp') IS NOT NULL
    DROP TABLE V078_UK_HH_gender_age_hh_counts_tmp;

 select cb_key_household,
        gender_desc,
        age_band,
        count(1) person_count
   into V078_UK_HH_gender_age_hh_counts_tmp
   from V078_UK_HH_demographics d
 group by cb_key_household, gender_desc, age_band
 order by cb_key_household, gender_desc, age_band;
--45,529,884 Row(s) affected
commit;


--add indexes
create index v078_uk_hh_genderage_cbkeyhh_idx on V078_uk_HH_gender_age_hh_counts_tmp (cb_key_household asc);
commit;




 /* code_location_A04 *********************************************************************
 *****                                                                                *****
 *****       MAKE THE UK SUMMARY TABLE                                                *****
 *****                                                                                *****
 ******************************************************************************************/

--make the summary table
IF object_id('V078_UK_hh_summary') IS NOT NULL
    DROP TABLE V078_UK_hh_summary
        CREATE TABLE barbera.V078_UK_hh_summary (
                cb_key_household                        bigint not null,
                account_number                          varchar(20) null, -- filled in if the household has [1] Sky account
                gender_desc                             varchar(8) null,
                age_band                                varchar(8) null,
                person_count                            integer default 0,
                single_proxy                            integer default 0,
                single_gender_with_kids                 integer default 0,
                single_gender_without_kids              integer default 0,
                single_gender_with_or_without_kids      integer default 0,
                single_age_with_kids                    integer default 0,
                single_age_without_kids                 integer default 0,
                single_age_with_or_without_kids         integer default 0,
                hh_has_child                            integer default 0,
                cb_key_household_exp_unknown            integer default 0, --indicates a HH that is unknown by Experian
                multi_account_HH                        integer default 0, -- 1 if the household has more than 1 Sky account
                multi_count                             integer default 0,
                uk                                      integer default 0,
                skybase                                 integer default 0,
                vespa                                   integer default 0,
                vespa14                                 integer default 0, --not required as should be same as vespa (now built on the same day)
                scaling_segment_id                      integer,
                weight                                  double
        );
commit;



/* code_location_A05 **********************************************************************
 *****                                                                                *****
 *****       Insert cb_key_households into the UK SUMMARY TABLE                       *****
 *****                                                                                *****
 ******************************************************************************************/

--insert all the UK demographics - cb_keys...
insert into V078_UK_hh_summary (cb_key_household)
  select cb_key_household
    from V078_UK_HH_demographics_max_hh_ranks_tmp mr;
--24,848,967 Row(s) affected
commit;




/* code_location_B01 **********************************************************************
 **********               B U I L D    the    S K Y B A S E                    ************
 ******************************************************************************************/

IF object_id('V078_skybase_tbl') IS NOT NULL
    DROP TABLE V078_skybase_tbl;


-- extract the Sky base (UK) on   @skybase_target_date    (set at the top of the script)
SELECT account_number,
       cb_key_household,
       cb_key_individual,
       current_short_description,
       rank() over (PARTITION BY account_number ORDER BY effective_from_dt desc, cb_row_id) AS rank,
       convert(bit, 0)  AS uk_standard_account,
       convert(VARCHAR(20), NULL) AS isba_tv_region,
       status_code
  INTO V078_skybase_tbl
  FROM sk_prod.cust_subs_hist
 WHERE subscription_sub_type IN ('DTV Primary Viewing')
   AND status_code IN ('AC','AB','PC')             -- DTV accounts which are active, in active block or pending cancel
   AND effective_from_dt    <= @skybase_target_date
   AND effective_to_dt      > @skybase_target_date
   AND effective_from_dt    <> effective_to_dt
   AND EFFECTIVE_FROM_DT    IS NOT NULL
   AND cb_key_household     > 0
   AND cb_key_household     IS NOT NULL
   AND account_number       IS NOT NULL
   AND service_instance_id  IS NOT NULL

DELETE FROM V078_skybase_tbl WHERE rank >1


-- Identify UK accounts only
UPDATE V078_skybase_tbl
     SET uk_standard_account = CASE WHEN b.acct_type='Standard'
                                     AND b.account_number <>'?'
                                     AND b.pty_country_code ='GBR'
                                    THEN 1
                                    ELSE 0 END,
              isba_tv_region = b.isba_tv_region,
           cb_key_individual = b.cb_key_individual -- Grab the cb_key_individual we need for consumerview join
    FROM V078_skybase_tbl AS a
      inner join sk_prod.cust_single_account_view AS b
      ON a.account_number = b.account_number

DELETE FROM V078_skybase_tbl WHERE uk_standard_account=0
commit;
-- 565187 Row(s) affected




/**************************************************************************************************
 **  We need an audit showing the list of households that make up the Skybase, and whether or    **
 **  not they contain multiple accounts. This is so we can keep track of the households that     **
 **  are part of the core analytics, as multiple account households are excluded due to the      **
 **  possibility that we would 'double count' (more than double count in most cases).            **
 **  A work-a-round for this could be developed in the future.                                   **
 **                                                                                              **
 **  We create the audit in two stages:                                                          **
 **         1. Identify the households that have multiple accounts associated with them          **
 **         2. Create a table that reports on every account in the Skybase,                      **
 **            and flag if they have a multiple account                                          **
 **                                                                                              **
 **     **  This process could be achieved in one step if a dense rank was used that             **
 **     **   highlighted any household > 1  but I didn't do that                                 **
 **                                                                                              **
 **************************************************************************************************/




/* code_location_B02 **********************************************************************
 **********               Identify multiple account households                 ************
 ******************************************************************************************/

-- use this to drop accounts out of the analysis. They belong to households with multiple accounts.
-- They could belong to 'sub-households' - which leads to duplicates when comparing against demographic data

--create table to hold info
IF object_id('htoi_multiaccount_household') IS NOT NULL
  begin
    truncate table htoi_multiaccount_household
    commit
  end
ELSE
  begin
    create table htoi_multiaccount_household (cb_key_household bigint not null, multi_count bigint not null)
    commit
  end;


INSERT INTO htoi_multiaccount_household
   select cb_key_household, count(1) multi_count
     from (select distinct account_number, cb_key_household --just create a distinct list of account/households
             from V078_skybase_tbl h) f
 group by cb_key_household
   having multi_count > 1;
--133,150 Row(s) affected
commit;



/* code_location_B03 **********************************************************************
 **********               Create a summary of the Skybase accounts             ************
 ******************************************************************************************/

--set a flag in the summary table to show if it should be excluded from analysis due to being a multi account household
IF object_id('V078_skybase_summary_tbl') IS NOT NULL
    DROP TABLE V078_skybase_summary_tbl;

select sky.cb_key_household,
       sky.account_number,
       case when multi.multi_count > 1 then 1 else 0 end multi_account_HH,
       multi.multi_count
  into V078_skybase_summary_tbl
  from V078_skybase_tbl sky,
       htoi_multiaccount_household multi
 where sky.cb_key_household *= multi.cb_key_household;
commit;
--237181 Row(s) affected




/* code_location_B04 **********************************************************************
 **********       Update missing households from the UK Summary from the        ***********
 **********       Skybase accounts, and higlight multiple a/c households        ***********
 ******************************************************************************************/

-- there are some households in the Skybase that are not in the UK listing, as Experian don't have them... so find out
-- what they are, and add them to the V078_UK_hh_summary list

-- we need to do this in 2.1 operations:
--   1.0 load in all the cb_key_households that are in the skybase, but not in the experian UK lisitng
--    1.1 mark all the additional households
--   2.0 mark those that are multi-account

--   1.0
Insert into V078_UK_hh_summary (cb_key_household, gender_desc, age_band, cb_key_household_exp_unknown)
   select distinct s.cb_key_household, 'Unknown', 'Unknown', 1 --these will not be found in the Experian data so flag them 'Unknown'
     from V078_skybase_summary_tbl s
    where s.cb_key_household not in (select distinct h.cb_key_household
                                       from V078_UK_HH_demographics h);
commit;
--561034 Row(s) affected


--2.0  update the multi-account HH flag for those marked in the Skybase with the flag
Update V078_UK_hh_summary uk
   set uk.multi_account_HH = s.multi_account_HH,
       uk.multi_count = s.multi_count
  from V078_skybase_summary_tbl s
 where uk.cb_key_household = s.cb_key_household
   and s.multi_account_HH = 1;
commit;
--133,150 Row(s) affected


--add indexes
create unique index V078_UK_hh_summary_cbkeyhh_idx on V078_UK_hh_summary (cb_key_household asc);
commit;



/* code_location_B05 **********************************************************************
 *********              Update Skybase accounts into the UK Summary              **********
 ******************************************************************************************/

--update the Skybase into the UK table
update V078_uk_hh_summary uk
   set uk.account_number = sky.account_number,
       uk.skybase = 1,
       uk.multi_count = 1 --update as this is a single account household
  from V078_skybase_summary_tbl sky
 where uk.cb_key_household = sky.cb_key_household
   and sky.multi_account_HH = 0;
commit;
--9,096,020 Row(s) affected





/* code_location_C01 **********************************************************************
 *********              Update UK Demographics into the UK Summary               **********
 ******************************************************************************************/

--update all the:  gender_desc & age_band & person_count  --   NON-FLAG  fields
update V078_uk_hh_summary s
   set s.gender_desc = case when max_hh_gender_rank > 1 then 'Mixed' else ga.gender_desc end,
       s.age_band = case when max_hh_age_rank > 1 then 'Mixed' else ga.age_band end,
       s.person_count = mr.max_hh_person_rank,
       s.hh_has_child = mr.hh_has_child,
       s.uk = 1
  from V078_UK_HH_gender_age_hh_counts_tmp ga,
       V078_UK_HH_demographics_max_hh_ranks_tmp mr
 where ga.cb_key_household = mr.cb_key_household
   and s.cb_key_household = ga.cb_key_household
   and s.uk = 0;--not yet updated
--6386264 Row(s) affected
commit;



/* code_location_D01 ***********************************************************************
 ********            Flag households that can act as a Single_Proxy (where all         *****
 ********            residents are iof the same gender and in the the same age_band)   *****
 *******************************************************************************************/

--update the single proxy HHs
update V078_uk_hh_summary s
   set  s.single_proxy = 1
   from V078_UK_HH_gender_age_hh_counts_tmp ga,
        V078_UK_HH_demographics_max_hh_ranks_tmp mr
  where ga.cb_key_household = mr.cb_key_household
    and mr.max_hh_age_rank = 1
    and mr.max_hh_gender_rank = 1
    and mr.hh_has_child = 0
    and s.cb_key_household = ga.cb_key_household;
--6386264 Row(s) affected
commit;



/* code_location_D02 ***********************************************************************
 ********         Flag households that are single GENDER, without kids                 *****
 *******************************************************************************************/
update V078_UK_hh_summary s
   set  s.single_gender_without_kids = 1 --update the single GENDER HHs  - no children
   from V078_UK_HH_gender_age_hh_counts_tmp ga,
        V078_UK_HH_demographics_max_hh_ranks_tmp mr
  where ga.cb_key_household = mr.cb_key_household
    and mr.max_hh_gender_rank = 1
    and mr.hh_has_child = 0
    and s.cb_key_household = ga.cb_key_household;
commit;
--477521 Row(s) affected


/* code_location_D03 ******************************************************************
 ********         Flag households that are single AGE, without kids               *****
 **************************************************************************************/
update V078_UK_hh_summary s
   set  s.single_age_without_kids = 1  --update the single AGE HHs  - no children
   from V078_UK_HH_gender_age_hh_counts_tmp ga,
        V078_UK_HH_demographics_max_hh_ranks_tmp mr
  where ga.cb_key_household = mr.cb_key_household
    and mr.max_hh_age_rank = 1
    and mr.hh_has_child = 0
    and s.cb_key_household = ga.cb_key_household;
commit;
--2396167 Row(s) affected


/* code_location_D04 ********************************************************************
 ********         Flag households that are single GENDER, with kids                 *****
 ****************************************************************************************/
update V078_UK_hh_summary s
   set  s.single_gender_with_kids = 1   --update the single GENDER HHs  - with children
   from V078_UK_HH_gender_age_hh_counts_tmp ga,
        V078_UK_HH_demographics_max_hh_ranks_tmp mr
  where ga.cb_key_household = mr.cb_key_household
    and mr.max_hh_gender_rank = 1
    and mr.hh_has_child = 1
    and s.cb_key_household = ga.cb_key_household;
commit;
--1,402,449 Row(s) affected



/* code_location_D05 *****************************************************************
 ********         Flag households that are single AGE, with kids                 *****
 *************************************************************************************/
update V078_UK_hh_summary s
   set  s.single_age_with_kids = 1   --update the single AGE HHs  - with children
   from V078_UK_HH_gender_age_hh_counts_tmp ga,
        V078_UK_HH_demographics_max_hh_ranks_tmp mr
  where ga.cb_key_household = mr.cb_key_household
    and mr.max_hh_age_rank = 1
    and mr.hh_has_child = 1
    and s.cb_key_household = ga.cb_key_household;
commit;
--1,078,670 Row(s) affected



/* code_location_D06 *****************************************************************
 ********         Flag households that are single AGE, with kids                 *****
 *************************************************************************************/
update V078_UK_hh_summary s
   set  s.single_gender_with_or_without_kids = 1   --update the single GENDER HHs  - with/without children
   from V078_UK_HH_gender_age_hh_counts_tmp ga,
        V078_UK_HH_demographics_max_hh_ranks_tmp mr
  where ga.cb_key_household = mr.cb_key_household
    and mr.max_hh_gender_rank = 1
    and s.cb_key_household = ga.cb_key_household;
commit;
--0 Row(s) affected



/* code_location_D07 *****************************************************************
 ********         Flag households that are single AGE, with or without kids      *****
 *************************************************************************************/
update V078_UK_hh_summary s
   set  s.single_age_with_or_without_kids = 1   --update the single AGE HHs  - with/without children
   from V078_UK_HH_gender_age_hh_counts_tmp ga,
        V078_UK_HH_demographics_max_hh_ranks_tmp mr
  where ga.cb_key_household = mr.cb_key_household
    and mr.max_hh_age_rank = 1
    and s.cb_key_household = ga.cb_key_household;
commit;
--0 Row(s) affected





/* code_location_E01 **************************************************************
 ********         Add scaling information to the UK Summary table             *****
 **********************************************************************************/

--ok so we need to add scaling/weighting column to this table (based on the 14th August)
update V078_UK_hh_summary smry
   set smry.scaling_segment_id = s.scaling_segment_id,
       smry.weight = w.weighting,
       smry.vespa = 1 --only add weights to households on the Vespa panel
  from vespa_analysts.SC2_intervals s,
       vespa_analysts.SC2_weightings w
 where s.account_number = smry.account_number
   and s.scaling_segment_id = w.scaling_segment_id
   and date('2012-08-14') = w.scaling_day
   and date('2012-08-14') between s.reporting_starts and s.reporting_ends;
commit;
--529,852 Row(s) affected



/* code_location_F01 ***********************************************************
 ********         Grant permissions to the UK Summary table                *****
 *******************************************************************************/

--grant permissions to those who require it
grant select on V078_UK_hh_summary to neighbom;
commit;





/* code_location_G01  *******************************************************
 ***  Now we need to extract program data for viewing events              ***
 ***  between the 14th & 20th August, 3mins continuous viewing or longer  ***
 ****************************************************************************/
-- so this code was not used due to G02 below
/*IF object_id('V078_viewing_3plus_continuous_minutes') IS NOT NULL
    DROP TABLE V078_viewing_3plus_continuous_minutes;

SELECT  a.account_number,
        b.pk_viewing_prog_instance_fact,
        b.genre_description,
        b.sub_genre_description,
        b.channel_name,
        b.programme_instance_name,
        b.subscriber_id,
        b.broadcast_start_date_time_utc,
        b.broadcast_end_date_time_utc,
        b.instance_start_date_time_utc,
        b.instance_end_date_time_utc,
        b.bss_name, b.cb_key_household,
        b.dk_programme_instance_dim,
        b.duration,
        b.event_start_date_time_utc,
        b.event_end_date_time_utc,
        b.programme_instance_duration,
        b.product_code,
        b.pay_free_indicator
  INTO V078_viewing_3plus_continuous_minutes
  FROM sk_prod.VESPA_EVENTS_VIEWED_ALL AS b INNER JOIN barbera.V098_panel12_accounts AS a
    ON a.account_number=b.account_number
 WHERE b.subscriber_id is NOT NULL
   AND b.broadcast_start_date_time_utc between '2012-08-14 00:00:00.000' and '2012-08-21 05:59:00.000'
   AND datediff(mi, instance_start_date_time_utc, instance_end_date_time_utc) >= 3     --greater or equal to 3mins continuous viewing
   AND b.reported_playback_speed is NULL --LIVE viewing
COMMIT;
*/



/* code_location_G02  *******************************************************
 ***           Prepare Capped data for the viewing event data.            ***
 ****************************************************************************/

/*
  The viewing events would usually come from 'sk_prod.VESPA_EVENTS_VIEWED_ALL' but at the time was
  a bit of a mess, and it's not capped viewing yet - so TonyK has done something to help us out...
  and the end result is used by this query below, to give results based on capped events.

-- table created by TonyK
    select top 1000 *
      from rombaoad.V98_CappedViewingtk capped


-- and this is joined with the events table borrowed from the Skybet project to save time, which is
  an exact copy of the majority of columns from 'sk_prod.VESPA_EVENTS_VIEWED_ALL' between the
  14th and 26th August 2012.... but produced with capped data, and deduplicated.

    select top 1000 *
      from rombaoad.V98_Viewing_Table_SkyBetDates_Final;
*/


--make the capped viewing table we need for this project
IF object_id('V078_viewing_3plus_continuous_minutes_capped') IS NOT NULL
  begin
    drop table V078_viewing_3plus_continuous_minutes_capped
    commit
  end;

select *
  into V078_viewing_3plus_continuous_minutes_capped
  from rombaoad.V98_CappedViewingtk capped,
       rombaoad.V98_Viewing_Table_SkyBetDates_Final events
 where capped.cb_row_id = events.pk_viewing_prog_instance_fact
   and events.subscriber_id is NOT NULL
   and events.broadcast_start_date_time_utc between '2012-08-14 05:00:00.000' and '2012-08-21 04:59:59.999'  -- 05:00 as utc (equates to 06:00 local time)
   AND datediff(mi, events.instance_start_date_time_utc, events.instance_end_date_time_utc) >= 3     --greater or equal to 3mins continuous viewing
   AND viewing_duration >=180--3mins
   AND events.reported_playback_speed is NULL --LIVE viewing
commit;
--53,689,418 Row(s) affected




/* code_location_G03  *******************************************************
 ***           Add service key to the capped viewing data.                ***
 ****************************************************************************/

-- ok so we didn't extract the service_key (as we used the pre-extracted tables) and we need to
-- get hold of that... and limit the data set to the channels we're actually interested in, so..

IF object_id('V078_viewing_3plus_continuous_minutes_capped_s2') IS NOT NULL
  begin
    drop table V078_viewing_3plus_continuous_minutes_capped_s2
    commit
  end;

select  c.*,
        e.service_key,
        e.barb_min_start_date_time_utc,
        p.broadcast_start_date_time_local
  into  V078_viewing_3plus_continuous_minutes_capped_s2
  from  V078_viewing_3plus_continuous_minutes_capped c,
        sk_prod.VESPA_EVENTS_VIEWED_ALL e,
        sk_prod.VESPA_PROGRAMME_SCHEDULE p
 where  c.pk_viewing_prog_instance_fact = e.pk_viewing_prog_instance_fact
   and  p.dk_programme_instance_dim = c.dk_programme_instance_dim
   and  e.service_key in (1402, 1752, 4061, 2201, 4066, 4063);
commit;
--1363023 Row(s) affected

drop table V078_viewing_3plus_continuous_minutes_capped; --as we don't need it anymore
commit;




/* code_location_G04  ********************************************
 ***              De-dupe viewing event data                   ***
 *****************************************************************/

--de-dupe, and add the number of accounts watching (which should be 1 as grouped by account)
 select  account_number,
         programme_name,
         channel_name,
         --add the standard channel name (removing the HD from the channel_name)
         CASE WHEN channel_name like 'Sky1%' THEN 'Sky1'
              WHEN channel_name like 'Sky Living%' THEN 'Sky Living'
              WHEN channel_name like 'Sky Arts 1%' THEN 'Sky Arts 1'
         ELSE channel_name END as standard_channel_name,
         service_key,
         broadcast_start_date_time_local,
         broadcast_start_date_time_local adjusted_broadcast_start_date_time_local, --will be updated in a minute..
         genre_description,
         sub_genre_description,
         dk_programme_instance_dim,
         count(distinct account_number) raw_count
    into V078_viewing_3plus_continuous_minutes_capped_deduped
    from V078_viewing_3plus_continuous_minutes_capped_s2
group by account_number, programme_name, channel_name, service_key, broadcast_start_date_time_local, genre_description, sub_genre_description, dk_programme_instance_dim;
commit;


-- time correction for Sky Living HD
UPDATE V078_viewing_3plus_continuous_minutes_capped_deduped
   SET adjusted_broadcast_start_date_time_local = '2012-08-15 07:00:01'
 where service_key = 4066
   and broadcast_start_date_time_local = '2012-08-15 07:00:00.000000'




/* code_location_G05  ********************************************
 ***               Add scaling weights                         ***
 *****************************************************************/
IF object_id('V078_viewing_3plus_continuous_minutes_capped_deduped_scaled') IS NOT NULL
    DROP TABLE V078_viewing_3plus_continuous_minutes_capped_deduped_scaled
    commit;

select v.*,
       s.scaling_segment_id,
       w.weighting weighted_count
  into V078_viewing_3plus_continuous_minutes_capped_deduped_scaled
  from vespa_analysts.SC2_intervals s,
     vespa_analysts.SC2_weightings w,
     V078_viewing_3plus_continuous_minutes_capped_deduped v
where  s.account_number = v.account_number
  and  s.scaling_segment_id = w.scaling_segment_id
  and  date(broadcast_start_date_time_local) = w.scaling_day
  and  date(broadcast_start_date_time_local) between s.reporting_starts and s.reporting_ends;
commit;
--1,063,422 Row(s) affected






/* code_location_H01  ***********************************************
 ***      Create table to hold the single proxy viewing data      ***
 ********************************************************************/
IF object_id('V078_viewing_3plusMins_by_singleproxy') IS NOT NULL
    DROP TABLE V078_viewing_3plusMins_by_singleproxy
    commit;

select  v.programme_name,
        v.channel_name,
        v.standard_channel_name,
        v.service_key,
        v.adjusted_broadcast_start_date_time_local,
        v.genre_description,
        v.sub_genre_description,
        v.dk_programme_instance_dim,
        age_band,
        gender_desc,
        sum(v.raw_count) vespa_count,
        round(sum(v.weighted_count),0) skybase_count
  into V078_viewing_3plusMins_by_singleproxy
  from V078_viewing_3plus_continuous_minutes_capped_deduped_scaled v,
       V078_UK_hh_summary s
 where v.account_number = s.account_number
   and s.single_proxy = 1
   --and just to make sure we have the right program date range
   and v.adjusted_broadcast_start_date_time_local between '2012-08-14 06:00:00.000' and '2012-08-21 05:59:59.999'
group by  v.programme_name, v.channel_name,
          v.standard_channel_name,
          v.service_key,
          v.adjusted_broadcast_start_date_time_local,
          v.genre_description, v.sub_genre_description, v.dk_programme_instance_dim,
          age_band,
          gender_desc;
commit;

grant select on V078_viewing_3plusMins_by_singleproxy to patelj, neighbom;
grant select on V078_viewing_3plusMins_hh_summary to patelj, neighbom;
commit;




/* code_location_H02  ***********************************************
 ***      Create table to hold the ADULT proxy viewing data       ***
 ********************************************************************/
IF object_id('V078_viewing_3plusMins_by_single_age_without_kids') IS NOT NULL
    DROP TABLE V078_viewing_3plusMins_by_single_age_without_kids
    commit;

select  v.programme_name, v.channel_name, v.standard_channel_name,
        v.service_key,
        v.adjusted_broadcast_start_date_time_local,
        v.genre_description, v.sub_genre_description, v.dk_programme_instance_dim,
        age_band,
        sum(v.raw_count) vespa_count,
        round(sum(v.weighted_count),0) skybase_count
  into V078_viewing_3plusMins_by_single_age_without_kids
  from V078_viewing_3plus_continuous_minutes_capped_deduped_scaled v,
       V078_UK_hh_summary s
 where v.account_number = s.account_number
   and single_age_without_kids = 1
   --and just to make sure we have the right program date range
   and v.adjusted_broadcast_start_date_time_local between '2012-08-14 06:00:00.000' and '2012-08-21 05:59:59.999'
group by  v.programme_name, v.channel_name, v.standard_channel_name,
          v.service_key,
          v.adjusted_broadcast_start_date_time_local,
          v.genre_description, v.sub_genre_description, v.dk_programme_instance_dim, age_band;
commit;


--set permissions
grant select on V078_viewing_3plusMins_by_single_age_without_kids to patelj, neighbom;




/* code_location_H03  ***********************************************
 ***      Create table to hold the GENDER proxy viewing data       ***
 ********************************************************************/
IF object_id('V078_viewing_3plusMins_by_single_gender_without_kids') IS NOT NULL
    DROP TABLE V078_viewing_3plusMins_by_single_gender_without_kids
    commit;

select  v.programme_name, v.channel_name, v.standard_channel_name,
        v.service_key,
        v.adjusted_broadcast_start_date_time_local,
        v.genre_description, v.sub_genre_description, v.dk_programme_instance_dim,
        gender_desc,
        sum(v.raw_count) vespa_count,
        round(sum(v.weighted_count),0) skybase_count
  into V078_viewing_3plusMins_by_single_gender_without_kids
  from V078_viewing_3plus_continuous_minutes_capped_deduped_scaled v,
       V078_viewing_3plusMins_hh_summary s
 where v.account_number = s.account_number
   and single_gender_without_kids = 1  --all female, all male
   --and just to make sure we have the right program date range
   and v.adjusted_broadcast_start_date_time_local between '2012-08-14 06:00:00.000' and '2012-08-21 05:59:59.999'
group by  v.programme_name, v.channel_name, v.standard_channel_name,
          v.service_key,
          v.adjusted_broadcast_start_date_time_local,
          v.genre_description, v.sub_genre_description, v.dk_programme_instance_dim,
          gender_desc;
commit;

--set permissions
grant select on V078_viewing_3plusMins_by_single_gender_without_kids to patelj, neighbom;





/* code_location_I01  ******************************************************
 ***                        Overview Counts                              ***
 ***************************************************************************/

select top 1000 *
from V078_UK_hh_summary;-- we use this table to collate all the counts, it contains all the flags and info we need


--lets just get an overview of what we have
select cb_key_household_exp_unknown, multi_account_HH, gender_desc, age_band, count(1)
from V078_UK_hh_summary
group by cb_key_household_exp_unknown, multi_account_HH, gender_desc, age_band
order by cb_key_household_exp_unknown, multi_account_HH, gender_desc, age_band;



--this is the list of distinct accounts in the Skybase
select count(distinct account_number)
from V078_UK_hh_summary
where multi_account_HH = 0;
--count(distinct V078_UK_hh_summary.account_number) 9,096,020


-- the number of accounts flagged as the Skybase must also equal the distinct number of accounts
select sum(skybase)
from V078_UK_hh_summary;
--sum(V078_UK_hh_summary.skybase) 9,096,020




/* code_location_I02  ******************************************************
 ***             INDIVIDUAL/HOUSEHOLD LEVEL COUNTS                       ***
 ***        for example,  HOW MANY MALES IN AN AGE_BAND x                ***
 ***************************************************************************/

-- this returns the counts broken down by whether or not the household is a multi-account one (multi_account_hh = 1)
-- or if the household is one listed by Experian (cb_key_household_exp_unknown = 0)
-- UK / Sky / Vespa / Vespa_weighted    HOUSEHOLD   count
select  cb_key_household_exp_unknown, multi_account_hh,
        sum(s.uk) uk_hh_count,
        sum(s.skybase) sky_hh_count,
        sum(s.vespa) vespa_hh_count,
        sum(s.weight) vespa_weighted_hh_count_v2,
        sum(multi_account_hh) multi_account_hh_count,
        sum(cb_key_household_exp_unknown) household_unknown
  from  V078_UK_hh_summary s
  group by cb_key_household_exp_unknown,
           multi_account_hh;



--
-- UK/Sky/Vespa  INDIVIDUAL (person)  count
select  sum(case when s.uk = 1 then s.person_count else 0 end) uk_person_count,
        sum(case when s.skybase = 1 then s.person_count else 0 end) sky_person_count,
        sum(case when s.vespa = 1 then s.person_count else 0 end) vespa_person_count
  from V078_UK_hh_summary s;
/*
        uk_person_count         sky_person_count        vespa_person_count
        49,410,460                18,699,421                1,092,571
*/


--
-- UK/Sky/Vespa  INDIVIDUAL - MALE/FEMALE   (person)  count
--single_gender_without_kids
select s.gender_desc,
       sum(case when s.uk = 1 then s.person_count else 0 end) uk_person_count,
       sum(case when s.skybase = 1 then s.person_count else 0 end) sky_person_count,
       sum(case when s.vespa = 1 then s.person_count else 0 end) vespa_person_count
  from V078_UK_hh_summary s
 where s.single_gender_without_kids = 1
group by s.gender_desc;
/*
        gender_desc     uk_person_count sky_person_count        vespa_person_count
        Male            4,553,778         1,144,500                 62,471
        Unknown            29,153             4,763                    148
        Female          5,873,618         1,418,939                 75,588
*/



--
-- UK/Sky/Vespa  INDIVIDUAL - Adults, same age_band   (person)  count
--single_age_without_kids
select  s.age_band,
        sum(case when s.uk = 1 then s.person_count else 0 end) uk_person_count,
        sum(case when s.skybase = 1 then s.person_count else 0 end) sky_person_count,
        sum(case when s.vespa = 1 then s.person_count else 0 end) vespa_person_count
  from V078_UK_hh_summary s
 where s.single_age_without_kids = 1
group by s.age_band
order by s.age_band;



--
-- UK/Sky/Vespa  INDIVIDUAL - Adults, same age_band   (person)  count
--single_age_without_kids
select  s.gender_desc, s.age_band,
        sum(case when s.uk = 1 then s.person_count else 0 end) uk_person_count,
        sum(case when s.skybase = 1 then s.person_count else 0 end) sky_person_count,
        sum(case when s.vespa = 1 then s.person_count else 0 end) vespa_person_count
  from V078_UK_hh_summary s
 where s.single_proxy = 1
group by s.gender_desc, s.age_band
order by s.gender_desc, s.age_band;



--
--single gender, vespa, single proxy
select  s.gender_desc,
        sum(s.vespa) proxy_hh_vespa_count,
        sum(s.person_count)  proxy_indiv_vespa_count,
        sum(weight) proxy_hh_vespa_weighted_count
  from V078_UK_hh_summary s
 where s.single_proxy = 1
   and s.vespa = 1 -- restrict to Vespa flagged households
group by s.gender_desc;



--
--single age, vespa, single proxy
select  s.age_band,
        sum(s.vespa) proxy_hh_vespa_count,
        sum(s.person_count)  proxy_indiv_vespa_count,
        sum(weight) proxy_hh_vespa_weighted_count
  from V078_UK_hh_summary s
 where s.single_age_without_kids = 1
   and s.vespa = 1
group by s.age_band
order by s.age_band;



--
--single age, vespa, single proxy
select  s.gender_desc, s.age_band,
        sum(s.vespa) proxy_hh_vespa_count,
        sum(s.person_count)  proxy_indiv_vespa_count,
        sum(weight) proxy_hh_vespa_weighted_count
  from V078_UK_hh_summary s
 where s.single_proxy = 1
   and s.vespa = 1
group by s.gender_desc, s.age_band
order by s.gender_desc, s.age_band;






-------------                  E   N   D                   -------------
-------------                                              -------------
-------------    C O M P L E T E D    C O D E    R U N     -------------
-------------                                              -------------
--********************************************************************--




