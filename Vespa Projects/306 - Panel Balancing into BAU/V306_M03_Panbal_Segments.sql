/*


                         $$$
                        I$$$
                        I$$$
               $$$$$$$$ I$$$    $$$$$      $$$ZDD    DDDDDDD.
             ,$$$$$$$$  I$$$   $$$$$$$    $$$ ODD  ODDDZ 7DDDD
             ?$$$,      I$$$ $$$$. $$$$  $$$= ODD  DDD     NDD
              $$$$$$$$= I$$$$$$$    $$$$.$$$  ODD +DD$     +DD$
                  :$$$$~I$$$ $$$$    $$$$$$   ODD  DDN     NDD.
               ,.   $$$+I$$$  $$$$    $$$$=   ODD  NDDN   NDDN
              $$$$$$$$$ I$$$   $$$$   .$$$    ODD   ZDDDDDDDN
                                      $$$      .      $DDZ
                                     $$$             ,NDDDDDDD
                                    $$$?

                      CUSTOMER INTELLIGENCE SERVICES

-----------------------------------------------------------------------------------

**Project Name:                         Panel Balancing
**Analysts:                             Jon Green   (Jonathan.Green@skyiq.co.uk)
                                        Leonardo Ripoli
**Lead(s):                              Hoi Yu Tang (hoiyu.tang@skyiq.co.uk)
**Stakeholder:                          Jose Loureda
**Project Code (Insight Collation):     V306
**SharePoint Folder:                    http://sp-department.bskyb.com/sites/IQSKY/SIG/Insight%20Collation%20Documents/Forms/AllItems.aspx?RootFolder=%2Fsites%2FIQSKY%2FSIG%2FInsight%20Collation%20Documents%2F01%20Analysis%20Requests%2FV306%20-%20Foundation%20-%20Platform%20Maintenance%2FPhase%202%2FPanel%20Balancing

**Business Brief:

Panel Balancing is a regular exercise to ensure that the viewing panel is as
representative of the wider Sky customer base as possible as the latter evolves over
time. Balancing is also crucial in ensuring that key KPI and contractual obligations
of account coverage are maintained.




**Module:                              M03_Panbal_Segments

This module categorises each account on the sky base against each of the balancing variables. A segment is created for each combination that at least one account matches.
A lookup table is created to find the segment ID for each account, and a lookup table is created with the segment details for each segment ID.

the balancing variables are:

adsmbl     (adsmartable)
region
hhcomp     (household composition)
tenure     (Sky tenure)
package    (Sky TV package)
mr         (multiscreen)
hd
pvr
valseg     (value segment)
mosaic
fss        (financial segment)
onnet      (located in an OnNet area)
skygo
st         (Sky Talk)
bb         (Broadband)
bb_capable


*/

  create or replace procedure V306_M03_PanBal_Segments
           @include_churned bit   = 0
      as begin

            create table temp_PanBal_weekly_sample (
                   account_number    varchar(30) null
                  ,cb_key_household  bigint null
                  ,cb_key_individual bigint null
                  ,adsmbl            varchar(30) default 'Non-Adsmartable' null
                  ,region            varchar(40) null
                  ,hhcomp            varchar(30) null
                  ,tenure            varchar(30) null
                  ,package           varchar(30) null
                  ,mr                bit         default 0
                  ,hd                bit         default 0
                  ,pvr               bit         default 0
                  ,valseg            varchar(30) null
                  ,mosaic            varchar(30) null
                  ,fss               varchar(30) null
                  ,onnet             bit         default 0
                  ,skygo             bit         default 0
                  ,st                bit         default 0
                  ,bb                bit         default 0
                  ,bb_capable        varchar(8)  default 'No Panel' null
                   )
            create unique hg index idx1 on temp_PanBal_weekly_sample(account_number)
            create        lf index idx2 on temp_PanBal_weekly_sample(region)
            create        lf index idx3 on temp_PanBal_weekly_sample(hhcomp)
            create        lf index idx4 on temp_PanBal_weekly_sample(tenure)
            create        lf index idx5 on temp_PanBal_weekly_sample(package)
            create        lf index idx6 on temp_PanBal_weekly_sample(valseg)
            create        lf index idx7 on temp_PanBal_weekly_sample(mosaic)
            create        lf index idx8 on temp_PanBal_weekly_sample(fss)

            create table temp_PanBal_segments_lookup(
                   segment_id        bigint identity primary key null
                  ,adsmbl            varchar(30)   default 'Non-Adsmartable' null
                  ,region            varchar(40) null
                  ,hhcomp            varchar(30)   default 'U' null
                  ,tenure            varchar(30) null
                  ,package           varchar(30) null
                  ,mr                bit           default 0
                  ,hd                bit           default 0
                  ,pvr               bit           default 0
                  ,valseg            varchar(30)   default 'Unknown' null
                  ,mosaic            varchar(30)   default 'U' null
                  ,fss               varchar(30)   default 'U' null
                  ,onnet             bit           default 0
                  ,skygo             bit           default 0
                  ,st                bit           default 0
                  ,bb                bit           default 0
                  ,bb_capable        varchar(8)    default 'No Panel' null
                  ,panel_accounts    decimal(10,2) default 0 null
                  ,base_accounts     int           default 0 null
                   )
            create lf index lfads on temp_PanBal_segments_lookup(adsmbl)
            create lf index lfreg on temp_PanBal_segments_lookup(region)
            create lf index lfhhc on temp_PanBal_segments_lookup(hhcomp)
            create lf index lften on temp_PanBal_segments_lookup(tenure)
            create lf index lfpac on temp_PanBal_segments_lookup(package)
            create lf index lfval on temp_PanBal_segments_lookup(valseg)
            create lf index lfmos on temp_PanBal_segments_lookup(mosaic)
            create lf index lffss on temp_PanBal_segments_lookup(fss)
            create lf index lfbbc on temp_PanBal_segments_lookup(bb_capable)

            create table temp_PanBal_segments_lookup_unnormalised(
                   segment_id bigint identity null
                  ,v1         varchar(30)   default 'Non-Adsmartable' null
                  ,v2         varchar(40) null
                  ,v3         varchar(30)   default 'U' null
                  ,v4         varchar(30) null
                  ,v5         varchar(30) null
                  ,v6         bit           default 0
                  ,v7         bit           default 0
                  ,v8         bit           default 0
                  ,v9         varchar(30)   default 'Unknown' null
                  ,v10        varchar(30)   default 'U' null
                  ,v11        varchar(30)   default 'U' null
                  ,v12        bit           default 0
                  ,v13        bit           default 0
                  ,v14        bit           default 0
                  ,v15        bit           default 0
                  ,v16        varchar(8)    default 'No Panel' null
                   )

            commit
            create hg index hgseg on temp_PanBal_segments_lookup_unnormalised(segment_id)
            create lf index lfv1  on temp_PanBal_segments_lookup_unnormalised(v1)
            create lf index lfv2  on temp_PanBal_segments_lookup_unnormalised(v2)
            create lf index lfv3  on temp_PanBal_segments_lookup_unnormalised(v3)
            create lf index lfv4  on temp_PanBal_segments_lookup_unnormalised(v4)
            create lf index lfv5  on temp_PanBal_segments_lookup_unnormalised(v5)
            create lf index lfv9  on temp_PanBal_segments_lookup_unnormalised(v9)
            create lf index lfv10 on temp_PanBal_segments_lookup_unnormalised(v10)
            create lf index lfv11 on temp_PanBal_segments_lookup_unnormalised(v11)
            create lf index lfv16 on temp_PanBal_segments_lookup_unnormalised(v16)

            create table temp_matches(
                   segment_id bigint
                  )

           declare @counter bigint

          truncate table panbal_variables
            insert into panbal_variables(
                   id
                  ,aggregation_variable
                   )
            select 1
                 ,'adsmbl'
             union
            select 2
                  ,'region'
             union
            select 3
                  ,'hhcomp'
             union
            select 4
                  ,'tenure'
             union
            select 5
                  ,'package'
             union
            select 6
                  ,'mr'
             union
            select 7
                  ,'hd'
--             union
--            select 8
--                  ,'pvr'
             union
            select 9
                  ,'valseg'
             union
            select 10
                  ,'mosaic'
             union
            select 11
                  ,'fss'
             union
            select 12
                  ,'onnet'
             union
            select 13
                  ,'skygo'
             union
            select 14
                  ,'st'
             union
            select 15
                  ,'bb'

           declare @profiling_thursday date
           execute vespa_analysts.Regulars_Get_report_end_date @profiling_thursday output  -- proc returns a Saturday
               set @profiling_thursday = @profiling_thursday - 2                           -- but we want a Thursday

            create table temp_weekly_sample(
                   account_number            varchar(30) null
                  ,cb_key_household          bigint null
                  ,cb_key_individual         bigint null
                  ,current_short_description varchar(50) null
                  ,rank                      int
                  ,uk_standard_account       bit default 0
                  ,isba_tv_region            varchar(20) null
                  )

                -- Captures all active accounts in cust_subs_hist
                if @include_churned = 1 begin
                      insert into temp_weekly_sample
                      SELECT account_number
                            ,cb_key_household
                            ,cb_key_individual
                            ,current_short_description
                            ,rank() over (PARTITION BY account_number ORDER BY effective_from_dt desc, cb_row_id) AS rank
                            ,convert(bit, 0)  AS uk_standard_account
                            ,convert(VARCHAR(20), NULL) AS isba_tv_region
                        FROM cust_subs_hist as csh
                       WHERE subscription_sub_type IN ('DTV Primary Viewing')
                         AND status_code IN ('AC', 'AB', 'PC', 'SC', 'PO')
                         AND effective_from_dt    <= @profiling_thursday
                         AND effective_to_dt      > @profiling_thursday
                         AND EFFECTIVE_FROM_DT    IS NOT NULL
                         AND cb_key_household     > 0
                         AND cb_key_household     IS NOT NULL
                         AND cb_key_individual    IS NOT NULL
                         AND service_instance_id  IS NOT NULL
               end
              else begin
                      insert into temp_weekly_sample
                      SELECT account_number
                            ,cb_key_household
                            ,cb_key_individual
                            ,current_short_description
                            ,rank() over (PARTITION BY account_number ORDER BY effective_from_dt desc, cb_row_id) AS rank
                            ,convert(bit, 0)  AS uk_standard_account
                            ,convert(VARCHAR(20), NULL) AS isba_tv_region
                        FROM cust_subs_hist as csh
                       WHERE subscription_sub_type IN ('DTV Primary Viewing')
                         AND status_code IN ('AC', 'AB', 'PC')
                         AND effective_from_dt    <= @profiling_thursday
                         AND effective_to_dt      > @profiling_thursday
                         AND EFFECTIVE_FROM_DT    IS NOT NULL
                         AND cb_key_household     > 0
                         AND cb_key_household     IS NOT NULL
                         AND cb_key_individual    IS NOT NULL
                         AND service_instance_id  IS NOT NULL
               end

                -- De-dupe accounts
            COMMIT
            DELETE FROM temp_weekly_sample WHERE rank > 1

            COMMIT
            CREATE UNIQUE hg INDEX uhacc ON temp_weekly_sample (account_number)
            CREATE        lf INDEX lfcur ON temp_weekly_sample (current_short_description)

                -- Take out non-standard accounts as these are not currently in the scope of Vespa
            UPDATE temp_weekly_sample
               SET uk_standard_account = CASE WHEN b.acct_type='Standard' AND b.account_number <>'?' THEN 1
                                              ELSE 0
                                         END
                  ,isba_tv_region      = b.isba_tv_region
                  ,cb_key_individual   = b.cb_key_individual
              FROM temp_weekly_sample AS a
                   inner join cust_single_account_view AS b ON a.account_number = b.account_number

            COMMIT
            DELETE FROM temp_weekly_sample WHERE uk_standard_account = 0


                /**************** L02: ASSIGN VARIABLES ****************/
                -- Since "h_household_composition" & "p_head_of_household" are in two separate tables, an intemidiary table is created
                -- so both variables are available for ranking function in the next step
            SELECT cv.cb_key_household
                  ,cv.cb_key_family
                  ,cv.cb_key_individual
                  ,min(cv.cb_row_id)               as cb_row_id
                  ,max(cv.h_household_composition) as h_household_composition
                  ,max(pp.p_head_of_household)     as p_head_of_household
                  ,max(h_mosaic_uk_group)          as mosaic
                  ,max(h_fss_v3_group)             as fss
              INTO temp_cv_pp
              FROM EXPERIAN_CONSUMERVIEW cv,
                   PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD pp
             WHERE cv.exp_cb_key_db_individual = pp.exp_cb_key_db_individual
               AND cv.cb_key_individual is not null
          GROUP BY cv.cb_key_household
                  ,cv.cb_key_family
                  ,cv.cb_key_individual

            COMMIT
            CREATE LF INDEX idx1 on temp_cv_pp(p_head_of_household)
            CREATE HG INDEX idx2 on temp_cv_pp(cb_key_family)
            CREATE HG INDEX idx3 on temp_cv_pp(cb_key_individual)

            SELECT cb_key_individual
                  ,cb_row_id
                  ,rank() over(partition by cb_key_family     ORDER BY p_head_of_household desc,  cb_row_id desc) as rank_fam
                  ,rank() over(partition by cb_key_individual ORDER BY p_head_of_household desc,  cb_row_id desc) as rank_ind
                  ,h_household_composition
                  ,mosaic
                  ,fss
              INTO temp_cv_keys
              FROM temp_cv_pp
             WHERE cb_key_individual IS not NULL
               AND cb_key_individual <> 0

            commit
            DELETE FROM temp_cv_keys WHERE rank_fam != 1 AND rank_ind != 1

            commit
            CREATE INDEX index_ac on temp_cv_keys (cb_key_individual)

                -- Populate package
            INSERT INTO temp_PanBal_weekly_sample (
                   account_number
                  ,cb_key_household
                  ,cb_key_individual
                  ,package
            )
            SELECT fbp.account_number
                  ,fbp.cb_key_household
                  ,fbp.cb_key_individual
                  ,CASE WHEN cel.prem_sports = 2 AND cel.prem_movies = 2 THEN               'Top Tier'
                        WHEN cel.prem_sports = 2 AND cel.prem_movies = 0 THEN               'Dual Sports'
                        WHEN cel.prem_sports = 0 AND cel.prem_movies = 2 THEN               'Dual Movies'
                        WHEN cel.prem_sports = 1 AND cel.prem_movies = 0 THEN               'Single Sports'
                        WHEN cel.prem_sports = 0 AND cel.prem_movies = 1 THEN               'Single Movies'
                        WHEN cel.prem_sports > 0 OR  cel.prem_movies > 0 THEN               'Other Premiums'
                        WHEN kids = 1 or music = 1 or news_events = 1 or knowledge = 1 then 'Basic - Ent Extra'
                        ELSE                                                                'Basic - Ent'
                   END
              FROM temp_weekly_sample AS fbp
                   left join cust_entitlement_lookup AS cel ON fbp.current_short_description = cel.short_description
             WHERE fbp.cb_key_household IS NOT NULL
               AND fbp.cb_key_individual IS NOT NULL

            commit

                -- Experian variables
            UPDATE temp_PanBal_weekly_sample as sws
               SET sws.hhcomp = case when cv.h_household_composition in ('00', '01', '02', '03', '09', '10')         then 'A'
                                     when cv.h_household_composition in ('04', '05')                                 then 'B'
                                     when cv.h_household_composition in ('06', '07', '08', '11')                     then 'C'
                                     else                                                                                 'D'
                                end
                  ,fss    = cv.fss
                  ,mosaic = cv.mosaic
              FROM temp_cv_keys AS cv
             where sws.cb_key_individual = cv.cb_key_individual

                -- Coalesce didn't work, so...
            UPDATE temp_PanBal_weekly_sample as sws set hhcomp = 'U' where hhcomp is null
            UPDATE temp_PanBal_weekly_sample as sws set mosaic = 'U' where mosaic is null
            UPDATE temp_PanBal_weekly_sample as sws set fss    = 'U'    where fss is null

                -- ROI region preliminary
            select bas.account_number
                  ,pty_country_code
                  ,isba_tv_region
                  ,case when cb_address_status = '1' and roi_address_match_source is not null and cb_address_county is not null then upper(cb_address_county) -- take cleansed geographic county where address has been fully matched to Geodirectory
                                                                        when upper(pty_county_raw) like '%DUBLIN%'    then 'DUBLIN' -- otherwise use standardised form of county from the Chordiant raw county field for all 26 counties
                                                                        when upper(pty_county_raw) like '%WESTMEATH%' then 'WESTMEATH' -- make sure WESTMEATH is above MEATH in the hierarchy otherwise WESTMEATH will get set to MEATH!
                                                                        when upper(pty_county_raw) like '%MEATH%'     then 'MEATH'
                        when upper(pty_county_raw) like '%CARLOW%'    then 'CARLOW'
                        when upper(pty_county_raw) like '%CAVAN%'     then 'CAVAN'
                        when upper(pty_county_raw) like '%CLARE%'     then 'CLARE'
                        when upper(pty_county_raw) like '%CORK%'      then 'CORK'
                        when upper(pty_county_raw) like '%DONEGAL%'   then 'DONEGAL'
                        when upper(pty_county_raw) like '%GALWAY%'    then 'GALWAY'
                        when upper(pty_county_raw) like '%KERRY%'     then 'KERRY'
                        when upper(pty_county_raw) like '%KILDARE%'   then 'KILDARE'
                        when upper(pty_county_raw) like '%KILKENNY%'  then 'KILKENNY'
                        when upper(pty_county_raw) like '%LAOIS%'     then 'LAOIS'
                        when upper(pty_county_raw) like '%LEITRIM%'   then 'LEITRIM'
                        when upper(pty_county_raw) like '%LIMERICK%'  then 'LIMERICK'
                        when upper(pty_county_raw) like '%LONGFORD%'  then 'LONGFORD'
                        when upper(pty_county_raw) like '%LOUTH%'     then 'LOUTH'
                        when upper(pty_county_raw) like '%MAYO%'      then 'MAYO'
                        when upper(pty_county_raw) like '%MONAGHAN%'  then 'MONAGHAN'
                        when upper(pty_county_raw) like '%OFFALY%'    then 'OFFALY'
                        when upper(pty_county_raw) like '%ROSCOMMON%' then 'ROSCOMMON'
                        when upper(pty_county_raw) like '%SLIGO%'     then 'SLIGO'
                        when upper(pty_county_raw) like '%TIPPERARY%' then 'TIPPERARY'
                        when upper(pty_county_raw) like '%WATERFORD%' then 'WATERFORD'
                        when upper(pty_county_raw) like '%WEXFORD%'   then 'WEXFORD'
                        when upper(pty_county_raw) like '%WICKLOW%'   then 'WICKLOW'
                                                                        when pty_county_raw is null and upper(pty_town_raw) like '%DUBLIN%' then 'DUBLIN' -- otherwise look for Dublin postal districts as raw county often null for these
                                                                        else 'Unknown'
                                                                end as roi_county
              into temp_roi_region
              from cust_single_account_view as sav
                   inner join temp_PanBal_weekly_sample as bas on sav.account_number = bas.account_number

                -- Region
            update temp_PanBal_weekly_sample as bas
               set region = case when pty_country_code = 'IRL' then case when ROI_County in ('DUBLIN','KILDARE','LAOIS','LONGFORD','LOUTH','MEATH','OFFALY','WESTMEATH','WICKLOW')    then 'ROI EASTERN AND MIDLANDS'
                                                                                                                                 when ROI_County in ('CAVAN','DONEGAL','GALWAY','LEITRIM','MAYO','MONAGHAN','ROSCOMMON','SLIGO')              then 'ROI NORTHERN AND WESTERN'
                                                                                                                                 when ROI_County in ('CARLOW','CLARE','CORK','KERRY','KILKENNY','LIMERICK','TIPPERARY','WATERFORD','WEXFORD') then 'ROI SOUTHERN'
                                                                                                                                else 'ROI Not Defined'
                                                                                                                        end
                                 else isba_tv_region
                             end
              from temp_roi_region as reg
             where bas.account_number = reg.account_number

                -- Tenure
            UPDATE temp_PanBal_weekly_sample as bas
               SET bas.tenure = CASE WHEN datediff(day,acct_first_account_activation_dt,@profiling_thursday) <=  304 THEN 'A) 0-10 Months'
                                     WHEN datediff(day,acct_first_account_activation_dt,@profiling_thursday) <=  730 THEN 'B) 10-24 Months'
                                     WHEN datediff(day,acct_first_account_activation_dt,@profiling_thursday) <= 3652 THEN 'C) 2-10 Years'
                                     WHEN datediff(day,acct_first_account_activation_dt,@profiling_thursday) >  3652 THEN 'D) 10 Years+'
                                     ELSE 'E) Unknown'
                                END
              FROM cust_single_account_view as sav
             WHERE bas.account_number = sav.account_number


            COMMIT

                -- MR, HD, PVR
            SELECT account_number
                  ,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV Extra Subscription' THEN 1 ELSE 0  END) AS MR
                  ,1 AS pvr
                  ,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV HD'                 THEN 1 ELSE 0  END) AS HD
              INTO temp_scaling_box_level_viewing
              FROM cust_subs_hist AS csh
             WHERE effective_FROM_dt <= @profiling_thursday
               AND effective_to_dt    > @profiling_thursday
               AND status_code IN  ('AC','AB','PC')
               AND SUBSCRIPTION_SUB_TYPE IN ('DTV Primary Viewing', 'DTV Sky+', 'DTV Extra Subscription', 'DTV HD')
          GROUP BY account_number

            commit

            update temp_PanBal_weekly_sample as bas
               set bas.hd = blv.hd
                  ,bas.mr = blv.mr
                  ,bas.pvr = blv.pvr
              from temp_scaling_box_level_viewing as blv
             where bas.account_number = blv.account_number

            update temp_PanBal_weekly_sample as bas
               set valseg = coalesce(seg.value_seg, 'Unknown')
              from VALUE_SEGMENTS_DATA as seg
             where bas.account_number = seg.account_number

                -- coalesce didn't work again, so...
            update temp_PanBal_weekly_sample as bas
               set valseg = 'Unknown' where valseg is null

            update temp_PanBal_weekly_sample as bas
               set skygo = 1
              from SKY_PLAYER_USAGE_DETAIL as spu
             where bas.account_number = spu.account_number
               and activity_dt >= '2011-08-18'
                -- this query takes 10 mins

                -- The OnNet goes by postcode, so...
            select account_number
                  ,min(cb_address_postcode) as postcode
                  ,convert(bit, 0) as onnet
              into temp_onnet_patch
              from cust_single_account_view
             where cust_active_dtv = 1
          group by account_number

            update temp_onnet_patch
               set postcode = upper(REPLACE(postcode,' ',''))

            commit
            create unique hg index idx1 on temp_onnet_patch (account_number)
            create        index joinsy  on temp_onnet_patch (postcode)

                -- 1) Get BROADBAND_POSTCODE_EXCHANGE postcodes
            SELECT cb_address_postcode as postcode, MAX(mdfcode) as exchID
              INTO temp_bpe
              FROM BROADBAND_POSTCODE_EXCHANGE
          GROUP BY postcode

            update temp_bpe
               set postcode = upper(REPLACE( postcode,' ',''))

            commit
            create unique index fake_pk on temp_bpe (postcode)

                -- 2) Get BB_POSTCODE_TO_EXCHANGE postcodes
            SELECT postcode as postcode, MAX(exchange_id) as exchID
              INTO temp_p2e
              FROM BB_POSTCODE_TO_EXCHANGE
          GROUP BY postcode

            update temp_p2e
               set postcode = upper(REPLACE( postcode,' ',''))

            commit
            create unique index fake_pk on temp_p2e (postcode)

                -- 3) Combine postcode lists taking BB_POSTCODE_TO_EXCHANGE exchange_id's where possible
            SELECT COALESCE(temp_p2e.postcode, temp_bpe.postcode) AS postcode
                  ,COALESCE(temp_p2e.exchID, temp_bpe.exchID) as exchange_id
                  ,'OFFNET' as exchange
              INTO temp_onnet_lookup
              FROM temp_bpe FULL JOIN temp_p2e ON temp_bpe.postcode = temp_p2e.postcode

            commit
            create unique index fake_pk on temp_onnet_lookup (postcode)

                -- 4) Update with latest Easynet exchange information
            UPDATE temp_onnet_lookup
               SET exchange = 'ONNET'
              FROM temp_onnet_lookup AS base
                   INNER JOIN easynet_rollout_data as easy on base.exchange_id = easy.exchange_id
             WHERE easy.exchange_status = 'ONNET'

                -- 5) Flag your base table with onnet exchange data. Note that this uses a postcode field with
                --   spaces removed so your table will either need to have a similar filed or use a REPLACE
                --   function in the join
            UPDATE temp_onnet_patch
               SET onnet = CASE WHEN tgt.exchange = 'ONNET'
                                THEN 1
                                ELSE 0
                           END
              FROM temp_onnet_patch AS bas
                   INNER JOIN temp_onnet_lookup AS tgt on bas.postcode = tgt.postcode

            commit

            update temp_PanBal_weekly_sample as bas
               set bas.onnet = onn.onnet
              from temp_onnet_patch as onn
             where bas.account_number = onn.account_number

            update temp_PanBal_weekly_sample as bas
               set bb = 1
              from cust_subs_hist as csh
             where bas.account_number = csh.account_number
               and subscription_sub_type = 'Broadband DSL Line'
               and status_code in ('AC', 'AB', 'PC', 'CF', 'PT')
               and effective_from_dt <= @profiling_thursday
               and effective_to_dt    > @profiling_thursday

            update temp_PanBal_weekly_sample as bas
               set st = 1
              from cust_subs_hist as csh
             where bas.account_number = csh.account_number
               and subscription_sub_type = 'SKY TALK SELECT'
               and status_code in ('A', 'FBP', 'PC', 'RI')
               and effective_from_dt <= @profiling_thursday
               and effective_to_dt    > @profiling_thursday

            create table temp_noconsent(account_number varchar(30))

            insert into temp_noconsent
            select account_number
              from cust_single_account_view as sav
             where cust_viewing_data_capture_allowed <> 'Y'
          group by account_number

            create table temp_adsmbl(account_number varchar(30))

            insert into temp_adsmbl
            select account_number
              from cust_set_top_box
             where active_box_flag = 'Y'
               and (x_pvr_type in ('PVR5', 'PVR6') and x_manufacturer not in ('Samsung'))
           group by account_number

            commit
            create unique hg index idx1 on temp_adsmbl(account_number)
            create unique hg index idx1 on temp_noconsent(account_number)

            update temp_PanBal_weekly_sample as bas
               set adsmbl = case when con.account_number is null then 'Adsmartable consent'
                                                                 else 'Adsmartable non-consent'
                            end
              from temp_adsmbl as ads
                   left join temp_noconsent as con on con.account_number = ads.account_number
             where bas.account_number = ads.account_number

--            update temp_PanBal_weekly_sample as sam
--               set bb_capable = l20_darwin
--              from waterfall_base as wat
--             where sam.account_number = wat.account_number
--               and l07_prod_latest_dtv        = 1
--               and l08_country                = 1
--               and l10_surname                = 1
--               and l11_standard_accounts      = 1
--               and l24_last_callback_dt       = 1

                -- set unused variables to a default valuefor ROI
            update temp_PanBal_weekly_sample as bas
               set fss = 'Not Defined'
                  ,hhcomp = 'Not Defined'
                  ,mosaic = 'Not Defined'
                  ,onnet = 0
              from cust_single_account_view as sav
             where bas.account_number = sav.account_number
               and pty_country_code = 'IRL'

                -- count boxes for every account
            select account_number
                  ,card_subscriber_id
              into temp_ccs
              from CUST_CARD_SUBSCRIBER_LINK as ccs
             where effective_to_dt = '9999-09-09'
          group by account_number
                  ,card_subscriber_id

            commit
            create hg index hgacc on temp_ccs(account_number)

            select ccs.account_number
                  ,sum(case when cust_active_dtv = 1 then 1 else 0 end) as boxes
              into temp_sky_box_count
              from temp_ccs as ccs
                   left join cust_single_account_view as sav on ccs.account_number = sav.account_number
          group by ccs.account_number

            insert into temp_PanBal_segments_lookup(
                   adsmbl
                  ,region
                  ,hhcomp
                  ,tenure
                  ,package
                  ,mr
                  ,hd
                  ,pvr
                  ,valseg
                  ,mosaic
                  ,fss
                  ,onnet
                  ,skygo
                  ,st
                  ,bb
                  ,bb_capable
                  ,base_accounts
            )
            select adsmbl
                  ,region
                  ,hhcomp
                  ,tenure
                  ,package
                  ,mr
                  ,hd
                  ,pvr
                  ,valseg
                  ,mosaic
                  ,fss
                  ,onnet
                  ,skygo
                  ,st
                  ,bb
                  ,bb_capable
                  ,sum(boxes)
              from temp_PanBal_weekly_sample as sam
                   inner join temp_sky_box_count as sbc on sam.account_number = sbc.account_number
          group by adsmbl
                  ,region
                  ,hhcomp
                  ,tenure
                  ,package
                  ,mr
                  ,hd
                  ,pvr
                  ,valseg
                  ,mosaic
                  ,fss
                  ,onnet
                  ,skygo
                  ,st
                  ,bb
                  ,bb_capable

            commit

                -- need to unnormalise the normalised table, so we can find the combinations that don't exist

            insert into temp_PanBal_segments_lookup_unnormalised(
                   v1
                  ,v2
                  ,v3
                  ,v4
                  ,v5
                  ,v6
                  ,v7
                  ,v8
                  ,v9
                  ,v10
                  ,v11
                  ,v12
                  ,v13
                  ,v14
                  ,v15
                  ,v16
                   )
            select max(case when aggregation_variable = 1 then value else null end)
                  ,max(case when aggregation_variable = 2 then value else null end)
                  ,max(case when aggregation_variable = 3 then value else null end)
                  ,max(case when aggregation_variable = 4 then value else null end)
                  ,max(case when aggregation_variable = 5 then value else null end)
                  ,max(case when aggregation_variable = 6 then value else null end)
                  ,max(case when aggregation_variable = 7 then value else null end)
                  ,max(case when aggregation_variable = 8 then value else null end)
                  ,max(case when aggregation_variable = 9 then value else null end)
                  ,max(case when aggregation_variable = 10 then value else null end)
                  ,max(case when aggregation_variable = 11 then value else null end)
                  ,max(case when aggregation_variable = 12 then value else null end)
                  ,max(case when aggregation_variable = 13 then value else null end)
                  ,max(case when aggregation_variable = 14 then value else null end)
                  ,max(case when aggregation_variable = 15 then value else null end)
                  ,max(case when aggregation_variable = 16 then value else null end)
              from panbal_segments_lookup_normalised
          group by segment_id

                -- update the lookup table with segment id from unnormalised table
                -- db space issue, so have to do this query a bit at a time
               set @counter = 0
             while @counter < (select max(segment_id) from temp_PanBal_segments_lookup_unnormalised) begin
                      update temp_PanBal_segments_lookup as lkp
                         set segment_id = unn.segment_id
                        from temp_PanBal_segments_lookup_unnormalised as unn
                       where v1 = adsmbl
                         and v2 = region
                         and v3 = hhcomp
                         and v4 = tenure
                         and v5 = package
                         and v6 = mr
                         and v7 = hd
                         and v8 = pvr
                         and v9 = valseg
                         and v10 = mosaic
                         and v11 = fss
                         and v12 = onnet
                         and v13 = skygo
                         and v14 = st
                         and v15 = bb
                         and v16 = bb_capable
                         and lkp.segment_id between @counter and @counter + 100000

                         set @counter = @counter +100000
               end

          truncate table PanBal_segment_snapshots
            insert into PanBal_segment_snapshots(account_number
                                                ,segment_id)
            select sam.account_number
                  ,segment_id
              from temp_PanBal_weekly_sample               as sam
                   inner join temp_PanBal_segments_lookup as lkp on sam.adsmbl     = lkp.adsmbl
                                                            and sam.region     = lkp.region
                                                            and sam.hhcomp     = lkp.hhcomp
                                                            and sam.tenure     = lkp.tenure
                                                            and sam.package    = lkp.package
                                                            and sam.mr         = lkp.mr
                                                            and sam.hd         = lkp.hd
                                                            and sam.pvr        = lkp.pvr
                                                            and sam.valseg     = lkp.valseg
                                                            and sam.mosaic     = lkp.mosaic
                                                            and sam.fss        = lkp.fss
                                                            and sam.onnet      = lkp.onnet
                                                            and sam.skygo      = lkp.skygo
                                                            and sam.st         = lkp.st
                                                            and sam.bb         = lkp.bb
                                                            and sam.bb_capable = lkp.bb_capable

                -- find the new segments
            insert into temp_matches
            select bas.segment_id
              from temp_PanBal_segments_lookup as bas
                   inner join temp_PanBal_segments_lookup_unnormalised as unn on v1 = adsmbl
                                                                         and v2 = region
                                                                         and v3 = hhcomp
                                                                         and v4 = tenure
                                                                         and v5 = package
                                                                         and v6 = mr
                                                                         and v7 = hd
                                                                         and v8 = pvr
                                                                         and v9 = valseg
                                                                         and v10 = mosaic
                                                                         and v11 = fss
                                                                         and v12 = onnet
                                                                         and v13 = skygo
                                                                         and v14 = st
                                                                         and v15 = bb
                                                                         and v16 = bb_capable

                -- normalise for new segments
            insert into panbal_segments_lookup_normalised(
                   segment_id
                  ,aggregation_variable
                  ,value
                   )
            select bas.segment_id
                  ,1
                  ,cast(adsmbl as varchar)
              from temp_PanBal_segments_lookup as bas
                   left join temp_matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,2
                  ,region
              from temp_PanBal_segments_lookup as bas
                   left join temp_matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,3
                  ,hhcomp
              from temp_PanBal_segments_lookup as bas
                   left join temp_matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,4
                  ,tenure
              from temp_PanBal_segments_lookup as bas
                   left join temp_matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,5
                  ,package
              from temp_PanBal_segments_lookup as bas
                   left join temp_matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,6
                  ,cast(mr as varchar)
              from temp_PanBal_segments_lookup as bas
                   left join temp_matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,7
                  ,cast(hd as varchar)
              from temp_PanBal_segments_lookup as bas
                   left join temp_matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,8
                  ,cast(pvr as varchar)
              from temp_PanBal_segments_lookup as bas
                   left join temp_matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,9
                  ,valseg
              from temp_PanBal_segments_lookup as bas
                   left join temp_matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,10
                  ,mosaic
              from temp_PanBal_segments_lookup as bas
                   left join temp_matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,11
                  ,fss
              from temp_PanBal_segments_lookup as bas
                   left join temp_matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,12
                  ,cast(onnet as varchar)
              from temp_PanBal_segments_lookup as bas
                   left join temp_matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,13
                  ,cast(skygo as varchar)
              from temp_PanBal_segments_lookup as bas
                   left join temp_matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,14
                  ,cast(st as varchar)
              from temp_PanBal_segments_lookup as bas
                   left join temp_matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,15
                  ,cast(bb as varchar)
              from temp_PanBal_segments_lookup as bas
                   left join temp_matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,16
                  ,cast(bb_capable as varchar)
              from temp_PanBal_segments_lookup as bas
                   left join temp_matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null

            update panbal_segments_lookup_normalised
               set curr = 0

            update panbal_segments_lookup_normalised as bas
               set curr = 1
              from panbal_segment_snapshots as snp
             where bas.segment_id = snp.segment_id

              drop table temp_PanBal_weekly_sample
              drop table temp_PanBal_segments_lookup
              drop table temp_matches
              drop table temp_weekly_sample
              drop table temp_cv_pp
              drop table temp_cv_keys
              drop table temp_roi_region
              drop table temp_scaling_box_level_viewing
              drop table temp_onnet_patch
              drop table temp_bpe
              drop table temp_p2e
              drop table temp_onnet_lookup
              drop table temp_noconsent
              drop table temp_adsmbl
              drop table temp_ccs
              drop table temp_sky_box_count
              drop table temp_PanBal_segments_lookup_unnormalised
     end; --V306_M03_PanBal_Segments
 commit;

 grant execute on V306_M03_PanBal_Segments to vespa_group_low_security;
 commit;



