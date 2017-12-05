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

-------------------------------------------------------------------------------------- [STEP 1]

create or replace procedure V306_M03_PanBal_Segments_adapted
	@weekending	date	= null
 as begin

            create table #PanBal_weekly_sample (account_number    varchar(30)
                  ,cb_key_household  bigint
                  ,cb_key_individual bigint
                  ,adsmbl            varchar(30) default 'Non-Adsmartable'
                  ,region            varchar(40)
                  ,hhcomp            varchar(30)
                  ,tenure            varchar(30)
                  ,package           varchar(30)
                  ,mr                bit         default 0
                  ,hd                bit         default 0
                  ,pvr               bit         default 0
                  ,valseg            varchar(30)
                  ,mosaic            varchar(30)
                  ,fss               varchar(30)
                  ,onnet             bit         default 0
                  ,skygo             bit         default 0
                  ,st                bit         default 0
                  ,bb                bit         default 0
                  ,bb_capable        varchar(8)  default 'No Panel'
                   )
            create unique hg index idx1 on #PanBal_weekly_sample(account_number)
            create        lf index idx2 on #PanBal_weekly_sample(region)
            create        lf index idx3 on #PanBal_weekly_sample(hhcomp)
            create        lf index idx4 on #PanBal_weekly_sample(tenure)
            create        lf index idx5 on #PanBal_weekly_sample(package)
            create        lf index idx6 on #PanBal_weekly_sample(valseg)
            create        lf index idx7 on #PanBal_weekly_sample(mosaic)
            create        lf index idx8 on #PanBal_weekly_sample(fss)

            create table #PanBal_segments_lookup(
                   segment_id        bigint identity primary key
                  ,adsmbl            varchar(30)   default 'Non-Adsmartable'
                  ,region            varchar(40)
                  ,hhcomp            varchar(30)   default 'U'
                  ,tenure            varchar(30)
                  ,package           varchar(30)
                  ,mr                bit           default 0
                  ,hd                bit           default 0
                  ,pvr               bit           default 0
                  ,valseg            varchar(30)   default 'Unknown'
                  ,mosaic            varchar(30)   default 'U'
                  ,fss               varchar(30)   default 'U'
                  ,onnet             bit           default 0
                  ,skygo             bit           default 0
                  ,st                bit           default 0
                  ,bb                bit           default 0
                  ,bb_capable        varchar(8)    default 'No Panel'
                  ,panel_accounts    decimal(10,2) default 0
                  ,base_accounts     int           default 0
                   )
            create lf index lfads on #PanBal_segments_lookup(adsmbl)
            create lf index lfreg on #PanBal_segments_lookup(region)
            create lf index lfhhc on #PanBal_segments_lookup(hhcomp)
            create lf index lften on #PanBal_segments_lookup(tenure)
            create lf index lfpac on #PanBal_segments_lookup(package)
            create lf index lfval on #PanBal_segments_lookup(valseg)
            create lf index lfmos on #PanBal_segments_lookup(mosaic)
            create lf index lffss on #PanBal_segments_lookup(fss)
            create lf index lfbbc on #PanBal_segments_lookup(bb_capable)

            create table #panbal_segments_lookup_unnormalised(
                   segment_id bigint --identity
                  ,v1         varchar(30)   default 'Non-Adsmartable'
                  ,v2         varchar(40)
                  ,v3         varchar(30)   default 'U'
                  ,v4         varchar(30)
                  ,v5         varchar(30)
                  ,v6         bit           default 0
                  ,v7         bit           default 0
                  ,v8         bit           default 0
                  ,v9         varchar(30)   default 'Unknown'
                  ,v10        varchar(30)   default 'U'
                  ,v11        varchar(30)   default 'U'
                  ,v12        bit           default 0
                  ,v13        bit           default 0
                  ,v14        bit           default 0
                  ,v15        bit           default 0
                  ,v16        varchar(8)    default 'No Panel'
                   )

            commit
            create hg index hgseg on #panbal_segments_lookup_unnormalised(segment_id)
            create lf index lfv1 on #panbal_segments_lookup_unnormalised(v1)
            create lf index lfv2 on #panbal_segments_lookup_unnormalised(v2)
            create lf index lfv3 on #panbal_segments_lookup_unnormalised(v3)
            create lf index lfv4 on #panbal_segments_lookup_unnormalised(v4)
            create lf index lfv5 on #panbal_segments_lookup_unnormalised(v5)
            create lf index lfv9 on #panbal_segments_lookup_unnormalised(v9)
            create lf index lfv10 on #panbal_segments_lookup_unnormalised(v10)
            create lf index lfv11 on #panbal_segments_lookup_unnormalised(v11)
            create lf index lfv16 on #panbal_segments_lookup_unnormalised(v16)

           create table #matches(
                  segment_id bigint
                  )

           declare @counter bigint

          truncate table panbal_variables
            insert into panbal_variables(
                   id
                  ,aggregation_variable
                   )
            select 1
                 ,'Adsmartable'
             union
            select 2
                  ,'Region'
             union
            select 3
                  ,'HH Composition'
             union
            select 4
                  ,'Tenure'
             union
            select 5
                  ,'Package'
             union
            select 6
                  ,'Multi-Room'
             union
            select 7
                  ,'HD'
             union
            select 8
                  ,'PVR'
             union
            select 9
                  ,'Value Segment'
             union
            select 10
                  ,'Mosaic'
             union
            select 11
                  ,'Financial Stress'
             union
            select 12
                  ,'On-Net'
             union
            select 13
                  ,'SkyGo'
             union
            select 14
                  ,'Sky-Talk'
             union
            select 15
                  ,'Broadband'
--             union
--            select 16
--                  ,'bb_capable'

			declare @profiling_thursday date
			--execute vespa_analysts.Regulars_Get_report_end_date @profiling_thursday output  -- proc returns a Saturday
            set @profiling_thursday = @weekending - 2                           -- but we want a Thursday

                   /**************** L01: ESTABLISH POPULATION ****************/
                -- Captures all active accounts in cust_subs_hist
				
				message now() to client
			Message 'Creating #weekly_sample' to client
				
            SELECT account_number
                  ,cb_key_household
                  ,cb_key_individual
                  ,current_short_description
                  ,rank() over (PARTITION BY account_number ORDER BY effective_from_dt desc, cb_row_id) AS rank
                  ,convert(bit, 0)  AS uk_standard_account
                  ,convert(VARCHAR(20), NULL) AS isba_tv_region
              INTO #weekly_sample
              FROM /*sk_prod.*/cust_subs_hist as csh
             WHERE subscription_sub_type IN ('DTV Primary Viewing')
               AND status_code IN ('AC','AB','PC')
               AND effective_from_dt    <= @profiling_thursday
               AND effective_to_dt      > @profiling_thursday
               AND EFFECTIVE_FROM_DT    IS NOT NULL
               AND cb_key_household     > 0
               AND cb_key_household     IS NOT NULL
               AND cb_key_individual    IS NOT NULL
               AND service_instance_id  IS NOT NULL

			   message now() to client
			Message '#weekly_sample DONE' to client
			   
			   message now() to client
			Message @@rowcount to client
			   
                -- De-dupes accounts
            COMMIT
            DELETE FROM #weekly_sample WHERE rank > 1

            COMMIT
            CREATE UNIQUE hg INDEX uhacc ON #weekly_sample (account_number)
            CREATE        lf INDEX lfcur ON #weekly_sample (current_short_description)

                -- Take out ROIs (Republic of Ireland) and non-standard accounts as these are not currently in the scope of Vespa
            UPDATE #weekly_sample
               SET uk_standard_account = CASE WHEN b.acct_type='Standard' AND b.account_number <>'?' AND b.pty_country_code ='GBR' THEN 1
                                              ELSE 0
                                         END
                  ,isba_tv_region      = b.isba_tv_region
                  ,cb_key_individual   = b.cb_key_individual
              FROM #weekly_sample AS a
                   inner join /*sk_prod.*/cust_single_account_view AS b ON a.account_number = b.account_number

            COMMIT
            DELETE FROM #weekly_sample WHERE uk_standard_account = 0


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
              INTO #cv_pp
              FROM /*sk_prod.*/EXPERIAN_CONSUMERVIEW cv,
                   /*sk_prod.*/PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD pp
             WHERE cv.exp_cb_key_db_individual = pp.exp_cb_key_db_individual
               AND cv.cb_key_individual is not null
          GROUP BY cv.cb_key_household
                  ,cv.cb_key_family
                  ,cv.cb_key_individual

            COMMIT
            CREATE LF INDEX idx1 on #cv_pp(p_head_of_household)
            CREATE HG INDEX idx2 on #cv_pp(cb_key_family)
            CREATE HG INDEX idx3 on #cv_pp(cb_key_individual)

            SELECT cb_key_individual
                  ,cb_row_id
                  ,rank() over(partition by cb_key_family     ORDER BY p_head_of_household desc,  cb_row_id desc) as rank_fam
                  ,rank() over(partition by cb_key_individual ORDER BY p_head_of_household desc,  cb_row_id desc) as rank_ind
                  ,h_household_composition
                  ,mosaic
                  ,fss
              INTO #cv_keys
              FROM #cv_pp
             WHERE cb_key_individual IS not NULL
               AND cb_key_individual <> 0

            commit
            DELETE FROM #cv_keys WHERE rank_fam != 1 AND rank_ind != 1

            commit
            CREATE INDEX index_ac on #cv_keys (cb_key_individual)
			
			message now() to client
			Message 'Creating #PanBal_weekly_sample ' to client
			
			
			
                -- Populate Package & ISBA TV Region
            INSERT INTO #PanBal_weekly_sample (
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
              FROM #weekly_sample AS fbp
                   left join /*sk_prod.*/cust_entitlement_lookup AS cel ON fbp.current_short_description = cel.short_description
             WHERE fbp.cb_key_household IS NOT NULL
               AND fbp.cb_key_individual IS NOT NULL

			   message now() to client
			Message '#PanBal_weekly_sample DONE' to client
			   
			   message now() to client
			Message @@rowcount to client
			   
			   
            commit
              drop table #weekly_sample

                -- Experian variables
            UPDATE #PanBal_weekly_sample as sws
               SET sws.hhcomp = case when cv.h_household_composition in ('00', '01', '02', '03', '09', '10')         then 'A'
                                     when cv.h_household_composition in ('04', '05')                                 then 'B'
                                     when cv.h_household_composition in ('06', '07', '08', '11')                     then 'C'
                                     else                                                                                 'D'
                                end
                  ,fss    = cv.fss
                  ,mosaic = cv.mosaic
              FROM #cv_keys AS cv
             where sws.cb_key_individual = cv.cb_key_individual

                -- coalesce didn't work, so...
            UPDATE #PanBal_weekly_sample as sws set hhcomp ='U' where hhcomp is null
            UPDATE #PanBal_weekly_sample as sws set mosaic ='U' where mosaic is null
            UPDATE #PanBal_weekly_sample as sws set fss ='U'    where fss is null

              drop table #cv_keys

                -- Tenure
            UPDATE #PanBal_weekly_sample as bas
               SET bas.tenure = CASE WHEN datediff(day,acct_first_account_activation_dt,@profiling_thursday) <=  304  THEN 'A) 0-10 Months'
                                     WHEN datediff(day,acct_first_account_activation_dt,@profiling_thursday) <=  730 THEN 'B) 10-24 Months'
                                     WHEN datediff(day,acct_first_account_activation_dt,@profiling_thursday) <= 3652 THEN 'B) 2-10 Years'
                                     WHEN datediff(day,acct_first_account_activation_dt,@profiling_thursday) >  3652 THEN 'C) 10 Years+'
                                     ELSE 'D) Unknown'
                                END
                  ,bas.region = sav.isba_tv_region
              FROM /*sk_prod.*/cust_single_account_view sav
             WHERE bas.account_number = sav.account_number

            COMMIT

                -- MR, HD, PVR
            SELECT account_number
                  ,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV Extra Subscription' THEN 1 ELSE 0  END) AS MR
                  ,1 AS pvr
                  ,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV HD'                 THEN 1 ELSE 0  END) AS HD
              INTO #scaling_box_level_viewing
              FROM /*sk_prod.*/cust_subs_hist AS csh
             WHERE effective_FROM_dt <= @profiling_thursday
               AND effective_to_dt    > @profiling_thursday
               AND status_code IN  ('AC','AB','PC')
               AND SUBSCRIPTION_SUB_TYPE IN ('DTV Primary Viewing', 'DTV Sky+', 'DTV Extra Subscription', 'DTV HD')
          GROUP BY account_number

            commit

            update #PanBal_weekly_sample as bas
               set bas.hd = blv.hd
                  ,bas.mr = blv.mr
                  ,bas.pvr = blv.pvr
              from #scaling_box_level_viewing as blv
             where bas.account_number = blv.account_number

            update #PanBal_weekly_sample as bas
               set valseg = coalesce(seg.value_seg, 'Unknown')
              from /*sk_prod.*/VALUE_SEGMENTS_DATA as seg
             where bas.account_number = seg.account_number

                -- coalesce didn't work again, so...
            update #PanBal_weekly_sample as bas
               set valseg = 'Unknown' where valseg is null

            update #PanBal_weekly_sample as bas
               set skygo = 1
              from /*sk_prod.*/SKY_PLAYER_USAGE_DETAIL as spu
             where bas.account_number = spu.account_number
               and activity_dt >= '2011-08-18'
			   and activity_dt <= @profiling_thursday
                -- this query takes 10 mins

                -- The OnNet goes by postcode, so...
            select account_number
                  ,min(cb_address_postcode) as postcode
                  ,convert(bit, 0) as onnet
              into #onnet_patch
              from /*sk_prod.*/cust_single_account_view
             where cust_active_dtv = 1
          group by account_number

            update #onnet_patch
               set postcode = upper(REPLACE(postcode,' ',''))

            commit
            create unique hg index idx1 on #onnet_patch (account_number)
            create        index joinsy  on #onnet_patch (postcode)

                -- 1) Get BROADBAND_POSTCODE_EXCHANGE postcodes
            SELECT cb_address_postcode as postcode, MAX(mdfcode) as exchID
              INTO #bpe
              FROM /*sk_prod.*/BROADBAND_POSTCODE_EXCHANGE
          GROUP BY postcode

            update #bpe
               set postcode = upper(REPLACE( postcode,' ',''))

            commit
            create unique index fake_pk on #bpe (postcode)

                -- 2) Get BB_POSTCODE_TO_EXCHANGE postcodes
            SELECT postcode as postcode, MAX(exchange_id) as exchID
              INTO #p2e
              FROM /*sk_prod.*/BB_POSTCODE_TO_EXCHANGE
          GROUP BY postcode

            update #p2e
               set postcode = upper(REPLACE( postcode,' ',''))

            commit
            create unique index fake_pk on #p2e (postcode)

                -- 3) Combine postcode lists taking BB_POSTCODE_TO_EXCHANGE exchange_id's where possible
            SELECT COALESCE(#p2e.postcode, #bpe.postcode) AS postcode
                  ,COALESCE(#p2e.exchID, #bpe.exchID) as exchange_id
                  ,'OFFNET' as exchange
              INTO #onnet_lookup
              FROM #bpe FULL JOIN #p2e ON #bpe.postcode = #p2e.postcode

            commit
            create unique index fake_pk on #onnet_lookup (postcode)

                -- 4) Update with latest Easynet exchange information
            UPDATE #onnet_lookup
               SET exchange = 'ONNET'
              FROM #onnet_lookup AS base
                   INNER JOIN /*sk_prod.*/easynet_rollout_data as easy on base.exchange_id = easy.exchange_id
             WHERE easy.exchange_status = 'ONNET'

                -- 5) Flag your base table with onnet exchange data. Note that this uses a postcode field with
                --   spaces removed so your table will either need to have a similar filed or use a REPLACE
                --   function in the join
            UPDATE #onnet_patch
               SET onnet = CASE WHEN tgt.exchange = 'ONNET'
                                THEN 1
                                ELSE 0
                           END
              FROM #onnet_patch AS bas
                   INNER JOIN #onnet_lookup AS tgt on bas.postcode = tgt.postcode

            commit

            update #PanBal_weekly_sample as bas
               set bas.onnet = onn.onnet
              from #onnet_patch as onn
             where bas.account_number = onn.account_number

            update #PanBal_weekly_sample as bas
               set bb = 1
              from /*sk_prod.*/cust_subs_hist as csh
             where bas.account_number = csh.account_number
               and subscription_sub_type = 'Broadband DSL Line'
               and status_code in ('AC', 'AB', 'PC', 'CF', 'PT')
               and effective_from_dt <= @profiling_thursday
               and effective_to_dt    > @profiling_thursday

            update #PanBal_weekly_sample as bas
               set st = 1
              from /*sk_prod.*/cust_subs_hist as csh
             where bas.account_number = csh.account_number
               and subscription_sub_type = 'SKY TALK SELECT'
               and status_code in ('A', 'FBP', 'PC', 'RI')
               and effective_from_dt <= @profiling_thursday
               and effective_to_dt    > @profiling_thursday

            select account_number
              into #noconsent
              from /*sk_prod.*/cust_single_account_view as sav
             where cust_viewing_data_capture_allowed <> 'Y'
          group by account_number

            select account_number
              into #adsmbl
              from /*sk_prod.*/cust_set_top_box
             where active_box_flag = 'Y'
               and (x_pvr_type in ('PVR5', 'PVR6') and x_manufacturer not in ('Samsung'))
           group by account_number

            commit
            create unique hg index idx1 on #adsmbl(account_number)
            create unique hg index idx1 on #noconsent(account_number)

            update #PanBal_weekly_sample as bas
               set adsmbl = case when con.account_number is null then 'Adsmartable consent'
                                                                 else 'Adsmartable non-consent'
                            end
              from #adsmbl as ads
                   left join #noconsent as con on con.account_number = ads.account_number
             where bas.account_number = ads.account_number

--            update #PanBal_weekly_sample as sam
--               set bb_capable = l20_darwin
--              from waterfall_base as wat
--             where sam.account_number = wat.account_number
--               and l07_prod_latest_dtv        = 1
--               and l08_country                = 1
--               and l10_surname                = 1
--               and l11_standard_accounts      = 1
--               and l24_last_callback_dt       = 1

                -- count boxes for every account
            select distinct (ccs.account_number)
                  ,count(distinct card_subscriber_id) as boxes
              into #sky_box_count
              from /*sk_prod.*/CUST_CARD_SUBSCRIBER_LINK as ccs
                   inner join /*sk_prod.*/cust_single_account_view as sav on ccs.account_number = sav.account_number
            where ccs.effective_from_dt<=@profiling_thursday and ccs.effective_to_dt>@profiling_thursday
 --            where effective_to_dt = '9999-09-09'
 --              and cust_active_dtv = 1
          group by ccs.account_number

            insert into #panbal_segments_lookup(
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
              from #PanBal_weekly_sample as sam
                   inner join #sky_box_count as sbc on sam.account_number = sbc.account_number
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
               --set temporary option identity_insert = ''
               --set temporary option identity_insert = '#panbal_segments_lookup_unnormalised'

            insert into #panbal_segments_lookup_unnormalised(
                   segment_id
                  ,v1
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
            select segment_id
                  ,max(case when aggregation_variable = 1 then value else null end)
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
              from panbal_segments_lookup_normalised -- [STATIC TABLE]
          group by segment_id

               set temporary option identity_insert = ''
               set temporary option identity_insert = '#panbal_segments_lookup'

                -- update with segment id from unnormalised table
                -- db space issue, so have to do this query a bit at a time
               set @counter = 0
             while @counter < (select max(segment_id) from #panbal_segments_lookup_unnormalised) begin
                      update #panbal_segments_lookup as lkp
                         set segment_id = unn.segment_id
                        from #panbal_segments_lookup_unnormalised as unn
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

			message now() to client
			Message 'Populating PanBal_segment_snapshots' to client
			
          truncate table PanBal_segment_snapshots
            insert into PanBal_segment_snapshots(account_number
                                                ,segment_id)
            select sam.account_number
                  ,segment_id
              from #PanBal_weekly_sample               as sam
                   inner join #PanBal_segments_lookup as lkp on sam.adsmbl     = lkp.adsmbl
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
			commit
			
			message now() to client
			Message 'Populating PanBal_segment_snapshots DONE' to client
			message @@Rowcount to client

                -- find the new segments
            insert into #matches
            select bas.segment_id
              from #panbal_segments_lookup as bas
                   inner join #panbal_segments_lookup_unnormalised as unn on v1 = adsmbl
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
				
			--truncate table panbal_segments_lookup_normalised
			--commit
			
            insert into panbal_segments_lookup_normalised(
                   segment_id
                  ,aggregation_variable
                  ,value
                   )
            select bas.segment_id
                  ,1
                  ,cast(adsmbl as varchar)
              from #panbal_segments_lookup as bas
                   left join #matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,2
                  ,region
              from #panbal_segments_lookup as bas
                   left join #matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,3
                  ,hhcomp
              from #panbal_segments_lookup as bas
                   left join #matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,4
                  ,tenure
              from #panbal_segments_lookup as bas
                   left join #matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,5
                  ,package
              from #panbal_segments_lookup as bas
                   left join #matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,6
                  ,cast(mr as varchar)
              from #panbal_segments_lookup as bas
                   left join #matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,7
                  ,cast(hd as varchar)
              from #panbal_segments_lookup as bas
                   left join #matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,8
                  ,cast(pvr as varchar)
              from #panbal_segments_lookup as bas
                   left join #matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,9
                  ,valseg
              from #panbal_segments_lookup as bas
                   left join #matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,10
                  ,mosaic
              from #panbal_segments_lookup as bas
                   left join #matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,11
                  ,fss
              from #panbal_segments_lookup as bas
                   left join #matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,12
                  ,cast(onnet as varchar)
              from #panbal_segments_lookup as bas
                   left join #matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,13
                  ,cast(skygo as varchar)
              from #panbal_segments_lookup as bas
                   left join #matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,14
                  ,cast(st as varchar)
              from #panbal_segments_lookup as bas
                   left join #matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,15
                  ,cast(bb as varchar)
              from #panbal_segments_lookup as bas
                   left join #matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null
             union
            select bas.segment_id
                  ,16
                  ,cast(bb_capable as varchar)
              from #panbal_segments_lookup as bas
                   left join #matches as mat on bas.segment_id = mat.segment_id
             where mat.segment_id is null

			commit
			 
            update panbal_segments_lookup_normalised
               set curr = 0

            update panbal_segments_lookup_normalised as bas
               set curr = 1
              from panbal_segment_snapshots as snp
             where bas.segment_id = snp.segment_id

     end --V306_M03_PanBal_Segments
 commit

grant execute on V306_M03_PanBal_Segments_adapted to vespa_group_low_security
commit


-------------------------------------------------------------------------------------- [STEP 2]
/*

if object_id('panbal_segments_lookup_normalised') is not null drop table panbal_segments_lookup_normalised

create table panbal_segments_lookup_normalised(
	segment_id          	bigint
	,aggregation_variable   tinyint
	,value                  varchar(40)
	,curr                   bit default 0
)

insert	into panbal_segments_lookup_normalised
select  *
from    vespa_analysts.panbal_segments_lookup_normalised
commit

create hg index hg1 on panbal_segments_lookup_normalised(segment_id)
create hg index hg2 on panbal_segments_lookup_normalised(aggregation_variable)
create hg index hg3 on panbal_segments_lookup_normalised(value)
grant select on panbal_segments_lookup_normalised to vespa_group_low_security
commit


if object_id('PanBal_segment_snapshots') is not null drop table PanBal_segment_snapshots

create table PanBal_segment_snapshots(
	   account_number varchar(30)
	  ,segment_id     int
)

grant select on PanBal_segment_snapshots to vespa_group_low_security
create unique hg index uhacc on PanBal_segment_snapshots(account_number)
commit

if object_id('panbal_variables') is not null drop table panbal_variables
            
create table panbal_variables(
	   id					int
	  ,aggregation_variable	varchar(30)
)

insert	into  panbal_variables
select	*
from	vespa_analysts.panbal_variables
commit

grant select on panbal_variables to vespa_group_low_security
create lf index lfid1 on panbal_variables(id)
commit
*/