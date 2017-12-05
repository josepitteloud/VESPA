--PanBal_segments_lookup - segment to variables
--PanBal_segment_snapshots - account to segment
--panbal_weekly_sample - all account details
--Applies to current panelists (daily plus alt.day) plus waterfall
    drop table PanBal_segments_lookup;
  create table PanBal_segments_lookup(segment_id        int identity primary key
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
);

  create lf index idx2 on PanBal_segments_lookup(region);
  create lf index idx3 on PanBal_segments_lookup(hhcomp);
  create lf index idx4 on PanBal_segments_lookup(tenure);
  create lf index idx5 on PanBal_segments_lookup(package);
  create lf index idx6 on PanBal_segments_lookup(valseg);
  create lf index idx7 on PanBal_segments_lookup(mosaic);
  create lf index idx8 on PanBal_segments_lookup(fss);
   grant select on PanBal_segments_lookup to vespa_group_low_security;

    drop table PanBal_weekly_sample;
  create table PanBal_weekly_sample (account_number    varchar(30)
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
);

  create unique hg index idx1 on PanBal_weekly_sample(account_number)
  create        lf index idx2 on PanBal_weekly_sample(region);
  create        lf index idx3 on PanBal_weekly_sample(hhcomp);
  create        lf index idx4 on PanBal_weekly_sample(tenure);
  create        lf index idx5 on PanBal_weekly_sample(package);
  create        lf index idx6 on PanBal_weekly_sample(valseg);
  create        lf index idx7 on PanBal_weekly_sample(mosaic);
  create        lf index idx8 on PanBal_weekly_sample(fss);

drop table PanBal_segment_snapshots;
  create table PanBal_segment_snapshots(
         account_number varchar(30)
        ,segment_id     int
);

   grant select on PanBal_segment_snapshots to vespa_group_low_security;

  create unique hg index uhacc on PanBal_segment_snapshots(account_number);
    drop procedure PanBal_segmentation;
  create procedure PanBal_segmentation as begin

          truncate table PanBal_weekly_sample
          truncate table PanBal_segments_lookup
          truncate table PanBal_segment_snapshots

           declare @profiling_thursday date
           execute vespa_analysts.Regulars_Get_report_end_date @profiling_thursday output  -- proc returns a Saturday
               set @profiling_thursday = @profiling_thursday - 2                           -- but we want a Thursday

                   /**************** L01: ESTABLISH POPULATION ****************/
                -- Captures all active accounts in cust_subs_hist
            SELECT account_number
                  ,cb_key_household
                  ,cb_key_individual
                  ,current_short_description
                  ,rank() over (PARTITION BY account_number ORDER BY effective_from_dt desc, cb_row_id) AS rank
                  ,convert(bit, 0)  AS uk_standard_account
                  ,convert(VARCHAR(20), NULL) AS isba_tv_region
              INTO #weekly_sample
              FROM sk_prod.cust_subs_hist as csh
             WHERE subscription_sub_type IN ('DTV Primary Viewing')
               AND status_code IN ('AC','AB','PC')
               AND effective_from_dt    <= @profiling_thursday
               AND effective_to_dt      > @profiling_thursday
               AND EFFECTIVE_FROM_DT    IS NOT NULL
               AND cb_key_household     > 0
               AND cb_key_household     IS NOT NULL
               AND cb_key_individual    IS NOT NULL
               AND service_instance_id  IS NOT NULL

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
                   inner join sk_prod.cust_single_account_view AS b ON a.account_number = b.account_number

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
              FROM sk_prod.EXPERIAN_CONSUMERVIEW cv,
                   sk_prod.PLAYPEN_CONSUMERVIEW_PERSON_AND_HOUSEHOLD pp
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

                -- Populate Package & ISBA TV Region
            INSERT INTO PanBal_weekly_sample (
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
                   left join sk_prod.cust_entitlement_lookup AS cel ON fbp.current_short_description = cel.short_description
             WHERE fbp.cb_key_household IS NOT NULL
               AND fbp.cb_key_individual IS NOT NULL

            commit
              drop table #weekly_sample

                -- Experian variables
            UPDATE PanBal_weekly_sample as sws
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
            UPDATE PanBal_weekly_sample as sws set hhcomp ='U' where hhcomp is null
            UPDATE PanBal_weekly_sample as sws set mosaic ='U' where mosaic is null
            UPDATE PanBal_weekly_sample as sws set fss ='U'    where fss is null

              drop table #cv_keys

                -- Tenure
            UPDATE PanBal_weekly_sample as bas
               SET bas.tenure = CASE WHEN datediff(day,acct_first_account_activation_dt,@profiling_thursday) <=  304  THEN 'A) 0-10 Months'
                                     WHEN datediff(day,acct_first_account_activation_dt,@profiling_thursday) <=  730 THEN 'B) 10-24 Months'
                                     WHEN datediff(day,acct_first_account_activation_dt,@profiling_thursday) <= 3652 THEN 'B) 2-10 Years'
                                     WHEN datediff(day,acct_first_account_activation_dt,@profiling_thursday) >  3652 THEN 'C) 10 Years+'
                                     ELSE 'D) Unknown'
                                END
                  ,bas.region = sav.isba_tv_region
              FROM sk_prod.cust_single_account_view sav
             WHERE bas.account_number = sav.account_number

            COMMIT

                -- MR, HD, PVR
            SELECT account_number
                  ,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV Extra Subscription' THEN 1 ELSE 0  END) AS MR
                  ,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV Sky+'               THEN 1 ELSE 0  END) AS pvr
                  ,MAX(CASE  WHEN csh.SUBSCRIPTION_SUB_TYPE ='DTV HD'                 THEN 1 ELSE 0  END) AS HD
              INTO #scaling_box_level_viewing
              FROM sk_prod.cust_subs_hist AS csh
             WHERE effective_FROM_dt <= @profiling_thursday
               AND effective_to_dt    > @profiling_thursday
               AND status_code IN  ('AC','AB','PC')
               AND SUBSCRIPTION_SUB_TYPE IN ('DTV Primary Viewing', 'DTV Sky+', 'DTV Extra Subscription', 'DTV HD')
          GROUP BY account_number

            commit

            update PanBal_weekly_sample as bas
               set bas.hd = blv.hd
                  ,bas.mr = blv.mr
                  ,bas.pvr = blv.pvr
              from #scaling_box_level_viewing as blv
             where bas.account_number = blv.account_number

            update PanBal_weekly_sample as bas
               set valseg = coalesce(seg.value_seg, 'Unknown')
              from sk_prod.VALUE_SEGMENTS_DATA as seg
             where bas.account_number = seg.account_number

                -- coalesce didn't work again, so...
            update PanBal_weekly_sample as bas
               set valseg = 'Unknown' where valseg is null

            update PanBal_weekly_sample as bas
               set skygo = 1
              from sk_prod.SKY_PLAYER_USAGE_DETAIL as spu
             where bas.account_number = spu.account_number
               and activity_dt >= '2011-08-18'

                -- The OnNet goes by postcode, so...
            select account_number
                  ,min(cb_address_postcode) as postcode
                  ,convert(bit, 0) as onnet
              into #onnet_patch
              from sk_prod.cust_single_account_view
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
              FROM sk_prod.BROADBAND_POSTCODE_EXCHANGE
          GROUP BY postcode

            update #bpe
               set postcode = upper(REPLACE( postcode,' ',''))

            commit
            create unique index fake_pk on #bpe (postcode)

                -- 2) Get BB_POSTCODE_TO_EXCHANGE postcodes
            SELECT postcode as postcode, MAX(exchange_id) as exchID
              INTO #p2e
              FROM sk_prod.BB_POSTCODE_TO_EXCHANGE
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
                   INNER JOIN sk_prod.easynet_rollout_data as easy on base.exchange_id = easy.exchange_id
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

            update PanBal_weekly_sample as bas
               set bas.onnet = onn.onnet
              from #onnet_patch as onn
             where bas.account_number = onn.account_number

            update PanBal_weekly_sample as bas
               set bb = 1
              from sk_prod.cust_subs_hist as csh
             where bas.account_number = csh.account_number
               and subscription_sub_type = 'Broadband DSL Line'
               and status_code in ('AC', 'AB', 'PC', 'CF', 'PT')
               and effective_from_dt <= @profiling_thursday
               and effective_to_dt    > @profiling_thursday

            update PanBal_weekly_sample as bas
               set st = 1
              from sk_prod.cust_subs_hist as csh
             where bas.account_number = csh.account_number
               and subscription_sub_type = 'SKY TALK SELECT'
               and status_code in ('A', 'FBP', 'PC', 'RI')
               and effective_from_dt <= @profiling_thursday
               and effective_to_dt    > @profiling_thursday

            select account_number
              into #noconsent
              from sk_prod.cust_single_account_view as sav
             where cust_viewing_data_capture_allowed <> 'Y'
          group by account_number

            select account_number
              into #adsmbl
              from sk_prod.cust_set_top_box
             where active_box_flag = 'Y'
               and (x_pvr_type in ('PVR5', 'PVR6') or (x_pvr_type = 'PVR4' and x_manufacturer in ('Samsung', 'Pace')))
          group by account_number

            commit
            create unique hg index idx1 on #adsmbl(account_number)
            create unique hg index idx1 on #noconsent(account_number)

            update PanBal_weekly_sample as bas
               set adsmbl = case when con.account_number is null then 'Adsmartable consent'
                                                                 else 'Adsmartable non-consent'
                            end
              from #adsmbl as ads
                   left join #noconsent as con on con.account_number = ads.account_number
             where bas.account_number = ads.account_number

            update PanBal_weekly_sample as sam
               set bb_capable = l32_darwin
              from vespa_analysts.waterfall_base as wat
             where sam.account_number = wat.account_number
--               and l07_prod_latest_dtv        = 1
--               and l08_country                = 1
--               and l10_surname                = 1
--               and l11_standard_accounts      = 1
--               and l24_last_callback_dt       = 1

            update PanBal_weekly_sample as sam
               set bb_capable = case when bb_capable = 'Yes' then 'Yes'
                                                                                                                                              when bb_capable is null then 'No'
                                                                                                                                                                                                                                                                                                        else 'No' end

                -- count boxes for every account
            select distinct (ccs.account_number)
                  ,count(distinct card_subscriber_id) as boxes
              into #sky_box_count
              from sk_prod.CUST_CARD_SUBSCRIBER_LINK as ccs
                   inner join sk_prod.cust_single_account_view as sav on ccs.account_number = sav.account_number
             where effective_to_dt = '9999-09-09'
               and cust_active_dtv = 1
          group by ccs.account_number

            insert into panbal_segments_lookup(
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
              from PanBal_weekly_sample as sam
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
            insert into PanBal_segment_snapshots(account_number
                                                ,segment_id)
            select sam.account_number
                  ,segment_id
              from PanBal_weekly_sample              as sam
                   inner join PanBal_segments_lookup as lkp on sam.adsmbl   = lkp.adsmbl
                                                           and sam.region   = lkp.region
                                                           and sam.hhcomp   = lkp.hhcomp
                                                           and sam.tenure   = lkp.tenure
                                                           and sam.package  = lkp.package
                                                           and sam.mr       = lkp.mr
                                                           and sam.hd       = lkp.hd
                                                           and sam.pvr      = lkp.pvr
                                                           and sam.valseg   = lkp.valseg
                                                           and sam.mosaic   = lkp.mosaic
                                                           and sam.fss      = lkp.fss
                                                           and sam.onnet    = lkp.onnet
                                                           and sam.skygo    = lkp.skygo
                                                           and sam.st       = lkp.st
                                                           and sam.bb       = lkp.bb
                                                           and sam.bb_capable = lkp.bb_capable

                -- update panel count for current panel
            select segment_id
                  ,account_number
                  ,min(case when sav.rq is null then case when cbck_rate is null then 1
                                                          else cbck_rate
                                                     end
                            when sav.rq > 1 then 1
                            else sav.rq
                       end * boxes) as rq
              into #rq
              from vespa_analysts.panbal_sav as sav
             where panel in (11, 12)
          group by segment_id
                  ,account_number

            select segment_id
                  ,sum(rq) as cow
              into #current
              from #rq
          group by segment_id

            update PanBal_segments_lookup as bas
               set panel_accounts = cur.cow
              from #current as cur
             where bas.segment_id = cur.segment_id

     end; --PanBal_segmentation
  
---







update vespa_analysts.PanBal_sav as sav
   set segment_id = snp.segment_id
  from panbal_segment_snapshots as snp
  where sav.account_number = snp.account_number
;

  select segment_id
        ,count(1) as accounts
    into #accounts_count
    from panbal_segment_snapshots
group by segment_id
;

  update panbal_segments_lookup as bas
     set base_accounts = accounts
    from #accounts_count as acc
   where bas.segment_id = acc.segment_id
;


grant execute on PanBal_segmentation to vespa_group_low_security;










