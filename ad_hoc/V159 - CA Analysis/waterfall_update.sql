--Copy of the procedure waterfall.
--Obtained by copying the output of the following command
--sp_helptext 'vespa_analysts.waterfall'
create procedure waterfall as begin
     truncate table waterfall_base

     insert into waterfall_base
     select
            sav.account_number
           ,min(case when sav.PROD_LATEST_DTV_STATUS = 'Active'        then 1 else 0 end) as l07_prod_latest_dtv
           ,min(case when sav.PTY_COUNTRY_CODE = 'GBR'                 then 1 else 0 end) as l08_country
           ,min(case when sav.CUST_VIEWING_DATA_CAPTURE_ALLOWED = 'Y'  then 1 else 0 end) as l09_data_capture
           ,min(case when sav.CB_NAME_SURNAME IS NOT NULL and
                          sav.CB_NAME_SURNAME <> ''                    then 1 else 0 end) as l10_surname
           ,min(case when (sav.CUST_PERSON_TYPE IN ('STD','?') OR sav.CUST_PERSON_TYPE IS NULL) and
                           sav.ACCT_TYPE = 'Standard'                  then 1 else 0 end) as l11_standard_accounts
           ,cast(0 as bit)                                                                as l12_TSA_opt_in
           ,cast(0 as bit)                                                                as l13_hibernators
           ,cast(0 as bit)                                                                as l14_not_vespa_panel
           ,cast(0 as bit)                                                                as l15_sky_view_panel
           ,cast(0 as bit)                                                                as l22_known_prefix
           ,cast(0 as bit)                                                                as l23_empty_prefix
           ,cast(0 as bit)                                                                as l24_last_callback_dt
           ,cast(0 as bit)                                                                as l28_HD_box
           ,cast(0 as bit)                                                                as l29_callback_day_in_range
           ,cast(0 as bit)                                                                as l30_ondemand_downloads
           ,cast(0 as decimal(10,1))                                                      as knockout_level
       from sk_prod.CUST_SINGLE_ACCOUNT_VIEW as sav
      where sav.CUST_ACTIVE_DTV = 1
      group by sav.account_number

     commit




       update waterfall_base as bas
          set l09_data_capture = 0
         from vespa_analysts.ConsentIssue_05_Revised_Consent_Info as exc
        where bas.account_number = exc.account_number


       update Waterfall_Base
          set l12_TSA_opt_in = 1
        where ACCOUNT_NUMBER NOT IN
               ( SELECT SAM_REGISTRANT.ACCOUNT_NUMBER
                   FROM sk_prod.SAM_REGISTRANT
                  WHERE SAM_REGISTRANT.TSA_OPT_IN = 'N'
                    AND SAM_REGISTRANT.ACCOUNT_NUMBER IS NOT NULL
               )
        commit



       update Waterfall_Base
          set l13_hibernators = 1
        where ACCOUNT_NUMBER not IN (select account_number from sk_prod.VESPA_PANEL_HIBERNATIONS)
       commit



       update Waterfall_Base
          set l14_not_vespa_panel = 1
        where ACCOUNT_NUMBER NOT IN (SELECT VESPA_PANEL_STATUS.ACCOUNT_NUMBER FROM sk_prod.VESPA_PANEL_STATUS WHERE VESPA_PANEL_STATUS.PANEL_NO in (6, 7, 12))
       commit



       update Waterfall_Base
          set l15_sky_view_panel = 1
        where ACCOUNT_NUMBER NOT IN (SELECT ACCOUNT_NUMBER FROM sk_prod.VESPA_SKY_VIEW_PANEL)
       commit



       update Waterfall_Base as bas
          set l30_ondemand_downloads = 1
         from (select
                     account_number,
                     min(last_modified_dt) as first_dl_date,
                     max(last_modified_dt) as last_dl_date
                 from sk_prod.CUST_ANYTIME_PLUS_DOWNLOADS


                group by account_number) as apd
        where bas.account_number = apd.account_number
       commit




       select account_number
             ,cast(card_subscriber_id as int) as subscriber_id
             ,service_instance_id
         into Waterfall_Box_Rules
         from sk_prod.cust_card_subscriber_link
        where current_flag = 'Y'
        group by account_number, card_subscriber_id, service_instance_id
       commit





     select
           subscriber_id,
           count(*) as cnt
       into #Ambiguous_Sub_Ids
       from Waterfall_Box_Rules
      group by subscriber_id having count(*) > 1
     commit



     delete from Waterfall_Box_Rules
      where account_number in (select
                                     account_number
                                 from #Ambiguous_Sub_Ids a,
                                      Waterfall_Box_Rules b
                                where a.subscriber_id = b.subscriber_id)
     commit







     alter table Waterfall_Box_Rules
       add (known_prefix        bit default 0,
            empty_prefix        bit default 0,
            last_callback_dt    bit default 0,
            HD_box              bit default 0,
            callback_day_in_range bit default 0)

select *
        into #Waterfall_Box_Rules2
        from Waterfall_Box_Rules;

--The following two sets of commands show what I think ought to be done for the prefixes
--The table #Waterfall_Box_Rules2 replaces Waterfall_Box_Rules.
select s.*
       into #waterfall_latest_prefix
       from (select  account_number
                    ,subscriber_id
                    ,prefix
                    ,dt
                    ,callback_seq
                    ,row_number() over(partition by subscriber_id
                                 order by dt desc, callback_seq) as rk
              from vespa_analysts.waterfall_callback_data) s
 where s.rk = 1;

     update #Waterfall_Box_Rules2 as bas
        set bas.known_prefix       = case when cb.subscriber_id is not null then 1 else 0 end,
            bas.empty_prefix       = case when trim(prefix) = '' or prefix is null then 1 else 0 end
       from #waterfall_latest_prefix  cb
      where bas.subscriber_id = cb.subscriber_id
        and cb.callback_seq = 1
     commit

select top 20 * from #waterfall_latest_prefix where account_number is not null and (trim(prefix) = '' or prefix is null)
select top 20 * from vespa_analysts.waterfall_callback_data order by account_number desc, dt desc, callback_seq;
select top 20 * from Waterfall_Box_Rules;
select top 20 * from #Waterfall_Box_Rules2;
select count( *) from Waterfall_Box_Rules;
select count(*) from #Waterfall_Box_Rules2;
select count(distinct subscriber_id) from Waterfall_Box_Rules;
select count(distinct subscriber_id) from #Waterfall_Box_Rules2;


--Join the two tables with 'empty_prefix' and see how they differ
--There appear to be 945,256 occasions when this happens.
select            bas1.account_number
                 ,bas1.subscriber_id
                 ,bas1.service_instance_id      as service_instance_id1
                 ,bas2.service_instance_id      as service_instance_id2
                 ,bas1.known_prefix             as known_prefix1
                 ,bas2.known_prefix             as known_prefix2
                 ,bas1.empty_prefix             as empty_prefix1
                 ,bas2.empty_prefix             as empty_prefix2
                 ,bas1.last_callback_dt         as last_callback_dt1
                 ,bas2.last_callback_dt         as last_callback_dt2
                 ,bas1.HD_box                   as HD_box1
                 ,bas2.HD_box                   as HD_box2
                 ,bas1.callback_day_in_range    as callback_day_in_range1
                 ,bas2.callback_day_in_range    as callback_day_in_range2
        into      #Waterfall_empty_difference
        from      #Waterfall_Box_Rules2 bas1
       inner join Waterfall_Box_Rules bas2
        on  bas1.subscriber_id = bas2.subscriber_id
        where bas1.empty_prefix <> bas2.empty_prefix;

select count(*) from #temp_test;
select count(*) from #temp_test
        where empty1 <> empty2;

select *
        from Waterfall_Box_Rules
        where subscriber_id = 31619754;
select *
        from #Waterfall_Box_Rules2
        where subscriber_id = 31619754;
select top 200 *
        from vespa_analysts.waterfall_callback_data
        where subscriber_id = 31619754
        order by dt desc, callback_seq desc;

select top 20 * from #waterfall_latest_prefix where prefix is not null
select count(distinct subscriber_id) from #waterfall_latest_prefix;
select count(*) from #waterfall_latest_prefix;

select top 20 * from #temp_test  order by subscriber_id desc

     declare @varLastCallbackDate date
     select @varLastCallbackDate = max(dt) from vespa_analysts.Waterfall_callback_data where dt < now()



     update #Waterfall_Box_Rules2 as bas
        set last_callback_dt = 0
     commit

     update #Waterfall_Box_Rules2 as bas
        set last_callback_dt = 1
       from (select
                   subscriber_id
               from vespa_analysts.Waterfall_SCMS_callback_data
              where Missing_Cbcks < Expected_Cbcks
              group by subscriber_id) gol
      where bas.subscriber_id = gol.subscriber_id
     commit



     select
           a.subscriber_id,
           a.service_instance_id,
           1 as HD_Box
       into #HD_boxes
       from #Waterfall_Box_Rules2 a,
            (select service_instance_id
                   ,rank () over (partition by service_instance_id order by ph_non_subs_link_sk desc) rank
               from sk_prod.cust_Set_top_box
              where x_box_type like '%HD%') b
      where a.service_instance_id = b.service_instance_id
        and b.rank = 1
     commit



     update #Waterfall_Box_Rules2 as bas
        set bas.HD_box = b.HD_Box
       from #HD_boxes b
      where bas.subscriber_id = b.subscriber_id
     commit



     update #Waterfall_Box_Rules2 as bas
        set callback_day_in_range  = 0
     commit

     update #Waterfall_Box_Rules2 as bas
        set callback_day_in_range  = case when cbk_day between 9 and 28 then 1 else 0 end
       from vespa_analysts.Waterfall_SCMS_callback_data  gol
      where bas.subscriber_id = gol.subscriber_id
     commit



     update Waterfall_Base base
        set base.l22_known_prefix        = det.known_prefix,
            base.l23_empty_prefix        = det.empty_prefix,
            base.l24_last_callback_dt    = det.last_callback_dt,
            base.l28_HD_box              = det.HD_box,
            base.l29_callback_day_in_range = det.callback_day_in_range
       from (select
                   account_number,
                   min(known_prefix)      as known_prefix,
                   min(empty_prefix)      as empty_prefix,
                   min(last_callback_dt)  as last_callback_dt,
                   max(HD_box)            as HD_box,
                   max(callback_day_in_range) as callback_day_in_range
               from #Waterfall_Box_Rules2
              group by account_number) det
      where base.account_number = det.account_number
     commit

select
                   account_number,
                   min(known_prefix)      as known_prefix,
                   min(empty_prefix)      as empty_prefix,
                   min(last_callback_dt)  as last_callback_dt,
                   max(HD_box)            as HD_box,
                   max(callback_day_in_range) as callback_day_in_range
                   into #temp_accounts_info
               from #Waterfall_Box_Rules2
              group by account_number

     update Waterfall_Base
        set knockout_level  = case
                                when l07_prod_latest_dtv        = 0 then 7
                                when l08_country                = 0 then 8
                                when l09_data_capture           = 0 then 9
                                when l10_surname                = 0 then 10
                                when l11_standard_accounts      = 0 then 11
                                when l12_TSA_opt_in             = 0 then 12
                                when l13_hibernators            = 0 then 13
                                when l14_not_vespa_panel        = 0 then 14
                                when l15_sky_view_panel         = 0 then 15
                                when l22_known_prefix           = 0 then 22
                                when l23_empty_prefix           = 0 then 23
                                when l24_last_callback_dt       = 0 then 24
                                when l28_hd_box                 = 0 then 28
                                  else 9999
                              end
     commit







end
