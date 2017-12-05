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



     update Waterfall_Box_Rules as bas
        set bas.known_prefix       = case when cb.subscriber_id is not null then 1 else 0 end,
            bas.empty_prefix       = case when trim(prefix) = '' or prefix is null then 1 else 0 end
       from vespa_analysts.Waterfall_callback_data  cb
      where bas.subscriber_id = cb.subscriber_id
        and cb.callback_seq = 1
     commit


     declare @varLastCallbackDate date
     select @varLastCallbackDate = max(dt) from Waterfall_callback_data where dt < now()



     update Waterfall_Box_Rules as bas
        set last_callback_dt = 0
     commit

     update Waterfall_Box_Rules as bas
        set last_callback_dt = 1
       from (select
                   subscriber_id
               from Waterfall_SCMS_callback_data
              where Missing_Cbcks < Expected_Cbcks
              group by subscriber_id) gol
      where bas.subscriber_id = gol.subscriber_id
     commit



     select
           a.subscriber_id,
           a.service_instance_id,
           1 as HD_Box
       into #HD_boxes
       from Waterfall_Box_Rules a,
            (select service_instance_id
                   ,rank () over (partition by service_instance_id order by ph_non_subs_link_sk desc) rank
               from sk_prod.cust_Set_top_box
              where x_box_type like '%HD%') b
      where a.service_instance_id = b.service_instance_id
        and b.rank = 1
     commit



     update Waterfall_Box_Rules as bas
        set bas.HD_box = b.HD_Box
       from #HD_boxes b
      where bas.subscriber_id = b.subscriber_id
     commit



     update Waterfall_Box_Rules as bas
        set callback_day_in_range  = 0
     commit

     update Waterfall_Box_Rules as bas
        set callback_day_in_range  = case when cbk_day between 9 and 28 then 1 else 0 end
       from Waterfall_SCMS_callback_data  gol
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
               from Waterfall_Box_Rules
              group by account_number) det
      where base.account_number = det.account_number
     commit



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










     drop table Waterfall_Box_Rules
     grant all on waterfall_base to vespa_group_low_security
end
