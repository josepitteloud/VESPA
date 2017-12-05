  create variable @Reference integer;
     set @Reference = 201311;
  create variable @Sample_1_EndString varchar(4);

--
   while @Reference <= 201410 begin
                if @Reference = 201313 begin
                   set @Reference = 201401
               end
              if @Reference = 201311 begin set @Sample_1_EndString = '47' end
              if @Reference = 201312 begin set @Sample_1_EndString = '48' end
              if @Reference = 201401 begin set @Sample_1_EndString = '14' end
              if @Reference = 201402 begin set @Sample_1_EndString = '64' end
              if @Reference = 201403 begin set @Sample_1_EndString = '78' end
              if @Reference = 201404 begin set @Sample_1_EndString = '70' end
              if @Reference = 201405 begin set @Sample_1_EndString = '05' end
              if @Reference = 201406 begin set @Sample_1_EndString = '63' end
              if @Reference = 201407 begin set @Sample_1_EndString = '42' end
              if @Reference = 201408 begin set @Sample_1_EndString = '11' end
              if @Reference = 201409 begin set @Sample_1_EndString = '89' end
              if @Reference = 201410 begin set @Sample_1_EndString = '01' end

            select account_number
                  ,event_dt as effective_from_dt
                  ,typeofevent as status_code
              into #table_churn_history1
              from citeam.View_CUST_CHURN_HIST

                -- select
            select *
              into #table_churn_history2
              from #table_churn_history1
             where account_number like '%' || @Sample_1_EndString

            select *
                  ,@Reference as [Reference3]
              into #table_churn_history3
              from #table_churn_history2

            select *
              into #table_churn_history4
              from #table_churn_history3 his
                   inner join SourceDates as sds on his.[Reference3] = sds.[Reference]

            select *
              into #table_churn_history_output
              from #table_churn_history4
             where effective_from_dt <= Snapshot_Date
               and effective_from_dt >[2_Years_Prior]
          ---------------------------------------------------------------------------------------------------------------------------------------------------------
            select account_number
                 ,event_dt
                 ,TypeOfEvent
             into #TA_call_history1
             from citeam.View_CUST_CALLS_HIST
            where TypeOfEvent = 'TA'

                -- select
            select *
              into #TA_call_history2
              from #TA_call_history1
             where account_number like '%' || @Sample_1_EndString

            select *
                  ,@Reference as [Reference3]
              into #TA_call_history3
              from #TA_call_history2

            select *
              into #TA_call_history4
              from #TA_call_history3 as his
                   inner join SourceDates as sds on his.[Reference3] = sds.[Reference]

            select *
              into #TA_call_history_output
              from #TA_call_history4
             where event_dt <= Snapshot_Date and event_dt > [2_Years_Prior]
          ---------------------------------------------------------------------------------------------------------------------------------------------------------
                -- datamart branch
           execute ('select *
              into #data_mart1
              from ' || case when @reference <= 201409 then 'yarlagaddar'
                             when @reference = 201410 then 'hutchij'
                             else 'sharmaa' end || '.View_attachments_' || @reference || '
             where Status_Code = ''AC''')

            select *
                  ,@Reference as [Reference2]
              into #data_mart2
              from #data_mart1

                -- merge
            select *
              into #data_mart3
              from #data_mart2 as dm2
                   inner join SourceDates sds on dm2.[Reference2] = sds.[Reference]

                -- select
            select *
              into #data_mart4
              from #data_mart3
             where account_number like '%' || @Sample_1_EndString

                -- filter
            select [Reference]
                  ,Attachments_Table
                  ,Account_Numbers_ending_in
                  ,Snapshot_Date
                  ,[2_Years_Prior]
                  ,[1_Year_Prior]
                  ,[10_Months_Prior]
                  ,[9_Months_Prior]
                  ,[6_Months_Prior]
                  ,[3_Months_Prior]
                  ,[1_Month_Prior]
                  ,[1_Month_Future]
                  ,[2_Months_Future]
                  ,[3_Months_Future]
                  ,[4_Months_Future]
                  ,[5_Months_Future]
                  ,[6_Months_Future]
                  ,MonthYear
                  ,Observation_dt
                  ,account_number
                  ,cb_key_household
                  ,birth_dt
                  ,affluence
                  ,life_stage
                  ,h_age_coarse
                  ,email_mkt_optin
                  ,txt_Mkt_OptIn
                  ,mail_Mkt_OptIn
                  ,tel_Mkt_OptIn
                  ,any_Mkt_OptIn
                  ,CQM_Score
                  ,DTV
                  ,MR
                  ,ThreeDTV
                  ,HDTV
                  ,SkyProtect
                  ,SkyGoExtra
                  ,SkyPlus
                  ,sav_acct_first_act_date
                  ,dtv_first_act_date
                  ,dtv_latest_act_date
                  ,SkyGo
                  ,ODConnected
                  ,SBO
                  ,SkyStore
                  ,SkyGo_First_login_date
                  ,SkyGo_last_login_date
                  ,total_skygo_logins
                  ,OD_first_dl_date
                  ,OD_last_dl_date
                  ,total_OD_dl_Counts
                  ,skygo_distinct_activitydate_last90days
                  ,skygo_distinct_activitydate_last180days
                  ,skygo_distinct_activitydate_last270days
                  ,skygo_distinct_activitydate_last360days
                  ,total_SkyGo_logins_last90days
                  ,total_SkyGo_logins_last180days
                  ,total_SkyGo_logins_last270days
                  ,total_SkyGo_logins_last360days
                  ,od_distinct_activitydate_last90days
                  ,od_distinct_activitydate_last180days
                  ,od_distinct_activitydate_last270days
                  ,od_distinct_activitydate_last360days
                  ,total_OD_dl_Counts_last90days
                  ,total_OD_dl_Counts_last180days
                  ,total_OD_dl_Counts_last270days
                  ,total_OD_dl_Counts_last360days
                  ,BroadBand
                  ,SABB
                  ,SkyTalk
                  ,WLR
                  ,BB_type
                  ,SkyTalk_type
                  ,Sports
                  ,Movies
                  ,TopTier
                  ,ESPN
                  ,Entertainment_Extra
                  ,BB_Contract
                  ,HDTV_Contract
                  ,Reinstate
                  ,Reinstate_lastdate
                  ,Reinstate_count
                  ,Movies_upgrade_last_30days
                  ,Movies_upgrade_last_60days
                  ,Movies_upgrade_last_90days
                  ,Movies_upgrade_last_180days
                  ,Movies_downgrade_last_30days
                  ,Movies_downgrade_last_60days
                  ,Movies_downgrade_last_90days
                  ,Movies_downgrade_last_180days
                  ,Sports_upgrade_last_30days
                  ,Sports_upgrade_last_60days
                  ,Sports_upgrade_last_90days
                  ,Sports_upgrade_last_180days
                  ,Sports_downgrade_last_30days
                  ,Sports_downgrade_last_60days
                  ,Sports_downgrade_last_90days
                  ,Sports_downgrade_last_180days
                  ,h_fss_v3_group_Description
                  ,h_age_coarse_Description
                  ,h_income_band_v2_Description
                  ,h_mosaic_uk_group_Description
                  ,kids_Description
                  ,BB_type_Description
                  ,SkyTalk_type_Description
                  ,Product_Holding
                  ,CVS_segment
                  ,package_detail_desc
                  ,package_desc
                  ,hdtv_premium
                  ,hdtv_sub_type
              into #data_mart5
              from #data_mart4

                -- end of datamart branch
                -- restructure
            select *
                  ,case when status_code='AB' then effective_from_dt else cast(NULL as date) end as status_code_AB_effective_from_dt
                  ,case when status_code='PO' then effective_from_dt else cast(NULL as date) end as status_code_PO_effective_from_dt
                  ,case when status_code='SC' then effective_from_dt else cast(NULL as date) end as status_code_SC_effective_from_dt
              into #data_mart6
              from #table_churn_history_output

                -- aggregate
            select account_number
                  ,max(status_code_AB_effective_from_dt) as status_code_AB_effective_from_dt_Max
                  ,max(status_code_PO_effective_from_dt) as status_code_PO_effective_from_dt_Max
                  ,max(status_code_SC_effective_from_dt) as status_code_SC_effective_from_dt_Max
              into #data_mart7
              from #data_mart6
          group by account_number

              -- flag
            select account_number as account_number8
                  ,status_code_AB_effective_from_dt_Max
                  ,status_code_PO_effective_from_dt_Max
                  ,status_code_SC_effective_from_dt_Max
                  ,case when status_code_AB_effective_from_dt_Max is not NULL then 1 else 0 end as status_code_AB_effective_from_dt_Max_flag
                  ,case when status_code_PO_effective_from_dt_Max is not NULL then 1 else 0 end as status_code_PO_effective_from_dt_Max_flag
                  ,case when status_code_SC_effective_from_dt_Max is not NULL then 1 else 0 end as status_code_SC_effective_from_dt_Max_flag
              into #data_mart8
              from #data_mart7

                -- merge with the datamart branch
            select *
              into #data_mart9
              from #data_mart5 as dm5
                   left join #data_mart8 as dm8 on dm5.account_number = dm8.account_number8

          --ta_call_history branch
                -- restructure
            select *
                  ,case when typeofevent='TA' then event_dt else cast(NULL as date) end as type_of_event_TA_event_dt
              into #data_mart10
              from #TA_call_history_output

                -- aggregate
            select account_number
                  ,max(type_of_event_TA_event_dt) as type_of_event_TA_event_dt_Max
              into #data_mart11
              from #data_mart10
          group by account_number

                -- flag
            select account_number as account_number12
                  ,type_of_event_TA_event_dt_Max
                  ,case when type_of_event_TA_event_dt_Max is not NULL then 1 else 0 end as type_of_event_TA_event_dt_Max_flag
              into #data_mart12
              from #data_mart11
                -- end of branch TA_call_history

                -- merge of branches TA_call_history and churn history
            select *
              into #data_mart_output
              from #data_mart9 as dm9
                   left join #data_mart12 as d12 on dm9.account_number = d12.account_number12

          ---------------------------------------------------------------------------------------------------------------------------------------------------------
                -- select
            select account_number
                  ,event_dt
                  ,TypeOfEvent
              into #futureTA1
              from citeam.View_CUST_CALLS_HIST
             where TypeOfEvent = 'TA'

                -- reference
            select *
              into #futureTA2
              from #futureTA1
             where account_number like '%' || @Sample_1_EndString

                -- merge
            select *
                  ,@Reference as [Reference3]
              into #futureTA3
              from #futureTA2

                -- select
            select *
              into #futureTA4
              from #futureTA3 fu3
                   inner join SourceDates as sds on fu3.[Reference3] = sds.[Reference]

            select *
              into #futureTA5
              from #futureTA4
             where event_dt > Snapshot_Date and event_dt <= [3_Months_Future]

                -- restructure
            select *
                  ,case when typeofevent = 'TA' then event_dt else cast(NULL as date) end as type_of_event_TA_event_dt
              into #futureTA6
              from #futureTA5

                -- aggregate
            select account_number
                  ,max(type_of_event_TA_event_dt) as type_of_event_TA_event_dt_Max
              into #futureTA7
              from #futureTA6
          group by account_number

                -- flag
            select *
                  ,case when type_of_event_TA_event_dt_Max is not NULL then 1 else 0 end as type_of_event_TA_event_dt_Max_flag
              into #futureTA8
              from #futureTA7

                -- filter
            select account_number
                  ,Type_Of_Event_TA_event_dt_Max
                  ,Type_Of_Event_TA_event_dt_Max_flag as TA_in_NEXT_3_months_flag
              into #futureTA_output
              from #futureTA8
          ---------------------------------------------------------------------------------------------------------------------------------------------------------
            select account_number
                  ,event_dt
                  ,TypeOfEvent
              into #futureAB1
              from citeam.View_CUST_CHURN_HIST
             where TypeofEvent = 'AB'

                -- select
            select *
              into #futureAB2
              from #futureAB1
             where account_number like '%' || @Sample_1_EndString

            select *
                  ,@Reference as Reference3
              into #futureAB3
              from #futureAB2

            select *
              into #futureAB4
              from #futureAB3 hi3
                   inner join SourceDates as sds on hi3.Reference3 = sds.[Reference]

            select *
              into #futureAB5
              from #futureAB4
             where event_dt > Snapshot_Date
               and event_dt <= [3_Months_Future]

                -- restructure
            select *
                  ,case when typeofevent='AB' then event_dt else cast(NULL as date) end as type_of_event_AB_event_dt
              into #futureAB6
              from #futureAB5

                -- aggregate
            select account_number
                  ,max(type_of_event_AB_event_dt) as type_of_event_AB_event_dt_Max
              into #futureAB7
              from #futureAB6
          group by account_number

                -- flag
            select account_number
                  ,type_of_event_AB_event_dt_Max
                  ,case when type_of_event_AB_event_dt_Max is not NULL then 1 else 0 end as type_of_event_AB_event_dt_Max_flag
              into #futureAB8
              from #futureAB7

                -- filter
            select account_number
                  ,Type_Of_Event_AB_event_dt_Max_flag as AB_in_NEXT_3_months_flag
              into #futureAB_output
              from #futureAB8
          ---------------------------------------------------------------------------------------------------------------------------------------------------------
            select account_number
                  ,event_dt
                  ,TypeOfEvent
              into #futureSC1
              from citeam.View_CUST_CHURN_HIST
             where TypeofEvent = 'SC'

                -- select
            select *
              into #futureSC2
              from #futureSC1
             where account_number like '%' || @Sample_1_EndString

            select *
                  ,@Reference as [Reference3]
              into #futureSC3
              from #futureSC2

            select *
              into #futureSC4
              from #futureSC3 as hi3
                   inner join SourceDates as sds on hi3.Reference3 = sds.[Reference]

            select *
              into #futureSC5
              from #futureSC4
             where event_dt > Snapshot_Date
               and event_dt <= [3_Months_Future]

                -- restructure
            select *
                  ,case when typeofevent='SC' then event_dt else cast(NULL as date) end as type_of_event_SC_event_dt
              into #futureSC6
              from #futureSC5

                -- aggregate
            select account_number
                  ,max(type_of_event_SC_event_dt) as type_of_event_SC_event_dt_Max
              into #futureSC7
              from #futureSC6
          group by account_number

                -- flag
            select account_number
                  ,type_of_event_SC_event_dt_Max
                  ,case when type_of_event_SC_event_dt_Max is not NULL then 1 else 0 end as type_of_event_SC_event_dt_Max_flag
              into #futureSC8
              from #futureSC7

                -- filter
            select account_number
                  ,type_of_event_SC_event_dt_Max_flag as SC_In_Next_4_Months_Flag
              into #futureSC_output
              from #futureSC8
          ---------------------------------------------------------------------------------------------------------------------------------------------------------
            select account_number
                  ,event_dt
                  ,TypeOfEvent
              into #futurePO1
              from citeam.View_CUST_CHURN_HIST
             where TypeofEvent = 'PO'

                -- select
            select *
              into #futurePO2
              from #futurePO1
             where account_number like '%' || @Sample_1_EndString

            select *
                  ,@Reference as [Reference3]
              into #futurePO3
              from #futurePO2

            select *
              into #futurePO4
              from #futurePO3 as hi3
                   inner join SourceDates as sds on hi3.Reference3 = sds.[Reference]

            select *
              into #futurePO5
              from #futurePO4
             where event_dt > Snapshot_Date
               and event_dt <= [3_Months_Future]

                -- restructure
            select *
                  ,case when typeofevent='PO' then event_dt else cast(NULL as date) end as type_of_event_PO_event_dt
              into #futurePO6
              from #futurePO5

                --aggregate
            select account_number
                  ,max(type_of_event_PO_event_dt) as type_of_event_PO_event_dt_Max
              into #futurePO7
              from #futurePO6
          group by account_number

                -- flag
            select account_number
                  ,type_of_event_PO_event_dt_Max
                  ,case when type_of_event_PO_event_dt_Max is not NULL then 1 else 0 end as type_of_event_PO_event_dt_Max_flag
              into #futurePO8
              from #futurePO7

                -- filter
            select account_number
                  ,Type_Of_Event_PO_event_dt_Max_flag as PO_In_Next_4_Months_Flag
              into #futurePO_output
              from #futurePO8
          ---------------------------------------------------------------------------------------------------------------------------------------------------------
            select account_number
                  ,event_dt
                  ,TypeOfEvent
              into #futureTA_4_6_m1
              from citeam.View_CUST_CALLS_HIST
             where TypeOfEvent = 'TA'

                -- select
            select *
              into #futureTA_4_6_m2
              from #futureTA_4_6_m1
             where account_number like '%' || @Sample_1_EndString

            select *
                  ,@Reference as Reference3
              into #futureTA_4_6_m3
              from #futureTA_4_6_m2

            select *
              into #futureTA_4_6_m4
              from #futureTA_4_6_m3 fu3
                   inner join SourceDates as sds on fu3.Reference3 = sds.[Reference]

            select *
              into #futureTA_4_6_m5
              from #futureTA_4_6_m4
             where event_dt > [3_Months_Future]
               and event_dt <= [6_Months_Future]

                -- restructure
            select *
                  ,case when typeofevent='TA' then event_dt else cast(NULL as date) end as type_of_event_TA_event_dt
              into #futureTA_4_6_m6
              from #futureTA_4_6_m5

                -- aggregate
            select account_number
                  ,max(type_of_event_TA_event_dt) as type_of_event_TA_event_dt_Max
              into #futureTA_4_6_m7
              from #futureTA_4_6_m6
          group by account_number

                -- flag
            select account_number
                  ,type_of_event_TA_event_dt_Max
                  ,case when type_of_event_TA_event_dt_Max is not NULL then 1 else 0 end as type_of_event_TA_event_dt_Max_flag
              into #futureTA_4_6_m8
              from #futureTA_4_6_m7

                -- filter
            select account_number
                  ,Type_Of_Event_TA_event_dt_Max_flag as TA_in_3_6_Months_Flag
              into #futureTA_4_6_m_output
              from #futureTA_4_6_m8
          ---------------------------------------------------------------------------------------------------------------------------------------------------------
            select fta.*
                  ,AB_in_NEXT_3_months_flag
              into #future_calling_churn1
              from #futureTA_output as fta
                   full outer join #futureAB_output fab on fta.account_number = fab.account_number

            select fc1.*
                  ,SC_In_Next_4_Months_Flag
              into #future_calling_churn2
              from #future_calling_churn1 as fc1
                   full outer join #futureSC_output fsc on fc1.account_number = fsc.account_number

            select fcc.*
                  ,PO_In_Next_4_Months_Flag
              into #future_calling_churn3
              from #future_calling_churn2 as fcc
                   full outer join #futurePO_output poo on fcc.account_number = poo.account_number

            select fcc.*
                  ,TA_in_3_6_Months_Flag
              into #future_calling_churn4
              from #future_calling_churn3 as fcc
                   full outer join #futureTA_4_6_m_output as tao on fcc.account_number = tao.account_number

select count() from #future_calling_churn3
1314514
select count() from #futureTA_4_6_m_output
600661
select count() from #future_calling_churn3 as a full outer join #futureTA_4_6_m_output as b on a.account_number = b.account_number
1856732
select count() from #future_calling_churn3 as a inner join #futureTA_4_6_m_output as b on a.account_number = b.account_number
58443

          ---------------------------------------------------------------------------------------------------------------------------------------------------------
            select account_number
                  ,event_dt
                  ,TypeOfEvent
              into #TA_calls_in1
              from citeam.View_CUST_CALLS_HIST
             where TypeOfEvent = 'TA'

            select *
                  ,@Reference as Reference3
              into #TA_calls_in2
              from #TA_calls_in1

                -- select
            select *
              into #TA_calls_in3
              from #TA_calls_in2
             where account_number like '%' || @Sample_1_EndString

            select *
              into #TA_calls_in_output
              from #TA_calls_in3 fut3
                   inner join SourceDates as sds on fut3.Reference3=sds.[Reference]
          ---------------------------------------------------------------------------------------------------------------------------------------------------------
              -- first branch
          select *
          into #TA_calls1
          from #TA_calls_in_output
          where
          event_dt > [2_Years_Prior] and event_dt <= Snapshot_Date

          select
          account_number
          ,max(event_dt) as event_dt_Max
          into #TA_calls2
          from #TA_calls1
          group by account_number

          select
          account_number
          ,event_dt_Max as Date_of_Last_TA_Call
          into #TA_calls3
          from #TA_calls2

          -- second branch
          select *
          into #TA_calls4
          from #TA_calls_in_output
          where
          event_dt > [2_Years_Prior] and event_dt <= Snapshot_Date

          select account_number
          ,count() as TA_Calls_Last_2_Years
          into #TA_calls5
          from #TA_calls4
          group by account_number
          -- output of second branch is #TA_calls5

          -- third branch
          select *
          into #TA_calls6
          from #TA_calls_in_output
          where
          event_dt > [1_Year_Prior] and event_dt <= Snapshot_Date

          select account_number
          ,count() as TA_Calls_Last_Year
          into #TA_calls7
          from #TA_calls6
          group by account_number

          -- fourth branch
          select *
          into #TA_calls8
          from #TA_calls_in_output
          where
          event_dt > [9_Months_Prior] and event_dt <= Snapshot_Date

          select account_number
          ,count() as TA_Calls_Last_9_Months
          into #TA_calls9
          from #TA_calls8
          group by account_number

          -- fifth branch
          select *
          into #TA_calls10
          from #TA_calls_in_output
          where
          event_dt > [6_Months_Prior] and event_dt <= Snapshot_Date

          select account_number
          ,count() as TA_Calls_Last_6_Months
          into #TA_calls11
          from #TA_calls10
          group by account_number

          -- sixth branch
          select *
          into #TA_calls12
          from #TA_calls_in_output
          where
          event_dt > [3_Months_Prior] and event_dt <= Snapshot_Date

          select account_number
          ,count() as TA_Calls_Last_3_Months
          into #TA_calls13
          from #TA_calls12
          group by account_number

            select ta3.*
                  ,TA_Calls_Last_2_Years
                  ,TA_Calls_Last_Year
                  ,TA_Calls_Last_9_Months
                  ,TA_Calls_Last_6_Months
                  ,TA_Calls_Last_3_Months
              into #TA_calls_output
              from #TA_calls3 as ta3
                   full outer join #TA_calls5 as ta5 on ta3.account_number = ta5.account_number
                   full outer join #TA_calls7 as ta7 on ta5.account_number = ta7.account_number
                   full outer join #TA_calls9 as ta9 on ta7.account_number = ta9.account_number
                   full outer join #TA_calls11 as t11 on ta9.account_number = t11.account_number
                   full outer join #TA_calls13 as t13 on t11.account_number = t13.account_number

            select account_number
                  ,event_dt
                  ,TypeOfEvent
              into #PAT_calls_in1
              from citeam.View_CUST_CALLS_HIST
             where TypeOfEvent = 'PAT'

            select *
                  ,@Reference as Reference3
              into #PAT_calls_in2
              from #PAT_calls_in1

                -- select
            select *
              into #PAT_calls_in3
              from #PAT_calls_in2
             where account_number like '%' || @Sample_1_EndString

            select *
              into #PAT_calls_in_output
              from #PAT_calls_in3 fut3
                   inner join SourceDates sd on fut3.Reference3=sd.[Reference]
          ---------------------------------------------------------------------------------------------------------------------------------------------------------
                -- first branch
            select *
              into #PAT_calls1
              from #PAT_calls_in_output
             where event_dt > [2_Years_Prior] and event_dt <= Snapshot_Date

          select
          account_number
          ,max(event_dt) as event_dt_Max
          into #PAT_calls2
          from #PAT_calls1
          group by account_number

          select
          account_number
          ,event_dt_Max as Date_of_Last_PAT_Call
          into #PAT_calls3
          from #PAT_calls2

              -- second branch
          select *
          into #PAT_calls4
          from #PAT_calls_in_output
          where event_dt > [2_Years_Prior] and event_dt <= Snapshot_Date

          select account_number
          ,count() as PAT_Calls_Last_2_Years
          into #PAT_calls5
          from #PAT_calls4
          group by account_number

              -- third branch
          select *
          into #PAT_calls6
          from #PAT_calls_in_output
          where
          event_dt > [1_Year_Prior] and event_dt <= Snapshot_Date

          select account_number
          ,count() as PAT_Calls_Last_Year
          into #PAT_calls7
          from #PAT_calls6
          group by account_number

              -- fourth branch
          select *
          into #PAT_calls8
          from #PAT_calls_in_output
          where
          event_dt > [9_Months_Prior] and event_dt <= Snapshot_Date

          select account_number
          ,count() as PAT_Calls_Last_9_Months
          into #PAT_calls9
          from #PAT_calls8
          group by account_number

              -- fifth branch
          select *
          into #PAT_calls10
          from #PAT_calls_in_output
          where
          event_dt > [6_Months_Prior] and event_dt <= Snapshot_Date

            select account_number
                  ,count() as PAT_Calls_Last_6_Months
              into #PAT_calls11
              from #PAT_calls10
          group by account_number

                -- sixth branch
            select *
              into #PAT_calls12
              from #PAT_calls_in_output
             where event_dt > [3_Months_Prior] and event_dt <= Snapshot_Date

            select account_number
                  ,count() as PAT_Calls_Last_3_Months
              into #PAT_calls13
              from #PAT_calls12
          group by account_number

            select pa3.*
                  ,PAT_Calls_Last_2_Years
                  ,PAT_Calls_Last_Year
                  ,PAT_Calls_Last_9_Months
                  ,PAT_Calls_Last_6_Months
                  ,PAT_Calls_Last_3_Months
              into #PAT_calls_output
              from #PAT_calls3 as pa3
                   full outer join #PAT_calls5  as pa5 on pa3.account_number = pa5.account_number
                   full outer join #PAT_calls7  as pa7 on pa3.account_number = pa7.account_number
                   full outer join #PAT_calls9  as pa9 on pa3.account_number = pa9.account_number
                   full outer join #PAT_calls11 as p11 on pa3.account_number = p11.account_number
                   full outer join #PAT_calls13 as p13 on pa3.account_number = p13.account_number

            select account_number
                  ,event_dt
                  ,TypeOfEvent
              into #IC_calls_in1
              from citeam.View_CUST_CALLS_HIST
             where TypeOfEvent = 'IC'

            select *
                  ,@Reference as Reference3
              into #IC_calls_in2
              from #IC_calls_in1

                -- select
            select *
              into #IC_calls_in3
              from #IC_calls_in2
             where account_number like '%' || @Sample_1_EndString

            select *
              into #IC_calls_in_output
              from #IC_calls_in3 fut3
                   inner join SourceDates sd on fut3.Reference3=sd.[Reference]
          ---------------------------------------------------------------------------------------------------------------------------------------------------------
              -- first branch
          select *
          into #IC_calls1
          from #IC_calls_in_output
          where
          event_dt > [2_Years_Prior] and event_dt <= Snapshot_Date

          select
          account_number
          ,max(event_dt) as event_dt_Max
          into #IC_calls2
          from #IC_calls1
          group by account_number

          select
          account_number
          ,event_dt_Max as Date_of_Last_IC_Call
          into #IC_calls3
          from #IC_calls2

              -- second branch
          select *
          into #IC_calls4
          from #IC_calls_in_output
          where
          event_dt > [2_Years_Prior] and event_dt <= Snapshot_Date

          select account_number
          ,count() as IC_Calls_Last_2_Years
          into #IC_calls5
          from #IC_calls4
          group by account_number

              -- third branch
          select *
          into #IC_calls6
          from #IC_calls_in_output
          where
          event_dt > [1_Year_Prior] and event_dt <= Snapshot_Date

          select account_number
          ,count() as IC_Calls_Last_Year
          into #IC_calls7
          from #IC_calls6
          group by account_number

              -- fourth branch
          select *
          into #IC_calls8
          from #IC_calls_in_output
          where
          event_dt > [9_Months_Prior] and event_dt <= Snapshot_Date

          select account_number
          ,count() as IC_Calls_Last_9_Months
          into #IC_calls9
          from #IC_calls8
          group by account_number

              -- fifth branch
          select *
          into #IC_calls10
          from #IC_calls_in_output
          where
          event_dt > [6_Months_Prior] and event_dt <= Snapshot_Date

          select account_number
          ,count() as IC_Calls_Last_6_Months
          into #IC_calls11
          from #IC_calls10
          group by account_number

              -- sixth branch
          select *
          into #IC_calls12
          from #IC_calls_in_output
          where
          event_dt > [3_Months_Prior] and event_dt <= Snapshot_Date

          select account_number
          ,count() as IC_Calls_Last_3_Months
          into #IC_calls13
          from #IC_calls12
          group by account_number

            select ic3.*
                  ,IC_Calls_Last_2_Years
                  ,IC_Calls_Last_Year
                  ,IC_Calls_Last_9_Months
                  ,IC_Calls_Last_6_Months
                  ,IC_Calls_Last_3_Months
              into #IC_calls_output
              from #IC_calls3 as ic3
                   full outer join #IC_calls5  as ic5 on ic3.account_number = ic5.account_number
                   full outer join #IC_calls7  as ic7 on ic3.account_number = ic7.account_number
                   full outer join #IC_calls9  as ic9 on ic3.account_number = ic9.account_number
                   full outer join #IC_calls11 as i11 on ic3.account_number = i11.account_number
                   full outer join #IC_calls13 as i13 on ic3.account_number = i13.account_number

          ---------------------------------------------------------------------------------------------------------------------------------------------------------
            select tao.*
                  ,Date_of_Last_PAT_Call
                  ,PAT_Calls_Last_2_Years
                  ,PAT_Calls_Last_Year
                  ,PAT_Calls_Last_9_Months
                  ,PAT_Calls_Last_6_Months
                  ,PAT_Calls_Last_3_Months
                  ,Date_of_Last_IC_Call
                  ,IC_Calls_Last_2_Years
                  ,IC_Calls_Last_Year
                  ,IC_Calls_Last_9_Months
                  ,IC_Calls_Last_6_Months
                  ,IC_Calls_Last_3_Months
              into #HistoricalCallingBehaviour1
              from #TA_calls_output as tao
                   full outer join #PAT_calls_output as pao on tao.account_number = pao.account_number
                   full outer join #IC_calls_output  as ico on tao.account_number = ico.account_number

            select account_number
                  ,case when IC_Calls_Last_Year      is null then 0 else IC_Calls_Last_Year      end as IC_Calls_Last_Year
                  ,case when IC_Calls_Last_3_Months  is null then 0 else IC_Calls_Last_3_Months  end as IC_Calls_Last_3_Months
                  ,case when IC_Calls_Last_6_Months  is null then 0 else IC_Calls_Last_6_Months  end as IC_Calls_Last_6_Months
                  ,case when IC_Calls_Last_9_Months  is null then 0 else IC_Calls_Last_9_Months  end as IC_Calls_Last_9_Months
                  ,case when IC_Calls_Last_2_Years   is null then 0 else IC_Calls_Last_2_Years   end as IC_Calls_Last_2_Years
                  ,Date_of_Last_IC_Call
                  ,case when PAT_Calls_Last_Year     is null then 0 else PAT_Calls_Last_Year     end as PAT_Calls_Last_Year
                  ,case when PAT_Calls_Last_3_Months is null then 0 else PAT_Calls_Last_3_Months end as PAT_Calls_Last_3_Months
                  ,case when PAT_Calls_Last_6_Months is null then 0 else PAT_Calls_Last_6_Months end as PAT_Calls_Last_6_Months
                  ,case when PAT_Calls_Last_9_Months is null then 0 else PAT_Calls_Last_9_Months end as PAT_Calls_Last_9_Months
                  ,case when PAT_Calls_Last_2_Years  is null then 0 else PAT_Calls_Last_2_Years  end as PAT_Calls_Last_2_Years
                  ,Date_of_Last_PAT_Call
                  ,case when TA_Calls_Last_Year      is null then 0 else TA_Calls_Last_Year      end as TA_Calls_Last_Year
                  ,case when TA_Calls_Last_3_Months  is null then 0 else TA_Calls_Last_3_Months  end as TA_Calls_Last_3_Months
                  ,case when TA_Calls_Last_6_Months  is null then 0 else TA_Calls_Last_6_Months  end as TA_Calls_Last_6_Months
                  ,case when TA_Calls_Last_9_Months  is null then 0 else TA_Calls_Last_9_Months  end as TA_Calls_Last_9_Months
                  ,case when TA_Calls_Last_2_Years   is null then 0 else TA_Calls_Last_2_Years   end as TA_Calls_Last_2_Years
                  ,Date_of_Last_TA_Call
              into #HistoricalCallingBehaviour_output
              from #HistoricalCallingBehaviour1

          ---------------------------------------------------------------------------------------------------------------------------------------------------------
                -- cust_churn_hist
            select account_number
                  ,event_dt
                  ,TypeOfEvent
              into #SC_events_in1
              from citeam.View_CUST_CHURN_HIST
             where TypeOfEvent = 'SC'

                -- reference
            select *
                  ,@Reference as [Reference2]
              into #SC_events_in2
              from #SC_events_in1

                -- select
            select *
              into #SC_events_in3
              from #SC_events_in2
             where account_number like '%' || @Sample_1_EndString

            select *
             into #SC_events_in_output
             from #SC_events_in3 as scc
                  inner join SourceDates as sds on scc.[Reference2] = sds.[Reference]

          ---------------------------------------------------------------------------------------------------------------------------------------------------------
                -- first branch
            select *
              into #SC_events1
              from #SC_events_in_output
             where event_dt > [2_Years_Prior]
               and event_dt <= Snapshot_Date

                -- aggregate
            select account_number
                  ,max(event_dt) as effective_from_dt_Max
              into #SC_events2
              from #SC_events1
          group by account_number

                -- filter
            select account_number
                  ,effective_from_dt_Max as Date_of_Last_SC_call
              into #SC_events3
              from #SC_events2

                -- second branch
            select *
              into #SC_events4
              from #SC_events_in_output
             where event_dt > [2_Years_Prior]
               and event_dt <= Snapshot_Date

                -- aggregate
            select account_number
                  ,count() as SC_events_Last_2_Years
              into #SC_events5
              from #SC_events4
          group by account_number

                -- third branch
            select *
              into #SC_events6
              from #SC_events_in_output
             where event_dt > [1_Year_Prior]
               and event_dt <= Snapshot_Date

                -- aggregate
            select account_number
                  ,count() as SC_events_Last_Year
              into #SC_events7
              from #SC_events6
          group by account_number

                -- fourth branch
            select *
              into #SC_events8
              from #SC_events_in_output
             where event_dt > [9_Months_Prior]
               and event_dt <= Snapshot_Date

                -- aggregate
            select account_number
                  ,count() as SC_events_Last_9_Months
              into #SC_events9
              from #SC_events8
          group by account_number

                -- fifth branch
            select *
              into #SC_events10
              from #SC_events_in_output
             where event_dt > [6_Months_Prior] and event_dt <= Snapshot_Date

                -- aggregate
            select account_number
                  ,count() as SC_events_Last_6_Months
              into #SC_events11
              from #SC_events10
          group by account_number

                -- sixth branch
            select *
              into #SC_events12
              from #SC_events_in_output
             where event_dt > [3_Months_Prior] and event_dt <= Snapshot_Date

            select account_number
                  ,count() as SC_events_Last_3_Months
              into #SC_events13
              from #SC_events12
          group by account_number

            select sc3.*
                  ,SC_events_Last_2_Years
                  ,SC_events_Last_Year
                  ,SC_events_Last_9_Months
                  ,SC_events_Last_6_Months
                  ,SC_events_Last_3_Months
              into #SC_events_output
              from #SC_events3 as sc3
                   full outer join #SC_events5  as sc5 on sc3.account_number = sc5.account_number
                   full outer join #SC_events7  as sc7 on sc3.account_number = sc7.account_number
                   full outer join #SC_events9  as sc9 on sc3.account_number = sc9.account_number
                   full outer join #SC_events11 as s11 on sc3.account_number = s11.account_number
                   full outer join #SC_events13 as s13 on sc3.account_number = s13.account_number

          ---------------------------------------------------------------------------------------------------------------------------------------------------------
                -- cust_churn_hist
            select account_number
                  ,event_dt
                  ,TypeOfEvent
              into #AB_events_in1
              from citeam.View_CUST_CHURN_HIST
             where TypeOfEvent = 'AB'

                -- reference
            select *
                  ,@Reference as [Reference2]
              into #AB_events_in2
              from #AB_events_in1

                -- select
            select *
              into #AB_events_in3
              from #AB_events_in2
             where account_number like '%' || @Sample_1_EndString

            select *
             into #AB_events_in_output
             from #AB_events_in3 as scc
                  inner join SourceDates as sds on scc.[Reference2] = sds.[Reference]
          ---------------------------------------------------------------------------------------------------------------------------------------------------------
                -- first branch
            select *
              into #AB_events1
              from #AB_events_in_output
             where event_dt > [2_Years_Prior]
               and event_dt <= Snapshot_Date

                -- aggregate
            select account_number
                  ,max(event_dt) as effective_from_dt_Max
              into #AB_events2
              from #AB_events1
          group by account_number

                -- filter
            select account_number
                  ,effective_from_dt_Max as Date_of_Last_AB_call
              into #AB_events3
              from #AB_events2

                -- second branch
            select *
              into #AB_events4
              from #AB_events_in_output
             where event_dt > [2_Years_Prior]
               and event_dt <= Snapshot_Date

                -- aggregate
            select account_number
                  ,count() as AB_events_Last_2_Years
              into #AB_events5
              from #AB_events4
          group by account_number

                -- third branch
            select *
              into #AB_events6
              from #AB_events_in_output
             where event_dt > [1_Year_Prior]
               and event_dt <= Snapshot_Date

                -- aggregate
            select account_number
                  ,count() as AB_events_Last_Year
              into #AB_events7
              from #AB_events6
          group by account_number

                -- fourth branch
            select *
              into #AB_events8
              from #AB_events_in_output
             where event_dt > [9_Months_Prior]
               and event_dt <= Snapshot_Date

                -- aggregate
            select account_number
                  ,count() as AB_events_Last_9_Months
              into #AB_events9
              from #AB_events8
          group by account_number

                -- fifth branch
            select *
              into #AB_events10
              from #AB_events_in_output
             where event_dt > [6_Months_Prior]
               and event_dt <= Snapshot_Date

                -- aggregate
            select account_number
                  ,count() as AB_events_Last_6_Months
              into #AB_events11
              from #AB_events10
          group by account_number

                -- sixth branch
            select *
              into #AB_events12
              from #AB_events_in_output
             where event_dt > [3_Months_Prior]
               and event_dt <= Snapshot_Date

            select account_number
                  ,count() as AB_events_Last_3_Months
              into #AB_events13
              from #AB_events12
          group by account_number

            select ab3.*
                  ,AB_events_Last_2_Years
                  ,AB_events_Last_Year
                  ,AB_events_Last_9_Months
                  ,AB_events_Last_6_Months
                  ,AB_events_Last_3_Months
              into #AB_events_output
              from #AB_events3 as ab3
                   full outer join #AB_events5  as ab5 on ab3.account_number = ab5.account_number
                   full outer join #AB_events7  as ab7 on ab3.account_number = ab7.account_number
                   full outer join #AB_events9  as ab9 on ab3.account_number = ab9.account_number
                   full outer join #AB_events11 as a11 on ab3.account_number = a11.account_number
                   full outer join #AB_events13 as a13 on ab3.account_number = a13.account_number

          ---------------------------------------------------------------------------------------------------------------------------------------------------------
                -- cust_churn_hist
            select account_number
                  ,event_dt
                  ,TypeOfEvent
              into #PO_events_in1
              from citeam.View_CUST_CHURN_HIST
             where TypeOfEvent = 'PO'

                -- reference
            select *
                  ,@Reference as [Reference2]
              into #PO_events_in2
              from #PO_events_in1

                -- select
            select *
              into #PO_events_in3
              from #PO_events_in2
             where account_number like '%' || @Sample_1_EndString

            select *
             into #PO_events_in_output
             from #PO_events_in3 as poe
                  inner join SourceDates as sds on poe.[Reference2] = sds.[Reference]
          ---------------------------------------------------------------------------------------------------------------------------------------------------------
                -- first branch
            select *
              into #PO_events1
              from #PO_events_in_output
             where event_dt > [2_Years_Prior]
               and event_dt <= Snapshot_Date

                -- aggregate
            select account_number
                  ,max(event_dt) as effective_from_dt_Max
              into #PO_events2
              from #PO_events1
          group by account_number

                -- filter
            select account_number
                  ,effective_from_dt_Max as Date_of_Last_PO_call
              into #PO_events3
              from #PO_events2

                -- second branch
            select *
              into #PO_events4
              from #PO_events_in_output
             where event_dt > [2_Years_Prior]
               and event_dt <= Snapshot_Date

                -- aggregate
            select account_number
                  ,count() as PO_events_Last_2_Years
              into #PO_events5
              from #PO_events4
          group by account_number

                -- third branch
            select *
              into #PO_events6
              from #PO_events_in_output
             where event_dt > [1_Year_Prior]
               and event_dt <= Snapshot_Date

                -- aggregate
            select account_number
                  ,count() as PO_events_Last_Year
              into #PO_events7
              from #PO_events6
          group by account_number

                -- fourth branch
            select *
              into #PO_events8
              from #PO_events_in_output
             where event_dt > [9_Months_Prior]
               and event_dt <= Snapshot_Date

                -- aggregate
            select account_number
                  ,count() as PO_events_Last_9_Months
              into #PO_events9
              from #PO_events8
          group by account_number

                -- fifth branch
            select *
              into #PO_events10
              from #PO_events_in_output
             where event_dt > [6_Months_Prior]
               and event_dt <= Snapshot_Date

                -- aggregate
            select account_number
                  ,count() as PO_events_Last_6_Months
              into #PO_events11
              from #PO_events10
          group by account_number

                -- sixth branch
            select *
              into #PO_events12
              from #PO_events_in_output
             where event_dt > [3_Months_Prior]
               and event_dt <= Snapshot_Date

            select account_number
                  ,count() as PO_events_Last_3_Months
              into #PO_events13
              from #PO_events12
          group by account_number

            select po3.*
                  ,PO_events_Last_2_Years
                  ,PO_events_Last_Year
                  ,PO_events_Last_9_Months
                  ,PO_events_Last_6_Months
                  ,PO_events_Last_3_Months
              into #PO_events_output
              from #PO_events3 as po3
                   full outer join #PO_events5  as po5 on po3.account_number = po5.account_number
                   full outer join #PO_events7  as po7 on po3.account_number = po7.account_number
                   full outer join #PO_events9  as po9 on po3.account_number = po9.account_number
                   full outer join #PO_events11 as p11 on po3.account_number = p11.account_number
                   full outer join #PO_events13 as p13 on po3.account_number = p13.account_number

          ---------------------------------------------------------------------------------------------------------------------------------------------------------
                -- cust_churn_hist
            select account_number
                  ,event_dt
                  ,TypeOfEvent
              into #PC_events_in1
              from citeam.View_CUST_CHURN_HIST
             where TypeOfEvent = 'PC'

                -- reference
            select *
                  ,@Reference as [Reference2]
              into #PC_events_in2
              from #PC_events_in1

                -- select
            select *
              into #PC_events_in3
              from #PC_events_in2
             where account_number like '%' || @Sample_1_EndString

            select *
             into #PC_events_in_output
             from #PC_events_in3 as pc3
                  inner join SourceDates as sds on pc3.[Reference2] = sds.[Reference]
          ---------------------------------------------------------------------------------------------------------------------------------------------------------
                -- first branch
            select *
              into #PC_events1
              from #PC_events_in_output
             where event_dt > [2_Years_Prior]
               and event_dt <= Snapshot_Date

                -- aggregate
            select account_number
                  ,max(event_dt) as effective_from_dt_Max
              into #PC_events2
              from #PC_events1
          group by account_number

                -- filter
            select account_number
                  ,effective_from_dt_Max as Date_of_Last_PC_call
              into #PC_events3
              from #PC_events2

                -- second branch
            select *
              into #PC_events4
              from #PC_events_in_output
             where event_dt > [2_Years_Prior]
               and event_dt <= Snapshot_Date

                -- aggregate
            select account_number
                  ,count() as PC_events_Last_2_Years
              into #PC_events5
              from #PC_events4
          group by account_number

                -- third branch
            select *
              into #PC_events6
              from #PC_events_in_output
             where event_dt > [1_Year_Prior]
               and event_dt <= Snapshot_Date

                -- aggregate
            select account_number
                  ,count() as PC_events_Last_Year
              into #PC_events7
              from #PC_events6
          group by account_number

                -- fourth branch
            select *
              into #PC_events8
              from #PC_events_in_output
             where event_dt > [9_Months_Prior]
               and event_dt <= Snapshot_Date

                -- aggregate
            select account_number
                  ,count() as PC_events_Last_9_Months
              into #PC_events9
              from #PC_events8
          group by account_number

                -- fifth branch
            select *
              into #PC_events10
              from #PC_events_in_output
             where event_dt > [6_Months_Prior]
               and event_dt <= Snapshot_Date

                -- aggregate
            select account_number
                  ,count() as PC_events_Last_6_Months
              into #PC_events11
              from #PC_events10
          group by account_number

                -- sixth branch
            select *
              into #PC_events12
              from #PC_events_in_output
             where event_dt > [3_Months_Prior]
               and event_dt <= Snapshot_Date

            select account_number
                  ,count() as PC_events_Last_3_Months
              into #PC_events13
              from #PC_events12
          group by account_number

            select pc3.*
                  ,PC_events_Last_2_Years
                  ,PC_events_Last_Year
                  ,PC_events_Last_9_Months
                  ,PC_events_Last_6_Months
                  ,PC_events_Last_3_Months
              into #PC_events_output
              from #PC_events3 as pc3
                   full outer join #PC_events5  as pc5 on pc3.account_number = pc5.account_number
                   full outer join #PC_events7  as pc7 on pc3.account_number = pc7.account_number
                   full outer join #PC_events9  as pc9 on pc3.account_number = pc9.account_number
                   full outer join #PC_events11 as p11 on pc3.account_number = p11.account_number
                   full outer join #PC_events13 as p13 on pc3.account_number = p13.account_number

          ---------------------------------------------------------------------------------------------------------------------------------------------------------
                -- merge
            select sco.*
                  ,Date_of_Last_AB_call
                  ,AB_events_Last_2_Years
                  ,AB_events_Last_Year
                  ,AB_events_Last_9_Months
                  ,AB_events_Last_6_Months
                  ,AB_events_Last_3_Months
                  ,Date_of_Last_PO_call
                  ,PO_events_Last_2_Years
                  ,PO_events_Last_Year
                  ,PO_events_Last_9_Months
                  ,PO_events_Last_6_Months
                  ,PO_events_Last_3_Months
                  ,Date_of_Last_PC_call
                  ,PC_events_Last_2_Years
                  ,PC_events_Last_Year
                  ,PC_events_Last_9_Months
                  ,PC_events_Last_6_Months
                  ,PC_events_Last_3_Months
              into #HistoricalChurnBehaviour1
              from #SC_events_output                 as sco
                   full outer join #AB_events_output as abo on sco.account_number = abo.account_number
                   full outer join #PO_events_output as poo on sco.account_number = poo.account_number
                   full outer join #PC_events_output as pco on sco.account_number = pco.account_number

                -- filter
            select account_number
                  ,case when SC_events_Last_Year     is null then 0 else SC_events_Last_Year     end as SC_events_Last_Year
                  ,case when SC_events_Last_3_Months is null then 0 else SC_events_Last_3_Months end as SC_events_Last_3_Months
                  ,case when SC_events_Last_6_Months is null then 0 else SC_events_Last_6_Months end as SC_events_Last_6_Months
                  ,case when SC_events_Last_9_Months is null then 0 else SC_events_Last_9_Months end as SC_events_Last_9_Months
                  ,case when SC_events_Last_2_Years  is null then 0 else SC_events_Last_2_Years  end as SC_events_Last_2_Years
                  ,Date_of_Last_SC_call
                  ,case when AB_events_Last_Year     is null then 0 else AB_events_Last_Year     end as AB_events_Last_Year
                  ,case when AB_events_Last_3_Months is null then 0 else AB_events_Last_3_Months end as AB_events_Last_3_Months
                  ,case when AB_events_Last_6_Months is null then 0 else AB_events_Last_6_Months end as AB_events_Last_6_Months
                  ,case when AB_events_Last_9_Months is null then 0 else AB_events_Last_9_Months end as AB_events_Last_9_Months
                  ,case when AB_events_Last_2_Years  is null then 0 else AB_events_Last_2_Years  end as AB_events_Last_2_Years
                  ,Date_of_Last_AB_call
                  ,case when PO_events_Last_Year     is null then 0 else PO_events_Last_Year     end as PO_events_Last_Year
                  ,case when PO_events_Last_3_Months is null then 0 else PO_events_Last_3_Months end as PO_events_Last_3_Months
                  ,case when PO_events_Last_6_Months is null then 0 else PO_events_Last_6_Months end as PO_events_Last_6_Months
                  ,case when PO_events_Last_9_Months is null then 0 else PO_events_Last_9_Months end as PO_events_Last_9_Months
                  ,case when PO_events_Last_2_Years  is null then 0 else PO_events_Last_2_Years  end as PO_events_Last_2_Years
                  ,Date_of_Last_PO_call
                  ,case when PC_events_Last_Year     is null then 0 else PC_events_Last_Year     end as PC_events_Last_Year
                  ,case when PC_events_Last_3_Months is null then 0 else PC_events_Last_3_Months end as PC_events_Last_3_Months
                  ,case when PC_events_Last_6_Months is null then 0 else PC_events_Last_6_Months end as PC_events_Last_6_Months
                  ,case when PC_events_Last_9_Months is null then 0 else PC_events_Last_9_Months end as PC_events_Last_9_Months
                  ,case when PC_events_Last_2_Years  is null then 0 else PC_events_Last_2_Years  end as PC_events_Last_2_Years
                  ,Date_of_Last_PC_call
              into #HistoricalChurnBehaviour_output
              from #HistoricalChurnBehaviour1
          ---------------------------------------------------------------------------------------------------------------------------------------------------------
             select *
              into #offers1
              from citeam.View_CUST_OFFER_HIST
             where offer_id not in (75680,75687,75685,75683,75686,75684,75681,75682,75680,75681,75682,75579,75580,75581,75583,75584,75586,75587,75589,75590,75592,75593,75594,75595,75598,75601,75602,75607,75610,75612,75613,75616,75618,75619,75620,75621,75622,75623,75624,75625,75626,75627,75628,75629,75630,75631,75634,75636,75638,75642,75643,75644,75647,75651,75654,75655,75656,75657,75664,75666,75667,75668,75673,75675,75677,75579,75583,75584,75589,75590,75592,75594,75595,75601,75602,75607,75610,75612,75613,75618,75619,75620,75621,75622,75623,75624,75625,75626,75627,75628,75629,75630,75631,75634,75636,75638,75654,75655,75656,75657,75673,75675,75677,75444,75443,75445)

                -- select
            select *
              into #offers2
              from #offers1
             where account_number like '%' || @Sample_1_EndString
                -- supernode
                          -- filter
                      select *
                            ,case when offer_start_dt is null then initial_effective_dt else offer_start_dt end
                        into #offers3
                        from #offers2

          --                 -- aggregate
          --             select sum(offer_id)
          --               into #offers4
          --               from #offers3

                          -- remove administration charges
                      select *
                        into #offers5
                        from #offers3 as of3
                       where lower(offer_dim_description) not like '%administration charge%'

                          -- remove office only
                      select *
                        into #offers6
                        from #offers5
                       where lower(offer_dim_description) not like '%staff offer%'

                          -- remove not relevant
                      select *
                        into #offers7
                        from #offers6
                       where lower(offer_dim_description) not like '%not relevant%'

                -- end supernode

                -- reference
            select *
                  ,@reference as [reference8]
              into #offers8
              from #offers7

                -- merge
            select *
              into #offers9
              from #offers8 as of8
                   inner join sourcedates as sou on of8.[reference8] = sou.[reference]

                -- select
            select *
              into #offers10
              from #offers9
             where offer_start_dt > [2_Years_Prior] and offer_start_dt <= Snapshot_Date

                -- branch 2
            execute('select account_number
                           ,status_code
                       into #offers11
                       from ' || case when @reference <= 201409 then 'yarlagaddar'
                                      when @reference = 201410 then 'hutchij'
                                      else 'sharmaa'
                                  end || '.View_attachments_' || @reference || '
             where Status_Code = ''AC''')

                -- select
            select *
              into #offers12
              from #offers11
             where account_number like '%' || @Sample_1_EndString

                -- merge
            select o10.*
                  ,status_code
              into #offers13
              from #offers10 as o10
                   inner join #offers12 as o12 on o10.account_number = o12.account_number

                -- supernode
                          -- filler
                      select *
                            ,trim(offer_type) as offer_type_trimmed
                        into #offers14
                        from #offers13

                          -- type
                      select *
                        into #offers15
                        from #offers14

                          -- SetToFlag
                      select *
                            ,case when offer_type_trimmed = 'Box Offer'        then 1 else 0 end as offer_type_box_offer
                            ,case when offer_type_trimmed = 'BroadBand & Talk' then 1 else 0 end as offer_type_broadband_and_talk
                            ,case when offer_type_trimmed = 'Install Offer'    then 1 else 0 end as offer_type_install_offer
                            ,case when offer_type_trimmed = 'Others'           then 1 else 0 end as offer_type_others
                            ,case when offer_type_trimmed = 'Others_PPO'       then 1 else 0 end as offer_type_others_ppo
                            ,case when offer_type_trimmed = 'Service Call'     then 1 else 0 end as offer_type_service_call
                            ,case when offer_type_trimmed = 'TV Packs'         then 1 else 0 end as offer_type_tv_packs
                        into #offers16
                        from #offers15

                          -- filter
                      select account_number
                            ,Status_Code
                            ,[Reference]
                            ,offer_start_dt
                            ,offer_end_dt
                            ,offer_amount
                            ,offer_status
                            ,offer_dim_description
                            ,initial_effective_dt
          --                  ,Offer_Duration_Months
                            ,Sky_Product
                            ,Offer_Type
                            ,Attachments_Table
                            ,Account_Numbers_ending_in
                            ,Snapshot_Date
                            ,[2_Years_Prior]
                            ,[1_Year_Prior]
                            ,[10_Months_Prior]
                            ,[9_Months_Prior]
                            ,[6_Months_Prior]
                            ,[3_Months_Prior]
                            ,[1_Month_Prior]
                            ,[1_Month_Future]
                            ,[2_Months_Future]
                            ,[3_Months_Future]
                            ,[4_Months_Future]
                            ,[5_Months_Future]
                            ,[6_Months_Future]
                            ,Offer_Type_Box_Offer as Box_Offer
                            ,Offer_Type_BroadBand_and_Talk as BroadBand_and_Talk
                            ,Offer_Type_Install_Offer as Install_Offer
                            ,Offer_Type_Others as other_offers
                            ,Offer_Type_Others_PPO as others_ppo
                            ,Offer_Type_Service_Call  as service_call
                            ,Offer_Type_TV_Packs as tv_packs
                        into #offers17
                        from #offers16

                -- type
            select *
              into #offers18
              from #offers17

                 -- summarise data supernode
             select *
                   ,case offer_type when 'BroadBand & Talk' then 'Software_Comms'
                                    when 'TV Packs'         then 'Software_DTV'
                                    else 'Others'
                     end as Offer_category
               into #offers19
               from #offers18

                 -- Last 2 years
                 -- select
             select *
               into #offers20
               from #offers19
              where offer_start_dt > [2_Years_Prior]
                and offer_start_dt <= Snapshot_Date

                 -- aggregate
             select account_number
                   ,max([Reference]) as [Reference]
                   ,max(Attachments_Table) as Attachments_Table
                   ,max(Account_Numbers_ending_in) as Account_Numbers_ending_in
                   ,max(Snapshot_Date) as Snapshot_Date
                   ,max([1_Year_Prior]) as [1_Year_Prior]
                   ,max([9_Months_Prior]) as [9_Months_Prior]
                   ,max([6_Months_Prior]) as [6_Months_Prior]
                   ,max([3_Months_Prior]) as [3_Months_Prior]
                   ,max([1_Month_Prior]) as [1_Month_Prior]
                   ,max([1_Month_Future]) as [1_Month_Future]
                   ,max([2_Months_Future]) as [2_Months_Future]
                   ,max([3_Months_Future]) as [3_Months_Future]
                   ,max([4_Months_Future]) as [4_Months_Future]
                   ,max([5_Months_Future]) as [5_Months_Future]
                   ,max([6_Months_Future]) as [6_Months_Future]
                   ,sum(box_offer)          as sum_box_offer
                   ,sum(BroadBand_and_Talk) as sum_BroadBand_and_Talk
                   ,sum(Install_Offer)      as sum_Install_Offer
                   ,sum(other_offers)       as sum_others
                   ,sum(others_ppo)         as sum_others_ppo
                   ,sum(service_call)       as sum_service_call
                   ,sum(tv_packs)           as sum_tv_packs
               into #offers21
               from #offers20
          group by account_number

                 -- filter
             select account_number
                   ,sum_box_offer          as Box_Offer_Last_2_Years
                   ,sum_BroadBand_and_Talk as BroadBand_and_Talk_Last_2_Years
                   ,sum_Install_Offer      as Install_Offer_Last_2_Years
                   ,sum_others             as Others_Last_2_Years
                   ,sum_others_ppo         as Others_PPO_Last_2_Years
                   ,sum_service_call       as Service_Call_Last_2_Years
                   ,sum_tv_packs           as TV_Packs_Last_2_Years
               into #offers22
               from #offers21

                 -- Last year
                 -- select
             select *
               into #offers23
               from #offers19
              where offer_start_dt > [1_Year_Prior]
                and offer_start_dt <= Snapshot_Date

                 -- aggregate
             select account_number
                   ,sum(box_offer)          as sum_box_offer
                   ,sum(BroadBand_and_Talk) as sum_BroadBand_and_Talk
                   ,sum(Install_Offer)      as sum_Install_Offer
                   ,sum(other_offers)       as sum_others
                   ,sum(others_ppo)         as sum_others_ppo
                   ,sum(service_call)       as sum_service_call
                   ,sum(tv_packs)           as sum_tv_packs
               into #offers24
               from #offers23
          group by account_number

                 -- filter
             select account_number
                   ,sum_box_offer          as Box_Offer_Last_Year
                   ,sum_BroadBand_and_Talk as BroadBand_and_Talk_Last_Year
                   ,sum_Install_Offer      as Install_Offer_Last_Year
                   ,sum_others             as Others_Last_Year
                   ,sum_others_ppo         as Others_PPO_Last_Year
                   ,sum_service_call       as Service_Call_Last_Year
                   ,sum_tv_packs           as TV_Packs_Last_Year
               into #offers25
               from #offers24

                 -- Last 6 months
                 -- select
             select *
               into #offers26
               from #offers19
              where offer_start_dt > [6_Months_Prior]
                and offer_start_dt <= Snapshot_Date

                 -- aggregate
             select account_number
                   ,sum(box_offer)          as sum_box_offer
                   ,sum(BroadBand_and_Talk) as sum_BroadBand_and_Talk
                   ,sum(Install_Offer)      as sum_Install_Offer
                   ,sum(other_offers)       as sum_others
                   ,sum(others_ppo)         as sum_others_ppo
                   ,sum(service_call)       as sum_service_call
                   ,sum(tv_packs)           as sum_tv_packs
               into #offers27
               from #offers26
          group by account_number

                 -- filter
             select account_number
                   ,sum_box_offer          as Box_Offer_Last_6_Months
                   ,sum_BroadBand_and_Talk as BroadBand_and_Talk_Last_6_Months
                   ,sum_Install_Offer      as Install_Offer_Last_6_Months
                   ,sum_others             as Others_Last_6_Months
                   ,sum_others_ppo         as Others_PPO_Last_6_Months
                   ,sum_service_call       as Service_Call_Last_6_Months
                   ,sum_tv_packs           as TV_Packs_Last_6_Months
               into #offers28
               from #offers27

                 -- Last 3 months
                 -- select
             select *
               into #offers29
               from #offers19
              where offer_start_dt > [3_Months_Prior]
                and offer_start_dt <= Snapshot_Date

                 -- aggregate
             select account_number
                   ,sum(box_offer)          as sum_box_offer
                   ,sum(BroadBand_and_Talk) as sum_BroadBand_and_Talk
                   ,sum(Install_Offer)      as sum_Install_Offer
                   ,sum(other_offers)       as sum_others
                   ,sum(others_ppo)         as sum_others_ppo
                   ,sum(service_call)       as sum_service_call
                   ,sum(tv_packs)           as sum_tv_packs
               into #offers30
               from #offers29
          group by account_number

                 -- filter
             select account_number
                   ,sum_box_offer          as Box_Offer_Last_3_Months
                   ,sum_BroadBand_and_Talk as BroadBand_and_Talk_Last_3_Months
                   ,sum_Install_Offer      as Install_Offer_Last_3_Months
                   ,sum_others             as Others_Last_3_Months
                   ,sum_others_ppo         as Others_PPO_Last_3_Months
                   ,sum_service_call       as Service_Call_Last_3_Months
                   ,sum_tv_packs           as TV_Packs_Last_3_Months
               into #offers31
               from #offers30

                 -- price protection
                 -- aggregate
             select account_number
                   ,sum(others_ppo)         as price_protection_flag
               into #offers32
               from #offers19
          group by account_number

                 -- filler
             select account_number
                   ,case when price_protection_flag > 1 then 1 else price_protection_flag end as price_protection_flag
               into #offers33
               from #offers32

                -- expiring offers next 4-6
                -- expiring
            select o19.*
              into #offers34
              from #offers19 as o19
             where offer_end_dt <= [6_Months_Future]
               and offer_end_dt > [3_Months_Future]
               and offer_start_dt <> offer_end_dt

                -- aggregate
            select account_number
                  ,offer_category
                  ,sum(offer_amount) as offer_amount_sum
                  ,count() as record_count
              into #offers35
              from #offers34
          group by account_number
                  ,offer_category

                -- restructure
            select *
                  ,case when offer_category = 'Software_Comms' then offer_amount_Sum else 0 end as Offer_Category_Software_Comms_offer_amount_sum
                  ,case when offer_category = 'Software_DTV'   then offer_amount_Sum else 0 end as Offer_Category_Software_DTV_offer_amount_sum
                  ,case when offer_category = 'Others'         then offer_amount_Sum else 0 end as Offer_Category_others_offer_amount_sum
                  ,case when offer_category = 'Software_Comms' then record_count     else 0 end as Offer_Category_Software_Comms_record_count
                  ,case when offer_category = 'Software_DTV'   then record_count     else 0 end as Offer_Category_Software_DTV_record_count
                  ,case when offer_category = 'Others'         then record_count     else 0 end as Offer_Category_others_record_count
              into #offers36
              from #offers35

                -- total expiring offer 4-6
            select *
                  ,Offer_Category_Software_Comms_offer_amount_Sum + Offer_Category_Others_offer_amount_Sum + Offer_Category_Software_DTV_offer_amount_Sum as Total_Expiring_Offer_Value_Next_4_6_Months
              into #offers37
              from #offers36

                -- aggregate
            select account_number
                  ,max(offer_category_software_comms_record_count) as Total_Expiring_Comms_Offers_Next_4_6_Months
                  ,max(offer_category_others_record_count)         as Total_Expiring_Other_Offers_Next_4_6_Months
                  ,max(offer_category_software_dtv_record_count)   as Total_Expiring_DTV_Offers_Next_4_6_months
                  ,sum(Total_Expiring_Offer_Value_Next_4_6_Months) as Total_Expiring_Offer_Value_Next_4_6_months
              into #offers38
              from #offers37
          group by account_number

                -- filler
            select account_number
                  ,case when Total_Expiring_Comms_Offers_Next_4_6_Months is null then 0 else Total_Expiring_Comms_Offers_Next_4_6_Months end as Total_Expiring_Comms_Offers_Next_4_6_Months
                  ,case when Total_Expiring_Other_Offers_Next_4_6_Months is null then 0 else Total_Expiring_Other_Offers_Next_4_6_Months end as Total_Expiring_Other_Offers_Next_4_6_Months
                  ,case when Total_Expiring_DTV_Offers_Next_4_6_months   is null then 0 else Total_Expiring_DTV_Offers_Next_4_6_months   end as Total_Expiring_DTV_Offers_Next_4_6_months
                  ,case when Total_Expiring_Offer_Value_Next_4_6_months  is null then 0 else Total_Expiring_Offer_Value_Next_4_6_months  end as Total_Expiring_Offer_Value_Next_4_6_months
              into #offers39
              from #offers38

                -- expiring offers next 1-3
                -- expiring
            select *
              into #offers40
              from #offers19
             where offer_end_dt <= [3_Months_Future]
               and offer_end_dt > [1_Month_Prior]
               and offer_start_dt <> offer_end_dt

                -- aggregate
            select account_number
                  ,offer_category
                  ,sum(offer_amount) as offer_amount_sum
                  ,count() as record_count
              into #offers41
              from #offers40
          group by account_number
                  ,offer_category

                -- restructure
            select *
                  ,case when offer_category = 'Software_Comms' then offer_amount_Sum else 0 end as Offer_Category_Software_Comms_offer_amount_sum
                  ,case when offer_category = 'Software_DTV'   then offer_amount_Sum else 0 end as Offer_Category_Software_DTV_offer_amount_sum
                  ,case when offer_category = 'Others'         then offer_amount_Sum else 0 end as Offer_Category_others_offer_amount_sum
                  ,case when offer_category = 'Software_Comms' then record_count     else 0 end as Offer_Category_Software_Comms_record_count
                  ,case when offer_category = 'Software_DTV'   then record_count     else 0 end as Offer_Category_Software_DTV_record_count
                  ,case when offer_category = 'Others'         then record_count     else 0 end as Offer_Category_others_record_count
              into #offers42
              from #offers41

                -- total expiring offer 3m
            select *
                  ,Offer_Category_Software_Comms_offer_amount_Sum + Offer_Category_Others_offer_amount_Sum + Offer_Category_Software_DTV_offer_amount_Sum as Total_Expiring_Offer_Value_Next_3_Months
              into #offers43
              from #offers42

                -- aggregate
            select account_number
                  ,max(offer_category_software_comms_record_count) as Total_Expiring_Comms_Offers_Next_3_Months
                  ,max(offer_category_others_record_count)         as Total_Expiring_Other_Offers_Next_3_Months
                  ,max(offer_category_software_dtv_record_count)   as Total_Expiring_DTV_Offers_Next_3_months
                  ,sum(Total_Expiring_Offer_Value_Next_3_Months)   as Total_Expiring_Offer_Value_Next_3_months
              into #offers44
              from #offers43
          group by account_number

                -- filler
            select account_number
                  ,case when Total_Expiring_Comms_Offers_Next_3_Months is null then 0 else Total_Expiring_Comms_Offers_Next_3_Months end as Total_Expiring_Comms_Offers_Next_3_Months
                  ,case when Total_Expiring_Other_Offers_Next_3_Months is null then 0 else Total_Expiring_Other_Offers_Next_3_Months end as Total_Expiring_Other_Offers_Next_3_Months
                  ,case when Total_Expiring_DTV_Offers_Next_3_months   is null then 0 else Total_Expiring_DTV_Offers_Next_3_months   end as Total_Expiring_DTV_Offers_Next_3_months
                  ,case when Total_Expiring_Offer_Value_Next_3_months  is null then 0 else Total_Expiring_Offer_Value_Next_3_months  end as Total_Expiring_Offer_Value_Next_3_months
              into #offers45
              from #offers44

                -- merge
            select o22.account_number
                  ,Box_Offer_Last_2_Years
                  ,BroadBand_and_Talk_Last_2_Years
                  ,Install_Offer_Last_2_Years
                  ,Others_Last_2_Years
                  ,Others_PPO_Last_2_Years
                  ,Service_Call_Last_2_Years
                  ,TV_Packs_Last_2_Years
                  ,Box_Offer_Last_Year
                  ,BroadBand_and_Talk_Last_Year
                  ,Install_Offer_Last_Year
                  ,Others_Last_Year
                  ,Others_PPO_Last_Year
                  ,Service_Call_Last_Year
                  ,TV_Packs_Last_Year
                  ,Box_Offer_Last_6_Months
                  ,BroadBand_and_Talk_Last_6_Months
                  ,Install_Offer_Last_6_Months
                  ,Others_Last_6_Months
                  ,Others_PPO_Last_6_Months
                  ,Service_Call_Last_6_Months
                  ,TV_Packs_Last_6_Months
                  ,Box_Offer_Last_3_Months
                  ,BroadBand_and_Talk_Last_3_Months
                  ,Install_Offer_Last_3_Months
                  ,Others_Last_3_Months
                  ,Others_PPO_Last_3_Months
                  ,Service_Call_Last_3_Months
                  ,TV_Packs_Last_3_Months
                  ,price_protection_flag
                  ,Total_Expiring_Comms_Offers_Next_4_6_Months
                  ,Total_Expiring_Other_Offers_Next_4_6_Months
                  ,Total_Expiring_DTV_Offers_Next_4_6_Months
                  ,Total_Expiring_Offer_Value_Next_4_6_Months
                  ,Total_Expiring_Comms_Offers_Next_3_Months
                  ,Total_Expiring_Other_Offers_Next_3_Months
                  ,Total_Expiring_DTV_Offers_Next_3_Months
                  ,Total_Expiring_Offer_Value_Next_3_Months
              into #offers46
              from #offers22 as o22
                   inner join #offers25 as o25 on o22.account_number = o25.account_number
                   inner join #offers28 as o28 on o22.account_number = o28.account_number
                   inner join #offers31 as o31 on o22.account_number = o31.account_number
                   inner join #offers33 as o33 on o22.account_number = o33.account_number
                   inner join #offers38 as o38 on o22.account_number = o38.account_number
                   inner join #offers44 as o44 on o22.account_number = o44.account_number

                -- filler
            select account_number
                  ,case when Box_Offer_Last_2_Years is null then 0 else box_Offer_Last_2_Years end as box_Offer_Last_2_Years
                  ,case when BroadBand_and_Talk_Last_2_Years is null then 0 else BroadBand_and_Talk_Last_2_Years end as BroadBand_and_Talk_Last_2_Years
                  ,case when Install_Offer_Last_2_Years is null then 0 else Install_Offer_Last_2_Years end as Install_Offer_Last_2_Years
                  ,case when Others_Last_2_Years is null then 0 else Others_Last_2_Years end as Others_Last_2_Years
                  ,case when Others_PPO_Last_2_Years is null then 0 else Others_PPO_Last_2_Years end as Others_PPO_Last_2_Years
                  ,case when Service_Call_Last_2_Years is null then 0 else Service_Call_Last_2_Years end as Service_Call_Last_2_Years
                  ,case when TV_Packs_Last_2_Years is null then 0 else TV_Packs_Last_2_Years end as TV_Packs_Last_2_Years
                  ,case when Box_Offer_Last_Year is null then 0 else Box_Offer_Last_Year end as Box_Offer_Last_Year
                  ,case when BroadBand_and_Talk_Last_Year is null then 0 else BroadBand_and_Talk_Last_Year end as BroadBand_and_Talk_Last_Year
                  ,case when Install_Offer_Last_Year is null then 0 else Install_Offer_Last_Year end as Install_Offer_Last_Year
                  ,case when Others_Last_Year is null then 0 else Others_Last_Year end as Others_Last_Year
                  ,case when Others_PPO_Last_Year is null then 0 else Others_PPO_Last_Year end as Others_PPO_Last_Year
                  ,case when Service_Call_Last_Year is null then 0 else Service_Call_Last_Year end as Service_Call_Last_Year
                  ,case when TV_Packs_Last_Year is null then 0 else TV_Packs_Last_Year end as TV_Packs_Last_Year
                  ,case when Box_Offer_Last_6_Months is null then 0 else Box_Offer_Last_6_Months end as Box_Offer_Last_6_Months
                  ,case when BroadBand_and_Talk_Last_6_Months is null then 0 else BroadBand_and_Talk_Last_6_Months end as BroadBand_and_Talk_Last_6_Months
                  ,case when Install_Offer_Last_6_Months is null then 0 else Install_Offer_Last_6_Months end as Install_Offer_Last_6_Months
                  ,case when Others_Last_6_Months is null then 0 else Others_Last_6_Months end as Others_Last_6_Months
                  ,case when Others_PPO_Last_6_Months is null then 0 else Others_PPO_Last_6_Months end as Others_PPO_Last_6_Months
                  ,case when Service_Call_Last_6_Months is null then 0 else Service_Call_Last_6_Months end as Service_Call_Last_6_Months
                  ,case when TV_Packs_Last_6_Months is null then 0 else TV_Packs_Last_6_Months end as TV_Packs_Last_6_Months
                  ,case when Box_Offer_Last_3_Months is null then 0 else Box_Offer_Last_3_Months end as Box_Offer_Last_3_Months
                  ,case when BroadBand_and_Talk_Last_3_Months is null then 0 else BroadBand_and_Talk_Last_3_Months end as BroadBand_and_Talk_Last_3_Months
                  ,case when Install_Offer_Last_3_Months is null then 0 else Install_Offer_Last_3_Months end as Install_Offer_Last_3_Months
                  ,case when Others_Last_3_Months is null then 0 else Others_Last_3_Months end as Others_Last_3_Months
                  ,case when Others_PPO_Last_3_Months is null then 0 else Others_PPO_Last_3_Months end as Others_PPO_Last_3_Months
                  ,case when Service_Call_Last_3_Months is null then 0 else Service_Call_Last_3_Months end as Service_Call_Last_3_Months
                  ,case when TV_Packs_Last_3_Months is null then 0 else TV_Packs_Last_3_Months end as TV_Packs_Last_3_Months
                  ,case when price_protection_flag is null then 0 else price_protection_flag end as price_protection_flag
                  ,case when Total_Expiring_Comms_Offers_Next_4_6_Months is null then 0 else Total_Expiring_Comms_Offers_Next_4_6_Months end as Total_Expiring_Comms_Offers_Next_4_6_Months
                  ,case when Total_Expiring_Other_Offers_Next_4_6_Months is null then 0 else Total_Expiring_Other_Offers_Next_4_6_Months end as Total_Expiring_Other_Offers_Next_4_6_Months
                  ,case when Total_Expiring_DTV_Offers_Next_4_6_Months is null then 0 else Total_Expiring_DTV_Offers_Next_4_6_Months end as Total_Expiring_DTV_Offers_Next_4_6_Months
                  ,case when Total_Expiring_Offer_Value_Next_4_6_Months is null then 0 else Total_Expiring_Offer_Value_Next_4_6_Months end as Total_Expiring_Offer_Value_Next_4_6_Months
                  ,case when Total_Expiring_Comms_Offers_Next_3_Months is null then 0 else Total_Expiring_Comms_Offers_Next_3_Months end as Total_Expiring_Comms_Offers_Next_3_Months
                  ,case when Total_Expiring_Other_Offers_Next_3_Months is null then 0 else Total_Expiring_Other_Offers_Next_3_Months end as Total_Expiring_Other_Offers_Next_3_Months
                  ,case when Total_Expiring_DTV_Offers_Next_3_Months is null then 0 else Total_Expiring_DTV_Offers_Next_3_Months end as Total_Expiring_DTV_Offers_Next_3_Months
                  ,case when Total_Expiring_Offer_Value_Next_3_Months is null then 0 else Total_Expiring_Offer_Value_Next_3_Months end as Total_Expiring_Offer_Value_Next_3_Months
              into #offers_output
              from #offers46
          ---------------------------------------------------------------------------------------------------------------------------------------------------------
                -- merge
            select dmt.*
                  ,AB_in_NEXT_3_months_flag
                  ,SC_In_Next_4_Months_Flag
                  ,PO_In_Next_4_Months_Flag
                  ,TA_in_3_6_Months_Flag
                  ,TA_in_NEXT_3_months_flag
              into #etl_main1
              from #data_mart_output as dmt
                   left join #future_calling_churn4 as fcc on dmt.account_number = fcc.account_number

                -- merge
            select et1.*
                  ,IC_Calls_Last_Year
                  ,IC_Calls_Last_3_Months
                  ,IC_Calls_Last_6_Months
                  ,IC_Calls_Last_9_Months
                  ,IC_Calls_Last_2_Years
                  ,Date_of_Last_IC_Call
                  ,PAT_Calls_Last_Year
                  ,PAT_Calls_Last_3_Months
                  ,PAT_Calls_Last_6_Months
                  ,PAT_Calls_Last_9_Months
                  ,PAT_Calls_Last_2_Years
                  ,Date_of_Last_PAT_Call
                  ,TA_Calls_Last_Year
                  ,TA_Calls_Last_3_Months
                  ,TA_Calls_Last_6_Months
                  ,TA_Calls_Last_9_Months
                  ,TA_Calls_Last_2_Years
                  ,Date_of_Last_TA_Call
              into #etl_main2
              from #etl_main1 as et1
                   left join #HistoricalCallingBehaviour_output as hcc on et1.account_number = hcc.account_number

                -- merge
            select et2.*
                  ,SC_events_Last_Year
                  ,SC_events_Last_3_Months
                  ,SC_events_Last_6_Months
                  ,SC_events_Last_9_Months
                  ,SC_events_Last_2_Years
                  ,Date_of_Last_SC_call
                  ,AB_events_Last_Year
                  ,AB_events_Last_3_Months
                  ,AB_events_Last_6_Months
                  ,AB_events_Last_9_Months
                  ,AB_events_Last_2_Years
                  ,Date_of_Last_AB_call
                  ,PO_events_Last_Year
                  ,PO_events_Last_3_Months
                  ,PO_events_Last_6_Months
                  ,PO_events_Last_9_Months
                  ,PO_events_Last_2_Years
                  ,Date_of_Last_PO_call
                  ,PC_events_Last_Year
                  ,PC_events_Last_3_Months
                  ,PC_events_Last_6_Months
                  ,PC_events_Last_9_Months
                  ,PC_events_Last_2_Years
                  ,Date_of_Last_PC_call
              into #etl_main3
              from #etl_main2 as et2
                   left join #HistoricalChurnBehaviour_output as hcc on et2.account_number = hcc.account_number

                -- merge
            select et3.*
                  ,Box_Offer_Last_2_Years
                  ,BroadBand_and_Talk_Last_2_Years
                  ,Install_Offer_Last_2_Years
                  ,Others_Last_2_Years
                  ,Others_PPO_Last_2_Years
                  ,Service_Call_Last_2_Years
                  ,TV_Packs_Last_2_Years
                  ,Box_Offer_Last_Year
                  ,BroadBand_and_Talk_Last_Year
                  ,Install_Offer_Last_Year
                  ,Others_Last_Year
                  ,Others_PPO_Last_Year
                  ,Service_Call_Last_Year
                  ,TV_Packs_Last_Year
                  ,Box_Offer_Last_6_Months
                  ,BroadBand_and_Talk_Last_6_Months
                  ,Install_Offer_Last_6_Months
                  ,Others_Last_6_Months
                  ,Others_PPO_Last_6_Months
                  ,Service_Call_Last_6_Months
                  ,TV_Packs_Last_6_Months
                  ,Box_Offer_Last_3_Months
                  ,BroadBand_and_Talk_Last_3_Months
                  ,Install_Offer_Last_3_Months
                  ,Others_Last_3_Months
                  ,Others_PPO_Last_3_Months
                  ,Service_Call_Last_3_Months
                  ,TV_Packs_Last_3_Months
                  ,price_protection_flag
                  ,Total_Expiring_Comms_Offers_Next_4_6_Months
                  ,Total_Expiring_Other_Offers_Next_4_6_Months
                  ,Total_Expiring_DTV_Offers_Next_4_6_Months
                  ,Total_Expiring_Offer_Value_Next_4_6_Months
                  ,Total_Expiring_Comms_Offers_Next_3_Months
                  ,Total_Expiring_Other_Offers_Next_3_Months
                  ,Total_Expiring_DTV_Offers_Next_3_Months
                  ,Total_Expiring_Offer_Value_Next_3_Months
              into #etl_main4
              from #etl_main3 as et3
                   left join #offers_output as oop on et3.account_number = oop.account_number

                -- filler
            select account_number
                  ,[reference]
                  ,dtv_first_act_date
                  ,dtv_latest_act_date
                  ,[10_Months_Prior]
                  ,[2_Years_Prior]
                  ,snapshot_date
                  ,skygo_distinct_activitydate_last90days
                  ,skygo_distinct_activitydate_last180days
                  ,skygo_distinct_activitydate_last270days
                  ,skygo_distinct_activitydate_last360days
                  ,case when IC_Calls_Last_Year is null then 0 else IC_Calls_Last_Year end as IC_Calls_Last_Year
                  ,case when IC_Calls_Last_3_Months is null then 0 else IC_Calls_Last_3_Months end as IC_Calls_Last_3_Months
                  ,case when IC_Calls_Last_6_Months is null then 0 else IC_Calls_Last_6_Months end as IC_Calls_Last_6_Months
                  ,case when IC_Calls_Last_9_Months is null then 0 else IC_Calls_Last_9_Months end as IC_Calls_Last_9_Months
                  ,case when PAT_Calls_Last_Year is null then 0 else PAT_Calls_Last_Year end as PAT_Calls_Last_Year
                  ,case when PAT_Calls_Last_3_Months is null then 0 else PAT_Calls_Last_3_Months end as PAT_Calls_Last_3_Months
                  ,case when PAT_Calls_Last_6_Months is null then 0 else PAT_Calls_Last_6_Months end as PAT_Calls_Last_6_Months
                  ,case when PAT_Calls_Last_9_Months is null then 0 else PAT_Calls_Last_9_Months end as PAT_Calls_Last_9_Months
                  ,case when TA_Calls_Last_Year is null then 0 else TA_Calls_Last_Year end as TA_Calls_Last_Year
                  ,case when TA_Calls_Last_3_Months is null then 0 else TA_Calls_Last_3_Months end as TA_Calls_Last_3_Months
                  ,case when TA_Calls_Last_6_Months is null then 0 else TA_Calls_Last_6_Months end as TA_Calls_Last_6_Months
                  ,case when TA_Calls_Last_9_Months is null then 0 else TA_Calls_Last_9_Months end as TA_Calls_Last_9_Months
                  ,case when PO_In_Next_4_Months_Flag is null then 0 else PO_In_Next_4_Months_Flag end as PO_In_Next_4_Months_Flag
                  ,case when TA_in_NEXT_3_months_flag is null then 0 else TA_in_NEXT_3_months_flag end as TA_in_NEXT_3_months_flag
                  ,case when AB_in_Next_3_Months_Flag is null then 0 else AB_in_Next_3_Months_Flag end as AB_in_Next_3_Months_Flag
                  ,case when SC_In_Next_4_Months_Flag is null then 0 else SC_In_Next_4_Months_Flag end as SC_In_Next_4_Months_Flag
                  ,case when IC_Calls_Last_2_Years is null then 0 else IC_Calls_Last_2_Years end as IC_Calls_Last_2_Years
                  ,case when TA_Calls_Last_2_Years is null then 0 else TA_Calls_Last_2_Years end as TA_Calls_Last_2_Years
                  ,case when PAT_Calls_Last_2_Years is null then 0 else PAT_Calls_Last_2_Years end as PAT_Calls_Last_2_Years
                  ,case when PO_Events_Last_Year is null then 0 else PO_Events_Last_Year end as PO_Events_Last_Year
                  ,case when PO_Events_Last_3_Months is null then 0 else PO_Events_Last_3_Months end as PO_Events_Last_3_Months
                  ,case when PO_Events_Last_6_Months is null then 0 else PO_Events_Last_6_Months end as PO_Events_Last_6_Months
                  ,case when PO_Events_Last_9_Months is null then 0 else PO_Events_Last_9_Months end as PO_Events_Last_9_Months
                  ,case when PO_Events_Last_2_Years is null then 0 else PO_Events_Last_2_Years end as PO_Events_Last_2_Years
                  ,case when AB_Events_Last_Year is null then 0 else AB_Events_Last_Year end as AB_Events_Last_Year
                  ,case when AB_Events_Last_3_Months is null then 0 else AB_Events_Last_3_Months end as AB_Events_Last_3_Months
                  ,case when AB_Events_Last_6_Months is null then 0 else AB_Events_Last_6_Months end as AB_Events_Last_6_Months
                  ,case when AB_Events_Last_9_Months is null then 0 else AB_Events_Last_9_Months end as AB_Events_Last_9_Months
                  ,case when AB_Events_Last_2_Years is null then 0 else AB_Events_Last_2_Years end as AB_Events_Last_2_Years
                  ,case when SC_Events_Last_Year is null then 0 else SC_Events_Last_Year end as SC_Events_Last_Year
                  ,case when SC_Events_Last_3_Months is null then 0 else SC_Events_Last_3_Months end as SC_Events_Last_3_Months
                  ,case when SC_Events_Last_6_Months is null then 0 else SC_Events_Last_6_Months end as SC_Events_Last_6_Months
                  ,case when SC_Events_Last_9_Months is null then 0 else SC_Events_Last_9_Months end as SC_Events_Last_9_Months
                  ,case when SC_Events_Last_2_Years is null then 0 else SC_Events_Last_2_Years end as SC_Events_Last_2_Years
                  ,case when PC_Events_Last_Year is null then 0 else PC_Events_Last_Year end as PC_Events_Last_Year
                  ,case when PC_Events_Last_3_Months is null then 0 else PC_Events_Last_3_Months end as PC_Events_Last_3_Months
                  ,case when PC_Events_Last_6_Months is null then 0 else PC_Events_Last_6_Months end as PC_Events_Last_6_Months
                  ,case when PC_Events_Last_9_Months is null then 0 else PC_Events_Last_9_Months end as PC_Events_Last_9_Months
                  ,case when PC_Events_Last_2_Years is null then 0 else PC_Events_Last_2_Years end as PC_Events_Last_2_Years
                  ,case when status_code_AB_effective_from_dt_Max_flag is null then 0 else status_code_AB_effective_from_dt_Max_flag end as AB_in_24m_flag
                  ,case when status_code_PO_effective_from_dt_Max_flag is null then 0 else status_code_PO_effective_from_dt_Max_flag end as cuscan_in_24m_flag
                  ,case when status_code_SC_effective_from_dt_Max_flag is null then 0 else status_code_SC_effective_from_dt_Max_flag end as syscan_in_24m_flag
                  ,case when Type_Of_Event_TA_event_dt_Max_flag is null then 0 else Type_Of_Event_TA_event_dt_Max_flag end as TA_in_24m_flag
                  ,case when TA_in_3_6_Months_Flag is null then 0 else TA_in_3_6_Months_Flag end as TA_in_3_6_Months_Flag
                  ,case when Box_Offer_Last_2_Years is null then 0 else Box_Offer_Last_2_Years end as Box_Offer_Last_2_Years
                  ,case when BroadBand_and_Talk_Last_2_Years is null then 0 else BroadBand_and_Talk_Last_2_Years end as BroadBand_and_Talk_Last_2_Years
                  ,case when Install_Offer_Last_2_Years is null then 0 else Install_Offer_Last_2_Years end as Install_Offer_Last_2_Years
                  ,case when Others_Last_2_Years is null then 0 else Others_Last_2_Years end as Others_Last_2_Years
                  ,case when Others_PPO_Last_2_Years is null then 0 else Others_PPO_Last_2_Years end as Others_PPO_Last_2_Years
                  ,case when Service_Call_Last_2_Years is null then 0 else Service_Call_Last_2_Years end as Service_Call_Last_2_Years
                  ,case when TV_Packs_Last_2_Years is null then 0 else TV_Packs_Last_2_Years end as TV_Packs_Last_2_Years
                  ,case when Box_Offer_Last_Year is null then 0 else Box_Offer_Last_Year end as Box_Offer_Last_Year
                  ,case when BroadBand_and_Talk_Last_Year is null then 0 else BroadBand_and_Talk_Last_Year end as BroadBand_and_Talk_Last_Year
                  ,case when Install_Offer_Last_Year is null then 0 else Install_Offer_Last_Year end as Install_Offer_Last_Year
                  ,case when Others_Last_Year is null then 0 else Others_Last_Year end as Others_Last_Year
                  ,case when Others_PPO_Last_Year is null then 0 else Others_PPO_Last_Year end as Others_PPO_Last_Year
                  ,case when Service_Call_Last_Year is null then 0 else Service_Call_Last_Year end as Service_Call_Last_Year
                  ,case when TV_Packs_Last_Year is null then 0 else TV_Packs_Last_Year end as TV_Packs_Last_Year
                  ,case when Box_Offer_Last_6_Months is null then 0 else Box_Offer_Last_6_Months end as Box_Offer_Last_6_Months
                  ,case when BroadBand_and_Talk_Last_6_Months is null then 0 else BroadBand_and_Talk_Last_6_Months end as BroadBand_and_Talk_Last_6_Months
                  ,case when Install_Offer_Last_6_Months is null then 0 else Install_Offer_Last_6_Months end as Install_Offer_Last_6_Months
                  ,case when Others_Last_6_Months is null then 0 else Others_Last_6_Months end as Others_Last_6_Months
                  ,case when Others_PPO_Last_6_Months is null then 0 else Others_PPO_Last_6_Months end as Others_PPO_Last_6_Months
                  ,case when Service_Call_Last_6_Months is null then 0 else Service_Call_Last_6_Months end as Service_Call_Last_6_Months
                  ,case when TV_Packs_Last_6_Months is null then 0 else TV_Packs_Last_6_Months end as TV_Packs_Last_6_Months
                  ,case when Box_Offer_Last_3_Months is null then 0 else Box_Offer_Last_3_Months end as Box_Offer_Last_3_Months
                  ,case when BroadBand_and_Talk_Last_3_Months is null then 0 else BroadBand_and_Talk_Last_3_Months end as BroadBand_and_Talk_Last_3_Months
                  ,case when Install_Offer_Last_3_Months is null then 0 else Install_Offer_Last_3_Months end as Install_Offer_Last_3_Months
                  ,case when Others_Last_3_Months is null then 0 else Others_Last_3_Months end as Others_Last_3_Months
                  ,case when Others_PPO_Last_3_Months is null then 0 else Others_PPO_Last_3_Months end as Others_PPO_Last_3_Months
                  ,case when Service_Call_Last_3_Months is null then 0 else Service_Call_Last_3_Months end as Service_Call_Last_3_Months
                  ,case when TV_Packs_Last_3_Months is null then 0 else TV_Packs_Last_3_Months end as TV_Packs_Last_3_Months
                  ,case when price_protection_flag is null then 0 else price_protection_flag end as price_protection_flag
                  ,case when Total_Expiring_Comms_Offers_Next_4_6_Months is null then 0 else Total_Expiring_Comms_Offers_Next_4_6_Months end as Total_Expiring_Comms_Offers_Next_4_6_Months
                  ,case when Total_Expiring_Other_Offers_Next_4_6_Months is null then 0 else Total_Expiring_Other_Offers_Next_4_6_Months end as Total_Expiring_Other_Offers_Next_4_6_Months
                  ,case when Total_Expiring_DTV_Offers_Next_4_6_Months is null then 0 else Total_Expiring_DTV_Offers_Next_4_6_Months end as Total_Expiring_DTV_Offers_Next_4_6_Months
                  ,case when Total_Expiring_Offer_Value_Next_4_6_Months is null then 0 else Total_Expiring_Offer_Value_Next_4_6_Months end as Total_Expiring_Offer_Value_Next_4_6_Months
                  ,case when Total_Expiring_Comms_Offers_Next_3_Months is null then 0 else Total_Expiring_Comms_Offers_Next_3_Months end as Total_Expiring_Comms_Offers_Next_3_Months
                  ,case when Total_Expiring_Other_Offers_Next_3_Months is null then 0 else Total_Expiring_Other_Offers_Next_3_Months end as Total_Expiring_Other_Offers_Next_3_Months
                  ,case when Total_Expiring_DTV_Offers_Next_3_Months is null then 0 else Total_Expiring_DTV_Offers_Next_3_Months end as Total_Expiring_DTV_Offers_Next_3_Months
                  ,case when Total_Expiring_Offer_Value_Next_3_Months is null then 0 else Total_Expiring_Offer_Value_Next_3_Months end as Total_Expiring_Offer_Value_Next_3_Months
                  ,Date_of_Last_IC_Call
                  ,Date_of_Last_PAT_Call
                  ,Date_of_Last_TA_Call
                  ,od_distinct_activitydate_last90days
                  ,od_distinct_activitydate_last180days
                  ,od_distinct_activitydate_last270days
                  ,mr
                  ,ThreeDTV
                  ,HDTV
                  ,TopTier
                  ,Movies
                  ,Movies_upgrade_last_30days
                  ,Movies_upgrade_last_60days
                  ,Movies_upgrade_last_90days
                  ,Movies_upgrade_last_180days
                  ,Movies_downgrade_last_30days
                  ,Movies_downgrade_last_60days
                  ,Movies_downgrade_last_90days
                  ,Movies_downgrade_last_180days
                  ,Sports
                  ,Sports_upgrade_last_30days
                  ,Sports_upgrade_last_60days
                  ,Sports_upgrade_last_90days
                  ,Sports_upgrade_last_180days
                  ,Sports_downgrade_last_30days
                  ,Sports_downgrade_last_60days
                  ,Sports_downgrade_last_90days
                  ,Sports_downgrade_last_180days
                  ,product_holding
                  ,cvs_segment
              into #etl_main5
              from #etl_main4

                -- segment derivations
            select *
              into #segment_derivations1
              from #etl_main5

                -- sum unstable flags
            select *
                  ,AB_in_24m_flag + cuscan_in_24m_flag + syscan_in_24m_flag + TA_in_24m_flag as sum_unstable_flags
              into #segment_derivations2
              from #segment_derivations1

                -- filter
            select *
              into #segment_derivations3
              from #segment_derivations2

                -- segment
            select *
                  ,case when dtv_first_act_date > [10_Months_Prior] then '<10_Months'
                        when dtv_first_act_date <= [10_Months_Prior] and dtv_first_act_date > [2_Years_Prior] then '10-24_Months'
                        when dtv_first_act_date <= [2_Years_Prior] and sum_unstable_flags = 0 then '24_Months+'
                        else 'Unstable' end as segment
              into #segment_derivations4
              from #segment_derivations3

                -- output
            insert into TA_MODELING_RAW_DATA(
                   account_number
                  ,[reference]
                  ,dtv_first_act_date
                  ,[10_Months_Prior]
                  ,[2_Years_Prior]
                  ,snapshot_date
                  ,skygo_distinct_activitydate_last90days
                  ,skygo_distinct_activitydate_last180days
                  ,skygo_distinct_activitydate_last270days
                  ,skygo_distinct_activitydate_last360days
                  ,IC_Calls_Last_Year
                  ,IC_Calls_Last_3_Months
                  ,IC_Calls_Last_6_Months
                  ,IC_Calls_Last_9_Months
                  ,PAT_Calls_Last_Year
                  ,PAT_Calls_Last_3_Months
                  ,PAT_Calls_Last_6_Months
                  ,PAT_Calls_Last_9_Months
                  ,TA_Calls_Last_Year
                  ,TA_Calls_Last_3_Months
                  ,TA_Calls_Last_6_Months
                  ,TA_Calls_Last_9_Months
                  ,PO_In_Next_4_Months_Flag
                  ,TA_in_NEXT_3_months_flag
                  ,AB_in_Next_3_Months_Flag
                  ,SC_In_Next_4_Months_Flag
                  ,IC_Calls_Last_2_Years
                  ,TA_Calls_Last_2_Years
                  ,PAT_Calls_Last_2_Years
                  ,PO_Events_Last_Year
                  ,PO_Events_Last_3_Months
                  ,PO_Events_Last_6_Months
                  ,PO_Events_Last_9_Months
                  ,PO_Events_Last_2_Years
                  ,AB_Events_Last_Year
                  ,AB_Events_Last_3_Months
                  ,AB_Events_Last_6_Months
                  ,AB_Events_Last_9_Months
                  ,AB_Events_Last_2_Years
                  ,SC_Events_Last_Year
                  ,SC_Events_Last_3_Months
                  ,SC_Events_Last_6_Months
                  ,SC_Events_Last_9_Months
                  ,SC_Events_Last_2_Years
                  ,PC_Events_Last_Year
                  ,PC_Events_Last_3_Months
                  ,PC_Events_Last_6_Months
                  ,PC_Events_Last_9_Months
                  ,PC_Events_Last_2_Years
                  ,AB_in_24m_flag
                  ,cuscan_in_24m_flag
                  ,syscan_in_24m_flag
                  ,TA_in_24m_flag
                  ,TA_in_3_6_Months_Flag
                  ,Box_Offer_Last_2_Years
                  ,BroadBand_and_Talk_Last_2_Years
                  ,Install_Offer_Last_2_Years
                  ,Others_Last_2_Years
                  ,Others_PPO_Last_2_Years
                  ,Service_Call_Last_2_Years
                  ,TV_Packs_Last_2_Years
                  ,Box_Offer_Last_Year
                  ,BroadBand_and_Talk_Last_Year
                  ,Install_Offer_Last_Year
                  ,Others_Last_Year
                  ,Others_PPO_Last_Year
                  ,Service_Call_Last_Year
                  ,TV_Packs_Last_Year
                  ,Box_Offer_Last_6_Months
                  ,BroadBand_and_Talk_Last_6_Months
                  ,Install_Offer_Last_6_Months
                  ,Others_Last_6_Months
                  ,Others_PPO_Last_6_Months
                  ,Service_Call_Last_6_Months
                  ,TV_Packs_Last_6_Months
                  ,Box_Offer_Last_3_Months
                  ,BroadBand_and_Talk_Last_3_Months
                  ,Install_Offer_Last_3_Months
                  ,Others_Last_3_Months
                  ,Others_PPO_Last_3_Months
                  ,Service_Call_Last_3_Months
                  ,TV_Packs_Last_3_Months
                  ,price_protection_flag
                  ,Total_Expiring_Comms_Offers_Next_4_6_Months
                  ,Total_Expiring_Other_Offers_Next_4_6_Months
                  ,Total_Expiring_DTV_Offers_Next_4_6_Months
                  ,Total_Expiring_Offer_Value_Next_4_6_Months
                  ,Total_Expiring_Comms_Offers_Next_3_Months
                  ,Total_Expiring_Other_Offers_Next_3_Months
                  ,Total_Expiring_DTV_Offers_Next_3_Months
                  ,Total_Expiring_Offer_Value_Next_3_Months
                  ,sum_unstable_flags
                  ,segment
                  ,od_distinct_activity_last90days
                  ,od_distinct_activity_last180days
                  ,od_distinct_activity_last270days
                  ,MR
                  ,ThreeDTV
                  ,HDTV
                  ,TopTier
                  ,Movies
                  ,Sports
                  ,Movies_upgrade_last_30days
                  ,Movies_upgrade_last_60days
                  ,Movies_upgrade_last_90days
                  ,Movies_upgrade_last_180days
                  ,Movies_downgrade_last_30days
                  ,Movies_downgrade_last_60days
                  ,Movies_downgrade_last_90days
                  ,Movies_downgrade_last_180days
                  ,Sports_upgrade_last_30days
                  ,Sports_upgrade_last_60days
                  ,Sports_upgrade_last_90days
                  ,Sports_upgrade_last_180days
                  ,Sports_downgrade_last_30days
                  ,Sports_downgrade_last_60days
                  ,Sports_downgrade_last_90days
                  ,Sports_downgrade_last_180days
                  ,dtv_latest_act_date
                  ,Date_of_Last_TA_Call
                  ,product_holding
                  ,cvs_segment
                  ,Date_of_Last_PAT_Call
                  ,Date_of_Last_IC_Call
                   )
            select account_number
                  ,[reference]
                  ,dtv_first_act_date
                  ,[10_Months_Prior]
                  ,[2_Years_Prior]
                  ,snapshot_date
                  ,skygo_distinct_activitydate_last90days
                  ,skygo_distinct_activitydate_last180days
                  ,skygo_distinct_activitydate_last270days
                  ,skygo_distinct_activitydate_last360days
                  ,IC_Calls_Last_Year
                  ,IC_Calls_Last_3_Months
                  ,IC_Calls_Last_6_Months
                  ,IC_Calls_Last_9_Months
                  ,PAT_Calls_Last_Year
                  ,PAT_Calls_Last_3_Months
                  ,PAT_Calls_Last_6_Months
                  ,PAT_Calls_Last_9_Months
                  ,TA_Calls_Last_Year
                  ,TA_Calls_Last_3_Months
                  ,TA_Calls_Last_6_Months
                  ,TA_Calls_Last_9_Months
                  ,PO_In_Next_4_Months_Flag
                  ,TA_in_NEXT_3_months_flag
                  ,AB_in_Next_3_Months_Flag
                  ,SC_In_Next_4_Months_Flag
                  ,IC_Calls_Last_2_Years
                  ,TA_Calls_Last_2_Years
                  ,PAT_Calls_Last_2_Years
                  ,PO_Events_Last_Year
                  ,PO_Events_Last_3_Months
                  ,PO_Events_Last_6_Months
                  ,PO_Events_Last_9_Months
                  ,PO_Events_Last_2_Years
                  ,AB_Events_Last_Year
                  ,AB_Events_Last_3_Months
                  ,AB_Events_Last_6_Months
                  ,AB_Events_Last_9_Months
                  ,AB_Events_Last_2_Years
                  ,SC_Events_Last_Year
                  ,SC_Events_Last_3_Months
                  ,SC_Events_Last_6_Months
                  ,SC_Events_Last_9_Months
                  ,SC_Events_Last_2_Years
                  ,PC_Events_Last_Year
                  ,PC_Events_Last_3_Months
                  ,PC_Events_Last_6_Months
                  ,PC_Events_Last_9_Months
                  ,PC_Events_Last_2_Years
                  ,AB_in_24m_flag
                  ,cuscan_in_24m_flag
                  ,syscan_in_24m_flag
                  ,TA_in_24m_flag
                  ,TA_in_3_6_Months_Flag
                  ,Box_Offer_Last_2_Years
                  ,BroadBand_and_Talk_Last_2_Years
                  ,Install_Offer_Last_2_Years
                  ,Others_Last_2_Years
                  ,Others_PPO_Last_2_Years
                  ,Service_Call_Last_2_Years
                  ,TV_Packs_Last_2_Years
                  ,Box_Offer_Last_Year
                  ,BroadBand_and_Talk_Last_Year
                  ,Install_Offer_Last_Year
                  ,Others_Last_Year
                  ,Others_PPO_Last_Year
                  ,Service_Call_Last_Year
                  ,TV_Packs_Last_Year
                  ,Box_Offer_Last_6_Months
                  ,BroadBand_and_Talk_Last_6_Months
                  ,Install_Offer_Last_6_Months
                  ,Others_Last_6_Months
                  ,Others_PPO_Last_6_Months
                  ,Service_Call_Last_6_Months
                  ,TV_Packs_Last_6_Months
                  ,Box_Offer_Last_3_Months
                  ,BroadBand_and_Talk_Last_3_Months
                  ,Install_Offer_Last_3_Months
                  ,Others_Last_3_Months
                  ,Others_PPO_Last_3_Months
                  ,Service_Call_Last_3_Months
                  ,TV_Packs_Last_3_Months
                  ,price_protection_flag
                  ,Total_Expiring_Comms_Offers_Next_4_6_Months
                  ,Total_Expiring_Other_Offers_Next_4_6_Months
                  ,Total_Expiring_DTV_Offers_Next_4_6_Months
                  ,Total_Expiring_Offer_Value_Next_4_6_Months
                  ,Total_Expiring_Comms_Offers_Next_3_Months
                  ,Total_Expiring_Other_Offers_Next_3_Months
                  ,Total_Expiring_DTV_Offers_Next_3_Months
                  ,Total_Expiring_Offer_Value_Next_3_Months
                  ,sum_unstable_flags
                  ,segment
                  ,od_distinct_activitydate_last90days as od_distinct_activity_last90days
                  ,od_distinct_activitydate_last180days as od_distinct_activity_last180days
                  ,od_distinct_activitydate_last270days as od_distinct_activity_last270days
                  ,MR
                  ,ThreeDTV
                  ,HDTV
                  ,TopTier
                  ,Movies
                  ,Sports
                  ,Movies_upgrade_last_30days
                  ,Movies_upgrade_last_60days
                  ,Movies_upgrade_last_90days
                  ,Movies_upgrade_last_180days
                  ,Movies_downgrade_last_30days
                  ,Movies_downgrade_last_60days
                  ,Movies_downgrade_last_90days
                  ,Movies_downgrade_last_180days
                  ,Sports_upgrade_last_30days
                  ,Sports_upgrade_last_60days
                  ,Sports_upgrade_last_90days
                  ,Sports_upgrade_last_180days
                  ,Sports_downgrade_last_30days
                  ,Sports_downgrade_last_60days
                  ,Sports_downgrade_last_90days
                  ,Sports_downgrade_last_180days
                  ,dtv_latest_act_date
                  ,Date_of_Last_TA_Call
                  ,product_holding
                  ,cvs_segment
                  ,Date_of_Last_PAT_Call
                  ,Date_of_Last_IC_Call
              from #segment_derivations4

              drop table #table_churn_history1
              drop table #table_churn_history2
              drop table #table_churn_history3
              drop table #table_churn_history4
              drop table #table_churn_history_output
              drop table #TA_call_history1
              drop table #TA_call_history2
              drop table #TA_call_history3
              drop table #TA_call_history4
              drop table #TA_call_history_output
              drop table #data_mart1
              drop table #data_mart2
              drop table #data_mart3
              drop table #data_mart4
              drop table #data_mart5
              drop table #data_mart6
              drop table #data_mart7
              drop table #data_mart8
              drop table #data_mart9
              drop table #data_mart10
              drop table #data_mart11
              drop table #data_mart12
              drop table #data_mart_output
              drop table #futureTA1
              drop table #futureTA2
              drop table #futureTA3
              drop table #futureTA4
              drop table #futureTA5
              drop table #futureTA6
              drop table #futureTA7
              drop table #futureTA8
              drop table #futureTA_output
              drop table #futureAB1
              drop table #futureAB2
              drop table #futureAB3
              drop table #futureAB4
              drop table #futureAB5
              drop table #futureAB6
              drop table #futureAB7
              drop table #futureAB8
              drop table #futureAB_output
              drop table #futureSC1
              drop table #futureSC2
              drop table #futureSC3
              drop table #futureSC4
              drop table #futureSC5
              drop table #futureSC6
              drop table #futureSC7
              drop table #futureSC8
              drop table #futureSC_output
              drop table #futurePO1
              drop table #futurePO2
              drop table #futurePO3
              drop table #futurePO4
              drop table #futurePO5
              drop table #futurePO6
              drop table #futurePO7
              drop table #futurePO8
              drop table #futurePO_output
              drop table #futureTA_4_6_m1
              drop table #futureTA_4_6_m2
              drop table #futureTA_4_6_m3
              drop table #futureTA_4_6_m4
              drop table #futureTA_4_6_m5
              drop table #futureTA_4_6_m6
              drop table #futureTA_4_6_m7
              drop table #futureTA_4_6_m8
              drop table #futureTA_4_6_m_output
              drop table #future_calling_churn1
              drop table #future_calling_churn2
              drop table #future_calling_churn3
              drop table #future_calling_churn4
              drop table #TA_calls_in1
              drop table #TA_calls_in2
              drop table #TA_calls_in3
              drop table #TA_calls_in_output
              drop table #TA_calls1
              drop table #TA_calls2
              drop table #TA_calls3
              drop table #TA_calls4
              drop table #TA_calls5
              drop table #TA_calls6
              drop table #TA_calls7
              drop table #TA_calls8
              drop table #TA_calls9
              drop table #TA_calls10
              drop table #TA_calls11
              drop table #TA_calls12
              drop table #TA_calls13
              drop table #TA_calls_output
              drop table #PAT_calls_in1
              drop table #PAT_calls_in2
              drop table #PAT_calls_in3
              drop table #PAT_calls_in_output
              drop table #PAT_calls1
              drop table #PAT_calls2
              drop table #PAT_calls3
              drop table #PAT_calls4
              drop table #PAT_calls5
              drop table #PAT_calls6
              drop table #PAT_calls7
              drop table #PAT_calls8
              drop table #PAT_calls9
              drop table #PAT_calls10
              drop table #PAT_calls11
              drop table #PAT_calls12
              drop table #PAT_calls13
              drop table #PAT_calls_output
              drop table #IC_calls_in1
              drop table #IC_calls_in2
              drop table #IC_calls_in3
              drop table #IC_calls_in_output
              drop table #IC_calls1
              drop table #IC_calls2
              drop table #IC_calls3
              drop table #IC_calls4
              drop table #IC_calls5
              drop table #IC_calls6
              drop table #IC_calls7
              drop table #IC_calls8
              drop table #IC_calls9
              drop table #IC_calls10
              drop table #IC_calls11
              drop table #IC_calls12
              drop table #IC_calls13
              drop table #IC_calls_output
              drop table #HistoricalCallingBehaviour1
              drop table #HistoricalCallingBehaviour_output
              drop table #SC_events_in1
              drop table #SC_events_in2
              drop table #SC_events_in3
              drop table #SC_events_in_output
              drop table #SC_events1
              drop table #SC_events2
              drop table #SC_events3
              drop table #SC_events4
              drop table #SC_events5
              drop table #SC_events6
              drop table #SC_events7
              drop table #SC_events8
              drop table #SC_events9
              drop table #SC_events10
              drop table #SC_events11
              drop table #SC_events12
              drop table #SC_events13
              drop table #SC_events_output
              drop table #AB_events_in1
              drop table #AB_events_in2
              drop table #AB_events_in3
              drop table #AB_events_in_output
              drop table #AB_events1
              drop table #AB_events2
              drop table #AB_events3
              drop table #AB_events4
              drop table #AB_events5
              drop table #AB_events6
              drop table #AB_events7
              drop table #AB_events8
              drop table #AB_events9
              drop table #AB_events10
              drop table #AB_events11
              drop table #AB_events12
              drop table #AB_events13
              drop table #AB_events_output
              drop table #PO_events_in1
              drop table #PO_events_in2
              drop table #PO_events_in3
              drop table #PO_events_in_output
              drop table #PO_events1
              drop table #PO_events2
              drop table #PO_events3
              drop table #PO_events4
              drop table #PO_events5
              drop table #PO_events6
              drop table #PO_events7
              drop table #PO_events8
              drop table #PO_events9
              drop table #PO_events10
              drop table #PO_events11
              drop table #PO_events12
              drop table #PO_events13
              drop table #PO_events_output
              drop table #PC_events_in1
              drop table #PC_events_in2
              drop table #PC_events_in3
              drop table #PC_events_in_output
              drop table #PC_events1
              drop table #PC_events2
              drop table #PC_events3
              drop table #PC_events4
              drop table #PC_events5
              drop table #PC_events6
              drop table #PC_events7
              drop table #PC_events8
              drop table #PC_events9
              drop table #PC_events10
              drop table #PC_events11
              drop table #PC_events12
              drop table #PC_events13
              drop table #PC_events_output
              drop table #HistoricalChurnBehaviour1
              drop table #HistoricalChurnBehaviour_output
              drop table #offers1
              drop table #offers2
              drop table #offers3
--              drop table --#offers4
              drop table #offers5
              drop table #offers6
              drop table #offers7
              drop table #offers8
              drop table #offers9
              drop table #offers10
              drop table #offers11
              drop table #offers12
              drop table #offers13
              drop table #offers14
              drop table #offers15
              drop table #offers16
              drop table #offers17
              drop table #offers18
              drop table #offers19
              drop table #offers20
              drop table #offers21
              drop table #offers22
              drop table #offers23
              drop table #offers24
              drop table #offers25
              drop table #offers26
              drop table #offers27
              drop table #offers28
              drop table #offers29
              drop table #offers30
              drop table #offers31
              drop table #offers32
              drop table #offers33
              drop table #offers34
              drop table #offers35
              drop table #offers36
              drop table #offers37
              drop table #offers38
              drop table #offers39
              drop table #offers40
              drop table #offers41
              drop table #offers42
              drop table #offers43
              drop table #offers44
              drop table #offers45
              drop table #offers46
              drop table #offers_output
              drop table #etl_main1
              drop table #etl_main2
              drop table #etl_main3
              drop table #etl_main4
              drop table #etl_main5
              drop table #segment_derivations1
              drop table #segment_derivations2
              drop table #segment_derivations3
              drop table #segment_derivations4

               set @reference = @reference + 1
            commit
     end
;
select [reference],count() from TA_MODELING_RAW_DATA group by [reference]



      -- new section to calcultae upgareds and downgrades
  select account_number
    into #accounts
    from TA_MODELING_RAW_DATA
group by account_number
;

  create unique hg index uhacc on #accounts(account_number);

  select [reference]
        ,cast(null as date) as dt
    into #refs
    from TA_MODELING_RAW_DATA
group by [reference]
        ,dt
;

  create date index dtdte on #refs([dt]);

  update #refs
     set dt = cast(left(cast([reference] as varchar), 4) || '-' || right(cast([reference] as varchar),2) || '-01' as date) - 2
;

  select csh.account_number
        ,effective_from_dt
        ,oce.prem_sports as old_sports
        ,nce.prem_sports as new_sports
        ,oce.prem_movies as old_movies
        ,nce.prem_movies as new_movies
    into #sub_changes
    from cust_subs_hist as csh
         inner join #accounts as acc on csh.account_number = acc.account_number
         inner join cust_entitlement_lookup as nce on csh.current_short_description  = nce.short_description
         inner join cust_entitlement_lookup as oce on csh.previous_short_description = oce.short_description
   where subscription_sub_type ='DTV Primary Viewing'
     and status_code in ('AC','AB','PC')
     and effective_from_dt < effective_to_dt
;

create hg index hgacc on #sub_changes(account_number);

  select account_number
        ,dt
        ,max(effective_from_dt)                                   as last_dt
        ,sum(case when effective_from_dt <= dt then 1 else 0 end) as num_ever
    into #changes
    from #sub_changes
         cross join #refs
   where new_sports < old_sports
group by account_number
        ,dt
;

create hg index hgacc on #changes(account_number);

  update TA_MODELING_RAW_DATA as bas
     set bas.last_sports_downgrades_dt   = cha.last_dt
        ,bas.num_sports_downgrades_ever  = cha.num_ever
    from #changes as cha
   where bas.account_number = cha.account_number
     and cast(left(cast([reference] as varchar), 4) || '-' || right(cast([reference] as varchar),2) || '-01' as date) - 2 = dt
;

truncate table #changes;

  insert into #changes
  select account_number
        ,dt
        ,max(effective_from_dt)                                   as last_dt
        ,sum(case when effective_from_dt <= dt then 1 else 0 end) as num_ever
    from #sub_changes
         cross join #refs
   where new_sports > old_sports
group by account_number
        ,dt
;

  update TA_MODELING_RAW_DATA as bas
     set bas.last_sports_upgrades_dt   = cha.last_dt
        ,bas.num_sports_upgrades_ever  = cha.num_ever
    from #changes as cha
   where bas.account_number = cha.account_number
     and cast(left(cast([reference] as varchar), 4) || '-' || right(cast([reference] as varchar),2) || '-01' as date) - 2 = dt
;

truncate table #changes;

  insert into #changes
  select account_number
        ,dt
        ,max(effective_from_dt)                                   as last_dt
        ,sum(case when effective_from_dt <= dt then 1 else 0 end) as num_ever
    from #sub_changes
         cross join #refs
   where new_movies > old_movies
group by account_number
        ,dt
;

  update TA_MODELING_RAW_DATA as bas
     set bas.last_movies_upgrades_dt = cha.last_dt
        ,bas.num_movies_upgrades_ever  = cha.num_ever
    from #changes as cha
   where bas.account_number = cha.account_number
     and cast(left(cast([reference] as varchar), 4) || '-' || right(cast([reference] as varchar),2) || '-01' as date) - 2 = dt
;

truncate table #changes;

  insert into #changes
  select account_number
        ,dt
        ,max(effective_from_dt)                                   as last_dt
        ,sum(case when effective_from_dt <= dt then 1 else 0 end) as num_ever
    from #sub_changes
         cross join #refs
   where new_movies < old_movies
group by account_number
        ,dt
;

  update TA_MODELING_RAW_DATA as bas
     set bas.last_movies_downgrades_dt   = cha.last_dt
        ,bas.num_movies_downgrades_ever  = cha.num_ever
    from #changes as cha
   where bas.account_number = cha.account_number
     and cast(left(cast([reference] as varchar), 4) || '-' || right(cast([reference] as varchar),2) || '-01' as date) - 2 = dt
;

  update TA_MODELING_RAW_DATA as bas
     set num_movies_downgrades_ever = coalesce(num_movies_downgrades_ever, 0)
        ,num_sports_downgrades_ever = coalesce(num_sports_downgrades_ever, 0)
        ,num_movies_upgrades_ever = coalesce(num_movies_upgrades_ever, 0)
        ,num_sports_upgrades_ever = coalesce(num_sports_upgrades_ever, 0)
;

  select account_number
        ,snapshot_date
        ,cast(0 as bit) as tv
        ,cast(0 as bit) as bb
        ,cast(0 as bit) as st
    into #products
    from TA_MODELING_RAW_DATA
;

  update #products as bas
     set tv = 1
    from cust_subs_hist as csh
   where bas.account_number = csh.account_number
     and subscription_sub_type = 'DTV Primary Viewing'
     and effective_from_dt <= snapshot_date
     and effective_to_dt   >  snapshot_date
     and status_code in ('AC', 'AB', 'PC')
;

  update #products as bas
     set bb = 1
    from cust_subs_hist as csh
   where bas.account_number = csh.account_number
     and subscription_sub_type = 'Broadband DSL Line'
     and effective_from_dt <= snapshot_date
     and effective_to_dt   >  snapshot_date
     and (status_code in ('AC','AB') or (status_code='PC' and prev_status_code not in ('?','RQ','AP','UB','BE','PA') )
          or (status_code='CF' and prev_status_code='PC')
          or (status_code='AP' and sale_type='SNS Bulk Migration'))
;

  update #products as bas
     set st = 1
    from cust_subs_hist as csh
   where bas.account_number = csh.account_number
     and (subscription_type = 'SKY TALK' or subscription_sub_type ='SKY TALK LINE RENTAL')
     and status_code in  ('A', 'L', 'RI')
     and effective_from_dt <= snapshot_date
     and effective_to_dt   >  snapshot_date
;

  update TA_MODELING_RAW_DATA as bas
     set product_holding = case when tv = 1 and bb = 0 and st = 0 then 'A. DTV Only'
                                when tv = 1 and bb = 1 and st = 1 then 'B. DTV + Triple play'
                                when tv = 1 and bb = 1 and st = 0 then 'C. DTV + BB Only'
                                when tv = 1 and bb = 0 and st = 1 then 'D. DTV + Other Comms'
                                when tv = 0 then 'E. SABB'
                            end
    from #products as pro
   where bas.account_number = pro.account_number
     and bas.snapshot_date = pro.snapshot_date
;


--select * into TA_MODELING_RAW_DATA_bak from TA_MODELING_RAW_DATA
--truncate table TA_MODELING_RAW_DATA
select top 10 * from TA_MODELING_RAW_DATA







---
select max(date_of_last_ic_Call) from ta_modeling_raw_data
select top 100 * from ta_modeling_raw_data























grant select on skybase_scores to tanghoi
select top 10 * from skybase_scores


