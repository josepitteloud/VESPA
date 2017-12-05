drop table accs;
drop table accounts;
drop table ta_modeling_raw_data;

  create variable @Reference integer;
     set @Reference = 201311;
  create variable @records int;
  create variable multiplier bigint;

  create table #count(
         account_number varchar(30)
        ,cow            int
        )
;

  create table #dates(
         account_number varchar(30)
        ,dt             date
        )
;

  create table ta_modeling_raw_data(
         account_number                               varchar(30)
        ,ref                                          int default 0
        ,ic_calls_last_3_months                       int default 0
        ,ic_calls_last_6_months                       int default 0
        ,ic_calls_last_9_months                       int default 0
        ,ic_calls_last_year                           int default 0
        ,ic_calls_last_2_years                        int default 0
        ,pat_calls_last_3_months                      int default 0
        ,pat_calls_last_6_months                      int default 0
        ,pat_calls_last_9_months                      int default 0
        ,pat_calls_last_year                          int default 0
        ,pat_calls_last_2_years                       int default 0
        ,ta_calls_last_3_months                       int default 0
        ,ta_calls_last_6_months                       int default 0
        ,ta_calls_last_9_months                       int default 0
        ,ta_calls_last_year                           int default 0
        ,ta_calls_last_2_years                        int default 0
        ,ta_in_next_3_months_flag                     bit default 0
        ,ta_in_3_6_months_flag                        bit default 0
        ,ta_in_24m_flag                               bit default 0
        ,po_events_last_3_months                      int default 0
        ,po_events_last_6_months                      int default 0
        ,po_events_last_9_months                      int default 0
        ,po_events_last_year                          int default 0
        ,po_events_last_2_years                       int default 0
        ,cuscan_in_24m_flag                           bit default 0
        ,po_in_next_3_months_flag                     bit default 0
        ,ab_events_last_3_months                      int default 0
        ,ab_events_last_6_months                      int default 0
        ,ab_events_last_9_months                      int default 0
        ,ab_events_last_year                          int default 0
        ,ab_events_last_2_years                       int default 0
        ,ab_in_24m_flag                               bit default 0
        ,ab_in_next_3_months_flag                     bit default 0
        ,sc_events_last_3_months                      int default 0
        ,sc_events_last_6_months                      int default 0
        ,sc_events_last_9_months                      int default 0
        ,sc_events_last_year                          int default 0
        ,sc_events_last_2_years                       int default 0
        ,syscan_in_24m_flag                           bit default 0
        ,sc_in_next_3_months_flag                     bit default 0
        ,pc_events_last_3_months                      int default 0
        ,pc_events_last_6_months                      int default 0
        ,pc_events_last_9_months                      int default 0
        ,pc_events_last_year                          int default 0
        ,pc_events_last_2_years                       int default 0
        ,tv_packs_last_2_years                        int default 0
        ,box_offer_last_3_months                      int default 0
        ,box_offer_last_6_months                      int default 0
        ,box_offer_last_year                          int default 0
        ,box_offer_last_2_years                       int default 0
        ,broadband_and_talk_last_3_months             int default 0
        ,broadband_and_talk_last_6_months             int default 0
        ,broadband_and_talk_last_year                 int default 0
        ,broadband_and_talk_last_2_years              int default 0
        ,install_offer_last_3_months                  int default 0
        ,install_offer_last_6_months                  int default 0
        ,install_offer_last_year                      int default 0
        ,install_offer_last_2_years                   int default 0
        ,others_last_3_months                         int default 0
        ,others_last_6_months                         int default 0
        ,others_last_year                             int default 0
        ,others_last_2_years                          int default 0
        ,others_ppo_last_3_months                     int default 0
        ,others_ppo_last_6_months                     int default 0
        ,others_ppo_last_year                         int default 0
        ,others_ppo_last_2_years                      int default 0
        ,price_protection_flag                        bit default 0
        ,service_call_last_3_months                   int default 0
        ,service_call_last_6_months                   int default 0
        ,service_call_last_year                       int default 0
        ,service_call_last_2_years                    int default 0
        ,tv_packs_last_3_months                       int default 0
        ,tv_packs_last_6_months                       int default 0
        ,tv_packs_last_year                           int default 0
        ,total_expiring_comms_offers_next_3_months    int default 0
        ,total_expiring_other_offers_next_3_months    int default 0
        ,total_expiring_dtv_offers_next_3_months      int default 0
        ,total_expiring_offer_value_next_3_months     decimal(10,2) default 0
        ,total_expiring_comms_offers_next_4_6_months  int default 0
        ,total_expiring_other_offers_next_4_6_months  int default 0
        ,total_expiring_dtv_offers_next_4_6_months    int default 0
        ,total_expiring_offer_value_next_4_6_months   decimal(10,2) default 0
        ,sum_unstable_flags                           int default 0
        ,segment                                      varchar(30)
        ,num_movies_downgrades_ever                   int default 0
        ,num_movies_upgrades_ever                     int default 0
        ,num_sports_downgrades_ever                   int default 0
        ,num_sports_upgrades_ever                     int default 0
        ,last_movies_downgrades_dt                    date
        ,last_movies_upgrades_dt                      date
        ,last_sports_downgrades_dt                    date
        ,last_sports_upgrades_dt                      date
        ,dtv_first_act_date                           date
        ,skygo_distinct_activitydate_last90days       int default 0
        ,skygo_distinct_activitydate_last180days      int default 0
        ,skygo_distinct_activitydate_last270days      int default 0
        ,skygo_distinct_activitydate_last360days      int default 0
        ,od_distinct_activity_last90days              int default 0
        ,od_distinct_activity_last180days             int default 0
        ,od_distinct_activity_last270days             int default 0
        ,MR                                           bit default 0
        ,ThreeDTV                                     bit default 0
        ,HDTV                                         bit default 0
        ,TopTier                                      bit default 0
        ,movies                                       bit default 0
        ,Sports                                       bit default 0
        ,Movies_upgrade_last_30days                   int default 0
        ,Movies_upgrade_last_60days                   int default 0
        ,Movies_upgrade_last_90days                   int default 0
        ,Movies_upgrade_last_180days                  int default 0
        ,Movies_downgrade_last_30days                 int default 0
        ,Movies_downgrade_last_60days                 int default 0
        ,Movies_downgrade_last_90days                 int default 0
        ,Movies_downgrade_last_180days                int default 0
        ,Sports_upgrade_last_30days                   int default 0
        ,Sports_upgrade_last_60days                   int default 0
        ,Sports_upgrade_last_90days                   int default 0
        ,Sports_upgrade_last_180days                  int default 0
        ,Sports_downgrade_last_30days                 int default 0
        ,Sports_downgrade_last_60days                 int default 0
        ,Sports_downgrade_last_90days                 int default 0
        ,Sports_downgrade_last_180days                int default 0
        ,dtv_latest_act_date                          date
        ,cvs_segment                                  varchar(30)
        ,product_holding                              varchar(30)
        ,date_of_last_ta_call                         date
        ,date_of_last_pat_call                        date
        ,date_of_last_ic_call                         date
);

truncate table ta_modeling_raw_data;

  create table accs(
         account_number varchar(30)
        )
;

  commit;
  create hg index hgran ON accs(account_number);

  create table accounts(
         account_number varchar(30)
        ,ref            int
        ,rand_num       float
        )
;

  create table #ta_modeling_raw_data(
         account_number varchar(30)
        ,ref            int
        ,rand_num       float
);

  commit;
  create hg index hgran ON accounts(rand_num);
  create hg index hgacc ON accounts(account_number);


truncate table accs;

      -- insert accounts into table
  insert into accs(account_number)
  select account_number
    from citeam.View_CUST_CHURN_HIST
group by account_number
   union
  select account_number
    from citeam.View_CUST_OFFER_HIST
group by account_number
;

   while @Reference <= 201410 begin
                if right(@Reference,2) = '13' begin
                        set @Reference = cast(left(cast(@reference as varchar),2) || cast(cast(substring(cast(@reference as varchar),3,2) as int) + 1 as varchar) || '01' as int)
               end

           execute('
            insert into accs(account_number)
            select account_number
              from ' || case when @reference <= 201409 then 'yarlagaddar'
                             when @reference = 201410 then 'hutchij'
                             else 'sharmaa' end || '.View_attachments_' || @reference || '
          group by account_number
                  ')

               set @reference = @reference + 1
     end
;

truncate table accounts;
     set @Reference = 201311;
   while @Reference <= 201410 begin
                if right(@Reference,2) = '13' begin
                        set @Reference = cast(left(cast(@reference as varchar),2) || cast(cast(substring(cast(@reference as varchar),3,2) as int) + 1 as varchar) || '01' as int)
               end

               set multiplier = DATEPART(millisecond,now()) + 1

            insert into accounts(
                   account_number
                  ,ref
                  ,rand_num
                   )
            select account_number
                  ,@reference
                  ,rand(number(*) * multiplier)
              from accs
          group by account_number

               set @reference = @reference + 1
     end
;

truncate table ta_modeling_raw_data;
     set @Reference = 201311;
   while @Reference <= 201410 begin
                if right(@Reference,2) = '13' begin
                        set @Reference = cast(left(cast(@reference as varchar),2) || cast(cast(substring(cast(@reference as varchar),3,2) as int) + 1 as varchar) || '01' as int)
               end

            select @records = count() / 100
              from accounts
             where @reference = ref

          truncate table #ta_modeling_raw_data
               set rowcount @records

                      insert into #ta_modeling_raw_data(
                             account_number
                            ,ref
                            ,rand_num
                            )
                      select account_number
                            ,@reference
                            ,rand_num
                        from accounts
                    order by rand_num

                      insert into ta_modeling_raw_data(
                             account_number
                            ,ref
                            )
                      select account_number
                            ,@reference
                        from accounts

               set rowcount 0
               set @reference = @reference + 1
     end
;

      -- variables taken directly from attachments tables
 execute ('
  update ta_modeling_raw_data as bas
     set dtv_first_act_date = att.dtv_first_act_date
        ,skygo_distinct_activitydate_last90days = att.skygo_distinct_activitydate_last90days
        ,skygo_distinct_activitydate_last180days = att.skygo_distinct_activitydate_last180days
        ,skygo_distinct_activitydate_last270days = att.skygo_distinct_activitydate_last270days
        ,skygo_distinct_activitydate_last360days = att.skygo_distinct_activitydate_last360days
        ,od_distinct_activity_last90days = att.od_distinct_activitydate_last90days
        ,od_distinct_activity_last180days = att.od_distinct_activitydate_last180days
        ,od_distinct_activity_last270days = att.od_distinct_activitydate_last270days
        ,MR = att.MR
        ,ThreeDTV = att.ThreeDTV
        ,HDTV = att.HDTV
        ,TopTier = att.TopTier
        ,movies = att.movies
        ,Sports = att.Sports
        ,Movies_upgrade_last_30days = att.Movies_upgrade_last_30days
        ,Movies_upgrade_last_60days = att.Movies_upgrade_last_60days
        ,Movies_upgrade_last_90days = att.Movies_upgrade_last_90days
        ,Movies_upgrade_last_180days = att.Movies_upgrade_last_180days
        ,Movies_downgrade_last_30days = att.Movies_downgrade_last_30days
        ,Movies_downgrade_last_60days = att.Movies_downgrade_last_60days
        ,Movies_downgrade_last_90days = att.Movies_downgrade_last_90days
        ,Movies_downgrade_last_180days = att.Movies_downgrade_last_180days
        ,Sports_upgrade_last_30days = att.Sports_upgrade_last_30days
        ,Sports_upgrade_last_60days = att.Sports_upgrade_last_60days
        ,Sports_upgrade_last_90days = att.Sports_upgrade_last_90days
        ,Sports_upgrade_last_180days = att.Sports_upgrade_last_180days
        ,Sports_downgrade_last_30days = att.Sports_downgrade_last_30days
        ,Sports_downgrade_last_60days = att.Sports_downgrade_last_60days
        ,Sports_downgrade_last_90days = att.Sports_downgrade_last_90days
        ,Sports_downgrade_last_180days = att.Sports_downgrade_last_180days
        ,dtv_latest_act_date = att.dtv_latest_act_date
        ,cvs_segment = att.cvs_segment
    from ' || case when @reference <= 201409 then 'yarlagaddar'
                   when @reference = 201410 then 'hutchij'
                   else 'sharmaa' end || '.View_attachments_' || @reference || ' as att
   where bas.account_number = att.account_number
     and ref = @reference
        ')
;

      -- ic_calls_last_3_months
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from View_CUST_CALLS_HIST as cch
         cross join sourcedates as sou
   where TypeOfEvent = 'IC'
     and event_dt > [3_Months_Prior] and event_dt <= Snapshot_Date
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set ic_calls_last_3_months = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- ic_calls_last_6_months
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from View_CUST_CALLS_HIST as cch
         cross join sourcedates as sou
   where TypeOfEvent = 'IC'
     and event_dt > [6_Months_Prior] and event_dt <= Snapshot_Date
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set ic_calls_last_6_months = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- ic_calls_last_9_months
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from View_CUST_CALLS_HIST as cch
         cross join sourcedates as sou
   where TypeOfEvent = 'IC'
     and event_dt > [9_Months_Prior] and event_dt <= Snapshot_Date
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set ic_calls_last_9_months = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- ic_calls_last_year
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from View_CUST_CALLS_HIST as cch
         cross join sourcedates as sou
   where TypeOfEvent = 'IC'
     and event_dt > [1_Year_Prior] and event_dt <= Snapshot_Date
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set ic_calls_last_year = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- ic_calls_last_2_years
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from View_CUST_CALLS_HIST as cch
         cross join sourcedates as sou
   where TypeOfEvent = 'IC'
     and event_dt > [2_Years_Prior] and event_dt <= Snapshot_Date
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set ic_calls_last_2_years = case when cow >= 1 then 1 end
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- pat_calls_last_3_months
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from View_CUST_CALLS_HIST as cch
         cross join sourcedates as sou
   where TypeOfEvent = 'PAT'
     and event_dt > [3_Months_Prior] and event_dt <= Snapshot_Date
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set pat_calls_last_3_months = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- pat_calls_last_6_months
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from View_CUST_CALLS_HIST as cch
         cross join sourcedates as sou
   where TypeOfEvent = 'PAT'
     and event_dt > [6_Months_Prior] and event_dt <= Snapshot_Date
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set pat_calls_last_6_months = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- pat_calls_last_9_months
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from View_CUST_CALLS_HIST as cch
         cross join sourcedates as sou
   where TypeOfEvent = 'PAT'
     and event_dt > [9_Months_Prior] and event_dt <= Snapshot_Date
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set pat_calls_last_9_months = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- pat_calls_last_year
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from View_CUST_CALLS_HIST as cch
         cross join sourcedates as sou
   where TypeOfEvent = 'PAT'
     and event_dt > [1_Year_Prior] and event_dt <= Snapshot_Date
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set pat_calls_last_year = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- pat_calls_last_2_years
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from View_CUST_CALLS_HIST as cch
         cross join sourcedates as sou
   where TypeOfEvent = 'PAT'
     and event_dt > [2_Years_Prior] and event_dt <= Snapshot_Date
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set pat_calls_last_2_years = case when cow >= 1 then 1 end
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- ta_calls_last_3_months
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from View_CUST_CALLS_HIST as cch
         cross join sourcedates as sou
   where TypeOfEvent = 'TA'
     and event_dt > [3_Months_Prior] and event_dt <= Snapshot_Date
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set ta_calls_last_3_months = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- ta_calls_last_6_months
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from View_CUST_CALLS_HIST as cch
         cross join sourcedates as sou
   where TypeOfEvent = 'TA'
     and event_dt > [6_Months_Prior] and event_dt <= Snapshot_Date
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set ta_calls_last_6_months = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- ta_calls_last_9_months
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from View_CUST_CALLS_HIST as cch
         cross join sourcedates as sou
   where TypeOfEvent = 'TA'
     and event_dt > [9_Months_Prior] and event_dt <= Snapshot_Date
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set ta_calls_last_9_months = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- ta_calls_last_year
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from View_CUST_CALLS_HIST as cch
         cross join sourcedates as sou
   where TypeOfEvent = 'TA'
     and event_dt > [1_Year_Prior] and event_dt <= Snapshot_Date
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set ta_calls_last_year = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- ta_calls_last_2_years
      -- ta_in_24m_flag
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from View_CUST_CALLS_HIST as cch
         cross join sourcedates as sou
   where TypeOfEvent = 'TA'
     and event_dt > [2_Years_Prior] and event_dt <= Snapshot_Date
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set ta_calls_last_2_years = cow
        ,ta_in_24m_flag = case when cow >= 1 then 1 end
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- ta_in_next_3_months_flag
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from View_CUST_CALLS_HIST as cch
         cross join sourcedates as sou
   where TypeOfEvent = 'TA'
     and event_dt > Snapshot_Date and event_dt <= [3_Months_Future]
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set ta_in_next_3_months_flag = case when cow >= 1 then 1 end
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- ta_in_3_6_months_flag
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from View_CUST_CALLS_HIST as cch
         cross join sourcedates as sou
   where TypeOfEvent = 'TA'
     and event_dt > [3_Months_Future] and event_dt <= [6_Months_Future]
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set ta_in_3_6_months_flag = case when cow >= 1 then 1 end
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- po_events_last_3_months
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from View_CUST_CHURN_HIST as cch
         cross join sourcedates as sou
   where TypeOfEvent = 'PO'
     and event_dt > [3_Months_Prior] and event_dt <= Snapshot_Date
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set po_events_last_3_months = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- po_events_last_6_months
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from View_CUST_CHURN_HIST as cch
         cross join sourcedates as sou
   where TypeOfEvent = 'PO'
     and event_dt > [6_Months_Prior] and event_dt <= Snapshot_Date
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set po_events_last_6_months = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- po_events_last_9_months
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from View_CUST_CHURN_HIST as cch
         cross join sourcedates as sou
   where TypeOfEvent = 'PO'
     and event_dt > [9_Months_Prior] and event_dt <= Snapshot_Date
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set po_events_last_9_months = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- po_events_last_year
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from View_CUST_CHURN_HIST as cch
         cross join sourcedates as sou
   where TypeOfEvent = 'PO'
     and event_dt > [1_Year_Prior] and event_dt <= Snapshot_Date
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set po_events_last_year = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- po_events_last_2_years
      -- cuscan_in_24m_flag
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from View_CUST_CHURN_HIST as cch
         cross join sourcedates as sou
   where TypeOfEvent = 'PO'
     and event_dt > [2_Years_Prior] and event_dt <= Snapshot_Date
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set po_events_last_2_years = cow
        ,cuscan_in_24m_flag = case when cow >= 1 then 1 end
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- po_in_next_3_months_flag
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from View_CUST_CHURN_HIST as cch
         cross join sourcedates as sou
   where TypeOfEvent = 'PO'
     and event_dt > Snapshot_Date and event_dt <= [3_Months_Future]
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set po_in_next_3_months_flag = case when cow >= 1 then 1 end
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- ab_events_last_3_months
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from View_CUST_CHURN_HIST as cch
         cross join sourcedates as sou
   where TypeOfEvent = 'AB'
     and event_dt > [3_Months_Prior] and event_dt <= Snapshot_Date
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set ab_events_last_3_months = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- ab_events_last_6_months
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from View_CUST_CHURN_HIST as cch
         cross join sourcedates as sou
   where TypeOfEvent = 'AB'
     and event_dt > [6_Months_Prior] and event_dt <= Snapshot_Date
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set ab_events_last_6_months = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- ab_events_last_9_months
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from View_CUST_CHURN_HIST as cch
         cross join sourcedates as sou
   where TypeOfEvent = 'AB'
     and event_dt > [9_Months_Prior] and event_dt <= Snapshot_Date
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set ab_events_last_9_months = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- ab_events_last_year
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from View_CUST_CHURN_HIST as cch
         cross join sourcedates as sou
   where TypeOfEvent = 'AB'
     and event_dt > [1_Year_Prior] and event_dt <= Snapshot_Date
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set ab_events_last_year = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- ab_events_last_2_years
      -- ab_in_24m_flag
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from View_CUST_CHURN_HIST as cch
         cross join sourcedates as sou
   where TypeOfEvent = 'AB'
     and event_dt > [2_Years_Prior] and event_dt <= Snapshot_Date
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set ab_events_last_2_years = cow
        ,ab_in_24m_flag = case when cow >= 1 then 1 end
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- ab_in_next_3_months_flag
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from View_CUST_CHURN_HIST as cch
         cross join sourcedates as sou
   where TypeOfEvent = 'AB'
     and event_dt > Snapshot_Date and event_dt <= [3_Months_Future]
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set ab_in_next_3_months_flag = case when cow >= 1 then 1 end
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- sc_events_last_3_months
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from View_CUST_CHURN_HIST as cch
         cross join sourcedates as sou
   where TypeOfEvent = 'SC'
     and event_dt > [3_Months_Prior] and event_dt <= Snapshot_Date
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set sc_events_last_3_months = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- sc_events_last_6_months
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from View_CUST_CHURN_HIST as cch
         cross join sourcedates as sou
   where TypeOfEvent = 'SC'
     and event_dt > [6_Months_Prior] and event_dt <= Snapshot_Date
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set sc_events_last_6_months = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- sc_events_last_9_months
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from View_CUST_CHURN_HIST as cch
         cross join sourcedates as sou
   where TypeOfEvent = 'SC'
     and event_dt > [9_Months_Prior] and event_dt <= Snapshot_Date
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set sc_events_last_9_months = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- sc_events_last_year
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from View_CUST_CHURN_HIST as cch
         cross join sourcedates as sou
   where TypeOfEvent = 'SC'
     and event_dt > [1_Year_Prior] and event_dt <= Snapshot_Date
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set sc_events_last_year = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- sc_events_last_2_years
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from View_CUST_CHURN_HIST as cch
         cross join sourcedates as sou
   where TypeOfEvent = 'SC'
     and event_dt > [2_Years_Prior] and event_dt <= Snapshot_Date
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set sc_events_last_2_years = cow
        ,cuscan_in_24m_flag = case when cow >= 1 then 1 end
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- sc_in_next_3_months_flag
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from View_CUST_CHURN_HIST as cch
         cross join sourcedates as sou
   where TypeOfEvent = 'SC'
     and event_dt > Snapshot_Date and event_dt <= [3_Months_Future]
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set sc_in_next_3_months_flag = case when cow >= 1 then 1 end
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- pc_events_last_3_months
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from View_CUST_CHURN_HIST as cch
         cross join sourcedates as sou
   where TypeOfEvent = 'PC'
     and event_dt > [3_Months_Prior] and event_dt <= Snapshot_Date
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set pc_events_last_3_months = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- pc_events_last_6_months
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from View_CUST_CHURN_HIST as cch
         cross join sourcedates as sou
   where TypeOfEvent = 'PC'
     and event_dt > [6_Months_Prior] and event_dt <= Snapshot_Date
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set pc_events_last_6_months = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- pc_events_last_9_months
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from View_CUST_CHURN_HIST as cch
         cross join sourcedates as sou
   where TypeOfEvent = 'PC'
     and event_dt > [9_Months_Prior] and event_dt <= Snapshot_Date
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set pc_events_last_9_months = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- pc_events_last_year
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from View_CUST_CHURN_HIST as cch
         cross join sourcedates as sou
   where TypeOfEvent = 'PC'
     and event_dt > [1_Year_Prior] and event_dt <= Snapshot_Date
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set pc_events_last_year = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- pc_events_last_2_years
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from View_CUST_CHURN_HIST as cch
         cross join sourcedates as sou
   where TypeOfEvent = 'PC'
     and event_dt > [2_Years_Prior] and event_dt <= Snapshot_Date
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set pc_events_last_2_years = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- box_offer_last_3_months
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from citeam.view_cust_offer_hist as coh
         cross join sourcedates as sou
   where offer_start_dt > [3_months_Prior] and offer_start_dt <= Snapshot_Date
     and ref = @reference
     and offer_type = 'Box Offer'
group by account_number
;

  update ta_modeling_raw_data as bas
     set box_offer_last_3_months = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- box_offer_last_6_months
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from citeam.view_cust_offer_hist
         cross join sourcedates as sou
   where offer_start_dt > [6_months_Prior] and offer_start_dt <= Snapshot_Date
     and ref = @reference
     and offer_type = 'Box Offer'
group by account_number
;

  update ta_modeling_raw_data as bas
     set box_offer_last_6_months = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- box_offer_last_year
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from citeam.view_cust_offer_hist
         cross join sourcedates as sou
   where offer_start_dt > [1_year_Prior] and offer_start_dt <= Snapshot_Date
     and ref = @reference
     and offer_type = 'Box Offer'
group by account_number
;

  update ta_modeling_raw_data as bas
     set box_offer_last_year = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- box_offer_last_2_years
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from citeam.view_cust_offer_hist
         cross join sourcedates as sou
   where offer_start_dt > [2_years_Prior] and offer_start_dt <= Snapshot_Date
     and ref = @reference
     and offer_type = 'Box Offer'
group by account_number
;

  update ta_modeling_raw_data as bas
     set box_offer_last_2_years = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- broadband_and_talk_last_3_months
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from citeam.view_cust_offer_hist
         cross join sourcedates as sou
   where offer_start_dt > [3_months_Prior] and offer_start_dt <= Snapshot_Date
     and ref = @reference
     and offer_type = 'Broadband & Talk'
group by account_number
;

  update ta_modeling_raw_data as bas
     set broadband_and_talk_last_3_months = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- broadband_and_talk_last_6_months
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from citeam.view_cust_offer_hist
         cross join sourcedates as sou
   where offer_start_dt > [6_months_Prior] and offer_start_dt <= Snapshot_Date
     and ref = @reference
     and offer_type = 'Broadband & Talk'
group by account_number
;

  update ta_modeling_raw_data as bas
     set broadband_and_talk_last_6_months = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- broadband_and_talk_last_year
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from citeam.view_cust_offer_hist
         cross join sourcedates as sou
   where offer_start_dt > [1_year_Prior] and offer_start_dt <= Snapshot_Date
     and ref = @reference
     and offer_type = 'Broadband & Talk'
group by account_number
;

  update ta_modeling_raw_data as bas
     set broadband_and_talk_last_year = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- broadband_and_talk_last_2_years
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from citeam.view_cust_offer_hist
         cross join sourcedates as sou
   where offer_start_dt > [2_years_Prior] and offer_start_dt <= Snapshot_Date
     and ref = @reference
     and offer_type = 'Broadband & Talk'
group by account_number
;

  update ta_modeling_raw_data as bas
     set broadband_and_talk_last_2_years = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- install_offer_last_3_months
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from citeam.view_cust_offer_hist
         cross join sourcedates as sou
   where offer_start_dt > [3_months_Prior] and offer_start_dt <= Snapshot_Date
     and ref = @reference
     and offer_type = 'Install Offer'
group by account_number
;

  update ta_modeling_raw_data as bas
     set install_offer_last_3_months = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- install_offer_last_6_months
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from citeam.view_cust_offer_hist
         cross join sourcedates as sou
   where offer_start_dt > [6_months_Prior] and offer_start_dt <= Snapshot_Date
     and ref = @reference
     and offer_type = 'Install Offer'
group by account_number
;

  update ta_modeling_raw_data as bas
     set install_offer_last_6_months = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- install_offer_last_year
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from citeam.view_cust_offer_hist
         cross join sourcedates as sou
   where offer_start_dt > [1_year_Prior] and offer_start_dt <= Snapshot_Date
     and ref = @reference
     and offer_type = 'Install Offer'
group by account_number
;

  update ta_modeling_raw_data as bas
     set install_offer_last_year = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- install_offer_last_2_years
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from citeam.view_cust_offer_hist
         cross join sourcedates as sou
   where offer_start_dt > [2_years_Prior] and offer_start_dt <= Snapshot_Date
     and ref = @reference
     and offer_type = 'Install Offer'
group by account_number
;

  update ta_modeling_raw_data as bas
     set install_offer_last_2_years = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- others_last_3_months
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from citeam.view_cust_offer_hist
         cross join sourcedates as sou
   where offer_start_dt > [3_months_Prior] and offer_start_dt <= Snapshot_Date
     and ref = @reference
     and offer_type = 'Others'
group by account_number
;

  update ta_modeling_raw_data as bas
     set others_last_3_months = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- others_last_6_months
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from citeam.view_cust_offer_hist
         cross join sourcedates as sou
   where offer_start_dt > [6_months_Prior] and offer_start_dt <= Snapshot_Date
     and ref = @reference
     and offer_type = 'Others'
group by account_number
;

  update ta_modeling_raw_data as bas
     set others_last_6_months = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- others_last_year
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from citeam.view_cust_offer_hist
         cross join sourcedates as sou
   where offer_start_dt > [1_year_Prior] and offer_start_dt <= Snapshot_Date
     and ref = @reference
     and offer_type = 'Others'
group by account_number
;

  update ta_modeling_raw_data as bas
     set others_last_year = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- others_last_2_years
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from citeam.view_cust_offer_hist
         cross join sourcedates as sou
   where offer_start_dt > [2_years_Prior] and offer_start_dt <= Snapshot_Date
     and ref = @reference
     and offer_type = 'Others'
group by account_number
;

  update ta_modeling_raw_data as bas
     set others_last_2_years = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- others_ppo_last_3_months
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from citeam.view_cust_offer_hist
         cross join sourcedates as sou
   where offer_start_dt > [3_months_Prior] and offer_start_dt <= Snapshot_Date
     and ref = @reference
     and offer_type = 'Others_PPO'
group by account_number
;

  update ta_modeling_raw_data as bas
     set others_ppo_last_3_months = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- others_ppo_last_6_months
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from citeam.view_cust_offer_hist
         cross join sourcedates as sou
   where offer_start_dt > [6_months_Prior] and offer_start_dt <= Snapshot_Date
     and ref = @reference
     and offer_type = 'Others_PPO'
group by account_number
;

  update ta_modeling_raw_data as bas
     set others_ppo_last_6_months = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- others_ppo_last_year
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from citeam.view_cust_offer_hist
         cross join sourcedates as sou
   where offer_start_dt > [1_year_Prior] and offer_start_dt <= Snapshot_Date
     and ref = @reference
     and offer_type = 'Others_PPO'
group by account_number
;

  update ta_modeling_raw_data as bas
     set others_ppo_last_year = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- others_ppo_last_2_years
      -- price_protection_flag
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from citeam.view_cust_offer_hist
         cross join sourcedates as sou
   where offer_start_dt > [2_years_Prior] and offer_start_dt <= Snapshot_Date
     and ref = @reference
     and offer_type = 'Others_PPO'
group by account_number
;

  update ta_modeling_raw_data as bas
     set others_ppo_last_2_years = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- service_call_last_3_months
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from citeam.view_cust_offer_hist
         cross join sourcedates as sou
   where offer_start_dt > [3_months_Prior] and offer_start_dt <= Snapshot_Date
     and ref = @reference
     and offer_type = 'Service Call'
group by account_number
;

  update ta_modeling_raw_data as bas
     set service_call_last_3_months = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- service_call_last_6_months
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from citeam.view_cust_offer_hist
         cross join sourcedates as sou
   where offer_start_dt > [6_months_Prior] and offer_start_dt <= Snapshot_Date
     and ref = @reference
     and offer_type = 'Service Call'
group by account_number
;

  update ta_modeling_raw_data as bas
     set service_call_last_6_months = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- service_call_last_year
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from citeam.view_cust_offer_hist
         cross join sourcedates as sou
   where offer_start_dt > [1_year_Prior] and offer_start_dt <= Snapshot_Date
     and ref = @reference
     and offer_type = 'Service Call'
group by account_number
;

  update ta_modeling_raw_data as bas
     set service_call_last_year = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- service_call_last_2_years
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from citeam.view_cust_offer_hist
         cross join sourcedates as sou
   where offer_start_dt > [2_years_Prior] and offer_start_dt <= Snapshot_Date
     and ref = @reference
     and offer_type = 'Service Call'
group by account_number
;

  update ta_modeling_raw_data as bas
     set service_call_last_2_years = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- TV_packs_last_3_months
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from citeam.view_cust_offer_hist
         cross join sourcedates as sou
   where offer_start_dt > [3_months_Prior] and offer_start_dt <= Snapshot_Date
     and ref = @reference
     and offer_type = 'TV Packs'
group by account_number
;

  update ta_modeling_raw_data as bas
     set tv_packs_last_3_months = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- TV_packs_last_6_months
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from citeam.view_cust_offer_hist
         cross join sourcedates as sou
   where offer_start_dt > [6_months_Prior] and offer_start_dt <= Snapshot_Date
     and ref = @reference
     and offer_type = 'TV Packs'
group by account_number
;

  update ta_modeling_raw_data as bas
     set tv_packs_last_6_months = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- TV_packs_last_year
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from citeam.view_cust_offer_hist
         cross join sourcedates as sou
   where offer_start_dt > [1_year_Prior] and offer_start_dt <= Snapshot_Date
     and ref = @reference
     and offer_type = 'TV Packs'
group by account_number
;

  update ta_modeling_raw_data as bas
     set tv_packs_last_year = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- TV_packs_last_2_years
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from citeam.view_cust_offer_hist
         cross join sourcedates as sou
   where offer_start_dt > [2_years_prior] and offer_start_dt <= Snapshot_Date
     and ref = @reference
     and offer_type = 'TV Packs'
group by account_number
;

  update ta_modeling_raw_data as bas
     set tv_packs_last_2_years = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- total_expiring_comms_offers_next_3_months
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from citeam.view_cust_offer_hist
         cross join sourcedates as sou
   where offer_start_dt > Snapshot_Date and offer_start_dt <= [3_months_future]
     and ref = @reference
     and offer_type = 'BroadBand & Talk'
group by account_number
;

  update ta_modeling_raw_data as bas
     set total_expiring_comms_offers_next_3_months = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- total_expiring_other_offers_next_3_months
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from citeam.view_cust_offer_hist
         cross join sourcedates as sou
   where offer_start_dt > Snapshot_Date and offer_start_dt <= [3_months_future]
     and ref = @reference
     and offer_type not in ('BroadBand & Talk', 'TV Packs')
group by account_number
;

  update ta_modeling_raw_data as bas
     set total_expiring_other_offers_next_3_months = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- total_expiring_dtv_offers_next_3_months
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from citeam.view_cust_offer_hist
         cross join sourcedates as sou
   where offer_start_dt > Snapshot_Date and offer_start_dt <= [3_months_future]
     and ref = @reference
     and offer_type = 'TV Packs'
group by account_number
;

  update ta_modeling_raw_data as bas
     set total_expiring_dtv_offers_next_3_months = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- total_expiring_offer_value_next_3_months
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,sum(offer_amount) as cow
    from citeam.view_cust_offer_hist
         cross join sourcedates as sou
   where offer_start_dt > Snapshot_Date and offer_start_dt <= [3_months_future]
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set total_expiring_offer_value_next_3_months = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- total_expiring_comms_offers_next_4_6_months
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from citeam.view_cust_offer_hist
         cross join sourcedates as sou
   where offer_start_dt > [4_months_future] and offer_start_dt <= [6_months_future]
     and ref = @reference
     and offer_type = 'BroadBand & Talk'
group by account_number
;

  update ta_modeling_raw_data as bas
     set total_expiring_comms_offers_next_4_6_months = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- total_expiring_other_offers_next_4_6_months
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from citeam.view_cust_offer_hist
         cross join sourcedates as sou
   where offer_start_dt > [4_months_future] and offer_start_dt <= [6_months_future]
     and ref = @reference
     and offer_type not in ('BroadBand & Talk', 'TV Packs')
group by account_number
;

  update ta_modeling_raw_data as bas
     set total_expiring_other_offers_next_4_6_months = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- total_expiring_dtv_offers_next_4_6_months
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,count() as cow
    from citeam.view_cust_offer_hist
         cross join sourcedates as sou
   where offer_start_dt > [4_months_future] and offer_start_dt <= [6_months_future]
     and ref = @reference
     and offer_type = 'TV Packs'
group by account_number
;

  update ta_modeling_raw_data as bas
     set total_expiring_dtv_offers_next_4_6_months = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- total_expiring_offer_value_next_4_6_months
truncate table #count;

  insert into #count(
         account_number
        ,cow
         )
  select account_number
        ,sum(offer_amount) as cow
    from citeam.view_cust_offer_hist
         cross join sourcedates as sou
   where offer_start_dt > [4_months_future] and offer_start_dt <= [6_months_future]
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set total_expiring_offer_value_next_4_6_months = cow
    from #count as cow
   where bas.account_number = cow.account_number
;

      -- Date_of_Last_TA_Call
truncate table #dates;

  insert into #dates(
         account_number
        ,dt
         )
  select account_number
        ,max(event_dt)
    from View_CUST_CALLS_HIST as cch
         cross join sourcedates as sou
   where TypeOfEvent = 'TA'
     and event_dt > [2_Years_Prior] and event_dt <= Snapshot_Date
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set Date_of_Last_TA_Call = dt
    from #dates as dts
   where bas.account_number = dts.account_number
;

      -- Date_of_Last_PAT_Call
truncate table #dates;

  insert into #dates(
         account_number
        ,dt
         )
  select account_number
        ,max(event_dt)
    from View_CUST_CALLS_HIST as cch
         cross join sourcedates as sou
   where TypeOfEvent = 'PAT'
     and event_dt > [2_Years_Prior] and event_dt <= Snapshot_Date
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set Date_of_Last_PAT_Call = dt
    from #dates as dts
   where bas.account_number = dts.account_number
;

      -- Date_of_Last_IC_Call
truncate table #dates;

  insert into #dates(
         account_number
        ,dt
         )
  select account_number
        ,max(event_dt)
    from View_CUST_CALLS_HIST as cch
         cross join sourcedates as sou
   where TypeOfEvent = 'IC'
     and event_dt > [2_Years_Prior] and event_dt <= Snapshot_Date
     and ref = @reference
group by account_number
;

  update ta_modeling_raw_data as bas
     set Date_of_Last_IC_Call = dt
    from #dates as dts
   where bas.account_number = dts.account_number
;

      -- sum_unstable_flags
  update ta_modeling_raw_data as bas
     set sum_unstable_flags = AB_in_24m_flag + cuscan_in_24m_flag + syscan_in_24m_flag + TA_in_24m_flag
;

  update ta_modeling_raw_data as bas
     set segment = case when dtv_first_act_date >  [10_Months_Prior] then '<10_Months'
                        when dtv_first_act_date <= [10_Months_Prior] and dtv_first_act_date > [2_Years_Prior] then '10-24_Months'
                        when dtv_first_act_date <= [2_Years_Prior]   and sum_unstable_flags = 0 then '24_Months+'
                        else 'Unstable'
                    end
    from sourcedates as sou
    where bas.ref = sou.ref
;

      -- section to calculate upgrades and downgrades
truncate table accs;

  insert into accs
  select account_number
    from TA_MODELING_RAW_DATA
group by account_number
;

  select ref
        ,cast(null as date) as dt
    into #refs
    from TA_MODELING_RAW_DATA
group by ref
        ,dt
;

  create date index dtdte on #refs([dt]);

  update #refs
     set dt = cast(left(cast(ref as varchar), 4) || '-' || right(cast(ref as varchar),2) || '-01' as date) - 2
;

  select csh.account_number
        ,effective_from_dt
        ,oce.prem_sports as old_sports
        ,nce.prem_sports as new_sports
        ,oce.prem_movies as old_movies
        ,nce.prem_movies as new_movies
    into #sub_changes
    from cust_subs_hist as csh
         inner join accs as acc on csh.account_number = acc.account_number
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
     and cast(left(cast(ref as varchar), 4) || '-' || right(cast(ref as varchar),2) || '-01' as date) - 2 = dt
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
     and cast(left(cast(ref as varchar), 4) || '-' || right(cast(ref as varchar),2) || '-01' as date) - 2 = dt
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
     and cast(left(cast(ref as varchar), 4) || '-' || right(cast(ref as varchar),2) || '-01' as date) - 2 = dt
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
     and cast(left(cast(ref as varchar), 4) || '-' || right(cast(ref as varchar),2) || '-01' as date) - 2 = dt
;

  update TA_MODELING_RAW_DATA as bas
     set num_movies_downgrades_ever = coalesce(num_movies_downgrades_ever, 0)
        ,num_sports_downgrades_ever = coalesce(num_sports_downgrades_ever, 0)
        ,num_movies_upgrades_ever = coalesce(num_movies_upgrades_ever, 0)
        ,num_sports_upgrades_ever = coalesce(num_sports_upgrades_ever, 0)
;

  select account_number
        ,snapshot_date
        ,bas.ref
        ,cast(0 as bit) as tv
        ,cast(0 as bit) as bb
        ,cast(0 as bit) as st
    into #products
    from TA_MODELING_RAW_DATA as bas
         cross join sourcedates as sou
   where bas.ref = sou.ref
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
     and bas.ref = pro.ref
;

--end of ETL stream

  select ref
        ,avg(  log(skygo_distinct_activitydate_last90days)) as avg_log_skygo_90days
        ,stdev(log(skygo_distinct_activitydate_last90days)) as stdev_log_skygo_90days
        ,avg(  log(skygo_distinct_activitydate_last180days - skygo_distinct_activitydate_last90days))  as avg_log_skygo_90_180days
        ,stdev(log(skygo_distinct_activitydate_last180days - skygo_distinct_activitydate_last90days))  as stdev_log_skygo_90_180days
        ,avg(  log(skygo_distinct_activitydate_last270days - skygo_distinct_activitydate_last180days)) as avg_log_skygo_180_270days
        ,stdev(log(skygo_distinct_activitydate_last270days - skygo_distinct_activitydate_last180days)) as stdev_log_skygo_180_270days
    into #logs
    from TA_MODELING_RAW_DATA
group by ref



  update TA_MODELING_RAW_DATA as bas
     set Last_90_Days_Sky_Go_Distinct_Logins_Category = case when skygo_distinct_activitydate_last90days) = 0 then 0
                                                             when log(skygo_distinct_activitydate_last90days) < (avg_log_skygo_90days - (0.5 * stdev_log_skygo_90days)) then 1
when (log(skygo_distinct_activitydate_last90days) >= avg_log_skygo_90days - ( 0.5 * stdev_log_skygo_90days)) and (skygo_distinct_activitydate_last90days_Log <= skygo_distinct_activitydate_last90days_Log_Mean+(0.5*skygo_distinct_activitydate_last90days_Log_SDev))
from #logs as lgs
where bas.ref = lgs.ref





select ref,count() from ta_modeling_raw_data
group by ref
