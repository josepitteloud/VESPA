drop table fields;
  create table fields(field varchar(50)
                     ,id int identity)
;

  insert into fields(field) select '10_Months_Prior';
  insert into fields(field) select '2_Years_Prior';
  insert into fields(field) select 'AB_Events_Last_2_Years';
  insert into fields(field) select 'AB_Events_Last_3_Months';
  insert into fields(field) select 'AB_Events_Last_6_Months';
  insert into fields(field) select 'AB_Events_Last_9_Months';
  insert into fields(field) select 'AB_Events_Last_Year';
  insert into fields(field) select 'AB_in_Next_3_Months_Flag';
  insert into fields(field) select 'account_number';
  insert into fields(field) select 'cvs_segment';
  insert into fields(field) select 'Date_of_Last_IC_Call';
  insert into fields(field) select 'Date_of_Last_PAT_Call';
  insert into fields(field) select 'Date_of_Last_TA_Call';
  insert into fields(field) select 'dtv_first_act_date';
  insert into fields(field) select 'dtv_latest_act_date';
  insert into fields(field) select 'HDTV';
  insert into fields(field) select 'IC_Calls_Last_2_Years';
  insert into fields(field) select 'IC_Calls_Last_3_Months';
  insert into fields(field) select 'IC_Calls_Last_6_Months';
  insert into fields(field) select 'IC_Calls_Last_9_Months';
  insert into fields(field) select 'IC_Calls_Last_Year';
  insert into fields(field) select 'last_movies_downgrades_dt';
  insert into fields(field) select 'last_movies_upgrades_dt';
  insert into fields(field) select 'last_sports_downgrades_dt';
  insert into fields(field) select 'last_sports_upgrades_dt';
  insert into fields(field) select 'movies';
  insert into fields(field) select 'Movies_downgrade_last_180days';
  insert into fields(field) select 'Movies_downgrade_last_30days';
  insert into fields(field) select 'Movies_downgrade_last_60days';
  insert into fields(field) select 'Movies_downgrade_last_90days';
  insert into fields(field) select 'Movies_upgrade_last_180days';
  insert into fields(field) select 'Movies_upgrade_last_30days';
  insert into fields(field) select 'Movies_upgrade_last_60days';
  insert into fields(field) select 'Movies_upgrade_last_90days';
  insert into fields(field) select 'MR';
  insert into fields(field) select 'num_movies_downgrades_ever';
  insert into fields(field) select 'num_movies_upgrades_ever';
  insert into fields(field) select 'num_sports_downgrades_ever';
  insert into fields(field) select 'num_sports_upgrades_ever';
  insert into fields(field) select 'Others_Last_2_Years';
  insert into fields(field) select 'Others_Last_3_Months';
  insert into fields(field) select 'Others_Last_6_Months';
  insert into fields(field) select 'Others_Last_Year';
  insert into fields(field) select 'Others_PPO_Last_2_Years';
  insert into fields(field) select 'Others_PPO_Last_3_Months';
  insert into fields(field) select 'Others_PPO_Last_6_Months';
  insert into fields(field) select 'Others_PPO_Last_Year';
  insert into fields(field) select 'PAT_Calls_Last_2_Years';
  insert into fields(field) select 'PAT_Calls_Last_3_Months';
  insert into fields(field) select 'PAT_Calls_Last_6_Months';
  insert into fields(field) select 'PAT_Calls_Last_9_Months';
  insert into fields(field) select 'PAT_Calls_Last_Year';
  insert into fields(field) select 'PC_Events_Last_2_Years';
  insert into fields(field) select 'PC_Events_Last_3_Months';
  insert into fields(field) select 'PC_Events_Last_6_Months';
  insert into fields(field) select 'PC_Events_Last_9_Months';
  insert into fields(field) select 'PC_Events_Last_Year';
  insert into fields(field) select 'PO_Events_Last_2_Years';
  insert into fields(field) select 'PO_Events_Last_3_Months';
  insert into fields(field) select 'PO_Events_Last_6_Months';
  insert into fields(field) select 'PO_Events_Last_9_Months';
  insert into fields(field) select 'PO_Events_Last_Year';
  insert into fields(field) select 'PO_In_Next_4_Months_Flag';
  insert into fields(field) select 'price_protection_flag';
  insert into fields(field) select 'Product_Holding';
  insert into fields(field) select 'reference';
  insert into fields(field) select 'SC_Events_Last_2_Years';
  insert into fields(field) select 'SC_Events_Last_3_Months';
  insert into fields(field) select 'SC_Events_Last_6_Months';
  insert into fields(field) select 'SC_Events_Last_9_Months';
  insert into fields(field) select 'SC_Events_Last_Year';
  insert into fields(field) select 'SC_In_Next_4_Months_Flag';
  insert into fields(field) select 'segment';
  insert into fields(field) select 'skygo_distinct_activitydate_last180days';
  insert into fields(field) select 'skygo_distinct_activitydate_last270days';
  insert into fields(field) select 'skygo_distinct_activitydate_last360days';
  insert into fields(field) select 'skygo_distinct_activitydate_last90days';
  insert into fields(field) select 'snapshot_date';
  insert into fields(field) select 'Sports';
  insert into fields(field) select 'Sports_downgrade_last_180days';
  insert into fields(field) select 'Sports_downgrade_last_30days';
  insert into fields(field) select 'Sports_downgrade_last_60days';
  insert into fields(field) select 'Sports_downgrade_last_90days';
  insert into fields(field) select 'Sports_upgrade_last_180days';
  insert into fields(field) select 'Sports_upgrade_last_30days';
  insert into fields(field) select 'Sports_upgrade_last_60days';
  insert into fields(field) select 'Sports_upgrade_last_90days';
  insert into fields(field) select 'sum_unstable_flags';
  insert into fields(field) select 'TA_Calls_Last_2_Years';
  insert into fields(field) select 'TA_Calls_Last_3_Months';
  insert into fields(field) select 'TA_Calls_Last_6_Months';
  insert into fields(field) select 'TA_Calls_Last_9_Months';
  insert into fields(field) select 'TA_Calls_Last_Year';
  insert into fields(field) select 'TA_in_NEXT_3_months_flag';
  insert into fields(field) select 'ThreeDTV';
  insert into fields(field) select 'TopTier';
  insert into fields(field) select 'Total_Expiring_Comms_Offers_Next_3_Months';
  insert into fields(field) select 'Total_Expiring_DTV_Offers_Next_3_Months';
  insert into fields(field) select 'Total_Expiring_Offer_Value_Next_3_Months';
  insert into fields(field) select 'Total_Expiring_Other_Offers_Next_3_Months';

   alter table fields add min_ varchar(50);
   alter table fields add max_ varchar(50);
   alter table fields add distincts_ varchar(50);

  create variable @counter int;
  create variable @val varchar(50);
  create variable @val2 varchar(50);
     set @counter = 201;
   while @counter <= 300 begin
            select @val = field from fields where id = @counter
           execute('select @val2 = min([' || @val || ']) from ta_modeling_raw_data_scoring')
            update fields set min_ = @val2 where id = @counter
           execute('select @val2 = max([' || @val || ']) from ta_modeling_raw_data_scoring')
            update fields set max_ = @val2 where id = @counter
           execute('select @val2 = count (distinct [' || @val || ']) from ta_modeling_raw_data_scoring')
            update fields set distincts_ = @val2 where id = @counter
               set @counter = @counter + 1
     end;

--update fields set field = field || ' Hoi' where field not like '%Jon'


select top 1000 * from fields



select min(date_of_last_pat_call) from ta_modeling_raw_data
select min(date_of_last_pat_call) from ta_modeling_raw_data_scoring
select count() from ta_modeling_raw_data where date_of_last_pat_call is null;
select count() from ta_modeling_raw_data_scoring where date_of_last_pat_call is null;
select count() from tanghoi.ta_modeling_raw_data where date_of_last_pat_call is null;

select count(),product_holding from ta_modeling_raw_data
group by product_holding

count() product_holding
5263197 A. DTV Only
4352873 B. DTV + Triple play
 153838 C. DTV + BB Only
 123899 D. DTV + Other Comms
 816510 E. SABB

count() product_holding
654784 A. DTV Only
494337 B. DTV + Triple play
 22653 C. DTV + BB Only
 17617 D. DTV + Other Comms
 84737 E. SABB

select segment,ab_in_24m_flag,count() from ta_modeling_raw_data
group by ab_in_24m_flag,segment

select segment,ab_events_last_2_years,count() from tanghoi.ta_modeling_raw_data
group by ab_events_last_2_years,segment

select top 1 * from tanghoi.ta_modeling_raw_data
select top 1 * from ta_modeling_raw_data






































