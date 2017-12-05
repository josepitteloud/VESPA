--drop table Modeler_output;
  select *
        ,dateadd(year, 1, dtv_latest_act_date) as contract_end_date
    into #dates1
    from TA2015_2
;

  select dat.*
        ,case when Contract_End_Date >= [1_Month_Prior] and Contract_End_Date <= [3_Months_Future] then 1 else 0 end as Contract_Expiry_Flag_Next_3_Months
    into #dates2
    from #dates1 as dat
         inner join sourcedates as sou on dat.[reference] = sou.[reference]
;

  select dat.*
        ,case when Contract_End_Date > [3_Months_Future] and Contract_End_Date <= [6_Months_Future] then 1 else 0 end as Contract_Expiry_Flag_Next_4_6_Months
    into #dates3
    from #dates2 as dat
         inner join sourcedates as sou on dat.[reference] = sou.[reference]
;

  select dat.*
        ,datediff(month, dtv_first_act_date, Snapshot_Date) as Total_Tenure_Months
    into #dates4
    from #dates3 as dat
;

  select dat.*
        ,datediff(month, dtv_latest_act_date, Snapshot_Date) as Current_Tenure_Months
    into #dates5
    from #dates4 as dat
;

  select *
        ,cast(case when Total_Tenure_Months < 10  then 'Less_than_10_Months'
              when Total_Tenure_Months < 24  then 'Less_than_2_Years'
              when Total_Tenure_Months < 42  then 'Less_than_3.5_Years'
              when Total_Tenure_Months < 60  then 'Less_than_5_Years'
              when Total_Tenure_Months < 120 then 'Less_than_10_Years'
              else 'More_than_10_Years'
          end as varchar) as Total_Tenure_Group
    into #dates6
    from #dates5 as dat
;

  select *
        ,cast(case when Current_Tenure_Months < 10  then 'Less_than_10_Months'
              when Current_Tenure_Months < 24  then 'Less_than_2_Years'
              when Current_Tenure_Months < 42  then 'Less_than_3.5_Years'
              when Current_Tenure_Months < 60  then 'Less_than_5_Years'
              when Current_Tenure_Months < 120 then 'Less_than_10_Years'
              else 'More_than_10_Years'
          end as varchar) as Current_Tenure_Group
    into #dates7
    from #dates6 as dat
;

  select *
        ,datediff(month, Date_of_Last_TA_Call, Snapshot_Date) as Months_Since_Last_TA_Call
    into #dates8
    from #dates7 as dat
;

  select *
        ,cast(case when Date_of_Last_TA_Call is null   then 'No_Calls_Made'
              when Months_Since_Last_TA_Call < 3  then '<3_Months'
              when Months_Since_Last_TA_Call < 6  then '3_6_Months'
              when Months_Since_Last_TA_Call < 9  then '6_9_Months'
              when Months_Since_Last_TA_Call < 12 then '9_12_Months'
              when Months_Since_Last_TA_Call < 15 then '12_15_Months'
              else '>15_Months'
          end as varchar) as Months_Since_Last_TA_Group
    into #dates9
    from #dates8 as dat
;

  select *
        ,datediff(month, Date_of_Last_PAT_Call, Snapshot_Date) as Months_Since_Last_PAT_Call
    into #dates10
    from #dates9 as dat
;

  select *
        ,cast(case when Date_of_Last_PAT_Call is null   then 'No_Calls_Made'
              when Months_Since_Last_PAT_Call < 3  then '<3_Months'
              when Months_Since_Last_PAT_Call < 6  then '3_6_Months'
              when Months_Since_Last_PAT_Call < 9  then '6_9_Months'
              when Months_Since_Last_PAT_Call < 12 then '9_12_Months'
              when Months_Since_Last_PAT_Call < 15 then '12_15_Months'
              else '>15_Months'
          end as varchar) as Months_Since_Last_PAT_Group
    into #dates11
    from #dates10 as dat
;

  select *
        ,datediff(month, Date_of_Last_IC_Call, Snapshot_Date) as Months_Since_Last_IC_Call
    into #dates12
    from #dates11 as dat
;

  select *
        ,cast(case when Date_of_Last_IC_Call is null   then 'No_Calls_Made'
              when Months_Since_Last_IC_Call < 3  then '<3_Months'
              when Months_Since_Last_IC_Call < 6  then '3_6_Months'
              when Months_Since_Last_IC_Call < 9  then '6_9_Months'
              when Months_Since_Last_IC_Call < 12 then '9_12_Months'
              when Months_Since_Last_IC_Call < 15 then '12_15_Months'
              else '>15_Months'
          end as varchar) as Months_Since_Last_IC_Group
    into #dates13
    from #dates12 as dat
;

  select account_number
        ,TA_in_NEXT_3_months_flag
        ,AB_in_24m_flag
        ,cuscan_in_24m_flag
        ,syscan_in_24m_flag
        ,TA_in_24m_flag
        ,TA_in_3_6_Months_Flag
        ,Box_Offer_Last_2_Years
        ,BroadBand_and_Talk_Last_2_Years
        ,Install_Offer_Last_2_Years
        ,Service_Call_Last_2_Years
        ,TV_Packs_Last_2_Years
        ,Box_Offer_Last_Year
        ,BroadBand_and_Talk_Last_Year
        ,Install_Offer_Last_Year
        ,Service_Call_Last_Year
        ,TV_Packs_Last_Year
        ,Box_Offer_Last_6_Months
        ,BroadBand_and_Talk_Last_6_Months
        ,Install_Offer_Last_6_Months
        ,Service_Call_Last_6_Months
        ,TV_Packs_Last_6_Months
        ,Box_Offer_Last_3_Months
        ,BroadBand_and_Talk_Last_3_Months
        ,Install_Offer_Last_3_Months
        ,Service_Call_Last_3_Months
        ,TV_Packs_Last_3_Months
        ,Total_Expiring_Comms_Offers_Next_4_6_Months
        ,Total_Expiring_Other_Offers_Next_4_6_Months
        ,Total_Expiring_DTV_Offers_Next_4_6_Months
        ,Total_Expiring_Offer_Value_Next_4_6_Months
        ,Total_Expiring_Comms_Offers_Next_3_Months
        ,Total_Expiring_Other_Offers_Next_3_Months
        ,Total_Expiring_DTV_Offers_Next_3_Months
        ,Total_Expiring_Offer_Value_Next_3_Months
        ,Segment
        ,last_sports_downgrades_dt
        ,last_sports_upgrades_dt
        ,last_movies_downgrades_dt
        ,last_movies_upgrades_dt
        ,num_sports_downgrades_ever
        ,num_sports_upgrades_ever
        ,num_movies_downgrades_ever
        ,num_movies_upgrades_ever
        ,TopTier
        ,Movies
        ,Sports
        ,Movies_upgrade_last_180days
        ,Movies_downgrade_last_180days
        ,Sports_upgrade_last_180days
        ,Sports_downgrade_last_180days
        ,Date_of_Last_TA_Call
        ,Date_of_Last_PAT_Call
        ,Date_of_Last_IC_Call
        ,Product_Holding
        ,cvs_segment
        ,combined_engagement_score
        ,combined_engagement_score_group
        ,[# of Expiring Offers_next_3_Months] as No_Of_Expiring_Offers_Next_3_Months
        ,Expiring_DTV_offers_Flag_Next_3_Months
        ,Expiring_Comms_Offer_Flag_Next_3_Months
        ,Expiring_Other_Offer_Flag_Next_3_Months
        ,Expiring_Offer_Type_Next_3_Months
        ,Expiring_Offer_Value_Group_Next_3_Months
        ,[# of Expiring Offers_next_4-6_Months] as No_Of_Expiring_Offers_Next_4_6_Months
        ,[Expiring_DTV_offers_Flag_Next_4-6_Months] as Expiring_DTV_offers_Flag_Next_4_6_Months
        ,[Expiring_Comms_Offer_Flag_Next_4-6_Months] as Expiring_Comms_Offer_Flag_Next_4_6_Months
        ,[Expiring_Other_Offer_Flag_Next_4-6_Months] as Expiring_Other_Offer_Flag_Next_4_6_Months
        ,[Expiring_Offer_Type_Next_4-6_Months] as Expiring_Offer_Type_Next_4_6_Months
        ,[Expiring_Offer_Value_Group_Next_4-6_Months] as Expiring_Offer_Value_Group_Next_4_6_Months
        ,No_Quarters_with_TA_Calls
        ,No_Quarters_with_PAT_Calls
        ,No_Quarters_with_IC_Calls
        ,No_Quarters_with_SC_Events
        ,No_Quarters_with_PO_Events
        ,No_Quarters_with_PC_Events
        ,No_Quarters_with_AB_Events
        ,Additional_Paid_Products
        ,Customer_Type
        ,Total_Product_Movement
        ,Net_Product_Movement
        ,Product_Movement_Type
        ,Contract_End_Date
        ,Contract_Expiry_Flag_Next_3_Months
        ,Contract_Expiry_Flag_Next_4_6_Months
        ,Total_Tenure_Months
        ,Current_Tenure_Months
        ,Total_Tenure_Group
        ,Current_Tenure_Group
        ,Months_Since_Last_TA_Call
        ,Months_Since_Last_TA_Group
        ,Months_Since_Last_PAT_Call
        ,Months_Since_Last_PAT_Group
        ,Months_Since_Last_IC_Call
        ,Months_Since_Last_IC_Group
    into Modeler_output
    from #dates13 as dat
;

---
alter table ta_scores_20150215 add contract_end_date varchar(50)


truncate table ta_scores_20150215;
    load table ta_scores_20150215(
         account_number',',
         Segment',',
         LRP_TA_in_3_6_Months_Flag'\n'
)
    from '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/output.csv'
  QUOTES OFF
 ESCAPES OFF
  NOTIFY 1000
;

truncate table model_results;
    load table model_results(
         account_number',',
         TA_in_3_6_Months_Flag',',
         Segment',',
         L_TA_in_3_6_Months_Flag',',
         LRP_TA_in_3_6_Months_Flag'\n'
)
    from '/ETL013/prod/sky/olive/data/share/clarityq/export/Decision_Sciences/Jon/output.csv'
  QUOTES OFF
 ESCAPES OFF
  NOTIFY 1000
skip 1
;

drop table model_results;
create table model_results(account_number varchar(30)
                          ,segment varchar(30)
                          ,TA_in_3_6_Months_Flag varchar(30)
                          ,L_TA_in_3_6_Months_Flag varchar(30)
                          ,LRP_TA_in_3_6_Months_Flag varchar(30)
);

select segment,cast((cast([LRP_TA_in_3_6_Months_Flag] as float) *10) as int) as t,count() from ta_scores_20150215 group by t,segment
order by segment,t
select segment,cast(ta_propensity *10 as int) as t,count() from vespa_analysts.SkyBase_TA_scores group by t,segment
order by segment,t
select segment,cast((cast([LRP_TA_in_3_6_Months_Flag] as float) *10) as int) as t,count() from model_results group by t,segment
order by segment,t
select segment,cast([TA_in_3_6_Months_Flag] as float) as t,count() from model_results group by t,segment
order by segment,t
select TA_in_3_6_Months_Flag,count() from modeler_output group by TA_in_3_6_Months_Flag
select TA_in_3_6_Months_Flag,count() from ta2015_2 group by TA_in_3_6_Months_Flag
select TA_in_3_6_Months_Flag,count() from ta2015_1 group by TA_in_3_6_Months_Flag
select TA_in_3_6_Months_Flag,count() from ta_modeling_raw_data group by TA_in_3_6_Months_Flag




select top 10 * from model_results
select avg(cast ([LRP_TA_in_3_6_Months_Flag] as float)) from ta_scores_20150215
select product_holding,count() from ta_modeling_raw_data_scoring
group by product_holding


alter table ta_scores_20150215 drop [$L-TA_in_3_6_Months_Flag]
alter table ta_scores_20150215 drop [$LP-TA_in_3_6_Months_Flag];
alter table ta_scores_20150215 drop [$LRP-TA_in_3_6_Months_Flag];
alter table ta_scores_20150215 drop [#_of_Expiring Offers_next_3_Months];
alter table ta_scores_20150215 drop [# of Expiring Offers_next_4_6_Months];
alter table ta_scores_20150215 drop L_TA_in_3_6_Months_Flag;

alter table ta_scores_20150215 add L_TA_in_3_6_Months_Flag double;
alter table ta_scores_20150215 add LP_TA_in_3_6_Months_Flag double;
alter table ta_scores_20150215 add LRP_TA_in_3_6_Months_Flag double;
alter table ta_scores_20150215 add no_of_Expiring_Offers_next_3_Months bigint;
alter table ta_scores_20150215 add no_of_Expiring_Offers_next_4_6_Months bigint;
alter table ta_scores_20150215 add Contract_Expiry_Flag_Next_4_6_Months int


select * from panbal_progress
select count() from ta_modeling_raw_data_scoring
dba.usp_iqcontext
select * from panbal_variables
select count() from ta_scores_20150215


insert into ta_scores_20150215 select * from ta_scores_20150215_2;
truncate table ta_scores_20150215;
drop table temp;
select top 50000 inp.*
into temp
from ta_modeling_raw_data_scoring as inp
left join ta_scores_20150215 as res on inp.account_number = res.account_number
where res.account_number is null


select dtv_latest_act_date from tem
select count(),count(distinct account_number) from ta_modeling_raw_data_scoring

select count() from ta_scores_20150215_2;


select count(),count(distinct account_number) from ta_scores_20150215;


select product_holding,segment,contract_expiry_flag_next_3_months,contract_expiry_flag_next_4_6_months,count()
from temp_out
group by product_holding,segment,contract_expiry_flag_next_3_months,contract_expiry_flag_next_4_6_months


select max(dt) from vespa_analysts.panel_data

select top 10 * from vespa_analysts.SkyBase_TA_scores

select top 10 * from ta_scores_20150215
where L_TA_in_3_6_Months_Flag + LP_TA_in_3_6_Months_Flag  > 0

select segment
      ,sum(L_TA_in_3_6_Months_Flag)
      ,sum(LP_TA_in_3_6_Months_Flag)
      ,sum(cast(LRP_TA_in_3_6_Months_Flag as float))
from ta_scores_20150215
group by segment

select segment,count() from ta_scores_20150215 group by segment

select replace(account_number,'"','')
      ,segment
      ,cast(LRP_TA_in_3_6_Months_Flag as float)
into skybase_ta_scores
from ta_scores_20150215

select account_number
      ,segment
      ,cast(LRP_TA_in_3_6_Months_Flag as float)
into skybase_ta_scores
from ta_scores_20150215

update skybase_ta_scores set account_number = replace(account_number,'"','')

alter table ta_scores_20150215


select top 100 * from ta_scores_20150215
order by L_TA_in_3_6_Months_Flag desc

update ta_scores_20150215
SET LRP_TA_in_3_6_Months_Flag = replace(LRP_TA_in_3_6_Months_Flag, char(13), '');




truncate table ta_scores_20150215
alter table ta_scores_20150215 drop LRP_TA_in_3_6_Months_Flag
alter table ta_scores_20150215 add LRP_TA_in_3_6_Months_Flag varchar(10)


select


select top 10 * from vespa_analysts.skybase_ta_scores_hist
select top 1000 * from skybase_ta_scores
order by [LRP_TA_in_3_6_Months_Flag] desc
select replaced_dt,count() from vespa_analysts.skybase_ta_scores_hist group by replaced_dt


insert into decile_check
select account_number
--      ,cast(([LRP_TA_in_3_6_Months_Flag] * 10) as int) as decile
      ,cast((ta_propensity * 10) as int) as decile
      ,'2014-06-01' as dt
  from vespa_analysts.skybase_ta_scores_hist

select decile,count() from decile_check
--where dt='2014-09-02'
--where dt='2015-02-23'
where dt='2014-06-01'
group by decile


select top 100 * from ta_scores_20150215 order by L_TA_in_3_6_Months_Flag desc


select top 10 * from skybase_scores

drop table skybase_scores
select account_number,segment,cast(LRP_TA_in_3_6_Months_Flag as double) as ta_propensity
into skybase_scores
from ta_scores_20150215

select count()
      ,cast((ta_propensity * 10) as int) as decile
  from skybase_scores
group by decile

select top 100 * from ta_scores_20150215
select count() from ta_scores_20150215
select top 100 * from modeler_output
select top 100 * from ta2015_1
select top 100 * from ta_modeling_raw_data

select segment,no_quarters_with_pc_events,count() from modeler_output group by no_quarters_with_pc_events,segment










select max(account_number), min(account_number)
,max(TA_in_NEXT_3_months_flag), min(TA_in_NEXT_3_months_flag)
,max(AB_in_24m_flag), min(AB_in_24m_flag)
,max(cuscan_in_24m_flag), min(cuscan_in_24m_flag)
,max(syscan_in_24m_flag), min(syscan_in_24m_flag)
,max(TA_in_24m_flag), min(TA_in_24m_flag)
,max(TA_in_3_6_Months_Flag), min(TA_in_3_6_Months_Flag)
,max(Box_Offer_Last_2_Years), min(Box_Offer_Last_2_Years)
,max(BroadBand_and_Talk_Last_2_Years), min(BroadBand_and_Talk_Last_2_Years)
,max(Install_Offer_Last_2_Years), min(Install_Offer_Last_2_Years)
,max(Service_Call_Last_2_Years), min(Service_Call_Last_2_Years)
,max(TV_Packs_Last_2_Years), min(TV_Packs_Last_2_Years)
,max(Box_Offer_Last_Year), min(Box_Offer_Last_Year)
,max(BroadBand_and_Talk_Last_Year), min(BroadBand_and_Talk_Last_Year)
,max(Install_Offer_Last_Year), min(Install_Offer_Last_Year)
,max(Service_Call_Last_Year), min(Service_Call_Last_Year)
,max(TV_Packs_Last_Year), min(TV_Packs_Last_Year)
,max(Box_Offer_Last_6_Months), min(Box_Offer_Last_6_Months)
,max(BroadBand_and_Talk_Last_6_Months), min(BroadBand_and_Talk_Last_6_Months)
,max(Install_Offer_Last_6_Months), min(Install_Offer_Last_6_Months)
,max(Service_Call_Last_6_Months), min(Service_Call_Last_6_Months)
,max(TV_Packs_Last_6_Months), min(TV_Packs_Last_6_Months)
,max(Box_Offer_Last_3_Months), min(Box_Offer_Last_3_Months)
,max(BroadBand_and_Talk_Last_3_Months), min(BroadBand_and_Talk_Last_3_Months)
,max(Install_Offer_Last_3_Months), min(Install_Offer_Last_3_Months)
,max(Service_Call_Last_3_Months), min(Service_Call_Last_3_Months)
,max(TV_Packs_Last_3_Months), min(TV_Packs_Last_3_Months)
,max(Total_Expiring_Comms_Offers_Next_4_6_Months), min(Total_Expiring_Comms_Offers_Next_4_6_Months)
,max(Total_Expiring_Other_Offers_Next_4_6_Months), min(Total_Expiring_Other_Offers_Next_4_6_Months)
,max(Total_Expiring_DTV_Offers_Next_4_6_Months), min(Total_Expiring_DTV_Offers_Next_4_6_Months)
,max(Total_Expiring_Offer_Value_Next_4_6_Months), min(Total_Expiring_Offer_Value_Next_4_6_Months)
,max(Total_Expiring_Comms_Offers_Next_3_Months), min(Total_Expiring_Comms_Offers_Next_3_Months)
,max(Total_Expiring_Other_Offers_Next_3_Months), min(Total_Expiring_Other_Offers_Next_3_Months)
,max(Total_Expiring_DTV_Offers_Next_3_Months), min(Total_Expiring_DTV_Offers_Next_3_Months)
,max(Total_Expiring_Offer_Value_Next_3_Months), min(Total_Expiring_Offer_Value_Next_3_Months)
,max(Segment), min(Segment)
,max(last_sports_downgrades_dt), min(last_sports_downgrades_dt)
,max(last_sports_upgrades_dt), min(last_sports_upgrades_dt)
,max(last_movies_downgrades_dt), min(last_movies_downgrades_dt)
,max(last_movies_upgrades_dt), min(last_movies_upgrades_dt)
,max(num_sports_downgrades_ever), min(num_sports_downgrades_ever)
,max(num_sports_upgrades_ever), min(num_sports_upgrades_ever)
,max(num_movies_downgrades_ever), min(num_movies_downgrades_ever)
,max(num_movies_upgrades_ever), min(num_movies_upgrades_ever)
,max(TopTier), min(TopTier)
,max(Movies), min(Movies)
,max(Sports), min(Sports)
,max(Movies_upgrade_last_180days), min(Movies_upgrade_last_180days)
,max(Movies_downgrade_last_180days), min(Movies_downgrade_last_180days)
,max(Sports_upgrade_last_180days), min(Sports_upgrade_last_180days)
,max(Sports_downgrade_last_180days), min(Sports_downgrade_last_180days)
,max(Date_of_Last_TA_Call), min(Date_of_Last_TA_Call)
,max(Date_of_Last_PAT_Call), min(Date_of_Last_PAT_Call)
,max(Date_of_Last_IC_Call), min(Date_of_Last_IC_Call)
,max(Product_Holding), min(Product_Holding)
,max(cvs_segment), min(cvs_segment)
,max(combined_engagement_score), min(combined_engagement_score)
,max(combined_engagement_score_group), min(combined_engagement_score_group)
,max([#_of_Expiring Offers_next_3_Months]), min([#_of_Expiring Offers_next_3_Months])
,max(Expiring_DTV_offers_Flag_Next_3_Months), min(Expiring_DTV_offers_Flag_Next_3_Months)
,max(Expiring_Comms_Offer_Flag_Next_3_Months), min(Expiring_Comms_Offer_Flag_Next_3_Months)
,max(Expiring_Other_Offer_Flag_Next_3_Months), min(Expiring_Other_Offer_Flag_Next_3_Months)
,max(Expiring_Offer_Type_Next_3_Months), min(Expiring_Offer_Type_Next_3_Months)
,max(Expiring_Offer_Value_Group_Next_3_Months), min(Expiring_Offer_Value_Group_Next_3_Months)
,max([#_of Expiring Offers_next_4_6_Months]), min([#_of Expiring Offers_next_4_6_Months])
,max(Expiring_DTV_offers_Flag_Next_4_6_Months), min(Expiring_DTV_offers_Flag_Next_4_6_Months)
,max(Expiring_Comms_Offer_Flag_Next_4_6_Months), min(Expiring_Comms_Offer_Flag_Next_4_6_Months)
,max(Expiring_Other_Offer_Flag_Next_4_6_Months), min(Expiring_Other_Offer_Flag_Next_4_6_Months)
,max(Expiring_Offer_Type_Next_4_6_Months), min(Expiring_Offer_Type_Next_4_6_Months)
,max(Expiring_Offer_Value_Group_Next_4_6_Months), min(Expiring_Offer_Value_Group_Next_4_6_Months)
,max(No_Quarters_with_TA_Calls), min(No_Quarters_with_TA_Calls)
,max(No_Quarters_with_PAT_Calls), min(No_Quarters_with_PAT_Calls)
,max(No_Quarters_with_IC_Calls), min(No_Quarters_with_IC_Calls)
,max(No_Quarters_with_SC_Events), min(No_Quarters_with_SC_Events)
,max(No_Quarters_with_PO_Events), min(No_Quarters_with_PO_Events)
,max(No_Quarters_with_PC_Events), min(No_Quarters_with_PC_Events)
,max(No_Quarters_with_AB_Events), min(No_Quarters_with_AB_Events)
,max(Additional_Paid_Products), min(Additional_Paid_Products)
,max(Customer_Type), min(Customer_Type)
,max(Total_Product_Movement), min(Total_Product_Movement)
,max(Net_Product_Movement), min(Net_Product_Movement)
,max(Product_Movement_Type), min(Product_Movement_Type)
,max(Contract_End_Date), min(Contract_End_Date)
,max(Contract_Expiry_Flag_Next_3_Months), min(Contract_Expiry_Flag_Next_3_Months)
,max(Contract_Expiry_Flag_Next_4_6_Months), min(Contract_Expiry_Flag_Next_4_6_Months)
,max(Total_Tenure_Months), min(Total_Tenure_Months)
,max(Current_Tenure_Months), min(Current_Tenure_Months)
,max(Total_Tenure_Group), min(Total_Tenure_Group)
,max(Current_Tenure_Group), min(Current_Tenure_Group)
,max(Months_Since_Last_TA_Call), min(Months_Since_Last_TA_Call)
,max(Months_Since_Last_TA_Group), min(Months_Since_Last_TA_Group)
,max(Months_Since_Last_PAT_Call), min(Months_Since_Last_PAT_Call)
,max(Months_Since_Last_PAT_Group), min(Months_Since_Last_PAT_Group)
,max(Months_Since_Last_IC_Call), min(Months_Since_Last_IC_Call)
,max(Months_Since_Last_IC_Group), min(Months_Since_Last_IC_Group)
from modeler_output


select segment,contract_expiry_flag_next_3_months,contract_expiry_flag_next_4_6_months,count()
from modeler_output
group by segment,contract_expiry_flag_next_3_months,contract_expiry_flag_next_4_6_months

select max(dt) from vespa_analysts.panel_data










select top 10 * from ta_scores_20150215






select top 10 * from cust_anytime_plus_downloads
select [reference],count() from TA_MODELING_RAW_DATA group by [reference]
select top 10 * from TA_MODELING_RAW_DATA


select top 10 * from vespa_analysts.vespa_single_box_view
select * from panbal_metrics
select max(dt) from vespa_analysts.panel_data



select top 10 * from tanghoi.ta_modeling_raw_data

select top 10 * from ta_modeling_raw_data

select
min([account_number]),max([account_number])
,min([reference]),max([reference])
,min([dtv_first_act_date]),max([dtv_first_act_date])
,min([10_Months_Prior]),max([10_Months_Prior])
,min([2_Years_Prior]),max([2_Years_Prior])
,min([IC_Calls_Last_Year]),max([IC_Calls_Last_Year])
,min([IC_Calls_Last_3_Months]),max([IC_Calls_Last_3_Months])
,min([IC_Calls_Last_6_Months]),max([IC_Calls_Last_6_Months])
,min([IC_Calls_Last_9_Months]),max([IC_Calls_Last_9_Months])
,min([PAT_Calls_Last_Year]),max([PAT_Calls_Last_Year])
,min([PAT_Calls_Last_3_Months]),max([PAT_Calls_Last_3_Months])
,min([PAT_Calls_Last_6_Months]),max([PAT_Calls_Last_6_Months])
,min([PAT_Calls_Last_9_Months]),max([PAT_Calls_Last_9_Months])
,min([TA_Calls_Last_Year]),max([TA_Calls_Last_Year])
,min([TA_Calls_Last_3_Months]),max([TA_Calls_Last_3_Months])
,min([TA_Calls_Last_6_Months]),max([TA_Calls_Last_6_Months])
,min([TA_Calls_Last_9_Months]),max([TA_Calls_Last_9_Months])
,min([PO_In_Next_4_Months_Flag]),max([PO_In_Next_4_Months_Flag])
,min([TA_in_NEXT_3_months_flag]),max([TA_in_NEXT_3_months_flag])
,min([AB_in_Next_3_Months_Flag]),max([AB_in_Next_3_Months_Flag])
,min([SC_In_Next_4_Months_Flag]),max([SC_In_Next_4_Months_Flag])
,min([IC_Calls_Last_2_Years]),max([IC_Calls_Last_2_Years])
,min([TA_Calls_Last_2_Years]),max([TA_Calls_Last_2_Years])
,min([PAT_Calls_Last_2_Years]),max([PAT_Calls_Last_2_Years])
,min([PO_Events_Last_Year]),max([PO_Events_Last_Year])
,min([PO_Events_Last_3_Months]),max([PO_Events_Last_3_Months])
,min([PO_Events_Last_6_Months]),max([PO_Events_Last_6_Months])
,min([PO_Events_Last_9_Months]),max([PO_Events_Last_9_Months])
,min([PO_Events_Last_2_Years]),max([PO_Events_Last_2_Years])
,min([AB_Events_Last_Year]),max([AB_Events_Last_Year])
,min([AB_Events_Last_3_Months]),max([AB_Events_Last_3_Months])
,min([AB_Events_Last_6_Months]),max([AB_Events_Last_6_Months])
,min([AB_Events_Last_9_Months]),max([AB_Events_Last_9_Months])
,min([AB_Events_Last_2_Years]),max([AB_Events_Last_2_Years])
,min([SC_Events_Last_Year]),max([SC_Events_Last_Year])
,min([SC_Events_Last_3_Months]),max([SC_Events_Last_3_Months])
,min([SC_Events_Last_6_Months]),max([SC_Events_Last_6_Months])
,min([SC_Events_Last_9_Months]),max([SC_Events_Last_9_Months])
,min([SC_Events_Last_2_Years]),max([SC_Events_Last_2_Years])
,min([PC_Events_Last_Year]),max([PC_Events_Last_Year])
,min([PC_Events_Last_3_Months]),max([PC_Events_Last_3_Months])
,min([PC_Events_Last_6_Months]),max([PC_Events_Last_6_Months])
,min([PC_Events_Last_9_Months]),max([PC_Events_Last_9_Months])
,min([PC_Events_Last_2_Years]),max([PC_Events_Last_2_Years])
,min([AB_in_24m_flag]),max([AB_in_24m_flag])
,min([cuscan_in_24m_flag]),max([cuscan_in_24m_flag])
,min([syscan_in_24m_flag]),max([syscan_in_24m_flag])
,min([TA_in_24m_flag]),max([TA_in_24m_flag])
,min([TA_in_3_6_Months_Flag]),max([TA_in_3_6_Months_Flag])
,min([Box_Offer_Last_2_Years]),max([Box_Offer_Last_2_Years])
,min([BroadBand_and_Talk_Last_2_Years]),max([BroadBand_and_Talk_Last_2_Years])
,min([Install_Offer_Last_2_Years]),max([Install_Offer_Last_2_Years])
,min([Others_Last_2_Years]),max([Others_Last_2_Years])
,min([Others_PPO_Last_2_Years]),max([Others_PPO_Last_2_Years])
,min([Service_Call_Last_2_Years]),max([Service_Call_Last_2_Years])
,min([TV_Packs_Last_2_Years]),max([TV_Packs_Last_2_Years])
,min([Box_Offer_Last_Year]),max([Box_Offer_Last_Year])
,min([BroadBand_and_Talk_Last_Year]),max([BroadBand_and_Talk_Last_Year])
,min([Install_Offer_Last_Year]),max([Install_Offer_Last_Year])
,min([Others_Last_Year]),max([Others_Last_Year])
,min([Others_PPO_Last_Year]),max([Others_PPO_Last_Year])
,min([Service_Call_Last_Year]),max([Service_Call_Last_Year])
,min([TV_Packs_Last_Year]),max([TV_Packs_Last_Year])
,min([Box_Offer_Last_6_Months]),max([Box_Offer_Last_6_Months])
,min([BroadBand_and_Talk_Last_6_Months]),max([BroadBand_and_Talk_Last_6_Months])
,min([Install_Offer_Last_6_Months]),max([Install_Offer_Last_6_Months])
,min([Others_Last_6_Months]),max([Others_Last_6_Months])
,min([Others_PPO_Last_6_Months]),max([Others_PPO_Last_6_Months])
,min([Service_Call_Last_6_Months]),max([Service_Call_Last_6_Months])
,min([TV_Packs_Last_6_Months]),max([TV_Packs_Last_6_Months])
,min([Box_Offer_Last_3_Months]),max([Box_Offer_Last_3_Months])
,min([BroadBand_and_Talk_Last_3_Months]),max([BroadBand_and_Talk_Last_3_Months])
,min([Install_Offer_Last_3_Months]),max([Install_Offer_Last_3_Months])
,min([Others_Last_3_Months]),max([Others_Last_3_Months])
,min([Others_PPO_Last_3_Months]),max([Others_PPO_Last_3_Months])
,min([Service_Call_Last_3_Months]),max([Service_Call_Last_3_Months])
,min([TV_Packs_Last_3_Months]),max([TV_Packs_Last_3_Months])
,min([price_protection_flag]),max([price_protection_flag])
,min([Total_Expiring_Comms_Offers_Next_4_6_Months]),max([Total_Expiring_Comms_Offers_Next_4_6_Months])
,min([Total_Expiring_Other_Offers_Next_4_6_Months]),max([Total_Expiring_Other_Offers_Next_4_6_Months])
,min([Total_Expiring_DTV_Offers_Next_4_6_Months]),max([Total_Expiring_DTV_Offers_Next_4_6_Months])
,min([Total_Expiring_Offer_Value_Next_4_6_Months]),max([Total_Expiring_Offer_Value_Next_4_6_Months])
,min([Total_Expiring_Comms_Offers_Next_3_Months]),max([Total_Expiring_Comms_Offers_Next_3_Months])
,min([Total_Expiring_Other_Offers_Next_3_Months]),max([Total_Expiring_Other_Offers_Next_3_Months])
,min([Total_Expiring_DTV_Offers_Next_3_Months]),max([Total_Expiring_DTV_Offers_Next_3_Months])
,min([Total_Expiring_Offer_Value_Next_3_Months]),max([Total_Expiring_Offer_Value_Next_3_Months])
,min([sum_unstable_flags]),max([sum_unstable_flags])
,min([segment]),max([segment])
,min([last_sports_downgrades_dt]),max([last_sports_downgrades_dt])
,min([last_sports_upgrades_dt]),max([last_sports_upgrades_dt])
,min([last_movies_downgrades_dt]),max([last_movies_downgrades_dt])
,min([last_movies_upgrades_dt]),max([last_movies_upgrades_dt])
,min([num_sports_downgrades_ever]),max([num_sports_downgrades_ever])
,min([num_sports_upgrades_ever]),max([num_sports_upgrades_ever])
,min([num_movies_downgrades_ever]),max([num_movies_downgrades_ever])
,min([num_movies_upgrades_ever]),max([num_movies_upgrades_ever])
,min([snapshot_date]),max([snapshot_date])
,min([skygo_distinct_activitydate_last90days]),max([skygo_distinct_activitydate_last90days])
,min([skygo_distinct_activitydate_last180days]),max([skygo_distinct_activitydate_last180days])
,min([skygo_distinct_activitydate_last270days]),max([skygo_distinct_activitydate_last270days])
,min([skygo_distinct_activitydate_last360days]),max([skygo_distinct_activitydate_last360days])
,min([od_distinct_activity_last90days]),max([od_distinct_activity_last90days])
,min([od_distinct_activity_last180days]),max([od_distinct_activity_last180days])
,min([od_distinct_activity_last270days]),max([od_distinct_activity_last270days])
,min([MR]),max([MR])
,min([ThreeDTV]),max([ThreeDTV])
,min([HDTV]),max([HDTV])
,min([TopTier]),max([TopTier])
,min([movies]),max([movies])
,min([Sports]),max([Sports])
,min([Movies_upgrade_last_30days]),max([Movies_upgrade_last_30days])
,min([Movies_upgrade_last_60days]),max([Movies_upgrade_last_60days])
,min([Movies_upgrade_last_90days]),max([Movies_upgrade_last_90days])
,min([Movies_upgrade_last_180days]),max([Movies_upgrade_last_180days])
,min([Movies_downgrade_last_30days]),max([Movies_downgrade_last_30days])
,min([Movies_downgrade_last_60days]),max([Movies_downgrade_last_60days])
,min([Movies_downgrade_last_90days]),max([Movies_downgrade_last_90days])
,min([Movies_downgrade_last_180days]),max([Movies_downgrade_last_180days])
,min([Sports_upgrade_last_30days]),max([Sports_upgrade_last_30days])
,min([Sports_upgrade_last_60days]),max([Sports_upgrade_last_60days])
,min([Sports_upgrade_last_90days]),max([Sports_upgrade_last_90days])
,min([Sports_upgrade_last_180days]),max([Sports_upgrade_last_180days])
,min([Sports_downgrade_last_30days]),max([Sports_downgrade_last_30days])
,min([Sports_downgrade_last_60days]),max([Sports_downgrade_last_60days])
,min([Sports_downgrade_last_90days]),max([Sports_downgrade_last_90days])
,min([Sports_downgrade_last_180days]),max([Sports_downgrade_last_180days])
,min([Date_of_Last_TA_Call]),max([Date_of_Last_TA_Call])
,min([Date_of_Last_PAT_Call]),max([Date_of_Last_PAT_Call])
,min([Date_of_Last_IC_Call]),max([Date_of_Last_IC_Call])
,min([Product_Holding]),max([Product_Holding])
,min([dtv_latest_act_date]),max([dtv_latest_act_date])
,min([cvs_segment]),max([cvs_segment])
from ta_modeling_raw_data

select product_holding,count() from tanghoi.ta_modeling_raw_data group by product_holding
select product_holding,count() from ta_modeling_raw_data group by product_holding
select product_holding,count() from yarlagaddar.view_attachments_201311 group by product_holding
select product_holding,count() from yarlagaddar.view_attachments_201409 group by product_holding
select product_holding,count() from hutchij.view_attachments_201410 group by product_holding

select top 10 * from TA_MODELING_RAW_DATA
snapshot_date

select top 10 account_number
,prod_active_broadband_package
,prod_active_sky_talk_package



create variable @username varchar(20);
set @username = 'greenj'

  SELECT table_name
         ,cast((select kbytes / 1024 from sp_iqtablesize(@username ||'."' ||table_name || '"')) as decimal(16,5)) as mbytes
    FROM sys.systable
   WHERE user_name(creator)  = @username



select count() from modeler_output
where date_of_last_ic_call is not null

select count() from ta_modeling_raw_data
where date_of_last_ic_call is null


