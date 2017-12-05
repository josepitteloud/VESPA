/*

purpose:
to develop code for TA, stripping SQL code out from spss

supernode - churn_history, file 2014-03-12_TA Model Development - ETL v3.0.str

*/

-- datamart branch

select *
into #data_mart1
from yarlagaddar.View_attachments_201308 where Status_Code = 'AC'
;

select *,@Reference as Reference
into #data_mart2
from #data_mart1
;

-- merge
select *
into #data_mart3
from
#data_mart2 dm2
inner join
SourceDates sd
on dm2.Reference=sd.Reference
;

create or replace variable @likestring varchar(4)
;

set @likestring='%' || @Sample_1_EndString

;
-- select
select *
into #data_mart4
from #data_mart3
where account_number like @likestring -- insert the variable


-- filter
select
Reference
,Attachments_Table
,Account_Numbers_ending_in
,Snapshot_Date
,2_Years_Prior
,1_Year_Prior
,10_Months_Prior
,9_Months_Prior
,6_Months_Prior
,3_Months_Prior
,1_Month_Prior
,1_Month_Future
,2_Months_Future
,3_Months_Future
,4_Months_Future
,5_Months_Future
,6_Months_Future
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
,num_sports_downgrades_ever
,last_sports_downgrades_dt
,num_sports_upgrades_ever
,last_sports_upgrades_dt
,num_movies_downgrades_ever
,last_movies_downgrades_dt
,num_movies_upgrades_ever
,last_movies_upgrades_dt
into #data_mart5
from #data_mart4
-- end of datamart branch
-- #data_mart5 is the output table


-- below the branch coming from supernode churnHistory
-- #table_churn_history_output is the output table coming from the ChurnHistory supernode

-- restructure
select *
, case when status_code='AB' then date_from_dt else cast(NULL as date) as status_code_AB_effective_from_dt
, case when status_code='PO' then date_from_dt else cast(NULL as date) as status_code_PO_effective_from_dt
, case when status_code='SC' then date_from_dt else cast(NULL as date) as status_code_SC_effective_from_dt
into #data_mart6
from #table_churn_history_output


-- aggregate
select account_number
,max(status_code_AB_effective_from_dt) as status_code_AB_effective_from_dt_Max
,max(status_code_PO_effective_from_dt) as status_code_PO_effective_from_dt_Max
,max(status_code_SC_effective_from_dt) as status_code_SC_effective_from_dt_Max
group by account_number
into #data_mart7
from #data_mart6


-- flag
select account_number
,status_code_AB_effective_from_dt_Max
,status_code_PO_effective_from_dt_Max
,status_code_SC_effective_from_dt_Max
,case when status_code_AB_effective_from_dt_Max is not NULL then 1 else 0 as status_code_AB_effective_from_dt_Max_flag
,case when status_code_PO_effective_from_dt_Max is not NULL then 1 else 0 as status_code_PO_effective_from_dt_Max_flag
,case when status_code_SC_effective_from_dt_Max is not NULL then 1 else 0 as status_code_SC_effective_from_dt_Max_flag
into #data_mart8
from #data_mart7

-- churnhistory branch, before the merge

-- merge with the datamart branch
select *
into #data_mart9
from
#data_mart5 d5
right join
#data_mart8 d8
on d5.account_number=d8.account_number
-- churnhistory branch, after the merge with datamart, bfore the merge with ta_call_history supernode
-- output is #data_mart9

--ta_call_history branch
-- restructure
select *
, case when type_of_event='TA' then event_dt else cast(NULL as date) as type_of_event_TA_event_dt
into #data_mart10
from #TA_call_history_output

--aggregate
select account_number
,max(type_of_event_TA_event_dt) as type_of_event_TA_event_dt_Max
group by account_number
into #data_mart11
from #data_mart10

--flag
select account_number
,type_of_event_TA_event_dt_Max
,case when type_of_event_TA_event_dt_Max is not NULL then 1 else 0 as type_of_event_TA_event_dt_Max_flag
into #data_mart12
from #data_mart11
-- end of branch TA_call_history
-- output is #data_mart12


-- merge of branches TA_call_history and churn history
select *
into #data_mart_output
from
#data_mart9 d9
right join
#data_mart12 d12
on d9.account_number=d12.account_number





