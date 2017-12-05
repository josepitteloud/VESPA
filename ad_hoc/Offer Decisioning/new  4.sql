/* *******************************************************************
                                                MCKINSEY_FULL_ACTIVE_OFFER_BASE
********************************************************************* */
 
SELECT
      a.account_number,
       a.subscription_id,
       a.currency_code,
       a.subscription_sub_type,
       a.effective_from_dt,
       a.effective_to_dt,
       a.status_code_changed,
       a.status_code,
       a.prev_status_code,
       a.ent_cat_prod_changed,
       a.current_product_description,
       b.prem_movies,
       b.prem_sports
  --     into MCKINSEY_FULL_ACTIVE_OFFER_BASE
 
       from cust_subs_hist a
 
       left join cust_entitlement_lookup b
       on a.current_short_description = b.short_description
 
       group by
        a.account_number,
        a.subscription_id,
        a.currency_code,
        a.subscription_sub_type,
        a.effective_from_dt,
        a.effective_to_dt,
        a.status_code_changed,
        a.prev_status_code,
        a.status_code,
        a.ent_cat_prod_changed,
        a.current_product_description,
        b.prem_movies,
        b.prem_sports;---1188833961 Row(s) affected
 
 
 
 
/* *******************************************************************
                                                MCKINSEY_FULL_BILLS_HIST
********************************************************************* */
 
select *
--INTO NORYD.MCKINSEY_FULL_BILLS_HIST
from (
    select *,
           RANK() OVER ( partition by account_number,  year_due ,month_due  order by sequence_num desc) as Billrank
    from (
            select
                datepart(year,payment_due_dt) as year_due,
                datepart(month,payment_due_dt) as month_due,
               cb.account_number,
                sequence_num,
                payment_due_dt,
                cb.total_new_charges as amount_due,
                count(*) as n
            from    cust_bills cb
                        inner join cust_subs_hist csh
                                on cb.account_number = csh.account_number
                where           cb.payment_due_dt >= '2012-01-01'
                and             csh.status_code in ('AC','PC','AB','A','PT','CF','BCRQ','FBI','FBP')
                and             csh.effective_from_dt <= cb.payment_due_dt
                and             csh.effective_to_dt > cb.payment_due_dt
                and             csh.subscription_sub_type not in ('CLOUDWIFI','MCAFEE')
                AND             csh.account_sub_type IN ('Normal','?')
            group by year_due, month_due,cb.account_number,sequence_num,payment_due_dt, amount_due
    ) bills
 
) bills_ranked
left join sky_calendar b on bills_ranked.payment_due_dt = b.calendar_date
where billrank=1;---643063725 Row(s) affected
 
 
 
 
 
 
/* *******************************************************************
                                                MCKINSEY_FULL_CALLS_HIST 
********************************************************************* */
 
 
select a.account_number,
       cast(created_date as date) As Event_Date,
       count(*) as Calls,
       0 as saves,
       'IC' as TypeOfEvent
   --    into MCKINSEY_FULL_CALLS_HIST_NEW_1
       from cust_contact a
       inner join mckinsey_offer_hist_master b
       on a.account_number = b.account_number
       where cast(created_date as date) >= '2011-01-01'
       and contact_channel = 'I PHONE COMMUNICATION'
       and contact_grouping_identity is not null
       group by a.account_number, event_date
 
       union
 
select a.account_number,
       event_dt as Event_Date,
       count(*) as Calls,
       sum(saves) as saves,
       'TA' as TypeOfEvent
       from view_cust_calls_hist a
       inner join mckinsey_offer_hist_master b
       on a.account_number = b.account_number
       where typeofevent = 'TA'
       and event_dt >= '2011-01-01'
       group by a.account_number, event_date
 
union
 
select a.account_number,
       event_dt as Event_Date,
       count(*) as Calls,
       sum(saves) as saves,
       'PAT' as TypeOfEvent
       from view_cust_calls_hist a
       inner join mckinsey_offer_hist_master b
       on a.account_number = b.account_number
       where typeofevent = 'PAT'
       and event_dt >= '2011-01-01'
       group by a.account_number, event_date;----330620418 Row(s) affected
 
 
 
 
/* *******************************************************************
                                                MCKINSEY_FULL_CALL_DETAIL 
********************************************************************* */
 
SELECT
   account_number
  ,call_date
  ,initial_sct_grouping
  ,initial_working_location
  ,final_working_location
  ,final_sct_grouping
  ,start_date_time
  ,total_transfers
--INTO noryd.MCKINSEY_FULL_CALL_DETAIL
FROM calls_details;
 
 
 
/* *******************************************************************
                                                Contact History
********************************************************************* */
 
select a.account_number,
       cast(created_date as date) As Event_Date,
       count(*) as Calls,
       'IC' as TypeOfEvent
     --  into MCKINSEY_FULL_CONTACT_HIST
       from cust_contact a
       where cast(created_date as date) >= '2011-01-01'
       and contact_channel = 'I PHONE COMMUNICATION'
       and contact_grouping_identity is not null
       group by a.account_number, event_date
 
       union
 
select a.account_number,
       event_dt as Event_Date,
       sum(total_calls) as Calls,
       'TA' as TypeOfEvent
       from cust_calls_hist a
 
       where typeofevent = 'TA'
       and event_dt >= '2011-01-01'
       group by a.account_number, event_date
 
union
 
select a.account_number,
       event_dt as Event_Date,
       sum(total_calls) as Calls,
       'PAT' as TypeOfEvent
       from cust_calls_hist a
             where typeofevent = 'PAT'
       and event_dt >= '2011-01-01'
       group by a.account_number, event_date;
 
 
/* *******************************************************************
                                                MCKINSEY_FULL_CONTRACT_HIST – this data is from VALER, LUKE
********************************************************************* */
select a.* into NORYD.MCKINSEY_CONTRACT_HIST
from valerl.MCKINSEY_CONTRACT_HIST_MASTER a inner join mckinsey_offer_hist_master b on a.account_number = b.account_number;
 
 
 
 
/* *******************************************************************
                                                MCKINSEY_FULL_OFFER_HIST
********************************************************************* */
 
 
select    account_number
        , subscription_id
        , subs_type
--      , currency_code
        , offer_id
        , offer_dim_description
        , Offer_Value
        , offer_duration
        , Total_Offer_Value_Yearly
        , Offer_Start_Dt_Actual
        , Offer_End_Dt_Actual
        , Intended_Offer_Start_Dt
        , Intended_Offer_End_Dt
        , Intended_Total_Offer_Value_Yearly
        , Status_Change_Date
        , Initial_Effective_Dt
        , created_dt
        , created_by_id
        , Intended_offer_amount
        , offer_status
        , src_system_id
        , order_id
        , ORIG_PORTFOLIO_OFFER_ID
   --   , Offer_Period
        , subs_first_act_dt
        , subs_latest_act_dt
        , sports_act_dt
        , movies_act_dt
        , dtv_first_act_dt
        , dtv_latest_act_dt
        , dtv_act_dt
        , bb_first_act_dt
        , bb_latest_act_dt
        , bb_act_dt
        , status_change
        , package_movement
        , ta, pat
        , coe
        , any_call
        , Offer_Segment
        , Offer_Segment_Detail
        , Offer_Segment_Grouped
--     , offer_value_ex_vat
        , offer_segment_grouped_1
--into noryd.MCKINSEY_FULL_OFFER_HIST
from CITEAM.offer_usage_all;----106164661 Row(s) affected
 
 
 
 
 
/* *******************************************************************
                                                MCKINSEY_FULL_OD_DOWNLOADS – This if from Luca
********************************************************************* */
 
 
select a.* into MCKINSEY_FULL_OD_DOWNLOADS
from OD_Summary_downloads_McKinsey a--1483825914 Row(s) affected
                                    ---1798892368 Row(s) affected
 
 
/* *******************************************************************
                                                MCKINSEY_FULL_BOX_HIST
********************************************************************* */
 
select account_number,
       created_dt,
       x_model_number,
       x_manufacturer,
       x_box_type
  --     into MCKINSEY_FULL_BOX_HIST
       from cust_set_top_box a
       group by  account_number,
       created_dt,
       x_model_number,
       x_manufacturer,
       x_box_type;
 
 
 
/* *******************************************************************
                                                MCKINSEY_FULL_SIMPLE_SEGMENT_HIST
********************************************************************* */
 
select account_number,
       segment,
       segment_lev2,
       observation_date
--      into MCKINSEY_FULL_SIMPLE_SEGMENT_HIST
       from simple_segments_history
       group by account_number,
       segment,
       segment_lev2,
       observation_date;--181555154 Row(s) affected
 
 
 
/* *******************************************************************
                                                MCKINSEY_FULL_BILL_DETAILS
********************************************************************* */
SELECT * 
--into NORYD.MCKINSEY_FULL_BILL_DETAILS 
From
billing_details--624825594 Row(s) affected
 
 
 
/* *******************************************************************
                                                MCKINSEY_FULL_ACTIVATIONS_RTM
********************************************************************* */
 
SET TEMPORARY OPTION Query_Temp_Space_Limit = 0;
 
WITH Activations
AS
(
 
Select acq.account_number,acq.Event_dt,acq.Acquisition_Type,ord.order_dt,ord.order_number,RTM_LEVEL_1,RTM_LEVEL_2,RTM_LEVEL_3,
row_number() over(partition by acq.account_number,acq.event_dt order by ord.order_dt desc,order_number desc) Order_Rnk
--into #Activations
from citeam.DTV_ACQUISITION_MART acq
     left join
     citeam.dm_orders ord
     on ord.account_number = acq.account_number
       and ord.order_dt between acq.event_dt - 90 and acq.event_dt
        and ord.order_status not in ('APPCAN','CANCLD')
        and Family_added + Variety_added + Original_added + SkyQ_Added > 0
        and Family_removed + Variety_removed + Original_removed + SkyQ_removed = 0
where acq.event_dt >= '2014-06-27'
    and acq.subscription_sub_type = 'DTV Primary Viewing'
--     and acq.status_code = 'AC'
--order by acq.account_number,acq.Event_dt,acq.Acquisition_Type,ord.order_dt desc,order_number desc;--3740981 Row(s) affected
)
SELECT * FROM Activations WHERE Order_Rnk = 1
 
 
/* *******************************************************************
                                                MCKINSEY_FULL_DEMOGRAPHICS
********************************************************************* */
 
select a.cb_key_household,
       b.account_number,
       affluence,
       lifestage,
       hh_composition,
       income,
       mosaic_group,
       hh_fss_group,
       hh_noumber_children as hh_number_children,
       h_age
       --postcode,
       --fibre,
       --cable
   --    into MCKINSEY_FULL_DEMOGRAPHICS
       from Demographics_McKinsey a
inner join cust_single_account_view b
on a.cb_key_household = b.cb_key_household
group by
       a.cb_key_household,
       b.account_number,
       affluence,
       lifestage,
       hh_composition,
       income,
       mosaic_group,
       hh_fss_group,
       hh_number_children,
       h_age; 
 
