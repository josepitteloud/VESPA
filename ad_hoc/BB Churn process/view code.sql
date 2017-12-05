create view Broadband_Comms_Pipeline_TEST
  as select a.Subs_Year,
    a.Subs_Week_Of_Year,
    a.Subs_Week_And_Year,
    a.Event_Dt,
    a.Order_ID,
    a.Account_Number,
    a.Country,
    a.Created_By_ID,
    a.prev_status_code,
    a.status_code,
    a.Enter_SysCan,
    case when b.account_number is not null then 0 else a.Enter_CusCan end as Enter_CusCan,
    case when b.account_number is not null then 1 else a.Enter_HM end as Enter_HM,
    a.Enter_3rd_Party,
    a.PC_Effective_To_Dt,
    a.PC_Future_Sub_Effective_Dt,
    a.PC_Next_Status_Code,
    a.AB_Effective_To_Dt,
    a.AB_Future_Sub_Effective_Dt,
    a.AB_Next_Status_Code,
    a.BCRQ_Effective_To_Dt,
    a.BCRQ_Future_Sub_Effective_Dt,
    a.BCRQ_Next_Status_Code,
    a.BB_Cust_Type,
    a.ProdPlat_Churn_Type
    from citeam.Broadband_Comms_Pipeline as a
      left outer join pitteloudj.BB_churn_HM_accounts as b on a.account_number = b.account_number
      and a.order_ID = b.order_ID
      and a.event_dt = b.event_dt
