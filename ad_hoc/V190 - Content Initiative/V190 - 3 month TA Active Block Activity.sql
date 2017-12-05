/*------------------------------------------------------------------------------------------------------------------
        Project:    V190-CONTENT INITIATIVE
        Program:    PRODUCE DATASET OF ACTIVE_BLOCKED, TA_FAILED,TA_SAVED, NONE FOR END OF FEB ACTIVE BASE, STATUS IN MARCH
        Version:    3 -change to 'churn definition' and now only foucsing on March 2013
        Created:    20130618
        Lead:       SUSANNE CHAN
        Analyst:    Dan Barnett
        SK Prod:    5
        QA:         
------------------------------------------------------------------------------------------------------------------*/
--GET ACTIVES AS OF FEB 28TH 2013;
if object_id ('V190_churn_ta_status_3months') is not null then
            drop table V190_churn_ta_status_3months
end if;

select          distinct(csh.account_number) as account_number
                ,1 as active_march_start        
into            V190_churn_ta_status_3months
FROM            sk_prod.cust_subs_hist csh
WHERE           csh.subscription_sub_type = 'DTV Primary Viewing'
and             status_code='AC'
and             effective_from_dt<= '2013-02-28'
and             effective_to_dt>'2013-02-28'
order by        account_number
;

--GET MARCH CHURN STATUS;
--drop table      #V190_status_change_201303;

select          account_number
                ,effective_from_dt as status_change_date
                ,case 
                when status_code ='AB' then 'Active Blocked'
                when status_code ='PC' then 'Pend Cancel'
                when status_code ='PO' then 'Cuscan'
                when status_code ='SC' then 'Syscan'
                else 'Not churn' end as status_201303
                ,rank() over (partition by  csh.account_number 
                order by  csh.effective_from_dt,csh.cb_row_id) as churn_rank--Rank to get the first event
into            #V190_status_change_201303
from            sk_prod.cust_subs_hist as csh
where           subscription_sub_type ='DTV Primary Viewing'     
and             status_code in ('PO','SC','PC','AB')                       
and             status_code_changed = 'Y' 
and             effective_from_dt >= '2013-03-01'
and             effective_from_dt <= '2013-05-31'                 
and             effective_from_dt != effective_to_dt
order by        account_number
;
delete from #V190_status_change_201303       
where churn_rank>1;

--select count(*),count(distinct(account_number)) from #V190_status_change_201303;



--TURN AROUNDS;

--drop table #V190_TA_201303;

SELECT      cca.account_number
            ,min(CASE WHEN cca.Wh_Attempt_Outcome_Description_1 IN ( 'Turnaround Saved'
                                                                 ,'Legacy Save'
                                                                 ,'Home Move Saved'
                                                                 ,'Home Move Accept Saved')
            then 'b)TA_SAVED' else 'a)TA_FAILED' end) as ta_outcome         
INTO        #V190_TA_201303
FROM        sk_prod.cust_change_attempt AS cca
inner join  sk_prod.cust_subscriptions AS subs
ON          cca.subscription_id = subs.subscription_id
WHERE       cca.change_attempt_type                  = 'CANCELLATION ATTEMPT'
AND         subs.ph_subs_subscription_sub_type       = 'DTV Primary Viewing'
AND         cca.attempt_date                           >= '2013-03-01' 
AND         cca.attempt_date                           <= '2013-05-31'    
AND         cca.created_by_id  NOT IN ('dpsbtprd', 'batchuser')
AND         cca.Wh_Attempt_Outcome_Description_1 in 
            ('Turnaround Saved','Legacy Save','Home Move Saved','Home Move Accept Saved','Turnaround Not Saved','Legacy Fail','Home Move Not Saved')
group by    cca.account_number
order by    cca.account_number
;
--delete from #V190_TA_201303 where rank_id>1;

--select count(*),count(distinct(account_number)) from #V190_TA_201303;


--COMBINE ACTIVE BASE AND CHURN STATUS;
alter table     V190_churn_ta_status_3months
                add status_201303 varchar(14)
                ,add status_change_date date
                ,add ta_status_201303 varchar(11)
                ,add ta_status_date date       
                ,add march_may_event varchar(14)
;

update          V190_churn_ta_status_3months
set             status_201303=      case when b.status_201303 is not null then b.status_201303 else 'Not churn' end
                ,status_change_date=case when b.status_change_date is not null then b.status_change_date else cast('9999-12-31' as date) end
from            V190_churn_ta_status_3months as a 
left join       #V190_status_change_201303 as b
on              a.account_number=b.account_number
order by        a.account_number
;
commit;

update          V190_churn_ta_status_3months
set             ta_status_201303=case when b.ta_outcome is not null then b.ta_outcome else '*****' end
--                ,ta_status_date= null
from            V190_churn_ta_status_3months as a 
left join       #V190_TA_201303 as b
on              a.account_number=b.account_number
order by        a.account_number
;
commit;

update          V190_churn_ta_status_3months
set             march_may_event=case 
                when status_201303='Active Blocked' then 'a)ACTIVE_BLOCK' 
                when ta_status_201303 ='a)TA_FAILED' then 'b)TA_FAILED'
                when ta_status_201303 ='b)TA_SAVED' then 'c)TA_SAVED'
                else 'd)NONE' end
;
commit;

--select march_may_event , count(*) as records from V190_churn_ta_status_3months group by march_may_event order by march_may_event;