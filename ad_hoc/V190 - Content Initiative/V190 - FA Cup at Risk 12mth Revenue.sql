

select segment , count(*), sum(scaled_accounts) 
from Shaha.FA_CUP_DATA_CUBE_With_Segments_Valuation 
group by segment order by segment;

select account_number
,scaled_accounts
into #high_risk_accounts
from Shaha.FA_CUP_DATA_CUBE_With_Segments_Valuation 
where segment in ('2a. Has ESPN, FA CUP has >=50% SOV on ESPN, ESPN Loyalty = All, Has Sky Sports'
,'2b. Has ESPN, FA CUP has >=50% SOV on ESPN, ESPN Loyalty = All, No Sky Sports')
;
--select top 100 * from #high_risk_accounts
commit;
create hg index idx1 on #high_risk_accounts(account_number);
commit;
--Get Bill details of previous 12 months
--drop table  #last_12M_paid_amt ;
select a.account_number
,a.scaled_accounts
,sum(b.total_paid_amt*-1) as total_bill_amt_paid
into #last_12M_paid_amt 
from #high_risk_accounts as a
left outer join sk_prod.cust_bills  as b
on a.account_number = b.account_number
where payment_due_dt between '2012-08-01' and '2013-07-31'
group by a.account_number
,a.scaled_accounts
;

--select top 500 * from #last_12M_paid_amt  order by total_bill_amt_paid 

select sum(total_bill_amt_paid*scaled_accounts) as total_last_12m_revenue from #last_12M_paid_amt;


---Repeat for Football League---
select football_league_segment , count(*) as accounts ,sum(scaled_accounts) as weighted_accounts 
from Shaha.F_DATA_CUBE_WITH_SEGMENTS_Valuation 
group by football_league_segment order by football_league_segment;


select account_number
,scaled_accounts
into #high_risk_accounts_FL
from Shaha.F_DATA_CUBE_WITH_SEGMENTS_Valuation
where football_league_segment in ('1a. Has SS, FL has highest SOV on SS, High FL Loyalty on SS (Highest Risk)'
,'1b. Has SS, FL has highest SOV on SS, Medium FL Loyalty on SS (High Risk)'
,'2a. Has SS, FL does NOT have highest SOV on SS, High FL Loyalty on SS (High Risk)')
;
--select top 100 * from #high_risk_accounts
commit;
create hg index idx1 on #high_risk_accounts_FL(account_number);
commit;
--Get Bill details of previous 12 months
--drop table  #last_12M_paid_amt ;
select a.account_number
,a.scaled_accounts
,sum(b.total_paid_amt*-1) as total_bill_amt_paid
into #last_12M_paid_amt_FL 
from #high_risk_accounts_FL as a
left outer join sk_prod.cust_bills  as b
on a.account_number = b.account_number
where payment_due_dt between '2012-08-01' and '2013-07-31'
group by a.account_number
,a.scaled_accounts
;


select sum(total_bill_amt_paid*scaled_accounts) as total_last_12m_revenue from #last_12M_paid_amt_FL;





/*
select * from sk_prod.cust_single_account_view where account_number = '620037809220'

select * from sk_prod.cust_subs_hist where account_number = '620037809220' and effective_to_dt = '9999-09-09'


select * from sk_prod.cust_bills 
where account_number = '620051965122' and payment_due_dt between '2012-08-01' and '2013-07-31' order by sequence_num;
*/
/*
select * from sk_prod.cust_bills 
where account_number = '620041578563'  and payment_due_dt between '2012-08-01' and '2013-07-31' order by sequence_num;
*/


