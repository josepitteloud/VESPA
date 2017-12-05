/*

purpose:
to develop code for TA, stripping SQL code out from spss

supernode - future calling churn, file 2014-03-12_TA Model Development - ETL v3.0.str

*/

select *
into #future_calling_churn1
from
#futureTA_output ta
full outer join
#futureAB_output ab
on ta.account_number=ab.account_number


select *
into #future_calling_churn2
from
#future_calling_churn1 c1
full outer join
#futureSC_output sc
on c1.account_number=sc.account_number


select *
into #future_calling_churn3
from
#future_calling_churn2 c2
full outer join
#futurePO_output sc
on c2.account_number=sc.account_number


select *
into #future_calling_churn4
from
#future_calling_churn3 c3
full outer join
#futureTA_4_6_m_output sc
on c3.account_number=sc.account_number

