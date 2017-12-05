  select account_number
        ,max(adjusted_event_start_date_vespa) as dt
    into #viq
    from viq_viewing_data_scaling
group by account_number
; --1,243,726

  select datediff(month, dt, now()) as m
        ,count() as cow
    into #months
    from #viq as viq
         inner join cust_single_account_view as sav on viq.account_number = sav.account_number
   where cust_active_dtv=0
group by m
;

  select m2.m
        ,sum(m1.cow)
    from #months as m1
         inner join #months as m2 on m1.m < m2.m
group by m2.m


-- 55k viewing panel members accounc churned in the last 12m
-- 91k in the last 18m
